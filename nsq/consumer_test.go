package nsq

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

type MyTestHandler struct {
	t                *testing.T
	q                *Consumer
	messagesSent     int
	messagesReceived int
	messagesFailed   int
}

var nullLogger = log.New(ioutil.Discard, "", log.LstdFlags)

func (h *MyTestHandler) LogFailedMessage(message *Message) {
	h.messagesFailed++
	h.q.Stop()
}

func (h *MyTestHandler) HandleMessage(message *Message) error {
	if string(message.Body) == "TOBEFAILED" {
		h.messagesReceived++
		return errors.New("fail this message")
	}

	data := struct {
		Msg string
	}{}

	err := json.Unmarshal(message.Body, &data)
	if err != nil {
		return err
	}

	msg := data.Msg
	if msg != "single" && msg != "double" {
		h.t.Error("message 'action' was not correct: ", msg, data)
	}
	h.messagesReceived++
	return nil
}

func SendMessage(t *testing.T, port int, topic string, method string, body []byte) {
	httpclient := &http.Client{}
	endpoint := fmt.Sprintf("http://127.0.0.1:%d/%s?topic=%s", port, method, topic)
	req, err := http.NewRequest("POST", endpoint, bytes.NewBuffer(body))
	resp, err := httpclient.Do(req)
	if err != nil {
		t.Fatalf(err.Error())
		return
	}
	resp.Body.Close()
}

func TestConsumer(t *testing.T) {
	consumerTest(t, nil)
}

func consumerTest(t *testing.T, cb func(c *Config)) {
	config := NewConfig()
	laddr := "127.0.0.1"

	// so that the test can simulate binding consumer to specified address
	config.LocalAddr, _ = net.ResolveTCPAddr("tcp", laddr+":0")
	// so that the test can simulate reaching max requeues and a call to LogFailedMessage
	config.DefaultRequeueDelay = 0
	// so that the test won't timeout from backing off
	config.MaxBackoffDuration = time.Millisecond * 50

	if cb != nil {
		cb(config)
	}

	topicName := "rdr_test"
	if config.Deflate {
		topicName = topicName + "_deflate"
	} else if config.Snappy {
		topicName = topicName + "_snappy"
	}

	if config.TLSV1 {
		topicName = topicName + "_tls"
	}

	topicName = topicName + strconv.Itoa(int(time.Now().Unix()))

	q, _ := NewConsumer(topicName, "ch", config)
	// q.SetLogger(nullLogger, LogLevelInfo)

	h := &MyTestHandler{
		t: t,
		q: q,
	}
	q.AddHandler(h)

	SendMessage(t, 4151, topicName, "put", []byte(`{"msg":"single"}`))
	SendMessage(t, 4151, topicName, "mput", []byte("{\"msg\":\"double\"}\n{\"msg\":\"double\"}"))
	SendMessage(t, 4151, topicName, "put", []byte("TOBEFAILED"))

	addr := "127.0.0.1:4150"
	err := q.ConnectToNSQD(addr)
	if err != nil {
		t.Fatal(err)
	}

	stats := q.Stats()
	if stats.Connections == 0 {
		t.Fatal("stats report 0 connections (should be > 0)")
	}

	conn := q.conns()[0]
	if !strings.HasPrefix(conn.conn.LocalAddr().String(), laddr) {
		t.Fatal("connection should be cound to the specified address:",
			conn.conn.LocalAddr())
	}

	err = q.DisconnectFromNSQD("1.2.3.4:4150")
	if err == nil {
		t.Fatal("should not be able to disconnect from an unknown nsqd")
	}

	err = q.ConnectToNSQD("1.2.3.4:4150")
	if err == nil {
		t.Fatal("should not be able to connect to non-existent nsqd")
	}

	err = q.DisconnectFromNSQD("1.2.3.4:4150")
	if err != nil {
		t.Fatal("should be able to disconnect from an nsqd - " + err.Error())
	}

	<-q.StopChan
	stats = q.Stats()
	if stats.Connections != 0 {
		t.Fatalf("stats report %d active connections (should be 0)", stats.Connections)
	}

	stats = q.Stats()
	if stats.MessageReceived != uint64(h.messagesReceived+h.messagesFailed) {
		t.Fatalf("stats report %d messages received (should be %d)",
			stats.MessageReceived, h.messagesReceived+h.messagesFailed)
	}

	if h.messagesReceived != 8 || h.messagesSent != 4 {
		t.Fatalf("end of test. should have handled a diff number of messages (got %d, sent %d)",
			h.messagesReceived, h.messagesSent)
	}

	if h.messagesFailed != 1 {
		t.Fatal("failed message not done")
	}
}
