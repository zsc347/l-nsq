package nsqd

import (
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/l-nsq/internal/test"
)

// ensure that we can push a message through a topic and get it out of a channel
func TestPutMessage(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)

	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_put_message" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channel := topic.GetChannel("ch")

	var id MessageID
	msg := NewMessage(id, []byte("test"))
	topic.PutMessage(msg)

	outputMsg := <-channel.memoryMsgChan
	test.Equal(t, msg.ID, outputMsg.ID)
	test.Equal(t, msg.Body, outputMsg.Body)
}
