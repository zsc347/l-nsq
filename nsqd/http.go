package nsqd

import (
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/l-nsq/internal/protocol"

	"github.com/l-nsq/internal/version"

	"github.com/l-nsq/internal/httprouter"

	"github.com/l-nsq/internal/http_api"
)

var boolParams = map[string]bool{
	"true":  true,
	"1":     true,
	"false": false,
	"0":     false,
}

type httpServer struct {
	ctx         *context
	tlsEnabled  bool
	tlsRequired bool
	router      http.Handler
}

func newHTTPServer(ctx *context, tlsEnabled bool, tlsRequired bool) *httpServer {
	log := http_api.Log(ctx.nsqd.logf) // log decorator

	router := httprouter.New()
	router.HandleMethodNotAllowed = true
	router.PanicHandler = http_api.LogPanicHandler(ctx.nsqd.logf)
	router.NotFound = http_api.LogNotFoundHandler(ctx.nsqd.logf)
	router.MethodNotAllowed = http_api.LogMethodNotAllowedHandler(ctx.nsqd.logf)

	s := &httpServer{
		ctx:         ctx,
		tlsEnabled:  tlsEnabled,
		tlsRequired: tlsRequired,
		router:      router,
	}

	router.Handle("GET", "/ping", http_api.Decorate(s.pingHandler, log, http_api.PlainText))
	router.Handle("GET", "/info", http_api.Decorate(s.doInfo, log, http_api.V1))

	// v1 negotiate
	router.Handle("POST", "/pub", http_api.Decorate(s.doPub, http_api.V1))

	return s
}

func (s *httpServer) pingHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	health := s.ctx.nsqd.GetHealth()
	if !s.ctx.nsqd.IsHealthy() {
		return nil, http_api.Err{500, health}
	}
	return health, nil
}

func (s *httpServer) doInfo(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, http_api.Err{500, err.Error()}
	}

	return struct {
		Version          string `json:"version"`
		BroadcastAddress string `json:"broadcast_address"`
		Hostname         string `json:"hostname"`
		HTTPPort         int    `json:"http_port"`
		TCPPort          int    `json:"tcp_port"`
		StartTime        int64  `json:"start_time"`
	}{
		Version:          version.Binary,
		BroadcastAddress: s.ctx.nsqd.getOpts().BroadcastAddress,
		Hostname:         hostname,
		TCPPort:          s.ctx.nsqd.RealTCPAddr().Port,
		HTTPPort:         s.ctx.nsqd.RealHTTPAddr().Port,
		StartTime:        s.ctx.nsqd.GetStartTime().Unix(),
	}, nil
}

func (s *httpServer) doPub(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	// TODO: one day I'd really like to just error on chunked requests
	// to be able to fail "too big" requests before we even read

	// This comment is no eady to handle, since we used go http handler
	// and http router, need a hook on the first place

	if req.ContentLength > s.ctx.nsqd.getOpts().MaxMsgSize {
		return nil, http_api.Err{413, "MSG_TOO_BIG"}
	}

	// add 1 so that it's greater than out max when we test for it
	// (LimitReader returns a "fake" EOF)
	readMax := s.ctx.nsqd.getOpts().MaxMsgSize + 1
	body, err := ioutil.ReadAll(io.LimitReader(req.Body, readMax))

	if err != nil {
		return nil, http_api.Err{500, "INTERNAL_ERROR"}
	}

	if int64(len(body)) == readMax {
		return nil, http_api.Err{500, "MSG_TOO_BIG"}
	}

	if len(body) == 0 {
		return nil, http_api.Err{400, "MSG_EMPTY"}
	}

	reqParams, topic, err := s.getTopicFromQuery(req)
	if err != nil {
		return nil, err
	}

	var deferred time.Duration
	if ds, ok := reqParams["defer"]; ok {
		var di int64
		di, err = strconv.ParseInt(ds[0], 10, 64)
		if err != nil {
			return nil, http_api.Err{400, "INVALID_DEFER"}
		}
		deferred = time.Duration(di) * time.Millisecond
		if deferred < 0 || deferred > s.ctx.nsqd.getOpts().MaxReqTimeout {
			return nil, http_api.Err{400, "INVALID_DEFER"}
		}
	}

	msg := NewMessage(topic.GenerateID(), body)
	msg.deferred = deferred
	err = topic.PutMessage(msg)
	if err != nil {
		return nil, http_api.Err{503, "EXITING"}
	}
	return "OK", nil
}

func (s *httpServer) getTopicFromQuery(req *http.Request) (url.Values, *Topic, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		s.ctx.nsqd.logf(LOG_ERROR, "failed to parse request params - %s", err)
		return nil, nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicNames, ok := reqParams["topic"]
	if !ok {
		return nil, nil, http_api.Err{400, "MISSING_ARG_TOPIC"}
	}

	topicName := topicNames[0]

	if !protocol.IsValidTopicName(topicName) {
		return nil, nil, http_api.Err{400, "INVALID_TOPIC"}
	}

	return reqParams, s.ctx.nsqd.GetTopic(topicName), nil
}
