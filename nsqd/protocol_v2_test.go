package nsqd

import (
	"io/ioutil"
	"net"
	"time"

	"github.com/l-nsq/nsq"
)

func mustStartNSQD(opts *Options) (*net.TCPAddr, *net.TCPAddr, *NSQD) {
	opts.TCPAddress = "127.0.0.1:0"
	opts.HTTPAddress = "127.0.0.1:0"
	opts.HTTPAddress = "127.0.0.1:0"

	if opts.DataPath == "" {
		tmpDir, err := ioutil.TempDir("", "nsq-test-")
		if err != nil {
			panic(err)
		}
		opts.DataPath = tmpDir
	}

	nsqd := New(opts)
	nsqd.Main()
	return nsqd.RealTCPAddr(), nsqd.RealHTTPAddr(), nsqd
}

func mustConnectNSQD(tcpAddr *net.TCPAddr) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", tcpAddr.String(), time.Second)
	if err != nil {
		return nil, err
	}
	conn.Write(nsq.MagicV2)
	return conn, nil
}
