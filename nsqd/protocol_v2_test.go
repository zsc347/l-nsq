package nsqd

import "net"

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
	return nsqd.
}
