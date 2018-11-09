package protocol

import (
	"errors"
)

var errorBase10 = errors.New("failed to convert to Base10")

func ByteToBase10(b []byte) (n uint64, err error) {
	base := uint64(10)

	for i := 0; i < len(b); i++ {
		var v byte
		d := b[i]
		switch {
		case '0' <= d && d <= '9':
			v = d - '0'
		default:
			return 0, errorBase10
		}
		n *= base
		n += uint64(v)
	}

	return n, err
}
