package protocol

import (
	"testing"

	"github.com/l-nsq/internal/test"
)

var result uint64

func TestByteToBase10Valid(t *testing.T) {
	bt := []byte{'3', '1', '4', '1', '5', '9', '2', '6'}
	n, _ := ByteToBase10(bt)
	test.Equal(t, uint64(31415926), n)
}

func BenchmarkByteToBase10Valid(b *testing.B) {
	bt := []byte{'3', '1', '4', '1', '5', '9', '2', '6'}
	var n uint64
	for i := 0; i < b.N; i++ {
		n, _ = ByteToBase10(bt)
	}
	result = n
}

func BenchmarkByteToBase10Invalid(b *testing.B) {
	bt := []byte{'3', '1', '4', '1', '5', '9', '2', '?'}
	var n uint64
	for i := 0; i < b.N; i++ {
		n, _ = ByteToBase10(bt)
	}
	result = n
}
