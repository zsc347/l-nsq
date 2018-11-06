package nsqd

import (
	"encoding/hex"
	"fmt"
	"testing"
	"time"
	"unsafe"
)

func BenchmarkGUIDCopy(b *testing.B) {
	source := make([]byte, 16)
	var dest MessageID
	for i := 0; i < b.N; i++ {
		copy(dest[:], source)
	}
}

func TestCopy(t *testing.T) {
	var a [3]int
	c := []int{1, 2, 3}
	copy(a[:], c)

	var h = make([]byte, 16)
	var g = int64(-1)
	var b [8]byte

	b[0] = byte(g >> 56)
	b[1] = byte(g >> 48)
	b[2] = byte(g >> 40)
	b[3] = byte(g >> 32)
	b[4] = byte(g >> 24)
	b[5] = byte(g >> 16)
	b[6] = byte(g >> 8)
	b[7] = byte(g)

	hex.Encode(h, b[:])
	fmt.Printf("binary: %+v\n", string(h))
}

func TestTime(t *testing.T) {
	v, err := time.Parse("2006-01-02T15:05:05Z", "2018-11-06T14:48:38Z")
	if err != nil {
		panic(err)
	}

	fmt.Println(v)

	fmt.Println(v.UnixNano())
	fmt.Println(v.UnixNano() >> 20)

	g := v.UnixNano()

	var h = make([]byte, 16)
	var b [8]byte
	b[0] = byte(g >> 56)
	b[1] = byte(g >> 48)
	b[2] = byte(g >> 40)
	b[3] = byte(g >> 32)
	b[4] = byte(g >> 24)
	b[5] = byte(g >> 16)
	b[6] = byte(g >> 8)
	b[7] = byte(g)

	hex.Encode(h, b[:])
	fmt.Printf("binary: [%+v] \n", string(h))
}

func BenchmarkGUIDUnsafe(b *testing.B) {
	source := make([]byte, 16)
	var dest MessageID
	for i := 0; i < b.N; i++ {
		dest = *(*MessageID)(unsafe.Pointer(&source[0]))
	}
	_ = dest
}

func BenchmarkGUID(b *testing.B) {
	var okays, errors, fails int64
	var previd guid
	factory := &guidFactory{}

	for i := 0; i < b.N; i++ {
		id, err := factory.NewGUID()
		if err != nil {
			errors++
		} else if id == previd {
			fails++
			b.Fail()
		} else {
			okays++
		}
		id.Hex()
	}
	b.Logf("okays=%d errors=%d bads=%d", okays, errors, fails)
}
