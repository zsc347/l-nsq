package nsqd

import (
	"testing"
)

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
