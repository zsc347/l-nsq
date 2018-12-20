package util

import (
	"testing"

	"github.com/l-nsq/internal/test"
)

func BenchmarkUniqRands5of5(b *testing.B) {
	for i := 0; i < b.N; i++ {
		UniqRands(5, 5)
	}
}

func BenchmarkUniqRands20of20(b *testing.B) {
	for i := 0; i < b.N; i++ {
		UniqRands(20, 10)
	}
}

func BenchmarkUniqRands20of50(b *testing.B) {
	for i := 0; i < b.N; i++ {
		UniqRands(20, 50)
	}
}

func TestUniq(t *testing.T) {
	x := UniqRands(100, 1000)
	m := make(map[int]bool)
	for _, v := range x {
		_, ok := m[v]
		if ok {
			t.Fatal("not unique")
		}
	}
}

func TestUniqRands(t *testing.T) {
	var x []int
	x = UniqRands(3, 10)
	test.Equal(t, 3, len(x))

	x = UniqRands(10, 5)
	test.Equal(t, 5, len(x))

	x = UniqRands(10, 20)
	test.Equal(t, 10, len(x))
}
