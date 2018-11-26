package nsqd

import (
	"math"
)

type Uint64Slice []uint64

func (s Uint64Slice) Len() int {
	return len(s)
}

func (s Uint64Slice) Less(i, j int) bool {
	return (s)[i] < (s)[j]
}

func (s Uint64Slice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func percentile(perc float64, arr []uint64, length int) uint64 {
	if length == 0 {
		return 0
	}
	indexOfPerc := int(math.Floor((perc/100.0)*float64(length) + 0.5))
	if indexOfPerc >= length {
		indexOfPerc = length - 1
	}
	return arr[indexOfPerc]
}
