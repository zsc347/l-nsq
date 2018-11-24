package quantile

import (
	"strings"
	"sync"
	"time"

	"github.com/l-nsq/internal/perks/quantile"
	"github.com/l-nsq/internal/stringy"
)

type Result struct {
	Count       int                  `json:"count"`
	Percentiles []map[string]float64 `json:"percentiles"`
}

func (r *Result) String() string {
	var s []string
	for _, item := range r.Percentiles {
		s = append(s, stringy.NanoSecondToHuman(item["value"]))
	}
	return strings.Join(s, ", ")
}

// why split to two stream ?
// seems nonsence, since the half stream not really used for query
// just help for merge items
// and quantile stream merge will cost time

type Quantile struct {
	sync.Mutex
	streams        [2]quantile.Stream
	currentIndex   uint8
	lastMoveWindow time.Time
	currentStream  *quantile.Stream

	Percentiles    []float64
	MoveWindowTime time.Duration
}

func New(WindowTime time.Duration, Percentiles []float64) *Quantile {
	q := Quantile{
		currentIndex:   0,
		lastMoveWindow: time.Now(),
		MoveWindowTime: WindowTime / 2,
		Percentiles:    Percentiles,
	}
	for i := 0; i < 2; i++ {
		q.streams[i] = *quantile.NewTargeted(Percentiles...)
	}
	q.currentStream = &q.streams[0]
	return &q
}

func (q *Quantile) Insert(msgStartTime int64) {
	q.Lock()
	defer q.Unlock()

	now := time.Now()
	for q.IsDataStale(now) {
		q.moveWindow()
	}

	q.currentStream.Insert(float64(now.UnixNano() - msgStartTime))
}

func (q *Quantile) Result() *Result {
	if q == nil {
		return &Result{}
	}
	queryHandler := q.QueryHandler()
	result := Result{
		Count:       queryHandler.Count(),
		Percentiles: make([]map[string]float64, len(q.Percentiles)),
	}
	for i, p := range q.Percentiles {
		value := queryHandler.Query(p)
		result.Percentiles[i] = map[string]float64{"quantile": p, "value": value}
	}
	return &result
}

func (q *Quantile) QueryHandler() *quantile.Stream {
	q.Lock()
	defer q.Unlock()
	now := time.Now()
	for q.IsDataStale(now) {
		q.moveWindow()
	}

	merged := quantile.NewTargeted(q.Percentiles...)
	merged.Merge(q.streams[0].Samples())
	merged.Merge(q.streams[1].Samples())
	return merged
}

func (q *Quantile) IsDataStale(now time.Time) bool {
	return now.After(q.lastMoveWindow.Add(q.MoveWindowTime))
}

func (q *Quantile) Merge(them *Quantile) {
	q.Lock()
	defer q.Unlock()
	them.Unlock()
	defer them.Unlock()

	iUs := q.currentIndex
	iThem := them.currentIndex

	q.streams[iUs].Merge(them.streams[iThem].Samples())

	iUs ^= 0x1
	iThem ^= 0x1
	q.streams[iUs].Merge(them.streams[iThem].Samples())

	if q.lastMoveWindow.Before(them.lastMoveWindow) {
		q.lastMoveWindow = them.lastMoveWindow
	}
}

// every time move window will clear current stream
func (q *Quantile) moveWindow() {
	q.currentIndex ^= 0x1
	q.currentStream = &q.streams[q.currentIndex]
	q.lastMoveWindow = q.lastMoveWindow.Add(q.MoveWindowTime)
	q.currentStream.Reset()
}
