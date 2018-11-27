package util

import (
	"sync"
)

type WaitGroupWrapper struct {
	sync.WaitGroup
}

func (w *WaitGroupWrapper) Wrap(cb func()) {
	// What's the purpose for this wrapper ?
	// once wrap has been called, call w.wait must wait for all cb called
	// if add called first, code after wait should run after cb called
	// but what if wait called before wrap called ?
	w.Add(1)
	go func() {
		cb()
		w.Done()
	}()
}
