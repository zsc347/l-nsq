package util

import (
	"sync"
)

// WaitGroupWrapper define a struct to wrap functions to a waitgroup
// once Wrap is called, it is ensured run to complete
type WaitGroupWrapper struct {
	sync.WaitGroup
}

// Wrap add callback to a wait group
func (w *WaitGroupWrapper) Wrap(cb func()) {
	// once wrap has been called, call w.wait must wait for all cb called
	// if add called first, code after wait should run after cb called
	// but what if wait called before wrap called ?

	// wait only called when nsqd exit
	// so this wrap function ensure once Wrap is called
	// it must run to complete before nsqd exit
	w.Add(1)
	go func() {
		cb()
		w.Done()
	}()
}
