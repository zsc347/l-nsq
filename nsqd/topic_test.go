package nsqd

import "errors"

type errorBackendQueue struct{}

func (d *errorBackendQueue) Put([]byte) error {
	return errors.New("never gonna happen")
}

func (d *errorBackendQueue) ReadChan() chan []byte {
	return nil
}

func (d *errorBackendQueue) Close() error {
	return nil
}

func (d *errorBackendQueue) Delete() error {
	return nil
}

func (d *errorBackendQueue) Depth() int64 {
	return 0
}

func (d *errorBackendQueue) Empty() error {
	return nil
}

type errorRecoverredBackendQueue struct{ errorBackendQueue }

func (d *errorRecoverredBackendQueue) Put([]byte) error {
	return nil
}
