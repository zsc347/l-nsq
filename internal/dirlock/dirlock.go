// +build !windows

package dirlock

import (
	"fmt"
	"os"
	"syscall"
)

// DirLock define struct to lock folder
type DirLock struct {
	dir string
	f   *os.File
}

// New create a lock struct on dir
func New(dir string) *DirLock {
	return &DirLock{
		dir: dir,
	}
}

// Lock lock folder
func (l *DirLock) Lock() error {
	f, err := os.Open(l.dir)
	if err != nil {
		return err
	}
	l.f = f
	err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		return fmt.Errorf("cannot flock directory %s - %s", l.dir, err)
	}
	return nil
}

// Unlock unlock folder
func (l *DirLock) Unlock() error {
	defer l.f.Close()
	return syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
}
