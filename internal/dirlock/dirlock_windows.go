package dirlock

type DirLock struct {
	dir string
}

// why windows no need to lock folder ?

func New(dir string) *DirLock {
	return &DirLock{
		dir: dir,
	}
}

func (l *DirLock) Lock() error {
	return nil
}

func (l *DirLock) Unlock() error {
	return nil
}
