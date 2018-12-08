package protocol

type FatalClientErr struct {
	ParentErr error
	Code      string
	Desc      string
}

// Error returns the mathine readable form
func (e *FatalClientErr) Error() string {
	return e.Code + " " + e.Desc
}

// Parent returns the parent error
func (e *FatalClientErr) Parent() error {
	return e.ParentErr
}

// NewClientErr creates a ClientErr with the supplied human and machine readable strings
func NewFatalClientErr(parent error, code string, description string) *FatalClientErr {
	return &FatalClientErr{parent, code, description}
}
