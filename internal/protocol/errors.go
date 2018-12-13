package protocol

// ChildErr define interface for error which has a parent
type ChildErr interface {
	Parent() error
}

// ClientErr define client error
type ClientErr struct {
	ParentErr error
	Code      string
	Desc      string
}

// Error returns string from mathine
func (e *ClientErr) Error() string {
	return e.Code + " " + e.Desc
}

// Parent returns parent error of client error
func (e *ClientErr) Parent() error {
	return e.ParentErr
}

// NewClientErr creates a ClientErr with the supplied human and machine readable strings
func NewClientErr(parent error, code string, description string) *ClientErr {
	return &ClientErr{parent, code, description}
}

// FatalClientErr define fatal error
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

// NewFatalClientErr creates a ClientErr with the supplied human and machine readable strings
func NewFatalClientErr(parent error, code string, description string) *FatalClientErr {
	return &FatalClientErr{parent, code, description}
}
