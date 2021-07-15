package walker

// Interface represents a visitable node.
type Interface interface {
}

type WalkError struct {
	Reason error
}

var _ error = &WalkError{}

func (e WalkError) Cause() error {
	return e.Reason
}

func (e WalkError) Error() string {
	return e.Reason.Error()
}
