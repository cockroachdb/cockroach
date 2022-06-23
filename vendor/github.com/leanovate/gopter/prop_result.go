package gopter

type propStatus int

const (
	// PropProof THe property was proved (i.e. it is known to be correct and will be always true)
	PropProof propStatus = iota
	// PropTrue The property was true this time
	PropTrue
	// PropFalse The property was false this time
	PropFalse
	// PropUndecided The property has no clear outcome this time
	PropUndecided
	// PropError The property has generated an error
	PropError
)

func (s propStatus) String() string {
	switch s {
	case PropProof:
		return "PROOF"
	case PropTrue:
		return "TRUE"
	case PropFalse:
		return "FALSE"
	case PropUndecided:
		return "UNDECIDED"
	case PropError:
		return "ERROR"
	}
	return ""
}

// PropResult contains the result of a property
type PropResult struct {
	Status     propStatus
	Error      error
	ErrorStack []byte
	Args       []*PropArg
	Labels     []string
}

// NewPropResult create a PropResult with label
func NewPropResult(success bool, label string) *PropResult {
	if success {
		return &PropResult{
			Status: PropTrue,
			Labels: []string{label},
			Args:   make([]*PropArg, 0),
		}
	}
	return &PropResult{
		Status: PropFalse,
		Labels: []string{label},
		Args:   make([]*PropArg, 0),
	}
}

// Success checks if the result was successful
func (r *PropResult) Success() bool {
	return r.Status == PropTrue || r.Status == PropProof
}

// WithArgs sets argument descriptors to the PropResult for reporting
func (r *PropResult) WithArgs(args []*PropArg) *PropResult {
	r.Args = args
	return r
}

// AddArgs add argument descriptors to the PropResult for reporting
func (r *PropResult) AddArgs(args ...*PropArg) *PropResult {
	r.Args = append(r.Args, args...)
	return r
}

// And combines two PropResult by an and operation.
// The resulting PropResult will be only true if both PropResults are true.
func (r *PropResult) And(other *PropResult) *PropResult {
	switch {
	case r.Status == PropError:
		return r
	case other.Status == PropError:
		return other
	case r.Status == PropFalse:
		return r
	case other.Status == PropFalse:
		return other
	case r.Status == PropUndecided:
		return r
	case other.Status == PropUndecided:
		return other
	case r.Status == PropProof:
		return r.mergeWith(other, other.Status)
	case other.Status == PropProof:
		return r.mergeWith(other, r.Status)
	case r.Status == PropTrue && other.Status == PropTrue:
		return r.mergeWith(other, PropTrue)
	default:
		return r
	}
}

func (r *PropResult) mergeWith(other *PropResult, status propStatus) *PropResult {
	return &PropResult{
		Status: status,
		Args:   append(append(make([]*PropArg, 0, len(r.Args)+len(other.Args)), r.Args...), other.Args...),
		Labels: append(append(make([]string, 0, len(r.Labels)+len(other.Labels)), r.Labels...), other.Labels...),
	}
}
