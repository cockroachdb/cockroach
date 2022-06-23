package errorsx

// String representing an error, useful for declaring string constants as errors.
type String string

func (t String) Error() string {
	return string(t)
}

// Is reports whether String matches with the target error
func (t String) Is(target error) bool {
	if target == nil {
		return false
	}

	return t.Error() == target.Error()
}
