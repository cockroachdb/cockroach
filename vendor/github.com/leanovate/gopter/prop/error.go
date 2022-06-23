package prop

import "github.com/leanovate/gopter"

// ErrorProp creates a property that will always fail with an error.
// Mostly used as a fallback when setup/initialization fails
func ErrorProp(err error) gopter.Prop {
	return func(genParams *gopter.GenParameters) *gopter.PropResult {
		return &gopter.PropResult{
			Status: gopter.PropError,
			Error:  err,
		}
	}
}
