// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package stripe

// BindingResult represents the result of a binding operation
type BindingResult struct {
	Errors []FieldError
}

// FieldError represents a validation or binding error for a specific field
type FieldError struct {
	Field   string
	Message string
}

// HasErrors returns true if there are any binding errors
func (br *BindingResult) HasErrors() bool {
	return len(br.Errors) > 0
}

// AddError adds a field error to the result
func (br *BindingResult) AddError(field, message string) {
	br.Errors = append(br.Errors, FieldError{
		Field:   field,
		Message: message,
	})
}

// ToError converts the binding result to an error if there are errors
func (br *BindingResult) ToError() error {
	if !br.HasErrors() {
		return nil
	}

	// Return the first error for simplicity - could be enhanced to combine all errors
	return &ValidationError{
		Field:   br.Errors[0].Field,
		Message: br.Errors[0].Message,
	}
}

// ValidationError represents a validation error
type ValidationError struct {
	Field   string
	Message string
}

// Error implements the error interface
func (ve *ValidationError) Error() string {
	return ve.Message
}
