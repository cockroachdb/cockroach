package misc

import (
	"fmt"
	"net/http"
)

// StatusCodeError represents an http response error.
// type httpStatusCode interface { HTTPStatusCode() int } to handle it.
type StatusCodeError struct {
	Code   int
	Status string
}

func (t StatusCodeError) Error() string {
	return fmt.Sprintf("slack server error: %s", t.Status)
}

func (t StatusCodeError) HTTPStatusCode() int {
	return t.Code
}

func (t StatusCodeError) Retryable() bool {
	if t.Code >= 500 || t.Code == http.StatusTooManyRequests {
		return true
	}
	return false
}
