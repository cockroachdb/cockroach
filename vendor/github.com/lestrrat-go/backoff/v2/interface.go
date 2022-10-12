package backoff

import (
	"context"
	"time"

	"github.com/lestrrat-go/option"
)

type Option = option.Interface

type Controller interface {
	Done() <-chan struct{}
	Next() <-chan struct{}
}

type IntervalGenerator interface {
	Next() time.Duration
}

// Policy is an interface for the backoff policies that this package
// implements. Users must create a controller object from this
// policy to actually do anything with it
type Policy interface {
	Start(context.Context) Controller
}

type Random interface {
	Float64() float64
}
