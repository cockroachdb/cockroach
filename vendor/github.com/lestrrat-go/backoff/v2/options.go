package backoff

import (
	"time"

	"github.com/lestrrat-go/option"
)

type identInterval struct{}
type identJitterFactor struct{}
type identMaxInterval struct{}
type identMaxRetries struct{}
type identMinInterval struct{}
type identMultiplier struct{}
type identRNG struct{}

// ControllerOption is an option that may be passed to Policy objects,
// but are ultimately passed down to the Controller objects.
// (Normally you do not have to care about the distinction)
type ControllerOption interface {
	ConstantOption
	ExponentialOption
	CommonOption
	controllerOption()
}

type controllerOption struct {
	Option
}

func (*controllerOption) exponentialOption() {}
func (*controllerOption) controllerOption()  {}
func (*controllerOption) constantOption()    {}

// ConstantOption is an option that is used by the Constant policy.
type ConstantOption interface {
	Option
	constantOption()
}

type constantOption struct {
	Option
}

func (*constantOption) constantOption() {}

// ExponentialOption is an option that is used by the Exponential policy.
type ExponentialOption interface {
	Option
	exponentialOption()
}

type exponentialOption struct {
	Option
}

func (*exponentialOption) exponentialOption() {}

// CommonOption is an option that can be passed to any of the backoff policies.
type CommonOption interface {
	ExponentialOption
	ConstantOption
}

type commonOption struct {
	Option
}

func (*commonOption) constantOption()    {}
func (*commonOption) exponentialOption() {}

// WithMaxRetries specifies the maximum number of attempts that can be made
// by the backoff policies. By default each policy tries up to 10 times.
//
// If you would like to retry forever, specify "0" and pass to the constructor
// of each policy.
//
// This option can be passed to all policy constructors except for NullPolicy
func WithMaxRetries(v int) ControllerOption {
	return &controllerOption{option.New(identMaxRetries{}, v)}
}

// WithInterval specifies the constant interval used in ConstantPolicy and
// ConstantInterval.
// The default value is 1 minute.
func WithInterval(v time.Duration) ConstantOption {
	return &constantOption{option.New(identInterval{}, v)}
}

// WithMaxInterval specifies the maximum duration used in exponential backoff
// The default value is 1 minute.
func WithMaxInterval(v time.Duration) ExponentialOption {
	return &exponentialOption{option.New(identMaxInterval{}, v)}
}

// WithMinInterval specifies the minimum duration used in exponential backoff.
// The default value is 500ms.
func WithMinInterval(v time.Duration) ExponentialOption {
	return &exponentialOption{option.New(identMinInterval{}, v)}
}

// WithMultiplier specifies the factor in which the backoff intervals are
// increased. By default this value is set to 1.5, which means that for
// every iteration a 50% increase in the interval for every iteration
// (up to the value specified by WithMaxInterval). this value must be greater
// than 1.0. If the value is less than equal to 1.0, the default value
// of 1.5 is used.
func WithMultiplier(v float64) ExponentialOption {
	return &exponentialOption{option.New(identMultiplier{}, v)}
}

// WithJitterFactor enables some randomness (jittering) in the computation of
// the backoff intervals. This value must be between 0.0 < v < 1.0. If a
// value outside of this range is specified, the value will be silently
// ignored and jittering is disabled.
//
// This option can be passed to ExponentialPolicy or ConstantPolicy constructor
func WithJitterFactor(v float64) CommonOption {
	return &commonOption{option.New(identJitterFactor{}, v)}
}

// WithRNG specifies the random number generator used for jittering.
// If not provided one will be created, but if you want a truly random
// jittering, make sure to provide one that you explicitly initialized
func WithRNG(v Random) CommonOption {
	return &commonOption{option.New(identRNG{}, v)}
}
