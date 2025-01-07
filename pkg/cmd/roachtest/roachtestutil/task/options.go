// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package task

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
)

type (
	// Options is a struct that contains the options that can be passed when
	// starting a task.
	Options struct {
		Name             string
		L                LogSupplierFunc
		PanicHandler     PanicHandlerFunc
		ErrorHandler     ErrorHandlerFunc
		DisableReporting bool
		Context          context.Context
	}

	// LogSupplierFunc is a function that supplies the task with a logger.
	LogSupplierFunc func(name string) (*logger.Logger, error)

	// PanicHandlerFunc is a function that handles panics. If a panic is recovered
	// during the execution of a task, the panic handler is called with the
	// recovered value. The function has the option to either return an error or
	// panic again.
	PanicHandlerFunc func(ctx context.Context, name string, l *logger.Logger, r interface{}) error

	// ErrorHandlerFunc is a function that handles errors. If an error is returned
	// from the execution of a task, or the task's panic handler, the error
	// handler is called with the error. The error can be augmented or transformed
	// by the error handler. The returned error is passed along to the framework
	ErrorHandlerFunc func(ctx context.Context, name string, l *logger.Logger, err error) error
)

type Option func(result *Options)

// Name is an option that sets the name of the task.
func Name(name string) Option {
	return func(result *Options) {
		result.Name = name
	}
}

// LoggerFunc is an option that sets the logger function that will provide the
// task with a logger. Use Logger to provide a logger directly.
func LoggerFunc(loggerFn LogSupplierFunc) Option {
	return func(result *Options) {
		result.L = loggerFn
	}
}

// Logger is an option that sets the logger that will be used by the task.
func Logger(l *logger.Logger) Option {
	return func(result *Options) {
		result.L = func(string) (*logger.Logger, error) {
			return l, nil
		}
	}
}

// PanicHandler is an option that sets the panic handler that will be used by the task.
func PanicHandler(handler PanicHandlerFunc) Option {
	return func(result *Options) {
		result.PanicHandler = handler
	}
}

// ErrorHandler is an option that sets the error handler that will be used by the task.
func ErrorHandler(handler ErrorHandlerFunc) Option {
	return func(result *Options) {
		result.ErrorHandler = handler
	}
}

// DisableReporting is an option that disables reporting errors and panics to the
// test framework.
func DisableReporting() Option {
	return func(result *Options) {
		result.DisableReporting = true
	}
}

// WithContext is an option that sets the context that will be used by the task.
// It will override the context passed to the task manager.
func WithContext(ctx context.Context) Option {
	return func(result *Options) {
		result.Context = ctx
	}
}

func OptionList(opts ...Option) Option {
	return func(result *Options) {
		for _, opt := range opts {
			opt(result)
		}
	}
}

func CombineOptions(opts ...Option) Options {
	result := Options{}
	for _, opt := range opts {
		opt(&result)
	}
	return result
}
