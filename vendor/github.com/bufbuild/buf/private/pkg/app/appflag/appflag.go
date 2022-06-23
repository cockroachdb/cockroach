// Copyright 2020-2021 Buf Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package appflag contains functionality to work with flags.
package appflag

import (
	"context"
	"time"

	"github.com/bufbuild/buf/private/pkg/app"
	"github.com/bufbuild/buf/private/pkg/app/applog"
	"github.com/bufbuild/buf/private/pkg/app/appname"
	"github.com/bufbuild/buf/private/pkg/app/appverbose"
	"github.com/spf13/pflag"
)

// Container is a container.
type Container interface {
	app.Container
	appname.Container
	applog.Container
	appverbose.Container
}

// Interceptor intercepts and adapts the request or response of run functions.
type Interceptor func(func(context.Context, Container) error) func(context.Context, Container) error

// Builder builds run functions.
type Builder interface {
	BindRoot(flagSet *pflag.FlagSet)
	NewRunFunc(func(context.Context, Container) error, ...Interceptor) func(context.Context, app.Container) error
}

// NewBuilder returns a new Builder.
func NewBuilder(appName string, options ...BuilderOption) Builder {
	return newBuilder(appName, options...)
}

// BuilderOption is an option for a new Builder
type BuilderOption func(*builder)

// BuilderWithTimeout returns a new BuilderOption that adds a timeout flag and the default timeout.
func BuilderWithTimeout(defaultTimeout time.Duration) BuilderOption {
	return func(builder *builder) {
		builder.defaultTimeout = defaultTimeout
	}
}

// BuilderWithTracing enables zap tracing for the builder.
func BuilderWithTracing() BuilderOption {
	return func(builder *builder) {
		builder.tracing = true
	}
}
