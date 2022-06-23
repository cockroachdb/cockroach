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

package appflag

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/bufbuild/buf/private/pkg/app"
	"github.com/bufbuild/buf/private/pkg/app/applog"
	"github.com/bufbuild/buf/private/pkg/app/appverbose"
	"github.com/bufbuild/buf/private/pkg/observability"
	"github.com/bufbuild/buf/private/pkg/observability/observabilityzap"
	"github.com/pkg/profile"
	"github.com/spf13/pflag"
	"go.opencensus.io/trace"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

type builder struct {
	appName string

	verbose   bool
	debug     bool
	noWarn    bool
	logFormat string

	profile           bool
	profilePath       string
	profileLoops      int
	profileType       string
	profileAllowError bool

	timeout time.Duration

	defaultTimeout time.Duration

	tracing bool
}

func newBuilder(appName string, options ...BuilderOption) *builder {
	builder := &builder{
		appName: appName,
	}
	for _, option := range options {
		option(builder)
	}
	return builder
}

func (b *builder) BindRoot(flagSet *pflag.FlagSet) {
	flagSet.BoolVarP(&b.verbose, "verbose", "v", false, "Turn on verbose mode.")
	flagSet.BoolVar(&b.debug, "debug", false, "Turn on debug logging.")
	flagSet.StringVar(&b.logFormat, "log-format", "color", "The log format [text,color,json].")
	if b.defaultTimeout > 0 {
		flagSet.DurationVar(&b.timeout, "timeout", b.defaultTimeout, `The duration until timing out.`)
	}

	flagSet.BoolVar(&b.profile, "profile", false, "Run profiling.")
	_ = flagSet.MarkHidden("profile")
	flagSet.StringVar(&b.profilePath, "profile-path", "", "The profile base directory path.")
	_ = flagSet.MarkHidden("profile-path")
	flagSet.IntVar(&b.profileLoops, "profile-loops", 1, "The number of loops to run.")
	_ = flagSet.MarkHidden("profile-loops")
	flagSet.StringVar(&b.profileType, "profile-type", "cpu", "The profile type [cpu,mem,block,mutex].")
	_ = flagSet.MarkHidden("profile-type")
	flagSet.BoolVar(&b.profileAllowError, "profile-allow-error", false, "Allow errors for profiled commands.")
	_ = flagSet.MarkHidden("profile-allow-error")

	// We do not officially support this flag, this is for testing, where we need warnings turned off.
	flagSet.BoolVar(&b.noWarn, "no-warn", false, "Turn off warn logging.")
	_ = flagSet.MarkHidden("no-warn")
}

func (b *builder) NewRunFunc(
	f func(context.Context, Container) error,
	interceptors ...Interceptor,
) func(context.Context, app.Container) error {
	interceptor := chainInterceptors(interceptors...)
	return func(ctx context.Context, appContainer app.Container) error {
		if interceptor != nil {
			return b.run(ctx, appContainer, interceptor(f))
		}
		return b.run(ctx, appContainer, f)
	}
}

func (b *builder) run(
	ctx context.Context,
	appContainer app.Container,
	f func(context.Context, Container) error,
) (retErr error) {
	logLevel, err := getLogLevel(b.debug, b.noWarn)
	if err != nil {
		return err
	}
	logger, err := applog.NewLogger(appContainer.Stderr(), logLevel, b.logFormat)
	if err != nil {
		return err
	}
	verbosePrinter := appverbose.NewVerbosePrinter(appContainer.Stderr(), b.appName, b.verbose)
	container, err := newContainer(appContainer, b.appName, logger, verbosePrinter)
	if err != nil {
		return err
	}

	var cancel context.CancelFunc
	if !b.profile && b.timeout != 0 {
		ctx, cancel = context.WithTimeout(ctx, b.timeout)
		defer cancel()
	}

	if b.tracing {
		closer := observability.Start(
			observability.StartWithTraceExportCloser(
				observabilityzap.NewTraceExportCloser(logger),
			),
		)
		defer func() {
			retErr = multierr.Append(retErr, closer.Close())
		}()
		var span *trace.Span
		ctx, span = trace.StartSpan(ctx, "command")
		defer span.End()
	}
	if !b.profile {
		return f(ctx, container)
	}
	return runProfile(
		logger,
		b.profilePath,
		b.profileType,
		b.profileLoops,
		b.profileAllowError,
		func() error {
			return f(ctx, container)
		},
	)
}

// runProfile profiles the function.
func runProfile(
	logger *zap.Logger,
	profilePath string,
	profileType string,
	profileLoops int,
	profileAllowError bool,
	f func() error,
) error {
	var err error
	if profilePath == "" {
		profilePath, err = os.MkdirTemp("", "")
		if err != nil {
			return err
		}
	}
	logger.Debug("profile", zap.String("path", profilePath))
	if profileType == "" {
		profileType = "cpu"
	}
	if profileLoops == 0 {
		profileLoops = 10
	}
	var profileFunc func(*profile.Profile)
	switch profileType {
	case "cpu":
		profileFunc = profile.CPUProfile
	case "mem":
		profileFunc = profile.MemProfile
	case "block":
		profileFunc = profile.BlockProfile
	case "mutex":
		profileFunc = profile.MutexProfile
	default:
		return fmt.Errorf("unknown profile type: %q", profileType)
	}
	stop := profile.Start(
		profile.Quiet,
		profile.ProfilePath(profilePath),
		profileFunc,
	)
	for i := 0; i < profileLoops; i++ {
		if err := f(); err != nil {
			if !profileAllowError {
				return err
			}
		}
	}
	stop.Stop()
	return nil
}

func getLogLevel(debugFlag bool, noWarnFlag bool) (string, error) {
	if debugFlag && noWarnFlag {
		return "", fmt.Errorf("cannot set both --debug and --no-warn")
	}
	if noWarnFlag {
		return "error", nil
	}
	if debugFlag {
		return "debug", nil
	}
	return "info", nil
}

// chainInterceptors consolidates the given interceptors into one.
// The interceptors are applied in the order they are declared.
func chainInterceptors(interceptors ...Interceptor) Interceptor {
	filtered := make([]Interceptor, 0, len(interceptors))
	for _, interceptor := range interceptors {
		if interceptor != nil {
			filtered = append(filtered, interceptor)
		}
	}
	switch n := len(filtered); n {
	case 0:
		return nil
	case 1:
		return filtered[0]
	default:
		first := filtered[0]
		return func(next func(context.Context, Container) error) func(context.Context, Container) error {
			for i := len(filtered) - 1; i > 0; i-- {
				next = filtered[i](next)
			}
			return first(next)
		}
	}
}
