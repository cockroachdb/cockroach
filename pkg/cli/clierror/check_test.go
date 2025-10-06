// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clierror

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/channel"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type logger struct {
	TB       testing.TB
	Severity log.Severity
	Channel  log.Channel
	Err      error
}

func (l *logger) Log(_ context.Context, sev log.Severity, msg string, args ...interface{}) {
	require.Equal(l.TB, 1, len(args), "expected to log one item")
	err, ok := args[0].(error)
	require.True(l.TB, ok, "expected to log an error")
	l.Severity = sev
	l.Channel = channel.SESSIONS
	l.Err = err
}

func TestErrorReporting(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tests := []struct {
		desc         string
		err          error
		wantSeverity log.Severity
		wantCLICause bool // should the cause be an *Error?
	}{
		{
			desc:         "plain",
			err:          errors.New("boom"),
			wantSeverity: severity.ERROR,
			wantCLICause: false,
		},
		{
			desc: "single cliError",
			err: NewErrorWithSeverity(
				errors.New("routine"),
				exit.UnspecifiedError(),
				severity.INFO,
			),
			wantSeverity: severity.INFO,
			wantCLICause: false,
		},
		{
			desc: "double cliError",
			err: NewErrorWithSeverity(
				NewErrorWithSeverity(
					errors.New("serious"),
					exit.UnspecifiedError(),
					severity.ERROR,
				),
				exit.UnspecifiedError(),
				severity.INFO,
			),
			wantSeverity: severity.INFO, // should only unwrap one layer
			wantCLICause: true,
		},
		{
			desc: "wrapped cliError",
			err: fmt.Errorf("some context: %w", NewErrorWithSeverity(
				errors.New("routine"),
				exit.UnspecifiedError(),
				severity.INFO,
			)),
			wantSeverity: severity.INFO,
			wantCLICause: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			got := &logger{TB: t}
			checked := CheckAndMaybeLog(tt.err, got.Log)
			assert.Equal(t, tt.err, checked, "should return error unchanged")
			assert.Equal(t, tt.wantSeverity, got.Severity, "wrong severity log")
			assert.Equal(t, channel.SESSIONS, got.Channel, "wrong channel")
			gotCLI := errors.HasType(got.Err, (*Error)(nil))
			if tt.wantCLICause {
				assert.True(t, gotCLI, "logged cause should be *Error, got %T", got.Err)
			} else {
				assert.False(t, gotCLI, "logged cause shouldn't be *Error, got %T", got.Err)
			}
		})
	}
}
