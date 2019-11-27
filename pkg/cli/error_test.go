// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

type logger struct {
	Severity log.Severity
	Err      error
}

func (l *logger) Log(_ context.Context, sev Severity, err error) {
	l.Severity = sev
	l.Err = err
}

func TestErrorReporting(t *testing.T) {
	tests := []struct {
		desc         string
		err          error
		wantSeverity log.Severity
		wantCLICause bool // should the cause be a *cliError?
	}{
		{
			desc:         "plain",
			err:          errors.New("boom"),
			wantSeverity: log.Severity_ERROR,
			wantCLICause: false,
		},
		{
			desc: "single cliError",
			err: &cliError{
				exitCode: 1,
				severity: log.Severity_INFO,
				cause:    errors.New("routine"),
			},
			wantSeverity: log.Severity_INFO,
			wantCLICause: true,
		},
		{
			desc: "double cliError",
			err: &cliError{
				exitCode: 1,
				severity: log.Severity_INFO,
				cause: &cliError{
					exitCode: 1,
					severity: log.Severity_ERROR,
					cause:    errors.New("serious"),
				},
			},
			wantSeverity: log.Severity_INFO, // should only unwrap one layer
			wantCLICause: true,
		},
		{
			desc: "wrapped cliError",
			err: fmt.Errorf("some context: %w", &cliError{
				exitCode: 1,
				severity: log.Severity_INFO,
				cause:    errors.New("routine"),
			}),
			wantSeverity: log.Severity_INFO,
			wantCLICause: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			var got logger
			checked := checkAndMaybeShoutTo(tt.err, got.Log)
			assert.Equal(t, tt.err, checked, "should return error unchanged")
			assert.Equal(t, tt.wantSeverity, got.Severity, "wrong severity log")
			_, gotCLI := got.Err.(*cliError)
			if tt.wantCLICause {
				assert.True(t, gotCLI, "logged cause should be *cliError")
			} else {
				assert.False(t, gotCLI, "logged cause shouldn't be *cliError")
			}
		})
	}
}
