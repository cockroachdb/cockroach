// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package joberror

import (
	"fmt"
	"testing"

	circuitbreaker "github.com/cockroachdb/circuitbreaker"
	"github.com/cockroachdb/cockroach/pkg/util/circuit"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

func TestErrBreakerOpenIsRetriable(t *testing.T) {
	br := circuit.NewBreaker(circuit.Options{
		Name: redact.Sprint("Breaker"),
		AsyncProbe: func(_ func(error), done func()) {
			done() // never untrip
		},
		EventHandler: &circuit.EventLogger{Log: func(redact.StringBuilder) {}},
	})
	br.Report(errors.New("test error"))
	utilBreakderErr := br.Signal().Err()
	// NB: This matches the error that dial produces.
	dialErr := errors.Wrapf(circuitbreaker.ErrBreakerOpen, "unable to dial n%d", 9)

	for _, e := range []error{
		utilBreakderErr,
		dialErr,
	} {
		t.Run(fmt.Sprintf("%s", e), func(t *testing.T) {
			require.False(t, IsPermanentBulkJobError(e))
		})
	}
}
