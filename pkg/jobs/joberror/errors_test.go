// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package joberror

import (
	"testing"

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
	})
	br.Report(errors.New("test error"))
	require.False(t, IsPermanentBulkJobError(br.Signal().Err()))
}
