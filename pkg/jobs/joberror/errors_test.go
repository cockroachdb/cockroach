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
		EventHandler: &circuit.EventLogger{Log: func(redact.StringBuilder) {}},
	})
	br.Report(errors.New("test error"))
	require.False(t, IsPermanentBulkJobError(br.Signal().Err()))
}
