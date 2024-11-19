// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

func TestErrorFormatting(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var e error = decommissionPurgatoryError{errors.New("hello")}
	require.Equal(t, "hello", redact.Sprint(e).Redact().StripMarkers())
	e = rangeMergePurgatoryError{errors.New("hello")}
	require.Equal(t, "hello", redact.Sprint(e).Redact().StripMarkers())
}
