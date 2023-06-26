// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
