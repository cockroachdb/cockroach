// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package descs

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

func TestTwoVersionInvariantViolationError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	err := &twoVersionInvariantViolationError{ids: []lease.IDVersion{
		{
			Name:    "foo",
			ID:      1,
			Version: 2,
		},
		{
			Name:    "bar",
			ID:      2,
			Version: 3,
		},
	}}
	require.EqualError(t, err, "cannot publish new versions for descriptors: "+
		"[{foo 1 2} {bar 2 3}], old versions still in use")
	require.Equal(t, "cannot publish new versions for descriptors: "+
		"[{‹×› 1 2} {‹×› 2 3}], old versions still in use",
		string(redact.Sprint(err).Redact()))
}
