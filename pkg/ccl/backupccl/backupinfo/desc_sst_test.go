// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backupinfo

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestDescSSTError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	iter := &DescIterator{
		backing: bytesIter{
			iterError: errors.New("internal iterator error"),
		},
	}

	iter.Next()

	valid, err := iter.Valid()
	require.False(t, valid)
	require.Error(t, err)
}
