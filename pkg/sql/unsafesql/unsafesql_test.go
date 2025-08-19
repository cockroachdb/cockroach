// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package unsafesql_test

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/unsafesql"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestCheckUnsafeInternalsAccess(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	t.Run("returns the right response with the right session data", func(t *testing.T) {
		for _, test := range []struct {
			Internal             bool
			AllowUnsafeInternals bool
			Passes               bool
		}{
			{Internal: true, AllowUnsafeInternals: true, Passes: true},
			{Internal: true, AllowUnsafeInternals: false, Passes: true},
			{Internal: false, AllowUnsafeInternals: true, Passes: true},
			{Internal: false, AllowUnsafeInternals: false, Passes: false},
		} {
			t.Run(fmt.Sprintf("%t", test), func(t *testing.T) {
				err := unsafesql.CheckInternalsAccess(&sessiondata.SessionData{
					SessionData: sessiondatapb.SessionData{
						Internal: test.Internal,
					},
					LocalOnlySessionData: sessiondatapb.LocalOnlySessionData{
						AllowUnsafeInternals: test.AllowUnsafeInternals,
					},
				})

				if test.Passes {
					require.NoError(t, err)
				} else {
					require.ErrorIs(t, err, sqlerrors.ErrUnsafeTableAccess)
				}
			})
		}
	})
}
