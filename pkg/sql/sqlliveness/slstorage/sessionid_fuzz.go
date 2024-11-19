// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package slstorage

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/enum"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func FuzzSessionIDEncoding(f *testing.F) {
	defer leaktest.AfterTest(f)()
	defer log.Scope(f).Close(f)

	f.Add(string(""))
	f.Add(string(uuid.MakeV4().GetBytes()))

	session, err := MakeSessionID(enum.One, uuid.MakeV4())
	require.NoError(f, err)
	f.Add(string(session))

	f.Fuzz(func(t *testing.T, randomSession string) {
		session := sqlliveness.SessionID(randomSession)
		region, id, err := UnsafeDecodeSessionID(session)
		if err == nil {
			if len([]byte(randomSession)) == 16 {
				// A 16 bytes session is always valid, because it is the legacy uuid encoding.
				require.Equal(t, []byte(randomSession), id)
			} else {
				// If the session is a valid encoding, then re-encoding the
				// decoded pieces should produce an identical session.
				require.Len(t, id, 16)
				reEncoded, err := MakeSessionID(region, uuid.FromBytesOrNil(id))
				require.NoError(t, err)
				require.Equal(t, session, reEncoded)
			}
		}
	})
}
