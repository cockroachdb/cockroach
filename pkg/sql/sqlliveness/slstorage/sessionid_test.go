// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package slstorage_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/enum"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slstorage"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func FuzzSessionIDEncoding(f *testing.F) {
	defer leaktest.AfterTest(f)()
	defer log.Scope(f).Close(f)

	f.Add(string(""))
	f.Add(string(uuid.FastMakeV4().GetBytes()))

	session, err := slstorage.MakeSessionID(enum.One, uuid.FastMakeV4())
	require.NoError(f, err)
	f.Add(string(session))

	f.Fuzz(func(t *testing.T, randomSession string) {
		session := sqlliveness.SessionID(randomSession)
		region, id, err := slstorage.UnsafeDecodeSessionID(session)
		if err == nil {
			if len([]byte(randomSession)) == 16 {
				// A 16 bytes session is always valid, because it is the legacy uuid encoding.
				require.Equal(t, []byte(randomSession), id)
			} else {
				// If the session is a valid encoding, then re-encoding the
				// decoded pieces should produce an identical session.
				require.Len(t, id, 16)
				reEncoded, err := slstorage.MakeSessionID(region, uuid.FromBytesOrNil(id))
				require.NoError(t, err)
				require.Equal(t, session, reEncoded)
			}
		}
	})
}

func TestMakeSessionIDValidation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	_, err := slstorage.MakeSessionID(nil, uuid.MakeV4())
	require.ErrorContains(t, err, "session id requires a non-empty region")
	_, err = slstorage.MakeSessionID([]byte{}, uuid.MakeV4())
	require.ErrorContains(t, err, "session id requires a non-empty region")
	_, err = slstorage.MakeSessionID(make([]byte, 256), uuid.MakeV4())
	require.ErrorContains(t, err, "region is too long")
}

func TestSessionIDEncoding(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	id1 := uuid.MakeV4()

	must := func(session sqlliveness.SessionID, err error) sqlliveness.SessionID {
		require.NoError(t, err)
		return session
	}

	testCases := []struct {
		name    string
		session sqlliveness.SessionID
		region  []byte
		id      uuid.UUID
		err     string
	}{
		{
			name:    "empty_session",
			session: "",
			err:     "session id is too short",
		},
		{
			name:    "legacy_session",
			session: sqlliveness.SessionID(id1.GetBytes()),
			region:  enum.One,
			id:      id1,
		},
		{
			name:    "session_v1",
			session: must(slstorage.MakeSessionID(enum.One, id1)),
			region:  enum.One,
			id:      id1,
		},
		{
			name: "region_len_too_large",
			session: func() sqlliveness.SessionID {
				session := []byte(must(slstorage.MakeSessionID([]byte{128}, id1)))
				session[1] = 3
				return sqlliveness.SessionID(session)
			}(),
			err:    "session id with length 19 is the wrong size to include a region with length 3",
			region: []byte{},
			id:     id1,
		},
		{
			name: "region_len_too_small",
			session: func() sqlliveness.SessionID {
				session := []byte(must(slstorage.MakeSessionID([]byte{128}, id1)))
				session[1] = 0
				return sqlliveness.SessionID(session)
			}(),
			err:    "session id with length 19 is the wrong size to include a region with length 0",
			region: []byte{},
			id:     id1,
		},
		{
			name: "session_id_too_short",
			session: func() sqlliveness.SessionID {
				smallestValidSession := must(slstorage.MakeSessionID([]byte{128}, id1))
				return smallestValidSession[:len(smallestValidSession)-1]
			}(),
			err: "session id is too short",
		},
		{
			name:    "session_v1_large_region",
			session: must(slstorage.MakeSessionID(make([]byte, 255), id1)),
			region:  make([]byte, 255),
			id:      id1,
		},
		{
			name: "invalid_version",
			session: func() sqlliveness.SessionID {
				session := []byte(must(slstorage.MakeSessionID(make([]byte, 255), id1)))
				session[0] = 2
				return sqlliveness.SessionID(session)
			}(),
			err: "invalid session id version: 2",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			region, uuid, err := slstorage.UnsafeDecodeSessionID(tc.session)
			if tc.err != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.err)
			} else {
				require.NoError(t, err)
				require.Equal(t, region, tc.region)
				require.Equal(t, uuid, tc.id.GetBytes())
			}
		})
	}
}
