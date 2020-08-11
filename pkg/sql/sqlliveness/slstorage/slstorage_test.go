// Copyright 2020 The Cockroach Authors.
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
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slstorage"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

type session struct {
	id  sqlliveness.SessionID
	exp hlc.Timestamp
}

// ID implements the Session interface method ID.
func (s *session) ID() sqlliveness.SessionID { return s.id }

// Expiration implements the Session interface method Expiration.
func (s *session) Expiration() hlc.Timestamp { return s.exp }

func TestSQLStorage(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	slstorage.DefaultGCInterval.Override(&s.ClusterSettings().SV, 1*time.Microsecond)
	sqlStorage := s.SQLLivenessStorage().(sqlliveness.Storage)

	sid := []byte{'1'}
	session := &session{id: sid, exp: s.Clock().Now()}
	err := sqlStorage.Insert(ctx, session)
	require.NoError(t, err)

	require.Eventually(
		t,
		func() bool {
			a, err := sqlStorage.IsAlive(ctx, nil, sid)
			return !a && err == nil
		},
		2*time.Second, 10*time.Millisecond,
	)

	found, err := sqlStorage.Update(ctx, session)
	require.NoError(t, err)
	require.False(t, found)
}
