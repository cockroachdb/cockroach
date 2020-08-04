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
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slinstance"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slstorage"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
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

	defer func(old int) {
		slinstance.DefaultMaxRetriesToExtend = old
	}(slinstance.DefaultMaxRetriesToExtend)
	slinstance.DefaultMaxRetriesToExtend = 1

	ctx := context.Background()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	db := s.DB()

	slstorage.DefaultGCInterval.Override(&s.ClusterSettings().SV, 1*time.Microsecond)
	sqlStorage := slstorage.NewStorage(
		ctx, s.Stopper(), s.Clock(), db, s.InternalExecutor().(sqlutil.InternalExecutor), s.ClusterSettings(),
	)

	sid1 := []byte{'1'}
	session := &session{id: sid1, exp: s.Clock().Now()}
	err := sqlStorage.Insert(ctx, session)
	require.NoError(t, err)

	opts := retry.Options{
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     time.Second,
		Multiplier:     2,
	}
	if err := retry.WithMaxAttempts(ctx, opts, 10, func() error {
		a, err := sqlStorage.IsAlive(ctx, nil, sid1)
		require.NoError(t, err)
		if !a {
			return errors.Errorf("Session %s did not expire", sid1)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
