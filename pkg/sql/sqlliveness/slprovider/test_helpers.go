// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package slprovider

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slinstance"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slstorage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

type testliveness struct {
	*slstorage.FakeStorage
	*slinstance.Instance
}

// TestLiveness interface exposes a test SQL liveness interface for unit tests.
type TestLiveness interface {
	sqlliveness.Liveness
	Start(ctx context.Context)
}

// NewTestSQLLiveness constructs a testliveness which implements the sqlliveness.Liveness interface.
func NewTestSQLLiveness(
	stopper *stop.Stopper, clock *hlc.Clock, settings *cluster.Settings,
) TestLiveness {
	fakeStorage := slstorage.NewFakeStorage()
	instance := slinstance.NewSQLInstance(stopper, clock, fakeStorage, settings, nil)
	return &testliveness{
		FakeStorage: fakeStorage,
		Instance:    instance,
	}
}

// Start starts the underlying slInstance subsystem.
func (tl *testliveness) Start(ctx context.Context) {
	tl.Instance.Start(ctx)
}
