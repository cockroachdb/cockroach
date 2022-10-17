// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package instanceprovider

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance/instancestorage"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// TestInstanceProvider exposes ShutdownSQLInstanceForTest
// and InitAndWaitForTest methods for testing purposes.
type TestInstanceProvider interface {
	sqlinstance.Provider
	InitForTest(context.Context)
}

// NewTestInstanceProvider initializes a instanceprovider.provider
// for test purposes
func NewTestInstanceProvider(
	stopper *stop.Stopper, session sqlliveness.Instance, addr func() string,
) TestInstanceProvider {
	storage := instancestorage.NewFakeStorage()
	p := &provider{
		storage:      storage,
		stopper:      stopper,
		session:      session,
		instanceAddr: addr,
		started:      true,
	}
	return p
}

// InitForTest explicitly calls initAndWait for testing purposes.
func (p *provider) InitForTest(ctx context.Context) {
	_ = p.init(ctx)
}
