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
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance/instancestorage"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// NewTestInstanceProvider initializes a instanceprovider.provider
// for test purposes
func NewTestInstanceProvider(
	stopper *stop.Stopper, session sqlliveness.Instance, addr string,
) sqlinstance.Provider {
	storage := instancestorage.NewFakeStorage()
	p := &provider{
		storage: storage,
		stopper: stopper,
	}
	p.mu.session = session
	p.mu.instanceAddr = addr
	return p
}
