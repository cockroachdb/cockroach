// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
)

// CurrentObservabilityDatabase implements the eval.SessionAccessor interface.
func (p *planner) CurrentObservabilityDatabase(ctx context.Context) (string, error) {
	// TODO(knz): put more intelligent logic in here.
	return catconstants.SystemDatabaseName, nil
}

// maybeSelectObservabilityDB, if the caller has requested so, auto-selects
// the Database session field based on the current location of the session db.
func (ie *InternalExecutor) maybeSelectObservabilityDB(
	ctx context.Context,
	opName string,
	txn *kv.Txn,
	sessionDataOverride sessiondata.InternalExecutorOverride,
	sd *sessiondata.SessionData,
) error {
	if !sessionDataOverride.AutoSelectObservabilityDatabase {
		return nil
	}

	// TODO(knz): put more intelligent logic in here.
	sd.Database = catconstants.SystemDatabaseName

	return nil
}
