// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package regionliveness

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// IsMultiRegionSystemDB detects if the database is multi-region
// and region aware queries are safe to use.
func IsMultiRegionSystemDB(ctx context.Context, executor isql.Executor, txn *kv.Txn) (bool, error) {
	_, err := executor.Exec(ctx,
		"check-for-mr-db",
		txn,
		"SELECT 'system.crdb_internal_region'::REGTYPE::OID")
	if err == nil {
		return true, nil
	}
	if pgerror.GetPGCode(err) == pgcode.UndefinedObject {
		return false, nil
	}
	return false, err
}

// IsQueryTimeoutErr determines if a query timeout error was hit, specifically
// when checking for region liveness.
func IsQueryTimeoutErr(err error) bool {
	return pgerror.GetPGCode(err) == pgcode.QueryCanceled ||
		errors.HasType(err, (*timeutil.TimeoutError)(nil))
}
