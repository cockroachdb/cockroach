// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sessionprotectedts

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptreconcile"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// SessionMetaType is the meta type for protected timestamp records associated with sessions.
const SessionMetaType = "sessions"

// MakeRecord makes a protected timestamp record to protect a timestamp on
// behalf of a session.
func MakeRecord(
	recordID uuid.UUID, sessionID []byte, tsToProtect hlc.Timestamp, target *ptpb.Target,
) *ptpb.Record {
	return &ptpb.Record{
		ID:        recordID.GetBytesMut(),
		Timestamp: tsToProtect,
		Mode:      ptpb.PROTECT_AFTER,
		MetaType:  SessionMetaType,
		Meta:      sessionID,
		Target:    target,
	}
}

// MakeStatusFunc returns a function which determines whether the session
// implied with this value of meta should be removed by the reconciler.
func MakeStatusFunc() ptreconcile.StatusFunc {
	return func(ctx context.Context, txn isql.Txn, meta []byte) (shouldRemove bool, _ error) {
		row, err := txn.QueryRowEx(ctx, "check-for-dead-session", txn.KV(),
			sessiondata.NodeUserSessionDataOverride,
			`SELECT EXISTS (SELECT 1 FROM crdb_internal.cluster_sessions WHERE session_id = $1 AND status IN ('ACTIVE', 'IDLE'))`, string(meta))
		if err != nil {
			return false, err
		}
		if row == nil {
			return false, errors.AssertionFailedf("no row returned when checking for a dead session")
		}
		sessionIsClosed := bool(!tree.MustBeDBool(row[0]))
		return sessionIsClosed, nil
	}
}
