// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package migrations

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sessionprotectedts"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

type migrationsManagerImpl struct {
	evalCtx   *eval.Context
	pts       protectedts.Storage
	txn       isql.Txn
	sessionID clusterunique.ID
}

func GetMigrationsManager(
	ctx context.Context,
	evalCtx *eval.Context,
	pts protectedts.Provider,
	txn isql.Txn,
	sessionID clusterunique.ID,
) eval.MigrationsManager {
	return migrationsManagerImpl{
		evalCtx:   evalCtx,
		pts:       pts.WithTxn(txn),
		txn:       txn,
		sessionID: sessionID,
	}
}

// ProtectTableForSession lays a protected timestamp for the specified table
// and timestamp.
func (m migrationsManagerImpl) ProtectTableForSession(
	ctx context.Context, tableID descpb.ID, timestamp hlc.Timestamp,
) error {
	ptsRecordID := uuid.MakeV4()
	ptsRecord := sessionprotectedts.MakeRecord(
		ptsRecordID,
		[]byte(m.sessionID.String()),
		timestamp,
		ptpb.MakeSchemaObjectsTarget([]descpb.ID{tableID}),
	)
	log.Infof(ctx, "protecting table %d as of timestamp: %v", tableID, timestamp)
	if err := m.pts.Protect(ctx, ptsRecord); err != nil {
		return err
	}
	return nil
}
