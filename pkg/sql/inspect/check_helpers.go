// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package inspect

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

func loadTableDesc(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	tableID descpb.ID,
	tableVersion descpb.DescriptorVersion,
	asOf hlc.Timestamp,
) (catalog.TableDescriptor, error) {
	var tableDesc catalog.TableDescriptor
	if err := execCfg.DistSQLSrv.DB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		if !asOf.IsEmpty() {
			if err := txn.KV().SetFixedTimestamp(ctx, asOf); err != nil {
				return err
			}
		}

		byIDGetter := txn.Descriptors().ByIDWithLeased(txn.KV())
		if !asOf.IsEmpty() {
			byIDGetter = txn.Descriptors().ByIDWithoutLeased(txn.KV())
		}

		var err error
		tableDesc, err = byIDGetter.WithoutNonPublic().Get().Table(ctx, tableID)
		if err != nil {
			return err
		}
		if tableVersion != 0 && tableDesc.GetVersion() != tableVersion {
			return errors.WithHintf(
				errors.Newf(
					"table %s [%d] has had a schema change since the job has started at %s",
					tableDesc.GetName(),
					tableDesc.GetID(),
					tableDesc.GetModificationTime().GoTime().Format(time.RFC3339),
				),
				"use AS OF SYSTEM TIME to avoid schema changes during inspection",
			)
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return tableDesc, nil
}
