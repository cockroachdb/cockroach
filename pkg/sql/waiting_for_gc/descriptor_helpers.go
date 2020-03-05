// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package waiting_for_gc

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

func getTableDescriptor(
	ctx context.Context, execCfg *sql.ExecutorConfig, ID sqlbase.ID,
) (*sqlbase.TableDescriptor, error) {
	var table *sqlbase.TableDescriptor
	if err := execCfg.DB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		var err error
		table, err = sqlbase.GetTableDescFromID(ctx, txn, ID)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return table, nil
}

func updateDescriptorMutations(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	table *sqlbase.TableDescriptor,
	garbageCollectedIndexIDs map[sqlbase.IndexID]struct{},
) error {
	// Remove the mutation from the table descriptor.
	updateTableMutations := func(tbl *sqlbase.MutableTableDescriptor) error {
		for i := 0; i < len(tbl.GCMutations); i++ {
			other := tbl.GCMutations[i]
			_, ok := garbageCollectedIndexIDs[other.IndexID]
			if ok {
				tbl.GCMutations = append(tbl.GCMutations[:i], tbl.GCMutations[i+1:]...)
				break
			}
		}

		return nil
	}

	_, err := execCfg.LeaseManager.Publish(
		ctx,
		table.ID,
		updateTableMutations,
		nil, /* logEvent */
	)
	if err != nil {
		return err
	}
	return nil
}
