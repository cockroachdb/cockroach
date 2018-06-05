// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// showBackupPlanHook implements PlanHookFn.
func showBackupPlanHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, sqlbase.ResultColumns, []sql.PlanNode, error) {
	backup, ok := stmt.(*tree.ShowBackup)
	if !ok {
		return nil, nil, nil, nil
	}

	if err := utilccl.CheckEnterpriseEnabled(
		p.ExecCfg().Settings, p.ExecCfg().ClusterID(), p.ExecCfg().Organization(), "SHOW BACKUP",
	); err != nil {
		return nil, nil, nil, err
	}

	if err := p.RequireSuperUser(ctx, "SHOW BACKUP"); err != nil {
		return nil, nil, nil, err
	}

	toFn, err := p.TypeAsString(backup.Path, "SHOW BACKUP")
	if err != nil {
		return nil, nil, nil, err
	}
	header := sqlbase.ResultColumns{
		{Name: "database", Typ: types.String},
		{Name: "table", Typ: types.String},
		{Name: "start_time", Typ: types.Timestamp},
		{Name: "end_time", Typ: types.Timestamp},
		{Name: "size_bytes", Typ: types.Int},
		{Name: "rows", Typ: types.Int},
	}
	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		// TODO(dan): Move this span into sql.
		ctx, span := tracing.ChildSpan(ctx, stmt.StatementTag())
		defer tracing.FinishSpan(span)

		str, err := toFn()
		if err != nil {
			return err
		}
		desc, err := ReadBackupDescriptorFromURI(ctx, str, p.ExecCfg().Settings)
		if err != nil {
			return err
		}
		descs := make(map[sqlbase.ID]string)
		for _, descriptor := range desc.Descriptors {
			if database := descriptor.GetDatabase(); database != nil {
				if _, ok := descs[database.ID]; !ok {
					descs[database.ID] = database.Name
				}
			}
		}
		descSizes := make(map[sqlbase.ID]roachpb.BulkOpSummary)
		for _, file := range desc.Files {
			// TODO(dan): This assumes each file in the backup only contains
			// data from a single table, which is usually but not always
			// correct. It does not account for interleaved tables or if a
			// BACKUP happened to catch a newly created table that hadn't yet
			// been split into its own range.
			_, tableID, err := encoding.DecodeUvarintAscending(file.Span.Key)
			if err != nil {
				continue
			}
			s := descSizes[sqlbase.ID(tableID)]
			s.Add(file.EntryCounts)
			descSizes[sqlbase.ID(tableID)] = s
		}
		start := tree.DNull
		if desc.StartTime.WallTime != 0 {
			start = tree.MakeDTimestamp(timeutil.Unix(0, desc.StartTime.WallTime), time.Nanosecond)
		}
		for _, descriptor := range desc.Descriptors {
			if table := descriptor.GetTable(); table != nil {
				dbName := descs[table.ParentID]
				resultsCh <- tree.Datums{
					tree.NewDString(dbName),
					tree.NewDString(table.Name),
					start,
					tree.MakeDTimestamp(timeutil.Unix(0, desc.EndTime.WallTime), time.Nanosecond),
					tree.NewDInt(tree.DInt(descSizes[table.ID].DataSize)),
					tree.NewDInt(tree.DInt(descSizes[table.ID].Rows)),
				}
			}
		}
		return nil
	}
	return fn, header, nil, nil
}

func init() {
	sql.AddPlanHook(showBackupPlanHook)
}
