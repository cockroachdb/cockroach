// Copyright 2018 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

// showBackupPlanHook implements PlanHookFn.
func showBackupPlanHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, sqlbase.ResultColumns, []sql.PlanNode, bool, error) {
	backup, ok := stmt.(*tree.ShowBackup)
	if !ok {
		return nil, nil, nil, false, nil
	}

	if err := utilccl.CheckEnterpriseEnabled(
		p.ExecCfg().Settings, p.ExecCfg().ClusterID(), p.ExecCfg().Organization(), "SHOW BACKUP",
	); err != nil {
		return nil, nil, nil, false, err
	}

	if err := p.RequireAdminRole(ctx, "SHOW BACKUP"); err != nil {
		return nil, nil, nil, false, err
	}

	toFn, err := p.TypeAsString(backup.Path, "SHOW BACKUP")
	if err != nil {
		return nil, nil, nil, false, err
	}

	expected := map[string]sql.KVStringOptValidate{backupOptEncPassphrase: sql.KVStringOptRequireValue}
	optsFn, err := p.TypeAsStringOpts(backup.Options, expected)
	if err != nil {
		return nil, nil, nil, false, err
	}

	var shower backupShower
	switch backup.Details {
	case tree.BackupRangeDetails:
		shower = backupShowerRanges
	case tree.BackupFileDetails:
		shower = backupShowerFiles
	default:
		shower = backupShowerDefault(ctx, p, backup.ShouldIncludeSchemas)
	}

	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		// TODO(dan): Move this span into sql.
		ctx, span := tracing.ChildSpan(ctx, stmt.StatementTag())
		defer tracing.FinishSpan(span)

		str, err := toFn()
		if err != nil {
			return err
		}

		opts, err := optsFn()
		if err != nil {
			return err
		}

		var encryption *roachpb.FileEncryptionOptions
		if passphrase, ok := opts[backupOptEncPassphrase]; ok {
			store, err := p.ExecCfg().DistSQLSrv.ExternalStorageFromURI(ctx, str)
			if err != nil {
				return errors.Wrapf(err, "make storage")
			}
			defer store.Close()
			opts, err := readEncryptionOptions(ctx, store)
			if err != nil {
				return err
			}
			encryptionKey := storageccl.GenerateKey([]byte(passphrase), opts.Salt)
			encryption = &roachpb.FileEncryptionOptions{Key: encryptionKey}
		}

		desc, err := ReadBackupManifestFromURI(
			ctx, str, p.ExecCfg().DistSQLSrv.ExternalStorageFromURI, encryption,
		)
		if err != nil {
			return err
		}
		// If we are restoring a backup with old-style foreign keys, skip over the
		// FKs for which we can't resolve the cross-table references. We can't
		// display them anyway, because we don't have the referenced table names,
		// etc.
		if err := maybeUpgradeTableDescsInBackupManifests(ctx, []BackupManifest{desc}, true /*skipFKsWithNoMatchingTable*/); err != nil {
			return err
		}

		for _, row := range shower.fn(desc) {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case resultsCh <- row:
			}
		}
		return nil
	}

	return fn, shower.header, nil, false, nil
}

type backupShower struct {
	header sqlbase.ResultColumns
	fn     func(BackupManifest) []tree.Datums
}

func backupShowerHeaders(showSchemas bool) sqlbase.ResultColumns {
	baseHeaders := sqlbase.ResultColumns{
		{Name: "database_name", Typ: types.String},
		{Name: "table_name", Typ: types.String},
		{Name: "start_time", Typ: types.Timestamp},
		{Name: "end_time", Typ: types.Timestamp},
		{Name: "size_bytes", Typ: types.Int},
		{Name: "rows", Typ: types.Int},
	}
	if showSchemas {
		baseHeaders = append(baseHeaders, sqlbase.ResultColumn{Name: "create_statement", Typ: types.String})
	}
	return baseHeaders
}

func backupShowerDefault(ctx context.Context, p sql.PlanHookState, showSchemas bool) backupShower {
	return backupShower{
		header: backupShowerHeaders(showSchemas),
		fn: func(desc BackupManifest) []tree.Datums {
			descs := make(map[sqlbase.ID]string)
			for _, descriptor := range desc.Descriptors {
				if database := descriptor.GetDatabase(); database != nil {
					if _, ok := descs[database.ID]; !ok {
						descs[database.ID] = database.Name
					}
				}
			}
			descSizes := make(map[sqlbase.ID]RowCount)
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
				s.add(file.EntryCounts)
				descSizes[sqlbase.ID(tableID)] = s
			}
			start := tree.DNull
			if desc.StartTime.WallTime != 0 {
				start = tree.MakeDTimestamp(timeutil.Unix(0, desc.StartTime.WallTime), time.Nanosecond)
			}
			var rows []tree.Datums
			var row tree.Datums
			for _, descriptor := range desc.Descriptors {
				if table := descriptor.Table(hlc.Timestamp{}); table != nil {
					dbName := descs[table.ParentID]
					row = tree.Datums{
						tree.NewDString(dbName),
						tree.NewDString(table.Name),
						start,
						tree.MakeDTimestamp(timeutil.Unix(0, desc.EndTime.WallTime), time.Nanosecond),
						tree.NewDInt(tree.DInt(descSizes[table.ID].DataSize)),
						tree.NewDInt(tree.DInt(descSizes[table.ID].Rows)),
					}
					if showSchemas {
						schema, err := p.ShowCreate(ctx, dbName, desc.Descriptors, table, sql.OmitMissingFKClausesFromCreate)
						if err != nil {
							continue
						}
						row = append(row, tree.NewDString(schema))
					}
					rows = append(rows, row)
				}
			}
			return rows
		},
	}
}

var backupShowerRanges = backupShower{
	header: sqlbase.ResultColumns{
		{Name: "start_pretty", Typ: types.String},
		{Name: "end_pretty", Typ: types.String},
		{Name: "start_key", Typ: types.Bytes},
		{Name: "end_key", Typ: types.Bytes},
	},

	fn: func(desc BackupManifest) (rows []tree.Datums) {
		for _, span := range desc.Spans {
			rows = append(rows, tree.Datums{
				tree.NewDString(span.Key.String()),
				tree.NewDString(span.EndKey.String()),
				tree.NewDBytes(tree.DBytes(span.Key)),
				tree.NewDBytes(tree.DBytes(span.EndKey)),
			})
		}
		return rows
	},
}

var backupShowerFiles = backupShower{
	header: sqlbase.ResultColumns{
		{Name: "path", Typ: types.String},
		{Name: "start_pretty", Typ: types.String},
		{Name: "end_pretty", Typ: types.String},
		{Name: "start_key", Typ: types.Bytes},
		{Name: "end_key", Typ: types.Bytes},
		{Name: "size_bytes", Typ: types.Int},
		{Name: "rows", Typ: types.Int},
	},

	fn: func(desc BackupManifest) (rows []tree.Datums) {
		for _, file := range desc.Files {
			rows = append(rows, tree.Datums{
				tree.NewDString(file.Path),
				tree.NewDString(file.Span.Key.String()),
				tree.NewDString(file.Span.EndKey.String()),
				tree.NewDBytes(tree.DBytes(file.Span.Key)),
				tree.NewDBytes(tree.DBytes(file.Span.EndKey)),
				tree.NewDInt(tree.DInt(file.EntryCounts.DataSize)),
				tree.NewDInt(tree.DInt(file.EntryCounts.Rows)),
			})
		}
		return rows
	},
}

func init() {
	sql.AddPlanHook(showBackupPlanHook)
}
