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
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage/cloudimpl"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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

	toFn, err := p.TypeAsString(ctx, backup.Path, "SHOW BACKUP")
	if err != nil {
		return nil, nil, nil, false, err
	}

	expected := map[string]sql.KVStringOptValidate{
		backupOptEncPassphrase:  sql.KVStringOptRequireValue,
		backupOptWithPrivileges: sql.KVStringOptRequireNoValue,
	}
	optsFn, err := p.TypeAsStringOpts(ctx, backup.Options, expected)
	if err != nil {
		return nil, nil, nil, false, err
	}
	opts, err := optsFn()
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
		shower = backupShowerDefault(ctx, p, backup.ShouldIncludeSchemas, opts)
	}

	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		// TODO(dan): Move this span into sql.
		ctx, span := tracing.ChildSpan(ctx, stmt.StatementTag())
		defer tracing.FinishSpan(span)

		str, err := toFn()
		if err != nil {
			return err
		}

		store, err := p.ExecCfg().DistSQLSrv.ExternalStorageFromURI(ctx, str, p.User())
		if err != nil {
			return errors.Wrapf(err, "make storage")
		}
		defer store.Close()

		var encryption *roachpb.FileEncryptionOptions
		if passphrase, ok := opts[backupOptEncPassphrase]; ok {
			opts, err := readEncryptionOptions(ctx, store)
			if err != nil {
				return err
			}
			encryptionKey := storageccl.GenerateKey([]byte(passphrase), opts.Salt)
			encryption = &roachpb.FileEncryptionOptions{Key: encryptionKey}
		}

		incPaths, err := findPriorBackups(ctx, store)
		if err != nil {
			if errors.Is(err, cloudimpl.ErrListingUnsupported) {
				// If we do not support listing, we have to just assume there are none
				// and show the specified base.
				log.Warningf(ctx, "storage sink %T does not support listing, only resolving the base backup", store)
				incPaths = nil
			} else {
				return err
			}
		}

		manifests := make([]BackupManifest, len(incPaths)+1)
		manifests[0], err = readBackupManifestFromStore(ctx, store, encryption)
		if err != nil {
			return err
		}

		for i := range incPaths {
			m, err := readBackupManifest(ctx, store, incPaths[i], encryption)
			if err != nil {
				return err
			}
			// Blank the stats to prevent memory blowup.
			m.DeprecatedStatistics = nil
			manifests[i+1] = m
		}

		// If we are restoring a backup with old-style foreign keys, skip over the
		// FKs for which we can't resolve the cross-table references. We can't
		// display them anyway, because we don't have the referenced table names,
		// etc.
		if err := maybeUpgradeTableDescsInBackupManifests(
			ctx, manifests, p.ExecCfg().Codec, true, /*skipFKsWithNoMatchingTable*/
		); err != nil {
			return err
		}

		datums, err := shower.fn(manifests)
		if err != nil {
			return err
		}
		for _, row := range datums {
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
	fn     func([]BackupManifest) ([]tree.Datums, error)
}

func backupShowerHeaders(showSchemas bool, opts map[string]string) sqlbase.ResultColumns {
	baseHeaders := sqlbase.ResultColumns{
		{Name: "database_name", Typ: types.String},
		{Name: "table_name", Typ: types.String},
		{Name: "start_time", Typ: types.Timestamp},
		{Name: "end_time", Typ: types.Timestamp},
		{Name: "size_bytes", Typ: types.Int},
		{Name: "rows", Typ: types.Int},
		{Name: "is_full_cluster", Typ: types.Bool},
	}
	if showSchemas {
		baseHeaders = append(baseHeaders, sqlbase.ResultColumn{Name: "create_statement", Typ: types.String})
	}
	if _, shouldShowPrivleges := opts[backupOptWithPrivileges]; shouldShowPrivleges {
		baseHeaders = append(baseHeaders, sqlbase.ResultColumn{Name: "privileges", Typ: types.String})
	}
	return baseHeaders
}

func backupShowerDefault(
	ctx context.Context, p sql.PlanHookState, showSchemas bool, opts map[string]string,
) backupShower {
	return backupShower{
		header: backupShowerHeaders(showSchemas, opts),
		fn: func(manifests []BackupManifest) ([]tree.Datums, error) {
			var rows []tree.Datums
			for _, manifest := range manifests {
				descs := make(map[sqlbase.ID]string)
				for _, descriptor := range manifest.Descriptors {
					if descriptor.GetDatabase() != nil {
						if _, ok := descs[descriptor.GetID()]; !ok {
							descs[descriptor.GetID()] = descriptor.GetName()
						}
					}
				}
				descSizes := make(map[sqlbase.ID]RowCount)
				for _, file := range manifest.Files {
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
				end, err := tree.MakeDTimestamp(timeutil.Unix(0, manifest.EndTime.WallTime), time.Nanosecond)
				if err != nil {
					return nil, err
				}
				if manifest.StartTime.WallTime != 0 {
					start, err = tree.MakeDTimestamp(timeutil.Unix(0, manifest.StartTime.WallTime), time.Nanosecond)
					if err != nil {
						return nil, err
					}
				}
				var row tree.Datums
				for _, descriptor := range manifest.Descriptors {
					if table := descriptor.Table(hlc.Timestamp{}); table != nil {
						dbName := descs[table.ParentID]
						row = tree.Datums{
							tree.NewDString(dbName),
							tree.NewDString(table.Name),
							start,
							end,
							tree.NewDInt(tree.DInt(descSizes[table.ID].DataSize)),
							tree.NewDInt(tree.DInt(descSizes[table.ID].Rows)),
							tree.MakeDBool(manifest.DescriptorCoverage == tree.AllDescriptors),
						}
						if showSchemas {
							displayOptions := sql.ShowCreateDisplayOptions{
								FKDisplayMode:  sql.OmitMissingFKClausesFromCreate,
								IgnoreComments: true,
							}
							schema, err := p.ShowCreate(ctx, dbName, manifest.Descriptors,
								sqlbase.NewImmutableTableDescriptor(*table), displayOptions)
							if err != nil {
								continue
							}
							row = append(row, tree.NewDString(schema))
						}
						if _, shouldShowPrivileges := opts[backupOptWithPrivileges]; shouldShowPrivileges {
							row = append(row, tree.NewDString(showPrivileges(descriptor)))
						}
						rows = append(rows, row)
					}
				}
			}
			return rows, nil
		},
	}
}

func showPrivileges(descriptor sqlbase.Descriptor) string {
	var privStringBuilder strings.Builder
	var privDesc *sqlbase.PrivilegeDescriptor
	if db := descriptor.GetDatabase(); db != nil {
		privDesc = db.GetPrivileges()
	} else if table := descriptor.Table(hlc.Timestamp{}); table != nil {
		privDesc = table.GetPrivileges()
	}
	if privDesc == nil {
		return ""
	}
	for _, userPriv := range privDesc.Show() {
		user := userPriv.User
		privs := userPriv.Privileges
		privStringBuilder.WriteString("GRANT ")
		if len(privs) == 0 {
			continue
		}

		for j, priv := range privs {
			if j != 0 {
				privStringBuilder.WriteString(", ")
			}
			privStringBuilder.WriteString(priv)
		}
		privStringBuilder.WriteString(" ON ")
		privStringBuilder.WriteString(descriptor.GetName())
		privStringBuilder.WriteString(" TO ")
		privStringBuilder.WriteString(user)
		privStringBuilder.WriteString("; ")
	}

	return privStringBuilder.String()
}

var backupShowerRanges = backupShower{
	header: sqlbase.ResultColumns{
		{Name: "start_pretty", Typ: types.String},
		{Name: "end_pretty", Typ: types.String},
		{Name: "start_key", Typ: types.Bytes},
		{Name: "end_key", Typ: types.Bytes},
	},

	fn: func(manifests []BackupManifest) (rows []tree.Datums, err error) {
		for _, manifest := range manifests {
			for _, span := range manifest.Spans {
				rows = append(rows, tree.Datums{
					tree.NewDString(span.Key.String()),
					tree.NewDString(span.EndKey.String()),
					tree.NewDBytes(tree.DBytes(span.Key)),
					tree.NewDBytes(tree.DBytes(span.EndKey)),
				})
			}
		}
		return rows, nil
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

	fn: func(manifests []BackupManifest) (rows []tree.Datums, err error) {
		for _, manifest := range manifests {
			for _, file := range manifest.Files {
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
		}
		return rows, nil
	},
}

func init() {
	sql.AddPlanHook(showBackupPlanHook)
}
