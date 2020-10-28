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
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
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
) (sql.PlanHookRowFn, colinfo.ResultColumns, []sql.PlanNode, bool, error) {
	backup, ok := stmt.(*tree.ShowBackup)
	if !ok {
		return nil, nil, nil, false, nil
	}

	if err := p.RequireAdminRole(ctx, "SHOW BACKUP"); err != nil {
		return nil, nil, nil, false, err
	}

	if backup.Path == nil && backup.InCollection != nil {
		return showBackupsInCollectionPlanHook(ctx, backup, p)
	}

	toFn, err := p.TypeAsString(ctx, backup.Path, "SHOW BACKUP")
	if err != nil {
		return nil, nil, nil, false, err
	}

	var inColFn func() (string, error)
	if backup.InCollection != nil {
		inColFn, err = p.TypeAsString(ctx, backup.InCollection, "SHOW BACKUP")
		if err != nil {
			return nil, nil, nil, false, err
		}
	}

	expected := map[string]sql.KVStringOptValidate{
		backupOptEncPassphrase:  sql.KVStringOptRequireValue,
		backupOptEncKMS:         sql.KVStringOptRequireValue,
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
		defer span.Finish()

		str, err := toFn()
		if err != nil {
			return err
		}

		if inColFn != nil {
			collection, err := inColFn()
			if err != nil {
				return err
			}
			parsed, err := url.Parse(collection)
			if err != nil {
				return err
			}
			parsed.Path = path.Join(parsed.Path, str)
			str = parsed.String()
		}

		store, err := p.ExecCfg().DistSQLSrv.ExternalStorageFromURI(ctx, str, p.User())
		if err != nil {
			return errors.Wrapf(err, "make storage")
		}
		defer store.Close()

		var encryption *jobspb.BackupEncryptionOptions
		if passphrase, ok := opts[backupOptEncPassphrase]; ok {
			opts, err := readEncryptionOptions(ctx, store)
			if err != nil {
				return err
			}
			encryptionKey := storageccl.GenerateKey([]byte(passphrase), opts.Salt)
			encryption = &jobspb.BackupEncryptionOptions{Mode: jobspb.EncryptionMode_Passphrase,
				Key: encryptionKey}
		} else if kms, ok := opts[backupOptEncKMS]; ok {
			opts, err := readEncryptionOptions(ctx, store)
			if err != nil {
				return err
			}

			env := &backupKMSEnv{p.ExecCfg().Settings, &p.ExecCfg().ExternalIODirConfig}
			defaultKMSInfo, err := validateKMSURIsAgainstFullBackup([]string{kms},
				newEncryptedDataKeyMapFromProtoMap(opts.EncryptedDataKeyByKMSMasterKeyID), env)
			if err != nil {
				return err
			}
			encryption = &jobspb.BackupEncryptionOptions{
				Mode:    jobspb.EncryptionMode_KMS,
				KMSInfo: defaultKMSInfo}
		}

		incPaths, err := findPriorBackupNames(ctx, store)
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
		if err := maybeUpgradeTableDescsInBackupManifests(ctx, manifests, true); err != nil {
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
	header colinfo.ResultColumns
	fn     func([]BackupManifest) ([]tree.Datums, error)
}

func backupShowerHeaders(showSchemas bool, opts map[string]string) colinfo.ResultColumns {
	baseHeaders := colinfo.ResultColumns{
		{Name: "database_name", Typ: types.String},
		{Name: "parent_schema_name", Typ: types.String},
		{Name: "object_name", Typ: types.String},
		{Name: "object_type", Typ: types.String},
		{Name: "start_time", Typ: types.Timestamp},
		{Name: "end_time", Typ: types.Timestamp},
		{Name: "size_bytes", Typ: types.Int},
		{Name: "rows", Typ: types.Int},
		{Name: "is_full_cluster", Typ: types.Bool},
	}
	if showSchemas {
		baseHeaders = append(baseHeaders, colinfo.ResultColumn{Name: "create_statement", Typ: types.String})
	}
	if _, shouldShowPrivleges := opts[backupOptWithPrivileges]; shouldShowPrivleges {
		baseHeaders = append(baseHeaders, colinfo.ResultColumn{Name: "privileges", Typ: types.String})
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
				// Map database ID to descriptor name.
				dbIDToName := make(map[descpb.ID]string)
				schemaIDToName := make(map[descpb.ID]string)
				schemaIDToName[keys.PublicSchemaID] = sessiondata.PublicSchemaName
				for i := range manifest.Descriptors {
					descriptor := &manifest.Descriptors[i]
					if descriptor.GetDatabase() != nil {
						id := descpb.GetDescriptorID(descriptor)
						if _, ok := dbIDToName[id]; !ok {
							dbIDToName[id] = descpb.GetDescriptorName(descriptor)
						}
					} else if descriptor.GetSchema() != nil {
						id := descpb.GetDescriptorID(descriptor)
						if _, ok := schemaIDToName[id]; !ok {
							schemaIDToName[id] = descpb.GetDescriptorName(descriptor)
						}
					}
				}
				descSizes := make(map[descpb.ID]RowCount)
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
					s := descSizes[descpb.ID(tableID)]
					s.add(file.EntryCounts)
					descSizes[descpb.ID(tableID)] = s
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
				for i := range manifest.Descriptors {
					descriptor := &manifest.Descriptors[i]

					var dbName string
					var parentSchemaName string
					var descriptorType string

					createStmtDatum := tree.DNull
					dataSizeDatum := tree.DNull
					rowCountDatum := tree.DNull

					desc := catalogkv.UnwrapDescriptorRaw(ctx, descriptor)

					descriptorName := desc.GetName()
					switch desc := desc.(type) {
					case catalog.DatabaseDescriptor:
						descriptorType = "database"
					case catalog.SchemaDescriptor:
						descriptorType = "schema"
						dbName = dbIDToName[desc.GetParentID()]
					case catalog.TypeDescriptor:
						descriptorType = "type"
						dbName = dbIDToName[desc.GetParentID()]
						parentSchemaName = schemaIDToName[desc.GetParentSchemaID()]
					case catalog.TableDescriptor:
						descriptorType = "table"
						dbName = dbIDToName[desc.GetParentID()]
						parentSchemaName = schemaIDToName[desc.GetParentSchemaID()]
						descSize := descSizes[desc.GetID()]
						dataSizeDatum = tree.NewDInt(tree.DInt(descSize.DataSize))
						rowCountDatum = tree.NewDInt(tree.DInt(descSize.Rows))

						displayOptions := sql.ShowCreateDisplayOptions{
							FKDisplayMode:  sql.OmitMissingFKClausesFromCreate,
							IgnoreComments: true,
						}
						createStmt, err := p.ShowCreate(ctx, dbName, manifest.Descriptors,
							tabledesc.NewImmutable(*desc.TableDesc()), displayOptions)
						if err != nil {
							// We expect that we might get an error here due to X-DB
							// references, which were possible on 20.2 betas and rcs.
							log.Errorf(ctx, "error while generating create statement: %+v", err)
						}
						createStmtDatum = nullIfEmpty(createStmt)
					default:
						descriptorType = "unknown"
					}

					row = tree.Datums{
						nullIfEmpty(dbName),
						nullIfEmpty(parentSchemaName),
						tree.NewDString(descriptorName),
						tree.NewDString(descriptorType),
						start,
						end,
						dataSizeDatum,
						rowCountDatum,
						tree.MakeDBool(manifest.DescriptorCoverage == tree.AllDescriptors),
					}
					if showSchemas {
						row = append(row, createStmtDatum)
					}
					if _, shouldShowPrivileges := opts[backupOptWithPrivileges]; shouldShowPrivileges {
						row = append(row, tree.NewDString(showPrivileges(descriptor)))
					}
					rows = append(rows, row)
				}
				for _, t := range manifest.Tenants {
					row := tree.Datums{
						tree.DNull, // Database
						tree.DNull, // Schema
						tree.NewDString(roachpb.MakeTenantID(t.ID).String()), // Object Name
						tree.NewDString("TENANT"),                            // Object Type
						start,
						end,
						tree.DNull, // DataSize
						tree.DNull, // RowCount
						tree.DNull, // Descriptor Coverage
					}
					if showSchemas {
						row = append(row, tree.DNull)
					}
					if _, shouldShowPrivileges := opts[backupOptWithPrivileges]; shouldShowPrivileges {
						row = append(row, tree.DNull)
					}
					rows = append(rows, row)
				}
			}
			return rows, nil
		},
	}
}

func nullIfEmpty(s string) tree.Datum {
	if s == "" {
		return tree.DNull
	}
	return tree.NewDString(s)
}

func showPrivileges(descriptor *descpb.Descriptor) string {
	var privStringBuilder strings.Builder

	var privDesc *descpb.PrivilegeDescriptor
	var objectType privilege.ObjectType
	if db := descriptor.GetDatabase(); db != nil {
		privDesc = db.GetPrivileges()
		objectType = privilege.Database
	} else if typ := descriptor.GetType(); typ != nil {
		privDesc = typ.GetPrivileges()
		objectType = privilege.Type
	} else if table := descpb.TableFromDescriptor(descriptor, hlc.Timestamp{}); table != nil {
		privDesc = table.GetPrivileges()
		objectType = privilege.Table
	} else if schema := descriptor.GetSchema(); schema != nil {
		privDesc = schema.GetPrivileges()
		objectType = privilege.Schema
	}
	if privDesc == nil {
		return ""
	}
	for _, userPriv := range privDesc.Show(objectType) {
		privs := userPriv.Privileges
		if len(privs) == 0 {
			continue
		}
		privStringBuilder.WriteString("GRANT ")

		for j, priv := range privs {
			if j != 0 {
				privStringBuilder.WriteString(", ")
			}
			privStringBuilder.WriteString(priv)
		}
		privStringBuilder.WriteString(" ON ")
		privStringBuilder.WriteString(descpb.GetDescriptorName(descriptor))
		privStringBuilder.WriteString(" TO ")
		privStringBuilder.WriteString(userPriv.User.SQLIdentifier())
		privStringBuilder.WriteString("; ")
	}

	return privStringBuilder.String()
}

var backupShowerRanges = backupShower{
	header: colinfo.ResultColumns{
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
	header: colinfo.ResultColumns{
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

// showBackupPlanHook implements PlanHookFn.
func showBackupsInCollectionPlanHook(
	ctx context.Context, backup *tree.ShowBackup, p sql.PlanHookState,
) (sql.PlanHookRowFn, colinfo.ResultColumns, []sql.PlanNode, bool, error) {

	collectionFn, err := p.TypeAsString(ctx, backup.InCollection, "SHOW BACKUPS")
	if err != nil {
		return nil, nil, nil, false, err
	}

	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		ctx, span := tracing.ChildSpan(ctx, backup.StatementTag())
		defer span.Finish()

		collection, err := collectionFn()
		if err != nil {
			return err
		}

		store, err := p.ExecCfg().DistSQLSrv.ExternalStorageFromURI(ctx, collection, p.User())
		if err != nil {
			return errors.Wrapf(err, "connect to external storage")
		}
		defer store.Close()
		res, err := store.ListFiles(ctx, "/*/*/*/"+backupManifestName)
		if err != nil {
			return err
		}
		for _, i := range res {
			resultsCh <- tree.Datums{tree.NewDString(strings.TrimSuffix(i, "/"+backupManifestName))}
		}
		return nil
	}
	return fn, colinfo.ResultColumns{{Name: "path", Typ: types.String}}, nil, false, nil
}

func init() {
	sql.AddPlanHook(showBackupPlanHook)
}
