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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/protoreflect"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

func checkShowBackupURIPrivileges(ctx context.Context, p sql.PlanHookState, uri string) error {
	conf, err := cloud.ExternalStorageConfFromURI(uri, p.User())
	if err != nil {
		return err
	}
	if conf.AccessIsWithExplicitAuth() {
		return nil
	}
	hasAdmin, err := p.HasAdminRole(ctx)
	if err != nil {
		return err
	}
	if !hasAdmin {
		return pgerror.Newf(
			pgcode.InsufficientPrivilege,
			"only users with the admin role are allowed to SHOW BACKUP from the specified %s URI",
			conf.Provider.String())
	}
	return nil
}

// showBackupPlanHook implements PlanHookFn.
func showBackupPlanHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, colinfo.ResultColumns, []sql.PlanNode, bool, error) {
	backup, ok := stmt.(*tree.ShowBackup)
	if !ok {
		return nil, nil, nil, false, nil
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
		backupOptAsJSON:         sql.KVStringOptRequireNoValue,
	}
	optsFn, err := p.TypeAsStringOpts(ctx, backup.Options, expected)
	if err != nil {
		return nil, nil, nil, false, err
	}
	opts, err := optsFn()
	if err != nil {
		return nil, nil, nil, false, err
	}

	if _, asJSON := opts[backupOptAsJSON]; asJSON {
		backup.Details = tree.BackupManifestAsJSON
	}

	var shower backupShower
	switch backup.Details {
	case tree.BackupRangeDetails:
		shower = backupShowerRanges
	case tree.BackupFileDetails:
		shower = backupShowerFiles
	case tree.BackupManifestAsJSON:
		shower = jsonShower
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

		if err := checkShowBackupURIPrivileges(ctx, p, str); err != nil {
			return err
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

		incPaths, err := FindPriorBackups(ctx, store, IncludeManifest)
		if err != nil {
			if errors.Is(err, cloud.ErrListingUnsupported) {
				// If we do not support listing, we have to just assume there are none
				// and show the specified base.
				log.Warningf(ctx, "storage sink %T does not support listing, only resolving the base backup", store)
				incPaths = nil
			} else {
				return err
			}
		}

		manifests := make([]BackupManifest, len(incPaths)+1)
		manifests[0], err = ReadBackupManifestFromStore(ctx, store, encryption)
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

		// Ensure that the descriptors in the backup manifests are up to date.
		//
		// This is necessary in particular for upgrading descriptors with old-style
		// foreign keys which are no longer supported.
		// If we are restoring a backup with old-style foreign keys, skip over the
		// FKs for which we can't resolve the cross-table references. We can't
		// display them anyway, because we don't have the referenced table names,
		// etc.
		err = maybeUpgradeDescriptorsInBackupManifests(ctx, manifests, true /* skipFKsWithNoMatchingTable */)
		if err != nil {
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
		{Name: "backup_type", Typ: types.String},
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
		baseHeaders = append(baseHeaders, colinfo.ResultColumn{Name: "owner", Typ: types.String})
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
				schemaIDToName[keys.PublicSchemaID] = catconstants.PublicSchemaName
				for i := range manifest.Descriptors {
					_, db, _, schema := descpb.FromDescriptor(&manifest.Descriptors[i])
					if db != nil {
						if _, ok := dbIDToName[db.ID]; !ok {
							dbIDToName[db.ID] = db.Name
						}
					} else if schema != nil {
						if _, ok := schemaIDToName[schema.ID]; !ok {
							schemaIDToName[schema.ID] = schema.Name
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
				backupType := tree.NewDString("full")
				if manifest.isIncremental() {
					backupType = tree.NewDString("incremental")
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

					desc := catalogkv.NewBuilder(descriptor).BuildExistingMutable()

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
							tabledesc.NewBuilder(desc.TableDesc()).BuildImmutableTable(), displayOptions)
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
						backupType,
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
						owner := desc.GetPrivileges().Owner().SQLIdentifier()
						row = append(row, tree.NewDString(owner))
					}
					rows = append(rows, row)
				}
				for _, t := range manifest.Tenants {
					row := tree.Datums{
						tree.DNull, // Database
						tree.DNull, // Schema
						tree.NewDString(roachpb.MakeTenantID(t.ID).String()), // Object Name
						tree.NewDString("TENANT"),                            // Object Type
						backupType,
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

	b := catalogkv.NewBuilder(descriptor)
	if b == nil {
		return ""
	}
	var objectType privilege.ObjectType
	switch b.DescriptorType() {
	case catalog.Database:
		objectType = privilege.Database
	case catalog.Table:
		objectType = privilege.Table
	case catalog.Type:
		objectType = privilege.Type
	case catalog.Schema:
		objectType = privilege.Schema
	default:
		return ""
	}
	privDesc := b.BuildImmutable().GetPrivileges()
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

var jsonShower = backupShower{
	header: colinfo.ResultColumns{
		{Name: "manifest", Typ: types.Jsonb},
	},

	fn: func(manifests []BackupManifest) ([]tree.Datums, error) {
		rows := make([]tree.Datums, len(manifests))
		for i, manifest := range manifests {
			j, err := protoreflect.MessageToJSON(&manifest, true)
			if err != nil {
				return nil, err
			}
			rows[i] = tree.Datums{tree.NewDJSON(j)}
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

		if err := checkShowBackupURIPrivileges(ctx, p, collection); err != nil {
			return err
		}

		store, err := p.ExecCfg().DistSQLSrv.ExternalStorageFromURI(ctx, collection, p.User())
		if err != nil {
			return errors.Wrapf(err, "connect to external storage")
		}
		defer store.Close()
		res, err := ListFullBackupsInCollection(ctx, store)
		if err != nil {
			return err
		}
		for _, i := range res {
			resultsCh <- tree.Datums{tree.NewDString(i)}
		}
		return nil
	}
	return fn, colinfo.ResultColumns{{Name: "path", Typ: types.String}}, nil, false, nil
}

func init() {
	sql.AddPlanHook(showBackupPlanHook)
}
