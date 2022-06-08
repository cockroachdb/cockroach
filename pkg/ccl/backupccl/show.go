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
	"github.com/cockroachdb/cockroach/pkg/cloud"
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
	if p.ExecCfg().ExternalIODirConfig.EnableNonAdminImplicitAndArbitraryOutbound {
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

type backupInfoReader interface {
	showBackup(
		context.Context,
		cloud.ExternalStorage,
		*jobspb.BackupEncryptionOptions,
		[]string,
		chan<- tree.Datums,
	) error
	header() colinfo.ResultColumns
}

type manifestInfoReader struct {
	shower backupShower
}

var _ backupInfoReader = manifestInfoReader{}

func (m manifestInfoReader) header() colinfo.ResultColumns {
	return m.shower.header
}

// showBackup reads backup info from the manifest, populates the manifestInfoReader,
// calls the backupShower to process the manifest info into datums,
// and pipes the information to the user's sql console via the results channel.
func (m manifestInfoReader) showBackup(
	ctx context.Context,
	store cloud.ExternalStorage,
	enc *jobspb.BackupEncryptionOptions,
	incPaths []string,
	resultsCh chan<- tree.Datums,
) error {
	var err error
	manifests := make([]BackupManifest, len(incPaths)+1)
	manifests[0], err = ReadBackupManifestFromStore(ctx, store, enc)

	if err != nil {
		if errors.Is(err, cloud.ErrFileDoesNotExist) {
			latestFileExists, errLatestFile := checkForLatestFileInCollection(ctx, store)

			if errLatestFile == nil && latestFileExists {
				return errors.WithHintf(err, "The specified path is the root of a backup collection. "+
					"Use SHOW BACKUPS IN with this path to list all the backup subdirectories in the"+
					" collection. SHOW BACKUP can be used with any of these subdirectories to inspect a"+
					" backup.")
			}
			return errors.CombineErrors(err, errLatestFile)
		}
		return err
	}

	for i := range incPaths {
		m, err := readBackupManifest(ctx, store, incPaths[i], enc)
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

	datums, err := m.shower.fn(manifests)
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
		backupOptWithDebugIDs:   sql.KVStringOptRequireNoValue,
		backupOptEncDir:         sql.KVStringOptRequireValue,
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

	var infoReader backupInfoReader
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
	infoReader = manifestInfoReader{shower}

	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		// TODO(dan): Move this span into sql.
		ctx, span := tracing.ChildSpan(ctx, stmt.StatementTag())
		defer span.Finish()

		dest, err := toFn()
		if err != nil {
			return err
		}

		var subdir string

		if inColFn != nil {
			subdir = dest
			dest, err = inColFn()
			if err != nil {
				return err
			}
		}

		if err := checkShowBackupURIPrivileges(ctx, p, dest); err != nil {
			return err
		}

		if subdir != "" {
			parsed, err := url.Parse(dest)
			if err != nil {
				return err
			}
			if strings.EqualFold(subdir, "LATEST") {
				subdir, err = readLatestFile(ctx, dest, p.ExecCfg().DistSQLSrv.ExternalStorageFromURI, p.User())
				if err != nil {
					return errors.Wrap(err, "read LATEST path")
				}
			}
			parsed.Path = path.Join(parsed.Path, subdir)
			dest = parsed.String()
		}

		store, err := p.ExecCfg().DistSQLSrv.ExternalStorageFromURI(ctx, dest, p.User())
		if err != nil {
			return errors.Wrapf(err, "make storage")
		}
		defer store.Close()

		// A user that calls SHOW BACKUP <incremental_dir> on an encrypted incremental
		// backup will need to pass their full backup's directory to the
		// encryption_info_dir parameter because the `ENCRYPTION-INFO` file
		// necessary to decode the incremental backup lives in the full backup dir.
		encStore := store
		if encDir, ok := opts[backupOptEncDir]; ok {
			encStore, err = p.ExecCfg().DistSQLSrv.ExternalStorageFromURI(ctx, encDir, p.User())
			if err != nil {
				return errors.Wrap(err, "make storage")
			}
			defer encStore.Close()
		}
		var encryption *jobspb.BackupEncryptionOptions
		showEncErr := `If you are running SHOW BACKUP exclusively on an incremental backup, 
you must pass the 'encryption_info_dir' parameter that points to the directory of your full backup`
		if passphrase, ok := opts[backupOptEncPassphrase]; ok {
			opts, err := readEncryptionOptions(ctx, encStore)
			if errors.Is(err, errEncryptionInfoRead) {
				return errors.WithHint(err, showEncErr)
			}
			if err != nil {
				return err
			}
			encryptionKey := storageccl.GenerateKey([]byte(passphrase), opts.Salt)
			encryption = &jobspb.BackupEncryptionOptions{Mode: jobspb.EncryptionMode_Passphrase,
				Key: encryptionKey}
		} else if kms, ok := opts[backupOptEncKMS]; ok {
			opts, err := readEncryptionOptions(ctx, encStore)
			if errors.Is(err, errEncryptionInfoRead) {
				return errors.WithHint(err, showEncErr)
			}
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

		return infoReader.showBackup(ctx, store, encryption, incPaths, resultsCh)
	}

	return fn, infoReader.header(), nil, false, nil
}

type backupShower struct {
	// header defines the columns of the table printed as output of the show command.
	header colinfo.ResultColumns

	// fn is the specific implementation of the shower that can either be a default, ranges, files,
	// or JSON shower.
	fn func([]BackupManifest) ([]tree.Datums, error)
}

// backupShowerHeaders defines the schema for the table presented to the user.
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

	if _, shouldShowIDs := opts[backupOptWithDebugIDs]; shouldShowIDs {
		baseHeaders = append(
			colinfo.ResultColumns{
				baseHeaders[0],
				{Name: "database_id", Typ: types.Int},
				baseHeaders[1],
				{Name: "parent_schema_id", Typ: types.Int},
				baseHeaders[2],
				{Name: "object_id", Typ: types.Int},
			},
			baseHeaders[3:]...,
		)
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
				tableSizes, err := getTableSizes(manifest.Files)
				if err != nil {
					return nil, err
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

					var dbID descpb.ID
					var parentSchemaID descpb.ID

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
						dbID = desc.GetParentID()
					case catalog.TypeDescriptor:
						descriptorType = "type"
						dbName = dbIDToName[desc.GetParentID()]
						dbID = desc.GetParentID()
						parentSchemaName = schemaIDToName[desc.GetParentSchemaID()]
						parentSchemaID = desc.GetParentSchemaID()
					case catalog.TableDescriptor:
						descriptorType = "table"
						dbName = dbIDToName[desc.GetParentID()]
						dbID = desc.GetParentID()
						parentSchemaName = schemaIDToName[desc.GetParentSchemaID()]
						parentSchemaID = desc.GetParentSchemaID()
						tableSize := tableSizes[desc.GetID()]
						dataSizeDatum = tree.NewDInt(tree.DInt(tableSize.DataSize))
						rowCountDatum = tree.NewDInt(tree.DInt(tableSize.Rows))

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
					if _, shouldShowIDs := opts[backupOptWithDebugIDs]; shouldShowIDs {
						// If showing debug IDs, interleave the IDs with the corresponding object names.
						row = append(
							tree.Datums{
								row[0],
								nullIfZero(dbID),
								row[1],
								nullIfZero(parentSchemaID),
								row[2],
								nullIfZero(desc.GetID()),
							},
							row[3:]...,
						)
					}
					rows = append(rows, row)
				}
				for _, t := range manifest.GetTenants() {
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
					if _, shouldShowIDs := opts[backupOptWithDebugIDs]; shouldShowIDs {
						// If showing debug IDs, interleave the IDs with the corresponding object names.
						row = append(
							tree.Datums{
								row[0],
								tree.DNull, // Database ID
								row[1],
								tree.DNull, // Parent Schema ID
								row[2],
								tree.NewDInt(tree.DInt(t.ID)), // Object ID
							},
							row[3:]...,
						)
					}
					rows = append(rows, row)
				}
			}
			return rows, nil
		},
	}
}

// getTableSizes gathers row and size count for each table in the manifest
func getTableSizes(files []BackupManifest_File) (map[descpb.ID]RowCount, error) {
	tableSizes := make(map[descpb.ID]RowCount)
	if len(files) == 0 {
		return tableSizes, nil
	}
	_, tenantID, err := keys.DecodeTenantPrefix(files[0].Span.Key)
	if err != nil {
		return nil, err
	}
	showCodec := keys.MakeSQLCodec(tenantID)

	for _, file := range files {
		// TODO(dan): This assumes each file in the backup only contains
		// data from a single table, which is usually but not always
		// correct. It does not account for interleaved tables or if a
		// BACKUP happened to catch a newly created table that hadn't yet
		// been split into its own range.

		// TODO(msbutler): after handling the todo above, understand whether
		// we should return an error if a key does not have tableId. The lack
		// of error handling let #77705 sneak by our unit tests.
		_, tableID, err := showCodec.DecodeTablePrefix(file.Span.Key)
		if err != nil {
			continue
		}
		s := tableSizes[descpb.ID(tableID)]
		s.add(file.EntryCounts)
		tableSizes[descpb.ID(tableID)] = s
	}
	return tableSizes, nil
}

func nullIfEmpty(s string) tree.Datum {
	if s == "" {
		return tree.DNull
	}
	return tree.NewDString(s)
}

func nullIfZero(i descpb.ID) tree.Datum {
	if i == 0 {
		return tree.DNull
	}
	return tree.NewDInt(tree.DInt(i))
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
		privStringBuilder.WriteString(strings.ToUpper(string(objectType)) + " ")
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
