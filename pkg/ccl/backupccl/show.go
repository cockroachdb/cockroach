// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backupccl

import (
	"context"
	"fmt"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backupbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backupdest"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backupencryption"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backupinfo"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backuppb"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backuputils"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudcheck"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudprivilege"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/doctor"
	"github.com/cockroachdb/cockroach/pkg/sql/exprutil"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/protoreflect"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

type backupInfoReader interface {
	showBackup(
		context.Context,
		*mon.BoundAccount,
		cloud.ExternalStorageFromURIFactory,
		backupInfo,
		username.SQLUsername,
		cloud.KMSEnv,
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
	mem *mon.BoundAccount,
	_ cloud.ExternalStorageFromURIFactory,
	info backupInfo,
	_ username.SQLUsername,
	kmsEnv cloud.KMSEnv,
	resultsCh chan<- tree.Datums,
) error {
	ctx, sp := tracing.ChildSpan(ctx, "backupccl.showBackup")
	defer sp.Finish()

	var memReserved int64

	defer func() {
		mem.Shrink(ctx, memReserved)
	}()
	// Ensure that the descriptors in the backup manifests are up to date.
	//
	// This is necessary in particular for upgrading descriptors with old-style
	// foreign keys which are no longer supported.
	// If we are restoring a backup with old-style foreign keys, skip over the
	// FKs for which we can't resolve the cross-table references. We can't
	// display them anyway, because we don't have the referenced table names,
	// etc.
	err := maybeUpgradeDescriptorsInBackupManifests(ctx,
		kmsEnv.ClusterSettings().Version.ActiveVersion(ctx),
		info.manifests,
		info.layerToIterFactory,
		true /* skipFKsWithNoMatchingTable */)
	if err != nil {
		return err
	}

	datums, err := m.shower.fn(ctx, info)
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

type metadataSSTInfoReader struct{}

var _ backupInfoReader = manifestInfoReader{}

func (m metadataSSTInfoReader) header() colinfo.ResultColumns {
	return colinfo.ResultColumns{
		{Name: "file", Typ: types.String},
		{Name: "key", Typ: types.String},
		{Name: "detail", Typ: types.Jsonb},
	}
}

func (m metadataSSTInfoReader) showBackup(
	ctx context.Context,
	_ *mon.BoundAccount,
	mkStore cloud.ExternalStorageFromURIFactory,
	info backupInfo,
	user username.SQLUsername,
	kmsEnv cloud.KMSEnv,
	resultsCh chan<- tree.Datums,
) error {
	filename := backupinfo.MetadataSSTName
	push := func(_, readable string, value json.JSON) error {
		val := tree.DNull
		if value != nil {
			val = tree.NewDJSON(value)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case resultsCh <- []tree.Datum{tree.NewDString(filename), tree.NewDString(readable), val}:
			return nil
		}
	}
	for _, uri := range info.defaultURIs {
		store, err := mkStore(ctx, uri, user)
		if err != nil {
			return errors.Wrapf(err, "creating external store")
		}
		defer store.Close()
		if err := backupinfo.DebugDumpMetadataSST(ctx, store, filename, info.enc,
			kmsEnv, push); err != nil {
			return err
		}
	}
	return nil
}

func showBackupTypeCheck(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (matched bool, header colinfo.ResultColumns, _ error) {
	backup, ok := stmt.(*tree.ShowBackup)
	if !ok {
		return false, nil, nil
	}
	if backup.Path == nil && backup.InCollection != nil {
		return showBackupsInCollectionTypeCheck(ctx, backup, p)
	}
	if err := exprutil.TypeCheck(
		ctx, "SHOW BACKUP", p.SemaCtx(),
		exprutil.Ints{
			backup.Options.CheckConnectionConcurrency,
		},
		exprutil.Strings{
			backup.Path,
			backup.Options.EncryptionPassphrase,
			backup.Options.EncryptionInfoDir,
			backup.Options.CheckConnectionTransferSize,
			backup.Options.CheckConnectionDuration,
		},
		exprutil.StringArrays{
			tree.Exprs(backup.InCollection),
			tree.Exprs(backup.Options.IncrementalStorage),
			tree.Exprs(backup.Options.DecryptionKMSURI),
		},
	); err != nil {
		return false, nil, err
	}
	if backup.Details == tree.BackupConnectionTest {
		return true, cloudcheck.Header, nil
	}
	infoReader := getBackupInfoReader(p, backup)
	return true, infoReader.header(), nil
}

// showBackupPlanHook implements PlanHookFn.
func showBackupPlanHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, colinfo.ResultColumns, []sql.PlanNode, bool, error) {
	showStmt, ok := stmt.(*tree.ShowBackup)
	if !ok {
		return nil, nil, nil, false, nil
	}
	exprEval := p.ExprEvaluator("SHOW BACKUP")

	// TODO(dt): find move this to its own hook.
	if showStmt.Details == tree.BackupConnectionTest {
		loc, err := exprEval.String(ctx, showStmt.Path)
		if err != nil {
			return nil, nil, nil, false, err
		}
		var params cloudcheck.Params
		if showStmt.Options.CheckConnectionTransferSize != nil {
			transferSizeStr, err := exprEval.String(ctx, showStmt.Options.CheckConnectionTransferSize)
			if err != nil {
				return nil, nil, nil, false, err
			}
			parsed, err := humanizeutil.ParseBytes(transferSizeStr)
			if err != nil {
				return nil, nil, nil, false, err
			}
			params.TransferSize = parsed
		}
		if showStmt.Options.CheckConnectionDuration != nil {
			durationStr, err := exprEval.String(ctx, showStmt.Options.CheckConnectionDuration)
			if err != nil {
				return nil, nil, nil, false, err
			}
			parsed, err := time.ParseDuration(durationStr)
			if err != nil {
				return nil, nil, nil, false, err
			}
			params.MinDuration = parsed
		}
		if showStmt.Options.CheckConnectionConcurrency != nil {
			concurrency, err := exprEval.Int(ctx, showStmt.Options.CheckConnectionConcurrency)
			if err != nil {
				return nil, nil, nil, false, err
			}
			params.Concurrency = concurrency
		}
		return cloudcheck.ShowCloudStorageTestPlanHook(ctx, p, loc, params)
	}

	if showStmt.Path == nil && showStmt.InCollection != nil {
		collection, err := exprEval.StringArray(
			ctx, tree.Exprs(showStmt.InCollection),
		)
		if err != nil {
			return nil, nil, nil, false, err
		}
		return showBackupsInCollectionPlanHook(ctx, collection, showStmt, p)
	}

	to, err := exprEval.String(ctx, showStmt.Path)
	if err != nil {
		return nil, nil, nil, false, err
	}

	var inCol []string
	if showStmt.InCollection != nil {
		inCol, err = exprEval.StringArray(ctx, tree.Exprs(showStmt.InCollection))
		if err != nil {
			return nil, nil, nil, false, err
		}
	}

	infoReader := getBackupInfoReader(p, showStmt)

	if err != nil {
		return nil, nil, nil, false, err
	}

	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		ctx, span := tracing.ChildSpan(ctx, stmt.StatementTag())
		defer span.Finish()

		var (
			dest   []string
			subdir string
		)
		// For old style show backup, 'to' is the resolved path to the full backup;
		// for new SHOW BACKUP, 'to' is the subdirectory.
		if inCol != nil {
			subdir = to
			dest = inCol
		} else {
			dest = append(dest, to)
			// Deprecation notice for old `SHOW BACKUP` syntax. Remove this once the syntax is
			// deleted in 22.2.
			p.BufferClientNotice(ctx,
				pgnotice.Newf("The `SHOW BACKUP` syntax without the `IN` keyword will be removed in a"+
					" future release. Please switch over to using `SHOW BACKUP FROM <backup> IN"+
					" <collection>` to view metadata on a backup collection: %s."+
					" Also note that backups created using the `BACKUP TO` syntax may not be showable or"+
					" restoreable in the next major version release. Use `BACKUP INTO` instead.",
					"https://www.cockroachlabs.com/docs/stable/show-backup.html"))
		}

		if err := cloudprivilege.CheckDestinationPrivileges(ctx, p, dest); err != nil {
			return err
		}

		fullyResolvedDest := dest
		if subdir != "" {
			if strings.EqualFold(subdir, backupbase.LatestFileName) {
				subdir, err = backupdest.ReadLatestFile(ctx, dest[0],
					p.ExecCfg().DistSQLSrv.ExternalStorageFromURI,
					p.User())
				if err != nil {
					return errors.Wrap(err, "read LATEST path")
				}
			}
			fullyResolvedDest, err = backuputils.AppendPaths(dest, subdir)
			if err != nil {
				return err
			}
		}
		baseStores := make([]cloud.ExternalStorage, len(fullyResolvedDest))
		for j := range fullyResolvedDest {
			baseStores[j], err = p.ExecCfg().DistSQLSrv.ExternalStorageFromURI(ctx, fullyResolvedDest[j], p.User())
			if err != nil {
				return errors.Wrapf(err, "make storage")
			}
			defer baseStores[j].Close()
		}

		// TODO(msbutler): put encryption resolution in helper function, hopefully shared with RESTORE

		encStore := baseStores[0]
		if showStmt.Options.EncryptionInfoDir != nil {
			encDir, err := exprEval.String(ctx, showStmt.Options.EncryptionInfoDir)
			if err != nil {
				return err
			}
			encStore, err = p.ExecCfg().DistSQLSrv.ExternalStorageFromURI(ctx, encDir, p.User())
			if err != nil {
				return errors.Wrap(err, "make storage")
			}
			defer encStore.Close()
		}
		var encryption *jobspb.BackupEncryptionOptions
		kmsEnv := backupencryption.MakeBackupKMSEnv(
			p.ExecCfg().Settings,
			&p.ExecCfg().ExternalIODirConfig,
			p.ExecCfg().InternalDB,
			p.User(),
		)
		showEncErr := `If you are running SHOW BACKUP exclusively on an incremental backup,
you must pass the 'encryption_info_dir' parameter that points to the directory of your full backup`
		if showStmt.Options.EncryptionPassphrase != nil {
			passphrase, err := exprEval.String(ctx, showStmt.Options.EncryptionPassphrase)
			if err != nil {
				return err
			}
			opts, err := backupencryption.ReadEncryptionOptions(ctx, encStore)
			if errors.Is(err, backupencryption.ErrEncryptionInfoRead) {
				return errors.WithHint(err, showEncErr)
			}
			if err != nil {
				return err
			}
			encryptionKey := storageccl.GenerateKey([]byte(passphrase), opts[0].Salt)
			encryption = &jobspb.BackupEncryptionOptions{
				Mode: jobspb.EncryptionMode_Passphrase,
				Key:  encryptionKey,
			}
		} else if showStmt.Options.DecryptionKMSURI != nil {
			kms, err := exprEval.StringArray(ctx, tree.Exprs(showStmt.Options.DecryptionKMSURI))
			if err != nil {
				return err
			}
			opts, err := backupencryption.ReadEncryptionOptions(ctx, encStore)
			if errors.Is(err, backupencryption.ErrEncryptionInfoRead) {
				return errors.WithHint(err, showEncErr)
			}
			if err != nil {
				return err
			}
			var defaultKMSInfo *jobspb.BackupEncryptionOptions_KMSInfo
			for _, encFile := range opts {
				defaultKMSInfo, err = backupencryption.ValidateKMSURIsAgainstFullBackup(
					ctx,
					kms,
					backupencryption.NewEncryptedDataKeyMapFromProtoMap(encFile.EncryptedDataKeyByKMSMasterKeyID),
					&kmsEnv,
				)
				if err == nil {
					break
				}
			}
			if err != nil {
				return err
			}
			encryption = &jobspb.BackupEncryptionOptions{
				Mode:    jobspb.EncryptionMode_KMS,
				KMSInfo: defaultKMSInfo,
			}
		}
		var explicitIncPaths []string
		if showStmt.Options.IncrementalStorage != nil {
			explicitIncPaths, err = exprEval.StringArray(ctx, tree.Exprs(showStmt.Options.IncrementalStorage))
			if err != nil {
				return err
			}
		}
		collections, computedSubdir, err := backupdest.CollectionsAndSubdir(dest, subdir)
		if err != nil {
			return err
		}
		fullyResolvedIncrementalsDirectory, err := backupdest.ResolveIncrementalsBackupLocation(
			ctx,
			p.User(),
			p.ExecCfg(),
			explicitIncPaths,
			collections,
			computedSubdir,
		)
		if err != nil {
			if errors.Is(err, cloud.ErrListingUnsupported) {
				// We can proceed with base backups here just fine, so log a warning and move on.
				// Note that actually _writing_ an incremental backup to this location would fail loudly.
				log.Warningf(
					ctx, "storage sink %v does not support listing, only showing the base backup", explicitIncPaths)
			} else {
				return err
			}
		}
		mem := p.ExecCfg().RootMemoryMonitor.MakeBoundAccount()
		defer mem.Close(ctx)

		var (
			info        backupInfo
			memReserved int64
		)
		info.collectionURI = dest[0]
		info.subdir = computedSubdir
		info.kmsEnv = &kmsEnv
		info.enc = encryption

		mkStore := p.ExecCfg().DistSQLSrv.ExternalStorageFromURI
		incStores, cleanupFn, err := backupdest.MakeBackupDestinationStores(ctx, p.User(), mkStore,
			fullyResolvedIncrementalsDirectory)
		if err != nil {
			return err
		}
		defer func() {
			if err := cleanupFn(); err != nil {
				log.Warningf(ctx, "failed to close incremental store: %+v", err)
			}
		}()

		info.defaultURIs, info.manifests, info.localityInfo, memReserved,
			err = backupdest.ResolveBackupManifests(
			ctx, &mem, baseStores, incStores, mkStore, fullyResolvedDest,
			fullyResolvedIncrementalsDirectory, hlc.Timestamp{}, encryption, &kmsEnv, p.User())
		defer func() {
			mem.Shrink(ctx, memReserved)
		}()
		if err != nil {
			if errors.Is(err, backupinfo.ErrLocalityDescriptor) && subdir == "" {
				p.BufferClientNotice(ctx,
					pgnotice.Newf("`SHOW BACKUP` using the old syntax ("+
						"without the `IN` keyword) on a locality aware backup does not display or validate"+
						" data specific to locality aware backups. "+
						"Consider using the new `BACKUP INTO` syntax and `SHOW BACKUP"+
						" FROM <backup> IN <collection>`"))
			} else if errors.Is(err, cloud.ErrFileDoesNotExist) {
				latestFileExists, errLatestFile := backupdest.CheckForLatestFileInCollection(ctx, baseStores[0])

				if errLatestFile == nil && latestFileExists {
					return errors.WithHintf(err, "The specified path is the root of a backup collection. "+
						"Use SHOW BACKUPS IN with this path to list all the backup subdirectories in the"+
						" collection. SHOW BACKUP can be used with any of these subdirectories to inspect a"+
						" backup.")
				}
				return errors.CombineErrors(err, errLatestFile)
			} else {
				return err
			}
		}

		info.layerToIterFactory, err = backupinfo.GetBackupManifestIterFactories(ctx, p.ExecCfg().DistSQLSrv.ExternalStorage, info.manifests, info.enc, info.kmsEnv)
		if err != nil {
			return err
		}

		// If backup is locality aware, check that user passed at least some localities.

		// TODO (msbutler): this is an extremely crude check that the user is
		// passing at least as many URIS as there are localities in the backup. This
		// check is only meant for the 22.1 backport. Ben is working on a much more
		// robust check.
		for _, locMap := range info.localityInfo {
			if len(locMap.URIsByOriginalLocalityKV) > len(dest) && subdir != "" {
				p.BufferClientNotice(ctx,
					pgnotice.Newf("The backup contains %d localities; however, "+
						"the SHOW BACKUP command contains only %d URIs. To capture all locality aware data, "+
						"pass every locality aware URI from the backup", len(locMap.URIsByOriginalLocalityKV),
						len(dest)))
			}
		}
		if showStmt.Options.CheckFiles {
			fileSizes, err := checkBackupFiles(ctx, info, p.ExecCfg(), p.User(), encryption, &kmsEnv)
			if err != nil {
				return err
			}
			info.fileSizes = fileSizes
		}
		if err := infoReader.showBackup(ctx, &mem, mkStore, info, p.User(), &kmsEnv, resultsCh); err != nil {
			return err
		}
		if showStmt.InCollection == nil {
			telemetry.Count("show-backup.deprecated-subdir-syntax")
		} else {
			telemetry.Count("show-backup.collection")
		}
		return nil
	}

	return fn, infoReader.header(), nil, false, nil
}

func getBackupInfoReader(p sql.PlanHookState, showStmt *tree.ShowBackup) backupInfoReader {
	var infoReader backupInfoReader
	if showStmt.Options.DebugMetadataSST {
		infoReader = metadataSSTInfoReader{}
	} else if showStmt.Options.AsJson {
		infoReader = manifestInfoReader{shower: jsonShower}
	} else {
		var shower backupShower
		switch showStmt.Details {
		case tree.BackupRangeDetails:
			shower = backupShowerRanges
		case tree.BackupFileDetails:
			shower = backupShowerFileSetup(p, showStmt.InCollection)
		case tree.BackupSchemaDetails:
			shower = backupShowerDefault(p, true, showStmt.Options)
		case tree.BackupValidateDetails:
			shower = backupShowerDoctor

		default:
			shower = backupShowerDefault(p, false, showStmt.Options)
		}
		infoReader = manifestInfoReader{shower: shower}
	}
	return infoReader
}

// checkBackupFiles validates that each SST is in its expected storage location
func checkBackupFiles(
	ctx context.Context,
	info backupInfo,
	execCfg *sql.ExecutorConfig,
	user username.SQLUsername,
	encryption *jobspb.BackupEncryptionOptions,
	kmsEnv cloud.KMSEnv,
) ([][]int64, error) {
	const maxMissingFiles = 10
	missingFiles := make(map[string]struct{}, maxMissingFiles)

	checkLayer := func(layer int) ([]int64, error) {
		// TODO (msbutler): Right now, checkLayer opens stores for each backup layer. In 22.2,
		// once a backup chain cannot have mixed localities, only create stores for full backup
		// and first incremental backup.
		defaultStore, err := execCfg.DistSQLSrv.ExternalStorageFromURI(ctx, info.defaultURIs[layer], user)
		if err != nil {
			return nil, err
		}
		localityStores := make(map[string]cloud.ExternalStorage)

		defer func() {
			if err := defaultStore.Close(); err != nil {
				log.Warningf(ctx, "close export storage failed %v", err)
			}
			for _, store := range localityStores {
				if err := store.Close(); err != nil {
					log.Warningf(ctx, "close export storage failed %v", err)
				}
			}
		}()
		// Check metadata files. Note: we do not check locality aware backup
		// metadata files ( prefixed with `backupPartitionDescriptorPrefix`) , as
		// they're validated in resolveBackupManifests.
		for _, metaFile := range []string{
			backupinfo.FileInfoPath,
			backupinfo.MetadataSSTName,
			backupbase.BackupManifestName + backupinfo.BackupManifestChecksumSuffix} {
			if _, err := defaultStore.Size(ctx, metaFile); err != nil {
				if metaFile == backupinfo.FileInfoPath || metaFile == backupinfo.MetadataSSTName {
					log.Warningf(ctx, `%v not found. This is only relevant if kv.bulkio.write_metadata_sst.enabled = true`, metaFile)
					continue
				}
				return nil, errors.Wrapf(err, "Error checking metadata file %s/%s",
					info.defaultURIs[layer], metaFile)
			}
		}
		// Check stat files.
		for _, statFile := range info.manifests[layer].StatisticsFilenames {
			if _, err := defaultStore.Size(ctx, statFile); err != nil {
				return nil, errors.Wrapf(err, "Error checking metadata file %s/%s",
					info.defaultURIs[layer], statFile)
			}
		}

		for locality, uri := range info.localityInfo[layer].URIsByOriginalLocalityKV {
			store, err := execCfg.DistSQLSrv.ExternalStorageFromURI(ctx, uri, user)
			if err != nil {
				return nil, err
			}
			localityStores[locality] = store
		}

		// Check all backup SSTs.
		fileSizes := make([]int64, 0)
		it, err := info.layerToIterFactory[layer].NewFileIter(ctx)
		if err != nil {
			return nil, err
		}
		defer it.Close()
		for ; ; it.Next() {
			if ok, err := it.Valid(); err != nil {
				return nil, err
			} else if !ok {
				break
			}

			f := it.Value()
			store := defaultStore
			uri := info.defaultURIs[layer]
			if _, ok := localityStores[f.LocalityKV]; ok {
				store = localityStores[f.LocalityKV]
				uri = info.localityInfo[layer].URIsByOriginalLocalityKV[f.LocalityKV]
			}
			sz, err := store.Size(ctx, f.Path)
			if err != nil {
				uriNoLocality := strings.Split(uri, "?")[0]
				missingFile := path.Join(uriNoLocality, f.Path)
				if _, ok := missingFiles[missingFile]; !ok {
					missingFiles[missingFile] = struct{}{}
					if maxMissingFiles == len(missingFiles) {
						break
					}
				}
				continue
			}
			fileSizes = append(fileSizes, sz)
		}

		return fileSizes, nil
	}

	manifestFileSizes := make([][]int64, len(info.manifests))
	for layer := range info.manifests {
		layerFileSizes, err := checkLayer(layer)
		if err != nil {
			return nil, err
		}
		if len(missingFiles) == maxMissingFiles {
			break
		}
		manifestFileSizes[layer] = layerFileSizes
	}
	if len(missingFiles) > 0 {
		filesForMsg := make([]string, 0, len(missingFiles))
		for file := range missingFiles {
			filesForMsg = append(filesForMsg, file)
		}
		errorMsgPrefix := "The following files are missing from the backup:"
		if len(missingFiles) == maxMissingFiles {
			errorMsgPrefix = "Multiple files cannot be read from the backup including:"
		}
		sort.Strings(filesForMsg)
		return nil, errors.Newf("%s\n\t%s", errorMsgPrefix, strings.Join(filesForMsg, "\n\t"))
	}
	return manifestFileSizes, nil
}

type backupInfo struct {
	collectionURI string
	defaultURIs   []string
	manifests     []backuppb.BackupManifest
	// layerToIterFactory is a mapping from the index of the backup layer in
	// manifests to its IterFactory.
	layerToIterFactory backupinfo.LayerToBackupManifestFileIterFactory
	subdir             string
	localityInfo       []jobspb.RestoreDetails_BackupLocalityInfo
	enc                *jobspb.BackupEncryptionOptions
	kmsEnv             cloud.KMSEnv
	fileSizes          [][]int64
}

type backupShower struct {
	// header defines the columns of the table printed as output of the show command.
	header colinfo.ResultColumns

	// fn is the specific implementation of the shower that can either be a default, ranges, files,
	// or JSON shower.
	fn func(ctx context.Context, info backupInfo) ([]tree.Datums, error)
}

// backupShowerHeaders defines the schema for the table presented to the user.
func backupShowerHeaders(showSchemas bool, opts tree.ShowBackupOptions) colinfo.ResultColumns {
	baseHeaders := colinfo.ResultColumns{
		{Name: "database_name", Typ: types.String},
		{Name: "parent_schema_name", Typ: types.String},
		{Name: "object_name", Typ: types.String},
		{Name: "object_type", Typ: types.String},
		{Name: "backup_type", Typ: types.String},
		{Name: "start_time", Typ: types.TimestampTZ},
		{Name: "end_time", Typ: types.TimestampTZ},
		{Name: "size_bytes", Typ: types.Int},
		{Name: "rows", Typ: types.Int},
		{Name: "is_full_cluster", Typ: types.Bool},
		{Name: "regions", Typ: types.String},
	}
	if showSchemas {
		baseHeaders = append(baseHeaders, colinfo.ResultColumn{Name: "create_statement", Typ: types.String})
	}
	if opts.Privileges {
		baseHeaders = append(baseHeaders, colinfo.ResultColumn{Name: "privileges", Typ: types.String})
		baseHeaders = append(baseHeaders, colinfo.ResultColumn{Name: "owner", Typ: types.String})
	}
	if opts.CheckFiles {
		baseHeaders = append(baseHeaders, colinfo.ResultColumn{Name: "file_bytes", Typ: types.Int})
	}
	if opts.DebugIDs {
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
	p sql.PlanHookState, showSchemas bool, opts tree.ShowBackupOptions,
) backupShower {
	return backupShower{
		header: backupShowerHeaders(showSchemas, opts),
		fn: func(ctx context.Context, info backupInfo) ([]tree.Datums, error) {
			ctx, sp := tracing.ChildSpan(ctx, "backupccl.backupShowerDefault.fn")
			defer sp.Finish()

			var rows []tree.Datums
			for layer, manifest := range info.manifests {
				descriptors, err := backupinfo.BackupManifestDescriptors(ctx, info.layerToIterFactory[layer], manifest.EndTime)
				if err != nil {
					return nil, err
				}
				var hydratedDescriptors []catalog.Descriptor
				showCreate := func(dbName string, tbl catalog.TableDescriptor) (string, error) {
					if len(hydratedDescriptors) == 0 {
						var c nstree.MutableCatalog
						for _, desc := range descriptors {
							c.UpsertDescriptor(desc)
						}
						if err := descs.HydrateCatalog(ctx, c); err != nil {
							return "", err
						}
						hydratedDescriptors = c.OrderedDescriptors()
					}
					return p.ShowCreate(
						ctx,
						dbName,
						hydratedDescriptors,
						tbl,
						sql.ShowCreateDisplayOptions{
							FKDisplayMode:  sql.OmitMissingFKClausesFromCreate,
							IgnoreComments: true,
						},
					)
				}

				// Map database ID to descriptor name.
				dbIDToName := make(map[descpb.ID]string)
				schemaIDToName := make(map[descpb.ID]string)
				typeIDToTypeDescriptor := make(map[descpb.ID]catalog.TypeDescriptor)
				schemaIDToName[keys.PublicSchemaIDForBackup] = catconstants.PublicSchemaName
				for _, desc := range descriptors {
					switch d := desc.(type) {
					case catalog.DatabaseDescriptor:
						if _, ok := dbIDToName[d.GetID()]; !ok {
							dbIDToName[d.GetID()] = d.GetName()
						}
					case catalog.TypeDescriptor:
						if _, ok := typeIDToTypeDescriptor[d.GetID()]; !ok {
							typeIDToTypeDescriptor[d.GetID()] = d
						}
					case catalog.SchemaDescriptor:
						if _, ok := schemaIDToName[d.GetID()]; !ok {
							schemaIDToName[d.GetID()] = d.GetName()
						}
					}
				}

				var fileSizes []int64
				if len(info.fileSizes) > 0 {
					fileSizes = info.fileSizes[layer]
				}

				var tableSizes map[catid.DescID]descriptorSize
				if !opts.SkipSize {
					tableSizes, err = getTableSizes(ctx, info.layerToIterFactory[layer], fileSizes)
					if err != nil {
						return nil, err
					}
				}

				backupType := tree.NewDString("full")
				if manifest.IsIncremental() {
					backupType = tree.NewDString("incremental")
				}
				start := tree.DNull
				end, err := tree.MakeDTimestampTZ(timeutil.Unix(0, manifest.EndTime.WallTime), time.Nanosecond)
				if err != nil {
					return nil, err
				}
				if manifest.StartTime.WallTime != 0 {
					start, err = tree.MakeDTimestampTZ(timeutil.Unix(0, manifest.StartTime.WallTime), time.Nanosecond)
					if err != nil {
						return nil, err
					}
				}
				var row tree.Datums

				for _, desc := range descriptors {
					var dbName string
					var parentSchemaName string
					var descriptorType string

					var dbID descpb.ID
					var parentSchemaID descpb.ID

					createStmtDatum := tree.DNull
					dataSizeDatum := tree.DNull
					rowCountDatum := tree.DNull
					fileSizeDatum := tree.DNull
					regionsDatum := tree.DNull

					descriptorName := desc.GetName()
					switch desc := desc.(type) {
					case catalog.DatabaseDescriptor:
						descriptorType = "database"
						if desc.IsMultiRegion() {
							regions, err := showRegions(typeIDToTypeDescriptor[desc.GetRegionConfig().RegionEnumID], desc.GetName())
							if err != nil {
								return nil, errors.Wrapf(err, "cannot generate regions column")
							}
							regionsDatum = nullIfEmpty(regions)
						}
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
					case catalog.FunctionDescriptor:
						descriptorType = "function"
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
						dataSizeDatum = tree.NewDInt(tree.DInt(tableSize.rowCount.DataSize))
						rowCountDatum = tree.NewDInt(tree.DInt(tableSize.rowCount.Rows))
						fileSizeDatum = tree.NewDInt(tree.DInt(tableSize.fileSize))

						// Only resolve the table schemas if running `SHOW BACKUP SCHEMAS`.
						// In all other cases we discard these results and so it is wasteful
						// to construct the SQL representation of the table's schema.
						if showSchemas {
							createStmt, err := showCreate(dbName, desc)
							if err != nil {
								// We expect that we might get an error here due to X-DB
								// references, which were possible on 20.2 betas and rcs.
								log.Errorf(ctx, "error while generating create statement: %+v", err)
							}
							createStmtDatum = nullIfEmpty(createStmt)
						}
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
						regionsDatum,
					}
					if showSchemas {
						row = append(row, createStmtDatum)
					}
					if opts.Privileges {
						showPrivs, err := showPrivileges(ctx, desc)
						if err != nil {
							return nil, err
						}
						row = append(row, tree.NewDString(showPrivs))
						owner := desc.GetPrivileges().Owner().SQLIdentifier()
						row = append(row, tree.NewDString(owner))
					}
					if opts.CheckFiles {
						row = append(row, fileSizeDatum)
					}
					if opts.DebugIDs {
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
				for _, t := range manifest.Tenants {
					tenantID, err := roachpb.MakeTenantID(t.ID)
					if err != nil {
						return nil, err
					}
					row := tree.Datums{
						tree.DNull,                         // Database
						tree.DNull,                         // Schema
						tree.NewDString(tenantID.String()), // Object Name
						tree.NewDString("TENANT"),          // Object Type
						backupType,
						start,
						end,
						tree.DNull, // DataSize
						tree.DNull, // RowCount
						tree.DNull, // Descriptor Coverage
						tree.DNull, // Regions
					}
					if showSchemas {
						row = append(row, tree.DNull)
					}
					if opts.Privileges {
						row = append(row, tree.DNull)
					}
					if opts.CheckFiles {
						row = append(row, tree.DNull)
					}
					if opts.DebugIDs {
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

type descriptorSize struct {
	rowCount roachpb.RowCount
	fileSize int64
}

// getLogicalSSTSize gets the total logical bytes stored in each SST. Note that a
// BackupManifest_File identifies a span in an SST and there can be multiple
// spans stored in an SST.
func getLogicalSSTSize(
	ctx context.Context, iterFactory *backupinfo.IterFactory,
) (map[string]int64, error) {
	ctx, span := tracing.ChildSpan(ctx, "backupccl.getLogicalSSTSize")
	defer span.Finish()

	sstDataSize := make(map[string]int64)
	it, err := iterFactory.NewFileIter(ctx)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	for ; ; it.Next() {
		if ok, err := it.Valid(); err != nil {
			return nil, err
		} else if !ok {
			break
		}

		f := it.Value()
		sstDataSize[f.Path] += f.EntryCounts.DataSize
	}
	return sstDataSize, nil
}

// approximateSpanPhysicalSize approximates the number of bytes written to disk for the span.
func approximateSpanPhysicalSize(
	logicalSpanSize int64, logicalSSTSize int64, physicalSSTSize int64,
) int64 {
	return int64(float64(physicalSSTSize) * (float64(logicalSpanSize) / float64(logicalSSTSize)))
}

// getTableSizes gathers row and size count for each table in the manifest
func getTableSizes(
	ctx context.Context, iterFactory *backupinfo.IterFactory, fileSizes []int64,
) (map[descpb.ID]descriptorSize, error) {
	ctx, span := tracing.ChildSpan(ctx, "backupccl.getTableSizes")
	defer span.Finish()

	logicalSSTSize, err := getLogicalSSTSize(ctx, iterFactory)
	if err != nil {
		return nil, err
	}

	it, err := iterFactory.NewFileIter(ctx)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	tableSizes := make(map[descpb.ID]descriptorSize)
	var tenantID roachpb.TenantID
	var showCodec keys.SQLCodec
	var idx int
	for ; ; it.Next() {
		if ok, err := it.Valid(); err != nil {
			return nil, err
		} else if !ok {
			break
		}

		f := it.Value()
		if !tenantID.IsSet() {
			var err error
			_, tenantID, err = keys.DecodeTenantPrefix(f.Span.Key)
			if err != nil {
				return nil, err
			}
			showCodec = keys.MakeSQLCodec(tenantID)
		}

		// TODO(dan): This assumes each file in the backup only
		// contains data from a single table, which is usually but
		// not always correct. It does not account for a BACKUP that
		// happened to catch a newly created table that hadn't yet
		// been split into its own range.

		// TODO(msbutler): after handling the todo above, understand whether
		// we should return an error if a key does not have tableId. The lack
		// of error handling let #77705 sneak by our unit tests.
		_, tableID, err := showCodec.DecodeTablePrefix(f.Span.Key)
		if err != nil {
			continue
		}
		s := tableSizes[descpb.ID(tableID)]
		s.rowCount.Add(f.EntryCounts)
		if len(fileSizes) > 0 {
			s.fileSize += approximateSpanPhysicalSize(f.EntryCounts.DataSize, logicalSSTSize[f.Path],
				fileSizes[idx])
		}
		tableSizes[descpb.ID(tableID)] = s
		idx++
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

// showRegions constructs a string containing the ALTER DATABASE
// commands that create the multi region specifications for a backed up database.
func showRegions(typeDesc catalog.TypeDescriptor, dbname string) (string, error) {
	var regionsStringBuilder strings.Builder
	if typeDesc == nil {
		return "", fmt.Errorf("type descriptor for %s is nil", dbname)
	}
	r := typeDesc.AsRegionEnumTypeDescriptor()
	if r == nil {
		return "", fmt.Errorf("type descriptor for %s is of kind %s", dbname, typeDesc.GetKind())
	}

	regionsStringBuilder.WriteString("ALTER DATABASE ")
	regionsStringBuilder.WriteString(dbname)
	regionsStringBuilder.WriteString(" SET PRIMARY REGION ")
	regionsStringBuilder.WriteString("\"" + r.PrimaryRegion().String() + "\"")
	regionsStringBuilder.WriteString(";")

	_ = r.ForEachPublicRegion(func(regionName catpb.RegionName) error {
		if regionName != r.PrimaryRegion() {
			regionsStringBuilder.WriteString(" ALTER DATABASE ")
			regionsStringBuilder.WriteString(dbname)
			regionsStringBuilder.WriteString(" ADD REGION ")
			regionsStringBuilder.WriteString("\"" + regionName.String() + "\"")
			regionsStringBuilder.WriteString(";")
		}
		return nil
	})
	return regionsStringBuilder.String(), nil
}

func showPrivileges(ctx context.Context, desc catalog.Descriptor) (string, error) {
	ctx, span := tracing.ChildSpan(ctx, "backupccl.showPrivileges")
	defer span.Finish()
	_ = ctx // ctx is currently unused, but this new ctx should be used below in the future.

	var privStringBuilder strings.Builder

	if desc == nil {
		return "", nil
	}
	privDesc := desc.GetPrivileges()
	objectType := desc.GetObjectType()
	if privDesc == nil {
		return "", nil
	}
	showList, err := privDesc.Show(objectType, false /* showImplicitOwnerPrivs */)
	if err != nil {
		return "", err
	}
	for _, userPriv := range showList {
		privs := userPriv.Privileges
		if len(privs) == 0 {
			continue
		}
		var privsWithGrantOption []string
		for _, priv := range privs {
			if priv.GrantOption {
				privsWithGrantOption = append(privsWithGrantOption,
					string(priv.Kind.DisplayName()))
			}
		}

		if len(privsWithGrantOption) > 0 {
			privStringBuilder.WriteString("GRANT ")
			privStringBuilder.WriteString(strings.Join(privsWithGrantOption, ", "))
			privStringBuilder.WriteString(" ON ")
			privStringBuilder.WriteString(strings.ToUpper(string(objectType)) + " ")
			privStringBuilder.WriteString(desc.GetName())
			privStringBuilder.WriteString(" TO ")
			privStringBuilder.WriteString(userPriv.User.SQLIdentifier())
			privStringBuilder.WriteString(" WITH GRANT OPTION; ")
		}

		var privsWithoutGrantOption []string
		for _, priv := range privs {
			if !priv.GrantOption {
				privsWithoutGrantOption = append(privsWithoutGrantOption,
					string(priv.Kind.DisplayName()))
			}

		}
		if len(privsWithoutGrantOption) > 0 {
			privStringBuilder.WriteString("GRANT ")
			privStringBuilder.WriteString(strings.Join(privsWithoutGrantOption, ", "))
			privStringBuilder.WriteString(" ON ")
			privStringBuilder.WriteString(strings.ToUpper(string(objectType)) + " ")
			privStringBuilder.WriteString(desc.GetName())
			privStringBuilder.WriteString(" TO ")
			privStringBuilder.WriteString(userPriv.User.SQLIdentifier())
			privStringBuilder.WriteString("; ")
		}
	}

	return privStringBuilder.String(), nil
}

var backupShowerRanges = backupShower{
	header: colinfo.ResultColumns{
		{Name: "start_pretty", Typ: types.String},
		{Name: "end_pretty", Typ: types.String},
		{Name: "start_key", Typ: types.Bytes},
		{Name: "end_key", Typ: types.Bytes},
	},

	fn: func(ctx context.Context, info backupInfo) (rows []tree.Datums, err error) {
		for _, manifest := range info.manifests {
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

var backupShowerDoctor = backupShower{
	header: colinfo.ResultColumns{
		{Name: "validation_status", Typ: types.String},
	},

	fn: func(ctx context.Context, info backupInfo) (rows []tree.Datums, err error) {
		var descTable doctor.DescriptorTable
		var namespaceTable doctor.NamespaceTable
		// Extract all the descriptors from the given manifest and generate the
		// namespace and descriptor tables needed by doctor.
		descriptors, _, err := backupinfo.LoadSQLDescsFromBackupsAtTime(ctx, info.manifests, info.layerToIterFactory, hlc.Timestamp{})
		if err != nil {
			return nil, err
		}
		for _, desc := range descriptors {
			bytes, err := protoutil.Marshal(desc.DescriptorProto())
			if err != nil {
				return nil, err
			}
			descTable = append(descTable,
				doctor.DescriptorTableRow{
					ID:        int64(desc.GetID()),
					DescBytes: bytes,
					ModTime:   desc.GetModificationTime(),
				})
			namespaceTable = append(namespaceTable,
				doctor.NamespaceTableRow{
					ID: int64(desc.GetID()),
					NameInfo: descpb.NameInfo{
						Name:           desc.GetName(),
						ParentID:       desc.GetParentID(),
						ParentSchemaID: desc.GetParentSchemaID(),
					},
				})
		}
		validationMessages := strings.Builder{}
		// We will intentionally not validate any jobs inside the manifest, since
		// these will be synthesized by the restore process.
		cv := clusterversion.DoctorBinaryVersion
		if len(info.manifests) > 0 {
			cv = info.manifests[len(info.manifests)-1].ClusterVersion
		}
		ok, err := doctor.Examine(ctx,
			clusterversion.ClusterVersion{Version: cv},
			descTable, namespaceTable,
			nil,
			false, /*validateJobs*/
			false,
			&validationMessages)
		if err != nil {
			return nil, err
		}
		if !ok {
			validationMessages.WriteString("ERROR: validation failed\n")
		} else {
			validationMessages.WriteString("No problems found!\n")
		}
		rows = append(rows, tree.Datums{
			tree.NewDString(validationMessages.String()),
		})
		return rows, nil
	},
}

func backupShowerFileSetup(
	p sql.PlanHookState, inCol tree.StringOrPlaceholderOptList,
) backupShower {
	return backupShower{header: colinfo.ResultColumns{
		{Name: "path", Typ: types.String},
		{Name: "backup_type", Typ: types.String},
		{Name: "start_pretty", Typ: types.String},
		{Name: "end_pretty", Typ: types.String},
		{Name: "start_key", Typ: types.Bytes},
		{Name: "end_key", Typ: types.Bytes},
		{Name: "size_bytes", Typ: types.Int},
		{Name: "rows", Typ: types.Int},
		{Name: "locality", Typ: types.String},
		{Name: "file_bytes", Typ: types.Int},
	},

		fn: func(ctx context.Context, info backupInfo) (rows []tree.Datums, err error) {

			var manifestDirs []string
			var localityAware bool
			if len(inCol) > 0 {
				manifestDirs, err = getManifestDirs(info.subdir, info.defaultURIs)
				if err != nil {
					return nil, err
				}

				if len(info.localityInfo[0].URIsByOriginalLocalityKV) > 0 {
					localityAware = true
				}
			}
			for i, manifest := range info.manifests {
				backupType := "full"
				if manifest.IsIncremental() {
					backupType = "incremental"
				}

				logicalSSTSize, err := getLogicalSSTSize(ctx, info.layerToIterFactory[i])
				if err != nil {
					return nil, err
				}

				it, err := info.layerToIterFactory[i].NewFileIter(ctx)
				if err != nil {
					return nil, err
				}
				defer it.Close()
				var idx int
				for ; ; it.Next() {
					if ok, err := it.Valid(); err != nil {
						return nil, err
					} else if !ok {
						break
					}
					file := it.Value()
					filePath := file.Path
					if inCol != nil {
						filePath = path.Join(manifestDirs[i], filePath)
					}
					locality := "NULL"
					if localityAware {
						locality = "default"
						if _, ok := info.localityInfo[i].URIsByOriginalLocalityKV[file.LocalityKV]; ok {
							locality = file.LocalityKV
						}
					}
					sz := int64(-1)
					if len(info.fileSizes) > 0 {
						sz = approximateSpanPhysicalSize(file.EntryCounts.DataSize,
							logicalSSTSize[file.Path], info.fileSizes[i][idx])
					}
					rows = append(rows, tree.Datums{
						tree.NewDString(filePath),
						tree.NewDString(backupType),
						tree.NewDString(file.Span.Key.String()),
						tree.NewDString(file.Span.EndKey.String()),
						tree.NewDBytes(tree.DBytes(file.Span.Key)),
						tree.NewDBytes(tree.DBytes(file.Span.EndKey)),
						tree.NewDInt(tree.DInt(file.EntryCounts.DataSize)),
						tree.NewDInt(tree.DInt(file.EntryCounts.Rows)),
						tree.NewDString(locality),
						tree.NewDInt(tree.DInt(sz)),
					})
					idx++
				}
			}
			return rows, nil
		},
	}
}

// getRootURI splits a fully resolved backup URI at the backup's subdirectory
// and returns the path to that subdirectory. e.g. for a full backup URI,
// getRootURI returns the collectionURI.
func getRootURI(defaultURI string, subdir string) (string, error) {
	splitFullBackupPath := strings.Split(defaultURI, subdir)
	if len(splitFullBackupPath) != 2 {
		return "", errors.AssertionFailedf(
			"the full backup URI %s does not contain 1 instance of the subdir %s"+
				"", defaultURI, subdir)
	}
	return splitFullBackupPath[0], nil
}

// getManifestDirs gathers the path to the directory of each backup manifest,
// relative to the collection root. Consider the following example: Suppose a
// backup chain contains a full backup with a defaultURI of
// 'userfile:///foo/fullSubdir' and an incremental backup with a defaultURI of
// 'userfile:///foo/incrementals/fullSubdir/incrementalSubdir'. getManifestDirs
// would return a relative path to the full backup manifest's directory, '/fullSubdir', and
// to the incremental backup manifest's directory
// '/incrementals/fullSubdir/incrementalSubdir'.
func getManifestDirs(fullSubdir string, defaultUris []string) ([]string, error) {
	manifestDirs := make([]string, len(defaultUris))

	// The full backup manifest path is always in the fullSubdir.
	manifestDirs[0] = fullSubdir

	if len(defaultUris) == 1 {
		return manifestDirs, nil
	}
	incRoot, err := getRootURI(defaultUris[1], fullSubdir)
	if err != nil {
		return nil, err
	}

	var incSubdir string
	if strings.HasSuffix(incRoot, backupbase.DefaultIncrementalsSubdir) {
		// The incremental backup is stored in the default incremental
		// directory (i.e. collectionURI/incrementals/fullSubdir)
		incSubdir = path.Join("/"+backupbase.DefaultIncrementalsSubdir, fullSubdir)
	} else {
		// Implies one of two scenarios:
		// 1) the incremental chain is stored in the pre 22.1
		//    default location: collectionURI/fullSubdir.
		// 2) incremental backups were created with `incremental_location`,
		//    so while the path to the subdirectory will be different
		//    than the full backup's, the incremental backups will have the
		//    same subdirectory, i.e. the full path is incrementalURI/fullSubdir.
		incSubdir = fullSubdir
	}

	for i, incURI := range defaultUris {
		// The first URI corresponds to the defaultURI of the full backup-- we have already dealt with
		// this.
		if i == 0 {
			continue
		}
		// the manifestDir for an incremental backup will have the following structure:
		// 'incSubdir/incSubSubSubDir', where incSubdir is resolved above,
		// and incSubSubDir corresponds to the path to the incremental backup within
		// the subdirectory.

		// remove locality info from URI
		incURI = strings.Split(incURI, "?")[0]

		// get the subdirectory within the incSubdir
		incSubSubDir := strings.Split(incURI, incSubdir)[1]
		manifestDirs[i] = path.Join(incSubdir, incSubSubDir)
	}
	return manifestDirs, nil
}

var jsonShower = backupShower{
	header: colinfo.ResultColumns{
		{Name: "manifest", Typ: types.Jsonb},
	},

	fn: func(ctx context.Context, info backupInfo) ([]tree.Datums, error) {
		rows := make([]tree.Datums, len(info.manifests))
		for i, manifest := range info.manifests {
			j, err := protoreflect.MessageToJSON(
				&manifest, protoreflect.FmtFlags{EmitDefaults: true, EmitRedacted: true})
			if err != nil {
				return nil, err
			}
			rows[i] = tree.Datums{tree.NewDJSON(j)}
		}
		return rows, nil
	},
}

func showBackupsInCollectionTypeCheck(
	ctx context.Context, backup *tree.ShowBackup, p sql.PlanHookState,
) (matched bool, header colinfo.ResultColumns, _ error) {
	if err := exprutil.TypeCheck(ctx, "SHOW BACKUPS", p.SemaCtx(),
		exprutil.StringArrays{tree.Exprs(backup.InCollection)},
	); err != nil {
		return false, nil, err
	}
	return true, showBackupsInCollectionHeader, nil
}

var showBackupsInCollectionHeader = colinfo.ResultColumns{
	{Name: "path", Typ: types.String},
}

// showBackupPlanHook implements PlanHookFn.
func showBackupsInCollectionPlanHook(
	ctx context.Context, collection []string, showStmt *tree.ShowBackup, p sql.PlanHookState,
) (sql.PlanHookRowFn, colinfo.ResultColumns, []sql.PlanNode, bool, error) {

	if err := cloudprivilege.CheckDestinationPrivileges(ctx, p, collection); err != nil {
		return nil, nil, nil, false, err
	}

	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		ctx, span := tracing.ChildSpan(ctx, showStmt.StatementTag())
		defer span.Finish()

		store, err := p.ExecCfg().DistSQLSrv.ExternalStorageFromURI(ctx, collection[0], p.User())
		if err != nil {
			return errors.Wrapf(err, "connect to external storage")
		}
		defer store.Close()
		res, err := backupdest.ListFullBackupsInCollection(ctx, store)
		if err != nil {
			return err
		}
		for _, i := range res {
			resultsCh <- tree.Datums{tree.NewDString(i)}
		}
		return nil
	}
	return fn, showBackupsInCollectionHeader, nil, false, nil
}

func init() {
	sql.AddPlanHook("backupccl.showBackupPlanHook", showBackupPlanHook, showBackupTypeCheck)
}
