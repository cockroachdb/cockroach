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
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backupbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backupdest"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backupencryption"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backupinfo"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backuppb"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backupresolver"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/featureflag"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	pbtypes "github.com/gogo/protobuf/types"
)

const (
	backupOptRevisionHistory  = "revision_history"
	backupOptWithPrivileges   = "privileges"
	backupOptAsJSON           = "as_json"
	backupOptWithDebugIDs     = "debug_ids"
	backupOptIncStorage       = "incremental_location"
	backupOptDebugMetadataSST = "debug_dump_metadata_sst"
	backupOptEncDir           = "encryption_info_dir"
	backupOptCheckFiles       = "check_files"
	// backupPartitionDescriptorPrefix is the file name prefix for serialized
	// BackupPartitionDescriptor protos.
	backupPartitionDescriptorPrefix = "BACKUP_PART"
)

type tableAndIndex struct {
	tableID descpb.ID
	indexID descpb.IndexID
}

// featureBackupEnabled is used to enable and disable the BACKUP feature.
var featureBackupEnabled = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"feature.backup.enabled",
	"set to true to enable backups, false to disable; default is true",
	featureflag.FeatureFlagEnabledDefault,
).WithPublic()

// forEachPublicIndexTableSpan constructs a span for each public index of the
// provided table and runs the given function on each of them. The added map is
// used to track duplicates. Duplicate indexes are not passed to the provided
// function.
func forEachPublicIndexTableSpan(
	table *descpb.TableDescriptor,
	added map[tableAndIndex]bool,
	codec keys.SQLCodec,
	f func(span roachpb.Span),
) {
	table.ForEachPublicIndex(func(idx *descpb.IndexDescriptor) {
		key := tableAndIndex{tableID: table.GetID(), indexID: idx.ID}
		if added[key] {
			return
		}
		added[key] = true
		prefix := roachpb.Key(rowenc.MakeIndexKeyPrefix(codec, table.GetID(), idx.ID))
		f(roachpb.Span{Key: prefix, EndKey: prefix.PrefixEnd()})
	})
}

// spansForAllTableIndexes returns non-overlapping spans for every index and
// table passed in. They would normally overlap if any of them are interleaved.
// Overlapping index spans are merged so as to optimize the size/number of the
// spans we BACKUP and lay protected ts records for.
func spansForAllTableIndexes(
	execCfg *sql.ExecutorConfig,
	tables []catalog.TableDescriptor,
	revs []backuppb.BackupManifest_DescriptorRevision,
) ([]roachpb.Span, error) {

	added := make(map[tableAndIndex]bool, len(tables))
	sstIntervalTree := interval.NewTree(interval.ExclusiveOverlapper)
	insertSpan := func(indexSpan roachpb.Span) {
		if err := sstIntervalTree.Insert(intervalSpan(indexSpan), true); err != nil {
			panic(errors.NewAssertionErrorWithWrappedErrf(err, "IndexSpan"))
		}
	}

	for _, table := range tables {
		forEachPublicIndexTableSpan(table.TableDesc(), added, execCfg.Codec, insertSpan)
	}

	// If there are desc revisions, ensure that we also add any index spans
	// in them that we didn't already get above e.g. indexes or tables that are
	// not in latest because they were dropped during the time window in question.
	for _, rev := range revs {
		// If the table was dropped during the last interval, it will have
		// at least 2 revisions, and the first one should have the table in a PUBLIC
		// state. We want (and do) ignore tables that have been dropped for the
		// entire interval. DROPPED tables should never later become PUBLIC.
		rawTbl, _, _, _, _ := descpb.FromDescriptor(rev.Desc)
		if rawTbl != nil && rawTbl.Public() {
			forEachPublicIndexTableSpan(rawTbl, added, execCfg.Codec, insertSpan)
		}
	}

	sstIntervalTree.AdjustRanges()
	spans := make([]roachpb.Span, 0, sstIntervalTree.Len())
	_ = sstIntervalTree.Do(func(r interval.Interface) bool {
		spans = append(spans, roachpb.Span{
			Key:    roachpb.Key(r.Range().Start),
			EndKey: roachpb.Key(r.Range().End),
		})
		return false
	})

	// Attempt to merge any contiguous spans generated from the tables and revs.
	// No need to check if the spans are distinct, since some of the merged
	// indexes may overlap between different revisions of the same descriptor.
	mergedSpans, _ := roachpb.MergeSpans(&spans)

	knobs := execCfg.BackupRestoreTestingKnobs
	if knobs != nil && knobs.CaptureResolvedTableDescSpans != nil {
		knobs.CaptureResolvedTableDescSpans(mergedSpans)
	}

	return mergedSpans, nil
}

func resolveOptionsForBackupJobDescription(
	opts tree.BackupOptions, kmsURIs []string, incrementalStorage []string,
) (tree.BackupOptions, error) {
	if opts.IsDefault() {
		return opts, nil
	}

	newOpts := tree.BackupOptions{
		CaptureRevisionHistory: opts.CaptureRevisionHistory,
		Detached:               opts.Detached,
	}

	if opts.EncryptionPassphrase != nil {
		newOpts.EncryptionPassphrase = tree.NewDString("redacted")
	}

	var err error
	// TODO(msbutler): use cloud.RedactKMSURI(uri) here instead?
	newOpts.EncryptionKMSURI, err = sanitizeURIList(kmsURIs)
	if err != nil {
		return tree.BackupOptions{}, err
	}

	newOpts.IncrementalStorage, err = sanitizeURIList(incrementalStorage)
	if err != nil {
		return tree.BackupOptions{}, err
	}

	return newOpts, nil
}

// GetRedactedBackupNode returns a copy of the argument `backup`, but with all
// the secret information redacted.
func GetRedactedBackupNode(
	backup *tree.Backup,
	to []string,
	incrementalFrom []string,
	kmsURIs []string,
	resolvedSubdir string,
	incrementalStorage []string,
	hasBeenPlanned bool,
) (*tree.Backup, error) {
	b := &tree.Backup{
		AsOf:    backup.AsOf,
		Targets: backup.Targets,
		Nested:  backup.Nested,
	}

	// We set Subdir to the directory resolved during BACKUP planning.
	//
	// - For `BACKUP INTO 'subdir' IN...` this would be the specified subdir
	// (with a single / prefix).
	// - For `BACKUP INTO LATEST...` this would be the sub-directory pointed to by
	// LATEST, where we are appending an incremental BACKUP.
	// - For `BACKUP INTO x` this would be the sub-directory we have selected to
	// write the BACKUP to.
	if b.Nested && hasBeenPlanned {
		b.Subdir = tree.NewDString(resolvedSubdir)
	}

	var err error
	b.To, err = sanitizeURIList(to)
	if err != nil {
		return nil, err
	}

	b.IncrementalFrom, err = sanitizeURIList(incrementalFrom)
	if err != nil {
		return nil, err
	}

	resolvedOpts, err := resolveOptionsForBackupJobDescription(backup.Options, kmsURIs,
		incrementalStorage)
	if err != nil {
		return nil, err
	}
	b.Options = resolvedOpts
	return b, nil
}

// sanitizeURIList sanitizes a list of URIS in order to build an AST
func sanitizeURIList(uris []string) ([]tree.Expr, error) {
	var sanitizedURIs []tree.Expr
	for _, uri := range uris {
		sanitizedURI, err := cloud.SanitizeExternalStorageURI(uri, nil /* extraParams */)
		if err != nil {
			return nil, err
		}
		sanitizedURIs = append(sanitizedURIs, tree.NewDString(sanitizedURI))
	}
	return sanitizedURIs, nil
}

func backupJobDescription(
	p sql.PlanHookState,
	backup *tree.Backup,
	to []string,
	incrementalFrom []string,
	kmsURIs []string,
	resolvedSubdir string,
	incrementalStorage []string,
) (string, error) {
	b, err := GetRedactedBackupNode(backup, to, incrementalFrom, kmsURIs,
		resolvedSubdir, incrementalStorage, true /* hasBeenPlanned */)
	if err != nil {
		return "", err
	}

	ann := p.ExtendedEvalContext().Annotations
	return tree.AsStringWithFQNames(b, ann), nil
}

// annotatedBackupStatement is a tree.Backup, optionally
// annotated with the scheduling information.
type annotatedBackupStatement struct {
	*tree.Backup
	*jobs.CreatedByInfo
}

func getBackupStatement(stmt tree.Statement) *annotatedBackupStatement {
	switch backup := stmt.(type) {
	case *annotatedBackupStatement:
		return backup
	case *tree.Backup:
		return &annotatedBackupStatement{Backup: backup}
	default:
		return nil
	}
}

func checkPrivilegesForBackup(
	ctx context.Context,
	backupStmt *annotatedBackupStatement,
	p sql.PlanHookState,
	targetDescs []catalog.Descriptor,
	to []string,
) error {
	hasAdmin, err := p.HasAdminRole(ctx)
	if err != nil {
		return err
	}
	if hasAdmin {
		return nil
	}
	// Do not allow full cluster backups.
	if backupStmt.Coverage() == tree.AllDescriptors {
		return pgerror.Newf(
			pgcode.InsufficientPrivilege,
			"only users with the admin role are allowed to perform full cluster backups")
	}
	// Do not allow tenant backups.
	if backupStmt.Targets != nil && backupStmt.Targets.TenantID.IsSet() {
		return pgerror.Newf(
			pgcode.InsufficientPrivilege,
			"only users with the admin role can perform BACKUP TENANT")
	}
	for _, desc := range targetDescs {
		switch desc := desc.(type) {
		case catalog.DatabaseDescriptor:
			if connectErr := p.CheckPrivilege(ctx, desc, privilege.CONNECT); connectErr != nil {
				// SELECT is being deprecated as privilege on Databases in 22.1.
				// In the meanwhile, we still allow backup if the user has SELECT.
				// TODO(richardjcai): Remove this check for SELECT in 22.1.
				if selectErr := p.CheckPrivilege(ctx, desc, privilege.SELECT); selectErr != nil {
					// Return the connectErr as we want users to grant CONNECT to perform
					// this backup and not select.
					return connectErr
				}
			}
		case catalog.TableDescriptor:
			if err := p.CheckPrivilege(ctx, desc, privilege.SELECT); err != nil {
				return err
			}
		case catalog.TypeDescriptor, catalog.SchemaDescriptor:
			if err := p.CheckPrivilege(ctx, desc, privilege.USAGE); err != nil {
				return err
			}
		}
	}
	if p.ExecCfg().ExternalIODirConfig.EnableNonAdminImplicitAndArbitraryOutbound {
		return nil
	}

	// Check destination specific privileges.
	for _, uri := range to {
		conf, err := cloud.ExternalStorageConfFromURI(uri, p.User())
		if err != nil {
			return err
		}

		// Check if the destination requires the user to be an admin.
		if !conf.AccessIsWithExplicitAuth() {
			return pgerror.Newf(
				pgcode.InsufficientPrivilege,
				"only users with the admin role are allowed to BACKUP to the specified %s URI",
				conf.Provider.String())
		}

		// If the backup is running to an External Connection, check that the user
		// has adequate privileges.
		if conf.Provider == cloudpb.ExternalStorageProvider_external {
			if !p.ExecCfg().Settings.Version.IsActive(ctx, clusterversion.SystemExternalConnectionsTable) {
				return pgerror.Newf(pgcode.FeatureNotSupported,
					"version %v must be finalized to backup to an External Connection",
					clusterversion.ByKey(clusterversion.SystemExternalConnectionsTable))
			}
			ecPrivilege := &syntheticprivilege.ExternalConnectionPrivilege{
				ConnectionName: conf.ExternalConnectionConfig.Name,
			}
			if err := p.CheckPrivilege(ctx, ecPrivilege, privilege.USAGE); err != nil {
				return err
			}
		}
	}

	return nil
}

func requireEnterprise(execCfg *sql.ExecutorConfig, feature string) error {
	if err := utilccl.CheckEnterpriseEnabled(
		execCfg.Settings, execCfg.NodeInfo.LogicalClusterID(), execCfg.Organization(),
		fmt.Sprintf("BACKUP with %s", feature),
	); err != nil {
		return err
	}
	return nil
}

// backupPlanHook implements PlanHookFn.
func backupPlanHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, colinfo.ResultColumns, []sql.PlanNode, bool, error) {
	backupStmt := getBackupStatement(stmt)
	if backupStmt == nil {
		return nil, nil, nil, false, nil
	}

	if err := featureflag.CheckEnabled(
		ctx,
		p.ExecCfg(),
		featureBackupEnabled,
		"BACKUP",
	); err != nil {
		return nil, nil, nil, false, err
	}

	// Deprecation notice for `BACKUP TO` syntax. Remove this once the syntax is
	// deleted in 22.2.
	if !backupStmt.Nested {
		p.BufferClientNotice(ctx,
			pgnotice.Newf("The `BACKUP TO` syntax will be removed in a future release, please"+
				" switch over to using `BACKUP INTO` to create a backup collection: %s. "+
				"Backups created using the `BACKUP TO` syntax may not be restoreable in the next major version release.",
				"https://www.cockroachlabs.com/docs/stable/backup.html#considerations"))
	}

	var err error
	subdirFn := func() (string, error) { return "", nil }
	if backupStmt.Subdir != nil {
		subdirFn, err = p.TypeAsString(ctx, backupStmt.Subdir, "BACKUP")
		if err != nil {
			return nil, nil, nil, false, err
		}
	}

	toFn, err := p.TypeAsStringArray(ctx, tree.Exprs(backupStmt.To), "BACKUP")
	if err != nil {
		return nil, nil, nil, false, err
	}
	incrementalFromFn, err := p.TypeAsStringArray(ctx, backupStmt.IncrementalFrom, "BACKUP")
	if err != nil {
		return nil, nil, nil, false, err
	}

	incToFn, err := p.TypeAsStringArray(ctx, tree.Exprs(backupStmt.Options.IncrementalStorage),
		"BACKUP")
	if err != nil {
		return nil, nil, nil, false, err
	}

	detached := false
	if backupStmt.Options.Detached == tree.DBoolTrue {
		detached = true
	}
	revisionHistoryFn := func() (bool, error) { return false, nil } // Defaults to false.
	if backupStmt.Options.CaptureRevisionHistory != nil {
		revisionHistoryFn, err = p.TypeAsBool(ctx, backupStmt.Options.CaptureRevisionHistory, "BACKUP")
		if err != nil {
			return nil, nil, nil, false, err
		}
	}

	encryptionParams := jobspb.BackupEncryptionOptions{Mode: jobspb.EncryptionMode_None}

	var pwFn func() (string, error)
	if backupStmt.Options.EncryptionPassphrase != nil {
		fn, err := p.TypeAsString(ctx, backupStmt.Options.EncryptionPassphrase, "BACKUP")
		if err != nil {
			return nil, nil, nil, false, err
		}
		pwFn = fn
		encryptionParams.Mode = jobspb.EncryptionMode_Passphrase
	}

	var kmsFn func() ([]string, error)
	if backupStmt.Options.EncryptionKMSURI != nil {
		if encryptionParams.Mode != jobspb.EncryptionMode_None {
			return nil, nil, nil, false,
				errors.New("cannot have both encryption_passphrase and kms option set")
		}
		fn, err := p.TypeAsStringArray(ctx, tree.Exprs(backupStmt.Options.EncryptionKMSURI),
			"BACKUP")
		if err != nil {
			return nil, nil, nil, false, err
		}
		kmsFn = func() ([]string, error) {
			res, err := fn()
			if err == nil {
				return res, nil
			}
			return nil, err
		}
		encryptionParams.Mode = jobspb.EncryptionMode_KMS
	}

	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		// TODO(dan): Move this span into sql.
		ctx, span := tracing.ChildSpan(ctx, stmt.StatementTag())
		defer span.Finish()

		if !(p.ExtendedEvalContext().TxnIsSingleStmt || detached) {
			return errors.Errorf("BACKUP cannot be used inside a multi-statement transaction without DETACHED option")
		}

		subdir, err := subdirFn()
		if err != nil {
			return err
		}

		to, err := toFn()
		if err != nil {
			return err
		}
		if len(to) > 1 {
			if err := requireEnterprise(p.ExecCfg(), "partitioned destinations"); err != nil {
				return err
			}
		}

		incrementalFrom, err := incrementalFromFn()
		if err != nil {
			return err
		}

		incrementalStorage, err := incToFn()
		if err != nil {
			return err
		}
		if !backupStmt.Nested && len(incrementalStorage) > 0 {
			return errors.New("incremental_location option not supported with `BACKUP TO` syntax")
		}
		if len(incrementalStorage) > 0 && (len(incrementalStorage) != len(to)) {
			return errors.New("the incremental_location option must contain the same number of locality" +
				" aware URIs as the full backup destination")
		}

		var asOfInterval int64
		endTime := p.ExecCfg().Clock.Now()
		if backupStmt.AsOf.Expr != nil {
			asOf, err := p.EvalAsOfTimestamp(ctx, backupStmt.AsOf)
			if err != nil {
				return err
			}
			endTime = asOf.Timestamp
			asOfInterval = asOf.Timestamp.WallTime - p.ExtendedEvalContext().StmtTimestamp.UnixNano()
		}

		switch encryptionParams.Mode {
		case jobspb.EncryptionMode_Passphrase:
			pw, err := pwFn()
			if err != nil {
				return err
			}
			if err := requireEnterprise(p.ExecCfg(), "encryption"); err != nil {
				return err
			}
			encryptionParams.RawPassphrae = pw
		case jobspb.EncryptionMode_KMS:
			encryptionParams.RawKmsUris, err = kmsFn()
			if err != nil {
				return err
			}
			if err := requireEnterprise(p.ExecCfg(), "encryption"); err != nil {
				return err
			}
		}

		revisionHistory, err := revisionHistoryFn()
		if err != nil {
			return err
		}
		if revisionHistory {
			if err := requireEnterprise(p.ExecCfg(), "revision_history"); err != nil {
				return err
			}
		}

		var targetDescs []catalog.Descriptor
		var completeDBs []descpb.ID
		var requestedDBs []catalog.DatabaseDescriptor
		var descsByTablePattern map[tree.TablePattern]catalog.Descriptor

		switch backupStmt.Coverage() {
		case tree.RequestedDescriptors:
			var err error
			targetDescs, completeDBs, requestedDBs, descsByTablePattern, err = backupresolver.ResolveTargetsToDescriptors(ctx, p, endTime, backupStmt.Targets)
			if err != nil {
				return errors.Wrap(err, "failed to resolve targets specified in the BACKUP stmt")
			}
		case tree.AllDescriptors:
			var err error
			targetDescs, completeDBs, err = fullClusterTargetsBackup(ctx, p.ExecCfg(), endTime)
			if err != nil {
				return err
			}
		default:
			return errors.AssertionFailedf("unexpected descriptor coverage %v", backupStmt.Coverage())
		}

		// Check BACKUP privileges.
		err = checkPrivilegesForBackup(ctx, backupStmt, p, targetDescs, to)
		if err != nil {
			return err
		}

		initialDetails := jobspb.BackupDetails{
			Destination:         jobspb.BackupDetails_Destination{To: to, IncrementalStorage: incrementalStorage},
			EndTime:             endTime,
			RevisionHistory:     revisionHistory,
			IncrementalFrom:     incrementalFrom,
			FullCluster:         backupStmt.Coverage() == tree.AllDescriptors,
			ResolvedCompleteDbs: completeDBs,
			EncryptionOptions:   &encryptionParams,
			AsOfInterval:        asOfInterval,
			Detached:            detached,
		}
		if backupStmt.CreatedByInfo != nil && backupStmt.CreatedByInfo.Name == jobs.CreatedByScheduledJobs {
			initialDetails.ScheduleID = backupStmt.CreatedByInfo.ID
		}

		// For backups of specific targets, those targets were resolved with this
		// planner's session, so we need to store the result of resolution. For
		// full-cluster we can just recompute it during execution.
		if !initialDetails.FullCluster {
			descriptorProtos := make([]descpb.Descriptor, 0, len(targetDescs))
			for _, desc := range targetDescs {
				descriptorProtos = append(descriptorProtos, *desc.DescriptorProto())
			}
			initialDetails.ResolvedTargets = descriptorProtos

			for _, desc := range descsByTablePattern {
				initialDetails.RequestedTargets = append(initialDetails.RequestedTargets, *desc.DescriptorProto())
			}

			for _, desc := range requestedDBs {
				initialDetails.RequestedTargets = append(initialDetails.RequestedTargets, *desc.DescriptorProto())
			}
		}

		if backupStmt.Nested {
			if backupStmt.AppendToLatest {
				initialDetails.Destination.Subdir = backupbase.LatestFileName
				initialDetails.Destination.Exists = true

			} else if subdir != "" {
				initialDetails.Destination.Subdir = "/" + strings.TrimPrefix(subdir, "/")
				initialDetails.Destination.Exists = true
			} else {
				initialDetails.Destination.Subdir = endTime.GoTime().Format(backupbase.DateBasedIntoFolderName)
			}
		}

		if backupStmt.Targets != nil && backupStmt.Targets.TenantID.IsSet() {
			if !p.ExecCfg().Codec.ForSystemTenant() {
				return pgerror.Newf(pgcode.InsufficientPrivilege, "only the system tenant can backup other tenants")
			}
			initialDetails.SpecificTenantIds = []roachpb.TenantID{roachpb.MakeTenantID(backupStmt.Targets.TenantID.ID)}
		}

		jobID := p.ExecCfg().JobRegistry.MakeJobID()

		description, err := backupJobDescription(p,
			backupStmt.Backup, to, incrementalFrom,
			encryptionParams.RawKmsUris,
			initialDetails.Destination.Subdir,
			initialDetails.Destination.IncrementalStorage,
		)
		if err != nil {
			return err
		}
		jr := jobs.Record{
			Description: description,
			Details:     initialDetails,
			Progress:    jobspb.BackupProgress{},
			CreatedBy:   backupStmt.CreatedByInfo,
			Username:    p.User(),
			DescriptorIDs: func() (sqlDescIDs []descpb.ID) {
				for i := range targetDescs {
					sqlDescIDs = append(sqlDescIDs, targetDescs[i].GetID())
				}
				return sqlDescIDs
			}(),
		}
		plannerTxn := p.Txn()

		if detached {
			// When running inside an explicit transaction, we simply create the job
			// record. We do not wait for the job to finish.
			_, err := p.ExecCfg().JobRegistry.CreateAdoptableJobWithTxn(
				ctx, jr, jobID, plannerTxn)
			if err != nil {
				return err
			}
			resultsCh <- tree.Datums{tree.NewDInt(tree.DInt(jobID))}
			return nil
		}
		var sj *jobs.StartableJob
		if err := func() (err error) {
			defer func() {
				if err == nil || sj == nil {
					return
				}
				if cleanupErr := sj.CleanupOnRollback(ctx); cleanupErr != nil {
					log.Errorf(ctx, "failed to cleanup job: %v", cleanupErr)
				}
			}()
			if err := p.ExecCfg().JobRegistry.CreateStartableJobWithTxn(ctx, &sj, jobID, plannerTxn, jr); err != nil {
				return err
			}
			// We commit the transaction here so that the job can be started. This
			// is safe because we're in an implicit transaction. If we were in an
			// explicit transaction the job would have to be run with the detached
			// option and would have been handled above.
			return plannerTxn.Commit(ctx)
		}(); err != nil {
			return err
		}
		if err := sj.Start(ctx); err != nil {
			return err
		}
		if err := sj.AwaitCompletion(ctx); err != nil {
			return err
		}
		return sj.ReportExecutionResults(ctx, resultsCh)
	}

	if detached {
		return fn, jobs.DetachedJobExecutionResultHeader, nil, false, nil
	}
	return fn, jobs.BulkJobExecutionResultHeader, nil, false, nil
}

func collectTelemetry(
	ctx context.Context,
	backupManifest backuppb.BackupManifest,
	initialDetails, backupDetails jobspb.BackupDetails,
	licensed bool,
	jobID jobspb.JobID,
) {
	// sourceSuffix specifies if this schedule was created by a schedule.
	sourceSuffix := ".manual"
	if backupDetails.ScheduleID != 0 {
		sourceSuffix = ".scheduled"
	}

	// countSource emits a telemetry counter and also adds a ".scheduled"
	// suffix if the job was created by a schedule.
	countSource := func(feature string) {
		telemetry.Count(feature + sourceSuffix)
	}

	countSource("backup.total.started")
	if backupManifest.IsIncremental() || backupDetails.EncryptionOptions != nil {
		countSource("backup.using-enterprise-features")
	}
	if licensed {
		countSource("backup.licensed")
	} else {
		countSource("backup.free")
	}
	if backupDetails.StartTime.IsEmpty() {
		countSource("backup.span.full")
	} else {
		countSource("backup.span.incremental")
		telemetry.CountBucketed("backup.incremental-span-sec",
			int64(backupDetails.EndTime.GoTime().Sub(backupDetails.StartTime.GoTime()).Seconds()))
		if len(initialDetails.IncrementalFrom) == 0 {
			countSource("backup.auto-incremental")
		}
	}
	if len(backupDetails.URIsByLocalityKV) > 1 {
		countSource("backup.partitioned")
	}
	if backupManifest.MVCCFilter == backuppb.MVCCFilter_All {
		countSource("backup.revision-history")
	}
	if backupDetails.EncryptionOptions != nil {
		countSource("backup.encrypted")
		switch backupDetails.EncryptionOptions.Mode {
		case jobspb.EncryptionMode_Passphrase:
			countSource("backup.encryption.passphrase")
		case jobspb.EncryptionMode_KMS:
			countSource("backup.encryption.kms")
		}
	}
	if backupDetails.CollectionURI != "" {
		countSource("backup.nested")
		timeBaseSubdir := true
		if _, err := time.Parse(backupbase.DateBasedIntoFolderName,
			initialDetails.Destination.Subdir); err != nil {
			timeBaseSubdir = false
		}
		if backupDetails.StartTime.IsEmpty() {
			if !timeBaseSubdir {
				countSource("backup.deprecated-full-nontime-subdir")
			} else if initialDetails.Destination.Exists {
				countSource("backup.deprecated-full-time-subdir")
			} else {
				countSource("backup.full-no-subdir")
			}
		} else {
			if initialDetails.Destination.Subdir == backupbase.LatestFileName {
				countSource("backup.incremental-latest-subdir")
			} else if !timeBaseSubdir {
				countSource("backup.deprecated-incremental-nontime-subdir")
			} else {
				countSource("backup.incremental-explicit-subdir")
			}
		}
	} else {
		countSource("backup.deprecated-non-collection")
	}
	if backupManifest.DescriptorCoverage == tree.AllDescriptors {
		countSource("backup.targets.full_cluster")
	}

	logBackupTelemetry(ctx, initialDetails, jobID)
}

func getScheduledBackupExecutionArgsFromSchedule(
	ctx context.Context,
	env scheduledjobs.JobSchedulerEnv,
	txn *kv.Txn,
	ie *sql.InternalExecutor,
	scheduleID int64,
) (*jobs.ScheduledJob, *backuppb.ScheduledBackupExecutionArgs, error) {
	// Load the schedule that has spawned this job.
	sj, err := jobs.LoadScheduledJob(ctx, env, scheduleID, ie, txn)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed to load scheduled job %d", scheduleID)
	}

	args := &backuppb.ScheduledBackupExecutionArgs{}
	if err := pbtypes.UnmarshalAny(sj.ExecutionArgs().Args, args); err != nil {
		return nil, nil, errors.Wrap(err, "un-marshaling args")
	}

	return sj, args, nil
}

// planSchedulePTSChaining populates backupDetails with information relevant to
// the chaining of protected timestamp records between scheduled backups.
// Depending on whether backupStmt is a full or incremental backup, we populate
// relevant fields that are used to perform this chaining, on successful
// completion of the backup job.
func planSchedulePTSChaining(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	txn *kv.Txn,
	backupDetails *jobspb.BackupDetails,
	createdBy *jobs.CreatedByInfo,
) error {
	env := scheduledjobs.ProdJobSchedulerEnv
	if knobs, ok := execCfg.DistSQLSrv.TestingKnobs.JobsTestingKnobs.(*jobs.TestingKnobs); ok {
		if knobs.JobSchedulerEnv != nil {
			env = knobs.JobSchedulerEnv
		}
	}
	// If this is not a scheduled backup, we do not chain pts records.
	if createdBy == nil || createdBy.Name != jobs.CreatedByScheduledJobs {
		return nil
	}

	_, args, err := getScheduledBackupExecutionArgsFromSchedule(
		ctx, env, txn, execCfg.InternalExecutor, createdBy.ID)
	if err != nil {
		return err
	}

	// If chaining of protected timestamp records is disabled, noop.
	if !args.ChainProtectedTimestampRecords {
		return nil
	}

	if args.BackupType == backuppb.ScheduledBackupExecutionArgs_FULL {
		// Check if there is a dependent incremental schedule associated with the
		// full schedule running the current backup.
		//
		// If present, the full backup on successful completion, will release the
		// pts record found on the incremental schedule, and replace it with a new
		// pts record protecting after the EndTime of the full backup.
		if args.DependentScheduleID == 0 {
			return nil
		}

		_, incArgs, err := getScheduledBackupExecutionArgsFromSchedule(
			ctx, env, txn, execCfg.InternalExecutor, args.DependentScheduleID)
		if err != nil {
			// We should always be able to resolve the dependent schedule ID. If the
			// incremental schedule was dropped then it would have unlinked itself
			// from the full schedule. Thus, we treat all errors as a problem.
			return errors.NewAssertionErrorWithWrappedErrf(err,
				"dependent schedule %d could not be resolved", args.DependentScheduleID)
		}
		backupDetails.SchedulePTSChainingRecord = &jobspb.SchedulePTSChainingRecord{
			ProtectedTimestampRecord: incArgs.ProtectedTimestampRecord,
			Action:                   jobspb.SchedulePTSChainingRecord_RELEASE,
		}
	} else {
		// In the case of a scheduled incremental backup we save the pts record id
		// that the job should update on successful completion, to protect data
		// after the current backups' EndTime.
		// We save this information on the job instead of reading it from the
		// schedule on completion, to prevent an "overhang" incremental from
		// incorrectly pulling forward a pts record that was written by a new full
		// backup that completed while the incremental was still executing.
		//
		// NB: An overhang incremental is defined as a scheduled incremental backup
		// that appends to the old full backup chain, and completes after a new full
		// backup has started another chain.
		backupDetails.SchedulePTSChainingRecord = &jobspb.SchedulePTSChainingRecord{
			ProtectedTimestampRecord: args.ProtectedTimestampRecord,
			Action:                   jobspb.SchedulePTSChainingRecord_UPDATE,
		}
	}
	return nil
}

// getReintroducedSpans checks to see if any spans need to be re-backed up from
// ts = 0. This may be the case if a span was OFFLINE in the previous backup and
// has come back online since. The entire span needs to be re-backed up because
// we may otherwise miss AddSSTable requests which write to a timestamp older
// than the last incremental.
func getReintroducedSpans(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	prevBackups []backuppb.BackupManifest,
	tables []catalog.TableDescriptor,
	revs []backuppb.BackupManifest_DescriptorRevision,
	endTime hlc.Timestamp,
) ([]roachpb.Span, error) {
	reintroducedTables := make(map[descpb.ID]struct{})

	offlineInLastBackup := make(map[descpb.ID]struct{})
	lastBackup := prevBackups[len(prevBackups)-1]
	for _, desc := range lastBackup.Descriptors {
		// TODO(pbardea): Also check that lastWriteTime is set once those are
		// populated on the table descriptor.
		if table, _, _, _, _ := descpb.FromDescriptor(&desc); table != nil && table.Offline() {
			offlineInLastBackup[table.GetID()] = struct{}{}
		}
	}

	// If the table was offline in the last backup, but becomes PUBLIC, then it
	// needs to be re-included since we may have missed non-transactional writes.
	tablesToReinclude := make([]catalog.TableDescriptor, 0)
	for _, desc := range tables {
		if _, wasOffline := offlineInLastBackup[desc.GetID()]; wasOffline && desc.Public() {
			tablesToReinclude = append(tablesToReinclude, desc)
			reintroducedTables[desc.GetID()] = struct{}{}
		}
	}

	// Tables should be re-introduced if any revision of the table was PUBLIC. A
	// table may have been OFFLINE at the time of the last backup, and OFFLINE at
	// the time of the current backup, but may have been PUBLIC at some time in
	// between.
	for _, rev := range revs {
		rawTable, _, _, _, _ := descpb.FromDescriptor(rev.Desc)
		if rawTable == nil {
			continue
		}
		table := tabledesc.NewBuilder(rawTable).BuildImmutableTable()
		if _, wasOffline := offlineInLastBackup[table.GetID()]; wasOffline && table.Public() {
			tablesToReinclude = append(tablesToReinclude, table)
			reintroducedTables[table.GetID()] = struct{}{}
		}
	}

	// All revisions of the table that we're re-introducing must also be
	// considered.
	allRevs := make([]backuppb.BackupManifest_DescriptorRevision, 0, len(revs))
	for _, rev := range revs {
		rawTable, _, _, _, _ := descpb.FromDescriptor(rev.Desc)
		if rawTable == nil {
			continue
		}
		if _, ok := reintroducedTables[rawTable.GetID()]; ok {
			allRevs = append(allRevs, rev)
		}
	}

	tableSpans, err := spansForAllTableIndexes(execCfg, tablesToReinclude, allRevs)
	if err != nil {
		return nil, err
	}
	return tableSpans, nil
}

func getProtectedTimestampTargetForBackup(backupManifest backuppb.BackupManifest) *ptpb.Target {
	if backupManifest.DescriptorCoverage == tree.AllDescriptors {
		return ptpb.MakeClusterTarget()
	}

	if len(backupManifest.Tenants) > 0 {
		tenantID := make([]roachpb.TenantID, 0)
		for _, tenant := range backupManifest.Tenants {
			tenantID = append(tenantID, roachpb.MakeTenantID(tenant.ID))
		}
		return ptpb.MakeTenantsTarget(tenantID)
	}

	// ResolvedCompleteDBs contains all the "complete" databases being backed up.
	//
	// This includes explicit `BACKUP DATABASE` targets as well as expansions as a
	// result of `BACKUP TABLE db.*`. In both cases we want to write a protected
	// timestamp record that covers the entire database.
	if len(backupManifest.CompleteDbs) > 0 {
		return ptpb.MakeSchemaObjectsTarget(backupManifest.CompleteDbs)
	}

	// At this point we are dealing with a `BACKUP TABLE`, so we write a protected
	// timestamp record on each table being backed up.
	tableIDs := make(descpb.IDs, 0)
	for _, desc := range backupManifest.Descriptors {
		t, _, _, _, _ := descpb.FromDescriptorWithMVCCTimestamp(&desc, hlc.Timestamp{})
		if t != nil {
			tableIDs = append(tableIDs, t.GetID())
		}
	}
	return ptpb.MakeSchemaObjectsTarget(tableIDs)
}

func protectTimestampForBackup(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	txn *kv.Txn,
	jobID jobspb.JobID,
	backupManifest backuppb.BackupManifest,
	backupDetails jobspb.BackupDetails,
) error {
	tsToProtect := backupManifest.EndTime
	if !backupManifest.StartTime.IsEmpty() {
		tsToProtect = backupManifest.StartTime
	}

	// Resolve the target that the PTS record will protect as part of this
	// backup.
	target := getProtectedTimestampTargetForBackup(backupManifest)

	// Records written by the backup job should be ignored when making GC
	// decisions on any table that has been marked as
	// `exclude_data_from_backup`. This ensures that the backup job does not
	// holdup GC on that table span for the duration of execution.
	target.IgnoreIfExcludedFromBackup = true
	rec := jobsprotectedts.MakeRecord(*backupDetails.ProtectedTimestampRecord, int64(jobID),
		tsToProtect, backupManifest.Spans, jobsprotectedts.Jobs, target)
	return execCfg.ProtectedTimestampProvider.Protect(ctx, txn, rec)
}

// checkForNewDatabases returns an error if any new complete databases were
// introduced.
func checkForNewCompleteDatabases(
	targetDescs []catalog.Descriptor, curDBs []descpb.ID, prevDBs map[descpb.ID]struct{},
) error {
	for _, dbID := range curDBs {
		if _, inPrevious := prevDBs[dbID]; !inPrevious {
			// Search for the name for a nicer error message.
			violatingDatabase := strconv.Itoa(int(dbID))
			for _, desc := range targetDescs {
				if desc.GetID() == dbID {
					violatingDatabase = desc.GetName()
					break
				}
			}
			return errors.Errorf("previous backup does not contain the complete database %q",
				violatingDatabase)
		}
	}
	return nil
}

// checkForNewTables returns an error if any new tables were introduced with the
// following exceptions:
// 1. A previous backup contained the entire DB.
// 2. The table was truncated after a previous backup was taken, so it's ID has
// changed.
func checkForNewTables(
	ctx context.Context,
	codec keys.SQLCodec,
	db *kv.DB,
	targetDescs []catalog.Descriptor,
	tablesInPrev map[descpb.ID]struct{},
	dbsInPrev map[descpb.ID]struct{},
	priorIDs map[descpb.ID]descpb.ID,
	startTime hlc.Timestamp,
	endTime hlc.Timestamp,
) error {
	for _, d := range targetDescs {
		t, ok := d.(catalog.TableDescriptor)
		if !ok {
			continue
		}
		// If we're trying to use a previous backup for this table, ideally it
		// actually contains this table.
		if _, ok := tablesInPrev[t.GetID()]; ok {
			continue
		}
		// This table isn't in the previous backup... maybe was added to a
		// DB that the previous backup captured?
		if _, ok := dbsInPrev[t.GetParentID()]; ok {
			continue
		}
		// Maybe this table is missing from the previous backup because it was
		// truncated?
		if replacement := t.GetReplacementOf(); replacement.ID != descpb.InvalidID {
			// Check if we need to lazy-load the priorIDs (i.e. if this is the first
			// truncate we've encountered in non-MVCC backup).
			if priorIDs == nil {
				priorIDs = make(map[descpb.ID]descpb.ID)
				_, err := getAllDescChanges(ctx, codec, db, startTime, endTime, priorIDs)
				if err != nil {
					return err
				}
			}
			found := false
			for was := replacement.ID; was != descpb.InvalidID && !found; was = priorIDs[was] {
				_, found = tablesInPrev[was]
			}
			if found {
				continue
			}
		}
		return errors.Errorf("previous backup does not contain table %q", t.GetName())
	}
	return nil
}

func getBackupDetailAndManifest(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	txn *kv.Txn,
	initialDetails jobspb.BackupDetails,
	user username.SQLUsername,
) (jobspb.BackupDetails, backuppb.BackupManifest, error) {
	makeCloudStorage := execCfg.DistSQLSrv.ExternalStorageFromURI
	// TODO(pbardea): Refactor (defaultURI and urisByLocalityKV) pairs into a
	// backupDestination struct.
	collectionURI, defaultURI, resolvedSubdir, urisByLocalityKV, prevs, err :=
		backupdest.ResolveDest(ctx, user, initialDetails.Destination, initialDetails.EndTime, initialDetails.IncrementalFrom, execCfg)
	if err != nil {
		return jobspb.BackupDetails{}, backuppb.BackupManifest{}, err
	}

	defaultStore, err := execCfg.DistSQLSrv.ExternalStorageFromURI(ctx, defaultURI, user)
	if err != nil {
		return jobspb.BackupDetails{}, backuppb.BackupManifest{}, err
	}
	defer defaultStore.Close()

	if err := backupinfo.CheckForPreviousBackup(ctx, defaultStore, defaultURI); err != nil {
		return jobspb.BackupDetails{}, backuppb.BackupManifest{}, err
	}

	kmsEnv := backupencryption.MakeBackupKMSEnv(execCfg.Settings, &execCfg.ExternalIODirConfig,
		execCfg.DB, user, execCfg.InternalExecutor)

	mem := execCfg.RootMemoryMonitor.MakeBoundAccount()
	defer mem.Close(ctx)

	prevBackups, encryptionOptions, memSize, err := backupinfo.FetchPreviousBackups(ctx, &mem, user,
		makeCloudStorage, prevs, *initialDetails.EncryptionOptions, &kmsEnv)

	if err != nil {
		return jobspb.BackupDetails{}, backuppb.BackupManifest{}, err
	}
	defer func() {
		mem.Shrink(ctx, memSize)
	}()

	if len(prevBackups) > 0 {
		baseManifest := prevBackups[0]
		if baseManifest.DescriptorCoverage == tree.AllDescriptors &&
			!initialDetails.FullCluster {
			return jobspb.BackupDetails{}, backuppb.BackupManifest{}, errors.Errorf("cannot append a backup of specific tables or databases to a cluster backup")
		}

		if err := requireEnterprise(execCfg, "incremental"); err != nil {
			return jobspb.BackupDetails{}, backuppb.BackupManifest{}, err
		}
		lastEndTime := prevBackups[len(prevBackups)-1].EndTime
		if lastEndTime.Compare(initialDetails.EndTime) > 0 {
			return jobspb.BackupDetails{}, backuppb.BackupManifest{},
				errors.Newf("`AS OF SYSTEM TIME` %s must be greater than "+
					"the previous backup's end time of %s.",
					initialDetails.EndTime.GoTime(), lastEndTime.GoTime())
		}
	}

	localityKVs := make([]string, len(urisByLocalityKV))
	i := 0
	for k := range urisByLocalityKV {
		localityKVs[i] = k
		i++
	}

	for i := range prevBackups {
		prevBackup := prevBackups[i]
		// IDs are how we identify tables, and those are only meaningful in the
		// context of their own cluster, so we need to ensure we only allow
		// incremental previous backups that we created.
		if fromCluster := prevBackup.ClusterID; !fromCluster.Equal(execCfg.NodeInfo.LogicalClusterID()) {
			return jobspb.BackupDetails{}, backuppb.BackupManifest{}, errors.Newf("previous BACKUP belongs to cluster %s", fromCluster.String())
		}

		prevLocalityKVs := prevBackup.LocalityKVs

		// Checks that each layer in the backup uses the same localities
		// Does NOT check that each locality/layer combination is actually at the
		// expected locations.
		// This is complex right now, but should be easier shortly.
		// TODO(benbardin): Support verifying actual existence of localities for
		// each layer after deprecating TO-syntax in 22.2
		sort.Strings(localityKVs)
		sort.Strings(prevLocalityKVs)
		if !(len(localityKVs) == 0 && len(prevLocalityKVs) == 0) && !reflect.DeepEqual(localityKVs,
			prevLocalityKVs) {
			// Note that this won't verify the default locality. That's not
			// necessary, because the default locality defines the backup manifest
			// location. If that URI isn't right, the backup chain will fail to
			// load.
			return jobspb.BackupDetails{}, backuppb.BackupManifest{}, errors.Newf(
				"Requested backup has localities %s, but a previous backup layer in this collection has localities %s. "+
					"Mismatched backup layers are not supported. Please take a new full backup with the new localities, or an "+
					"incremental backup with matching localities.",
				localityKVs, prevLocalityKVs,
			)
		}
	}

	// updatedDetails and backupManifest should be treated as read-only after
	// they're returned from their respective functions. Future changes to those
	// objects should be made within those functions.
	updatedDetails, err := updateBackupDetails(
		ctx,
		initialDetails,
		collectionURI,
		defaultURI,
		resolvedSubdir,
		urisByLocalityKV,
		prevBackups,
		encryptionOptions,
		&kmsEnv)
	if err != nil {
		return jobspb.BackupDetails{}, backuppb.BackupManifest{}, err
	}

	backupManifest, err := createBackupManifest(
		ctx,
		execCfg,
		txn,
		updatedDetails,
		prevBackups)
	if err != nil {
		return jobspb.BackupDetails{}, backuppb.BackupManifest{}, err
	}

	return updatedDetails, backupManifest, nil
}

func getTenantInfo(
	ctx context.Context, execCfg *sql.ExecutorConfig, txn *kv.Txn, jobDetails jobspb.BackupDetails,
) ([]roachpb.Span, []descpb.TenantInfoWithUsage, error) {
	var spans []roachpb.Span
	var tenants []descpb.TenantInfoWithUsage
	var err error
	if jobDetails.FullCluster && execCfg.Codec.ForSystemTenant() {
		// Include all tenants.
		tenants, err = retrieveAllTenantsMetadata(
			ctx, execCfg.InternalExecutor, txn,
		)
		if err != nil {
			return nil, nil, err
		}
	} else if len(jobDetails.SpecificTenantIds) > 0 {
		for _, id := range jobDetails.SpecificTenantIds {
			tenantInfo, err := retrieveSingleTenantMetadata(
				ctx, execCfg.InternalExecutor, txn, id,
			)
			if err != nil {
				return nil, nil, err
			}
			tenants = append(tenants, tenantInfo)
		}
	}
	if len(tenants) > 0 && jobDetails.RevisionHistory {
		return spans, tenants, errors.UnimplementedError(
			errors.IssueLink{IssueURL: "https://github.com/cockroachdb/cockroach/issues/47896"},
			"can not backup tenants with revision history",
		)
	}
	for i := range tenants {
		prefix := keys.MakeTenantPrefix(roachpb.MakeTenantID(tenants[i].ID))
		spans = append(spans, roachpb.Span{Key: prefix, EndKey: prefix.PrefixEnd()})
	}
	return spans, tenants, nil
}

// TODO(adityamaru): We need to move this method into manifest_handling.go but
// the method needs to be decomposed to decouple it from other planning related
// operations.
func createBackupManifest(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	txn *kv.Txn,
	jobDetails jobspb.BackupDetails,
	prevBackups []backuppb.BackupManifest,
) (backuppb.BackupManifest, error) {
	mvccFilter := backuppb.MVCCFilter_Latest
	if jobDetails.RevisionHistory {
		mvccFilter = backuppb.MVCCFilter_All
	}
	endTime := jobDetails.EndTime
	var targetDescs []catalog.Descriptor
	var descriptorProtos []descpb.Descriptor
	var err error
	if jobDetails.FullCluster {
		var err error
		targetDescs, _, err = fullClusterTargetsBackup(ctx, execCfg, endTime)
		if err != nil {
			return backuppb.BackupManifest{}, err
		}
		descriptorProtos = make([]descpb.Descriptor, len(targetDescs))
		for i, desc := range targetDescs {
			descriptorProtos[i] = *desc.DescriptorProto()
		}
	} else {
		descriptorProtos = jobDetails.ResolvedTargets
		targetDescs = make([]catalog.Descriptor, len(descriptorProtos))
		for i := range descriptorProtos {
			targetDescs[i] = descbuilder.NewBuilder(&descriptorProtos[i]).BuildExistingMutable()
		}
	}

	startTime := jobDetails.StartTime

	var tables []catalog.TableDescriptor
	statsFiles := make(map[descpb.ID]string)
	for _, desc := range targetDescs {
		switch desc := desc.(type) {
		case catalog.TableDescriptor:
			tables = append(tables, desc)
			// TODO (anzo): look into the tradeoffs of having all objects in the array to be in the same file,
			// vs having each object in a separate file, or somewhere in between.
			statsFiles[desc.GetID()] = backupinfo.BackupStatisticsFileName
		}
	}

	var newSpans roachpb.Spans
	var priorIDs map[descpb.ID]descpb.ID

	var revs []backuppb.BackupManifest_DescriptorRevision
	if mvccFilter == backuppb.MVCCFilter_All {
		priorIDs = make(map[descpb.ID]descpb.ID)
		revs, err = getRelevantDescChanges(ctx, execCfg, startTime, endTime, targetDescs,
			jobDetails.ResolvedCompleteDbs, priorIDs, jobDetails.FullCluster)
		if err != nil {
			return backuppb.BackupManifest{}, err
		}
	}

	var spans []roachpb.Span
	var tenants []descpb.TenantInfoWithUsage
	tenantSpans, tenantInfos, err := getTenantInfo(
		ctx, execCfg, txn, jobDetails,
	)
	if err != nil {
		return backuppb.BackupManifest{}, err
	}
	spans = append(spans, tenantSpans...)
	tenants = append(tenants, tenantInfos...)

	tableSpans, err := spansForAllTableIndexes(execCfg, tables, revs)
	if err != nil {
		return backuppb.BackupManifest{}, err
	}
	spans = append(spans, tableSpans...)

	if len(prevBackups) > 0 {
		tablesInPrev := make(map[descpb.ID]struct{})
		dbsInPrev := make(map[descpb.ID]struct{})
		rawDescs := prevBackups[len(prevBackups)-1].Descriptors
		for i := range rawDescs {
			if t, _, _, _, _ := descpb.FromDescriptor(&rawDescs[i]); t != nil {
				tablesInPrev[t.ID] = struct{}{}
			}
		}
		for _, d := range prevBackups[len(prevBackups)-1].CompleteDbs {
			dbsInPrev[d] = struct{}{}
		}

		if !jobDetails.FullCluster {
			if err := checkForNewTables(ctx, execCfg.Codec, execCfg.DB, targetDescs, tablesInPrev, dbsInPrev, priorIDs, startTime, endTime); err != nil {
				return backuppb.BackupManifest{}, err
			}
			// Let's check that we're not widening the scope of this backup to an
			// entire database, even if no tables were created in the meantime.
			if err := checkForNewCompleteDatabases(targetDescs, jobDetails.ResolvedCompleteDbs, dbsInPrev); err != nil {
				return backuppb.BackupManifest{}, err
			}
		}

		newSpans = filterSpans(spans, prevBackups[len(prevBackups)-1].Spans)

		tableSpans, err := getReintroducedSpans(ctx, execCfg, prevBackups, tables, revs, endTime)
		if err != nil {
			return backuppb.BackupManifest{}, err
		}
		newSpans = append(newSpans, tableSpans...)
	}

	// if CompleteDbs is lost by a 1.x node, FormatDescriptorTrackingVersion
	// means that a 2.0 node will disallow `RESTORE DATABASE foo`, but `RESTORE
	// foo.table1, foo.table2...` will still work. MVCCFilter would be
	// mis-handled, but is disallowed above. IntroducedSpans may also be lost by
	// a 1.x node, meaning that if 1.1 nodes may resume a backup, the limitation
	// of requiring full backups after schema changes remains.

	coverage := tree.RequestedDescriptors
	if jobDetails.FullCluster {
		coverage = tree.AllDescriptors
	}

	backupManifest := backuppb.BackupManifest{
		StartTime:           startTime,
		EndTime:             endTime,
		MVCCFilter:          mvccFilter,
		Descriptors:         descriptorProtos,
		Tenants:             tenants,
		DescriptorChanges:   revs,
		CompleteDbs:         jobDetails.ResolvedCompleteDbs,
		Spans:               spans,
		IntroducedSpans:     newSpans,
		FormatVersion:       backupinfo.BackupFormatDescriptorTrackingVersion,
		BuildInfo:           build.GetInfo(),
		ClusterVersion:      execCfg.Settings.Version.ActiveVersion(ctx).Version,
		ClusterID:           execCfg.NodeInfo.LogicalClusterID(),
		StatisticsFilenames: statsFiles,
		DescriptorCoverage:  coverage,
	}
	if err := checkCoverage(ctx, backupManifest.Spans, append(prevBackups, backupManifest)); err != nil {
		return backuppb.BackupManifest{}, errors.Wrap(err, "new backup would not cover expected time")
	}
	return backupManifest, nil
}

func updateBackupDetails(
	ctx context.Context,
	details jobspb.BackupDetails,
	collectionURI string,
	defaultURI string,
	resolvedSubdir string,
	urisByLocalityKV map[string]string,
	prevBackups []backuppb.BackupManifest,
	encryptionOptions *jobspb.BackupEncryptionOptions,
	kmsEnv *backupencryption.BackupKMSEnv,
) (jobspb.BackupDetails, error) {
	var err error
	var startTime hlc.Timestamp
	if len(prevBackups) > 0 {
		startTime = prevBackups[len(prevBackups)-1].EndTime
	}

	// If we didn't load any prior backups from which get encryption info, we
	// need to generate encryption specific data.
	var encryptionInfo *jobspb.EncryptionInfo
	if encryptionOptions == nil {
		encryptionOptions, encryptionInfo, err = backupencryption.MakeNewEncryptionOptions(ctx, *details.EncryptionOptions, kmsEnv)
		if err != nil {
			return jobspb.BackupDetails{}, err
		}
	}

	details.Destination = jobspb.BackupDetails_Destination{Subdir: resolvedSubdir}
	details.StartTime = startTime
	details.URI = defaultURI
	details.URIsByLocalityKV = urisByLocalityKV
	details.EncryptionOptions = encryptionOptions
	details.EncryptionInfo = encryptionInfo
	details.CollectionURI = collectionURI

	return details, nil
}

func init() {
	sql.AddPlanHook("backup", backupPlanHook)
}
