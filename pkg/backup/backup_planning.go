// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package backup implements backup logic.
package backup

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/backup/backupbase"
	"github.com/cockroachdb/cockroach/pkg/backup/backupresolver"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/featureflag"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/exprutil"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

const (
	// backupPartitionDescriptorPrefix is the file name prefix for serialized
	// BackupPartitionDescriptor protos.
	backupPartitionDescriptorPrefix = "BACKUP_PART"

	deprecatedPrivilegesBackupPreamble = "The existing privileges are being deprecated " +
		"in favour of a fine-grained privilege model explained here " +
		"https://www.cockroachlabs.com/docs/stable/backup.html#required-privileges. In a future release, to run"

	deprecatedPrivilegesRestorePreamble = "The existing privileges are being deprecated " +
		"in favour of a fine-grained privilege model explained here " +
		"https://www.cockroachlabs.com/docs/stable/restore.html#required-privileges. In a future release, to run"
)

type tableAndIndex struct {
	tableID descpb.ID
	indexID descpb.IndexID
}

// featureBackupEnabled is used to enable and disable the BACKUP feature.
var featureBackupEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"feature.backup.enabled",
	"set to true to enable backups, false to disable; default is true",
	featureflag.FeatureFlagEnabledDefault,
	settings.WithPublic)

func resolveOptionsForBackupJobDescription(
	opts tree.BackupOptions, kmsURIs []string, incrementalStorage []string,
) (tree.BackupOptions, error) {
	if opts.IsDefault() {
		return opts, nil
	}

	newOpts := tree.BackupOptions{
		CaptureRevisionHistory:          opts.CaptureRevisionHistory,
		IncludeAllSecondaryTenants:      opts.IncludeAllSecondaryTenants,
		Detached:                        opts.Detached,
		ExecutionLocality:               opts.ExecutionLocality,
		UpdatesClusterMonitoringMetrics: opts.UpdatesClusterMonitoringMetrics,
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
	kmsURIs []string,
	resolvedSubdir string,
	incrementalStorage []string,
	hasBeenPlanned bool,
) (*tree.Backup, error) {
	b := &tree.Backup{
		AsOf:           backup.AsOf,
		Targets:        backup.Targets,
		AppendToLatest: backup.AppendToLatest,
	}

	// We set Subdir to the directory resolved during BACKUP planning.
	//
	// - For `BACKUP INTO 'subdir' IN...` this would be the specified subdir
	// (with a single / prefix).
	// - For `BACKUP INTO LATEST...` this would be the sub-directory pointed to by
	// LATEST, where we are appending an incremental BACKUP.
	// - For `BACKUP INTO x` this would be the sub-directory we have selected to
	// write the BACKUP to.
	if hasBeenPlanned {
		b.Subdir = tree.NewDString(resolvedSubdir)
	}

	var err error
	b.To, err = sanitizeURIList(to)
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
	kmsURIs []string,
	resolvedSubdir string,
	incrementalStorage []string,
) (string, error) {
	b, err := GetRedactedBackupNode(backup, to, kmsURIs,
		resolvedSubdir, incrementalStorage, true /* hasBeenPlanned */)
	if err != nil {
		return "", err
	}

	ann := p.ExtendedEvalContext().Annotations
	return tree.AsStringWithFlags(
		b, tree.FmtAlwaysQualifyNames|tree.FmtShowFullURIs, tree.FmtAnnotations(ann),
	), nil
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

// backupPrivilegesDeprecationNotice returns a notice that outlines the
// deprecation of the existing privilege model for backups, and the steps to
// take to adopt the new privilege model, based on the type of backup being run.
func backupPrivilegesDeprecationNotice(
	p sql.PlanHookState, backupStmt *annotatedBackupStatement, targetDescs []catalog.Descriptor,
) string {
	if backupStmt.Targets == nil {
		return ""
	}

	// If a user is running `BACKUP DATABASE` buffer all the databases that will
	// require the `BACKUP` privilege.
	var notice string
	if backupStmt.Targets.Databases != nil {
		dbsRequireBackupPrivilege := make([]string, 0)
		for _, desc := range targetDescs {
			switch desc := desc.(type) {
			case catalog.DatabaseDescriptor:
				dbsRequireBackupPrivilege = append(dbsRequireBackupPrivilege, desc.GetName())
			}
		}

		notice = fmt.Sprintf("%s BACKUP DATABASE, user %s will exclusively require the "+
			"BACKUP privilege on database %s.",
			deprecatedPrivilegesBackupPreamble, p.User().Normalized(), strings.Join(dbsRequireBackupPrivilege, ", "))
	} else if backupStmt.Targets.Tables.TablePatterns != nil {
		// If a user is running `BACKUP TABLE` buffer all the tables that will require the `BACKUP` privilege.
		tablesRequireBackupPrivilege := make([]string, 0)
		for _, desc := range targetDescs {
			switch desc := desc.(type) {
			case catalog.TableDescriptor:
				tablesRequireBackupPrivilege = append(tablesRequireBackupPrivilege, desc.GetName())
			}
		}

		notice = fmt.Sprintf("%s BACKUP TABLE, user %s will exclusively require the "+
			"BACKUP privilege on tables: %s.",
			deprecatedPrivilegesBackupPreamble,
			p.User().Normalized(),
			strings.Join(tablesRequireBackupPrivilege, ", "))
	}

	return notice
}

// checkPrivilegesForBackup is the driver method for privilege checks for all
// flavours of backup. It checks that the user has sufficient privileges to read
// the targets in the database, as well as use the External Storage URIs passed
// in the backup statement.
func checkPrivilegesForBackup(
	ctx context.Context,
	backupStmt *annotatedBackupStatement,
	p sql.PlanHookState,
	targetDescs []catalog.Descriptor,
	to []string,
) error {
	// If the user is admin no further checks need to be performed.
	hasAdmin, err := p.HasAdminRole(ctx)
	if err != nil {
		return err
	}
	if hasAdmin {
		return nil
	}

	{
		// Cluster and tenant backups require the `BACKUP` system privilege.
		requiresBackupSystemPrivilege := backupStmt.Coverage() == tree.AllDescriptors ||
			(backupStmt.Targets != nil && backupStmt.Targets.TenantID.IsSet())

		if requiresBackupSystemPrivilege {
			if err := p.CheckPrivilegeForUser(
				ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.BACKUP, p.User(),
			); err != nil {
				return pgerror.Wrapf(
					err,
					pgcode.InsufficientPrivilege,
					"only users with the admin role or the BACKUP system privilege are allowed to perform full cluster backups")
			}
			return sql.CheckDestinationPrivileges(ctx, p, to)
		}
	}

	// Target is only nil in cluster backups for which we have already checked
	// privileges above.
	if backupStmt.Targets == nil {
		return errors.AssertionFailedf("programming error: target should be non-nil " +
			"for database and table backups")
	}

	hasRequiredBackupPrivileges := true
	// If running a database backup, check that the user has the `BACKUP`
	// privilege on all the target databases.
	//
	// TODO(adityamaru): In 23.1 a missing `BACKUP` privilege should return an
	// error. In 22.2 we continue to check for old style privileges on the
	// descriptors.
	if backupStmt.Targets.Databases != nil {
		for _, desc := range targetDescs {
			switch desc := desc.(type) {
			case catalog.DatabaseDescriptor:
				if hasRequiredBackupPrivileges {
					if ok, err := p.HasPrivilege(ctx, desc, privilege.BACKUP, p.User()); err != nil {
						return err
					} else {
						hasRequiredBackupPrivileges = ok
					}
				}
			}
		}
	} else if backupStmt.Targets.Tables.TablePatterns != nil {
		// If running a table backup, check that the user has the `BACKUP` privilege
		// on all the target tables.
		//
		// TODO(adityamaru): In 23.1 a missing `BACKUP` privilege should return an
		// error. In 22.2 we continue to check for old style privileges on the
		// descriptors.
		for _, desc := range targetDescs {
			switch desc := desc.(type) {
			case catalog.TableDescriptor:
				if hasRequiredBackupPrivileges {
					if ok, err := p.HasPrivilege(ctx, desc, privilege.BACKUP, p.User()); err != nil {
						return err
					} else {
						hasRequiredBackupPrivileges = ok
					}
				}
			}
		}
	} else {
		return errors.AssertionFailedf("unknown backup target")
	}

	// If a user has the BACKUP privilege on the target databases or tables we can
	// move on to checking the destination URIs.
	if hasRequiredBackupPrivileges {
		return sql.CheckDestinationPrivileges(ctx, p, to)
	}

	// The following checks are to maintain compatability with pre-22.2 privilege
	// requirements to run the backup. If we have failed to find the appropriate
	// `BACKUP` privileges, we default to our old-style privilege checks and buffer
	// a notice urging users to switch to `BACKUP` privileges.
	//
	// TODO(adityamaru): Delete deprecated privilege checks in 23.1. Users will be
	// required to have the appropriate `BACKUP` privilege instead.
	notice := backupPrivilegesDeprecationNotice(p, backupStmt, targetDescs)
	p.BufferClientNotice(ctx, pgnotice.Newf("%s", notice))

	for _, desc := range targetDescs {
		switch desc := desc.(type) {
		case catalog.DatabaseDescriptor:
			if err := p.CheckPrivilege(ctx, desc, privilege.CONNECT); err != nil {
				return errors.WithHint(err, notice)
			}
		case catalog.TableDescriptor:
			if err := p.CheckPrivilege(ctx, desc, privilege.SELECT); err != nil {
				return errors.WithHint(err, notice)
			}
		case catalog.TypeDescriptor, catalog.SchemaDescriptor:
			if err := p.CheckPrivilege(ctx, desc, privilege.USAGE); err != nil {
				return errors.WithHint(err, notice)
			}
		case catalog.FunctionDescriptor:
			if err := p.CheckPrivilege(ctx, desc, privilege.EXECUTE); err != nil {
				return errors.WithHint(err, notice)
			}
		}
	}

	return sql.CheckDestinationPrivileges(ctx, p, to)
}

func backupTypeCheck(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (matched bool, header colinfo.ResultColumns, _ error) {
	backupStmt := getBackupStatement(stmt)
	if backupStmt == nil {
		return false, nil, nil
	}
	detached := backupStmt.Options.Detached == tree.DBoolTrue
	if detached {
		header = jobs.DetachedJobExecutionResultHeader
	} else {
		header = jobs.BackupRestoreJobResultHeader
	}
	if err := exprutil.TypeCheck(
		ctx, "BACKUP", p.SemaCtx(),
		exprutil.Strings{
			backupStmt.Subdir,
			backupStmt.Options.EncryptionPassphrase,
			backupStmt.Options.ExecutionLocality,
		},
		exprutil.StringArrays{
			tree.Exprs(backupStmt.To),
			tree.Exprs(backupStmt.Options.IncrementalStorage),
			tree.Exprs(backupStmt.Options.EncryptionKMSURI),
		},
		exprutil.Bools{
			backupStmt.Options.CaptureRevisionHistory,
			backupStmt.Options.IncludeAllSecondaryTenants,
			backupStmt.Options.UpdatesClusterMonitoringMetrics,
		}); err != nil {
		return false, nil, err
	}
	return true, header, nil
}

// backupPlanHook implements PlanHookFn.
func backupPlanHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, colinfo.ResultColumns, bool, error) {
	backupStmt := getBackupStatement(stmt)
	if backupStmt == nil {
		return nil, nil, false, nil
	}
	if err := featureflag.CheckEnabled(
		ctx,
		p.ExecCfg(),
		featureBackupEnabled,
		"BACKUP",
	); err != nil {
		return nil, nil, false, err
	}

	detached := backupStmt.Options.Detached == tree.DBoolTrue

	exprEval := p.ExprEvaluator("BACKUP")

	var err error
	var subdir string
	if backupStmt.Subdir != nil {
		subdir, err = exprEval.String(ctx, backupStmt.Subdir)
		if err != nil {
			return nil, nil, false, err
		}
	}

	to, err := exprEval.StringArray(ctx, tree.Exprs(backupStmt.To))
	if err != nil {
		return nil, nil, false, err
	}

	incrementalStorage, err := exprEval.StringArray(
		ctx, tree.Exprs(backupStmt.Options.IncrementalStorage),
	)
	if err != nil {
		return nil, nil, false, err
	}

	var revisionHistory bool
	if backupStmt.Options.CaptureRevisionHistory != nil {
		revisionHistory, err = exprEval.Bool(
			ctx, backupStmt.Options.CaptureRevisionHistory,
		)
		if err != nil {
			return nil, nil, false, err
		}
	}

	var executionLocality roachpb.Locality
	if backupStmt.Options.ExecutionLocality != nil {
		s, err := exprEval.String(ctx, backupStmt.Options.ExecutionLocality)
		if err != nil {
			return nil, nil, false, err
		}
		if s != "" {
			if err := executionLocality.Set(s); err != nil {
				return nil, nil, false, err
			}
		}
	}

	var includeAllSecondaryTenants bool
	if backupStmt.Options.IncludeAllSecondaryTenants != nil {
		includeAllSecondaryTenants, err = exprEval.Bool(
			ctx, backupStmt.Options.IncludeAllSecondaryTenants,
		)
		if err != nil {
			return nil, nil, false, err
		}
	}

	encryptionParams := jobspb.BackupEncryptionOptions{
		Mode: jobspb.EncryptionMode_None,
	}

	var pw string
	if backupStmt.Options.EncryptionPassphrase != nil {
		pw, err = exprEval.String(ctx, backupStmt.Options.EncryptionPassphrase)
		if err != nil {
			return nil, nil, false, err
		}
		encryptionParams.Mode = jobspb.EncryptionMode_Passphrase
	}

	var kms []string
	if backupStmt.Options.EncryptionKMSURI != nil {
		if encryptionParams.Mode != jobspb.EncryptionMode_None {
			return nil, nil, false,
				errors.New("cannot have both encryption_passphrase and kms option set")
		}
		kms, err = exprEval.StringArray(
			ctx, tree.Exprs(backupStmt.Options.EncryptionKMSURI),
		)
		if err != nil {
			return nil, nil, false, err
		}
		encryptionParams.Mode = jobspb.EncryptionMode_KMS
		if err = logAndSanitizeKmsURIs(ctx, kms...); err != nil {
			return nil, nil, false, err
		}
	}

	var updatesClusterMonitoringMetrics bool
	if backupStmt.Options.UpdatesClusterMonitoringMetrics != nil {
		updatesClusterMonitoringMetrics, err = exprEval.Bool(
			ctx, backupStmt.Options.UpdatesClusterMonitoringMetrics,
		)
		if err != nil {
			return nil, nil, false, err
		}
	}

	fn := func(ctx context.Context, resultsCh chan<- tree.Datums) error {
		// TODO(dan): Move this span into sql.
		ctx, span := tracing.ChildSpan(ctx, stmt.StatementTag())
		defer span.Finish()

		if !(p.ExtendedEvalContext().TxnIsSingleStmt || detached) {
			return errors.Errorf("BACKUP cannot be used inside a multi-statement transaction without DETACHED option")
		}

		if len(incrementalStorage) > 0 && (len(incrementalStorage) != len(to)) {
			return errors.New("the incremental_location option must contain the same number of locality" +
				" aware URIs as the full backup destination")
		}

		if includeAllSecondaryTenants && backupStmt.Coverage() != tree.AllDescriptors {
			return errors.New("the include_all_virtual_clusters option is only supported for full cluster backups")
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
			encryptionParams.RawPassphrase = pw
		case jobspb.EncryptionMode_KMS:
			encryptionParams.RawKmsUris = kms
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

		for _, t := range targetDescs {
			if tbl, ok := t.(catalog.TableDescriptor); ok && tbl.ExternalRowData() != nil {
				if tbl.ExternalRowData().TenantID.IsSet() {
					return errors.UnimplementedError(errors.IssueLink{}, "backing up from a replication target is not supported")
				}
				return errors.UnimplementedError(errors.IssueLink{}, "backing up from external tables is not supported")
			}
		}

		// Check BACKUP privileges.
		err = checkPrivilegesForBackup(ctx, backupStmt, p, targetDescs, to)
		if err != nil {
			return err
		}

		// Check that a node will currently be able to run this before we create it.
		if executionLocality.NonEmpty() {
			if _, err := p.DistSQLPlanner().GetAllInstancesByLocality(ctx, executionLocality); err != nil {
				return err
			}
		}

		initialDetails := jobspb.BackupDetails{
			Destination:                     jobspb.BackupDetails_Destination{To: to, IncrementalStorage: incrementalStorage},
			EndTime:                         endTime,
			RevisionHistory:                 revisionHistory,
			IncludeAllSecondaryTenants:      includeAllSecondaryTenants,
			FullCluster:                     backupStmt.Coverage() == tree.AllDescriptors,
			ResolvedCompleteDbs:             completeDBs,
			EncryptionOptions:               &encryptionParams,
			AsOfInterval:                    asOfInterval,
			Detached:                        detached,
			ApplicationName:                 p.SessionData().ApplicationName,
			ExecutionLocality:               executionLocality,
			UpdatesClusterMonitoringMetrics: updatesClusterMonitoringMetrics,
		}
		if backupStmt.CreatedByInfo != nil {
			initialDetails.ScheduleID = backupStmt.CreatedByInfo.ScheduleID()
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

		if backupStmt.AppendToLatest {
			initialDetails.Destination.Subdir = backupbase.LatestFileName
			initialDetails.Destination.Exists = true
		} else if subdir != "" {
			initialDetails.Destination.Subdir = "/" + strings.TrimPrefix(subdir, "/")
			initialDetails.Destination.Exists = true
		} else {
			initialDetails.Destination.Subdir = endTime.GoTime().Format(backupbase.DateBasedIntoFolderName)
		}

		if backupStmt.Targets != nil && backupStmt.Targets.TenantID.IsSet() {
			if !p.ExecCfg().Codec.ForSystemTenant() {
				return pgerror.Newf(pgcode.InsufficientPrivilege, "only the system tenant can backup other tenants")
			}
			tid, err := roachpb.MakeTenantID(backupStmt.Targets.TenantID.ID)
			if err != nil {
				return err
			}
			initialDetails.SpecificTenantIds = []roachpb.TenantID{tid}
		}

		jobID := p.ExecCfg().JobRegistry.MakeJobID()

		if err := logAndSanitizeBackupDestinations(ctx, to...); err != nil {
			return errors.Wrap(err, "logging backup destinations")
		}

		description, err := backupJobDescription(p,
			backupStmt.Backup, to,
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
				ctx, jr, jobID, p.InternalSQLTxn())
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
			if err := p.ExecCfg().JobRegistry.CreateStartableJobWithTxn(
				ctx, &sj, jobID, p.InternalSQLTxn(), jr,
			); err != nil {
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
		// Release all descriptor leases here. Note that we committed the
		// underlying transaction in the above closure -- so we're not using any
		// leases anymore, but we might be holding some because some sql queries
		// might have been executed by this transaction (indeed some certainly
		// were when we created the job we're going to run).
		//
		// This call is not strictly necessary, but it's parallel to the other
		// locations where we commit a transaction and wait for a job; it seems
		// best to release leases we don't need.
		p.InternalSQLTxn().Descriptors().ReleaseAll(ctx)
		if err := sj.Start(ctx); err != nil {
			return err
		}
		if err := sj.AwaitCompletion(ctx); err != nil {
			return err
		}
		return sj.ReportExecutionResults(ctx, resultsCh)
	}

	if detached {
		return fn, jobs.DetachedJobExecutionResultHeader, false, nil
	}
	return fn, jobs.BackupRestoreJobResultHeader, false, nil
}

func logAndSanitizeKmsURIs(ctx context.Context, kmsURIs ...string) error {
	for _, dest := range kmsURIs {
		clean, err := cloud.RedactKMSURI(dest)
		if err != nil {
			return err
		}
		log.Ops.Infof(ctx, "backup planning to connect to KMS destination %v", redact.Safe(clean))
	}
	return nil
}

func logAndSanitizeBackupDestinations(ctx context.Context, backupDestinations ...string) error {
	for _, dest := range backupDestinations {
		clean, err := cloud.SanitizeExternalStorageURI(dest, nil)
		if err != nil {
			return err
		}
		log.Ops.Infof(ctx, "backup planning to connect to destination %v", redact.Safe(clean))
	}
	return nil
}

func init() {
	sql.AddPlanHook(
		"backup.backupPlanHook",
		backupPlanHook,
		backupTypeCheck,
	)
}
