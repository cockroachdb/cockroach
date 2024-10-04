// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package backupccl implements backup logic.
package backupccl

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backupbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backupencryption"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backupinfo"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backuppb"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backupresolver"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudprivilege"
	"github.com/cockroachdb/cockroach/pkg/featureflag"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/exprutil"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
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
	"github.com/cockroachdb/redact"
	pbtypes "github.com/gogo/protobuf/types"
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

// includeTableSpans returns true if the backup should include spans for the
// given table descriptor.
func includeTableSpans(table *descpb.TableDescriptor) bool {
	// We do not backup spans for views here as they do not contain data.
	//
	// Additionally, because we do not split ranges at view boundaries, it is
	// possible that the range the view span belongs to is shared by another
	// object in the cluster (that we may or may not be backing up) that might
	// have its own bespoke zone configurations, namely one with a short GC TTL.
	// This could lead to a situation where the short GC TTL on the range we are
	// not backing up causes our protectedts verification to fail when attempting
	// to backup the view span.
	return table.IsPhysicalTable()
}

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
	if !includeTableSpans(table) {
		return
	}

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
		rawTbl, _, _, _, _ := descpb.GetDescriptors(rev.Desc)
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
	incrementalFrom []string,
	kmsURIs []string,
	resolvedSubdir string,
	incrementalStorage []string,
	hasBeenPlanned bool,
) (*tree.Backup, error) {
	b := &tree.Backup{
		AsOf:           backup.AsOf,
		Targets:        backup.Targets,
		Nested:         backup.Nested,
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
	return tree.AsStringWithFlags(
		b, tree.FmtAlwaysQualifyTableNames|tree.FmtShowFullURIs, tree.FmtAnnotations(ann),
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
			return cloudprivilege.CheckDestinationPrivileges(ctx, p, to)
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
		return cloudprivilege.CheckDestinationPrivileges(ctx, p, to)
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

	return cloudprivilege.CheckDestinationPrivileges(ctx, p, to)
}

func requireEnterprise(execCfg *sql.ExecutorConfig, feature string) error {
	if err := utilccl.CheckEnterpriseEnabled(
		execCfg.Settings, execCfg.NodeInfo.LogicalClusterID(),
		fmt.Sprintf("BACKUP with %s", feature),
	); err != nil {
		return err
	}
	return nil
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
		header = jobs.BulkJobExecutionResultHeader
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
			backupStmt.IncrementalFrom,
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

	detached := backupStmt.Options.Detached == tree.DBoolTrue

	// Deprecation notice for `BACKUP TO` syntax. Remove this once the syntax is
	// deleted in 22.2.
	if !backupStmt.Nested {
		p.BufferClientNotice(ctx,
			pgnotice.Newf("The `BACKUP TO` syntax will be removed in a future release, please"+
				" switch over to using `BACKUP INTO` to create a backup collection: %s. "+
				"Backups created using the `BACKUP TO` syntax may not be restoreable in the next major version release.",
				"https://www.cockroachlabs.com/docs/stable/backup.html#considerations"))
	}

	exprEval := p.ExprEvaluator("BACKUP")

	var err error
	var subdir string
	if backupStmt.Subdir != nil {
		subdir, err = exprEval.String(ctx, backupStmt.Subdir)
		if err != nil {
			return nil, nil, nil, false, err
		}
	}

	to, err := exprEval.StringArray(ctx, tree.Exprs(backupStmt.To))
	if err != nil {
		return nil, nil, nil, false, err
	}
	incrementalFrom, err := exprEval.StringArray(ctx, backupStmt.IncrementalFrom)
	if err != nil {
		return nil, nil, nil, false, err
	}

	incrementalStorage, err := exprEval.StringArray(
		ctx, tree.Exprs(backupStmt.Options.IncrementalStorage),
	)
	if err != nil {
		return nil, nil, nil, false, err
	}

	var revisionHistory bool
	if backupStmt.Options.CaptureRevisionHistory != nil {
		revisionHistory, err = exprEval.Bool(
			ctx, backupStmt.Options.CaptureRevisionHistory,
		)
		if err != nil {
			return nil, nil, nil, false, err
		}
	}

	var executionLocality roachpb.Locality
	if backupStmt.Options.ExecutionLocality != nil {
		s, err := exprEval.String(ctx, backupStmt.Options.ExecutionLocality)
		if err != nil {
			return nil, nil, nil, false, err
		}
		if s != "" {
			if err := executionLocality.Set(s); err != nil {
				return nil, nil, nil, false, err
			}
		}
	}

	var includeAllSecondaryTenants bool
	if backupStmt.Options.IncludeAllSecondaryTenants != nil {
		includeAllSecondaryTenants, err = exprEval.Bool(
			ctx, backupStmt.Options.IncludeAllSecondaryTenants,
		)
		if err != nil {
			return nil, nil, nil, false, err
		}
	}

	encryptionParams := jobspb.BackupEncryptionOptions{
		Mode: jobspb.EncryptionMode_None,
	}

	var pw string
	if backupStmt.Options.EncryptionPassphrase != nil {
		pw, err = exprEval.String(ctx, backupStmt.Options.EncryptionPassphrase)
		if err != nil {
			return nil, nil, nil, false, err
		}
		encryptionParams.Mode = jobspb.EncryptionMode_Passphrase
	}

	var kms []string
	if backupStmt.Options.EncryptionKMSURI != nil {
		if encryptionParams.Mode != jobspb.EncryptionMode_None {
			return nil, nil, nil, false,
				errors.New("cannot have both encryption_passphrase and kms option set")
		}
		kms, err = exprEval.StringArray(
			ctx, tree.Exprs(backupStmt.Options.EncryptionKMSURI),
		)
		if err != nil {
			return nil, nil, nil, false, err
		}
		encryptionParams.Mode = jobspb.EncryptionMode_KMS
		if err = logAndSanitizeKmsURIs(ctx, kms...); err != nil {
			return nil, nil, nil, false, err
		}
	}

	var updatesClusterMonitoringMetrics bool
	if backupStmt.Options.UpdatesClusterMonitoringMetrics != nil {
		updatesClusterMonitoringMetrics, err = exprEval.Bool(
			ctx, backupStmt.Options.UpdatesClusterMonitoringMetrics,
		)
		if err != nil {
			return nil, nil, nil, false, err
		}
	}

	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		// TODO(dan): Move this span into sql.
		ctx, span := tracing.ChildSpan(ctx, stmt.StatementTag())
		defer span.Finish()

		if !(p.ExtendedEvalContext().TxnIsSingleStmt || detached) {
			return errors.Errorf("BACKUP cannot be used inside a multi-statement transaction without DETACHED option")
		}

		if len(to) > 1 {
			if err := requireEnterprise(p.ExecCfg(), "partitioned destinations"); err != nil {
				return err
			}
		}

		if !backupStmt.Nested && len(incrementalStorage) > 0 {
			return errors.New("incremental_location option not supported with `BACKUP TO` syntax")
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
			if err := requireEnterprise(p.ExecCfg(), "encryption"); err != nil {
				return err
			}
			encryptionParams.RawPassphrase = pw
		case jobspb.EncryptionMode_KMS:
			encryptionParams.RawKmsUris = kms
			if err := requireEnterprise(p.ExecCfg(), "encryption"); err != nil {
				return err
			}
		}

		if revisionHistory {
			if err := requireEnterprise(p.ExecCfg(), "revision_history"); err != nil {
				return err
			}
		}

		if executionLocality.NonEmpty() {
			if err := requireEnterprise(p.ExecCfg(), "execution locality"); err != nil {
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
			IncrementalFrom:                 incrementalFrom,
			FullCluster:                     backupStmt.Coverage() == tree.AllDescriptors,
			ResolvedCompleteDbs:             completeDBs,
			EncryptionOptions:               &encryptionParams,
			AsOfInterval:                    asOfInterval,
			Detached:                        detached,
			ApplicationName:                 p.SessionData().ApplicationName,
			ExecutionLocality:               executionLocality,
			UpdatesClusterMonitoringMetrics: updatesClusterMonitoringMetrics,
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
			tid, err := roachpb.MakeTenantID(backupStmt.Targets.TenantID.ID)
			if err != nil {
				return err
			}
			initialDetails.SpecificTenantIds = []roachpb.TenantID{tid}
		}

		jobID := p.ExecCfg().JobRegistry.MakeJobID()

		if err := logAndSanitizeBackupDestinations(ctx, append(to, incrementalFrom...)...); err != nil {
			return errors.Wrap(err, "logging backup destinations")
		}

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
		return fn, jobs.DetachedJobExecutionResultHeader, nil, false, nil
	}
	return fn, jobs.BulkJobExecutionResultHeader, nil, false, nil
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

func collectTelemetry(
	ctx context.Context,
	backupManifest *backuppb.BackupManifest,
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
	storage jobs.ScheduledJobStorage,
	scheduleID int64,
) (*jobs.ScheduledJob, *backuppb.ScheduledBackupExecutionArgs, error) {
	// Load the schedule that has spawned this job.
	sj, err := storage.Load(ctx, env, scheduleID)
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
	knobs *jobs.TestingKnobs,
	txn isql.Txn,
	backupDetails *jobspb.BackupDetails,
	createdBy *jobs.CreatedByInfo,
) error {
	env := scheduledjobs.ProdJobSchedulerEnv
	if knobs != nil && knobs.JobSchedulerEnv != nil {
		env = knobs.JobSchedulerEnv
	}
	// If this is not a scheduled backup, we do not chain pts records.
	if createdBy == nil || createdBy.Name != jobs.CreatedByScheduledJobs {
		return nil
	}

	_, args, err := getScheduledBackupExecutionArgsFromSchedule(
		ctx, env, jobs.ScheduledJobTxn(txn), createdBy.ID,
	)
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
			ctx, env, jobs.ScheduledJobTxn(txn), args.DependentScheduleID,
		)
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
	layerToIterFactory backupinfo.LayerToBackupManifestFileIterFactory,
	tables []catalog.TableDescriptor,
	revs []backuppb.BackupManifest_DescriptorRevision,
	endTime hlc.Timestamp,
) ([]roachpb.Span, error) {
	reintroducedTables := make(map[descpb.ID]struct{})

	// First, create a map that indicates which tables from the previous backup
	// were offline when the last backup was taken. To create this map, we must
	// iterate two fields in the _last_ backup's manifest:
	//
	// 1. manifest.Descriptors contains a list of descriptors _explicitly_
	// included in the backup, gathered at backup startTime. This includes all
	// public descriptors, and in the case of cluster backups, offline
	// descriptors.
	//
	// 2. manifest.DescriptorChanges contains a list of descriptor changes tracked
	// in the backup. While investigating #88042, it was discovered that during
	// revision history database and table.* backups, a table can get included in
	// manifest.DescriptorChanges when it transitions from an online to offline
	// state, causing its spans to get backed up. However, if this table was
	// offline when the backup began, it's excluded from manifest.Descriptors.
	// Therefore, to find all descriptors covered in the backup that were offline
	// at backup time, we must find all tables in manifest.DescriptorChanges whose
	// last change brought the table offline.
	offlineInLastBackup := make(map[descpb.ID]struct{})
	lastIterFactory := layerToIterFactory[len(prevBackups)-1]

	descIt := lastIterFactory.NewDescIter(ctx)
	defer descIt.Close()

	for ; ; descIt.Next() {
		if ok, err := descIt.Valid(); err != nil {
			return nil, err
		} else if !ok {
			break
		}

		// TODO(pbardea): Also check that lastWriteTime is set once those are
		// populated on the table descriptor.
		if table, _, _, _, _ := descpb.GetDescriptors(descIt.Value()); table != nil && table.Offline() {
			offlineInLastBackup[table.GetID()] = struct{}{}
		}
	}

	// Gather all the descriptors that were offline at the endTime of the last
	// backup, according the backup's descriptor changes. If the last descriptor
	// change in the previous backup interval put the table offline, then that
	// backup was offline at the endTime of the last backup.
	latestTableDescChangeInLastBackup := make(map[descpb.ID]*descpb.TableDescriptor)
	descRevIt := lastIterFactory.NewDescriptorChangesIter(ctx)
	defer descRevIt.Close()
	for ; ; descRevIt.Next() {
		if ok, err := descRevIt.Valid(); err != nil {
			return nil, err
		} else if !ok {
			break
		}

		if table, _, _, _, _ := descpb.GetDescriptors(descRevIt.Value().Desc); table != nil {
			if trackedRev, ok := latestTableDescChangeInLastBackup[table.GetID()]; !ok {
				latestTableDescChangeInLastBackup[table.GetID()] = table
			} else if trackedRev.Version < table.Version {
				latestTableDescChangeInLastBackup[table.GetID()] = table
			}
		}
	}

	for _, table := range latestTableDescChangeInLastBackup {
		if table.Offline() {
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
		rawTable, _, _, _, _ := descpb.GetDescriptors(rev.Desc)
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
		rawTable, _, _, _, _ := descpb.GetDescriptors(rev.Desc)
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

func getProtectedTimestampTargetForBackup(
	backupManifest *backuppb.BackupManifest,
) (*ptpb.Target, error) {
	if backupManifest.DescriptorCoverage == tree.AllDescriptors {
		return ptpb.MakeClusterTarget(), nil
	}

	if len(backupManifest.Tenants) > 0 {
		tenantID := make([]roachpb.TenantID, 0, len(backupManifest.Tenants))
		for _, tenant := range backupManifest.Tenants {
			tid, err := roachpb.MakeTenantID(tenant.ID)
			if err != nil {
				return nil, err
			}
			tenantID = append(tenantID, tid)
		}
		return ptpb.MakeTenantsTarget(tenantID), nil
	}

	// ResolvedCompleteDBs contains all the "complete" databases being backed up.
	//
	// This includes explicit `BACKUP DATABASE` targets as well as expansions as a
	// result of `BACKUP TABLE db.*`. In both cases we want to write a protected
	// timestamp record that covers the entire database.
	if len(backupManifest.CompleteDbs) > 0 {
		return ptpb.MakeSchemaObjectsTarget(backupManifest.CompleteDbs), nil
	}

	// At this point we are dealing with a `BACKUP TABLE`, so we write a protected
	// timestamp record on each table being backed up.
	tableIDs := make(descpb.IDs, 0)
	for _, desc := range backupManifest.Descriptors {
		t, _, _, _, _ := descpb.GetDescriptors(&desc)
		if t != nil {
			tableIDs = append(tableIDs, t.GetID())
		}
	}
	return ptpb.MakeSchemaObjectsTarget(tableIDs), nil
}

func protectTimestampForBackup(
	ctx context.Context,
	jobID jobspb.JobID,
	pts protectedts.Storage,
	backupManifest *backuppb.BackupManifest,
	backupDetails jobspb.BackupDetails,
) error {
	tsToProtect := backupManifest.EndTime
	if !backupManifest.StartTime.IsEmpty() {
		tsToProtect = backupManifest.StartTime
	}

	// Resolve the target that the PTS record will protect as part of this
	// backup.
	target, err := getProtectedTimestampTargetForBackup(backupManifest)
	if err != nil {
		return err
	}

	// Records written by the backup job should be ignored when making GC
	// decisions on any table that has been marked as
	// `exclude_data_from_backup`. This ensures that the backup job does not
	// holdup GC on that table span for the duration of execution.
	target.IgnoreIfExcludedFromBackup = true
	return pts.Protect(ctx, jobsprotectedts.MakeRecord(
		*backupDetails.ProtectedTimestampRecord,
		int64(jobID),
		tsToProtect,
		backupManifest.Spans,
		jobsprotectedts.Jobs,
		target,
	))
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

func getTenantInfo(
	ctx context.Context,
	settings *cluster.Settings,
	codec keys.SQLCodec,
	txn isql.Txn,
	jobDetails jobspb.BackupDetails,
) ([]roachpb.Span, []mtinfopb.TenantInfoWithUsage, error) {
	var spans []roachpb.Span
	var tenants []mtinfopb.TenantInfoWithUsage
	var err error
	if jobDetails.FullCluster && codec.ForSystemTenant() && jobDetails.IncludeAllSecondaryTenants {
		// Include all tenants.
		tenants, err = retrieveAllTenantsMetadata(
			ctx, txn, settings,
		)
		if err != nil {
			return nil, nil, err
		}
	} else if len(jobDetails.SpecificTenantIds) > 0 {
		for _, id := range jobDetails.SpecificTenantIds {
			tenantInfo, err := retrieveSingleTenantMetadata(ctx, txn, id, settings)
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
		// NB: We use MustMakeTenantID here since the data is
		// coming from the tenants table and we should only
		// ever have valid tenant IDs returned to us.
		prefix := keys.MakeTenantPrefix(roachpb.MustMakeTenantID(tenants[i].ID))
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
	txn isql.Txn,
	jobDetails jobspb.BackupDetails,
	prevBackups []backuppb.BackupManifest,
	layerToIterFactory backupinfo.LayerToBackupManifestFileIterFactory,
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
		targetDescs = make([]catalog.Descriptor, 0, len(descriptorProtos))
		for i := range descriptorProtos {
			targetDescs = append(targetDescs, backupinfo.NewDescriptorForManifest(&descriptorProtos[i]))
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

	var newSpans, reintroducedSpans roachpb.Spans
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
	var tenants []mtinfopb.TenantInfoWithUsage
	tenantSpans, tenantInfos, err := getTenantInfo(
		ctx, execCfg.Settings, execCfg.Codec, txn, jobDetails,
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

		descIt := layerToIterFactory[len(prevBackups)-1].NewDescIter(ctx)
		defer descIt.Close()
		for ; ; descIt.Next() {
			if ok, err := descIt.Valid(); err != nil {
				return backuppb.BackupManifest{}, err
			} else if !ok {
				break
			}

			if t, _, _, _, _ := descpb.GetDescriptors(descIt.Value()); t != nil {
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

		reintroducedSpans, err = getReintroducedSpans(ctx, execCfg, prevBackups, layerToIterFactory, tables, revs, endTime)
		if err != nil {
			return backuppb.BackupManifest{}, err
		}
		newSpans = append(newSpans, reintroducedSpans...)
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
	sql.AddPlanHook(
		"backupccl.backupPlanHook",
		backupPlanHook,
		backupTypeCheck,
	)
}
