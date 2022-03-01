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
	"crypto"
	cryptorand "crypto/rand"
	"fmt"
	"net/url"
	"path"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backupresolver"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/cloud"
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
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	pbtypes "github.com/gogo/protobuf/types"
)

const (
	backupOptRevisionHistory  = "revision_history"
	backupOptEncPassphrase    = "encryption_passphrase"
	backupOptEncKMS           = "kms"
	backupOptWithPrivileges   = "privileges"
	backupOptAsJSON           = "as_json"
	backupOptWithDebugIDs     = "debug_ids"
	backupOptIncStorage       = "incremental_location"
	localityURLParam          = "COCKROACH_LOCALITY"
	defaultLocalityValue      = "default"
	backupOptDebugMetadataSST = "debug_dump_metadata_sst"
)

type tableAndIndex struct {
	tableID descpb.ID
	indexID descpb.IndexID
}

type backupKMSEnv struct {
	settings *cluster.Settings
	conf     *base.ExternalIODirConfig
}

var _ cloud.KMSEnv = &backupKMSEnv{}

// featureBackupEnabled is used to enable and disable the BACKUP feature.
var featureBackupEnabled = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"feature.backup.enabled",
	"set to true to enable backups, false to disable; default is true",
	featureflag.FeatureFlagEnabledDefault,
).WithPublic()

func (p *backupKMSEnv) ClusterSettings() *cluster.Settings {
	return p.settings
}

func (p *backupKMSEnv) KMSConfig() *base.ExternalIODirConfig {
	return p.conf
}

type (
	plaintextMasterKeyID string
	hashedMasterKeyID    string
	encryptedDataKeyMap  struct {
		m map[hashedMasterKeyID][]byte
	}
)

func newEncryptedDataKeyMap() *encryptedDataKeyMap {
	return &encryptedDataKeyMap{make(map[hashedMasterKeyID][]byte)}
}

func newEncryptedDataKeyMapFromProtoMap(protoDataKeyMap map[string][]byte) *encryptedDataKeyMap {
	encMap := &encryptedDataKeyMap{make(map[hashedMasterKeyID][]byte)}
	for k, v := range protoDataKeyMap {
		encMap.m[hashedMasterKeyID(k)] = v
	}

	return encMap
}

func (e *encryptedDataKeyMap) addEncryptedDataKey(
	masterKeyID plaintextMasterKeyID, encryptedDataKey []byte,
) {
	// Hash the master key ID before writing to the map.
	hasher := crypto.SHA256.New()
	hasher.Write([]byte(masterKeyID))
	hash := hasher.Sum(nil)
	e.m[hashedMasterKeyID(hash)] = encryptedDataKey
}

func (e *encryptedDataKeyMap) getEncryptedDataKey(
	masterKeyID plaintextMasterKeyID,
) ([]byte, error) {
	// Hash the master key ID before reading from the map.
	hasher := crypto.SHA256.New()
	hasher.Write([]byte(masterKeyID))
	hash := hasher.Sum(nil)
	var encDataKey []byte
	var ok bool
	if encDataKey, ok = e.m[hashedMasterKeyID(hash)]; !ok {
		return nil, errors.New("could not find an entry in the encryptedDataKeyMap")
	}

	return encDataKey, nil
}

func (e *encryptedDataKeyMap) rangeOverMap(fn func(masterKeyID hashedMasterKeyID, dataKey []byte)) {
	for k, v := range e.m {
		fn(k, v)
	}
}

// getPublicIndexTableSpans returns all the public index spans of the
// provided table.
func getPublicIndexTableSpans(
	table catalog.TableDescriptor, added map[tableAndIndex]bool, codec keys.SQLCodec,
) ([]roachpb.Span, error) {
	publicIndexSpans := make([]roachpb.Span, 0)
	if err := catalog.ForEachActiveIndex(table, func(idx catalog.Index) error {
		key := tableAndIndex{tableID: table.GetID(), indexID: idx.GetID()}
		if added[key] {
			return nil
		}
		added[key] = true
		publicIndexSpans = append(publicIndexSpans, table.IndexSpan(codec, idx.GetID()))
		return nil
	}); err != nil {
		return nil, err
	}

	return publicIndexSpans, nil
}

// spansForAllTableIndexes returns non-overlapping spans for every index and
// table passed in. They would normally overlap if any of them are interleaved.
// Overlapping index spans are merged so as to optimize the size/number of the
// spans we BACKUP and lay protected ts records for.
func spansForAllTableIndexes(
	execCfg *sql.ExecutorConfig,
	tables []catalog.TableDescriptor,
	revs []BackupManifest_DescriptorRevision,
) ([]roachpb.Span, error) {

	added := make(map[tableAndIndex]bool, len(tables))
	sstIntervalTree := interval.NewTree(interval.ExclusiveOverlapper)
	var publicIndexSpans []roachpb.Span
	var err error

	for _, table := range tables {
		publicIndexSpans, err = getPublicIndexTableSpans(table, added, execCfg.Codec)
		if err != nil {
			return nil, err
		}

		for _, indexSpan := range publicIndexSpans {
			if err := sstIntervalTree.Insert(intervalSpan(indexSpan), false); err != nil {
				panic(errors.NewAssertionErrorWithWrappedErrf(err, "IndexSpan"))
			}
		}
	}
	// If there are desc revisions, ensure that we also add any index spans
	// in them that we didn't already get above e.g. indexes or tables that are
	// not in latest because they were dropped during the time window in question.
	for _, rev := range revs {
		// If the table was dropped during the last interval, it will have
		// at least 2 revisions, and the first one should have the table in a PUBLIC
		// state. We want (and do) ignore tables that have been dropped for the
		// entire interval. DROPPED tables should never later become PUBLIC.
		rawTbl, _, _, _ := descpb.FromDescriptor(rev.Desc)
		if rawTbl != nil && rawTbl.Public() {
			tbl := tabledesc.NewBuilder(rawTbl).BuildImmutableTable()
			revSpans, err := getPublicIndexTableSpans(tbl, added, execCfg.Codec)
			if err != nil {
				return nil, err
			}

			publicIndexSpans = append(publicIndexSpans, revSpans...)
			for _, indexSpan := range publicIndexSpans {
				if err := sstIntervalTree.Insert(intervalSpan(indexSpan), false); err != nil {
					panic(errors.NewAssertionErrorWithWrappedErrf(err, "IndexSpan"))
				}
			}
		}
	}

	var spans []roachpb.Span
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

func getLocalityAndBaseURI(uri, appendPath string) (string, string, error) {
	parsedURI, err := url.Parse(uri)
	if err != nil {
		return "", "", err
	}
	q := parsedURI.Query()
	localityKV := q.Get(localityURLParam)
	// Remove the backup locality parameter.
	q.Del(localityURLParam)
	parsedURI.RawQuery = q.Encode()
	if appendPath != "" {
		parsedURI.Path = path.Join(parsedURI.Path, appendPath)
	}
	baseURI := parsedURI.String()
	return localityKV, baseURI, nil
}

// getURIsByLocalityKV takes a slice of URIs for a single (possibly partitioned)
// backup, and returns the default backup destination URI and a map of all other
// URIs by locality KV, appending appendPath to the path component of both the
// default URI and all the locality URIs. The URIs in the result do not include
// the COCKROACH_LOCALITY parameter.
func getURIsByLocalityKV(to []string, appendPath string) (string, map[string]string, error) {
	urisByLocalityKV := make(map[string]string)
	if len(to) == 1 {
		localityKV, baseURI, err := getLocalityAndBaseURI(to[0], appendPath)
		if err != nil {
			return "", nil, err
		}
		if localityKV != "" && localityKV != defaultLocalityValue {
			return "", nil, errors.Errorf("%s %s is invalid for a single BACKUP location",
				localityURLParam, localityKV)
		}
		return baseURI, urisByLocalityKV, nil
	}

	var defaultURI string
	for _, uri := range to {
		localityKV, baseURI, err := getLocalityAndBaseURI(uri, appendPath)
		if err != nil {
			return "", nil, err
		}
		if localityKV == "" {
			return "", nil, errors.Errorf(
				"multiple URLs are provided for partitioned BACKUP, but %s is not specified",
				localityURLParam,
			)
		}
		if localityKV == defaultLocalityValue {
			if defaultURI != "" {
				return "", nil, errors.Errorf("multiple default URLs provided for partition backup")
			}
			defaultURI = baseURI
		} else {
			kv := roachpb.Tier{}
			if err := kv.FromString(localityKV); err != nil {
				return "", nil, errors.Wrap(err, "failed to parse backup locality")
			}
			if _, ok := urisByLocalityKV[localityKV]; ok {
				return "", nil, errors.Errorf("duplicate URIs for locality %s", localityKV)
			}
			urisByLocalityKV[localityKV] = baseURI
		}
	}
	if defaultURI == "" {
		return "", nil, errors.Errorf("no default URL provided for partitioned backup")
	}
	return defaultURI, urisByLocalityKV, nil
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

// validateKMSURIsAgainstFullBackup ensures that the KMS URIs provided to an
// incremental BACKUP are a subset of those used during the full BACKUP. It does
// this by ensuring that the KMS master key ID of each KMS URI specified during
// the incremental BACKUP can be found in the map written to `encryption-info`
// during a base BACKUP.
//
// The method also returns the KMSInfo to be used for all subsequent
// encryption/decryption operations during this BACKUP. By default it is the
// first KMS URI passed during the incremental BACKUP.
func validateKMSURIsAgainstFullBackup(
	kmsURIs []string, kmsMasterKeyIDToDataKey *encryptedDataKeyMap, kmsEnv cloud.KMSEnv,
) (*jobspb.BackupEncryptionOptions_KMSInfo, error) {
	var defaultKMSInfo *jobspb.BackupEncryptionOptions_KMSInfo
	for _, kmsURI := range kmsURIs {
		kms, err := cloud.KMSFromURI(kmsURI, kmsEnv)
		if err != nil {
			return nil, err
		}

		defer func() {
			_ = kms.Close()
		}()

		// Depending on the KMS specific implementation, this may or may not contact
		// the remote KMS.
		id, err := kms.MasterKeyID()
		if err != nil {
			return nil, err
		}

		encryptedDataKey, err := kmsMasterKeyIDToDataKey.getEncryptedDataKey(plaintextMasterKeyID(id))
		if err != nil {
			return nil,
				errors.Wrap(err,
					"one of the provided URIs was not used when encrypting the base BACKUP")
		}

		if defaultKMSInfo == nil {
			defaultKMSInfo = &jobspb.BackupEncryptionOptions_KMSInfo{
				Uri:              kmsURI,
				EncryptedDataKey: encryptedDataKey,
			}
		}
	}

	return defaultKMSInfo, nil
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
	// Check that none of the destinations require an admin role.
	for _, uri := range to {
		conf, err := cloud.ExternalStorageConfFromURI(uri, p.User())
		if err != nil {
			return err
		}
		if !conf.AccessIsWithExplicitAuth() {
			return pgerror.Newf(
				pgcode.InsufficientPrivilege,
				"only users with the admin role are allowed to BACKUP to the specified %s URI",
				conf.Provider.String())
		}
	}

	return nil
}

func requireEnterprise(execCfg *sql.ExecutorConfig, feature string) error {
	if err := utilccl.CheckEnterpriseEnabled(
		execCfg.Settings, execCfg.ClusterID(), execCfg.Organization(),
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

		if !(p.ExtendedEvalContext().TxnImplicit || backupStmt.Options.Detached) {
			return errors.Errorf("BACKUP cannot be used inside a transaction without DETACHED option")
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

		endTime := p.ExecCfg().Clock.Now()
		if backupStmt.AsOf.Expr != nil {
			asOf, err := p.EvalAsOfTimestamp(ctx, backupStmt.AsOf)
			if err != nil {
				return err
			}
			endTime = asOf.Timestamp
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

		var revisionHistory bool
		if backupStmt.Options.CaptureRevisionHistory {
			if err := requireEnterprise(p.ExecCfg(), "revision_history"); err != nil {
				return err
			}
			revisionHistory = true
		}

		var targetDescs []catalog.Descriptor
		var completeDBs []descpb.ID

		switch backupStmt.Coverage() {
		case tree.RequestedDescriptors:
			var err error
			targetDescs, completeDBs, err = backupresolver.ResolveTargetsToDescriptors(ctx, p, endTime, backupStmt.Targets)
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
		}

		if backupStmt.Nested {
			if backupStmt.AppendToLatest {
				initialDetails.Destination.Subdir = latestFileName
			} else if subdir != "" {
				initialDetails.Destination.Subdir = "/" + strings.TrimPrefix(subdir, "/")
			} else {
				initialDetails.Destination.Subdir = endTime.GoTime().Format(DateBasedIntoFolderName)
			}
		}

		if backupStmt.Targets != nil && backupStmt.Targets.TenantID.IsSet() {
			if !p.ExecCfg().Codec.ForSystemTenant() {
				return pgerror.Newf(pgcode.InsufficientPrivilege, "only the system tenant can backup other tenants")
			}
			initialDetails.SpecificTenantIds = []roachpb.TenantID{backupStmt.Targets.TenantID.TenantID}
		}

		jobID := p.ExecCfg().JobRegistry.MakeJobID()

		if p.ExecCfg().Settings.Version.IsActive(ctx, clusterversion.BackupResolutionInJob) {
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
			plannerTxn := p.ExtendedEvalContext().Txn

			if backupStmt.Options.Detached {
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

		// TODO(dt): delete this in 22.2.
		backupDetails, backupManifest, err := getBackupDetailAndManifest(
			ctx, p.ExecCfg(), p.ExtendedEvalContext().Txn, initialDetails, p.User(),
		)
		if err != nil {
			return err
		}

		description, err := backupJobDescription(p, backupStmt.Backup, to, incrementalFrom, encryptionParams.RawKmsUris, backupDetails.Destination.Subdir, initialDetails.Destination.IncrementalStorage)
		if err != nil {
			return err
		}

		// We create the job record in the planner's transaction to ensure that
		// the job record creation happens transactionally.
		plannerTxn := p.ExtendedEvalContext().Txn

		// Write backup manifest into a temporary checkpoint file.
		// This accomplishes 2 purposes:
		//  1. Persists large state needed for backup job completion.
		//  2. Verifies we can write to destination location.
		// This temporary checkpoint file gets renamed to real checkpoint
		// file when the backup jobs starts execution.
		//
		// TODO (pbardea): For partitioned backups, also add verification for other
		// stores we are writing to in addition to the default.
		if err := planSchedulePTSChaining(ctx, p.ExecCfg(), plannerTxn, &backupDetails, backupStmt.CreatedByInfo); err != nil {
			return err
		}

		if len(backupManifest.Spans) > 0 && p.ExecCfg().Codec.ForSystemTenant() {
			protectedtsID := uuid.MakeV4()
			backupDetails.ProtectedTimestampRecord = &protectedtsID
		}

		jr := jobs.Record{
			Description: description,
			Username:    p.User(),
			// TODO(yevgeniy): Consider removing -- this info available in backup manifest.
			DescriptorIDs: func() (sqlDescIDs []descpb.ID) {
				for i := range backupManifest.Descriptors {
					sqlDescIDs = append(sqlDescIDs,
						descpb.GetDescriptorID(&backupManifest.Descriptors[i]))
				}
				return sqlDescIDs
			}(),
			Details:   backupDetails,
			Progress:  jobspb.BackupProgress{},
			CreatedBy: backupStmt.CreatedByInfo,
		}

		lic := utilccl.CheckEnterpriseEnabled(
			p.ExecCfg().Settings, p.ExecCfg().ClusterID(), p.ExecCfg().Organization(), "",
		) != nil

		if err := protectTimestampForBackup(
			ctx, p.ExecCfg(), plannerTxn, jobID, backupManifest, backupDetails,
		); err != nil {
			return err
		}

		if backupStmt.Options.Detached {
			// When running inside an explicit transaction, we simply create the job
			// record. We do not wait for the job to finish.
			_, err := p.ExecCfg().JobRegistry.CreateAdoptableJobWithTxn(
				ctx, jr, jobID, plannerTxn)
			if err != nil {
				return err
			}

			if err := writeBackupManifestCheckpoint(
				ctx, jobID, backupDetails, &backupManifest, p.ExecCfg(), p.User(),
			); err != nil {
				return err
			}

			resultsCh <- tree.Datums{tree.NewDInt(tree.DInt(jobID))}
			collectTelemetry(backupManifest, initialDetails, backupDetails, lic)
			return nil
		}

		// Construct the job and commit the transaction. Perform this work in a
		// closure to ensure that the job is cleaned up if an error occurs.
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
			if err := writeBackupManifestCheckpoint(
				ctx, jobID, backupDetails, &backupManifest, p.ExecCfg(), p.User(),
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

		collectTelemetry(backupManifest, initialDetails, backupDetails, lic)
		if err := sj.Start(ctx); err != nil {
			return err
		}
		if err := sj.AwaitCompletion(ctx); err != nil {
			return err
		}
		return sj.ReportExecutionResults(ctx, resultsCh)
	}

	if backupStmt.Options.Detached {
		return fn, jobs.DetachedJobExecutionResultHeader, nil, false, nil
	}
	return fn, jobs.BulkJobExecutionResultHeader, nil, false, nil
}

func collectTelemetry(
	backupManifest BackupManifest, initialDetails, backupDetails jobspb.BackupDetails, licensed bool,
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
	if backupManifest.isIncremental() || backupDetails.EncryptionOptions != nil {
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
	if backupManifest.MVCCFilter == MVCCFilter_All {
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
		if initialDetails.Destination.Subdir == latestFileName {
			countSource("backup.into-latest")
		}
	}
	if backupManifest.DescriptorCoverage == tree.AllDescriptors {
		countSource("backup.targets.full_cluster")
	}
}

func getScheduledBackupExecutionArgsFromSchedule(
	ctx context.Context,
	env scheduledjobs.JobSchedulerEnv,
	txn *kv.Txn,
	ie *sql.InternalExecutor,
	scheduleID int64,
) (*jobs.ScheduledJob, *ScheduledBackupExecutionArgs, error) {
	// Load the schedule that has spawned this job.
	sj, err := jobs.LoadScheduledJob(ctx, env, scheduleID, ie, txn)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed to load scheduled job %d", scheduleID)
	}

	args := &ScheduledBackupExecutionArgs{}
	if err := pbtypes.UnmarshalAny(sj.ExecutionArgs().Args, args); err != nil {
		return nil, nil, errors.Wrap(err, "un-marshaling args")
	}

	return sj, args, nil
}

func writeBackupManifestCheckpoint(
	ctx context.Context,
	jobID jobspb.JobID,
	backupDetails jobspb.BackupDetails,
	backupManifest *BackupManifest,
	execCfg *sql.ExecutorConfig,
	user security.SQLUsername,
) error {
	defaultStore, err := execCfg.DistSQLSrv.ExternalStorageFromURI(ctx, backupDetails.URI, user)
	if err != nil {
		return err
	}
	defer defaultStore.Close()

	if err := writeBackupManifest(
		ctx, execCfg.Settings, defaultStore, tempCheckpointFileNameForJob(jobID),
		backupDetails.EncryptionOptions, backupManifest,
	); err != nil {
		return errors.Wrapf(err, "writing checkpoint file")
	}
	return nil
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
	if !args.ChainProtectedTimestampRecords {
		return nil
	}

	if args.BackupType == ScheduledBackupExecutionArgs_FULL {
		// Check if there is a dependent incremental schedule associated with the
		// full schedule running the current backup.
		// If present, the full backup on successful completion, will release the
		// pts record found on the incremental schedule, and replace it with a new
		// pts record protecting after the EndTime of the full backup.
		if args.DependentScheduleID == 0 {
			return nil
		}

		_, incArgs, err := getScheduledBackupExecutionArgsFromSchedule(
			ctx, env, txn, execCfg.InternalExecutor, args.DependentScheduleID)
		if err != nil {
			// If we are unable to resolve the dependent incremental schedule (it
			// could have been dropped) we do not need to perform any chaining.
			//
			// TODO(adityamaru): Update this comment when DROP SCHEDULE is taught
			// to clear the dependent ID. Once that is done, we should not encounter
			// this error.
			if jobs.HasScheduledJobNotFoundError(err) {
				log.Warningf(ctx, "could not find dependent schedule with id %d",
					args.DependentScheduleID)
				return nil
			}
			return err
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
		// schedule on completion, so as to prevent an "overhang" incremental from
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
	prevBackups []BackupManifest,
	tables []catalog.TableDescriptor,
	revs []BackupManifest_DescriptorRevision,
	endTime hlc.Timestamp,
) ([]roachpb.Span, error) {
	reintroducedTables := make(map[descpb.ID]struct{})

	offlineInLastBackup := make(map[descpb.ID]struct{})
	lastBackup := prevBackups[len(prevBackups)-1]
	for _, desc := range lastBackup.Descriptors {
		// TODO(pbardea): Also check that lastWriteTime is set once those are
		// populated on the table descriptor.
		if table, _, _, _ := descpb.FromDescriptor(&desc); table != nil && table.Offline() {
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
		rawTable, _, _, _ := descpb.FromDescriptor(rev.Desc)
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
	allRevs := make([]BackupManifest_DescriptorRevision, 0, len(revs))
	for _, rev := range revs {
		rawTable, _, _, _ := descpb.FromDescriptor(rev.Desc)
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

func makeNewEncryptionOptions(
	ctx context.Context, encryptionParams jobspb.BackupEncryptionOptions, kmsEnv cloud.KMSEnv,
) (*jobspb.BackupEncryptionOptions, *jobspb.EncryptionInfo, error) {
	var encryptionOptions *jobspb.BackupEncryptionOptions
	var encryptionInfo *jobspb.EncryptionInfo
	switch encryptionParams.Mode {
	case jobspb.EncryptionMode_Passphrase:
		salt, err := storageccl.GenerateSalt()
		if err != nil {
			return nil, nil, err
		}

		encryptionInfo = &jobspb.EncryptionInfo{Salt: salt}
		encryptionOptions = &jobspb.BackupEncryptionOptions{
			Mode: jobspb.EncryptionMode_Passphrase,
			Key:  storageccl.GenerateKey([]byte(encryptionParams.RawPassphrae), salt),
		}
	case jobspb.EncryptionMode_KMS:
		// Generate a 32 byte/256-bit crypto-random number which will serve as
		// the data key for encrypting the BACKUP data and manifest files.
		plaintextDataKey := make([]byte, 32)
		_, err := cryptorand.Read(plaintextDataKey)
		if err != nil {
			return nil, nil, errors.Wrap(err, "failed to generate DataKey")
		}

		encryptedDataKeyByKMSMasterKeyID, defaultKMSInfo, err :=
			getEncryptedDataKeyByKMSMasterKeyID(ctx, encryptionParams.RawKmsUris, plaintextDataKey, kmsEnv)
		if err != nil {
			return nil, nil, err
		}

		encryptedDataKeyMapForProto := make(map[string][]byte)
		encryptedDataKeyByKMSMasterKeyID.rangeOverMap(
			func(masterKeyID hashedMasterKeyID, dataKey []byte) {
				encryptedDataKeyMapForProto[string(masterKeyID)] = dataKey
			})

		encryptionInfo = &jobspb.EncryptionInfo{EncryptedDataKeyByKMSMasterKeyID: encryptedDataKeyMapForProto}
		encryptionOptions = &jobspb.BackupEncryptionOptions{
			Mode:    jobspb.EncryptionMode_KMS,
			KMSInfo: defaultKMSInfo,
		}
	}
	return encryptionOptions, encryptionInfo, nil
}

func getProtectedTimestampTargetForBackup(backupManifest BackupManifest) *ptpb.Target {
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
		t, _, _, _ := descpb.FromDescriptorWithMVCCTimestamp(&desc, hlc.Timestamp{})
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
	backupManifest BackupManifest,
	backupDetails jobspb.BackupDetails,
) error {
	if backupDetails.ProtectedTimestampRecord == nil {
		return nil
	}
	if len(backupManifest.Spans) > 0 {
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
		err := execCfg.ProtectedTimestampProvider.Protect(ctx, txn, rec)
		if err != nil {
			return err
		}
	}
	return nil
}

func getEncryptedDataKeyFromURI(
	ctx context.Context, plaintextDataKey []byte, kmsURI string, kmsEnv cloud.KMSEnv,
) (string, []byte, error) {
	kms, err := cloud.KMSFromURI(kmsURI, kmsEnv)
	if err != nil {
		return "", nil, err
	}

	defer func() {
		_ = kms.Close()
	}()

	kmsURL, err := url.ParseRequestURI(kmsURI)
	if err != nil {
		return "", nil, errors.Wrap(err, "cannot parse KMSURI")
	}
	encryptedDataKey, err := kms.Encrypt(ctx, plaintextDataKey)
	if err != nil {
		return "", nil, errors.Wrapf(err, "failed to encrypt data key for KMS scheme %s",
			kmsURL.Scheme)
	}

	masterKeyID, err := kms.MasterKeyID()
	if err != nil {
		return "", nil, errors.Wrapf(err, "failed to get master key ID for KMS scheme %s",
			kmsURL.Scheme)
	}

	return masterKeyID, encryptedDataKey, nil
}

// getEncryptedDataKeyByKMSMasterKeyID constructs a mapping {MasterKeyID :
// EncryptedDataKey} for each KMS URI provided during a full BACKUP. The
// MasterKeyID is hashed before writing it to the map.
//
// The method also returns the KMSInfo to be used for all subsequent
// encryption/decryption operations during this BACKUP. By default it is the
// first KMS URI.
func getEncryptedDataKeyByKMSMasterKeyID(
	ctx context.Context, kmsURIs []string, plaintextDataKey []byte, kmsEnv cloud.KMSEnv,
) (*encryptedDataKeyMap, *jobspb.BackupEncryptionOptions_KMSInfo, error) {
	encryptedDataKeyByKMSMasterKeyID := newEncryptedDataKeyMap()
	// The coordinator node contacts every KMS and records the encrypted data
	// key for each one.
	var kmsInfo *jobspb.BackupEncryptionOptions_KMSInfo
	for _, kmsURI := range kmsURIs {
		masterKeyID, encryptedDataKey, err := getEncryptedDataKeyFromURI(ctx,
			plaintextDataKey, kmsURI, kmsEnv)
		if err != nil {
			return nil, nil, err
		}

		// By default we use the first KMS URI and encrypted data key for subsequent
		// encryption/decryption operation during a BACKUP.
		if kmsInfo == nil {
			kmsInfo = &jobspb.BackupEncryptionOptions_KMSInfo{
				Uri:              kmsURI,
				EncryptedDataKey: encryptedDataKey,
			}
		}

		encryptedDataKeyByKMSMasterKeyID.addEncryptedDataKey(plaintextMasterKeyID(masterKeyID),
			encryptedDataKey)
	}

	return encryptedDataKeyByKMSMasterKeyID, kmsInfo, nil
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
	user security.SQLUsername,
) (jobspb.BackupDetails, BackupManifest, error) {
	makeCloudStorage := execCfg.DistSQLSrv.ExternalStorageFromURI

	mvccFilter := MVCCFilter_Latest
	if initialDetails.RevisionHistory {
		mvccFilter = MVCCFilter_All
	}
	endTime := initialDetails.EndTime

	var targetDescs []catalog.Descriptor
	var descriptorProtos []descpb.Descriptor
	if initialDetails.FullCluster {
		var err error
		targetDescs, _, err = fullClusterTargetsBackup(ctx, execCfg, endTime)
		if err != nil {
			return jobspb.BackupDetails{}, BackupManifest{}, err
		}
		descriptorProtos = make([]descpb.Descriptor, len(targetDescs))
		for i, desc := range targetDescs {
			descriptorProtos[i] = *desc.DescriptorProto()
		}
	} else {
		descriptorProtos = initialDetails.ResolvedTargets
		targetDescs = make([]catalog.Descriptor, len(descriptorProtos))
		for i := range descriptorProtos {
			targetDescs[i] = descbuilder.NewBuilder(&descriptorProtos[i]).BuildExistingMutable()
		}
	}

	// TODO(pbardea): Refactor (defaultURI and urisByLocalityKV) pairs into a
	// backupDestination struct.
	collectionURI, defaultURI, resolvedSubdir, urisByLocalityKV, prevs, err :=
		resolveDest(ctx, user, initialDetails.Destination, makeCloudStorage, endTime, initialDetails.IncrementalFrom)
	if err != nil {
		return jobspb.BackupDetails{}, BackupManifest{}, err
	}

	defaultStore, err := execCfg.DistSQLSrv.ExternalStorageFromURI(ctx, defaultURI, user)
	if err != nil {
		return jobspb.BackupDetails{}, BackupManifest{}, err
	}
	defer defaultStore.Close()

	if err := checkForPreviousBackup(ctx, defaultStore, defaultURI); err != nil {
		return jobspb.BackupDetails{}, BackupManifest{}, err
	}

	kmsEnv := &backupKMSEnv{settings: execCfg.Settings, conf: &execCfg.ExternalIODirConfig}

	mem := execCfg.RootMemoryMonitor.MakeBoundAccount()
	defer mem.Close(ctx)

	prevBackups, encryptionOptions, memSize, err := fetchPreviousBackups(ctx, &mem, user,
		makeCloudStorage, prevs, *initialDetails.EncryptionOptions, kmsEnv)
	if err != nil {
		return jobspb.BackupDetails{}, BackupManifest{}, err
	}
	defer func() {
		mem.Shrink(ctx, memSize)
	}()

	if len(prevBackups) > 0 {
		baseManifest := prevBackups[0]
		if baseManifest.DescriptorCoverage == tree.AllDescriptors &&
			!initialDetails.FullCluster {
			return jobspb.BackupDetails{}, BackupManifest{}, errors.Errorf("cannot append a backup of specific tables or databases to a cluster backup")
		}
	}

	var startTime hlc.Timestamp
	if len(prevBackups) > 0 {
		if err := requireEnterprise(execCfg, "incremental"); err != nil {
			return jobspb.BackupDetails{}, BackupManifest{}, err
		}
		startTime = prevBackups[len(prevBackups)-1].EndTime
	}

	var tables []catalog.TableDescriptor
	statsFiles := make(map[descpb.ID]string)
	for _, desc := range targetDescs {
		switch desc := desc.(type) {
		case catalog.TableDescriptor:
			tables = append(tables, desc)
			// TODO (anzo): look into the tradeoffs of having all objects in the array to be in the same file,
			// vs having each object in a separate file, or somewhere in between.
			statsFiles[desc.GetID()] = backupStatisticsFileName
		}
	}

	clusterID := execCfg.ClusterID()
	for i := range prevBackups {
		// IDs are how we identify tables, and those are only meaningful in the
		// context of their own cluster, so we need to ensure we only allow
		// incremental previous backups that we created.
		if fromCluster := prevBackups[i].ClusterID; !fromCluster.Equal(clusterID) {
			return jobspb.BackupDetails{}, BackupManifest{}, errors.Newf("previous BACKUP belongs to cluster %s", fromCluster.String())
		}
	}

	var newSpans roachpb.Spans

	var priorIDs map[descpb.ID]descpb.ID

	var revs []BackupManifest_DescriptorRevision
	if mvccFilter == MVCCFilter_All {
		priorIDs = make(map[descpb.ID]descpb.ID)
		revs, err = getRelevantDescChanges(ctx, execCfg, startTime, endTime, targetDescs,
			initialDetails.ResolvedCompleteDbs, priorIDs, initialDetails.FullCluster)
		if err != nil {
			return jobspb.BackupDetails{}, BackupManifest{}, err
		}
	}

	var spans []roachpb.Span
	var tenants []descpb.TenantInfoWithUsage

	if initialDetails.FullCluster && execCfg.Codec.ForSystemTenant() {
		// Include all tenants.
		tenants, err = retrieveAllTenantsMetadata(
			ctx, execCfg.InternalExecutor, txn,
		)
		if err != nil {
			return jobspb.BackupDetails{}, BackupManifest{}, err
		}
	} else if len(initialDetails.SpecificTenantIds) > 0 {
		for _, id := range initialDetails.SpecificTenantIds {
			tenantInfo, err := retrieveSingleTenantMetadata(
				ctx, execCfg.InternalExecutor, txn, id,
			)
			if err != nil {
				return jobspb.BackupDetails{}, BackupManifest{}, err
			}
			tenants = append(tenants, tenantInfo)
		}
	}

	if len(tenants) > 0 {
		if initialDetails.RevisionHistory {
			return jobspb.BackupDetails{}, BackupManifest{}, errors.UnimplementedError(
				errors.IssueLink{IssueURL: "https://github.com/cockroachdb/cockroach/issues/47896"},
				"can not backup tenants with revision history",
			)
		}
		for i := range tenants {
			prefix := keys.MakeTenantPrefix(roachpb.MakeTenantID(tenants[i].ID))
			spans = append(spans, roachpb.Span{Key: prefix, EndKey: prefix.PrefixEnd()})
		}
	}

	tableSpans, err := spansForAllTableIndexes(execCfg, tables, revs)
	if err != nil {
		return jobspb.BackupDetails{}, BackupManifest{}, err
	}
	spans = append(spans, tableSpans...)

	if len(prevBackups) > 0 {
		tablesInPrev := make(map[descpb.ID]struct{})
		dbsInPrev := make(map[descpb.ID]struct{})
		rawDescs := prevBackups[len(prevBackups)-1].Descriptors
		for i := range rawDescs {
			if t, _, _, _ := descpb.FromDescriptor(&rawDescs[i]); t != nil {
				tablesInPrev[t.ID] = struct{}{}
			}
		}
		for _, d := range prevBackups[len(prevBackups)-1].CompleteDbs {
			dbsInPrev[d] = struct{}{}
		}

		if !initialDetails.FullCluster {
			if err := checkForNewTables(ctx, execCfg.Codec, execCfg.DB, targetDescs, tablesInPrev, dbsInPrev, priorIDs, startTime, endTime); err != nil {
				return jobspb.BackupDetails{}, BackupManifest{}, err
			}
			// Let's check that we're not widening the scope of this backup to an
			// entire database, even if no tables were created in the meantime.
			if err := checkForNewCompleteDatabases(targetDescs, initialDetails.ResolvedCompleteDbs, dbsInPrev); err != nil {
				return jobspb.BackupDetails{}, BackupManifest{}, err
			}
		}

		newSpans = filterSpans(spans, prevBackups[len(prevBackups)-1].Spans)

		tableSpans, err := getReintroducedSpans(ctx, execCfg, prevBackups, tables, revs, endTime)
		if err != nil {
			return jobspb.BackupDetails{}, BackupManifest{}, err
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
	if initialDetails.FullCluster {
		coverage = tree.AllDescriptors
	}

	backupManifest := BackupManifest{
		StartTime:           startTime,
		EndTime:             endTime,
		MVCCFilter:          mvccFilter,
		Descriptors:         descriptorProtos,
		Tenants:             tenants,
		DescriptorChanges:   revs,
		CompleteDbs:         initialDetails.ResolvedCompleteDbs,
		Spans:               spans,
		IntroducedSpans:     newSpans,
		FormatVersion:       BackupFormatDescriptorTrackingVersion,
		BuildInfo:           build.GetInfo(),
		ClusterVersion:      execCfg.Settings.Version.ActiveVersion(ctx).Version,
		ClusterID:           execCfg.ClusterID(),
		StatisticsFilenames: statsFiles,
		DescriptorCoverage:  coverage,
	}

	// Verify this backup on its prior chain cover its spans up to its end time,
	// as restore would do if it tried to restore this backup.
	if err := checkCoverage(ctx, spans, append(prevBackups, backupManifest)); err != nil {
		return jobspb.BackupDetails{}, BackupManifest{}, errors.Wrap(err, "new backup would not cover expected time")
	}

	// If we didn't load any prior backups from which get encryption info, we
	// need to generate encryption specific data.
	var encryptionInfo *jobspb.EncryptionInfo
	if encryptionOptions == nil {
		encryptionOptions, encryptionInfo, err = makeNewEncryptionOptions(ctx, *initialDetails.EncryptionOptions, kmsEnv)
		if err != nil {
			return jobspb.BackupDetails{}, BackupManifest{}, err
		}
	}

	return jobspb.BackupDetails{
		Destination:       jobspb.BackupDetails_Destination{Subdir: resolvedSubdir},
		StartTime:         startTime,
		EndTime:           endTime,
		URI:               defaultURI,
		URIsByLocalityKV:  urisByLocalityKV,
		EncryptionOptions: encryptionOptions,
		EncryptionInfo:    encryptionInfo,
		CollectionURI:     collectionURI,
	}, backupManifest, nil
}

func init() {
	sql.AddPlanHook("backup", backupPlanHook)
}
