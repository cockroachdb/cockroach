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
	"sort"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/featureflag"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/covering"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/storage/cloudimpl"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

const (
	backupOptRevisionHistory = "revision_history"
	backupOptEncPassphrase   = "encryption_passphrase"
	backupOptEncKMS          = "kms"
	backupOptWithPrivileges  = "privileges"
	localityURLParam         = "COCKROACH_LOCALITY"
	defaultLocalityValue     = "default"
)

type encryptionMode int

const (
	noEncryption encryptionMode = iota
	passphrase
	kms
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

type plaintextMasterKeyID string
type hashedMasterKeyID string
type encryptedDataKeyMap struct {
	m map[hashedMasterKeyID][]byte
}

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

type sortedIndexIDs []descpb.IndexID

func (s sortedIndexIDs) Less(i, j int) bool {
	return s[i] < s[j]
}

func (s sortedIndexIDs) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s sortedIndexIDs) Len() int {
	return len(s)
}

// getLogicallyMergedTableSpans returns all the non-drop index spans of the
// provided table but after merging them so as to minimize the number of spans
// generated. The following rules are used to logically merge the sorted set of
// non-drop index spans:
// - Contiguous index spans are merged.
// - Two non-contiguous index spans are merged if a scan request for the index
// IDs between them does not return any results.
//
// Egs: {/Table/51/1 - /Table/51/2}, {/Table/51/3 - /Table/51/4} => {/Table/51/1 - /Table/51/4}
// provided the dropped index represented by the span
// {/Table/51/2 - /Table/51/3} has been gc'ed.
func getLogicallyMergedTableSpans(
	table catalog.TableDescriptor,
	added map[tableAndIndex]bool,
	codec keys.SQLCodec,
	endTime hlc.Timestamp,
	checkForKVInBounds func(start, end roachpb.Key, endTime hlc.Timestamp) (bool, error),
) ([]roachpb.Span, error) {
	var nonDropIndexIDs []descpb.IndexID
	if err := catalog.ForEachNonDropIndex(table, func(idx catalog.Index) error {
		key := tableAndIndex{tableID: table.GetID(), indexID: idx.GetID()}
		if added[key] {
			return nil
		}
		added[key] = true
		nonDropIndexIDs = append(nonDropIndexIDs, idx.GetID())
		return nil
	}); err != nil {
		return nil, err
	}

	if len(nonDropIndexIDs) == 0 {
		return nil, nil
	}

	// There is no merging possible with only a single index, short circuit.
	if len(nonDropIndexIDs) == 1 {
		return []roachpb.Span{table.IndexSpan(codec, nonDropIndexIDs[0])}, nil
	}

	sort.Sort(sortedIndexIDs(nonDropIndexIDs))

	var mergedIndexSpans []roachpb.Span

	// mergedSpan starts off as the first span in the set of spans being
	// considered for a logical merge.
	// The logical span merge algorithm walks over the table's non drop indexes
	// using an lhsSpan and rhsSpan  (always offset by 1). It checks all index IDs
	// between lhsSpan and rhsSpan to look for dropped but non-gced KVs. The
	// existence of such a KV indicates that the rhsSpan cannot be included in the
	// current set of spans being logically merged, and so we update the
	// mergedSpan to encompass the lhsSpan as that is the furthest we can go.
	// After recording the new "merged" span, we update mergedSpan to be the
	// rhsSpan, and start processing the next logically mergeable span set.
	mergedSpan := table.IndexSpan(codec, nonDropIndexIDs[0])
	for curIndex := 0; curIndex < len(nonDropIndexIDs)-1; curIndex++ {
		lhsIndexID := nonDropIndexIDs[curIndex]
		rhsIndexID := nonDropIndexIDs[curIndex+1]

		lhsSpan := table.IndexSpan(codec, lhsIndexID)
		rhsSpan := table.IndexSpan(codec, rhsIndexID)

		lhsIndex, err := table.FindIndexWithID(lhsIndexID)
		if err != nil {
			return nil, err
		}
		rhsIndex, err := table.FindIndexWithID(rhsIndexID)
		if err != nil {
			return nil, err
		}

		// If either the lhs or rhs is an interleaved index, we do not attempt to
		// perform a logical merge of the spans because the index span for
		// interleaved contains the tableID/indexID of the furthest ancestor in
		// the interleaved chain.
		if lhsIndex.IsInterleaved() || rhsIndex.IsInterleaved() {
			mergedIndexSpans = append(mergedIndexSpans, mergedSpan)
			mergedSpan = rhsSpan
		} else {
			var foundDroppedKV bool
			// Iterate over all index IDs between the two candidates (lhs and rhs)
			// which may be logically merged. These index IDs represent dropped
			// indexes between the two non-drop index spans.
			for i := lhsIndexID + 1; i < rhsIndexID; i++ {
				// If we find an index which has been dropped but not gc'ed, we cannot
				// merge the lhs and rhs spans.
				foundDroppedKV, err = checkForKVInBounds(lhsSpan.EndKey, rhsSpan.Key, endTime)
				if err != nil {
					return nil, err
				}
				if foundDroppedKV {
					mergedSpan.EndKey = lhsSpan.EndKey
					mergedIndexSpans = append(mergedIndexSpans, mergedSpan)
					mergedSpan = rhsSpan
					break
				}
			}
		}

		// The loop will terminate after this iteration and so we must update the
		// current mergedSpan to encompass the last element in the nonDropIndexIDs
		// slice as well.
		if curIndex == len(nonDropIndexIDs)-2 {
			mergedSpan.EndKey = rhsSpan.EndKey
			mergedIndexSpans = append(mergedIndexSpans, mergedSpan)
		}
	}

	return mergedIndexSpans, nil
}

// spansForAllTableIndexes returns non-overlapping spans for every index and
// table passed in. They would normally overlap if any of them are interleaved.
// The outputted spans are merged as described by the method
// getLogicallyMergedTableSpans, so as to optimize the size/number of the spans
// we BACKUP and lay protected ts records for.
func spansForAllTableIndexes(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	endTime hlc.Timestamp,
	tables []catalog.TableDescriptor,
	revs []BackupManifest_DescriptorRevision,
) ([]roachpb.Span, error) {

	added := make(map[tableAndIndex]bool, len(tables))
	sstIntervalTree := interval.NewTree(interval.ExclusiveOverlapper)
	var mergedIndexSpans []roachpb.Span
	var err error

	// checkForKVInBounds issues a scan request between start and end at endTime,
	// and returns true if a non-nil result is returned.
	checkForKVInBounds := func(start, end roachpb.Key, endTime hlc.Timestamp) (bool, error) {
		var foundKV bool
		err := execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			txn.SetFixedTimestamp(ctx, endTime)
			res, err := txn.Scan(ctx, start, end, 1 /* maxRows */)
			if err != nil {
				return err
			}
			foundKV = len(res) != 0
			return nil
		})
		return foundKV, err
	}

	for _, table := range tables {
		mergedIndexSpans, err = getLogicallyMergedTableSpans(table, added, execCfg.Codec, endTime,
			checkForKVInBounds)
		if err != nil {
			return nil, err
		}

		for _, indexSpan := range mergedIndexSpans {
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
		// TODO(pbardea): Consider and test the interaction between revision_history
		// backups and OFFLINE tables.
		rawTbl := descpb.TableFromDescriptor(rev.Desc, hlc.Timestamp{})
		if rawTbl != nil && rawTbl.State != descpb.DescriptorState_DROP {
			tbl := tabledesc.NewImmutable(*rawTbl)
			revSpans, err := getLogicallyMergedTableSpans(tbl, added, execCfg.Codec, rev.Time,
				checkForKVInBounds)
			if err != nil {
				return nil, err
			}

			mergedIndexSpans = append(mergedIndexSpans, revSpans...)
			for _, indexSpan := range mergedIndexSpans {
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
	mergedSpans, distinct := roachpb.MergeSpans(spans)
	if !distinct {
		return nil, errors.NewAssertionErrorWithWrappedErrf(errors.New("expected all resolved spans for the BACKUP to be distinct"), "IndexSpan")
	}

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
	opts tree.BackupOptions, kmsURIs []string,
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

	for _, uri := range kmsURIs {
		redactedURI, err := cloudimpl.RedactKMSURI(uri)
		if err != nil {
			return tree.BackupOptions{}, err
		}
		newOpts.EncryptionKMSURI = append(newOpts.EncryptionKMSURI, tree.NewDString(redactedURI))
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

	for _, t := range to {
		sanitizedTo, err := cloudimpl.SanitizeExternalStorageURI(t, nil /* extraParams */)
		if err != nil {
			return nil, err
		}
		b.To = append(b.To, tree.NewDString(sanitizedTo))
	}

	for _, from := range incrementalFrom {
		sanitizedFrom, err := cloudimpl.SanitizeExternalStorageURI(from, nil /* extraParams */)
		if err != nil {
			return nil, err
		}
		b.IncrementalFrom = append(b.IncrementalFrom, tree.NewDString(sanitizedFrom))
	}

	resolvedOpts, err := resolveOptionsForBackupJobDescription(backup.Options, kmsURIs)
	if err != nil {
		return nil, err
	}
	b.Options = resolvedOpts
	return b, nil
}

func backupJobDescription(
	p sql.PlanHookState,
	backup *tree.Backup,
	to []string,
	incrementalFrom []string,
	kmsURIs []string,
	resolvedSubdir string,
) (string, error) {
	b, err := GetRedactedBackupNode(backup, to, incrementalFrom, kmsURIs, resolvedSubdir,
		true /* hasBeenPlanned */)
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

// backupEncryptionParams is a structured representation of the encryption
// options that the user provided in the backup statement.
type backupEncryptionParams struct {
	encryptMode          encryptionMode
	kmsURIs              []string
	encryptionPassphrase []byte

	kmsEnv *backupKMSEnv
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
	if backupStmt.Targets != nil && backupStmt.Targets.Tenant != (roachpb.TenantID{}) {
		return pgerror.Newf(
			pgcode.InsufficientPrivilege,
			"only users with the admin role can perform BACKUP TENANT")
	}
	for _, desc := range targetDescs {
		switch desc := desc.(type) {
		case catalog.DatabaseDescriptor, catalog.TableDescriptor:
			if err := p.CheckPrivilege(ctx, desc, privilege.SELECT); err != nil {
				return err
			}
		case catalog.TypeDescriptor, catalog.SchemaDescriptor:
			if err := p.CheckPrivilege(ctx, desc, privilege.USAGE); err != nil {
				return err
			}
		}
	}
	knobs := p.ExecCfg().BackupRestoreTestingKnobs
	if knobs != nil && knobs.AllowImplicitAccess {
		return nil
	}
	// Check that none of the destinations require an admin role.
	for _, uri := range to {
		hasExplicitAuth, uriScheme, err := cloud.AccessIsWithExplicitAuth(uri)
		if err != nil {
			return err
		}
		if !hasExplicitAuth {
			return pgerror.Newf(
				pgcode.InsufficientPrivilege,
				"only users with the admin role are allowed to BACKUP to the specified %s URI",
				uriScheme)
		}
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

	encryptionParams := backupEncryptionParams{}

	var pwFn func() (string, error)
	encryptionParams.encryptMode = noEncryption
	if backupStmt.Options.EncryptionPassphrase != nil {
		fn, err := p.TypeAsString(ctx, backupStmt.Options.EncryptionPassphrase, "BACKUP")
		if err != nil {
			return nil, nil, nil, false, err
		}
		pwFn = fn
		encryptionParams.encryptMode = passphrase
	}

	var kmsFn func() ([]string, *backupKMSEnv, error)
	if backupStmt.Options.EncryptionKMSURI != nil {
		if encryptionParams.encryptMode != noEncryption {
			return nil, nil, nil, false,
				errors.New("cannot have both encryption_passphrase and kms option set")
		}
		fn, err := p.TypeAsStringArray(ctx, tree.Exprs(backupStmt.Options.EncryptionKMSURI),
			"BACKUP")
		if err != nil {
			return nil, nil, nil, false, err
		}
		kmsFn = func() ([]string, *backupKMSEnv, error) {
			res, err := fn()
			if err == nil {
				return res, &backupKMSEnv{
					settings: p.ExecCfg().Settings,
					conf:     &p.ExecCfg().ExternalIODirConfig,
				}, nil
			}
			return nil, nil, err
		}
		encryptionParams.encryptMode = kms
	}

	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		// TODO(dan): Move this span into sql.
		ctx, span := tracing.ChildSpan(ctx, stmt.StatementTag())
		defer span.Finish()

		if !(p.ExtendedEvalContext().TxnImplicit || backupStmt.Options.Detached) {
			return errors.Errorf("BACKUP cannot be used inside a transaction without DETACHED option")
		}

		var isEnterprise bool
		requireEnterprise := func(feature string) error {
			if isEnterprise {
				return nil
			}
			if err := utilccl.CheckEnterpriseEnabled(
				p.ExecCfg().Settings, p.ExecCfg().ClusterID(), p.ExecCfg().Organization(),
				fmt.Sprintf("BACKUP with %s", feature),
			); err != nil {
				return err
			}
			isEnterprise = true
			return nil
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
			if err := requireEnterprise("partitioned destinations"); err != nil {
				return err
			}
		}

		incrementalFrom, err := incrementalFromFn()
		if err != nil {
			return err
		}

		endTime := p.ExecCfg().Clock.Now()
		if backupStmt.AsOf.Expr != nil {
			var err error
			if endTime, err = p.EvalAsOfTimestamp(ctx, backupStmt.AsOf); err != nil {
				return err
			}
		}

		mvccFilter := MVCCFilter_Latest
		if backupStmt.Options.CaptureRevisionHistory {
			if err := requireEnterprise("revision_history"); err != nil {
				return err
			}
			mvccFilter = MVCCFilter_All
		}

		targetDescs, completeDBs, err := ResolveTargetsToDescriptors(ctx, p, endTime, backupStmt.Targets)
		if err != nil {
			return errors.Wrap(err, "failed to resolve targets specified in the BACKUP stmt")
		}

		if backupStmt.Coverage() == tree.AllDescriptors && len(targetDescs) == 0 {
			return errors.New("no descriptors available to backup at selected time")
		}

		// Check BACKUP privileges.
		err = checkPrivilegesForBackup(ctx, backupStmt, p, targetDescs, to)
		if err != nil {
			return err
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

		if err := ensureInterleavesIncluded(tables); err != nil {
			return err
		}

		makeCloudStorage := p.ExecCfg().DistSQLSrv.ExternalStorageFromURI

		switch encryptionParams.encryptMode {
		case passphrase:
			pw, err := pwFn()
			if err != nil {
				return err
			}
			if err := requireEnterprise("encryption"); err != nil {
				return err
			}
			encryptionParams.encryptionPassphrase = []byte(pw)
		case kms:
			encryptionParams.kmsURIs, encryptionParams.kmsEnv, err = kmsFn()
			if err != nil {
				return err
			}
			if err := requireEnterprise("encryption"); err != nil {
				return err
			}
		}

		defaultURI, urisByLocalityKV, err := getURIsByLocalityKV(to, "")
		if err != nil {
			return err
		}

		// TODO(pbardea): Refactor (defaultURI and urisByLocalityKV) pairs into a
		// backupDestination struct.
		collectionURI, defaultURI, resolvedSubdir, urisByLocalityKV, prevs, err :=
			resolveDest(ctx, p.User(), backupStmt.Nested, backupStmt.AppendToLatest, defaultURI,
				urisByLocalityKV, makeCloudStorage, endTime, to, incrementalFrom, subdir)
		if err != nil {
			return err
		}
		prevBackups, encryptionOptions, err := fetchPreviousBackups(ctx, p.User(), makeCloudStorage, prevs,
			encryptionParams)
		if err != nil {
			return err
		}
		if len(prevBackups) > 0 {
			baseManifest := prevBackups[0]
			if baseManifest.DescriptorCoverage == tree.AllDescriptors &&
				backupStmt.Coverage() != tree.AllDescriptors {
				return errors.Errorf("cannot append a backup of specific tables or databases to a cluster backup")
			}
		}

		clusterID := p.ExecCfg().ClusterID()
		for i := range prevBackups {
			// IDs are how we identify tables, and those are only meaningful in the
			// context of their own cluster, so we need to ensure we only allow
			// incremental previous backups that we created.
			if fromCluster := prevBackups[i].ClusterID; !fromCluster.Equal(clusterID) {
				return errors.Newf("previous BACKUP belongs to cluster %s", fromCluster.String())
			}
		}

		var startTime hlc.Timestamp
		var newSpans roachpb.Spans
		if len(prevBackups) > 0 {
			if err := requireEnterprise("incremental"); err != nil {
				return err
			}
			startTime = prevBackups[len(prevBackups)-1].EndTime
		}

		var priorIDs map[descpb.ID]descpb.ID

		var revs []BackupManifest_DescriptorRevision
		if mvccFilter == MVCCFilter_All {
			priorIDs = make(map[descpb.ID]descpb.ID)
			revs, err = getRelevantDescChanges(ctx, p.ExecCfg().Codec, p.ExecCfg().DB, startTime, endTime, targetDescs, completeDBs, priorIDs, backupStmt.Coverage())
			if err != nil {
				return err
			}
		}

		var spans []roachpb.Span
		var tenantRows []tree.Datums
		if backupStmt.Targets != nil && backupStmt.Targets.Tenant != (roachpb.TenantID{}) {
			if !p.ExecCfg().Codec.ForSystemTenant() {
				return pgerror.Newf(pgcode.InsufficientPrivilege, "only the system tenant can backup other tenants")
			}

			tenID := backupStmt.Targets.Tenant
			id := backupStmt.Targets.Tenant.ToUint64()
			ds, err := p.ExecCfg().InternalExecutor.QueryRow(
				ctx, "backup-lookup-tenant", p.ExtendedEvalContext().Txn,
				`SELECT id, active, info FROM system.tenants WHERE id = $1`, id,
			)

			if err != nil {
				return err
			}

			if ds == nil {
				return errors.Errorf("tenant %s does not exist", tenID)
			}

			if !tree.MustBeDBool(ds[1]) {
				return errors.Errorf("tenant %d is not active", id)
			}

			tenantRows = append(tenantRows, ds)
		} else {
			tableSpans, err := spansForAllTableIndexes(ctx, p.ExecCfg(), endTime, tables, revs)
			if err != nil {
				return err
			}
			spans = append(spans, tableSpans...)

			// Include all tenants.
			// TODO(tbg): make conditional on cluster setting.
			tenantRows, err = p.ExecCfg().InternalExecutor.Query(
				ctx, "backup-lookup-tenant", p.ExtendedEvalContext().Txn,
				`SELECT id, active, info FROM system.tenants`,
			)

			if err != nil {
				return err
			}
		}

		var tenants []descpb.TenantInfo
		for _, row := range tenantRows {
			// TODO isn't there a general problem here with tenant IDs > MaxInt64?
			id := uint64(tree.MustBeDInt(row[0]))

			info := descpb.TenantInfo{ID: id}
			infoBytes := []byte(tree.MustBeDBytes(row[2]))
			if err := protoutil.Unmarshal(infoBytes, &info); err != nil {
				return err
			}
			tenants = append(tenants, info)

			prefix := keys.MakeTenantPrefix(roachpb.MakeTenantID(id))
			spans = append(spans, roachpb.Span{Key: prefix, EndKey: prefix.PrefixEnd()})
		}

		if len(tenants) > 0 && backupStmt.Options.CaptureRevisionHistory {
			return errors.UnimplementedError(
				errors.IssueLink{IssueURL: "https://github.com/cockroachdb/cockroach/issues/47896"},
				"can not backup tenants with revision history",
			)
		}

		if len(prevBackups) > 0 {
			tablesInPrev := make(map[descpb.ID]struct{})
			dbsInPrev := make(map[descpb.ID]struct{})
			rawDescs := prevBackups[len(prevBackups)-1].Descriptors
			for i := range rawDescs {
				if t := descpb.TableFromDescriptor(&rawDescs[i], hlc.Timestamp{}); t != nil {
					tablesInPrev[t.ID] = struct{}{}
				}
			}
			for _, d := range prevBackups[len(prevBackups)-1].CompleteDbs {
				dbsInPrev[d] = struct{}{}
			}

			if backupStmt.Coverage() != tree.AllDescriptors {
				if err := checkForNewTables(ctx, p.ExecCfg().Codec, p.ExecCfg().DB, targetDescs, tablesInPrev, dbsInPrev, priorIDs, startTime, endTime); err != nil {
					return err
				}
				// Let's check that we're not widening the scope of this backup to an
				// entire database, even if no tables were created in the meantime.
				if err := checkForNewCompleteDatabases(targetDescs, completeDBs, dbsInPrev); err != nil {
					return err
				}
			}

			var err error
			_, coveredTime, err := makeImportSpans(
				spans,
				prevBackups,
				nil, /*backupLocalityInfo*/
				keys.MinKey,
				p.User(),
				func(span covering.Range, start, end hlc.Timestamp) error {
					if start.IsEmpty() {
						newSpans = append(newSpans, roachpb.Span{Key: span.Start, EndKey: span.End})
						return nil
					}
					return errOnMissingRange(span, start, end)
				},
			)
			if err != nil {
				return errors.Wrap(err, "invalid previous backups")
			}
			if coveredTime != startTime {
				return errors.Wrapf(err, "expected previous backups to cover until time %v, got %v", startTime, coveredTime)
			}
		}

		// if CompleteDbs is lost by a 1.x node, FormatDescriptorTrackingVersion
		// means that a 2.0 node will disallow `RESTORE DATABASE foo`, but `RESTORE
		// foo.table1, foo.table2...` will still work. MVCCFilter would be
		// mis-handled, but is disallowed above. IntroducedSpans may also be lost by
		// a 1.x node, meaning that if 1.1 nodes may resume a backup, the limitation
		// of requiring full backups after schema changes remains.
		descriptorProtos := make([]descpb.Descriptor, 0, len(targetDescs))
		for _, desc := range targetDescs {
			descriptorProtos = append(descriptorProtos, *desc.DescriptorProto())
		}

		backupManifest := BackupManifest{
			StartTime:           startTime,
			EndTime:             endTime,
			MVCCFilter:          mvccFilter,
			Descriptors:         descriptorProtos,
			Tenants:             tenants,
			DescriptorChanges:   revs,
			CompleteDbs:         completeDBs,
			Spans:               spans,
			IntroducedSpans:     newSpans,
			FormatVersion:       BackupFormatDescriptorTrackingVersion,
			BuildInfo:           build.GetInfo(),
			ClusterID:           p.ExecCfg().ClusterID(),
			StatisticsFilenames: statsFiles,
			DescriptorCoverage:  backupStmt.Coverage(),
		}

		// Sanity check: re-run the validation that RESTORE will do, but this time
		// including this backup, to ensure that the this backup plus any previous
		// backups does cover the interval expected.
		if _, coveredEnd, err := makeImportSpans(
			spans,
			append(prevBackups, backupManifest),
			nil, /*backupLocalityInfo*/
			keys.MinKey,
			p.User(),
			errOnMissingRange,
		); err != nil {
			return err
		} else if coveredEnd != endTime {
			return errors.Errorf("expected backup (along with any previous backups) to cover to %v, not %v", endTime, coveredEnd)
		}

		description, err := backupJobDescription(p, backupStmt.Backup, to, incrementalFrom, encryptionParams.kmsURIs, resolvedSubdir)
		if err != nil {
			return err
		}

		// If we didn't load any prior backups from which get encryption info, we
		// need to generate encryption specific data.
		var encryptionInfo *jobspb.EncryptionInfo
		if encryptionOptions == nil {
			encryptionOptions, encryptionInfo, err = makeNewEncryptionOptions(ctx, encryptionParams)
			if err != nil {
				return err
			}
		}

		defaultStore, err := makeCloudStorage(ctx, defaultURI, p.User())
		if err != nil {
			return err
		}
		defer defaultStore.Close()

		if err := checkForPreviousBackup(ctx, defaultStore, defaultURI); err != nil {
			return err
		}
		// TODO (pbardea): For partitioned backups, also add verification for other
		// stores we are writing to in addition to the default.
		baseURI := collectionURI
		if baseURI == "" {
			baseURI = defaultURI
		}

		// Write backup manifest into a temporary checkpoint file.
		// This accomplishes 2 purposes:
		//  1. Persists large state needed for backup job completion.
		//  2. Verifies we can write to destination location.
		// This temporary checkpoint file gets renamed to real checkpoint
		// file when the backup jobs starts execution.
		doWriteBackupManifestCheckpoint := func(ctx context.Context, jobID int64) error {
			if err := writeBackupManifest(
				ctx, p.ExecCfg().Settings, defaultStore, tempCheckpointFileNameForJob(jobID),
				encryptionOptions, &backupManifest,
			); err != nil {
				return errors.Wrapf(err, "writing checkpoint file")
			}
			return nil
		}

		backupDetails := jobspb.BackupDetails{
			StartTime:         startTime,
			EndTime:           endTime,
			URI:               defaultURI,
			URIsByLocalityKV:  urisByLocalityKV,
			EncryptionOptions: encryptionOptions,
			EncryptionInfo:    encryptionInfo,
			CollectionURI:     collectionURI,
		}
		if len(spans) > 0 && p.ExecCfg().Codec.ForSystemTenant() {
			protectedtsID := uuid.MakeV4()
			backupDetails.ProtectedTimestampRecord = &protectedtsID
		}

		collectTelemetry := func() {
			// sourceSuffix specifies if this schedule was created by a schedule.
			sourceSuffix := ".manual"
			if backupStmt.CreatedByInfo != nil &&
				backupStmt.CreatedByInfo.Name == jobs.CreatedByScheduledJobs {
				sourceSuffix = ".scheduled"
			}

			// countSource emits a telemetry counter and also adds a ".scheduled"
			// suffix if the job was created by a schedule.
			countSource := func(feature string) {
				telemetry.Count(feature + sourceSuffix)
			}

			countSource("backup.total.started")
			if isEnterprise {
				countSource("backup.licensed")
				countSource("backup.using-enterprise-features")
			} else {
				if err := utilccl.CheckEnterpriseEnabled(
					p.ExecCfg().Settings, p.ExecCfg().ClusterID(), p.ExecCfg().Organization(), "",
				); err == nil {
					countSource("backup.licensed")
				} else {
					countSource("backup.free")
				}
			}
			if startTime.IsEmpty() {
				countSource("backup.span.full")
			} else {
				countSource("backup.span.incremental")
				telemetry.CountBucketed("backup.incremental-span-sec", int64(timeutil.Since(startTime.GoTime()).Seconds()))
				if len(incrementalFrom) == 0 {
					countSource("backup.auto-incremental")
				}
			}
			if len(backupStmt.To) > 1 {
				countSource("backup.partitioned")
			}
			if mvccFilter == MVCCFilter_All {
				countSource("backup.revision-history")
			}
			if encryptionOptions != nil {
				countSource("backup.encrypted")
				switch encryptionOptions.Mode {
				case jobspb.EncryptionMode_Passphrase:
					countSource("backup.encryption.passphrase")
				case jobspb.EncryptionMode_KMS:
					countSource("backup.encryption.kms")
				}
			}
			if backupStmt.Nested {
				countSource("backup.nested")
				if backupStmt.AppendToLatest {
					countSource("backup.into-latest")
				}
			}
			if backupStmt.Coverage() == tree.AllDescriptors {
				countSource("backup.targets.full_cluster")
			}
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

		if backupStmt.Options.Detached {
			// When running inside an explicit transaction, we simply create the job
			// record. We do not wait for the job to finish.
			aj, err := p.ExecCfg().JobRegistry.CreateAdoptableJobWithTxn(
				ctx, jr, p.ExtendedEvalContext().Txn)
			if err != nil {
				return err
			}

			if err := doWriteBackupManifestCheckpoint(ctx, *aj.ID()); err != nil {
				return err
			}

			// The protect timestamp logic for a DETACHED BACKUP can be run within the
			// same txn as the BACKUP is being planned in, because we do not wait for
			// the BACKUP job to complete.
			err = protectTimestampForBackup(ctx, p, p.ExtendedEvalContext().Txn, *aj.ID(), spans,
				startTime, endTime, backupDetails)
			if err != nil {
				return err
			}

			resultsCh <- tree.Datums{tree.NewDInt(tree.DInt(*aj.ID()))}
			collectTelemetry()
			return nil
		}

		var sj *jobs.StartableJob
		if err := p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
			sj, err = p.ExecCfg().JobRegistry.CreateStartableJobWithTxn(ctx, jr, txn, resultsCh)
			if err != nil {
				return err
			}
			if err := doWriteBackupManifestCheckpoint(ctx, *sj.ID()); err != nil {
				return err
			}

			return protectTimestampForBackup(ctx, p, txn, *sj.ID(), spans, startTime, endTime,
				backupDetails)
		}); err != nil {
			if sj != nil {
				if cleanupErr := sj.CleanupOnRollback(ctx); cleanupErr != nil {
					log.Warningf(ctx, "failed to cleanup StartableJob: %v", cleanupErr)
				}
			}
			return err
		}

		collectTelemetry()

		return sj.Run(ctx)
	}

	if backupStmt.Options.Detached {
		return fn, utilccl.DetachedJobExecutionResultHeader, nil, false, nil
	}
	return fn, utilccl.BulkJobExecutionResultHeader, nil, false, nil
}

func makeNewEncryptionOptions(
	ctx context.Context, encryptionParams backupEncryptionParams,
) (*jobspb.BackupEncryptionOptions, *jobspb.EncryptionInfo, error) {
	var encryptionOptions *jobspb.BackupEncryptionOptions
	var encryptionInfo *jobspb.EncryptionInfo
	switch encryptionParams.encryptMode {
	case passphrase:
		salt, err := storageccl.GenerateSalt()
		if err != nil {
			return nil, nil, err
		}

		encryptionInfo = &jobspb.EncryptionInfo{Salt: salt}
		encryptionOptions = &jobspb.BackupEncryptionOptions{
			Mode: jobspb.EncryptionMode_Passphrase,
			Key:  storageccl.GenerateKey(encryptionParams.encryptionPassphrase, salt)}
	case kms:
		// Generate a 32 byte/256-bit crypto-random number which will serve as
		// the data key for encrypting the BACKUP data and manifest files.
		plaintextDataKey := make([]byte, 32)
		_, err := cryptorand.Read(plaintextDataKey)
		if err != nil {
			return nil, nil, errors.Wrap(err, "failed to generate DataKey")
		}

		encryptedDataKeyByKMSMasterKeyID, defaultKMSInfo, err :=
			getEncryptedDataKeyByKMSMasterKeyID(ctx, encryptionParams.kmsURIs, plaintextDataKey, encryptionParams.kmsEnv)
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
			KMSInfo: defaultKMSInfo}
	}
	return encryptionOptions, encryptionInfo, nil
}

func protectTimestampForBackup(
	ctx context.Context,
	p sql.PlanHookState,
	txn *kv.Txn,
	jobID int64,
	spans []roachpb.Span,
	startTime, endTime hlc.Timestamp,
	backupDetails jobspb.BackupDetails,
) error {
	if backupDetails.ProtectedTimestampRecord == nil {
		return nil
	}
	if len(spans) > 0 {
		tsToProtect := endTime
		if !startTime.IsEmpty() {
			tsToProtect = startTime
		}
		rec := jobsprotectedts.MakeRecord(*backupDetails.ProtectedTimestampRecord, jobID,
			tsToProtect, spans)
		err := p.ExecCfg().ProtectedTimestampProvider.Protect(ctx, txn, rec)
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

func init() {
	sql.AddPlanHook(backupPlanHook)
}
