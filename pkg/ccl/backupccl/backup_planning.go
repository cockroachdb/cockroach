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
	"io/ioutil"
	"net/url"
	"path"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/covering"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/storage/cloudimpl"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
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

// TODO(pbardea): We should move to a model of having the system tables opt-
// {in,out} of being included in a full cluster backup. See #43781.
var fullClusterSystemTables = []string{
	// System config tables.
	sqlbase.UsersTable.Name,
	sqlbase.ZonesTable.Name,
	sqlbase.SettingsTable.Name,
	// Rest of system tables.
	sqlbase.LocationsTable.Name,
	sqlbase.RoleMembersTable.Name,
	sqlbase.UITable.Name,
	sqlbase.CommentsTable.Name,
	sqlbase.JobsTable.Name,
	// Table statistics are backed up in the backup descriptor for now.
}

type tableAndIndex struct {
	tableID descpb.ID
	indexID descpb.IndexID
}

type backupKMSEnv struct {
	settings *cluster.Settings
	conf     *base.ExternalIODirConfig
}

var _ cloud.KMSEnv = &backupKMSEnv{}

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

// spansForAllTableIndexes returns non-overlapping spans for every index and
// table passed in. They would normally overlap if any of them are interleaved.
func spansForAllTableIndexes(
	codec keys.SQLCodec, tables []sqlbase.TableDescriptor, revs []BackupManifest_DescriptorRevision,
) []roachpb.Span {

	added := make(map[tableAndIndex]bool, len(tables))
	sstIntervalTree := interval.NewTree(interval.ExclusiveOverlapper)
	for _, table := range tables {
		for _, index := range table.AllNonDropIndexes() {
			if err := sstIntervalTree.Insert(intervalSpan(table.IndexSpan(codec, index.ID)), false); err != nil {
				panic(errors.NewAssertionErrorWithWrappedErrf(err, "IndexSpan"))
			}
			added[tableAndIndex{tableID: table.GetID(), indexID: index.ID}] = true
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
		rawTbl := sqlbase.TableFromDescriptor(rev.Desc, hlc.Timestamp{})
		if rawTbl != nil && rawTbl.State != descpb.TableDescriptor_DROP {
			tbl := sqlbase.NewImmutableTableDescriptor(*rawTbl)
			for _, idx := range tbl.AllNonDropIndexes() {
				key := tableAndIndex{tableID: tbl.ID, indexID: idx.ID}
				if !added[key] {
					if err := sstIntervalTree.Insert(intervalSpan(tbl.IndexSpan(codec, idx.ID)), false); err != nil {
						panic(errors.NewAssertionErrorWithWrappedErrf(err, "IndexSpan"))
					}
					added[key] = true
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
	return spans
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
		parsedURI.Path = parsedURI.Path + appendPath
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

func backupJobDescription(
	p sql.PlanHookState, backup *tree.Backup, to []string, incrementalFrom []string, kmsURIs []string,
) (string, error) {
	b := &tree.Backup{
		AsOf:    backup.AsOf,
		Targets: backup.Targets,
	}

	for _, t := range to {
		sanitizedTo, err := cloudimpl.SanitizeExternalStorageURI(t, nil /* extraParams */)
		if err != nil {
			return "", err
		}
		b.To = append(b.To, tree.NewDString(sanitizedTo))
	}

	for _, from := range incrementalFrom {
		sanitizedFrom, err := cloudimpl.SanitizeExternalStorageURI(from, nil /* extraParams */)
		if err != nil {
			return "", err
		}
		b.IncrementalFrom = append(b.IncrementalFrom, tree.NewDString(sanitizedFrom))
	}

	resolvedOpts, err := resolveOptionsForBackupJobDescription(backup.Options, kmsURIs)
	if err != nil {
		return "", err
	}
	b.Options = resolvedOpts

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

// backupPlanHook implements PlanHookFn.
func backupPlanHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, sqlbase.ResultColumns, []sql.PlanNode, bool, error) {
	backupStmt := getBackupStatement(stmt)
	if backupStmt == nil {
		return nil, nil, nil, false, nil
	}

	toFn, err := p.TypeAsStringArray(ctx, tree.Exprs(backupStmt.To), "BACKUP")
	if err != nil {
		return nil, nil, nil, false, err
	}
	incrementalFromFn, err := p.TypeAsStringArray(ctx, backupStmt.IncrementalFrom, "BACKUP")
	if err != nil {
		return nil, nil, nil, false, err
	}

	var pwFn func() (string, error)
	encryptMode := noEncryption
	if backupStmt.Options.EncryptionPassphrase != nil {
		fn, err := p.TypeAsString(ctx, backupStmt.Options.EncryptionPassphrase, "BACKUP")
		if err != nil {
			return nil, nil, nil, false, err
		}
		pwFn = fn
		encryptMode = passphrase
	}

	var kmsFn func() ([]string, *backupKMSEnv, error)
	if backupStmt.Options.EncryptionKMSURI != nil {
		if encryptMode != noEncryption {
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
		encryptMode = kms
	}

	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		// TODO(dan): Move this span into sql.
		ctx, span := tracing.ChildSpan(ctx, stmt.StatementTag())
		defer tracing.FinishSpan(span)

		if err := p.RequireAdminRole(ctx, "BACKUP"); err != nil {
			return err
		}

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

		to, err := toFn()
		if err != nil {
			return err
		}
		if len(to) > 1 {
			if err := requireEnterprise("partitoned destinations"); err != nil {
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
			return err
		}

		var tables []sqlbase.TableDescriptor
		statsFiles := make(map[descpb.ID]string)
		for _, desc := range targetDescs {
			switch desc := desc.(type) {
			case sqlbase.DatabaseDescriptor:
				if err := p.CheckPrivilege(ctx, desc, privilege.SELECT); err != nil {
					return err
				}
			case sqlbase.TableDescriptor:
				if err := p.CheckPrivilege(ctx, desc, privilege.SELECT); err != nil {
					return err
				}
				tables = append(tables, desc)

				// TODO (anzo): look into the tradeoffs of having all objects in the array to be in the same file,
				// vs having each object in a separate file, or somewhere in between.
				statsFiles[desc.GetID()] = BackupStatisticsFileName
			}
		}

		if err := ensureInterleavesIncluded(tables); err != nil {
			return err
		}

		makeCloudStorage := p.ExecCfg().DistSQLSrv.ExternalStorageFromURI

		var encryptionPassphrase []byte
		var kmsURIs []string
		var kmsEnv cloud.KMSEnv
		switch encryptMode {
		case passphrase:
			pw, err := pwFn()
			if err != nil {
				return err
			}
			if err := requireEnterprise("encryption"); err != nil {
				return err
			}
			encryptionPassphrase = []byte(pw)
		case kms:
			kmsURIs, kmsEnv, err = kmsFn()
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

		// TODO(dt): pull this block and the block below into a `resolveDest` helper
		// that does all the rewites of `to`/`defaultURI`/etc.

		// chosenSuffix is the automaically chosen suffix within the collection path
		// if we're backing up INTO a collection.
		var chosenSuffix string
		var collectionURI string
		if backupStmt.Nested {
			collectionURI = defaultURI
			if backupStmt.AppendToLatest {
				collection, err := makeCloudStorage(ctx, collectionURI, p.User())
				if err != nil {
					return err
				}
				defer collection.Close()
				latestFile, err := collection.ReadFile(ctx, latestFileName)
				if err != nil {
					if errors.Is(err, cloudimpl.ErrFileDoesNotExist) {
						return pgerror.Wrapf(err, pgcode.UndefinedFile, "path does not contain a completed latest backup")
					}
					return pgerror.WithCandidateCode(err, pgcode.Io)
				}
				latest, err := ioutil.ReadAll(latestFile)
				if err != nil {
					return err
				}
				if len(latest) == 0 {
					return errors.Errorf("malformed LATEST file")
				}
				chosenSuffix = string(latest)
			} else {
				chosenSuffix = endTime.GoTime().Format(dateBasedFolderName)
			}
			defaultURI, urisByLocalityKV, err = getURIsByLocalityKV(to, chosenSuffix)
			if err != nil {
				return err
			}
		}

		var encryption *jobspb.BackupEncryptionOptions
		var prevBackups []BackupManifest
		g := ctxgroup.WithContext(ctx)
		if len(incrementalFrom) > 0 {
			if encryptMode != noEncryption {
				exportStore, err := makeCloudStorage(ctx, incrementalFrom[0], p.User())
				if err != nil {
					return err
				}
				defer exportStore.Close()
				opts, err := readEncryptionOptions(ctx, exportStore)
				if err != nil {
					return err
				}

				switch encryptMode {
				case passphrase:
					encryption = &jobspb.BackupEncryptionOptions{
						Mode: jobspb.EncryptionMode_Passphrase,
						Key:  storageccl.GenerateKey(encryptionPassphrase, opts.Salt),
					}
				case kms:
					defaultKMSInfo, err := validateKMSURIsAgainstFullBackup(kmsURIs,
						newEncryptedDataKeyMapFromProtoMap(opts.EncryptedDataKeyByKMSMasterKeyID), kmsEnv)
					if err != nil {
						return err
					}
					encryption = &jobspb.BackupEncryptionOptions{
						Mode:    jobspb.EncryptionMode_KMS,
						KMSInfo: defaultKMSInfo}
				}
			}
			prevBackups = make([]BackupManifest, len(incrementalFrom))
			for i := range incrementalFrom {
				i := i
				g.GoCtx(func(ctx context.Context) error {
					// TODO(lucy): We may want to upgrade the table descs to the newer
					// foreign key representation here, in case there are backups from an
					// older cluster. Keeping the descriptors as they are works for now
					// since all we need to do is get the past backups' table/index spans,
					// but it will be safer for future code to avoid having older-style
					// descriptors around.
					uri := incrementalFrom[i]
					desc, err := ReadBackupManifestFromURI(
						ctx, uri, p.User(), makeCloudStorage, encryption,
					)
					if err != nil {
						return errors.Wrapf(err, "failed to read backup from %q", uri)
					}
					prevBackups[i] = desc
					return nil
				})
			}
			if err := g.Wait(); err != nil {
				return err
			}
		} else {
			defaultStore, err := makeCloudStorage(ctx, defaultURI, p.User())
			if err != nil {
				return err
			}
			defer defaultStore.Close()
			exists, err := containsManifest(ctx, defaultStore)
			if err != nil {
				return err
			}
			if exists {
				if encryptMode != noEncryption {
					encOpts, err := readEncryptionOptions(ctx, defaultStore)
					if err != nil {
						return err
					}

					switch encryptMode {
					case passphrase:
						encryption = &jobspb.BackupEncryptionOptions{
							Mode: jobspb.EncryptionMode_Passphrase,
							Key:  storageccl.GenerateKey(encryptionPassphrase, encOpts.Salt),
						}
					case kms:
						defaultKMSInfo, err := validateKMSURIsAgainstFullBackup(kmsURIs,
							newEncryptedDataKeyMapFromProtoMap(encOpts.EncryptedDataKeyByKMSMasterKeyID), kmsEnv)
						if err != nil {
							return err
						}
						encryption = &jobspb.BackupEncryptionOptions{
							Mode:    jobspb.EncryptionMode_KMS,
							KMSInfo: defaultKMSInfo}
					}
				}

				prev, err := findPriorBackups(ctx, defaultStore)
				if err != nil {
					return errors.Wrapf(err, "determining base for incremental backup")
				}
				prevBackups = make([]BackupManifest, len(prev)+1)

				m, err := readBackupManifestFromStore(ctx, defaultStore, encryption)
				if err != nil {
					return errors.Wrap(err, "loading base backup manifest")
				}
				prevBackups[0] = m

				if m.DescriptorCoverage == tree.AllDescriptors &&
					backupStmt.Coverage() != tree.AllDescriptors {
					return errors.Errorf("cannot append a backup of specific tables or databases to a full-cluster backup")
				}

				for i := range prev {
					i := i
					g.GoCtx(func(ctx context.Context) error {
						inc := prev[i]
						m, err := readBackupManifest(ctx, defaultStore, inc, encryption)
						if err != nil {
							return errors.Wrapf(err, "loading prior backup part manifest %q", inc)
						}
						prevBackups[i+1] = m
						return nil
					})
				}
				if err := g.Wait(); err != nil {
					return err
				}

				// Pick a piece-specific suffix and update the destination path(s).
				partName := endTime.GoTime().Format(dateBasedFolderName)
				partName = path.Join(chosenSuffix, partName)
				defaultURI, urisByLocalityKV, err = getURIsByLocalityKV(to, partName)
				if err != nil {
					return errors.Wrap(err, "adjusting backup destination to append new layer to existing backup")
				}
			} else if backupStmt.AppendToLatest {
				// If we came here because the LATEST file told us to but didn't find an
				// existing backup here we should raise an error.
				return pgerror.Newf(pgcode.UndefinedFile, "backup not found in location recorded latest file: %q", chosenSuffix)
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
			revs, err = getRelevantDescChanges(ctx, p.ExecCfg().DB, startTime, endTime, targetDescs, completeDBs, priorIDs)
			if err != nil {
				return err
			}
		}

		var spans []roachpb.Span
		var tenants []BackupManifest_Tenant
		if backupStmt.Targets != nil && backupStmt.Targets.Tenant != (roachpb.TenantID{}) {
			if !p.ExecCfg().Codec.ForSystemTenant() {
				return pgerror.Newf(pgcode.InsufficientPrivilege, "only the system tenant can backup other tenants")
			}
			id := backupStmt.Targets.Tenant.ToUint64()

			res, err := p.ExecCfg().InternalExecutor.QueryRow(
				ctx, "backup-lookup-tenant", p.ExtendedEvalContext().Txn,
				`SELECT active, info FROM system.tenants WHERE id = $1`, id,
			)
			if err != nil {
				return err
			}
			if res == nil {
				return errors.Errorf("tenant %d does not exist", id)
			}
			if !tree.MustBeDBool(res[0]) {
				return errors.Errorf("tenant %d is not active", id)
			}
			var info []byte
			if res[1] != tree.DNull {
				info = []byte(tree.MustBeDBytes(res[1]))
			}

			prefix := keys.MakeTenantPrefix(backupStmt.Targets.Tenant)
			spans = []roachpb.Span{{Key: prefix, EndKey: prefix.PrefixEnd()}}
			tenants = []BackupManifest_Tenant{{ID: id, Info: info}}
		} else {
			spans = spansForAllTableIndexes(p.ExecCfg().Codec, tables, revs)
		}

		if len(prevBackups) > 0 {
			tablesInPrev := make(map[descpb.ID]struct{})
			dbsInPrev := make(map[descpb.ID]struct{})
			rawDescs := prevBackups[len(prevBackups)-1].Descriptors
			for i := range rawDescs {
				if t := sqlbase.TableFromDescriptor(&rawDescs[i], hlc.Timestamp{}); t != nil {
					tablesInPrev[t.ID] = struct{}{}
				}
			}
			for _, d := range prevBackups[len(prevBackups)-1].CompleteDbs {
				dbsInPrev[d] = struct{}{}
			}

			if backupStmt.Coverage() != tree.AllDescriptors {
				if err := checkForNewTables(ctx, p.ExecCfg().DB, targetDescs, tablesInPrev, dbsInPrev, priorIDs, startTime, endTime); err != nil {
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
					if (start == hlc.Timestamp{}) {
						newSpans = append(newSpans, roachpb.Span{Key: span.Start, EndKey: span.End})
						return nil
					}
					return errOnMissingRange(span, start, end)
				},
			)
			if err != nil {
				return errors.Wrapf(err, "invalid previous backups (a new full backup may be required if a table has been created, dropped or truncated)")
			}
			if coveredTime != startTime {
				return errors.Wrapf(err, "expected previous backups to cover until time %v, got %v", startTime, coveredTime)
			}
		}

		nodeID, err := p.ExecCfg().NodeID.OptionalNodeIDErr(47970)
		if err != nil {
			return err
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
			NodeID:              nodeID,
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

		descBytes, err := protoutil.Marshal(&backupManifest)
		if err != nil {
			return err
		}

		description, err := backupJobDescription(p, backupStmt.Backup, to, incrementalFrom, kmsURIs)
		if err != nil {
			return err
		}

		// If we didn't load any prior backups from which get encryption info, we
		// need to generate encryption specific data.
		if encryption == nil {
			exportStore, err := makeCloudStorage(ctx, defaultURI, p.User())
			if err != nil {
				return err
			}
			defer exportStore.Close()

			switch encryptMode {
			case passphrase:
				salt, err := storageccl.GenerateSalt()
				if err != nil {
					return err
				}

				if err := writeEncryptionOptions(ctx, &EncryptionInfo{Salt: salt},
					exportStore); err != nil {
					return err
				}
				encryption = &jobspb.BackupEncryptionOptions{
					Mode: jobspb.EncryptionMode_Passphrase,
					Key:  storageccl.GenerateKey(encryptionPassphrase, salt)}
			case kms:
				// Generate a 32 byte/256-bit crypto-random number which will serve as
				// the data key for encrypting the BACKUP data and manifest files.
				plaintextDataKey := make([]byte, 32)
				_, err := cryptorand.Read(plaintextDataKey)
				if err != nil {
					return errors.Wrap(err, "failed to generate DataKey")
				}

				encryptedDataKeyByKMSMasterKeyID, defaultKMSInfo, err :=
					getEncryptedDataKeyByKMSMasterKeyID(ctx, kmsURIs, plaintextDataKey, kmsEnv)
				if err != nil {
					return err
				}

				encryptedDataKeyMapForProto := make(map[string][]byte)
				encryptedDataKeyByKMSMasterKeyID.rangeOverMap(
					func(masterKeyID hashedMasterKeyID, dataKey []byte) {
						encryptedDataKeyMapForProto[string(masterKeyID)] = dataKey
					})
				if err := writeEncryptionOptions(ctx, &EncryptionInfo{
					EncryptedDataKeyByKMSMasterKeyID: encryptedDataKeyMapForProto,
				}, exportStore); err != nil {
					return err
				}

				encryption = &jobspb.BackupEncryptionOptions{
					Mode:    jobspb.EncryptionMode_KMS,
					KMSInfo: defaultKMSInfo}
			}
		}

		defaultStore, err := makeCloudStorage(ctx, defaultURI, p.User())
		if err != nil {
			return err
		}
		defer defaultStore.Close()

		// TODO (lucy): For partitioned backups, also add verification for other
		// stores we are writing to in addition to the default.
		if err := VerifyUsableExportTarget(
			ctx, p.ExecCfg().Settings, defaultStore, defaultURI, encryption,
		); err != nil {
			return err
		}

		backupDetails := jobspb.BackupDetails{
			StartTime:        startTime,
			EndTime:          endTime,
			URI:              defaultURI,
			URIsByLocalityKV: urisByLocalityKV,
			BackupManifest:   descBytes,
			Encryption:       encryption,
			CollectionURI:    collectionURI,
		}
		if len(spans) > 0 {
			protectedtsID := uuid.MakeV4()
			backupDetails.ProtectedTimestampRecord = &protectedtsID
		}

		collectTelemetry := func() {
			telemetry.Count("backup.total.started")
			if isEnterprise {
				telemetry.Count("backup.licensed")
				telemetry.Count("backup.using-enterprise-features")
			} else {
				if err := utilccl.CheckEnterpriseEnabled(
					p.ExecCfg().Settings, p.ExecCfg().ClusterID(), p.ExecCfg().Organization(), "",
				); err == nil {
					telemetry.Count("backup.licensed")
				} else {
					telemetry.Count("backup.free")
				}
			}
			if startTime.IsEmpty() {
				telemetry.Count("backup.span.full")
			} else {
				telemetry.Count("backup.span.incremental")
				telemetry.CountBucketed("backup.incremental-span-sec", int64(timeutil.Since(startTime.GoTime()).Seconds()))
				if len(incrementalFrom) == 0 {
					telemetry.Count("backup.auto-incremental")
				}
			}
			if len(backupStmt.To) > 1 {
				telemetry.Count("backup.partitioned")
			}
			if mvccFilter == MVCCFilter_All {
				telemetry.Count("backup.revision-history")
			}
			if encryption != nil {
				telemetry.Count("backup.encrypted")
			}
			if backupStmt.Coverage() == tree.AllDescriptors {
				telemetry.Count("backup.targets.full_cluster")
			}
		}

		jr := jobs.Record{
			Description: description,
			Username:    p.User(),
			DescriptorIDs: func() (sqlDescIDs []descpb.ID) {
				for i := range backupManifest.Descriptors {
					sqlDescIDs = append(sqlDescIDs,
						sqlbase.GetDescriptorID(&backupManifest.Descriptors[i]))
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
			err = protectTimestampForBackup(ctx, p, txn, *sj.ID(), spans, startTime, endTime,
				backupDetails)
			return err
		}); err != nil {
			if sj != nil {
				if cleanupErr := sj.CleanupOnRollback(ctx); cleanupErr != nil {
					log.Warningf(ctx, "failed to cleanup StartableJob: %v", cleanupErr)
				}
			}
		}

		collectTelemetry()

		return sj.Run(ctx)
	}

	if backupStmt.Options.Detached {
		return fn, utilccl.DetachedJobExecutionResultHeader, nil, false, nil
	}
	return fn, utilccl.BulkJobExecutionResultHeader, nil, false, nil
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

// checkForNewTables returns an error if any new tables were introduced with the
// following exceptions:
// 1. A previous backup contained the entire DB.
// 2. The table was truncated after a previous backup was taken, so it's ID has
// changed.
func checkForNewTables(
	ctx context.Context,
	db *kv.DB,
	targetDescs []sqlbase.Descriptor,
	tablesInPrev map[descpb.ID]struct{},
	dbsInPrev map[descpb.ID]struct{},
	priorIDs map[descpb.ID]descpb.ID,
	startTime hlc.Timestamp,
	endTime hlc.Timestamp,
) error {
	for _, d := range targetDescs {
		t, ok := d.(sqlbase.TableDescriptor)
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
				_, err := getAllDescChanges(ctx, db, startTime, endTime, priorIDs)
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
