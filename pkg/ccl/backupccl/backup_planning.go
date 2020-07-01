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
	"net/url"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/covering"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
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
	backupOptWithPrivileges  = "privileges"
	localityURLParam         = "COCKROACH_LOCALITY"
	defaultLocalityValue     = "default"
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

var useTBI = settings.RegisterBoolSetting(
	"kv.bulk_io_write.experimental_incremental_export_enabled",
	"use experimental time-bound file filter when exporting in BACKUP",
	true,
)

type tableAndIndex struct {
	tableID sqlbase.ID
	indexID sqlbase.IndexID
}

// spansForAllTableIndexes returns non-overlapping spans for every index and
// table passed in. They would normally overlap if any of them are interleaved.
func spansForAllTableIndexes(
	codec keys.SQLCodec,
	tables []sqlbase.TableDescriptorInterface,
	revs []BackupManifest_DescriptorRevision,
) []roachpb.Span {

	added := make(map[tableAndIndex]bool, len(tables))
	sstIntervalTree := interval.NewTree(interval.ExclusiveOverlapper)
	for _, table := range tables {
		tableDesc := table.TableDesc()
		for _, index := range tableDesc.AllNonDropIndexes() {
			if err := sstIntervalTree.Insert(intervalSpan(tableDesc.IndexSpan(codec, index.ID)), false); err != nil {
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
		if tbl := rev.Desc.Table(hlc.Timestamp{}); tbl != nil && tbl.State != sqlbase.TableDescriptor_DROP {
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

func optsToKVOptions(opts map[string]string) tree.KVOptions {
	if len(opts) == 0 {
		return nil
	}
	sortedOpts := make([]string, 0, len(opts))
	for k := range opts {
		sortedOpts = append(sortedOpts, k)
	}
	sort.Strings(sortedOpts)
	kvopts := make(tree.KVOptions, 0, len(opts))
	for _, k := range sortedOpts {
		opt := tree.KVOption{Key: tree.Name(k)}
		if v := opts[k]; v != "" {
			if k == backupOptEncPassphrase {
				v = "redacted"
			}
			opt.Value = tree.NewDString(v)
		}
		kvopts = append(kvopts, opt)
	}
	return kvopts
}

// getURIsByLocalityKV takes a slice of URIs for a single (possibly partitioned)
// backup, and returns the default backup destination URI and a map of all other
// URIs by locality KV, apppending appendPath to the path component of both the
// default URI and all the locality URIs. The URIs in the result do not include
// the COCKROACH_LOCALITY parameter.
func getURIsByLocalityKV(to []string, appendPath string) (string, map[string]string, error) {
	localityAndBaseURI := func(uri string) (string, string, error) {
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

	urisByLocalityKV := make(map[string]string)
	if len(to) == 1 {
		localityKV, baseURI, err := localityAndBaseURI(to[0])
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
		localityKV, baseURI, err := localityAndBaseURI(uri)
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

func backupJobDescription(
	p sql.PlanHookState, backup *tree.Backup, to []string, incrementalFrom []string,
) (string, error) {
	b := &tree.Backup{
		AsOf:               backup.AsOf,
		Options:            backup.Options,
		Targets:            backup.Targets,
		DescriptorCoverage: backup.DescriptorCoverage,
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

	ann := p.ExtendedEvalContext().Annotations
	return tree.AsStringWithFQNames(b, ann), nil
}

// backupPlanHook implements PlanHookFn.
func backupPlanHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, sqlbase.ResultColumns, []sql.PlanNode, bool, error) {
	backupStmt, ok := stmt.(*tree.Backup)
	if !ok {
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
	if backupStmt.Options.EncryptionPassphrase != nil {
		fn, err := p.TypeAsString(ctx, backupStmt.Options.EncryptionPassphrase, "BACKUP")
		if err != nil {
			return nil, nil, nil, false, err
		}
		pwFn = fn
	}

	header := sqlbase.ResultColumns{
		{Name: "job_id", Typ: types.Int},
		{Name: "status", Typ: types.String},
		{Name: "fraction_completed", Typ: types.Float},
		{Name: "rows", Typ: types.Int},
		{Name: "index_entries", Typ: types.Int},
		{Name: "bytes", Typ: types.Int},
	}

	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		// TODO(dan): Move this span into sql.
		ctx, span := tracing.ChildSpan(ctx, stmt.StatementTag())
		defer tracing.FinishSpan(span)

		if err := utilccl.CheckEnterpriseEnabled(
			p.ExecCfg().Settings, p.ExecCfg().ClusterID(), p.ExecCfg().Organization(), "BACKUP",
		); err != nil {
			return err
		}

		if err := p.RequireAdminRole(ctx, "BACKUP"); err != nil {
			return err
		}

		if !p.ExtendedEvalContext().TxnImplicit {
			return errors.Errorf("BACKUP cannot be used inside a transaction")
		}

		to, err := toFn()
		if err != nil {
			return err
		}
		if len(to) > 1 &&
			!p.ExecCfg().Settings.Version.IsActive(ctx, clusterversion.VersionPartitionedBackup) {
			return errors.Errorf("partitioned backups can only be made on a cluster that has been fully upgraded to version 19.2")
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
			mvccFilter = MVCCFilter_All
		}

		targetDescs, completeDBs, err := ResolveTargetsToDescriptors(ctx, p, endTime, backupStmt.Targets, backupStmt.DescriptorCoverage)
		if err != nil {
			return err
		}

		var tables []sqlbase.TableDescriptorInterface
		statsFiles := make(map[sqlbase.ID]string)
		for _, desc := range targetDescs {
			if dbDesc := desc.GetDatabase(); dbDesc != nil {
				db := sqlbase.NewImmutableDatabaseDescriptor(*dbDesc)
				if err := p.CheckPrivilege(ctx, db, privilege.SELECT); err != nil {
					return err
				}
			}
			if tableDesc := desc.Table(hlc.Timestamp{}); tableDesc != nil {
				// TODO(ajwerner): This construction of a wrapper is unfortunate and should
				// go away in this PR.
				table := sqlbase.NewImmutableTableDescriptor(*tableDesc)
				if err := p.CheckPrivilege(ctx, table, privilege.SELECT); err != nil {
					return err
				}
				tables = append(tables, table)

				// TODO (anzo): look into the tradeoffs of having all objects in the array to be in the same file,
				// vs having each object in a separate file, or somewhere in between.
				statsFiles[tableDesc.GetID()] = BackupStatisticsFileName
			}
		}

		if err := ensureInterleavesIncluded(tables); err != nil {
			return err
		}

		makeCloudStorage := p.ExecCfg().DistSQLSrv.ExternalStorageFromURI

		var encryptionPassphrase []byte
		if pwFn != nil {
			pw, err := pwFn()
			if err != nil {
				return err
			}
			encryptionPassphrase = []byte(pw)
		}

		defaultURI, urisByLocalityKV, err := getURIsByLocalityKV(to, "")
		if err != nil {
			return err
		}
		defaultStore, err := makeCloudStorage(ctx, defaultURI, p.User())
		if err != nil {
			return err
		}
		// We can mutate `defaultStore` below so we defer a func which closes over
		// the var, instead of defering the Close() method directly on this specifc
		// instance.
		defer func() {
			defaultStore.Close()
		}()

		var encryption *roachpb.FileEncryptionOptions
		var prevBackups []BackupManifest
		g := ctxgroup.WithContext(ctx)
		if len(incrementalFrom) > 0 {
			if encryptionPassphrase != nil {
				exportStore, err := makeCloudStorage(ctx, incrementalFrom[0], p.User())
				if err != nil {
					return err
				}
				defer exportStore.Close()
				opts, err := readEncryptionOptions(ctx, exportStore)
				if err != nil {
					return err
				}
				encryption = &roachpb.FileEncryptionOptions{
					Key: storageccl.GenerateKey(encryptionPassphrase, opts.Salt),
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
			exists, err := containsManifest(ctx, defaultStore)
			if err != nil {
				return err
			}
			if exists {
				if encryptionPassphrase != nil {
					encOpts, err := readEncryptionOptions(ctx, defaultStore)
					if err != nil {
						return err
					}
					encryption = &roachpb.FileEncryptionOptions{
						Key: storageccl.GenerateKey(encryptionPassphrase, encOpts.Salt),
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
					backupStmt.DescriptorCoverage != tree.AllDescriptors {
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
				partName := endTime.GoTime().Format("/20060102/150405.00")
				defaultURI, urisByLocalityKV, err = getURIsByLocalityKV(to, partName)
				if err != nil {
					return errors.Wrap(err, "adjusting backup destination to append new layer to existing backup")
				}
				// Close the old store before overwriting the reference with the new
				// subdir store.
				defaultStore.Close()
				defaultStore, err = makeCloudStorage(ctx, defaultURI, p.User())
				if err != nil {
					return errors.Wrap(err, "re-opening layer-specific destination location")
				}
				// Note that a Close() is already deferred above.
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
			startTime = prevBackups[len(prevBackups)-1].EndTime
		}

		var priorIDs map[sqlbase.ID]sqlbase.ID

		var revs []BackupManifest_DescriptorRevision
		if mvccFilter == MVCCFilter_All {
			priorIDs = make(map[sqlbase.ID]sqlbase.ID)
			revs, err = getRelevantDescChanges(ctx, p.ExecCfg().DB, startTime, endTime, targetDescs, completeDBs, priorIDs)
			if err != nil {
				return err
			}
		}

		spans := spansForAllTableIndexes(p.ExecCfg().Codec, tables, revs)

		if len(prevBackups) > 0 {
			tablesInPrev := make(map[sqlbase.ID]struct{})
			dbsInPrev := make(map[sqlbase.ID]struct{})
			for _, d := range prevBackups[len(prevBackups)-1].Descriptors {
				if t := d.Table(hlc.Timestamp{}); t != nil {
					tablesInPrev[t.ID] = struct{}{}
				}
			}
			for _, d := range prevBackups[len(prevBackups)-1].CompleteDbs {
				dbsInPrev[d] = struct{}{}
			}

			if backupStmt.DescriptorCoverage != tree.AllDescriptors {
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

		backupManifest := BackupManifest{
			StartTime:           startTime,
			EndTime:             endTime,
			MVCCFilter:          mvccFilter,
			Descriptors:         targetDescs,
			DescriptorChanges:   revs,
			CompleteDbs:         completeDBs,
			Spans:               spans,
			IntroducedSpans:     newSpans,
			FormatVersion:       BackupFormatDescriptorTrackingVersion,
			BuildInfo:           build.GetInfo(),
			NodeID:              nodeID,
			ClusterID:           p.ExecCfg().ClusterID(),
			StatisticsFilenames: statsFiles,
			DescriptorCoverage:  backupStmt.DescriptorCoverage,
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

		description, err := backupJobDescription(p, backupStmt, to, incrementalFrom)
		if err != nil {
			return err
		}

		// If we didn't load any prior backups from which get encryption info, we
		// need to pick a new salt and record it.
		if encryptionPassphrase != nil && encryption == nil {
			salt, err := storageccl.GenerateSalt()
			if err != nil {
				return err
			}
			exportStore, err := makeCloudStorage(ctx, defaultURI, p.User())
			if err != nil {
				return err
			}
			defer exportStore.Close()
			if err := writeEncryptionOptions(ctx, &EncryptionInfo{Salt: salt}, exportStore); err != nil {
				return err
			}
			encryption = &roachpb.FileEncryptionOptions{Key: storageccl.GenerateKey(encryptionPassphrase, salt)}
		}

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
		}
		if len(spans) > 0 {
			protectedtsID := uuid.MakeV4()
			backupDetails.ProtectedTimestampRecord = &protectedtsID
		}

		jr := jobs.Record{
			Description: description,
			Username:    p.User(),
			DescriptorIDs: func() (sqlDescIDs []sqlbase.ID) {
				for _, sqlDesc := range backupManifest.Descriptors {
					sqlDescIDs = append(sqlDescIDs, sqlDesc.GetID())
				}
				return sqlDescIDs
			}(),
			Details:  backupDetails,
			Progress: jobspb.BackupProgress{},
		}
		var sj *jobs.StartableJob
		if err := p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
			sj, err = p.ExecCfg().JobRegistry.CreateStartableJobWithTxn(ctx, jr, txn, resultsCh)
			if err != nil {
				return err
			}
			if len(spans) > 0 {
				tsToProtect := endTime
				rec := jobsprotectedts.MakeRecord(*backupDetails.ProtectedTimestampRecord, *sj.ID(), tsToProtect, spans)
				return p.ExecCfg().ProtectedTimestampProvider.Protect(ctx, txn, rec)
			}
			return nil
		}); err != nil {
			if sj != nil {
				if cleanupErr := sj.CleanupOnRollback(ctx); cleanupErr != nil {
					log.Warningf(ctx, "failed to cleanup StartableJob: %v", cleanupErr)
				}
			}
		}

		// Collect telemetry.
		{
			telemetry.Count("backup.total.started")
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
			if backupStmt.DescriptorCoverage == tree.AllDescriptors {
				telemetry.Count("backup.targets.full_cluster")
			}
		}

		errCh, err := sj.Start(ctx)
		if err != nil {
			return err
		}
		return <-errCh
	}
	return fn, header, nil, false, nil
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
	tablesInPrev map[sqlbase.ID]struct{},
	dbsInPrev map[sqlbase.ID]struct{},
	priorIDs map[sqlbase.ID]sqlbase.ID,
	startTime hlc.Timestamp,
	endTime hlc.Timestamp,
) error {
	for _, d := range targetDescs {
		if t := d.Table(hlc.Timestamp{}); t != nil {
			// If we're trying to use a previous backup for this table, ideally it
			// actually contains this table.
			if _, ok := tablesInPrev[t.ID]; ok {
				continue
			}
			// This table isn't in the previous backup... maybe was added to a
			// DB that the previous backup captured?
			if _, ok := dbsInPrev[t.ParentID]; ok {
				continue
			}
			// Maybe this table is missing from the previous backup because it was
			// truncated?
			if t.ReplacementOf.ID != sqlbase.InvalidID {
				// Check if we need to lazy-load the priorIDs (i.e. if this is the first
				// truncate we've encountered in non-MVCC backup).
				if priorIDs == nil {
					priorIDs = make(map[sqlbase.ID]sqlbase.ID)
					_, err := getAllDescChanges(ctx, db, startTime, endTime, priorIDs)
					if err != nil {
						return err
					}
				}
				found := false
				for was := t.ReplacementOf.ID; was != sqlbase.InvalidID && !found; was = priorIDs[was] {
					_, found = tablesInPrev[was]
				}
				if found {
					continue
				}
			}
			return errors.Errorf("previous backup does not contain table %q", t.Name)
		}
	}
	return nil
}

func init() {
	sql.AddPlanHook(backupPlanHook)
}
