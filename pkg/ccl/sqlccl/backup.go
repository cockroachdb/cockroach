// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Cockroach Community Licence (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/pkg/ccl/LICENSE

package sqlccl

import (
	"bytes"
	"io/ioutil"
	"sort"
	"strings"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl/intervalccl"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/pkg/errors"
)

const (
	// BackupDescriptorName is the file name used for serialized
	// BackupDescriptor protos.
	BackupDescriptorName = "BACKUP"
)

// ReadBackupDescriptor reads and unmarshals a BackupDescriptor from given base.
func ReadBackupDescriptor(
	ctx context.Context, dir storageccl.ExportStorage,
) (BackupDescriptor, error) {
	r, err := dir.ReadFile(ctx, BackupDescriptorName)
	if err != nil {
		return BackupDescriptor{}, err
	}
	defer r.Close()
	descBytes, err := ioutil.ReadAll(r)
	if err != nil {
		return BackupDescriptor{}, err
	}
	var backupDesc BackupDescriptor
	if err := backupDesc.Unmarshal(descBytes); err != nil {
		return BackupDescriptor{}, err
	}
	// TODO(dan): Sanity check this BackupDescriptor: non-empty EndTime,
	// non-empty Paths, and non-overlapping Spans and keyranges in Files.
	return backupDesc, nil
}

// ValidatePreviousBackups checks that the timestamps of previous backups are
// consistent. The most recently backed-up time is returned.
//
// TODO(dan): This should call into restoreTablesRequests instead to get the
// full validation logic.
func ValidatePreviousBackups(ctx context.Context, uris []string) (hlc.Timestamp, error) {
	if len(uris) == 0 || len(uris) == 1 && uris[0] == "" {
		// Full backup.
		return hlc.Timestamp{}, nil
	}
	var endTime hlc.Timestamp
	for _, uri := range uris {
		dir, err := storageccl.ExportStorageFromURI(ctx, uri)
		if err != nil {
			return hlc.Timestamp{}, err
		}
		backupDesc, err := ReadBackupDescriptor(ctx, dir)
		if err != nil {
			return hlc.Timestamp{}, err
		}
		// TODO(dan): This check assumes that every backup is of the entire
		// database, which is stricter than it needs to be.
		if backupDesc.StartTime != endTime {
			return hlc.Timestamp{}, errors.Errorf("missing backup between %s and %s in %s",
				endTime, backupDesc.StartTime, dir)
		}
		endTime = backupDesc.EndTime
	}
	return endTime, nil
}

func allSQLDescriptors(txn *client.Txn) ([]sqlbase.Descriptor, error) {
	startKey := roachpb.Key(keys.MakeTablePrefix(keys.DescriptorTableID))
	endKey := startKey.PrefixEnd()
	rows, err := txn.Scan(startKey, endKey, 0)
	if err != nil {
		return nil, err
	}

	sqlDescs := make([]sqlbase.Descriptor, len(rows))
	for i, row := range rows {
		if err := row.ValueProto(&sqlDescs[i]); err != nil {
			return nil, errors.Wrapf(err, "%s: unable to unmarshal SQL descriptor", row.Key)
		}
	}
	return sqlDescs, nil
}

// spansForAllTableIndexes returns non-overlapping spans for every index and
// table passed in. They would normally overlap if any of them are interleaved.
func spansForAllTableIndexes(tables []*sqlbase.TableDescriptor) []roachpb.Span {
	sstIntervalTree := interval.Tree{Overlapper: interval.Range.OverlapExclusive}
	for _, table := range tables {
		for _, index := range table.AllNonDropIndexes() {
			startKey := roachpb.Key(sqlbase.MakeIndexKeyPrefix(table, index.ID))
			ie := intervalSpan(roachpb.Span{Key: startKey, EndKey: startKey.PrefixEnd()})
			// Errors are only returned if end <= start, which is never the case
			// here because we use `PrefixEnd` to compute end.
			_ = sstIntervalTree.Insert(ie, false)
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

type backupFileDescriptors []BackupDescriptor_File

func (r backupFileDescriptors) Len() int      { return len(r) }
func (r backupFileDescriptors) Swap(i, j int) { r[i], r[j] = r[j], r[i] }
func (r backupFileDescriptors) Less(i, j int) bool {
	if cmp := bytes.Compare(r[i].Span.Key, r[j].Span.Key); cmp != 0 {
		return cmp < 0
	}
	return bytes.Compare(r[i].Span.EndKey, r[j].Span.EndKey) < 0
}

// Backup exports a snapshot of every kv entry into ranged sstables.
//
// The output is an sstable per range with files in the following locations:
// - <dir>/<unique_int>.sst
// - <dir> is given by the user and may be cloud storage
// - Each file contains data for a key range that doesn't overlap with any other
//   file.
func Backup(
	ctx context.Context,
	db client.DB,
	uri string,
	targets parser.TargetList,
	startTime, endTime hlc.Timestamp,
) (BackupDescriptor, error) {
	// TODO(dan): Figure out how permissions should work. #6713 is tracking this
	// for grpc.

	var sqlDescs []sqlbase.Descriptor

	exportStore, err := storageccl.ExportStorageFromURI(ctx, uri)
	if err != nil {
		return BackupDescriptor{}, err
	}
	defer exportStore.Close()

	{
		txn := client.NewTxn(ctx, db)
		opt := client.TxnExecOptions{AutoRetry: true, AutoCommit: true}
		err := txn.Exec(opt, func(txn *client.Txn, opt *client.TxnExecOptions) error {
			var err error
			sql.SetTxnTimestamps(txn, endTime)
			sqlDescs, err = allSQLDescriptors(txn)
			return err
		})
		if err != nil {
			return BackupDescriptor{}, err
		}
	}

	// TODO(dan): Plumb the session database down.
	sessionDatabase := ""
	if sqlDescs, err = descriptorsMatchingTargets(sessionDatabase, sqlDescs, targets); err != nil {
		return BackupDescriptor{}, err
	}

	// Backup users, descriptors, and the entire keyspace for user data.
	tables := []*sqlbase.TableDescriptor{&sqlbase.DescriptorTable, &sqlbase.UsersTable}
	for _, desc := range sqlDescs {
		if tableDesc := desc.GetTable(); tableDesc != nil {
			tables = append(tables, tableDesc)
		}
	}
	spans := spansForAllTableIndexes(tables)

	mu := struct {
		syncutil.Mutex
		files    []BackupDescriptor_File
		dataSize int64
	}{}

	var wg syncutil.WaitGroupWithError
	header := roachpb.Header{Timestamp: endTime}
	storageConf := exportStore.Conf()
	for i := range spans {
		wg.Add(1)
		go func(span roachpb.Span) {
			req := &roachpb.ExportRequest{
				Span:      span,
				Storage:   storageConf,
				StartTime: startTime,
			}
			res, pErr := client.SendWrappedWith(ctx, db.GetSender(), header, req)
			if pErr != nil {
				wg.Done(pErr.GoError())
				return
			}
			mu.Lock()
			defer mu.Unlock()
			for _, file := range res.(*roachpb.ExportResponse).Files {
				mu.files = append(mu.files, BackupDescriptor_File{
					Span:   file.Span,
					Path:   file.Path,
					Sha512: file.Sha512,
				})
				mu.dataSize += file.DataSize
			}
			wg.Done(nil)
		}(spans[i])
	}
	if err := wg.Wait(); err != nil {
		return BackupDescriptor{}, errors.Wrapf(err, "exporting %d ranges", len(spans))
	}
	files, dataSize := mu.files, mu.dataSize // No more concurrency, so this is safe.

	desc := BackupDescriptor{
		StartTime:   startTime,
		EndTime:     endTime,
		Descriptors: sqlDescs,
		Spans:       spans,
		Files:       files,
		DataSize:    dataSize,
	}
	sort.Sort(backupFileDescriptors(desc.Files))

	descBuf, err := desc.Marshal()
	if err != nil {
		return BackupDescriptor{}, err
	}
	writer, err := exportStore.PutFile(ctx, BackupDescriptorName)
	if err != nil {
		return BackupDescriptor{}, err
	}
	defer writer.Cleanup()
	if err = ioutil.WriteFile(writer.LocalFile(), descBuf, 0600); err != nil {
		return BackupDescriptor{}, err
	}
	if err := writer.Finish(); err != nil {
		return BackupDescriptor{}, err
	}

	return desc, nil
}

// Import loads some data in sstables into an empty range. Only the keys between
// startKey and endKey are loaded. Every row's key is rewritten to be for
// newTableID.
func Import(
	ctx context.Context,
	db client.DB,
	startKey, endKey roachpb.Key,
	files []roachpb.ImportRequest_File,
	kr storageccl.KeyRewriter,
) error {
	if log.V(1) {
		log.Infof(ctx, "import %s-%s (%d files)", startKey, endKey, len(files))
	}
	if len(files) == 0 {
		return nil
	}

	newStartKey, ok := kr.RewriteKey(append([]byte(nil), startKey...))
	if !ok {
		return errors.Errorf("could not rewrite key: %s", startKey)
	}

	req := &roachpb.ImportRequest{
		// Import is a point request because we don't want DistSender to split
		// it. Assume (but don't require) the entire post-rewrite span is on the
		// same range.
		Span: roachpb.Span{Key: newStartKey},
		DataSpan: roachpb.Span{
			Key:    startKey,
			EndKey: endKey,
		},
		Files:       files,
		KeyRewrites: kr,
	}
	b := &client.Batch{}
	b.AddRawRequest(req)
	return db.Run(ctx, b)
}

func assertDatabasesExist(
	txn *client.Txn,
	databasesByID map[sqlbase.ID]*sqlbase.DatabaseDescriptor,
	tables []*sqlbase.TableDescriptor,
) (map[sqlbase.ID]sqlbase.ID, error) {
	remap := make(map[sqlbase.ID]sqlbase.ID, len(databasesByID))
	for _, table := range tables {
		database, ok := databasesByID[table.ParentID]
		if !ok {
			return nil, errors.Errorf("no database with ID %d for table %q", table.ParentID, table.Name)
		}

		// Make sure there's a database with a name that matches the original.
		existingDatabaseID, err := txn.Get(sqlbase.MakeNameMetadataKey(0, database.Name))
		if err != nil {
			return nil, err
		}
		if existingDatabaseID.Value == nil {
			// TODO(dan): Add the ability to restore the database from backups.
			return nil, errors.Errorf("a database named %q needs to exist to restore table %q",
				database.Name, table.Name)
		}
		newParentID, err := existingDatabaseID.Value.GetInt()
		if err != nil {
			return nil, err
		}
		remap[table.ParentID] = sqlbase.ID(newParentID)

	}
	return remap, nil
}

func newTableIDs(
	ctx context.Context,
	db client.DB,
	databasesByID map[sqlbase.ID]*sqlbase.DatabaseDescriptor,
	tables []*sqlbase.TableDescriptor,
) (map[sqlbase.ID]sqlbase.ID, error) {
	var newTableIDs map[sqlbase.ID]sqlbase.ID
	newTableIDsFunc := func(txn *client.Txn) error {
		newTableIDs = make(map[sqlbase.ID]sqlbase.ID, len(tables))
		for _, table := range tables {
			newTableID, err := sql.GenerateUniqueDescID(txn)
			if err != nil {
				return err
			}
			newTableIDs[table.ID] = newTableID
		}
		return nil
	}
	if err := db.Txn(ctx, newTableIDsFunc); err != nil {
		return nil, err
	}
	return newTableIDs, nil
}

type intervalSpan roachpb.Span

var _ interval.Interface = intervalSpan{}

// ID is part of `interval.Interface` but unused in makeImportRequests.
func (ie intervalSpan) ID() uintptr { return 0 }

// Range is part of `interval.Interface`.
func (ie intervalSpan) Range() interval.Range {
	return interval.Range{Start: []byte(ie.Key), End: []byte(ie.EndKey)}
}

type importEntryType int

const (
	backupSpan importEntryType = iota
	backupFile
	tableSpan
	request
)

type importEntry struct {
	roachpb.Span
	entryType importEntryType

	// Only set if entryType is backupSpan
	backup BackupDescriptor

	// Only set if entryType is backupFile
	dir  roachpb.ExportStorage
	file BackupDescriptor_File

	// Only set if entryType is request
	files []roachpb.ImportRequest_File
}

// makeImportRequests pivots the backups, which are grouped by time, into
// requests for import, which are grouped by keyrange.
//
// The core logic of this is in OverlapCoveringMerge, which accepts sets of
// non-overlapping key ranges (aka coverings) each with a payload, and returns
// them aligned with the payloads in the same order as in the input.
//
// Example (input):
// - [A, C) backup t0 to t1 -> /file1
// - [C, D) backup t0 to t1 -> /file2
// - [A, B) backup t1 to t2 -> /file3
// - [B, C) backup t1 to t2 -> /file4
// - [C, D) backup t1 to t2 -> /file5
// - [B, D) requested table data to be restored
//
// Example (output):
// - [A, B) -> /file1, /file3
// - [B, C) -> /file1, /file4, requested (note that file1 was split into two ranges)
// - [C, D) -> /file2, /file5, requested
//
// This would be turned into two Import requests, one restoring [B, C) out of
// /file1 and /file3, the other restoring [C, D) out of /file2 and /file5.
// Nothing is restored out of /file3 and only part of /file1 is used.
//
// NB: All grouping operates in the pre-rewrite keyspace, meaning the keyranges
// as they were backed up, not as they're being restored.
func makeImportRequests(
	tables []*sqlbase.TableDescriptor, backups []BackupDescriptor,
) ([]importEntry, error) {
	// Put the merged table data covering first into the OverlapCoveringMerge
	// input.
	var tableSpanCovering intervalccl.Covering
	for _, span := range spansForAllTableIndexes(tables) {
		tableSpanCovering = append(tableSpanCovering, intervalccl.Range{
			Start: span.Key,
			End:   span.EndKey,
			Payload: importEntry{
				Span:      span,
				entryType: tableSpan,
			},
		})
	}
	backupCoverings := []intervalccl.Covering{tableSpanCovering}

	// Iterate over backups creating two coverings for each. First the spans
	// that were backed up, then the files in the backup. The latter is a subset
	// when some of the keyranges in the former didn't change since the previous
	// backup. These alternate (backup1 spans, backup1 files, backup2 spans,
	// backup2 files) so they will retain that alternation in the output of
	// OverlapCoveringMerge.
	for _, b := range backups {
		var backupSpanCovering intervalccl.Covering
		for _, s := range b.Spans {
			backupSpanCovering = append(backupSpanCovering, intervalccl.Range{
				Start:   s.Key,
				End:     s.EndKey,
				Payload: importEntry{Span: s, entryType: backupSpan, backup: b},
			})
		}
		backupCoverings = append(backupCoverings, backupSpanCovering)
		var backupFileCovering intervalccl.Covering
		for _, f := range b.Files {
			backupFileCovering = append(backupFileCovering, intervalccl.Range{
				Start: f.Span.Key,
				End:   f.Span.EndKey,
				Payload: importEntry{
					Span:      f.Span,
					entryType: backupFile,
					dir:       b.Dir,
					file:      f,
				},
			})
		}
		backupCoverings = append(backupCoverings, backupFileCovering)
	}

	// Group ranges covered by backups with ones needed to restore the selected
	// tables. Note that this breaks intervals up as necessary to align them.
	// See the function godoc for details.
	importRanges := intervalccl.OverlapCoveringMerge(backupCoverings)

	// Translate the output of OverlapCoveringMerge into requests.
	var requestEntries []importEntry
	for _, importRange := range importRanges {
		needed := false
		var ts hlc.Timestamp
		var files []roachpb.ImportRequest_File
		payloads := importRange.Payload.([]interface{})
		for _, p := range payloads {
			ie := p.(importEntry)
			switch ie.entryType {
			case tableSpan:
				needed = true
			case backupSpan:
				if ts != ie.backup.StartTime {
					return nil, errors.Errorf("mismatched start time %s vs %s", ts, ie.backup.StartTime)
				}
				ts = ie.backup.EndTime
			case backupFile:
				if len(ie.file.Path) > 0 {
					files = append(files, roachpb.ImportRequest_File{
						Dir:    ie.dir,
						Path:   ie.file.Path,
						Sha512: ie.file.Sha512,
					})
				}
			}
		}
		if needed {
			// If needed is false, we have data backed up that is not necessary
			// for this restore. Skip it.
			requestEntries = append(requestEntries, importEntry{
				Span:      roachpb.Span{Key: importRange.Start, EndKey: importRange.End},
				entryType: request,
				files:     files,
			})
		}
	}
	return requestEntries, nil
}

// presplitRanges concurrently creates the splits described by `input`. It does
// this by finding the middle key, splitting and recursively presplitting the
// resulting left and right hand ranges. NB: The split code assumes that the LHS
// of the resulting ranges is the smaller, so normally you'd split from the
// left, but this method should only be called on empty keyranges, so it's okay.
//
// The `input` parameter expected to be sorted.
func presplitRanges(baseCtx context.Context, db client.DB, input []roachpb.Key) error {
	// TODO(dan): This implementation does nothing to control the maximum
	// parallelization or number of goroutines spawned. Revisit (possibly via a
	// semaphore) if this becomes a problem in practice.

	ctx, span := tracing.ChildSpan(baseCtx, "presplitRanges")
	defer tracing.FinishSpan(span)

	if len(input) == 0 {
		return nil
	}

	var wg syncutil.WaitGroupWithError
	var splitFn func([]roachpb.Key)
	splitFn = func(splitPoints []roachpb.Key) {
		// Pick the index such that it's 0 if len(splitPoints) == 1.
		splitIdx := len(splitPoints) / 2
		// AdminSplit requires that the key be a valid table key, which means
		// the last byte is a uvarint indicating how much of the end of the key
		// needs to be stripped off to get the key's row prefix. The start keys
		// input to restore don't have this suffix, so make them row sentinels,
		// which means nothing should be stripped (aka appends 0). See
		// EnsureSafeSplitKey for more context.
		splitKey := append([]byte(nil), splitPoints[splitIdx]...)
		splitKey = keys.MakeRowSentinelKey(splitKey)
		if err := db.AdminSplit(ctx, splitKey); err != nil {
			if !strings.Contains(err.Error(), "already split at") {
				log.Errorf(ctx, "presplitRanges: %+v", err)
				wg.Done(err)
				return
			}
		}

		splitPointsLeft, splitPointsRight := splitPoints[:splitIdx], splitPoints[splitIdx+1:]
		if len(splitPointsLeft) > 0 {
			wg.Add(1)
			go splitFn(splitPointsLeft)
		}
		if len(splitPointsRight) > 0 {
			wg.Add(1)
			// Save a few goroutines by reusing this one.
			splitFn(splitPointsRight)
		}
		wg.Done(nil)
	}

	wg.Add(1)
	splitFn(input)
	return wg.Wait()
}

func restoreTableDescs(
	ctx context.Context,
	db client.DB,
	databasesByID map[sqlbase.ID]*sqlbase.DatabaseDescriptor,
	tables []*sqlbase.TableDescriptor,
	newTableIDs map[sqlbase.ID]sqlbase.ID,
) ([]sqlbase.TableDescriptor, error) {
	ctx, span := tracing.ChildSpan(ctx, "restoreTableDescs")
	defer tracing.FinishSpan(span)

	newTables := make([]sqlbase.TableDescriptor, len(tables))
	restoreTableDescsFunc := func(txn *client.Txn) error {
		// Recheck that the necessary databases exist. This was checked at the
		// beginning, but check again in case one was deleted or renamed during
		// the data import.
		newDBs, err := assertDatabasesExist(txn, databasesByID, tables)
		if err != nil {
			return err
		}

		for i := range tables {
			newTables[i] = *tables[i]
			newTableID, ok := newTableIDs[newTables[i].ID]
			if !ok {
				return errors.Errorf("missing table ID for %d %q", newTables[i].ID, newTables[i].Name)
			}
			newTables[i].ID = newTableID
			newTables[i].ParentID = newDBs[tables[i].ParentID]

			if err := newTables[i].ForeachNonDropIndex(func(index *sqlbase.IndexDescriptor) error {
				// TODO(dan): We need this sort of logic for FKs, too.

				for j := range index.Interleave.Ancestors {
					newTableID, ok = newTableIDs[index.Interleave.Ancestors[j].TableID]
					if !ok {
						return errors.Errorf("not restoring %d", index.Interleave.Ancestors[j].TableID)
					}
					index.Interleave.Ancestors[j].TableID = newTableID
				}

				oldInterleavedBy := index.InterleavedBy
				index.InterleavedBy = nil
				for _, ib := range oldInterleavedBy {
					if newTableID, ok = newTableIDs[ib.Table]; ok {
						newIB := ib
						newIB.Table = newTableID
						index.InterleavedBy = append(index.InterleavedBy, newIB)
					}
				}

				return nil
			}); err != nil {
				return err
			}

			// Pass the descriptors by value to keep this idempotent.
			if err := restoreTableDesc(ctx, txn, newTables[i]); err != nil {
				return err
			}
		}
		for _, newTable := range newTables {
			if err := newTable.Validate(txn); err != nil {
				return err
			}
		}
		return nil
	}
	if err := db.Txn(ctx, restoreTableDescsFunc); err != nil {
		return nil, err
	}
	return newTables, nil
}

func restoreTableDesc(ctx context.Context, txn *client.Txn, table sqlbase.TableDescriptor) error {
	tableIDKey := sqlbase.MakeNameMetadataKey(table.ParentID, table.Name)
	tableDescKey := sqlbase.MakeDescMetadataKey(table.ID)

	// Check for an existing table.
	var existingDesc sqlbase.Descriptor
	existingIDKV, err := txn.Get(tableIDKey)
	if err != nil {
		return err
	}
	if existingIDKV.Value != nil {
		existingID, err := existingIDKV.Value.GetInt()
		if err != nil {
			return err
		}
		existingDescKV, err := txn.Get(sqlbase.MakeDescMetadataKey(sqlbase.ID(existingID)))
		if err != nil {
			return err
		}
		if err := existingDescKV.Value.GetProto(&existingDesc); err != nil {
			return err
		}
	}

	// Write the new descriptors. First the ID -> TableDescriptor for the new
	// table, then flip (or initialize) the name -> ID entry so any new queries
	// will use the new one. If there was an existing table, it can now be
	// cleaned up.
	b := txn.NewBatch()
	b.CPut(tableDescKey, sqlbase.WrapDescriptor(&table), nil)
	if existingTable := existingDesc.GetTable(); existingTable == nil {
		b.CPut(tableIDKey, table.ID, nil)
	} else {
		existingIDKV.Value.ClearChecksum()
		b.CPut(tableIDKey, table.ID, existingIDKV.Value)
		zoneKey, _, descKey := sql.GetKeysForTableDescriptor(existingTable)
		// Delete the desc and zone entries. Leave the name because the new
		// table is using it.
		b.Del(descKey)
		b.Del(zoneKey)
	}
	return txn.Run(b)
}

// Restore imports a SQL table (or tables) from sets of non-overlapping sstable
// files.
func Restore(
	ctx context.Context, db client.DB, uris []string, targets parser.TargetList,
) ([]sqlbase.TableDescriptor, error) {
	backupDescs := make([]BackupDescriptor, len(uris))
	for i, uri := range uris {
		dir, err := storageccl.ExportStorageFromURI(ctx, uri)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to create export storage handler from %q", uri)
		}
		backupDescs[i], err = ReadBackupDescriptor(ctx, dir)
		if err != nil {
			return nil, errors.Wrap(err, "failed to read backup descriptor")
		}
		backupDescs[i].Dir = dir.Conf()
	}
	if len(backupDescs) == 0 {
		return nil, errors.Errorf("no backups found")
	}
	lastBackupDesc := backupDescs[len(backupDescs)-1]

	databasesByID := make(map[sqlbase.ID]*sqlbase.DatabaseDescriptor)
	var tables []*sqlbase.TableDescriptor
	{
		// TODO(dan): Plumb the session database down.
		sessionDatabase := ""
		sqlDescs := lastBackupDesc.Descriptors
		var err error
		if sqlDescs, err = descriptorsMatchingTargets(sessionDatabase, sqlDescs, targets); err != nil {
			return nil, err
		}
		for _, desc := range sqlDescs {
			if dbDesc := desc.GetDatabase(); dbDesc != nil {
				databasesByID[dbDesc.ID] = dbDesc
			} else if tableDesc := desc.GetTable(); tableDesc != nil {
				if tableDesc.ParentID == keys.SystemDatabaseID {
					return nil, errors.Errorf("cannot restore system table: %s", tableDesc.Name)
				}
				tables = append(tables, tableDesc)
			}
		}
		if len(tables) == 0 {
			return nil, errors.Errorf("no tables found: %s", parser.AsString(targets))
		}
	}

	// Fail fast if the necessary databases don't exist since the below logic
	// leaks table IDs when Restore fails.
	if err := db.Txn(ctx, func(txn *client.Txn) error {
		_, err := assertDatabasesExist(txn, databasesByID, tables)
		return err
	}); err != nil {
		return nil, err
	}

	// Assign a new ID for each table. This will leak if Restore later returns
	// an error, but we can't use a KV transaction as restarts would be terrible
	// (and our bulk import primitives are non-transactional).
	//
	// TODO(dan): For now, we're always generating a new ID, but varints get
	// longer as they get bigger and so our keys will, too. We should someday
	// figure out how to reclaim ids.
	newTableIDs, err := newTableIDs(ctx, db, databasesByID, tables)
	if err != nil {
		return nil, errors.Wrapf(err, "reserving %d new table IDs for restore", len(tables))
	}
	kr, err := MakeKeyRewriterForNewTableIDs(tables, newTableIDs)
	if err != nil {
		return nil, errors.Wrapf(err, "creating key rewriter for %d tables", len(tables))
	}

	// Verify that for any interleaved index being restored, the interleave
	// parent is also being restored. Otherwise, the interleave entries in the
	// restored IndexDescriptors won't have anything to point to.
	// TODO(dan): It seems like this restriction could be lifted by restoring
	// stub TableDescriptors for the missing interleave parents.
	for _, table := range tables {
		for _, index := range table.AllNonDropIndexes() {
			for _, a := range index.Interleave.Ancestors {
				if _, ok := newTableIDs[a.TableID]; !ok {
					return nil, errors.Errorf(
						"cannot restore table %q without interleave parent table %d",
						table.Name, a.TableID)
				}
			}
			for _, d := range index.InterleavedBy {
				if _, ok := newTableIDs[d.Table]; !ok {
					return nil, errors.Errorf(
						"cannot restore table %q without interleave child table %d",
						table.Name, d.Table)
				}
			}
		}
	}

	// Pivot the backups, which are grouped by time, into requests for import,
	// which are grouped by keyrange.
	importRequests, err := makeImportRequests(tables, backupDescs)
	if err != nil {
		return nil, errors.Wrapf(err, "making import requests for %d backups", len(backupDescs))
	}

	// The Import (and resulting WriteBatch) requests made below run on
	// leaseholders, so presplit the ranges to balance the work among many
	// nodes.
	splitKeys := make([]roachpb.Key, len(importRequests))
	for i, r := range importRequests {
		var ok bool
		splitKeys[i], ok = kr.RewriteKey(append([]byte(nil), r.Key...))
		if !ok {
			return nil, errors.Errorf("failed to rewrite key: %s", r.Key)
		}
	}
	if err := presplitRanges(ctx, db, splitKeys); err != nil {
		return nil, errors.Wrapf(err, "presplitting %d ranges", len(importRequests))
	}
	// TODO(dan): Wait for the newly created ranges (and leaseholders) to
	// rebalance.

	var wg syncutil.WaitGroupWithError
	for i := range importRequests {
		wg.Add(1)
		go func(i importEntry) {
			wg.Done(Import(ctx, db, i.Key, i.EndKey, i.files, kr))
		}(importRequests[i])
	}
	if err := wg.Wait(); err != nil {
		// This leaves the data that did get imported in case the user wants to
		// retry.
		// TODO(dan): Build tooling to allow a user to restart a failed restore.
		return nil, errors.Wrapf(err, "importing %d ranges", len(importRequests))
	}

	// Write the new TableDescriptors and flip the namespace entries over to
	// them. After this call, any queries on a table will be served by the newly
	// restored data.
	// TODO(dan): Gossip this out and wait for any outstanding leases to expire.
	newTables, err := restoreTableDescs(ctx, db, databasesByID, tables, newTableIDs)
	if err != nil {
		return nil, errors.Wrapf(err, "restoring %d TableDescriptors", len(tables))
	}

	// TODO(dan): Delete any old table data here. The first version of restore
	// assumes that it's operating on a new cluster. If it's not empty,
	// everything works but the table data is left abandoned.

	return newTables, nil
}

func backupPlanHook(
	baseCtx context.Context, stmt parser.Statement, cfg *sql.ExecutorConfig,
) (func() ([]parser.Datums, error), sql.ResultColumns, error) {
	backup, ok := stmt.(*parser.Backup)
	if !ok {
		return nil, nil, nil
	}
	if err := utilccl.CheckEnterpriseEnabled("BACKUP"); err != nil {
		return nil, nil, err
	}
	header := sql.ResultColumns{
		{Name: "to", Typ: parser.TypeString},
		{Name: "startTs", Typ: parser.TypeString},
		{Name: "endTs", Typ: parser.TypeString},
		{Name: "dataSize", Typ: parser.TypeInt},
	}
	fn := func() ([]parser.Datums, error) {
		// TODO(dan): Move this span into sql.
		ctx, span := tracing.ChildSpan(baseCtx, stmt.StatementTag())
		defer tracing.FinishSpan(span)

		var startTime hlc.Timestamp
		if backup.IncrementalFrom != nil {
			var err error
			startTime, err = ValidatePreviousBackups(ctx, backup.IncrementalFrom)
			if err != nil {
				return nil, err
			}
		}
		endTime := cfg.Clock.Now()
		if backup.AsOf.Expr != nil {
			var err error
			if endTime, err = sql.EvalAsOfTimestamp(nil, backup.AsOf, endTime); err != nil {
				return nil, err
			}
		}
		desc, err := Backup(ctx, *cfg.DB, backup.To, backup.Targets, startTime, endTime)
		if err != nil {
			return nil, err
		}
		ret := []parser.Datums{{
			parser.NewDString(backup.To),
			parser.NewDString(startTime.String()),
			parser.NewDString(endTime.String()),
			parser.NewDInt(parser.DInt(desc.DataSize)),
		}}
		return ret, nil
	}
	return fn, header, nil
}

func restorePlanHook(
	baseCtx context.Context, stmt parser.Statement, cfg *sql.ExecutorConfig,
) (func() ([]parser.Datums, error), sql.ResultColumns, error) {
	restore, ok := stmt.(*parser.Restore)
	if !ok {
		return nil, nil, nil
	}
	if err := utilccl.CheckEnterpriseEnabled("RESTORE"); err != nil {
		return nil, nil, err
	}

	fn := func() ([]parser.Datums, error) {
		// TODO(dan): Move this span into sql.
		ctx, span := tracing.ChildSpan(baseCtx, stmt.StatementTag())
		defer tracing.FinishSpan(span)

		_, err := Restore(ctx, *cfg.DB, restore.From, restore.Targets)
		return nil, err
	}
	return fn, nil, nil
}

func init() {
	sql.AddPlanHook(backupPlanHook)
	sql.AddPlanHook(restorePlanHook)
}
