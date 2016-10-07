// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Daniel Harrison (daniel.harrison@gmail.com)

package sql

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/internal/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/pkg/errors"
)

const (
	dataSSTableName      = "data.sst"
	backupDescriptorName = "BACKUP"
)

// AllRangeDescriptors fetches all meta2 RangeDescriptor using the given txn.
func AllRangeDescriptors(txn *client.Txn) ([]roachpb.RangeDescriptor, error) {
	// TODO(dan): Iterate with some batch size.
	rows, err := txn.Scan(keys.Meta2Prefix, keys.MetaMax, 0)
	if err != nil {
		return nil, errors.Wrap(err, "unable to scan range descriptors")
	}

	rangeDescs := make([]roachpb.RangeDescriptor, len(rows))
	for i, row := range rows {
		if err := row.ValueProto(&rangeDescs[i]); err != nil {
			return nil, errors.Wrapf(err, "%s: unable to unmarshal range descriptor", row.Key)
		}
	}
	return rangeDescs, nil
}

func allSQLDescriptors(txn *client.Txn) ([]sqlbase.Descriptor, error) {
	startKey := roachpb.Key(keys.MakeTablePrefix(keys.DescriptorTableID))
	endKey := startKey.PrefixEnd()
	// TODO(dan): Iterate with some batch size.
	rows, err := txn.Scan(startKey, endKey, 0)
	if err != nil {
		return nil, errors.Wrap(err, "unable to scan SQL descriptors")
	}

	sqlDescs := make([]sqlbase.Descriptor, len(rows))
	for i, row := range rows {
		if err := row.ValueProto(&sqlDescs[i]); err != nil {
			return nil, errors.Wrapf(err, "%s: unable to unmarshal SQL descriptor", row.Key)
		}
	}
	return sqlDescs, nil
}

// Backup exports a snapshot of every kv entry into ranged sstables.
//
// The output is an sstable per range with files in the following locations:
// - /<base>/<node_id>/<key_range>/data.sst
// - <base> is given by the user and is expected to eventually be cloud storage
// - The <key_range>s are non-overlapping.
//
// TODO(dan): Bikeshed this directory structure and naming.
func Backup(
	ctx context.Context, db client.DB, base string, endTime hlc.Timestamp,
) (desc sqlbase.BackupDescriptor, retErr error) {
	// TODO(dan): Optionally take a start time for an incremental backup.
	// TODO(dan): Take a uri for the path prefix and support various cloud storages.
	// TODO(dan): Figure out how permissions should work. #6713 is tracking this
	// for grpc.

	var rangeDescs []roachpb.RangeDescriptor
	var sqlDescs []sqlbase.Descriptor

	opt := client.TxnExecOptions{
		AutoRetry:  true,
		AutoCommit: true,
	}

	{
		// TODO(dan): Pick an appropriate end time and set it in the txn.
		txn := client.NewTxn(ctx, db)
		err := txn.Exec(opt, func(txn *client.Txn, opt *client.TxnExecOptions) error {
			var err error
			setTxnTimestamps(txn, endTime)

			rangeDescs, err = AllRangeDescriptors(txn)
			if err != nil {
				return err
			}
			sqlDescs, err = allSQLDescriptors(txn)
			return err
		})
		if err != nil {
			return sqlbase.BackupDescriptor{}, err
		}
	}

	var dataSize int64
	backupDescs := make([]sqlbase.BackupRangeDescriptor, len(rangeDescs))
	crc := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	for i, rangeDesc := range rangeDescs {
		backupDescs[i] = sqlbase.BackupRangeDescriptor{
			StartKey:  rangeDesc.StartKey.AsRawKey(),
			EndKey:    rangeDesc.EndKey.AsRawKey(),
			StartTime: hlc.Timestamp{},
		}
		if backupDescs[i].StartKey.Compare(keys.LocalMax) < 0 {
			backupDescs[i].StartKey = keys.LocalMax
		}

		nodeID := 0
		dir := filepath.Join(base, fmt.Sprintf("%03d", nodeID))
		dir = filepath.Join(dir, fmt.Sprintf("%x-%x", rangeDesc.StartKey, rangeDesc.EndKey))
		if err := os.MkdirAll(dir, 0700); err != nil {
			return sqlbase.BackupDescriptor{}, err
		}

		var kvs []client.KeyValue

		txn := client.NewTxn(ctx, db)
		err := txn.Exec(opt, func(txn *client.Txn, opt *client.TxnExecOptions) error {
			var err error
			setTxnTimestamps(txn, endTime)

			// TODO(dan): Iterate with some batch size.
			kvs, err = txn.Scan(backupDescs[i].StartKey, backupDescs[i].EndKey, 0)
			return err
		})
		if err != nil {
			return sqlbase.BackupDescriptor{}, err
		}
		if len(kvs) == 0 {
			if log.V(1) {
				log.Infof(ctx, "skipping backup of empty range %s-%s",
					backupDescs[i].StartKey, backupDescs[i].EndKey)
			}
			continue
		}

		backupDescs[i].Path = filepath.Join(dir, dataSSTableName)

		writeSST := func() (writeSSTErr error) {
			// This is a function so the defered Close (and resultant flush) is
			// called before the checksum is computed.
			sst := engine.MakeRocksDBSstFileWriter()
			if err := sst.Open(backupDescs[i].Path); err != nil {
				return err
			}
			defer func() {
				if closeErr := sst.Close(); closeErr != nil && writeSSTErr == nil {
					writeSSTErr = closeErr
				}
			}()
			// TODO(dan): Move all this iteration into cpp to avoid the cgo calls.
			for _, kv := range kvs {
				mvccKV := engine.MVCCKeyValue{
					Key:   engine.MVCCKey{Key: kv.Key, Timestamp: kv.Value.Timestamp},
					Value: kv.Value.RawBytes,
				}
				if err := sst.Add(mvccKV); err != nil {
					return err
				}
			}
			dataSize += sst.DataSize
			return nil
		}
		if err := writeSST(); err != nil {
			return sqlbase.BackupDescriptor{}, err
		}

		crc.Reset()
		f, err := os.Open(backupDescs[i].Path)
		if err != nil {
			return sqlbase.BackupDescriptor{}, err
		}
		defer f.Close()
		if _, err := io.Copy(crc, f); err != nil {
			return sqlbase.BackupDescriptor{}, err
		}
		backupDescs[i].CRC = crc.Sum32()
	}

	desc = sqlbase.BackupDescriptor{
		EndTime:  endTime,
		Ranges:   backupDescs,
		SQL:      sqlDescs,
		DataSize: dataSize,
	}

	descBuf, err := desc.Marshal()
	if err != nil {
		return sqlbase.BackupDescriptor{}, err
	}
	if err = ioutil.WriteFile(filepath.Join(base, backupDescriptorName), descBuf, 0600); err != nil {
		return sqlbase.BackupDescriptor{}, err
	}

	return desc, nil
}

// Ingest loads some data in an sstable into an empty range. Only the keys
// between startKey and endKey are loaded. If newTableID is non-zero, every
// row's key is rewritten to be for that table.
func Ingest(
	ctx context.Context,
	txn *client.Txn,
	path string,
	checksum uint32,
	startKey, endKey roachpb.Key,
	newTableID sqlbase.ID,
) error {
	// TODO(mjibson): An appropriate value for this should be determined. The
	// current value was guessed at but appears to work well.
	const batchSize = 10000

	// TODO(dan): Check if the range being ingested into is empty. If newTableID
	// is non-zero, it'll have to be derived from startKey and endKey.

	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	crc := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	if _, err := io.Copy(crc, f); err != nil {
		return nil
	}
	if c := crc.Sum32(); c != checksum {
		return errors.Errorf("%s: checksum mismatch got %d expected %d", path, c, checksum)
	}

	sst, err := engine.MakeRocksDBSstFileReader()
	if err != nil {
		return err
	}
	defer sst.Close()
	if err := sst.AddFile(path); err != nil {
		return err
	}

	b := txn.NewBatch()
	var v roachpb.Value
	count := 0
	ingestFunc := func(kv engine.MVCCKeyValue) (bool, error) {
		v = roachpb.Value{RawBytes: kv.Value}
		v.ClearChecksum()
		if log.V(3) {
			log.Infof(ctx, "Put %s %s\n", kv.Key.Key, v.PrettyPrint())
		}
		b.Put(kv.Key.Key, &v)
		count++
		if count > batchSize {
			if err := txn.Run(b); err != nil {
				return true, err
			}
			b = txn.NewBatch()
			count = 0
		}
		return false, nil
	}
	if newTableID != 0 {
		// MakeRekeyMVCCKeyValFunc modifies the keys, but this is safe because
		// the one we get back from rocksDBIterator.Key is a copy (not a
		// reference to the mmaped file.)
		ingestFunc = MakeRekeyMVCCKeyValFunc(newTableID, ingestFunc)
	}
	startKeyMVCC, endKeyMVCC := engine.MVCCKey{Key: startKey}, engine.MVCCKey{Key: endKey}
	if err := sst.Iterate(startKeyMVCC, endKeyMVCC, ingestFunc); err != nil {
		return err
	}
	return txn.Run(b)
}

// IntersectHalfOpen returns the common range between two key intervals or
// (nil, nil) if there is no common range. Exported for testing.
func IntersectHalfOpen(start1, end1, start2, end2 []byte) ([]byte, []byte) {
	if bytes.Compare(start1, end2) < 0 && bytes.Compare(start2, end1) < 0 {
		start, end := start1, end1
		if bytes.Compare(start1, start2) < 0 {
			start = start2
		}
		if bytes.Compare(end1, end2) > 0 {
			end = end2
		}
		return start, end
	}
	return nil, nil
}

// restoreTable inserts the given DatabaseDescriptor. If the name conflicts with
// an existing table, the one being restored is rekeyed with a new ID and the
// old data is deleted.
func restoreTable(
	ctx context.Context,
	txn *client.Txn,
	database sqlbase.DatabaseDescriptor,
	table *sqlbase.TableDescriptor,
	ranges []sqlbase.BackupRangeDescriptor,
) error {
	log.Infof(ctx, "Restoring Table %q", table.Name)

	// Make sure there's a database with a name that matches the original.
	existingDatabaseID, err := txn.Get(tableKey{name: database.Name}.Key())
	if err != nil {
		return err
	}
	if existingDatabaseID.Value == nil {
		// TODO(dan): Add the ability to restore the database from backups.
		return errors.Errorf("a database named %q needs to exist to restore table %q",
			database.Name, table.Name)
	}
	newParentID, err := existingDatabaseID.Value.GetInt()
	if err != nil {
		return err
	}
	table.ParentID = sqlbase.ID(newParentID)

	// Create the iteration keys before we give the table its new ID.
	tableStartKeyOld := roachpb.Key(sqlbase.MakeIndexKeyPrefix(table, table.PrimaryIndex.ID))
	tableEndKeyOld := tableStartKeyOld.PrefixEnd()

	// Assign a new ID for the table.
	// TODO(dan): For now, we're always generating a new ID, but varints get
	// longer as they get bigger and so our keys will, too. We should someday
	// figure out how to overwrite an existing table and steal its ID.
	table.ID, err = generateUniqueDescID(txn)
	if err != nil {
		return err
	}
	tableIDKey := tableKey{parentID: table.ParentID, name: table.Name}.Key()
	tableDescKey := sqlbase.MakeDescMetadataKey(table.ID)

	// This loop makes restoring multiple tables O(N*M), where N is the number
	// of tables and M is the number of ranges. We could reduce this using an
	// interval tree if necessary.
	for _, r := range ranges {
		if len(r.Path) == 0 {
			// Empty path means empty range.
			continue
		}

		intersectBegin, intersectEnd := IntersectHalfOpen(
			r.StartKey, r.EndKey, tableStartKeyOld, tableEndKeyOld)
		if intersectBegin != nil && intersectEnd != nil {
			// Write the data under the new ID.
			// TODO(dan): There's no SQL descriptors that point at this yet, so it
			// should be possible to remove it from the one txn this is all currently
			// run under. If we do that, make sure this data gets cleaned up on errors.
			if err := Ingest(ctx, txn, r.Path, r.CRC, intersectBegin, intersectEnd, table.ID); err != nil {
				return err
			}
		}
	}

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
	// will use the new one. If there was an exiting table, it can now be
	// cleaned up.
	b := txn.NewBatch()
	b.CPut(tableDescKey, sqlbase.WrapDescriptor(table), nil)
	if existingTable := existingDesc.GetTable(); existingTable == nil {
		b.CPut(tableIDKey, table.ID, nil)
	} else {
		existingIDKV.Value.ClearChecksum()
		b.CPut(tableIDKey, table.ID, existingIDKV.Value)
		// TODO(dan): This doesn't work for interleaved tables. Fix it when we
		// fix the empty range interleaved table TODO below.
		existingDataPrefix := roachpb.Key(keys.MakeTablePrefix(uint32(existingTable.ID)))
		b.DelRange(existingDataPrefix, existingDataPrefix.PrefixEnd(), false)
		zoneKey, _, descKey := getKeysForTableDescriptor(existingTable)
		// Delete the desc and zone entries. Leave the name because the new
		// table is using it.
		b.Del(descKey)
		b.Del(zoneKey)
	}
	return txn.Run(b)
}

func userTablesAndDBsMatchingName(
	descs []sqlbase.Descriptor, name parser.TableName,
) ([]sqlbase.Descriptor, error) {
	tableName := sqlbase.NormalizeName(name.TableName)
	dbName := sqlbase.NormalizeName(name.DatabaseName)

	matches := make([]sqlbase.Descriptor, 0, len(descs))
	dbIDsToName := make(map[sqlbase.ID]string)
	for _, desc := range descs {
		if db := desc.GetDatabase(); db != nil {
			if db.ID == keys.SystemDatabaseID {
				continue // Not a user database.
			}
			if n := sqlbase.NormalizeName(parser.Name(db.Name)); dbName == "*" || n == dbName {
				matches = append(matches, desc)
				dbIDsToName[db.ID] = n
			}
			continue
		}
	}
	for _, desc := range descs {
		if table := desc.GetTable(); table != nil {
			if _, ok := dbIDsToName[table.ParentID]; !ok {
				continue
			}
			if tableName == "*" || sqlbase.NormalizeName(parser.Name(table.Name)) == tableName {
				matches = append(matches, desc)
			}
		}
	}
	return matches, nil
}

// Restore imports a SQL table (or tables) from a set of non-overlapping sstable
// files.
func Restore(
	ctx context.Context, db client.DB, base string, table parser.TableName,
) ([]sqlbase.TableDescriptor, error) {
	// TODO(dan): It's currently impossible to restore two interleaved tables
	// because one of them won't be to an empty range.

	descBytes, err := ioutil.ReadFile(filepath.Join(base, backupDescriptorName))
	if err != nil {
		return nil, err
	}
	var backupDesc sqlbase.BackupDescriptor
	if err := backupDesc.Unmarshal(descBytes); err != nil {
		return nil, err
	}

	matches, err := userTablesAndDBsMatchingName(backupDesc.SQL, table)
	if err != nil {
		return nil, err
	}
	if len(matches) == 0 {
		return nil, errors.Errorf("no tables found: %q", table)
	}
	databasesByID := make(map[sqlbase.ID]*sqlbase.DatabaseDescriptor)
	for _, desc := range matches {
		if db := desc.GetDatabase(); db != nil {
			databasesByID[db.ID] = db
		}
	}

	// TODO(dan): This uses one giant transaction for the entire restore, which
	// works for small datasets, but not for big ones.
	var restored []sqlbase.TableDescriptor
	err = db.Txn(ctx, func(txn *client.Txn) error {
		for _, desc := range matches {
			if tableOld := desc.GetTable(); tableOld != nil {
				table := *tableOld
				database, ok := databasesByID[table.ParentID]
				if !ok {
					return errors.Wrapf(err, "no database with ID %d", table.ParentID)
				}
				if err := restoreTable(ctx, txn, *database, &table, backupDesc.Ranges); err != nil {
					return err
				}
				restored = append(restored, table)
			}
		}
		return nil
	})
	return restored, err
}

// MakeRekeyMVCCKeyValFunc takes an iterator function for MVCCKeyValues and
// returns a new iterator function where the keys are rewritten inline to the
// have the given table ID.
func MakeRekeyMVCCKeyValFunc(
	newTableID sqlbase.ID, f func(kv engine.MVCCKeyValue) (bool, error),
) func(engine.MVCCKeyValue) (bool, error) {
	encodedNewTableID := encoding.EncodeUvarintAscending(nil, uint64(newTableID))
	return func(kv engine.MVCCKeyValue) (bool, error) {
		if encoding.PeekType(kv.Key.Key) != encoding.Int {
			return false, errors.Errorf("unable to decode table key: %s", kv.Key.Key)
		}
		existingTableIDLen, err := encoding.PeekLength(kv.Key.Key)
		if err != nil {
			return false, err
		}
		if existingTableIDLen == len(encodedNewTableID) {
			copy(kv.Key.Key, encodedNewTableID)
		} else {
			kv.Key.Key = append(encodedNewTableID, kv.Key.Key[existingTableIDLen:]...)
		}
		return f(kv)
	}
}
