// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/LICENSE

package sampledataccl

import (
	"bytes"
	gosql "database/sql"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/sqlccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

// TODO(dan): Once the interface for this has settled a bit, move it out of ccl.

// Data is a representation of a sql table, used for creating test data in
// various forms (sql rows, kvs, enterprise backup). All data tables live in the
// `data` database.
type Data interface {
	// Name returns the fully-qualified name of the represented table.
	Name() string

	// Schema returns the schema for the represented table, such that it can be
	// used in fmt.Sprintf(`CREATE TABLE %s %s`, data.Name(), data.Schema`())`.
	Schema() string

	// NextRow iterates through the rows in the represented table, returning one
	// per call. The row is represented as a slice of strings, each string one
	// of the columns in the row. When no more rows are available, the bool
	// returned is false.
	NextRow() ([]string, bool)

	// NextRow iterates through the split points in the represented table,
	// returning one per call. The split is represented as a slice of strings,
	// each string one of the columns in the split. When no more splits are
	// available, the bool returned is false.
	NextSplit() ([]string, bool)
}

// InsertBatched inserts all rows represented by `data` into `db` in batches of
// `batchSize` rows. The table must exist.
func InsertBatched(db *gosql.DB, data Data, batchSize int) error {
	var stmt bytes.Buffer
	for {
		stmt.Reset()
		fmt.Fprintf(&stmt, `INSERT INTO %s VALUES `, data.Name())
		i := 0
		done := false
		for ; i < batchSize; i++ {
			row, ok := data.NextRow()
			if !ok {
				done = true
				break
			}
			if i != 0 {
				stmt.WriteRune(',')
			}
			fmt.Fprintf(&stmt, `(%s)`, strings.Join(row, `,`))
		}
		if i > 0 {
			if _, err := db.Exec(stmt.String()); err != nil {
				return err
			}
		}
		if done {
			return nil
		}
		continue
	}
}

// Split creates the configured number of ranges in an already created created
// version of the table represented by `data`.
func Split(db *gosql.DB, data Data) error {
	var stmt bytes.Buffer
	fmt.Fprintf(&stmt, `ALTER TABLE %s SPLIT AT VALUES `, data.Name())
	i := 0
	for ; ; i++ {
		split, ok := data.NextSplit()
		if !ok {
			break
		}
		if i != 0 {
			stmt.WriteRune(',')
		}
		fmt.Fprintf(&stmt, `(%s)`, strings.Join(split, `,`))
	}
	if i > 0 {
		if _, err := db.Exec(stmt.String()); err != nil {
			return err
		}
	}
	return nil
}

// Setup creates a table in `db` with all the rows and splits of `data`.
func Setup(db *gosql.DB, data Data) error {
	if _, err := db.Exec(`CREATE DATABASE IF NOT EXISTS data`); err != nil {
		return err
	}
	if _, err := db.Exec(fmt.Sprintf(`CREATE TABLE %s %s`, data.Name(), data.Schema())); err != nil {
		return err
	}
	const batchSize = 1000
	if err := InsertBatched(db, data, batchSize); err != nil {
		return err
	}
	// This occasionally flakes, so ignore errors.
	_ = Split(db, data)
	return nil
}

// ToBackup creates an enterprise backup in `dir`.
func ToBackup(t testing.TB, data Data, dir string) (*Backup, error) {
	return toBackup(t, data, dir, 0)
}

func toBackup(t testing.TB, data Data, dir string, chunkBytes int64) (*Backup, error) {
	tempDir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()

	// TODO(dan): Get rid of the `t testing.TB` parameter and this `TestServer`.
	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	if _, err := db.Exec(`CREATE DATABASE data`); err != nil {
		return nil, err
	}

	var stmts bytes.Buffer
	fmt.Fprintf(&stmts, "CREATE TABLE %s %s;\n", data.Name(), data.Schema())
	for {
		row, ok := data.NextRow()
		if !ok {
			break
		}
		fmt.Fprintf(&stmts, "INSERT INTO %s VALUES (%s);\n", data.Name(), strings.Join(row, `,`))
	}

	// TODO(dan): The csv load will be less overhead, use it when we have it.
	ts := hlc.Timestamp{WallTime: hlc.UnixNano()}
	desc, err := sqlccl.Load(ctx, db, &stmts, `data`, `nodelocal://`+dir, ts, chunkBytes, tempDir)
	if err != nil {
		return nil, err
	}
	return &Backup{BaseDir: dir, Desc: desc}, nil
}

// Backup is a representation of an enterprise BACKUP.
type Backup struct {
	// BaseDir can be used for a RESTORE. All paths in the descriptor are
	// relative to this.
	BaseDir string
	Desc    sqlccl.BackupDescriptor

	fileIdx int
	iterIdx int
}

// ResetKeyValueIteration resets the NextKeyValues iteration to the first kv.
func (b *Backup) ResetKeyValueIteration() {
	b.fileIdx = 0
	b.iterIdx = 0
}

// NextKeyValues iterates and returns every *user table data* key-value in the
// backup. At least `count` kvs will be returned, but rows are not broken up, so
// slightly more than `count` may come back. If fewer than `count` are
// available, err will be `io.EOF` and kvs may be partially filled with the
// remainer.
func (b *Backup) NextKeyValues(
	count int, newTableID sqlbase.ID,
) ([]engine.MVCCKeyValue, roachpb.Span, error) {
	var userTables []*sqlbase.TableDescriptor
	for _, d := range b.Desc.Descriptors {
		if t := d.GetTable(); t != nil && t.ParentID != keys.SystemDatabaseID {
			userTables = append(userTables, t)
		}
	}
	if len(userTables) != 1 {
		return nil, roachpb.Span{}, errors.Errorf(
			"only backups of one table are currently supported, got %d", len(userTables))
	}
	tableDesc := userTables[0]

	newDesc := *tableDesc
	newDesc.ID = newTableID
	newDescBytes, err := sqlbase.WrapDescriptor(&newDesc).Marshal()
	if err != nil {
		return nil, roachpb.Span{}, err
	}
	kr, err := storageccl.MakeKeyRewriter([]roachpb.ImportRequest_TableRekey{{
		OldID: uint32(tableDesc.ID), NewDesc: newDescBytes,
	}})
	if err != nil {
		return nil, roachpb.Span{}, err
	}

	var kvs []engine.MVCCKeyValue
	span := roachpb.Span{Key: keys.MaxKey}
	for ; b.fileIdx < len(b.Desc.Files); b.fileIdx++ {
		file := b.Desc.Files[b.fileIdx]

		sst := engine.MakeRocksDBSstFileReader()
		defer sst.Close()
		fileContents, err := ioutil.ReadFile(filepath.Join(b.BaseDir, file.Path))
		if err != nil {
			return nil, roachpb.Span{}, err
		}
		if err := sst.IngestExternalFile(fileContents); err != nil {
			return nil, roachpb.Span{}, err
		}

		it := sst.NewIterator(false)
		defer it.Close()
		it.Seek(engine.MVCCKey{Key: file.Span.Key})

		iterIdx := 0
		for ; ; it.Next() {
			if len(kvs) >= count {
				break
			}
			if iterIdx < b.iterIdx {
				iterIdx++
				continue
			}

			ok, err := it.Valid()
			if err != nil {
				return nil, roachpb.Span{}, err
			}
			if !ok || it.UnsafeKey().Key.Compare(file.Span.EndKey) >= 0 {
				break
			}

			if iterIdx < b.iterIdx {
				break
			}
			b.iterIdx = iterIdx

			key := it.Key()
			key.Key, ok, err = kr.RewriteKey(key.Key)
			if err != nil {
				return nil, roachpb.Span{}, err
			}
			if !ok {
				return nil, roachpb.Span{}, errors.Errorf("rewriter did not match key: %s", key.Key)
			}
			v := roachpb.Value{RawBytes: it.Value()}
			v.ClearChecksum()
			v.InitChecksum(key.Key)
			kvs = append(kvs, engine.MVCCKeyValue{Key: key, Value: v.RawBytes})

			if key.Key.Compare(span.Key) < 0 {
				span.Key = append(span.Key[:0], key.Key...)
			}
			if key.Key.Compare(span.EndKey) > 0 {
				span.EndKey = append(span.EndKey[:0], key.Key...)
			}
			iterIdx++
		}
		b.iterIdx = iterIdx

		if len(kvs) >= count {
			break
		}
		if ok, _ := it.Valid(); !ok {
			b.iterIdx = 0
		}
	}

	span.EndKey = span.EndKey.Next()
	if len(kvs) < count {
		return kvs, span, io.EOF
	}
	return kvs, span, nil
}

type bankData struct {
	Rows         int
	PayloadBytes int
	Ranges       int
	Rng          *rand.Rand

	rowIdx   int
	splitIdx int
}

var _ Data = &bankData{}

// bankConfigDefault will trigger the default for any of the parameters for
// `Bank`.
const bankConfigDefault = -1

// BankRows returns Bank testdata with the given number of rows and default
// payload size and range count.
func BankRows(rows int) Data {
	return Bank(rows, bankConfigDefault, bankConfigDefault)
}

// Bank returns a bank table with three columns: an `id INT PRIMARY KEY`
// representing an account number, a `balance` INT, and a `payload` BYTES to pad
// the size of the rows for various tests.
func Bank(rows int, payloadBytes int, ranges int) Data {
	// TODO(dan): This interface is a little wonky, but it's a bit annoying to
	// replace it with a struct because that would make most of the callsites
	// wrap. Complicating things is that there needs to be a distinction between
	// the default value and an explicit 0 for ranges and payloadBytes.
	if rows == bankConfigDefault {
		rows = 1000
	}
	if payloadBytes == bankConfigDefault {
		payloadBytes = 100
	}
	if ranges == bankConfigDefault {
		ranges = 10
	}
	if ranges > rows {
		ranges = rows
	}
	rng, _ := randutil.NewPseudoRand()
	return &bankData{Rows: rows, PayloadBytes: payloadBytes, Ranges: ranges, Rng: rng}
}

// Name implements the Data interface.
func (d *bankData) Name() string {
	return `data.bank`
}

// Schema implements the Data interface.
func (d *bankData) Schema() string {
	return `(
		id INT PRIMARY KEY,
		balance INT,
		payload STRING,
		FAMILY (id, balance, payload)
	)`
}

// NextRow implements the Data interface.
func (d *bankData) NextRow() ([]string, bool) {
	if d.rowIdx >= d.Rows {
		return nil, false
	}
	payload := fmt.Sprintf(`'initial-%x'`, randutil.RandBytes(d.Rng, d.PayloadBytes))
	row := []string{
		strconv.Itoa(d.rowIdx), // id
		`0`,     // balance
		payload, // payload
	}
	d.rowIdx++
	return row, true
}

// NextSplit implements the Data interface.
func (d *bankData) NextSplit() ([]string, bool) {
	if d.splitIdx+1 >= d.Ranges {
		return nil, false
	}
	split := []string{strconv.Itoa((d.splitIdx + 1) * (d.Rows / d.Ranges))}
	d.splitIdx++
	return split, true
}
