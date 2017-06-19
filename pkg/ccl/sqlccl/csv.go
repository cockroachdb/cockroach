// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/LICENSE

package sqlccl

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/cli"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

func init() {
	loadCSVCmd := &cobra.Command{
		Use:   "csv table=name dest=directory [data=file [...]] [comment=comment] [delimiter=delim] [nullif=null] [overwrite=true]",
		Short: "convert a CSV file into enterprise backup format",
		Long: `
The file at table must contain a single CREATE TABLE statement. The
files at the various data options (or table.dat if unspecified) must be
CSV files with data matching the table. A comma is used as a delimiter,
but can be changed with the delimiter option. Lines beginning with
comment are ignored. Fields are considered null if equal to nullif
(may be the empty string). Created files by default do not overwrite
existing files unless overwrite is true.

It requires approximately 2x the size of the data files of free disk
space. An intermediate copy will be stored in the temp directory,
and the final copy in the dest directory.

For example, if there were a file at /data/names containing:

	CREATE TABLE names (first string, last string)

And a file at /data/names.dat containing:

	James,Kirk
	Leonard,McCoy
	Spock,

Then the file could be converted and saved to /data/backup with:

	cockroach load csv table=/data/names nullif= dest=/data/backup
`,
		RunE: cli.MaybeDecorateGRPCError(runLoadCSV),
	}
	loadCmds := &cobra.Command{
		Use:   "load [command]",
		Short: "loading commands",
		Long:  `Various commands for loading external files.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Usage()
		},
	}
	loadCmds.AddCommand(loadCSVCmd)
	cli.AddCmd(loadCmds)
}

func runLoadCSV(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	var (
		comma        rune
		comment      rune
		table        string
		nullif       *string
		data         []string
		dest         string
		canOverwrite bool
	)

	seen := make(map[string]bool)
	getRune := func(s string) (rune, error) {
		r, sz := utf8.DecodeRuneInString(s)
		if r == utf8.RuneError || sz != len(s) {
			return r, errors.Errorf("invalid delimiter: %s", s)
		}
		return r, nil
	}

	var err error
	for _, arg := range args {
		parts := strings.SplitN(arg, "=", 2)
		if len(parts) != 2 {
			return errors.Errorf("malformed option: %s", arg)
		}
		name := strings.ToLower(parts[0])
		if seen[name] && name != "data" {
			return errors.Errorf("%s already set", parts[0])
		}
		seen[name] = true
		switch name {
		case "delimiter":
			comma, err = getRune(parts[1])
			if err != nil {
				return errors.Wrapf(err, "could not parse %s", arg)
			}
		case "comment":
			comment, err = getRune(parts[1])
			if err != nil {
				return errors.Wrapf(err, "could not parse %s", arg)
			}
		case "table":
			table = parts[1]
		case "data":
			data = append(data, strings.Split(parts[1], ",")...)
		case "nullif":
			nullif = &parts[1]
		case "dest":
			dest = parts[1]
		case "overwrite":
			var err error
			canOverwrite, err = strconv.ParseBool(parts[1])
			if err != nil {
				return errors.Wrapf(err, "could not parse %s", arg)
			}
		default:
			return errors.Errorf("unknown option %s", parts[0])
		}
	}

	csv, kv, sst, err := loadCSV(ctx, table, data, dest, comma, comment, nullif, canOverwrite)
	if err != nil {
		return err
	}
	log.Infof(ctx, "CSV rows read: %d", csv)
	log.Infof(ctx, "KVs pairs created: %d", kv)
	log.Infof(ctx, "SST files written: %d", sst)

	return nil
}

func loadCSV(
	ctx context.Context,
	table string,
	dataFiles []string,
	dest string,
	comma, comment rune,
	nullif *string,
	canOverwrite bool,
) (csvCount, kvCount, sstCount int64, err error) {
	if table == "" {
		return 0, 0, 0, errors.New("no table specified")
	}
	if dest == "" {
		return 0, 0, 0, errors.New("no destination specified")
	}
	if len(dataFiles) == 0 {
		dataFiles = []string{fmt.Sprintf("%s.dat", table)}
	}
	tableDefStr, err := ioutil.ReadFile(table)
	if err != nil {
		return 0, 0, 0, err
	}
	stmt, err := parser.ParseOne(string(tableDefStr))
	if err != nil {
		return 0, 0, 0, err
	}
	create, ok := stmt.(*parser.CreateTable)
	if !ok {
		return 0, 0, 0, errors.New("expected CREATE TABLE statement in table file")
	}
	if create.IfNotExists {
		return 0, 0, 0, errors.New("unsupported IF NOT EXISTS")
	}
	if create.Interleave != nil {
		return 0, 0, 0, errors.New("interleaved not supported")
	}
	if create.AsSource != nil {
		return 0, 0, 0, errors.New("CREATE AS not supported")
	}
	// TODO(mjibson): error on FKs

	const (
		parentID = 50
		id       = 51
	)
	tableDesc, err := sql.MakeTableDesc(ctx, nil /* txn */, sql.NilVirtualTabler, nil /* SearchPath */, create, parentID, id, sqlbase.NewDefaultPrivilegeDescriptor(), nil /* affected */, "" /* sessionDB */, nil /* EvalContext */)
	if err != nil {
		return 0, 0, 0, errors.Wrap(err, "creating table descriptor")
	}

	rocksdbDest, err := ioutil.TempDir("", "cockroach-csv-rocksdb")
	if err != nil {
		return 0, 0, 0, err
	}
	defer func() {
		_ = os.RemoveAll(rocksdbDest)
	}()

	const (
		batchCount = 10000
		sstSize    = 1024 * 1024 * 50
	)

	group, ctx := errgroup.WithContext(ctx)
	records := make(chan csvRecord, batchCount)
	kvs := make(chan roachpb.KeyValue, batchCount)
	contents := make(chan sstContent)
	group.Go(func() error {
		defer close(records)
		var err error
		csvCount, err = readCSV(ctx, comma, comment, len(tableDesc.VisibleColumns()), dataFiles, records)
		return err
	})
	group.Go(func() error {
		defer close(kvs)
		return groupWorkers(ctx, runtime.NumCPU(), func(ctx context.Context) error {
			return convertRecord(ctx, records, kvs, nullif, &tableDesc)
		})
	})
	group.Go(func() error {
		defer close(contents)
		var err error
		kvCount, err = writeRocksDB(ctx, kvs, rocksdbDest, sstSize, batchCount, contents)
		return err
	})
	group.Go(func() error {
		var err error
		sstCount, err = makeBackup(ctx, parentID, &tableDesc, dest, contents, canOverwrite)
		return err
	})
	return csvCount, kvCount, sstCount, group.Wait()
}

// groupWorkers creates num worker go routines in an error group.
func groupWorkers(ctx context.Context, num int, f func(context.Context) error) error {
	group, ctx := errgroup.WithContext(ctx)
	for i := 0; i < num; i++ {
		group.Go(func() error {
			return f(ctx)
		})
	}
	return group.Wait()
}

// readCSV sends records on ch from CSV listed by dataFiles. comma, if
// non-zero, specifies the field separator. comment, if non-zero, specifies
// the comment character. It returns the number of rows read.
func readCSV(
	ctx context.Context,
	comma, comment rune,
	expectedCols int,
	dataFiles []string,
	ch chan<- csvRecord,
) (int64, error) {
	expectedColsExtra := expectedCols + 1
	done := ctx.Done()
	var count int64
	for _, dataFile := range dataFiles {
		select {
		case <-done:
			return 0, ctx.Err()
		default:
		}
		err := func() error {
			f, err := os.Open(dataFile)
			if err != nil {
				return err
			}
			defer f.Close()
			cr := csv.NewReader(f)
			if comma != 0 {
				cr.Comma = comma
			}
			cr.FieldsPerRecord = -1
			cr.LazyQuotes = true
			cr.Comment = comment
			for i := 1; ; i++ {
				record, err := cr.Read()
				if err == io.EOF {
					break
				}
				if err != nil {
					return errors.Wrapf(err, "row %d: reading CSV record", i)
				}
				if len(record) == expectedCols {
					// Expected number of columns.
				} else if len(record) == expectedColsExtra && record[expectedCols] == "" {
					// Line has the optional trailing comma, ignore the empty field.
					record = record[:expectedCols]
				} else {
					return errors.Errorf("row %d: expected %d fields, got %d", i, expectedCols, len(record))
				}
				select {
				case <-done:
					return ctx.Err()
				case ch <- csvRecord{
					r:    record,
					file: dataFile,
					row:  i,
				}:
					count++
				}
			}
			return nil
		}()
		if err != nil {
			return 0, errors.Wrapf(err, dataFile)
		}
	}
	return count, nil
}

type csvRecord struct {
	r    []string
	file string
	row  int
}

// convertRecord converts CSV records KV pairs and sends them on kvs.
func convertRecord(
	ctx context.Context,
	records <-chan csvRecord,
	kvCh chan<- roachpb.KeyValue,
	nullif *string,
	tableDesc *sqlbase.TableDescriptor,
) error {
	done := ctx.Done()

	visibleCols := tableDesc.VisibleColumns()
	for _, col := range visibleCols {
		if col.DefaultExpr != nil {
			return errors.Errorf("column %q: DEFAULT expresion unsupported", col.Name)
		}
	}
	keyDatums := make(sqlbase.EncDatumRow, len(tableDesc.PrimaryIndex.ColumnIDs))
	// keyDatumIdx maps ColumnIDs to indexes in keyDatums.
	keyDatumIdx := make(map[sqlbase.ColumnID]int)
	for _, id := range tableDesc.PrimaryIndex.ColumnIDs {
		for _, col := range visibleCols {
			if col.ID == id {
				keyDatumIdx[id] = len(keyDatumIdx)
				break
			}
		}
	}

	ri, err := sqlbase.MakeRowInserter(nil /* txn */, tableDesc, nil /* fkTables */, tableDesc.Columns, false /* checkFKs */)
	if err != nil {
		return errors.Wrap(err, "make row inserter")
	}

	parse := parser.Parser{}
	evalCtx := parser.EvalContext{}
	cols, defaultExprs, err := sqlbase.ProcessDefaultColumns(tableDesc.Columns, tableDesc, &parse, &evalCtx)
	if err != nil {
		return errors.Wrap(err, "process default columns")
	}

	datums := make([]parser.Datum, len(visibleCols))
	var inserterr error
	for record := range records {
		for i, r := range record.r {
			if nullif != nil && r == *nullif {
				datums[i] = parser.DNull
			} else {
				datums[i], err = parser.ParseStringAs(visibleCols[i].Type.ToDatumType(), r, time.UTC)
				if err != nil {
					return errors.Wrapf(err, "%s: row %d: parse %q as %s", record.file, record.row, visibleCols[i].Name, visibleCols[i].Type.SQLString())
				}
			}
			if idx, ok := keyDatumIdx[visibleCols[i].ID]; ok {
				keyDatums[idx] = sqlbase.DatumToEncDatum(visibleCols[i].Type, datums[i])
			}
		}

		row, err := sql.GenerateInsertRow(defaultExprs, ri.InsertColIDtoRowIndex, cols, evalCtx, tableDesc, datums)
		if err != nil {
			return errors.Wrapf(err, "generate insert row: %s: row %d", record.file, record.row)
		}
		inserterr = nil
		if err := ri.InsertRow(ctx, inserter(func(kv roachpb.KeyValue) {
			select {
			case kvCh <- kv:
			case <-done:
				inserterr = ctx.Err()
			}
		}), row, true /* ignoreConflicts */, false /* traceKV */); err != nil {
			return errors.Wrapf(err, "insert row: %s: row %d", record.file, record.row)
		}
		if inserterr != nil {
			return inserterr
		}
	}
	return nil
}

type sstContent struct {
	content []byte
	size    int64
	span    roachpb.Span
}

// writeRocksDB writes kvs to a RocksDB instance that is created at
// rocksdbDir. After kvs is closed, sst files are created of size maxSize
// and sent on contents. It returns the number of KV pairs created.
func writeRocksDB(
	ctx context.Context,
	kvs <-chan roachpb.KeyValue,
	rocksdbDir string,
	maxSize, batchCount int64,
	contents chan<- sstContent,
) (int64, error) {
	cache := engine.NewRocksDBCache(0)
	defer cache.Release()
	r, err := engine.NewRocksDB(roachpb.Attributes{}, rocksdbDir, cache, 0 /* maxSize */, 1024 /* maxOpenFiles */)
	if err != nil {
		return 0, errors.Wrap(err, "create rocksdb instance")
	}
	defer r.Close()
	b := r.NewBatch()
	var mk engine.MVCCKey
	var bs int64
	for kv := range kvs {
		mk.Key = kv.Key

		// We need to detect duplicate primary/unique keys. This means we can't
		// call .Put with the same keys, because it will overwrite a previous
		// key. Calling .Get before each .Put is very slow. Instead, give each key
		// a unique timestamp. When we iterate in order, this timestamp will be set
		// to a static value, which will cause duplicate keys to error during sst.Add.
		mk.Timestamp.WallTime++

		if err := b.Put(mk, kv.Value.RawBytes); err != nil {
			return 0, err
		}
		bs++
		if bs > batchCount {
			if err := b.Commit(false /* sync */); err != nil {
				return 0, err
			}
			bs = 0
			b = r.NewBatch()
		}
	}
	if bs > 0 {
		if err := b.Commit(false /* sync */); err != nil {
			return 0, err
		}
	}
	it := r.NewIterator(false /* prefix */)
	defer it.Close()
	sst, err := engine.MakeRocksDBSstFileWriter()
	if err != nil {
		return 0, err
	}
	defer sst.Close()

	var kv engine.MVCCKeyValue
	var firstKey roachpb.Key
	writeSST := func() error {
		if firstKey == nil {
			return nil
		}
		content, err := sst.Finish()
		if err != nil {
			return err
		}
		endKey := make(roachpb.Key, len(kv.Key.Key))
		copy(endKey, kv.Key.Key)
		sc := sstContent{
			content: content,
			size:    sst.DataSize,
			span: roachpb.Span{
				Key:    firstKey,
				EndKey: kv.Key.Key.PrefixEnd(),
			},
		}
		select {
		case contents <- sc:
		case <-ctx.Done():
			return ctx.Err()
		}
		sst.Close()
		firstKey = nil
		return nil
	}

	ts := timeutil.Now().UnixNano()
	var count int64
	for it.Seek(engine.MVCCKey{}); ; it.Next() {
		if ok, err := it.Valid(); err != nil {
			return 0, err
		} else if !ok {
			break
		}
		count++
		kv.Key = it.UnsafeKey()
		kv.Key.Timestamp.WallTime = ts
		kv.Value = it.UnsafeValue()

		if err := sst.Add(kv); err != nil {
			return 0, errors.Wrapf(err, "SST creation error at %s; this can happen when a primary or unique index has duplicate values", kv.Key.Key)
		}
		// Save the first key for the span.
		if firstKey == nil {
			firstKey = make([]byte, len(kv.Key.Key))
			copy(firstKey, kv.Key.Key)
		}
		if sst.DataSize > maxSize {
			if err := writeSST(); err != nil {
				return 0, err
			}

			sst, err = engine.MakeRocksDBSstFileWriter()
			if err != nil {
				return 0, err
			}
			defer sst.Close()
		}
	}
	return count, writeSST()
}

// makeBackup writes sst files from contents to destDir and creates a backup
// descriptor. It returns the number of SST files written.
func makeBackup(
	ctx context.Context,
	parentID sqlbase.ID,
	tableDesc *sqlbase.TableDescriptor,
	destDir string,
	contents <-chan sstContent,
	canOverwrite bool,
) (int64, error) {
	backupDesc := BackupDescriptor{
		FormatVersion: BackupFormatInitialVersion,
	}

	i := 0
	for sst := range contents {
		backupDesc.DataSize += sst.size
		checksum, err := storageccl.SHA512ChecksumData(sst.content)
		if err != nil {
			return 0, err
		}
		i++
		name := fmt.Sprintf("%d.sst", i)
		path := filepath.Join(destDir, name)
		if err := writeFile(path, sst.content, canOverwrite); err != nil {
			return 0, err
		}

		backupDesc.Files = append(backupDesc.Files, BackupDescriptor_File{
			Path:   name,
			Span:   sst.span,
			Sha512: checksum,
		})
	}
	if len(backupDesc.Files) == 0 {
		return 0, errors.New("no files in backup")
	}

	sort.Slice(backupDesc.Files, func(i, j int) bool {
		if cmp := bytes.Compare(backupDesc.Files[i].Span.Key, backupDesc.Files[j].Span.Key); cmp != 0 {
			return cmp < 0
		}
		return bytes.Compare(backupDesc.Files[i].Span.EndKey, backupDesc.Files[j].Span.EndKey) < 0
	})
	backupDesc.Spans = []roachpb.Span{
		{
			Key:    backupDesc.Files[0].Span.Key,
			EndKey: backupDesc.Files[len(backupDesc.Files)-1].Span.EndKey,
		},
	}
	backupDesc.Descriptors = []sqlbase.Descriptor{
		*sqlbase.WrapDescriptor(&sqlbase.DatabaseDescriptor{
			Name: "csv",
			ID:   parentID,
		}),
		*sqlbase.WrapDescriptor(tableDesc),
	}
	descBuf, err := backupDesc.Marshal()
	if err != nil {
		return 0, err
	}
	err = writeFile(filepath.Join(destDir, BackupDescriptorName), descBuf, canOverwrite)
	return int64(len(backupDesc.Files)), err
}

// writeFile writes data to filename, but only overwrites it if canOverwrite
// is true. Implementation copied from ioutil.WriteFile.
func writeFile(filename string, data []byte, canOverwrite bool) error {
	flags := os.O_WRONLY | os.O_CREATE
	if canOverwrite {
		flags |= os.O_TRUNC
	} else {
		flags |= os.O_EXCL
	}
	f, err := os.OpenFile(filename, flags, 0666)
	if err != nil {
		return err
	}
	n, err := f.Write(data)
	if err == nil && n < len(data) {
		err = io.ErrShortWrite
	}
	if err1 := f.Close(); err == nil {
		err = err1
	}
	return err
}
