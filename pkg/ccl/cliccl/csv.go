// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/LICENSE

package cliccl

import (
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"time"
	"unicode/utf8"

	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/cli"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

func init() {
	loadCSVCmd := &cobra.Command{
		Use:   "csv --table=name --dest=directory [--data=file [...]] [flags]",
		Short: "convert CSV files into enterprise backup format",
		Long: `
Convert CSV files into enterprise backup format.

The file at table must contain a single CREATE TABLE statement. The
files at the various data options (or table.dat if unspecified) must be
CSV files with data matching the table. A comma is used as a delimiter,
but can be changed with the delimiter option. Lines beginning with
comment are ignored. Fields are considered null if equal to nullif
(may be the empty string).

The backup's tables are created in the "csv" database.

It requires approximately 2x the size of the data files of free disk
space. An intermediate copy will be stored in the OS temp directory,
and the final copy in the dest directory.

For example, if there were a file at /data/names containing:

	CREATE TABLE names (first string, last string)

And a file at /data/names.dat containing:

	James,Kirk
	Leonard,McCoy
	Spock,

Then the file could be converted and saved to /data/backup with:

	cockroach load csv --table '/data/names' --nullif '' --dest '/data/backup'
`,
		RunE: cli.MaybeDecorateGRPCError(runLoadCSV),
	}
	flags := loadCSVCmd.PersistentFlags()
	flags.StringVar(&csvTableName, "table", "", "location of a file containing a single CREATE TABLE statement")
	flags.StringSliceVar(&csvDataNames, "data", nil, "filenames of CSV data; uses <table>.dat if empty")
	flags.StringVar(&csvDest, "dest", "", "destination directory for backup files")
	flags.StringVar(&csvNullIf, "nullif", "", "if specified, the value of NULL; can specify the empty string")
	flags.StringVar(&csvComma, "comma", "", "if specified, the CSV delimiter instead of a comma")
	flags.StringVar(&csvComment, "comment", "", "if specified, the CSV comment character")

	loadCmds := &cobra.Command{
		Use:   "load [command]",
		Short: "loading commands",
		Long:  `Commands for bulk loading external files.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Usage()
		},
	}
	cli.AddCmd(loadCmds)
	loadCmds.AddCommand(loadCSVCmd)
}

var (
	csvComma     string
	csvComment   string
	csvDataNames []string
	csvDest      string
	csvNullIf    string
	csvTableName string
)

func runLoadCSV(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	getRune := func(s string) (rune, error) {
		if s == "" {
			return 0, nil
		}
		r, sz := utf8.DecodeRuneInString(s)
		if r == utf8.RuneError {
			return r, errors.Errorf("invalid character: %s", s)
		}
		if sz != len(s) {
			return r, errors.New("must be only one character")
		}
		return r, nil
	}

	comma, err := getRune(csvComma)
	if err != nil {
		return errors.Wrap(err, "comma flag")
	}
	comment, err := getRune(csvComment)
	if err != nil {
		return errors.Wrap(err, "comment flag")
	}
	var nullIf *string
	// pflags doesn't have an option to have a flag without a default value
	// (which would leave it as nil). Instead, we must iterate through all set
	// flags and detect its presence ourselves.
	cmd.Flags().Visit(func(f *pflag.Flag) {
		if f.Name != "nullif" {
			return
		}
		s := f.Value.String()
		nullIf = &s
	})

	csv, kv, sst, err := loadCSV(ctx, csvTableName, csvDataNames, csvDest, comma, comment, nullIf, batchMaxSizeDefault, sstMaxSizeDefault)
	if err != nil {
		return err
	}
	log.Infof(ctx, "CSV rows read: %d", csv)
	log.Infof(ctx, "KVs pairs created: %d", kv)
	log.Infof(ctx, "SST files written: %d", sst)

	return nil
}

const (
	batchMaxSizeDefault = 1024 * 50
	sstMaxSizeDefault   = 1024 * 1024 * 50
)

func loadCSV(
	ctx context.Context,
	table string,
	dataFiles []string,
	dest string,
	comma, comment rune,
	nullif *string,
	batchMaxSize int,
	sstMaxSize int64,
) (csvCount, kvCount, sstCount int64, err error) {
	if table == "" {
		return 0, 0, 0, errors.New("no table specified")
	}
	if dest == "" {
		return 0, 0, 0, errors.New("no destination specified")
	}
	if comma == 0 {
		comma = ','
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
		// We need to choose arbitrary database and table IDs. These aren't important,
		// but they do match what would happen when creating a new database and
		// table on an empty cluster.
		parentID = keys.MaxReservedDescID + 1
		id       = parentID + 1
	)
	tableDesc, err := sql.MakeTableDesc(
		ctx,
		nil, /* txn */
		sql.NilVirtualTabler,
		nil, /* SearchPath */
		create,
		parentID,
		id,
		sqlbase.NewDefaultPrivilegeDescriptor(),
		nil, /* affected */
		"",  /* sessionDB */
		nil, /* EvalContext */
	)
	if err != nil {
		return 0, 0, 0, errors.Wrap(err, "creating table descriptor")
	}

	rocksdbDest, err := ioutil.TempDir("", "cockroach-csv-rocksdb")
	if err != nil {
		return 0, 0, 0, err
	}
	defer func() {
		if err := os.RemoveAll(rocksdbDest); err != nil {
			log.Infof(ctx, "could not remove temp directory %s: %s", rocksdbDest, err)
		}
	}()

	// Some channels are buffered because reads happen in bursts, so having lots
	// of pre-computed data improves overall performance.
	const chanSize = 10000

	group, gCtx := errgroup.WithContext(ctx)
	recordCh := make(chan csvRecord, chanSize)
	kvCh := make(chan roachpb.KeyValue, chanSize)
	contentCh := make(chan sstContent)
	group.Go(func() error {
		defer close(recordCh)
		var err error
		csvCount, err = readCSV(gCtx, comma, comment, len(tableDesc.VisibleColumns()), dataFiles, recordCh)
		return err
	})
	group.Go(func() error {
		defer close(kvCh)
		return groupWorkers(gCtx, runtime.NumCPU(), func(ctx context.Context) error {
			return convertRecord(ctx, recordCh, kvCh, nullif, &tableDesc)
		})
	})
	group.Go(func() error {
		defer close(contentCh)
		var err error
		kvCount, err = writeRocksDB(gCtx, kvCh, rocksdbDest, sstMaxSize, batchMaxSize, contentCh)
		return err
	})
	group.Go(func() error {
		var err error
		sstCount, err = makeBackup(gCtx, parentID, &tableDesc, dest, contentCh)
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
	recordCh chan<- csvRecord,
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
			cr.Comma = comma
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
				cr := csvRecord{
					r:    record,
					file: dataFile,
					row:  i,
				}
				select {
				case <-done:
					return ctx.Err()
				case recordCh <- cr:
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

// convertRecord converts CSV records KV pairs and sends them on the kvCh chan.
func convertRecord(
	ctx context.Context,
	recordCh <-chan csvRecord,
	kvCh chan<- roachpb.KeyValue,
	nullif *string,
	tableDesc *sqlbase.TableDescriptor,
) error {
	done := ctx.Done()

	visibleCols := tableDesc.VisibleColumns()
	for _, col := range visibleCols {
		if col.DefaultExpr != nil {
			return errors.Errorf("column %q: DEFAULT expression unsupported", col.Name)
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
	var insertErr error
	for record := range recordCh {
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
		insertErr = nil
		if err := ri.InsertRow(ctx, sqlccl.Inserter(func(kv roachpb.KeyValue) {
			select {
			case kvCh <- kv:
			case <-done:
				insertErr = ctx.Err()
			}
		}), row, true /* ignoreConflicts */, false /* traceKV */); err != nil {
			return errors.Wrapf(err, "insert row: %s: row %d", record.file, record.row)
		}
		if insertErr != nil {
			return insertErr
		}
	}
	return nil
}

type sstContent struct {
	data []byte
	size int64
	span roachpb.Span
}

// writeRocksDB writes kvs to a RocksDB instance that is created at
// rocksdbDir. After kvs is closed, sst files are created of size maxSize
// and sent on contents. It returns the number of KV pairs created.
func writeRocksDB(
	ctx context.Context,
	kvCh <-chan roachpb.KeyValue,
	rocksdbDir string,
	sstMaxSize int64,
	batchMaxSize int,
	contentCh chan<- sstContent,
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
	for kv := range kvCh {
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
		if len(b.Repr()) > batchMaxSize {
			if err := b.Commit(false /* sync */); err != nil {
				return 0, err
			}
			b = r.NewBatch()
		}
	}
	if len(b.Repr()) > 0 {
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

	writeSST := func(key, endKey roachpb.Key) error {
		data, err := sst.Finish()
		if err != nil {
			return err
		}
		sc := sstContent{
			data: data,
			size: sst.DataSize,
			span: roachpb.Span{
				Key:    key,
				EndKey: endKey,
			},
		}
		select {
		case contentCh <- sc:
		case <-ctx.Done():
			return ctx.Err()
		}
		sst.Close()
		return nil
	}

	var kv engine.MVCCKeyValue
	var firstKey, lastKey roachpb.Key
	ts := timeutil.Now().UnixNano()
	var count int64
	for it.Seek(engine.MVCCKey{}); ; it.Next() {
		if ok, err := it.Valid(); err != nil {
			return 0, err
		} else if !ok {
			break
		}
		count++

		// Save the first key for the span.
		if firstKey == nil {
			firstKey = make([]byte, len(it.UnsafeKey().Key))
			copy(firstKey, it.UnsafeKey().Key)

			// Ensure the first key doesn't match the last key of the previous SST.
			if firstKey.Equal(lastKey) {
				return 0, errors.Errorf("duplicate key: %s", firstKey)
			}
		}

		kv.Key = it.UnsafeKey()
		kv.Key.Timestamp.WallTime = ts
		kv.Value = it.UnsafeValue()

		if err := sst.Add(kv); err != nil {
			return 0, errors.Wrapf(err, "SST creation error at %s; this can happen when a primary or unique index has duplicate keys", kv.Key.Key)
		}
		if sst.DataSize > sstMaxSize {
			if err := writeSST(firstKey, kv.Key.Key.Next()); err != nil {
				return 0, err
			}
			firstKey = nil
			lastKey = make([]byte, len(kv.Key.Key))
			copy(lastKey, kv.Key.Key)

			sst, err = engine.MakeRocksDBSstFileWriter()
			if err != nil {
				return 0, err
			}
			defer sst.Close()
		}
	}
	if firstKey != nil {
		if err := writeSST(firstKey, kv.Key.Key.Next()); err != nil {
			return 0, err
		}
	}
	return count, nil
}

// makeBackup writes sst files from contents to destDir and creates a backup
// descriptor. It returns the number of SST files written.
func makeBackup(
	ctx context.Context,
	parentID sqlbase.ID,
	tableDesc *sqlbase.TableDescriptor,
	destDir string,
	contentCh <-chan sstContent,
) (int64, error) {
	backupDesc := sqlccl.BackupDescriptor{
		FormatVersion: sqlccl.BackupFormatInitialVersion,
	}

	i := 0
	for sst := range contentCh {
		backupDesc.EntryCounts.DataSize += sst.size
		checksum, err := storageccl.SHA512ChecksumData(sst.data)
		if err != nil {
			return 0, err
		}
		i++
		name := fmt.Sprintf("%d.sst", i)
		path := filepath.Join(destDir, name)
		if err := ioutil.WriteFile(path, sst.data, 0666); err != nil {
			return 0, err
		}

		backupDesc.Files = append(backupDesc.Files, sqlccl.BackupDescriptor_File{
			Path:   name,
			Span:   sst.span,
			Sha512: checksum,
		})
	}
	if len(backupDesc.Files) == 0 {
		return 0, errors.New("no files in backup")
	}

	sort.Sort(sqlccl.BackupFileDescriptors(backupDesc.Files))
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
	err = ioutil.WriteFile(filepath.Join(destDir, sqlccl.BackupDescriptorName), descBuf, 0666)
	return int64(len(backupDesc.Files)), err
}
