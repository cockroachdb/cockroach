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
	"math/rand"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"

	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/pkg/errors"
)

const (
	importOptionComma       = "comma"
	importOptionComment     = "comment"
	importOptionDistributed = "distributed"
	importOptionNullIf      = "nullif"
	importOptionSSTSize     = "sstsize"
	importOptionTemp        = "temp"
)

// LoadCSV converts CSV files into enterprise backup format.
func LoadCSV(
	ctx context.Context,
	table string,
	dataFiles []string,
	dest string,
	comma, comment rune,
	nullif *string,
	sstMaxSize int64,
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
	createTable, err := readCreateTableFromStore(ctx, fmt.Sprintf("nodelocal://%s", table))
	if err != nil {
		return 0, 0, 0, err
	}

	var parentID = defaultCSVParentID

	tableDesc, err := makeCSVTableDescriptor(ctx, createTable, parentID, defaultCSVTableID)
	if err != nil {
		return 0, 0, 0, err
	}

	for i, f := range dataFiles {
		dataFiles[i] = fmt.Sprintf("nodelocal://%s", f)
	}
	// TODO(mjibson): allow users to optionally specify a full URI to an export store.
	dest = fmt.Sprintf("nodelocal://%s", dest)

	return doLocalCSVTransform(
		ctx, parentID, tableDesc, dest, dataFiles, comma, comment, nullif, sstMaxSize,
	)
}

func doLocalCSVTransform(
	ctx context.Context,
	parentID sqlbase.ID,
	tableDesc *sqlbase.TableDescriptor,
	dest string,
	dataFiles []string,
	comma, comment rune,
	nullif *string,
	sstMaxSize int64,
) (csvCount, kvCount, sstCount int64, err error) {

	// Some channels are buffered because reads happen in bursts, so having lots
	// of pre-computed data improves overall performance.
	const chanSize = 10000

	rocksdbDest, err := ioutil.TempDir("", "cockroach-csv-rocksdb")
	if err != nil {
		return 0, 0, 0, err
	}
	defer func() {
		if err := os.RemoveAll(rocksdbDest); err != nil {
			log.Infof(ctx, "could not remove temp directory %s: %s", rocksdbDest, err)
		}
	}()

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
			return convertRecord(ctx, recordCh, kvCh, nullif, tableDesc)
		})
	})
	group.Go(func() error {
		defer close(contentCh)
		var err error
		kvCount, err = writeRocksDB(gCtx, kvCh, rocksdbDest, sstMaxSize, contentCh)
		return err
	})
	group.Go(func() error {
		var err error
		sstCount, err = makeBackup(gCtx, parentID, tableDesc, dest, contentCh)
		return err
	})
	return csvCount, kvCount, sstCount, group.Wait()
}

const (
	// We need to choose arbitrary database and table IDs. These aren't important,
	// but they do match what would happen when creating a new database and
	// table on an empty cluster.
	defaultCSVParentID sqlbase.ID = keys.MaxReservedDescID + 1
	defaultCSVTableID  sqlbase.ID = defaultCSVParentID + 1
)

func readCreateTableFromStore(ctx context.Context, filename string) (*parser.CreateTable, error) {
	store, err := exportStorageFromURI(ctx, filename)
	if err != nil {
		return nil, err
	}
	defer store.Close()
	reader, err := store.ReadFile(ctx, "")
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	tableDefStr, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	stmt, err := parser.ParseOne(string(tableDefStr))
	if err != nil {
		return nil, err
	}
	create, ok := stmt.(*parser.CreateTable)
	if !ok {
		return nil, errors.New("expected CREATE TABLE statement in table file")
	}
	return create, nil
}

func makeCSVTableDescriptor(
	ctx context.Context, create *parser.CreateTable, parentID, tableID sqlbase.ID,
) (*sqlbase.TableDescriptor, error) {
	if create.IfNotExists {
		return nil, errors.New("unsupported IF NOT EXISTS")
	}
	if create.Interleave != nil {
		return nil, errors.New("interleaved not supported")
	}
	if create.AsSource != nil {
		return nil, errors.New("CREATE AS not supported")
	}
	// TODO(mjibson): error on FKs
	// TODO(mjibson): pass in an appropriate creation time #17526
	tableDesc, err := sql.MakeTableDesc(
		ctx,
		nil, /* txn */
		sql.NilVirtualTabler,
		nil, /* SearchPath */
		create,
		parentID,
		tableID,
		hlc.Timestamp{},
		sqlbase.NewDefaultPrivilegeDescriptor(),
		nil, /* affected */
		"",  /* sessionDB */
		nil, /* EvalContext */
	)
	if err != nil {
		return nil, err
	}

	visibleCols := tableDesc.VisibleColumns()
	for _, col := range visibleCols {
		if col.DefaultExpr != nil {
			return nil, errors.Errorf("column %q: DEFAULT expression unsupported", col.Name)
		}
	}

	return &tableDesc, nil
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
	if comma == 0 {
		comma = ','
	}
	for _, dataFile := range dataFiles {
		select {
		case <-done:
			return 0, ctx.Err()
		default:
		}
		err := func() error {
			conf, err := storageccl.ExportStorageConfFromURI(dataFile)
			if err != nil {
				return err
			}
			es, err := storageccl.MakeExportStorage(ctx, conf)
			if err != nil {
				return err
			}
			defer es.Close()
			f, err := es.ReadFile(ctx, "")
			if err != nil {
				return err
			}
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

	ri, err := sqlbase.MakeRowInserter(nil /* txn */, tableDesc, nil, /* fkTables */
		tableDesc.Columns, false /* checkFKs */, &sqlbase.DatumAlloc{})
	if err != nil {
		return errors.Wrap(err, "make row inserter")
	}

	parse := parser.Parser{}
	evalCtx := parser.EvalContext{}
	// Although we don't yet support DEFAULT expressions on visible columns,
	// we do on hidden columns (which is only the default _rowid one). This
	// allows those expressions to run.
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
		if err := ri.InsertRow(ctx, inserter(func(kv roachpb.KeyValue) {
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
	contentCh chan<- sstContent,
) (int64, error) {
	const batchMaxSize = 1024 * 50

	cache := engine.NewRocksDBCache(0)
	defer cache.Release()
	r, err := engine.NewRocksDB(engine.RocksDBConfig{
		RocksDBSettings: cluster.MakeClusterSettings().RocksDBSettings,
		Dir:             rocksdbDir,
		MaxSizeBytes:    0,
		MaxOpenFiles:    1024,
	}, cache)
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
			firstKey = append([]byte(nil), it.UnsafeKey().Key...)

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
			lastKey = append([]byte(nil), kv.Key.Key...)

			sst, err = engine.MakeRocksDBSstFileWriter()
			if err != nil {
				return 0, err
			}
			defer sst.Close()
		}
	}
	if sst.DataSize > 0 {
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
	backupDesc := BackupDescriptor{
		FormatVersion: BackupFormatInitialVersion,
	}

	conf, err := storageccl.ExportStorageConfFromURI(destDir)
	if err != nil {
		return 0, err
	}
	es, err := storageccl.MakeExportStorage(ctx, conf)
	if err != nil {
		return 0, err
	}
	defer es.Close()

	i := 0
	for sst := range contentCh {
		backupDesc.EntryCounts.DataSize += sst.size
		checksum, err := storageccl.SHA512ChecksumData(sst.data)
		if err != nil {
			return 0, err
		}
		i++
		name := fmt.Sprintf("%d.sst", i)
		if err := es.WriteFile(ctx, name, bytes.NewReader(sst.data)); err != nil {
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

	sort.Sort(backupFileDescriptors(backupDesc.Files))
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
	err = es.WriteFile(ctx, BackupDescriptorName, bytes.NewReader(descBuf))
	return int64(len(backupDesc.Files)), err
}

func importPlanHook(
	stmt parser.Statement, p sql.PlanHookState,
) (func(context.Context, chan<- parser.Datums) error, sqlbase.ResultColumns, error) {
	importStmt, ok := stmt.(*parser.Import)
	if !ok {
		return nil, nil, nil
	}
	// No enterprise check here: IMPORT is always available.
	if err := p.RequireSuperUser("IMPORT"); err != nil {
		return nil, nil, err
	}

	filesFn, err := p.TypeAsStringArray(importStmt.Files, "IMPORT")
	if err != nil {
		return nil, nil, err
	}

	var createFileFn func() (string, error)
	if importStmt.CreateDefs == nil {
		createFileFn, err = p.TypeAsString(importStmt.CreateFile, "IMPORT")
		if err != nil {
			return nil, nil, err
		}
	}

	if importStmt.FileFormat != "CSV" {
		// not possible with current parser rules.
		return nil, nil, errors.Errorf("unsupported import format: %q", importStmt.FileFormat)
	}

	optsFn, err := p.TypeAsStringOpts(importStmt.Options)
	if err != nil {
		return nil, nil, err
	}
	// TODO(dan): This entire method is a placeholder to get the distsql
	// plumbing worked out while mjibson works on the new processors and router.
	// Currently, it "uses" distsql to compute an int and this method returns
	// it.
	header := sqlbase.ResultColumns{
		{Name: "data_size", Typ: parser.TypeInt},
	}
	fn := func(ctx context.Context, resultsCh chan<- parser.Datums) error {
		// TODO(dan): Move this span into sql.
		ctx, span := tracing.ChildSpan(ctx, importStmt.StatementTag())
		defer tracing.FinishSpan(span)

		opts, err := optsFn()
		if err != nil {
			return err
		}

		files, err := filesFn()
		if err != nil {
			return err
		}

		comma := ','
		if override, ok := opts[importOptionComma]; ok {
			comma, err = util.GetSingleRune(override)
			if err != nil {
				return errors.Wrap(err, "invalid comma value")
			}
		}

		var comment rune
		if override, ok := opts[importOptionComment]; ok {
			comment, err = util.GetSingleRune(override)
			if err != nil {
				return errors.Wrap(err, "invalid comment value")
			}
		}

		var nullif *string
		if override, ok := opts[importOptionNullIf]; ok {
			nullif = &override
		}

		var temp string
		if override, ok := opts[importOptionTemp]; ok {
			temp = override
		} else {
			return errors.Errorf("must provide a temporary storage location")
		}

		sstSize := config.DefaultZoneConfig().RangeMaxBytes / 2
		if override, ok := opts[importOptionSSTSize]; ok {
			sz, err := humanizeutil.ParseBytes(override)
			if err != nil {
				return err
			}
			sstSize = sz
		}

		distributed := false
		if override, ok := opts[importOptionDistributed]; ok {
			if override != "" {
				return errors.New("option 'distributed' does not take a value")
			}
			distributed = true
		}

		var create *parser.CreateTable
		if importStmt.CreateDefs != nil {
			normName := parser.NormalizableTableName{TableNameReference: importStmt.Table}
			create = &parser.CreateTable{Table: normName, Defs: importStmt.CreateDefs}
		} else {
			filename, err := createFileFn()
			if err != nil {
				return err
			}
			create, err = readCreateTableFromStore(ctx, filename)
			if err != nil {
				return err
			}
			if named, parsed := importStmt.Table.String(), create.Table.String(); parsed != named {
				return errors.Errorf("importing table %q, but file specifies a schema for table %q", named, parsed)
			}
		}

		parentID := defaultCSVParentID
		tableDesc, err := makeCSVTableDescriptor(ctx, create, parentID, defaultCSVTableID)
		if err != nil {
			return err
		}

		var total int64

		if distributed {

			evalCtx := p.EvalContext()

			// TODO(dan): Filter out unhealthy nodes.
			resp, err := p.ExecCfg().StatusServer.Nodes(ctx, &serverpb.NodesRequest{})
			if err != nil {
				return err
			}

			var nodes []roachpb.NodeDescriptor
			for _, node := range resp.Nodes {
				nodes = append(nodes, node.Desc)
			}

			// TODO(dan/mjibson): Fill in the real planning code.
			ci := sqlbase.ColTypeInfoFromColTypes([]sqlbase.ColumnType{
				{SemanticType: sqlbase.ColumnType_INT},
			})
			rows := sqlbase.NewRowContainer(evalCtx.Mon.MakeBoundAccount(), ci, 0)
			defer func() {
				if rows != nil {
					rows.Close(ctx)
				}
			}()

			if err := p.DistLoader().LoadCSV(
				ctx, p.ExecCfg().DB, evalCtx, p.ExecCfg().NodeID.Get(), nodes, rows, tableDesc, files, temp,
			); err != nil {
				return err
			}
			for i := 0; i < rows.Len(); i++ {
				row := rows.At(i)
				total += int64(*row[0].(*parser.DInt))
			}
		} else {
			_, _, total, err = doLocalCSVTransform(
				ctx, parentID, tableDesc, temp, files,
				comma, comment, nullif, sstSize,
			)
			if err != nil {
				return err
			}
		}

		// TODO(dt): Run restore of temp data.

		resultsCh <- parser.Datums{
			parser.NewDInt(parser.DInt(total)),
		}
		return nil
	}
	return fn, header, nil
}

var csvOutputTypes = []sqlbase.ColumnType{
	{SemanticType: sqlbase.ColumnType_BYTES},
	{SemanticType: sqlbase.ColumnType_BYTES},
}

func newReadCSVProcessor(
	flowCtx *distsqlrun.FlowCtx, spec distsqlrun.ReadCSVSpec, output distsqlrun.RowReceiver,
) (distsqlrun.Processor, error) {
	cp := &readCSVProcessor{
		comma:      spec.Comma,
		comment:    spec.Comment,
		nullif:     spec.Nullif,
		sampleSize: spec.SampleSize,
		tableDesc:  spec.TableDesc,
		uri:        spec.Uri,
		output:     output,
	}
	if err := cp.out.Init(&distsqlrun.PostProcessSpec{}, csvOutputTypes, &flowCtx.EvalCtx, output); err != nil {
		return nil, err
	}
	return cp, nil
}

type readCSVProcessor struct {
	comma      rune
	comment    rune
	nullif     *string
	sampleSize int32
	tableDesc  sqlbase.TableDescriptor
	uri        string
	out        distsqlrun.ProcOutputHelper
	output     distsqlrun.RowReceiver
}

var _ distsqlrun.Processor = &readCSVProcessor{}

func (cp *readCSVProcessor) OutputTypes() []sqlbase.ColumnType {
	return csvOutputTypes
}

func (cp *readCSVProcessor) Run(ctx context.Context, wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}

	group, gCtx := errgroup.WithContext(ctx)
	done := gCtx.Done()
	recordCh := make(chan csvRecord)
	kvCh := make(chan roachpb.KeyValue)
	sampleCh := make(chan sqlbase.EncDatumRow)

	// Read CSV into CSV records
	group.Go(func() error {
		defer close(recordCh)
		_, err := readCSV(gCtx, cp.comma, cp.comment, len(cp.tableDesc.VisibleColumns()), []string{cp.uri}, recordCh)
		return err
	})
	// Convert CSV records to KVs
	group.Go(func() error {
		defer close(kvCh)
		return groupWorkers(gCtx, runtime.NumCPU(), func(ctx context.Context) error {
			return convertRecord(ctx, recordCh, kvCh, cp.nullif, &cp.tableDesc)
		})
	})
	// Sample KVs
	group.Go(func() error {
		defer close(sampleCh)
		var fn sampleFunc
		if cp.sampleSize == 0 {
			fn = sampleAll
		} else {
			sr := sampleRate{
				rnd:        rand.New(rand.NewSource(rand.Int63())),
				sampleSize: float64(cp.sampleSize),
			}
			fn = sr.sample
		}
		typeBytes := sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BYTES}
		for kv := range kvCh {
			if fn(kv) {
				row := sqlbase.EncDatumRow{
					sqlbase.DatumToEncDatum(typeBytes, parser.NewDBytes(parser.DBytes(kv.Key))),
					sqlbase.DatumToEncDatum(typeBytes, parser.NewDBytes(parser.DBytes(kv.Value.RawBytes))),
				}
				select {
				case <-done:
					return gCtx.Err()
				case sampleCh <- row:
				}
			}
		}
		return nil
	})
	// Send sampled KVs to dist sql
	group.Go(func() error {
		for row := range sampleCh {
			cs, err := cp.out.EmitRow(gCtx, row)
			if err != nil {
				return err
			}
			if cs != distsqlrun.NeedMoreRows {
				return errors.New("unexpected closure of consumer")
			}
		}
		return nil
	})
	if err := group.Wait(); err != nil {
		distsqlrun.DrainAndClose(ctx, cp.output, err)
		return
	}

	cp.out.Close()
}

type sampleFunc func(roachpb.KeyValue) bool

// sampleRate is a sampleFunc that samples a row with a probability of the
// row's size / the sample size.
type sampleRate struct {
	rnd        *rand.Rand
	sampleSize float64
}

func (s sampleRate) sample(kv roachpb.KeyValue) bool {
	sz := float64(len(kv.Key) + len(kv.Value.RawBytes))
	prob := sz / s.sampleSize
	return prob > s.rnd.Float64()
}

func sampleAll(kv roachpb.KeyValue) bool {
	return true
}

func init() {
	sql.AddPlanHook(importPlanHook)
	distsqlrun.NewReadCSVProcessor = newReadCSVProcessor
}
