// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"bytes"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/parquet"
	"github.com/cockroachdb/errors"
)

// includeParquestTestMetadata configures the parquet writer to write extra
// column metadata for parquet files in tests.
var includeParquestTestMetadata = buildutil.CrdbTestBuild ||
	envutil.EnvOrDefaultBool("COCKROACH_CHANGEFEED_TESTING_INCLUDE_PARQUET_TEST_METADATA",
		false)

type parquetWriter struct {
	inner        *parquet.Writer
	encodingOpts changefeedbase.EncodingOptions
	schemaDef    *parquet.SchemaDefinition
	datumAlloc   []tree.Datum

	// Cached object builder for previous row when using the `diff` option.
	prevState struct {
		vb      *json.FixedKeysObjectBuilder
		version descpb.DescriptorVersion
	}
}

// newParquetSchemaDefintion returns a parquet schema definition based on the
// cdcevent.Row and the number of cols in the schema.
func newParquetSchemaDefintion(
	row cdcevent.Row, encodingOpts changefeedbase.EncodingOptions,
) (*parquet.SchemaDefinition, error) {
	var columnNames []string
	var columnTypes []*types.T
	seenColumnNames := make(map[string]bool)

	numCols := 0
	if err := row.ForAllColumns().Col(func(col cdcevent.ResultColumn) error {
		if _, ok := seenColumnNames[col.Name]; ok {
			// If a column is both the primary key and one of the selected columns in
			// a cdc query, we do not want to duplicate it in the parquet output. We
			// deduplicate that here and where we populate the datums (see
			// populateDatums).
			return nil
		}
		seenColumnNames[col.Name] = true
		columnNames = append(columnNames, col.Name)
		columnTypes = append(columnTypes, col.Typ)
		numCols += 1
		return nil
	}); err != nil {
		return nil, err
	}

	columnNames = append(columnNames, parquetCrdbEventTypeColName)
	columnTypes = append(columnTypes, types.String)
	numCols += 1

	columnNames, columnTypes = appendMetadataColsToSchema(columnNames, columnTypes, encodingOpts)

	schemaDef, err := parquet.NewSchema(columnNames, columnTypes)
	if err != nil {
		return nil, err
	}
	return schemaDef, nil
}

const parquetOptUpdatedTimestampColName = metaSentinel + changefeedbase.OptUpdatedTimestamps
const parquetOptMVCCTimestampColName = metaSentinel + changefeedbase.OptMVCCTimestamps
const parquetOptDiffColName = metaSentinel + "before"

func appendMetadataColsToSchema(
	columnNames []string, columnTypes []*types.T, encodingOpts changefeedbase.EncodingOptions,
) (updatedNames []string, updatedTypes []*types.T) {
	if encodingOpts.UpdatedTimestamps {
		columnNames = append(columnNames, parquetOptUpdatedTimestampColName)
		columnTypes = append(columnTypes, types.String)
	}
	if encodingOpts.MVCCTimestamps {
		columnNames = append(columnNames, parquetOptMVCCTimestampColName)
		columnTypes = append(columnTypes, types.String)
	}
	if encodingOpts.Diff {
		columnNames = append(columnNames, parquetOptDiffColName)
		columnTypes = append(columnTypes, types.Json)
	}
	return columnNames, columnTypes
}

// newParquetWriterFromRow constructs a new parquet writer which outputs to
// the given sink. This function interprets the schema from the supplied row.
func newParquetWriterFromRow(
	row cdcevent.Row,
	sink io.Writer,
	encodingOpts changefeedbase.EncodingOptions,
	opts ...parquet.Option,
) (*parquetWriter, error) {
	schemaDef, err := newParquetSchemaDefintion(row, encodingOpts)
	if err != nil {
		return nil, err
	}

	if includeParquestTestMetadata {
		if opts, err = addParquetTestMetadata(row, encodingOpts, opts); err != nil {
			return nil, err
		}
		opts = append(opts, parquet.WithMetadata(parquet.MakeReaderMetadata(schemaDef)))
	}
	writer, err := parquet.NewWriter(schemaDef, sink, opts...)

	if err != nil {
		return nil, err
	}
	return &parquetWriter{
		inner:        writer,
		encodingOpts: encodingOpts,
		schemaDef:    schemaDef,
		datumAlloc:   make([]tree.Datum, schemaDef.NumColumns()),
	}, nil
}

// addData writes the updatedRow, adding the row's event type. There is no guarantee
// that data will be flushed after this function returns.
func (w *parquetWriter) addData(
	updatedRow cdcevent.Row, prevRow cdcevent.Row, updated, mvcc hlc.Timestamp,
) error {
	if err := w.populateDatums(updatedRow, prevRow, updated, mvcc); err != nil {
		return err
	}

	return w.inner.AddRow(w.datumAlloc)
}

// estimatedBufferedBytes returns the number of bytes estimated to be buffered
// and not yet written to the sink.
func (w *parquetWriter) estimatedBufferedBytes() int64 {
	return w.inner.BufferedBytesEstimate()
}

// flush flushes buffered datums to the sink.
func (w *parquetWriter) flush() error {
	return w.inner.Flush()
}

// close closes the writer and flushes any buffered data to the sink.
func (w *parquetWriter) close() error {
	return w.inner.Close()
}

// populateDatums writes the appropriate datums into the datumAlloc slice.
func (w *parquetWriter) populateDatums(
	updatedRow cdcevent.Row, prevRow cdcevent.Row, updated, mvcc hlc.Timestamp,
) error {
	datums := w.datumAlloc[:0]

	seenColumnNames := make(map[string]bool)
	if err := updatedRow.ForAllColumns().Datum(func(d tree.Datum, col cdcevent.ResultColumn) error {
		if _, ok := seenColumnNames[col.Name]; ok {
			// If a column is both the primary key and one of the selected columns in
			// a cdc query, we do not want to duplicate it in the parquet output. We
			// deduplicate that here and in the schema definition (see
			// newParquetSchemaDefintion).
			return nil
		}
		seenColumnNames[col.Name] = true
		datums = append(datums, d)
		return nil
	}); err != nil {
		return err
	}
	datums = append(datums, getEventTypeDatum(updatedRow, prevRow).DString())

	if w.encodingOpts.UpdatedTimestamps {
		datums = append(datums, tree.NewDString(updated.AsOfSystemTime()))
	}
	if w.encodingOpts.MVCCTimestamps {
		datums = append(datums, tree.NewDString(mvcc.AsOfSystemTime()))
	}
	if w.encodingOpts.Diff {
		if prevRow.IsDeleted() {
			datums = append(datums, tree.DNull)
		} else {
			if w.prevState.vb == nil || w.prevState.version != prevRow.Version {
				keys := make([]string, 0, len(prevRow.ResultColumns()))
				_ = prevRow.ForEachColumn().Col(func(col cdcevent.ResultColumn) error {
					keys = append(keys, col.Name)
					return nil
				})
				valueBuilder, err := json.NewFixedKeysObjectBuilder(keys)
				if err != nil {
					return err
				}
				w.prevState.version = prevRow.Version
				w.prevState.vb = valueBuilder
			}

			if err := prevRow.ForEachColumn().Datum(func(d tree.Datum, col cdcevent.ResultColumn) error {
				j, err := tree.AsJSON(d, sessiondatapb.DataConversionConfig{}, time.UTC)
				if err != nil {
					return err
				}
				return w.prevState.vb.Set(col.Name, j)
			}); err != nil {
				return err
			}

			j, err := w.prevState.vb.Build()
			if err != nil {
				return err
			}
			datums = append(datums, tree.NewDJSON(j))
		}
	}

	return nil
}

// addParquetTestMetadata appends options to the provided options to configure the
// parquet writer to write metadata required by cdc test feed factories.
//
// Generally, cdc tests will convert the row to JSON loosely in the form:
// `[0]->{"b": "b", "c": "c"}` with the key columns in square brackets and value
// columns in a JSON object. The metadata generated by this function contains
// key and value column names along with their offsets in the parquet file.
func addParquetTestMetadata(
	row cdcevent.Row, encodingOpts changefeedbase.EncodingOptions, parquetOpts []parquet.Option,
) ([]parquet.Option, error) {
	// NB: Order matters. When iterating using ForAllColumns, which is used when
	// writing datums and defining the schema, the order of columns usually
	// matches the underlying table. If a composite key is defined, the order in
	// ForEachKeyColumn may not match. In tests, we want to use the latter
	// order when printing the keys.
	keyCols := map[string]int{}
	var keysInOrder []string
	if err := row.ForEachKeyColumn().Col(func(col cdcevent.ResultColumn) error {
		keyCols[col.Name] = -1
		keysInOrder = append(keysInOrder, col.Name)
		return nil
	}); err != nil {
		return parquetOpts, err
	}

	// NB: We do not use ForAllColumns here because it will always contain the
	// key. In tests where we target a column family without a key in it,
	// ForEachColumn will exclude the primary key of the table, which is what
	// we want.
	valueCols := map[string]int{}
	var valuesInOrder []string
	if err := row.ForEachColumn().Col(func(col cdcevent.ResultColumn) error {
		valueCols[col.Name] = -1
		valuesInOrder = append(valuesInOrder, col.Name)
		return nil
	}); err != nil {
		return parquetOpts, err
	}

	// Iterate over ForAllColumns to determine the offets of each column
	// in a parquet row (ie. the slice of datums provided to addData). We don't
	// do this above because there is no way to determine it from
	// cdcevent.ResultColumn. The Ordinal() method may return an invalid
	// number for virtual columns.
	idx := 0
	seenColumnNames := make(map[string]bool)
	if err := row.ForAllColumns().Col(func(col cdcevent.ResultColumn) error {
		if _, ok := seenColumnNames[col.Name]; ok {
			// Since we deduplicate columns with the same name in the parquet output,
			// we should not even increment our index for columns we've seen before.
			// Since we have already seen this column name we have also already found
			// the relevant index.
			return nil
		}
		seenColumnNames[col.Name] = true
		if _, colIsInKey := keyCols[col.Name]; colIsInKey {
			keyCols[col.Name] = idx
		}
		if _, colIsInValue := valueCols[col.Name]; colIsInValue {
			valueCols[col.Name] = idx
		}
		idx += 1
		return nil
	}); err != nil {
		return parquetOpts, err
	}
	valuesInOrder = append(valuesInOrder, parquetCrdbEventTypeColName)
	valueCols[parquetCrdbEventTypeColName] = idx
	idx += 1

	if encodingOpts.UpdatedTimestamps {
		valuesInOrder = append(valuesInOrder, parquetOptUpdatedTimestampColName)
		valueCols[parquetOptUpdatedTimestampColName] = idx
		idx += 1
	}
	if encodingOpts.MVCCTimestamps {
		valuesInOrder = append(valuesInOrder, parquetOptMVCCTimestampColName)
		valueCols[parquetOptMVCCTimestampColName] = idx
		idx += 1
	}
	if encodingOpts.Diff {
		valuesInOrder = append(valuesInOrder, parquetOptDiffColName)
		valueCols[parquetOptDiffColName] = idx
		idx += 1
	}

	parquetOpts = append(parquetOpts, parquet.WithMetadata(map[string]string{"keyCols": serializeMap(keysInOrder, keyCols)}))
	parquetOpts = append(parquetOpts, parquet.WithMetadata(map[string]string{"allCols": serializeMap(valuesInOrder, valueCols)}))
	return parquetOpts, nil
}

// serializeMap serializes a map to a string. For example, orderedKeys=["b",
// "a"] m={"a": 1", "b": 2, "c":3} will return the string "b,2,a,1".
func serializeMap(orderedKeys []string, m map[string]int) string {
	var buf bytes.Buffer
	for i, k := range orderedKeys {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(k)
		buf.WriteString(",")
		buf.WriteString(strconv.Itoa(m[k]))
	}
	return buf.String()
}

// deserializeMap deserializes a string in the form "b,2,a,1" and returns a map
// representation along with the keys in order: m={"a": 1", "b": 2}
// orderedKeys=["b", "a"].
func deserializeMap(s string) (orderedKeys []string, m map[string]int, err error) {
	keyValues := strings.Split(s, ",")
	if len(keyValues)%2 != 0 {
		return nil, nil,
			errors.AssertionFailedf("list of elements %s should have an even length", s)
	}
	for i := 0; i < len(keyValues); i += 2 {
		key := keyValues[i]
		value, err := strconv.Atoi(keyValues[i+1])
		if err != nil {
			return nil, nil, err
		}
		orderedKeys = append(orderedKeys, key)
		if i == 0 {
			m = map[string]int{}
		}
		m[key] = value
	}
	return orderedKeys, m, nil
}

// TestingGetEventTypeColIdx returns the index of the extra column added to
// every parquet file which indicate the type of event that generated a
// particular row. Please read parquetCrdbEventTypeColName and
// addParquetTestMetadata for more details.
func TestingGetEventTypeColIdx(rd parquet.ReadDatumsMetadata) (int, error) {
	columnsNamesString, ok := rd.MetaFields["allCols"]
	if !ok {
		return -1, errors.Errorf("could not find column names in parquet metadata")
	}
	_, columnNameSet, err := deserializeMap(columnsNamesString)
	if err != nil {
		return -1, err
	}
	return columnNameSet[parquetCrdbEventTypeColName], nil
}
