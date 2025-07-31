// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cdcevent

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

const virtualColOrd = 1<<31 - 1

// Metadata describes event metadata.
type Metadata struct {
	TableID          descpb.ID                // Table ID.
	TableName        string                   // Table name.
	Version          descpb.DescriptorVersion // Table descriptor version.
	FamilyID         descpb.FamilyID          // Column family ID.
	FamilyName       string                   // Column family name.
	HasOtherFamilies bool                     // True if the table multiple families.
	SchemaTS         hlc.Timestamp            // Schema timestamp for table descriptor.
}

// Decoder is an interface for decoding KVs into cdc event row.
type Decoder interface {
	// DecodeKV decodes specified key value to Row.
	DecodeKV(ctx context.Context, kv roachpb.KeyValue, rt RowType, schemaTS hlc.Timestamp, keyOnly bool) (Row, error)
}

// Row holds a row corresponding to an event.
type Row struct {
	*EventDescriptor
	MvccTimestamp hlc.Timestamp // Mvcc timestamp of this row.

	// datums is the new value of a changed table row.
	datums rowenc.EncDatumRow

	// deleted is true if row is a deletion. In this case, only the primary
	// key columns are guaranteed to be set in `datums`.
	deleted bool

	// Alloc used when decoding datums.
	alloc *tree.DatumAlloc
}

// DatumFn is a callback function invoked for each decoded datum.
// Function may return iterutil.StopIteration() error to stop iteration early.
// Any other error propagated as a return value.
type DatumFn func(d tree.Datum, col ResultColumn) error

// ColumnFn is a callback functioned invoked for each column type.
type ColumnFn func(col ResultColumn) error

// Iterator is an iterator over datums.
type Iterator interface {
	// Datum invokes fn for each decoded datum.
	Datum(fn DatumFn) error
	// Col invokes fn for each column.
	Col(fn ColumnFn) error
}

// EncDatums returns EncDatumRow.
func (r Row) EncDatums() rowenc.EncDatumRow {
	return r.datums
}

// ForEachKeyColumn returns Iterator for each key column
func (r Row) ForEachKeyColumn() Iterator {
	return iter{r: r, cols: r.keyCols}
}

// ForEachColumn returns Iterator for each column.
func (r Row) ForEachColumn() Iterator {
	return iter{r: r, cols: r.valueCols}
}

// ForAllColumns returns Iterator for all columns.
func (r Row) ForAllColumns() Iterator {
	return iter{r: r, cols: r.allCols}
}

// ForEachUDTColumn returns Datum iterator for each column containing user defined types.
func (r Row) ForEachUDTColumn() Iterator {
	return iter{r: r, cols: r.udtCols}
}

// DatumNamed returns the datum with the specified column name, in the form of an Iterator.
func (r Row) DatumNamed(n string) (Iterator, error) {
	idx, ok := r.EventDescriptor.colsByName[n]
	if !ok {
		return nil, errors.AssertionFailedf("No column with name %s in this row", n)
	}
	return iter{r: r, cols: []int{idx}}, nil
}

// DatumsNamed returns the datums with the specified column names, in the form of an Iterator.
func (r Row) DatumsNamed(names []string) (Iterator, error) {
	cols := make([]int, 0, len(names))
	for _, n := range names {
		idx, ok := r.EventDescriptor.colsByName[n]
		if !ok {
			return nil, errors.AssertionFailedf("No column with name %s in this row", n)
		}
		cols = append(cols, idx)
	}
	return iter{r: r, cols: cols}, nil
}

// IsDeleted returns true if event corresponds to a deletion event.
func (r Row) IsDeleted() bool {
	return r.deleted
}

// IsInitialized returns true if event row is initialized.
func (r Row) IsInitialized() bool {
	return r.EventDescriptor != nil
}

// HasValues returns true if event row has values to decode.
func (r Row) HasValues() bool {
	return r.datums != nil
}

// DebugString returns debug string describing event source.
func (m Metadata) DebugString() string {
	return fmt.Sprintf("{table: %d family: %d}", m.TableID, m.FamilyID)
}

// DebugString returns row string.
func (r Row) DebugString() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Row%s{", r.Metadata.DebugString()))
	first := true
	err := r.ForAllColumns().Datum(func(d tree.Datum, col ResultColumn) error {
		if !first {
			sb.WriteString(", ")
		}
		sb.WriteString(fmt.Sprintf("%s: %s", col.Name, d.String()))
		first = false
		return nil
	})
	if err != nil {
		sb.WriteString(fmt.Sprintf("err: %s", err))
	}
	sb.WriteByte('}')
	return sb.String()
}

func (r Row) ToJSON() (*tree.DJSON, error) {
	builder := json.NewObjectBuilder(len(r.cols))
	err := r.ForAllColumns().Datum(func(d tree.Datum, col ResultColumn) error {
		val, err := tree.AsJSON(
			d,
			sessiondatapb.DataConversionConfig{},
			time.UTC,
		)
		if err != nil {
			return err
		}
		builder.Add(col.Name, val)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return tree.NewDJSON(builder.Build()), nil
}

// forEachColumn is a helper which invokes fn for reach column in the ordColumn list.
func (r Row) forEachDatum(fn DatumFn, colIndexes []int) error {
	for _, colIdx := range colIndexes {
		col := r.cols[colIdx]
		switch {
		case col.ord == virtualColOrd:
			// A datum row will never contain virtual columns.
			// Insert null values as placeholders for virtual columns.
			if err := fn(tree.DNull, col); err != nil {
				return iterutil.Map(err)
			}
		case col.ord < len(r.datums):
			encDatum := r.datums[col.ord]
			if err := encDatum.EnsureDecoded(col.Typ, r.alloc); err != nil {
				return errors.Wrapf(err, "error decoding column %q as type %s", col.Name, col.Typ.String())
			}
			if err := fn(encDatum.Datum, col); err != nil {
				return iterutil.Map(err)
			}
		default:
			return errors.AssertionFailedf("index [%d] out of range for column %q", col.ord, col.Name)
		}
	}
	return nil
}

// forEachColumn is a helper which invokes fn for reach column in the ordColumn list.
func (r Row) forEachColumn(fn ColumnFn, colIndexes []int) error {
	for _, colIdx := range colIndexes {
		if err := fn(r.cols[colIdx]); err != nil {
			return iterutil.Map(err)
		}
	}
	return nil
}

// ResultColumn associates ResultColumn with an ordinal position where
// such column expected to be found.
type ResultColumn struct {
	colinfo.ResultColumn
	Computed  bool
	ord       int
	sqlString string
}

// SQLStringNotHumanReadable returns the SQL statement describing the column.
func (c ResultColumn) SQLStringNotHumanReadable() string {
	return c.sqlString
}

// Ordinal returns ordinal position of this column in the underlying event descriptor.
func (c ResultColumn) Ordinal() int {
	return c.ord
}

// EventDescriptor is a cdc event descriptor: collection of information describing Row.
type EventDescriptor struct {
	Metadata

	td catalog.TableDescriptor

	// List of result columns produced by this descriptor.
	// This may be different from the table descriptors public columns
	// (e.g. in case of projection).
	cols []ResultColumn

	// Precomputed index lists into cols.
	// TODO(yang): Consider refactoring so that valueCols is empty when keyOnly.
	keyCols    []int          // Primary key columns.
	valueCols  []int          // Column family (+ virtual if includeVirtualColumns) columns / primary key columns (if keyOnly).
	udtCols    []int          // UDT columns.
	allCols    []int          // All columns.
	colsByName map[string]int // All columns, map[col.GetName()]idx in cols.
}

// NewEventDescriptor returns EventDescriptor for specified table and family descriptors.
func NewEventDescriptor(
	desc catalog.TableDescriptor,
	family *descpb.ColumnFamilyDescriptor,
	includeVirtualColumns bool,
	keyOnly bool,
	schemaTS hlc.Timestamp,
) (*EventDescriptor, error) {
	sd := EventDescriptor{
		Metadata: Metadata{
			TableID:          desc.GetID(),
			TableName:        desc.GetName(),
			Version:          desc.GetVersion(),
			FamilyID:         family.ID,
			FamilyName:       family.Name,
			HasOtherFamilies: desc.NumFamilies() > 1,
			SchemaTS:         schemaTS,
		},
		td:         desc,
		colsByName: make(map[string]int),
	}

	// addColumn is a helper to add a column to this descriptor.
	addColumn := func(col catalog.Column, ord int) int {
		resultColumn := ResultColumn{
			ResultColumn: colinfo.ResultColumn{
				Name:           col.GetName(),
				Typ:            col.GetType(),
				TableID:        desc.GetID(),
				PGAttributeNum: uint32(col.GetPGAttributeNum()),
			},
			Computed:  col.IsComputed(),
			ord:       ord,
			sqlString: col.ColumnDesc().SQLStringNotHumanReadable(),
		}

		colIdx := len(sd.cols)
		sd.cols = append(sd.cols, resultColumn)
		sd.colsByName[col.GetName()] = colIdx

		if col.GetType().UserDefined() {
			sd.udtCols = append(sd.udtCols, colIdx)
		}
		return colIdx
	}

	// Primary key columns must be added in the same order they
	// appear in the primary key index.
	primaryIdx := desc.GetPrimaryIndex()
	colOrd := catalog.ColumnIDToOrdinalMap(desc.PublicColumns())
	writeOnlyAndPublic := catalog.ColumnIDToOrdinalMap(desc.WritableColumns())
	var primaryKeyOrdinal catalog.TableColMap

	ordIdx := 0
	for i := 0; i < primaryIdx.NumKeyColumns(); i++ {
		ord, ok := colOrd.Get(primaryIdx.GetKeyColumnID(i))
		// Columns going through mutation can exist in the PK, but not
		// be public, since a later primary index will make these fully
		// public.
		if !ok {
			if _, isWriteOnlyColumn := writeOnlyAndPublic.Get(primaryIdx.GetKeyColumnID(i)); isWriteOnlyColumn {
				continue
			}
			return nil, errors.AssertionFailedf("expected to find column %d", ord)
		}
		primaryKeyOrdinal.Set(desc.PublicColumns()[ord].GetID(), ordIdx)
		ordIdx += 1
	}
	sd.keyCols = make([]int, ordIdx)

	switch {
	case keyOnly:
		ord := 0
		for _, col := range desc.PublicColumns() {
			pKeyOrd, isPKey := primaryKeyOrdinal.Get(col.GetID())
			if !isPKey {
				continue
			}
			colIdx := addColumn(col, ord)
			ord++
			sd.keyCols[pKeyOrd] = colIdx
			sd.valueCols = append(sd.valueCols, colIdx)
		}
	default:
		// Remaining columns go in same order as public columns,
		// with the exception that virtual columns are assigned
		// a sentinel ordinal position of virtualColOrd.
		inFamily := catalog.MakeTableColSet(family.ColumnIDs...)
		ord := 0
		for _, col := range desc.PublicColumns() {
			if isVirtual := col.IsVirtual(); isVirtual && includeVirtualColumns {
				colIdx := addColumn(col, virtualColOrd)
				sd.valueCols = append(sd.valueCols, colIdx)
				continue
			}
			pKeyOrd, isPKey := primaryKeyOrdinal.Get(col.GetID())
			isInFamily := inFamily.Contains(col.GetID())
			if !isPKey && !isInFamily {
				continue
			}
			colIdx := addColumn(col, ord)
			ord++
			if isPKey {
				sd.keyCols[pKeyOrd] = colIdx
			}
			if isInFamily {
				sd.valueCols = append(sd.valueCols, colIdx)
			}
		}
	}

	allCols := make([]int, len(sd.cols))
	for i := 0; i < len(sd.cols); i++ {
		allCols[i] = i
	}
	sd.allCols = allCols

	return &sd, nil
}

// DebugString returns event descriptor debug information.
func (d *EventDescriptor) DebugString() string {
	return fmt.Sprintf("EventDescriptor{table: %q(%d) family: %q(%d) pkCols=%v valCols=%v",
		d.TableName, d.TableID, d.FamilyName, d.FamilyID, d.keyCols, d.valueCols)
}

// SafeFormat implements SafeFormatter interface.
func (d *EventDescriptor) SafeFormat(p redact.SafePrinter, _ rune) {
	p.Print(d.DebugString())
}

// ResultColumns returns all results columns in this descriptor.
func (d *EventDescriptor) ResultColumns() []ResultColumn {
	return d.cols
}

// EqualsVersion returns true if this descriptor equals other.
func (d *EventDescriptor) EqualsVersion(other *EventDescriptor) bool {
	return d.TableID == other.TableID &&
		d.Version == other.Version &&
		d.FamilyID == other.FamilyID
}

// EqualsWithUDTCheck returns true if event descriptors are the same version and
// their user defined types (if any) are also matching.
func (d *EventDescriptor) EqualsWithUDTCheck(
	other *EventDescriptor,
) (sameVersion bool, typesHaveSameVersion bool) {
	if d.EqualsVersion(other) {
		return true, catalog.UserDefinedTypeColsHaveSameVersion(d.td, other.td)
	}
	return false, false
}

// TableDescriptor returns underlying table descriptor.  This method is exposed
// to make it easier to integrate with the rest of descriptor APIs; prefer to use
// higher level methods/structs (e.g. Metadata) instead.
func (d *EventDescriptor) TableDescriptor() catalog.TableDescriptor {
	return d.td
}

type eventDescriptorFactory func(
	desc catalog.TableDescriptor,
	family *descpb.ColumnFamilyDescriptor,
	schemaTS hlc.Timestamp,
) (*EventDescriptor, error)

type eventDecoder struct {
	// Cached allocations for *row.Fetcher
	rfCache *rowFetcherCache

	// kvProvider used to feed KVs into fetcher to decode them into datums.
	kvProvider row.KVProvider

	// factory for constructing event descriptors.
	getEventDescriptor eventDescriptorFactory

	// Alloc used when decoding datums.
	alloc tree.DatumAlloc

	// State pertaining for decoding of a single key.
	fetcher  fetcher                        // Fetcher to decode KV
	desc     catalog.TableDescriptor        // Current descriptor
	family   *descpb.ColumnFamilyDescriptor // Current family
	schemaTS hlc.Timestamp                  // Schema timestamp.
}

func getEventDescriptorCached(
	desc catalog.TableDescriptor,
	family *descpb.ColumnFamilyDescriptor,
	includeVirtual bool,
	keyOnly bool,
	schemaTS hlc.Timestamp,
	cache *cache.UnorderedCache,
) (*EventDescriptor, error) {
	idVer := CacheKey{ID: desc.GetID(), Version: desc.GetVersion(), FamilyID: family.ID}

	if v, ok := cache.Get(idVer); ok {
		ed := v.(*EventDescriptor)
		if catalog.UserDefinedTypeColsHaveSameVersion(ed.td, desc) {
			return ed, nil
		}
	}

	ed, err := NewEventDescriptor(desc, family, includeVirtual, keyOnly, schemaTS)
	if err != nil {
		return nil, err
	}
	cache.Add(idVer, ed)
	return ed, nil
}

// NewEventDecoder returns key value decoder.
func NewEventDecoder(
	ctx context.Context,
	cfg *sql.ExecutorConfig,
	targets changefeedbase.Targets,
	includeVirtual bool,
	keyOnly bool,
) (Decoder, error) {
	rfCache, err := newRowFetcherCache(
		ctx,
		cfg.Codec,
		cfg.LeaseManager,
		cfg.CollectionFactory,
		cfg.DB,
		cfg.Settings,
		targets,
	)
	if err != nil {
		return nil, err
	}

	return NewEventDecoderWithCache(ctx, rfCache, includeVirtual, keyOnly), nil
}

// NewEventDecoderWithCache returns key value decoder.
func NewEventDecoderWithCache(
	ctx context.Context, rfCache *rowFetcherCache, includeVirtual bool, keyOnly bool,
) Decoder {
	eventDescriptorCache := cache.NewUnorderedCache(DefaultCacheConfig)
	getEventDescriptor := func(
		desc catalog.TableDescriptor,
		family *descpb.ColumnFamilyDescriptor,
		schemaTS hlc.Timestamp,
	) (*EventDescriptor, error) {
		return getEventDescriptorCached(desc, family, includeVirtual, keyOnly, schemaTS, eventDescriptorCache)
	}

	return &eventDecoder{
		getEventDescriptor: getEventDescriptor,
		rfCache:            rfCache,
	}
}

// RowType is the type of the row being decoded.
type RowType int

const (
	CurrentRow RowType = iota
	PrevRow
)

// DecodeKV decodes key value at specified schema timestamp.
func (d *eventDecoder) DecodeKV(
	ctx context.Context, kv roachpb.KeyValue, rt RowType, schemaTS hlc.Timestamp, keyOnly bool,
) (Row, error) {
	r, err := d.decodeKV(ctx, kv, rt, schemaTS, keyOnly)
	if err == nil {
		return r, nil
	}
	// Unwatched family errors aren't terminal so return early and let caller
	// decide what to do with it.
	if errors.Is(err, ErrUnwatchedFamily) {
		return Row{}, err
	}

	// Failure to decode roachpb.KeyValue we received from rangefeed is pretty bad.
	// At this point, we only have guesses why this happened (schema change? data corruption?).
	// Retrying this error however is likely to produce exactly the same result.
	// So, be loud and treat this error as a terminal changefeed error.
	kvBytes, marshalErr := protoutil.Marshal(&kv)
	if marshalErr != nil {
		// That's mighty surprising.  Just shove error message into kvBytes.
		kvBytes = []byte(fmt.Sprintf("marshalErr<%s>", marshalErr.Error()))
	}
	err = changefeedbase.WithTerminalError(errors.Wrapf(err,
		"error decoding key %s@%s (hex_kv: %x)",
		keys.PrettyPrint(nil, kv.Key), kv.Value.Timestamp, kvBytes))
	log.Errorf(ctx, "terminal error decoding KV: %v", err)
	return Row{}, err
}

// decodeKV decodes key value at specified schema timestamp.
func (d *eventDecoder) decodeKV(
	ctx context.Context, kv roachpb.KeyValue, rt RowType, schemaTS hlc.Timestamp, keyOnly bool,
) (Row, error) {
	if err := d.initForKey(ctx, kv.Key, schemaTS, keyOnly); err != nil {
		return Row{}, err
	}

	d.kvProvider.KVs = d.kvProvider.KVs[:0]
	d.kvProvider.KVs = append(d.kvProvider.KVs, kv)
	if err := d.fetcher.ConsumeKVProvider(ctx, &d.kvProvider); err != nil {
		return Row{}, err
	}

	datums, isDeleted, err := d.nextRow(ctx, rt == PrevRow)
	if err != nil {
		return Row{}, err
	}

	ed, err := d.getEventDescriptor(d.desc, d.family, schemaTS)
	if err != nil {
		return Row{}, err
	}

	return Row{
		EventDescriptor: ed,
		MvccTimestamp:   kv.Value.Timestamp,
		datums:          datums,
		deleted:         isDeleted,
		alloc:           &d.alloc,
	}, nil
}

// initForKey initializes decoder state to prepare it to decode
// key/value at specified timestamp.
func (d *eventDecoder) initForKey(
	ctx context.Context, key roachpb.Key, schemaTS hlc.Timestamp, keyOnly bool,
) error {
	desc, familyID, err := d.rfCache.tableDescForKey(ctx, key, schemaTS)
	if err != nil {
		return err
	}

	fetcher, family, err := d.rfCache.RowFetcherForColumnFamily(desc, familyID, systemColumns, keyOnly)
	if err != nil {
		return err
	}

	d.schemaTS = schemaTS
	d.desc = desc
	d.family = family
	d.fetcher.Fetcher = fetcher
	return nil
}

// systemColumns is a list of system columns we add to the fetcher spec.
// It's just an alias for the colinfo.AllSystemColumns, but written out explicitly
// to make the order of system columns clear and explicit.
// In particular, when decoding previous row, we strip table OID column
// since it makes little sense to include it in the previous row value.
var systemColumns = []descpb.ColumnDescriptor{
	colinfo.MVCCTimestampColumnDesc,
	colinfo.TableOIDColumnDesc,
	colinfo.OriginIDColumnDesc,
	colinfo.OriginTimestampColumnDesc,
}

type fetcher struct {
	*row.Fetcher
}

// nextRow returns the next row from the fetcher, but strips out
// system columns.
func (f *fetcher) nextRow(ctx context.Context, isPrev bool) (rowenc.EncDatumRow, error) {
	r, _, err := f.Fetcher.NextRow(ctx)
	if err != nil {
		return nil, err
	}
	if isPrev {
		r = r[:len(r)-len(systemColumns)]
	}
	return r, nil
}

// nextRow returns next encoded row, and a flag indicating if a row was deleted.
func (d *eventDecoder) nextRow(ctx context.Context, isPrev bool) (rowenc.EncDatumRow, bool, error) {
	datums, err := d.fetcher.nextRow(ctx, isPrev)
	if err != nil {
		return nil, false, err
	}
	if datums == nil {
		return nil, false, errors.AssertionFailedf("unexpected empty datums")
	}

	// Copy datums since row fetcher reuses alloc.
	datums = append(rowenc.EncDatumRow(nil), datums...)
	isDeleted := d.fetcher.RowIsDeleted()

	// Assert that we don't get a second row from the row.Fetcher. We
	// fed it a single KV, so that would be surprising.
	if nd, _, err := d.fetcher.NextRow(ctx); err != nil || nd != nil {
		if err != nil {
			return nil, false, err
		}
		if nd != nil {
			return nil, false, errors.AssertionFailedf("unexpected non-empty datums")
		}
	}
	return datums, isDeleted, nil
}

type iter struct {
	r    Row
	cols []int
}

var _ Iterator = iter{}

// Datum implements Iterator interface.
func (it iter) Datum(fn DatumFn) error {
	return it.r.forEachDatum(fn, it.cols)
}

// Col implements Iterator interface.
func (it iter) Col(fn ColumnFn) error {
	return it.r.forEachColumn(fn, it.cols)
}

// TestingMakeEventRow initializes Row with provided arguments.
// Exposed for unit tests.
func TestingMakeEventRow(
	desc catalog.TableDescriptor, familyID descpb.FamilyID, encRow rowenc.EncDatumRow, deleted bool,
) Row {
	family, err := catalog.MustFindFamilyByID(desc, familyID)
	if err != nil {
		panic(err) // primary column family always exists.
	}
	const includeVirtual = false
	ed, err := NewEventDescriptor(desc, family, includeVirtual, false, hlc.Timestamp{})
	if err != nil {
		panic(err)
	}
	var alloc tree.DatumAlloc
	return Row{
		EventDescriptor: ed,
		datums:          encRow,
		deleted:         deleted,
		alloc:           &alloc,
	}
}

// TestingMakeEventRowFromDatums initializes a Row that will return the provided datums when
// ForEachColumn is called. If anything else needs to be hydrated, use TestingMakeEventRow
// instead.
func TestingMakeEventRowFromDatums(datums tree.Datums) Row {
	var desc EventDescriptor
	var encRow rowenc.EncDatumRow
	var alloc tree.DatumAlloc
	for i, d := range datums {
		desc.cols = append(desc.cols, ResultColumn{ord: i})
		desc.valueCols = append(desc.valueCols, i)
		encRow = append(encRow, rowenc.DatumToEncDatum(d.ResolvedType(), d))
	}
	return Row{
		EventDescriptor: &desc,
		datums:          encRow,
		alloc:           &alloc,
	}
}

// TestingGetFamilyIDFromKey returns family ID encoded in the specified roachpb.Key.
// Exposed for testing.
func TestingGetFamilyIDFromKey(
	decoder Decoder, key roachpb.Key, ts hlc.Timestamp,
) (descpb.FamilyID, error) {
	_, familyID, err := decoder.(*eventDecoder).rfCache.tableDescForKey(context.Background(), key, ts)
	return familyID, err
}

// MakeRowFromTuple converts a SQL datum produced by, for example, SELECT ROW(foo.*),
// into the same kind of cdcevent.Row you'd get as a result of an insert, but without
// the primary key.
func MakeRowFromTuple(ctx context.Context, evalCtx *eval.Context, t *tree.DTuple) Row {
	r := Projection{EventDescriptor: &EventDescriptor{}}
	names := t.ResolvedType().TupleLabels()
	for i, d := range t.D {
		var name string
		if names == nil {
			name = fmt.Sprintf("col%d", i+1)
		} else {
			name = names[i]
		}
		r.AddValueColumn(name, d.ResolvedType())
		if err := r.SetValueDatumAt(i, d); err != nil {
			if build.IsRelease() {
				log.Warningf(ctx, "failed to set row value from tuple due to error %v", err)
				_ = r.SetValueDatumAt(i, tree.DNull)
			} else {
				panic(err)
			}
		}
	}
	return Row(r)
}

// TestingMakeEventRowFromEncDatums creates event row from specified  enc datum row.
// encRow assumed to contain *already decoded* datums.
// The first numKeyCols are assumed to be primary key columns.
// Columns names are generated based on the datum types.
func TestingMakeEventRowFromEncDatums(
	encRow rowenc.EncDatumRow, colTypes []*types.T, numKeyCols int, deleted bool,
) Row {
	if len(encRow) != len(colTypes) {
		panic("unexpected length mismatch")
	}
	intRange := func(start, end int) (res []int) {
		for i := 0; i < end; i++ {
			res = append(res, i)
		}
		return res
	}
	ed := &EventDescriptor{
		Metadata: Metadata{
			TableID:    42,
			TableName:  "randtbl",
			FamilyName: "primary",
		},
		cols: func() (cols []ResultColumn) {
			names := make(map[string]int, len(encRow))
			for i, typ := range colTypes {
				colName := fmt.Sprintf("col_%s", typ.String())
				names[colName]++
				if names[colName] > 1 {
					colName += fmt.Sprintf("_%d", names[colName]-1)
				}
				cols = append(cols, ResultColumn{
					ResultColumn: colinfo.ResultColumn{
						Name:    colName,
						Typ:     typ,
						TableID: 42,
					},
					ord: i,
				})
			}
			return cols
		}(),
		keyCols:   intRange(0, numKeyCols),
		valueCols: intRange(0, len(encRow)),
	}

	var alloc tree.DatumAlloc
	return Row{
		EventDescriptor: ed,
		datums:          encRow,
		deleted:         deleted,
		alloc:           &alloc,
	}
}

// getRelevantColumnsForFamily returns an array of column ids for public columns
// including only primary key columns and columns in the specified familyDesc,
// If includeVirtual is true, virtual columns, which may be outside the specified
// family, will be included.
func getRelevantColumnsForFamily(
	tableDesc catalog.TableDescriptor, familyDesc *descpb.ColumnFamilyDescriptor,
) ([]descpb.ColumnID, error) {
	cols := tableDesc.GetPrimaryIndex().CollectKeyColumnIDs()
	for _, colID := range familyDesc.ColumnIDs {
		cols.Add(colID)
	}

	// Maintain the ordering of tableDesc.PublicColumns(), which is
	// matches the order of columns in the SQL table.
	idx := 0
	result := make([]descpb.ColumnID, cols.Len())
	for _, colID := range tableDesc.PublicColumnIDs() {
		if cols.Contains(colID) {
			result[idx] = colID
			idx++
		}
	}

	// Some columns in familyDesc.ColumnIDs may not be public, so
	// result may contain fewer columns than cols.
	result = result[:idx]
	return result, nil
}
