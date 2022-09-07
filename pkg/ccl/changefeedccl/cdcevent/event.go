// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cdcevent

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

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
	DecodeKV(ctx context.Context, kv roachpb.KeyValue, schemaTS hlc.Timestamp) (Row, error)
}

// Row holds a row corresponding to an event.
type Row struct {
	*EventDescriptor

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

// ForEachKeyColumn returns Iterator for each key column
func (r Row) ForEachKeyColumn() Iterator {
	return iter{r: r, cols: r.keyCols}
}

// ForEachColumn returns Iterator for each column.
func (r Row) ForEachColumn() Iterator {
	return iter{r: r, cols: r.valueCols}
}

// ForEachUDTColumn returns Datum iterator for each column containing user defined types.
func (r Row) ForEachUDTColumn() Iterator {
	return iter{r: r, cols: r.udtCols}
}

// DatumAt returns Datum at specified position.
func (r Row) DatumAt(at int) (tree.Datum, error) {
	if at >= len(r.cols) {
		return nil, errors.AssertionFailedf("column at %d out of bounds", at)
	}
	col := r.cols[at]
	if col.ord >= len(r.datums) {
		return nil, errors.AssertionFailedf("column ordinal at %d out of bounds", col.ord)
	}
	encDatum := r.datums[col.ord]
	if err := encDatum.EnsureDecoded(col.Typ, r.alloc); err != nil {
		return nil, errors.Wrapf(err, "error decoding column %q as type %s", col.Name, col.Typ.String())
	}
	return encDatum.Datum, nil
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

// forEachColumn is a helper which invokes fn for reach column in the ordColumn list.
func (r Row) forEachDatum(fn DatumFn, colIndexes []int) error {
	for _, colIdx := range colIndexes {
		col := r.cols[colIdx]
		if col.ord >= len(r.datums) {
			return errors.AssertionFailedf("index [%d] out of range for column %q", col.ord, col.Name)
		}
		encDatum := r.datums[col.ord]
		if err := encDatum.EnsureDecoded(col.Typ, r.alloc); err != nil {
			return errors.Wrapf(err, "error decoding column %q as type %s", col.Name, col.Typ.String())
		}

		if err := fn(encDatum.Datum, col); err != nil {
			return iterutil.Map(err)
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
	keyCols   []int // Primary key columns.
	valueCols []int // All column family columns.
	udtCols   []int // Columns containing UDTs.
}

// NewEventDescriptor returns EventDescriptor for specified table and family descriptors.
func NewEventDescriptor(
	desc catalog.TableDescriptor,
	family *descpb.ColumnFamilyDescriptor,
	includeVirtualColumns bool,
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
		td: desc,
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
			ord:       ord,
			sqlString: col.ColumnDesc().SQLStringNotHumanReadable(),
		}

		colIdx := len(sd.cols)
		sd.cols = append(sd.cols, resultColumn)

		if col.GetType().UserDefined() {
			sd.udtCols = append(sd.udtCols, colIdx)
		}
		return colIdx
	}

	// Primary key columns must be added in the same order they
	// appear in the primary key index.
	primaryIdx := desc.GetPrimaryIndex()
	colOrd := catalog.ColumnIDToOrdinalMap(desc.PublicColumns())
	sd.keyCols = make([]int, primaryIdx.NumKeyColumns())
	var primaryKeyOrdinal catalog.TableColMap

	for i := 0; i < primaryIdx.NumKeyColumns(); i++ {
		ord, ok := colOrd.Get(primaryIdx.GetKeyColumnID(i))
		if !ok {
			return nil, errors.AssertionFailedf("expected to find column %d", ord)
		}
		primaryKeyOrdinal.Set(desc.PublicColumns()[ord].GetID(), i)
	}

	// Remaining columns go in same order as public columns.
	inFamily := catalog.MakeTableColSet(family.ColumnIDs...)
	for ord, col := range desc.PublicColumns() {
		isInFamily := inFamily.Contains(col.GetID())
		virtual := col.IsVirtual() && includeVirtualColumns
		isValueCol := isInFamily || virtual
		pKeyOrd, isPKey := primaryKeyOrdinal.Get(col.GetID())
		if isValueCol || isPKey {
			colIdx := addColumn(col, ord)
			if isValueCol {
				sd.valueCols = append(sd.valueCols, colIdx)
			}

			if isPKey {
				sd.keyCols[pKeyOrd] = colIdx
			}
		}
	}

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

// HasUserDefinedTypes returns true if this descriptor contains user defined columns.
func (d *EventDescriptor) HasUserDefinedTypes() bool {
	return len(d.udtCols) > 0
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

	// kvFetcher used to decode datums.
	kvFetcher row.SpanKVFetcher

	// factory for constructing event descriptors.
	getEventDescriptor eventDescriptorFactory

	// Alloc used when decoding datums.
	alloc tree.DatumAlloc

	// State pertaining for decoding of a single key.
	fetcher  *row.Fetcher                   // Fetcher to decode KV
	desc     catalog.TableDescriptor        // Current descriptor
	family   *descpb.ColumnFamilyDescriptor // Current family
	schemaTS hlc.Timestamp                  // Schema timestamp.
}

func getEventDescriptorCached(
	desc catalog.TableDescriptor,
	family *descpb.ColumnFamilyDescriptor,
	includeVirtual bool,
	schemaTS hlc.Timestamp,
	cache *cache.UnorderedCache,
) (*EventDescriptor, error) {
	idVer := idVersion{id: desc.GetID(), version: desc.GetVersion(), family: family.ID}

	if v, ok := cache.Get(idVer); ok {
		ed := v.(*EventDescriptor)
		if catalog.UserDefinedTypeColsHaveSameVersion(ed.td, desc) {
			return ed, nil
		}
	}

	ed, err := NewEventDescriptor(desc, family, includeVirtual, schemaTS)
	if err != nil {
		return nil, err
	}
	cache.Add(idVer, ed)
	return ed, nil
}

// NewEventDecoder returns key value decoder.
func NewEventDecoder(
	ctx context.Context,
	cfg *execinfra.ServerConfig,
	targets changefeedbase.Targets,
	includeVirtual bool,
) (Decoder, error) {
	rfCache, err := newRowFetcherCache(
		ctx,
		cfg.Codec,
		cfg.LeaseManager.(*lease.Manager),
		cfg.CollectionFactory,
		cfg.DB,
		targets,
	)
	if err != nil {
		return nil, err
	}

	eventDescriptorCache := cache.NewUnorderedCache(defaultCacheConfig)
	getEventDescriptor := func(
		desc catalog.TableDescriptor,
		family *descpb.ColumnFamilyDescriptor,
		schemaTS hlc.Timestamp,
	) (*EventDescriptor, error) {
		return getEventDescriptorCached(desc, family, includeVirtual, schemaTS, eventDescriptorCache)
	}

	return &eventDecoder{
		getEventDescriptor: getEventDescriptor,
		rfCache:            rfCache,
	}, nil
}

// DecodeKV decodes key value at specified schema timestamp.
func (d *eventDecoder) DecodeKV(
	ctx context.Context, kv roachpb.KeyValue, schemaTS hlc.Timestamp,
) (Row, error) {
	if err := d.initForKey(ctx, kv.Key, schemaTS); err != nil {
		return Row{}, err
	}

	d.kvFetcher.KVs = d.kvFetcher.KVs[:0]
	d.kvFetcher.KVs = append(d.kvFetcher.KVs, kv)
	if err := d.fetcher.StartScanFrom(ctx, &d.kvFetcher); err != nil {
		return Row{}, err
	}

	datums, isDeleted, err := d.nextRow(ctx)
	if err != nil {
		return Row{}, err
	}

	ed, err := d.getEventDescriptor(d.desc, d.family, schemaTS)
	if err != nil {
		return Row{}, err
	}

	return Row{
		EventDescriptor: ed,
		datums:          datums,
		deleted:         isDeleted,
		alloc:           &d.alloc,
	}, nil
}

// initForKey initializes decoder state to prepare it to decode
// key/value at specified timestamp.
func (d *eventDecoder) initForKey(
	ctx context.Context, key roachpb.Key, schemaTS hlc.Timestamp,
) error {
	desc, familyID, err := d.rfCache.tableDescForKey(ctx, key, schemaTS)
	if err != nil {
		return err
	}

	fetcher, family, err := d.rfCache.RowFetcherForColumnFamily(desc, familyID)
	if err != nil {
		return err
	}

	d.schemaTS = schemaTS
	d.desc = desc
	d.family = family
	d.fetcher = fetcher
	return nil
}

// nextRow returns next encoded row, and a flag indicating if a row was deleted.
func (d *eventDecoder) nextRow(ctx context.Context) (rowenc.EncDatumRow, bool, error) {
	datums, _, err := d.fetcher.NextRow(ctx)
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

// String returns debug string describing event source.
func (m Metadata) String() string {
	return fmt.Sprintf("{table: %d family: %d}", m.TableID, m.FamilyID)
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
	family, err := desc.FindFamilyByID(familyID)
	if err != nil {
		panic(err) // primary column family always exists.
	}
	const includeVirtual = false
	ed, err := NewEventDescriptor(desc, family, includeVirtual, hlc.Timestamp{})
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
func MakeRowFromTuple(evalCtx *eval.Context, t *tree.DTuple) Row {
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
		if err := r.SetValueDatumAt(evalCtx, i, d); err != nil {
			if build.IsRelease() {
				log.Warningf(context.Background(), "failed to set row value from tuple due to error %v", err)
				_ = r.SetValueDatumAt(evalCtx, i, tree.DNull)
			} else {
				panic(err)
			}
		}
	}
	return Row(r)
}
