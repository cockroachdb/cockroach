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

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/errors"
)

// EventSource identifies the source of the event.
// This interface restricts the interface exposed by catalog.TableDescriptor
// to a small subset of methods necessary to perform event processing.
type EventSource interface {
	// TableID returns the id of the table.
	TableID() descpb.ID
	// TableName returns the name of the table.
	TableName() string
	// Version returns table descriptor version.
	Version() descpb.DescriptorVersion
	// FamilyID returns familyID for the event.
	FamilyID() descpb.FamilyID
	// FamilyName returns family name.
	FamilyName() string
	// NumFamilies returns number of families in the table.
	NumFamilies() int
}

// EventDecoder is an interface for decoding KVs into cdc event row.
type EventDecoder interface {
	// DecodeKV decodes specified key value to EventRow.
	DecodeKV(ctx context.Context, kv roachpb.KeyValue, schemaTS hlc.Timestamp) (EventRow, error)
}

// EventRow holds a row corresponding to an event.
type EventRow struct {
	*eventDescriptor

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
func (e EventRow) ForEachKeyColumn() Iterator {
	return iter{r: e, cols: e.keyCols}
}

// ForEachColumn returns Iterator for each column.
func (e EventRow) ForEachColumn() Iterator {
	return iter{r: e, cols: e.familyCols}
}

// ForEachUDTColumn returns Datum iterator for each column containing user defined types.
func (e EventRow) ForEachUDTColumn() Iterator {
	return iter{r: e, cols: e.udtCols}
}

// IsDeleted returns true if event corresponds to a deletion event.
func (e EventRow) IsDeleted() bool {
	return e.deleted
}

// IsInitialized returns true if event row is initialized.
func (e EventRow) IsInitialized() bool {
	return e.eventDescriptor != nil
}

// HasValues returns true if event row has values to decode.
func (e EventRow) HasValues() bool {
	return e.datums != nil
}

// forEachColumn is a helper which invokes fn for reach column in the ordColumn list.
func (e EventRow) forEachDatum(fn DatumFn, colIndexes []int) error {
	for _, colIdx := range colIndexes {
		col := e.cols[colIdx]
		if col.ord >= len(e.datums) {
			return errors.AssertionFailedf("index [%d] out of range for column %q", col.ord, col.Name)
		}
		encDatum := e.datums[col.ord]
		if err := encDatum.EnsureDecoded(col.Typ, e.alloc); err != nil {
			return errors.Wrapf(err, "error decoding column %q as type %s", col.Name, col.Typ.String())
		}

		if err := fn(encDatum.Datum, col); err != nil {
			if iterutil.Done(err) {
				return nil
			}
			return err
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

// eventDescriptor implements EventSource interface, and associates
// table/family descriptor with the information needed to decode events.
type eventDescriptor struct {
	desc   catalog.TableDescriptor
	family *descpb.ColumnFamilyDescriptor

	// List of result columns produced by this descriptor.
	// This may be different from the table descriptors public columns
	// (e.g. in case of projection).
	cols []ResultColumn

	// Precomputed index lists into cols.
	keyCols    []int // Primary key columns.
	familyCols []int // All column family columns.
	udtCols    []int // Columns containing UDTs.
}

var _ EventSource = (*eventDescriptor)(nil)

func newEventDescriptor(
	desc catalog.TableDescriptor, family *descpb.ColumnFamilyDescriptor, includeVirtualColumns bool,
) (*eventDescriptor, error) {
	var inFamily catalog.TableColSet
	for _, colID := range family.ColumnIDs {
		inFamily.Add(colID)
	}

	sd := eventDescriptor{
		family: family,
		desc:   desc,
	}

	// addColumn is a helper to add a column to this descriptor.
	addColumn := func(col catalog.Column, ord int, colIdxSlice *[]int) {
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
		*colIdxSlice = append(*colIdxSlice, colIdx)

		if col.GetType().UserDefined() {
			sd.udtCols = append(sd.udtCols, colIdx)
		}
	}

	// Primary key columns must be added in the same order they
	// appear in the primary key index.
	primaryIdx := desc.GetPrimaryIndex()
	colOrd := catalog.ColumnIDToOrdinalMap(desc.PublicColumns())
	for i := 0; i < primaryIdx.NumKeyColumns(); i++ {
		ord, ok := colOrd.Get(primaryIdx.GetKeyColumnID(i))
		if !ok {
			return nil, errors.AssertionFailedf("expected to find column %d", ord)
		}
		addColumn(desc.PublicColumns()[ord], ord, &sd.keyCols)
	}

	// Remaining columns go in same order as public columns.
	for ord, col := range desc.PublicColumns() {
		isInFamily := inFamily.Contains(col.GetID())
		virtual := col.IsVirtual() && includeVirtualColumns
		if isInFamily || virtual {
			addColumn(col, ord, &sd.familyCols)
		}
	}

	return &sd, nil
}

// TableID implements EventSource.
func (d *eventDescriptor) TableID() descpb.ID {
	return d.desc.GetID()
}

// TableName implements EventSource.
func (d *eventDescriptor) TableName() string {
	return d.desc.GetName()
}

// Version implements EventSource.
func (d *eventDescriptor) Version() descpb.DescriptorVersion {
	return d.desc.GetVersion()
}

// FamilyID implements EventSource.
func (d *eventDescriptor) FamilyID() descpb.FamilyID {
	return d.family.ID
}

// FamilyName implements EventSource.
func (d *eventDescriptor) FamilyName() string {
	return d.family.Name
}

// NumFamilies implements EventSource.
func (d *eventDescriptor) NumFamilies() int {
	return d.desc.NumFamilies()
}

// forEachColumn is a helper which invokes fn for reach column in the ordColumn list.
func (d *eventDescriptor) forEachColumn(fn ColumnFn, colIndexes []int) error {
	for _, colIdx := range colIndexes {
		if err := fn(d.cols[colIdx]); err != nil {
			if iterutil.Done(err) {
				return nil
			}
			return err
		}
	}
	return nil
}

// DebugString returns event descriptor debug information.
func (d *eventDescriptor) DebugString() string {
	return fmt.Sprintf("eventDescriptor{table: %q(%d) family: %q(%d) pkCols=%v valCols=%v",
		d.TableName(), d.TableID(), d.FamilyName(), d.FamilyID(), d.keyCols, d.familyCols)
}

type eventDescriptorFactory func(
	desc catalog.TableDescriptor,
	family *descpb.ColumnFamilyDescriptor,
) (*eventDescriptor, error)

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
	cache *cache.UnorderedCache,
) (*eventDescriptor, error) {
	idVer := idVersion{id: desc.GetID(), version: desc.GetVersion(), family: family.ID}

	if v, ok := cache.Get(idVer); ok {
		ed := v.(*eventDescriptor)

		// Normally, this is a no-op since majority of changefeeds do not use UDTs.
		// However, in case we do, we must update cached UDT information based on this
		// descriptor since it has up-to-date type information.
		for _, udtColIdx := range ed.udtCols {
			ord := ed.cols[udtColIdx].ord
			ed.cols[udtColIdx].Typ = desc.PublicColumns()[ord].GetType()
		}

		return ed, nil
	}

	ed, err := newEventDescriptor(desc, family, includeVirtual)
	if err != nil {
		return nil, err
	}
	cache.Add(idVer, ed)
	return ed, nil
}

// NewEventDecoder returns key value decoder.
func NewEventDecoder(
	ctx context.Context, cfg *execinfra.ServerConfig, details jobspb.ChangefeedDetails,
) EventDecoder {
	rfCache := newRowFetcherCache(
		ctx,
		cfg.Codec,
		cfg.LeaseManager.(*lease.Manager),
		cfg.CollectionFactory,
		cfg.DB,
		details,
	)

	includeVirtual := details.Opts[changefeedbase.OptVirtualColumns] == string(changefeedbase.OptVirtualColumnsNull)
	eventDescriptorCache := cache.NewUnorderedCache(defaultCacheConfig)
	getEventDescriptor := func(
		desc catalog.TableDescriptor,
		family *descpb.ColumnFamilyDescriptor,
	) (*eventDescriptor, error) {
		return getEventDescriptorCached(desc, family, includeVirtual, eventDescriptorCache)
	}

	return &eventDecoder{
		getEventDescriptor: getEventDescriptor,
		rfCache:            rfCache,
	}
}

// DecodeKV decodes key value at specified schema timestamp.
func (d *eventDecoder) DecodeKV(
	ctx context.Context, kv roachpb.KeyValue, schemaTS hlc.Timestamp,
) (EventRow, error) {
	if err := d.initForKey(ctx, kv.Key, schemaTS); err != nil {
		return EventRow{}, err
	}

	d.kvFetcher.KVs = d.kvFetcher.KVs[:0]
	d.kvFetcher.KVs = append(d.kvFetcher.KVs, kv)
	if err := d.fetcher.StartScanFrom(ctx, &d.kvFetcher, false /* traceKV */); err != nil {
		return EventRow{}, err
	}
	// Copy datums since row fetcher reuses alloc.
	datums, isDeleted, err := d.nextRow(ctx)
	if err != nil {
		return EventRow{}, errors.AssertionFailedf("unexpected non-empty datums")
	}

	ed, err := d.getEventDescriptor(d.desc, d.family)
	if err != nil {
		return EventRow{}, err
	}

	return EventRow{
		eventDescriptor: ed,
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

// EventSourceString returns debug string describing event source.
func EventSourceString(src EventSource) string {
	return fmt.Sprintf("{table: %d family: %d}", src.TableID(), src.FamilyID())
}

type iter struct {
	r    EventRow
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

// TestingMakeEventRow initializes EventRow with provided arguments.
// Exposed for unit tests.
func TestingMakeEventRow(
	desc catalog.TableDescriptor, encRow rowenc.EncDatumRow, deleted bool,
) EventRow {
	family, err := desc.FindFamilyByID(0)
	if err != nil {
		panic(err) // primary column family always exists.
	}
	const includeVirtual = false
	ed, err := newEventDescriptor(desc, family, includeVirtual)
	if err != nil {
		panic(err)
	}
	var alloc tree.DatumAlloc
	return EventRow{
		eventDescriptor: ed,
		datums:          encRow,
		deleted:         deleted,
		alloc:           &alloc,
	}
}
