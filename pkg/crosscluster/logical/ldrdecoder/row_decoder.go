// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ldrdecoder

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/sqlwriter"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// TableMapping represents a mapping from a remote descriptor
// to a local descriptor.
type TableMapping struct {
	SourceDescriptor catalog.TableDescriptor
	DestID           descpb.ID
}

type tableDecoder struct {
	decoder   cdcevent.Decoder
	srcToDest map[descpb.ID]*destinationTable
}

func newTableDecoder(
	ctx context.Context,
	descriptors descs.DB,
	settings *cluster.Settings,
	tableMappings []TableMapping,
) (tableDecoder, error) {
	srcToDest := make(map[descpb.ID]*destinationTable, len(tableMappings))
	err := descriptors.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		for _, table := range tableMappings {
			descriptor, err := txn.Descriptors().GetLeasedImmutableTableByID(ctx, txn.KV(), table.DestID)
			if err != nil {
				return err
			}

			columns := sqlwriter.GetColumnSchema(descriptor)
			columnNames := make([]string, 0, len(columns))
			columnTypes := make([]*types.T, 0, len(columns))
			for _, column := range columns {
				columnNames = append(columnNames, column.Column.GetName())
				columnTypes = append(columnTypes, column.Column.GetType().Canonical())
			}

			srcToDest[table.SourceDescriptor.GetID()] = &destinationTable{
				id:          table.DestID,
				columns:     columnNames,
				columnTypes: columnTypes,
			}
		}
		return nil
	})
	if err != nil {
		return tableDecoder{}, err
	}

	decoder, err := NewCdcEventDecoder(ctx, tableMappings, settings)
	if err != nil {
		return tableDecoder{}, err
	}

	return tableDecoder{
		decoder:   decoder,
		srcToDest: srcToDest,
	}, nil
}

type destinationTable struct {
	id          descpb.ID
	columns     []string
	columnTypes []*types.T
}

func (t *tableDecoder) decodeEvent(
	ctx context.Context, event streampb.StreamEvent_KV,
) (DecodedRow, cdcevent.Row, error) {
	var err error
	event.KeyValue.Key, err = keys.StripTenantPrefix(event.KeyValue.Key)
	if err != nil {
		return DecodedRow{}, cdcevent.Row{}, errors.Wrap(err, "failed to strip tenant prefix")
	}

	decodedRow, status, err := t.decoder.DecodeKV(ctx, event.KeyValue, cdcevent.CurrentRow, event.KeyValue.Value.Timestamp, false)
	if err != nil {
		return DecodedRow{}, cdcevent.Row{}, err
	}
	if status != cdcevent.DecodeOK {
		return DecodedRow{}, cdcevent.Row{}, errors.Newf("unexpected decode status: %v", status)
	}

	dstTable, ok := t.srcToDest[decodedRow.TableID]
	if !ok {
		return DecodedRow{}, cdcevent.Row{}, errors.AssertionFailedf("table %d not found", decodedRow.TableID)
	}

	row, err := dstTable.toLocalDatums(decodedRow)
	if err != nil {
		return DecodedRow{}, cdcevent.Row{}, err
	}

	var prevRow tree.Datums
	if event.PrevValue.RawBytes != nil {
		var prev roachpb.KeyValue
		prev.Key = event.KeyValue.Key
		prev.Value = event.PrevValue

		decodedPrevRow, prevStatus, err := t.decoder.DecodeKV(ctx, prev, cdcevent.PrevRow, event.PrevValue.Timestamp, false)
		if err != nil {
			return DecodedRow{}, cdcevent.Row{}, err
		}
		if prevStatus != cdcevent.DecodeOK {
			return DecodedRow{}, cdcevent.Row{}, errors.Newf("unexpected decode status: %v", prevStatus)
		}

		prevRow, err = dstTable.toLocalDatums(decodedPrevRow)
		if err != nil {
			return DecodedRow{}, cdcevent.Row{}, err
		}
	}

	return DecodedRow{
		TableID:      dstTable.id,
		IsDelete:     decodedRow.IsDeleted(),
		RowTimestamp: event.KeyValue.Value.Timestamp,
		Row:          row,
		PrevRow:      prevRow,
	}, decodedRow, nil
}

// toLocalDatums creates a row with the types of the datums converted to to the
// types required by the remote descriptor. toLocalDatums takes ownerhsip of
// the input row and may modify datums from the decoded input row.
func (d *destinationTable) toLocalDatums(row cdcevent.Row) (tree.Datums, error) {
	localRow := make(tree.Datums, len(d.columns))

	it, err := row.DatumsNamed(d.columns)
	if err != nil {
		return nil, err
	}

	columnIndex := 0
	if err := it.Datum(func(datum tree.Datum, col cdcevent.ResultColumn) error {
		typ := d.columnTypes[columnIndex]

		switch d := datum.(type) {
		case *tree.DEnum:
			d.EnumTyp = typ
		case *tree.DArray:
			d.ParamTyp = typ
		case *tree.DTuple:
			datum = tree.NewDTuple(typ, d.D...)
		}

		localRow[columnIndex] = datum

		columnIndex += 1
		return nil
	}); err != nil {
		return nil, err
	}

	return localRow, nil
}
