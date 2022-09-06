// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cdcevent

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/cast"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// Projection is a helper to facilitate construction of "projection" rows.
// Projection is constructed given the underlying event descriptor.  Only the key
// columns from the descriptor are initialized upon construction.  All other
// value columns returned by projection need to be configured separated via AddValueColumn,
// and the value for that column must later be set via SetValueDatumAt.
// All columns added to this projection are in the ordinal order.
type Projection Row

// MakeProjection returns Projection builder given underlying descriptor.
func MakeProjection(d *EventDescriptor) Projection {
	p := Projection{
		EventDescriptor: &EventDescriptor{Metadata: d.Metadata},
	}

	// Add all primary key columns.
	for _, colIdx := range d.keyCols {
		col := d.cols[colIdx]
		p.addColumn(col.Name, col.Typ, col.sqlString, &p.keyCols)
	}
	return p
}

func (p *Projection) addColumn(name string, typ *types.T, sqlString string, colIdxSlice *[]int) {
	ord := len(p.cols)
	p.cols = append(p.cols, ResultColumn{
		ResultColumn: colinfo.ResultColumn{
			Name: name,
			Typ:  typ,
		},
		ord:       ord,
		sqlString: sqlString,
	})

	p.datums = append(p.datums, rowenc.EncDatum{})
	*colIdxSlice = append(*colIdxSlice, ord)
	if typ.UserDefined() {
		p.udtCols = append(p.udtCols, ord)
	}
}

// AddValueColumn adds a value column to this projection builder.
func (p *Projection) AddValueColumn(name string, typ *types.T) {
	p.addColumn(name, typ, "", &p.valueCols)
}

// SetValueDatumAt sets value datum at specified position.
func (p *Projection) SetValueDatumAt(evalCtx *eval.Context, pos int, d tree.Datum) error {
	pos += len(p.keyCols)
	if pos >= len(p.datums) {
		return errors.AssertionFailedf("%d out of bounds", pos)
	}

	col := p.cols[pos]
	if !col.Typ.Equal(d.ResolvedType()) {
		if !cast.ValidCast(d.ResolvedType(), col.Typ, cast.ContextImplicit) {
			return pgerror.Newf(pgcode.DatatypeMismatch,
				"expected type %s for column %s@%d, found %s", col.Typ, col.Name, pos, d.ResolvedType())
		}
		cd, err := eval.PerformCast(evalCtx, d, col.Typ)
		if err != nil {
			return errors.Wrapf(err, "expected type %s for column %s@%d, found %s",
				col.Typ, col.Name, pos, d.ResolvedType())
		}
		d = cd
	}

	p.datums[pos].Datum = d
	return nil
}

// Project returns row projection.
func (p *Projection) Project(r Row) (Row, error) {
	p.deleted = r.IsDeleted()

	// Copy key datums.
	idx := 0
	if err := r.ForEachKeyColumn().Datum(func(d tree.Datum, col ResultColumn) error {
		if idx >= len(p.keyCols) || idx >= len(p.datums) {
			return errors.AssertionFailedf("%d out of bounds when projecting key column %s", idx, col.Name)
		}

		p.datums[idx].Datum = d
		idx++
		return nil
	}); err != nil {
		return Row{}, err
	}

	return Row(*p), nil
}
