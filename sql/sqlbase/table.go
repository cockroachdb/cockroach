// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package sqlbase

import (
	"errors"
	"fmt"

	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util"
)

// MakeTableDesc creates a table descriptor from a CreateTable statement.
func MakeTableDesc(p *parser.CreateTable, parentID ID) (TableDescriptor, error) {
	desc := TableDescriptor{}
	if err := p.Table.NormalizeTableName(""); err != nil {
		return desc, err
	}
	desc.Name = p.Table.Table()
	desc.ParentID = parentID
	desc.FormatVersion = BaseFormatVersion
	// We don't use version 0.
	desc.Version = 1

	var primaryIndexColumnSet map[parser.Name]struct{}
	for _, def := range p.Defs {
		switch d := def.(type) {
		case *parser.ColumnTableDef:
			col, idx, err := MakeColumnDefDescs(d)
			if err != nil {
				return desc, err
			}
			desc.AddColumn(*col)
			if idx != nil {
				if err := desc.AddIndex(*idx, d.PrimaryKey); err != nil {
					return desc, err
				}
			}
		case *parser.IndexTableDef:
			idx := IndexDescriptor{
				Name:             string(d.Name),
				StoreColumnNames: d.Storing,
			}
			if err := idx.FillColumns(d.Columns); err != nil {
				return desc, err
			}
			if err := desc.AddIndex(idx, false); err != nil {
				return desc, err
			}
		case *parser.UniqueConstraintTableDef:
			idx := IndexDescriptor{
				Name:             string(d.Name),
				Unique:           true,
				StoreColumnNames: d.Storing,
			}
			if err := idx.FillColumns(d.Columns); err != nil {
				return desc, err
			}
			if err := desc.AddIndex(idx, d.PrimaryKey); err != nil {
				return desc, err
			}
			if d.PrimaryKey {
				primaryIndexColumnSet = make(map[parser.Name]struct{})
				for _, c := range d.Columns {
					primaryIndexColumnSet[c.Column] = struct{}{}
				}
			}
		default:
			return desc, util.Errorf("unsupported table def: %T", def)
		}
	}

	if primaryIndexColumnSet != nil {
		// Primary index columns are not nullable.
		for i := range desc.Columns {
			if _, ok := primaryIndexColumnSet[parser.Name(desc.Columns[i].Name)]; ok {
				desc.Columns[i].Nullable = false
			}
		}
	}

	return desc, nil
}

func defaultContainsPlaceholdersError(typedExpr parser.TypedExpr) error {
	return fmt.Errorf("default expression '%s' may not contain placeholders", typedExpr)
}

func incompatibleColumnDefaultTypeError(colDatumType parser.Datum, defaultType parser.Datum) error {
	return fmt.Errorf("incompatible column type and default expression: %s vs %s",
		colDatumType.Type(), defaultType.Type())
}

// SanitizeDefaultExpr verifies a default expression is valid and has the
// correct type.
func SanitizeDefaultExpr(expr parser.Expr, colDatumType parser.Datum) error {
	typedExpr, err := parser.TypeCheck(expr, nil, colDatumType)
	if err != nil {
		return err
	}
	if defaultType := typedExpr.ReturnType(); colDatumType != defaultType {
		return incompatibleColumnDefaultTypeError(colDatumType, defaultType)
	}
	if parser.ContainsVars(typedExpr) {
		return defaultContainsPlaceholdersError(typedExpr)
	}
	return nil
}

// MakeColumnDefDescs creates the column descriptor for a column, as well as the
// index descriptor if the column is a primary key or unique.
func MakeColumnDefDescs(d *parser.ColumnTableDef) (*ColumnDescriptor, *IndexDescriptor, error) {
	col := &ColumnDescriptor{
		Name:     string(d.Name),
		Nullable: d.Nullable != parser.NotNull && !d.PrimaryKey,
	}

	var colDatumType parser.Datum
	switch t := d.Type.(type) {
	case *parser.BoolType:
		col.Type.Kind = ColumnType_BOOL
		colDatumType = parser.DummyBool
	case *parser.IntType:
		col.Type.Kind = ColumnType_INT
		col.Type.Width = int32(t.N)
		colDatumType = parser.DummyInt
	case *parser.FloatType:
		col.Type.Kind = ColumnType_FLOAT
		col.Type.Precision = int32(t.Prec)
		colDatumType = parser.DummyFloat
	case *parser.DecimalType:
		col.Type.Kind = ColumnType_DECIMAL
		col.Type.Width = int32(t.Scale)
		col.Type.Precision = int32(t.Prec)
		colDatumType = parser.DummyDecimal
	case *parser.DateType:
		col.Type.Kind = ColumnType_DATE
		colDatumType = parser.DummyDate
	case *parser.TimestampType:
		col.Type.Kind = ColumnType_TIMESTAMP
		colDatumType = parser.DummyTimestamp
	case *parser.TimestampTZType:
		col.Type.Kind = ColumnType_TIMESTAMPTZ
		colDatumType = parser.DummyTimestampTZ
	case *parser.IntervalType:
		col.Type.Kind = ColumnType_INTERVAL
		colDatumType = parser.DummyInterval
	case *parser.StringType:
		col.Type.Kind = ColumnType_STRING
		col.Type.Width = int32(t.N)
		colDatumType = parser.DummyString
	case *parser.BytesType:
		col.Type.Kind = ColumnType_BYTES
		colDatumType = parser.DummyBytes
	default:
		return nil, nil, util.Errorf("unexpected type %T", t)
	}

	if col.Type.Kind == ColumnType_DECIMAL {
		switch {
		case col.Type.Precision == 0 && col.Type.Width > 0:
			// TODO (seif): Find right range for error message.
			return nil, nil, errors.New("invalid NUMERIC precision 0")
		case col.Type.Precision < col.Type.Width:
			return nil, nil, fmt.Errorf("NUMERIC scale %d must be between 0 and precision %d",
				col.Type.Width, col.Type.Precision)
		}
	}

	if d.DefaultExpr != nil {
		// Verify the default expression type is compatible with the column type.
		if err := SanitizeDefaultExpr(d.DefaultExpr, colDatumType); err != nil {
			return nil, nil, err
		}
		s := d.DefaultExpr.String()
		col.DefaultExpr = &s
	}

	// CHECK expressions seem to vary across databases. Wikipedia's entry on
	// Check_constraint (https://en.wikipedia.org/wiki/Check_constraint) says
	// that if the constraint refers to a single column only, it is possible to
	// specify the constraint as part of the column definition. Postgres allows
	// specifying them anywhere about any columns, but it moves all constraints to
	// the table level (i.e., columns never have a check constraint themselves). We
	// will adhere to the stricter definition.
	if d.CheckExpr != nil {
		s := d.CheckExpr.String()
		col.CheckExpr = &s

		preFn := func(expr parser.Expr) (err error, recurse bool, newExpr parser.Expr) {
			qname, ok := expr.(*parser.QualifiedName)
			if !ok {
				// Not a qname, don't do anything to this node.
				return nil, true, expr
			}
			if err := qname.NormalizeColumnName(); err != nil {
				return err, false, nil
			}
			if qname.IsStar() || qname.Base != "" || qname.Column() != col.Name {
				err := fmt.Errorf("argument of CHECK cannot refer to other columns: \"%s\"", qname)
				return err, false, nil
			}
			// Convert to a dummy datum of the correct type.
			return nil, false, colDatumType
		}

		expr, err := parser.SimpleVisit(d.CheckExpr, preFn)
		if err != nil {
			return nil, nil, err
		}
		if typedExpr, err := expr.TypeCheck(nil, parser.DummyBool); err != nil {
			return nil, nil, err
		} else if typ := typedExpr.ReturnType(); !typ.TypeEqual(parser.DummyBool) {
			return nil, nil, fmt.Errorf("argument of CHECK must be type bool, not type %s", typ.Type())
		}
	}

	var idx *IndexDescriptor
	if d.PrimaryKey || d.Unique {
		idx = &IndexDescriptor{
			Unique:           true,
			ColumnNames:      []string{string(d.Name)},
			ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC},
		}
	}

	return col, idx, nil
}
