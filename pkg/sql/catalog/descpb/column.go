// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package descpb

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

var _ cat.Column = &ColumnDescriptor{}

// IsNullable is part of the cat.Column interface.
func (desc *ColumnDescriptor) IsNullable() bool {
	return desc.Nullable
}

// HasNullDefault checks that the column descriptor has a default of NULL.
func (desc *ColumnDescriptor) HasNullDefault() bool {
	if !desc.HasDefault() {
		return false
	}
	defaultExpr, err := parser.ParseExpr(*desc.DefaultExpr)
	if err != nil {
		panic(errors.NewAssertionErrorWithWrappedErrf(err,
			"failed to parse default expression %s", *desc.DefaultExpr))
	}
	return defaultExpr == tree.DNull
}

// ColID is part of the cat.Column interface.
func (desc *ColumnDescriptor) ColID() cat.StableID {
	return cat.StableID(desc.ID)
}

// ColName is part of the cat.Column interface.
func (desc *ColumnDescriptor) ColName() tree.Name {
	return tree.Name(desc.Name)
}

// DatumType is part of the cat.Column interface.
func (desc *ColumnDescriptor) DatumType() *types.T {
	return desc.Type
}

// ColTypePrecision is part of the cat.Column interface.
func (desc *ColumnDescriptor) ColTypePrecision() int {
	if desc.Type.Family() == types.ArrayFamily {
		if desc.Type.ArrayContents().Family() == types.ArrayFamily {
			panic(errors.AssertionFailedf("column type should never be a nested array"))
		}
		return int(desc.Type.ArrayContents().Precision())
	}
	return int(desc.Type.Precision())
}

// ColTypeWidth is part of the cat.Column interface.
func (desc *ColumnDescriptor) ColTypeWidth() int {
	if desc.Type.Family() == types.ArrayFamily {
		if desc.Type.ArrayContents().Family() == types.ArrayFamily {
			panic(errors.AssertionFailedf("column type should never be a nested array"))
		}
		return int(desc.Type.ArrayContents().Width())
	}
	return int(desc.Type.Width())
}

// ColTypeStr is part of the cat.Column interface.
func (desc *ColumnDescriptor) ColTypeStr() string {
	return desc.Type.SQLString()
}

// IsHidden is part of the cat.Column interface.
func (desc *ColumnDescriptor) IsHidden() bool {
	return desc.Hidden
}

// HasDefault is part of the cat.Column interface.
func (desc *ColumnDescriptor) HasDefault() bool {
	return desc.DefaultExpr != nil
}

// IsComputed is part of the cat.Column interface.
func (desc *ColumnDescriptor) IsComputed() bool {
	return desc.ComputeExpr != nil
}

// DefaultExprStr is part of the cat.Column interface.
func (desc *ColumnDescriptor) DefaultExprStr() string {
	return *desc.DefaultExpr
}

// ComputedExprStr is part of the cat.Column interface.
func (desc *ColumnDescriptor) ComputedExprStr() string {
	return *desc.ComputeExpr
}

// CheckCanBeFKRef returns whether the given column is computed.
func (desc *ColumnDescriptor) CheckCanBeFKRef() error {
	if desc.IsComputed() {
		return unimplemented.NewWithIssuef(
			46672, "computed column %q cannot be a foreign key reference",
			desc.Name,
		)
	}
	return nil
}

// GetPGAttributeNum returns the PGAttributeNum of the ColumnDescriptor
// if the PGAttributeNum is set (non-zero). Returns the ID of the
// ColumnDescriptor if the PGAttributeNum is not set.
func (desc ColumnDescriptor) GetPGAttributeNum() uint32 {
	if desc.PGAttributeNum != 0 {
		return desc.PGAttributeNum
	}

	return uint32(desc.ID)
}

// SQLStringNotHumanReadable returns the SQL statement describing the column.
//
// Note that this function does not serialize user defined types into a human
// readable format. schemaexpr.FormatColumnForDisplay should be used in cases
// where human-readability is required.
func (desc *ColumnDescriptor) SQLStringNotHumanReadable() string {
	f := tree.NewFmtCtx(tree.FmtSimple)
	f.FormatNameP(&desc.Name)
	f.WriteByte(' ')
	f.WriteString(desc.Type.SQLString())
	if desc.Nullable {
		f.WriteString(" NULL")
	} else {
		f.WriteString(" NOT NULL")
	}
	if desc.DefaultExpr != nil {
		f.WriteString(" DEFAULT ")
		f.WriteString(*desc.DefaultExpr)
	}
	if desc.IsComputed() {
		f.WriteString(" AS (")
		f.WriteString(*desc.ComputeExpr)
		f.WriteString(") STORED")
	}
	return f.CloseAndGetString()
}
