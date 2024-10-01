// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package descpb

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
)

// HasDefault returns true if the column has a default value.
func (desc *ColumnDescriptor) HasDefault() bool {
	return desc.DefaultExpr != nil
}

// HasOnUpdate returns true if the column has an on update expression.
func (desc *ColumnDescriptor) HasOnUpdate() bool {
	return desc.OnUpdateExpr != nil
}

// IsComputed returns true if this is a computed column.
func (desc *ColumnDescriptor) IsComputed() bool {
	return desc.ComputeExpr != nil
}

// ColName returns the name of the column as a tree.Name.
func (desc *ColumnDescriptor) ColName() tree.Name {
	return tree.Name(desc.Name)
}

// CheckCanBeOutboundFKRef returns whether the given column can be on the
// referencing (origin) side of a foreign key relation.
func (desc *ColumnDescriptor) CheckCanBeOutboundFKRef() error {
	if desc.Inaccessible {
		return pgerror.Newf(
			pgcode.UndefinedColumn,
			"column %q is inaccessible and cannot reference a foreign key",
			desc.Name,
		)
	}
	if desc.Virtual {
		return unimplemented.NewWithIssuef(
			59671, "virtual column %q cannot reference a foreign key",
			desc.Name,
		)
	}
	return nil
}

// CheckCanBeInboundFKRef returns whether the given column can be on the
// referenced (target) side of a foreign key relation.
func (desc *ColumnDescriptor) CheckCanBeInboundFKRef() error {
	if desc.Inaccessible {
		return pgerror.Newf(
			pgcode.UndefinedColumn,
			"column %q is inaccessible and cannot be referenced by a foreign key",
			desc.Name,
		)
	}
	if desc.Virtual {
		return unimplemented.NewWithIssuef(
			59671, "virtual column %q cannot be referenced by a foreign key",
			desc.Name,
		)
	}
	return nil
}

// GetPGAttributeNum returns the PGAttributeNum of the ColumnDescriptor
// if the PGAttributeNum is set (non-zero). Returns the ID of the
// ColumnDescriptor if the PGAttributeNum is not set.
func (desc ColumnDescriptor) GetPGAttributeNum() PGAttributeNum {
	if desc.PGAttributeNum != 0 {
		return desc.PGAttributeNum
	}

	return PGAttributeNum(desc.ID)
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
