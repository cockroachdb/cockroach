// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in licenses/BSD-vitess.txt.

// Portions of this file are additionally subject to the following
// license and copyright.
//
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

// This code was derived from https://github.com/youtube/vitess.

package tree

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"

	"golang.org/x/text/language"

	"github.com/pkg/errors"
)

// CreateDatabase represents a CREATE DATABASE statement.
type CreateDatabase struct {
	IfNotExists bool
	Name        Name
	Template    string
	Encoding    string
	Collate     string
	CType       string
}

// Format implements the NodeFormatter interface.
func (node *CreateDatabase) Format(ctx *FmtCtx) {
	ctx.WriteString("CREATE DATABASE ")
	if node.IfNotExists {
		ctx.WriteString("IF NOT EXISTS ")
	}
	ctx.FormatNode(&node.Name)
	if node.Template != "" {
		ctx.WriteString(" TEMPLATE = ")
		lex.EncodeSQLStringWithFlags(ctx.Buffer, node.Template, ctx.flags.EncodeFlags())
	}
	if node.Encoding != "" {
		ctx.WriteString(" ENCODING = ")
		lex.EncodeSQLStringWithFlags(ctx.Buffer, node.Encoding, ctx.flags.EncodeFlags())
	}
	if node.Collate != "" {
		ctx.WriteString(" LC_COLLATE = ")
		lex.EncodeSQLStringWithFlags(ctx.Buffer, node.Collate, ctx.flags.EncodeFlags())
	}
	if node.CType != "" {
		ctx.WriteString(" LC_CTYPE = ")
		lex.EncodeSQLStringWithFlags(ctx.Buffer, node.CType, ctx.flags.EncodeFlags())
	}
}

// IndexElem represents a column with a direction in a CREATE INDEX statement.
type IndexElem struct {
	Column    Name
	Direction Direction
}

// Format implements the NodeFormatter interface.
func (node *IndexElem) Format(ctx *FmtCtx) {
	ctx.FormatNode(&node.Column)
	if node.Direction != DefaultDirection {
		ctx.WriteByte(' ')
		ctx.WriteString(node.Direction.String())
	}
}

// IndexElemList is list of IndexElem.
type IndexElemList []IndexElem

// Format pretty-prints the contained names separated by commas.
// Format implements the NodeFormatter interface.
func (l *IndexElemList) Format(ctx *FmtCtx) {
	for i := range *l {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(&(*l)[i])
	}
}

// CreateIndex represents a CREATE INDEX statement.
type CreateIndex struct {
	Name        Name
	Table       NormalizableTableName
	Unique      bool
	Inverted    bool
	IfNotExists bool
	Columns     IndexElemList
	// Extra columns to be stored together with the indexed ones as an optimization
	// for improved reading performance.
	Storing     NameList
	Interleave  *InterleaveDef
	PartitionBy *PartitionBy
}

// Format implements the NodeFormatter interface.
func (node *CreateIndex) Format(ctx *FmtCtx) {
	ctx.WriteString("CREATE ")
	if node.Unique {
		ctx.WriteString("UNIQUE ")
	}
	if node.Inverted {
		ctx.WriteString("INVERTED ")
	}
	ctx.WriteString("INDEX ")
	if node.IfNotExists {
		ctx.WriteString("IF NOT EXISTS ")
	}
	if node.Name != "" {
		ctx.FormatNode(&node.Name)
		ctx.WriteByte(' ')
	}
	ctx.WriteString("ON ")
	ctx.FormatNode(&node.Table)
	ctx.WriteString(" (")
	ctx.FormatNode(&node.Columns)
	ctx.WriteByte(')')
	if len(node.Storing) > 0 {
		ctx.WriteString(" STORING (")
		ctx.FormatNode(&node.Storing)
		ctx.WriteByte(')')
	}
	if node.Interleave != nil {
		ctx.FormatNode(node.Interleave)
	}
	if node.PartitionBy != nil {
		ctx.FormatNode(node.PartitionBy)
	}
}

// TableDef represents a column, index or constraint definition within a CREATE
// TABLE statement.
type TableDef interface {
	NodeFormatter
	// Placeholder function to ensure that only desired types (*TableDef) conform
	// to the TableDef interface.
	tableDef()

	// SetName replaces the name of the definition in-place. Used in the parser.
	SetName(name Name)
}

func (*ColumnTableDef) tableDef() {}
func (*IndexTableDef) tableDef()  {}
func (*FamilyTableDef) tableDef() {}

// TableDefs represents a list of table definitions.
type TableDefs []TableDef

// Format implements the NodeFormatter interface.
func (node *TableDefs) Format(ctx *FmtCtx) {
	for i, n := range *node {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(n)
	}
}

// Nullability represents either NULL, NOT NULL or an unspecified value (silent
// NULL).
type Nullability int

// The values for NullType.
const (
	NotNull Nullability = iota
	Null
	SilentNull
)

// ColumnTableDef represents a column definition within a CREATE TABLE
// statement.
type ColumnTableDef struct {
	Name     Name
	Type     coltypes.T
	Nullable struct {
		Nullability    Nullability
		ConstraintName Name
	}
	PrimaryKey           bool
	Unique               bool
	UniqueConstraintName Name
	DefaultExpr          struct {
		Expr           Expr
		ConstraintName Name
	}
	CheckExprs []ColumnTableDefCheckExpr
	References struct {
		Table          NormalizableTableName
		Col            Name
		ConstraintName Name
		Actions        ReferenceActions
	}
	Computed struct {
		Computed bool
		Expr     Expr
	}
	Family struct {
		Name        Name
		Create      bool
		IfNotExists bool
	}
}

// ColumnTableDefCheckExpr represents a check constraint on a column definition
// within a CREATE TABLE statement.
type ColumnTableDefCheckExpr struct {
	Expr           Expr
	ConstraintName Name
}

func processCollationOnType(name Name, typ coltypes.T, c ColumnCollation) (coltypes.T, error) {
	locale := string(c)
	switch s := typ.(type) {
	case *coltypes.TString:
		return &coltypes.TCollatedString{Name: s.Name, N: s.N, Locale: locale}, nil
	case *coltypes.TCollatedString:
		return nil, pgerror.NewErrorf(pgerror.CodeSyntaxError,
			"multiple COLLATE declarations for column %q", name)
	case *coltypes.TArray:
		var err error
		s.ParamType, err = processCollationOnType(name, s.ParamType, c)
		if err != nil {
			return nil, err
		}
		return s, nil
	default:
		return nil, pgerror.NewErrorf(pgerror.CodeDatatypeMismatchError,
			"COLLATE declaration for non-string-typed column %q", name)
	}
}

// NewColumnTableDef constructs a column definition for a CreateTable statement.
func NewColumnTableDef(
	name Name, typ coltypes.T, qualifications []NamedColumnQualification,
) (*ColumnTableDef, error) {
	d := &ColumnTableDef{
		Name: name,
		Type: typ,
	}
	d.Nullable.Nullability = SilentNull
	for _, c := range qualifications {
		switch t := c.Qualification.(type) {
		case ColumnCollation:
			locale := string(t)
			_, err := language.Parse(locale)
			if err != nil {
				return nil, errors.Wrapf(err, "invalid locale %s", locale)
			}
			d.Type, err = processCollationOnType(name, d.Type, t)
			if err != nil {
				return nil, err
			}
		case *ColumnDefault:
			if d.HasDefaultExpr() {
				return nil, pgerror.NewErrorf(pgerror.CodeSyntaxError,
					"multiple default values specified for column %q", name)
			}
			d.DefaultExpr.Expr = t.Expr
			d.DefaultExpr.ConstraintName = c.Name
		case NotNullConstraint:
			if d.Nullable.Nullability == Null {
				return nil, pgerror.NewErrorf(pgerror.CodeSyntaxError,
					"conflicting NULL/NOT NULL declarations for column %q", name)
			}
			d.Nullable.Nullability = NotNull
			d.Nullable.ConstraintName = c.Name
		case NullConstraint:
			if d.Nullable.Nullability == NotNull {
				return nil, pgerror.NewErrorf(pgerror.CodeSyntaxError,
					"conflicting NULL/NOT NULL declarations for column %q", name)
			}
			d.Nullable.Nullability = Null
			d.Nullable.ConstraintName = c.Name
		case PrimaryKeyConstraint:
			d.PrimaryKey = true
			d.UniqueConstraintName = c.Name
		case UniqueConstraint:
			d.Unique = true
			d.UniqueConstraintName = c.Name
		case *ColumnCheckConstraint:
			d.CheckExprs = append(d.CheckExprs, ColumnTableDefCheckExpr{
				Expr:           t.Expr,
				ConstraintName: c.Name,
			})
		case *ColumnFKConstraint:
			if d.HasFKConstraint() {
				return nil, pgerror.NewErrorf(pgerror.CodeInvalidTableDefinitionError,
					"multiple foreign key constraints specified for column %q", name)
			}
			d.References.Table = t.Table
			d.References.Col = t.Col
			d.References.ConstraintName = c.Name
			d.References.Actions = t.Actions
		case *ColumnComputedDef:
			d.Computed.Computed = true
			d.Computed.Expr = t.Expr
		case *ColumnFamilyConstraint:
			if d.HasColumnFamily() {
				return nil, pgerror.NewErrorf(pgerror.CodeInvalidTableDefinitionError,
					"multiple column families specified for column %q", name)
			}
			d.Family.Name = t.Family
			d.Family.Create = t.Create
			d.Family.IfNotExists = t.IfNotExists
		default:
			panic(fmt.Sprintf("unexpected column qualification: %T", c))
		}
	}
	return d, nil
}

// SetName implements the TableDef interface.
func (node *ColumnTableDef) SetName(name Name) {
	node.Name = name
}

// HasDefaultExpr returns if the ColumnTableDef has a default expression.
func (node *ColumnTableDef) HasDefaultExpr() bool {
	return node.DefaultExpr.Expr != nil
}

// HasFKConstraint returns if the ColumnTableDef has a foreign key constraint.
func (node *ColumnTableDef) HasFKConstraint() bool {
	return node.References.Table.TableNameReference != nil
}

// IsComputed returns if the ColumnTableDef is a computed column.
func (node *ColumnTableDef) IsComputed() bool {
	return node.Computed.Computed
}

// HasColumnFamily returns if the ColumnTableDef has a column family.
func (node *ColumnTableDef) HasColumnFamily() bool {
	return node.Family.Name != "" || node.Family.Create
}

// Format implements the NodeFormatter interface.
func (node *ColumnTableDef) Format(ctx *FmtCtx) {
	ctx.FormatNode(&node.Name)
	ctx.WriteByte(' ')
	node.Type.Format(ctx.Buffer, ctx.flags.EncodeFlags())
	if node.Nullable.Nullability != SilentNull && node.Nullable.ConstraintName != "" {
		ctx.WriteString(" CONSTRAINT ")
		ctx.FormatNode(&node.Nullable.ConstraintName)
	}
	switch node.Nullable.Nullability {
	case Null:
		ctx.WriteString(" NULL")
	case NotNull:
		ctx.WriteString(" NOT NULL")
	}
	if node.PrimaryKey {
		ctx.WriteString(" PRIMARY KEY")
	} else if node.Unique {
		ctx.WriteString(" UNIQUE")
	}
	if node.HasDefaultExpr() {
		if node.DefaultExpr.ConstraintName != "" {
			ctx.WriteString(" CONSTRAINT ")
			ctx.FormatNode(&node.DefaultExpr.ConstraintName)
		}
		ctx.WriteString(" DEFAULT ")
		ctx.FormatNode(node.DefaultExpr.Expr)
	}
	for _, checkExpr := range node.CheckExprs {
		if checkExpr.ConstraintName != "" {
			ctx.WriteString(" CONSTRAINT ")
			ctx.FormatNode(&checkExpr.ConstraintName)
		}
		ctx.WriteString(" CHECK (")
		ctx.FormatNode(checkExpr.Expr)
		ctx.WriteByte(')')
	}
	if node.HasFKConstraint() {
		if node.References.ConstraintName != "" {
			ctx.WriteString(" CONSTRAINT ")
			ctx.FormatNode(&node.References.ConstraintName)
		}
		ctx.WriteString(" REFERENCES ")
		ctx.FormatNode(&node.References.Table)
		if node.References.Col != "" {
			ctx.WriteString(" (")
			ctx.FormatNode(&node.References.Col)
			ctx.WriteByte(')')
		}
		ctx.FormatNode(&node.References.Actions)
	}
	if node.IsComputed() {
		ctx.WriteString(" AS (")
		ctx.FormatNode(node.Computed.Expr)
		ctx.WriteString(") STORED")
	}
	if node.HasColumnFamily() {
		if node.Family.Create {
			ctx.WriteString(" CREATE")
		}
		if node.Family.IfNotExists {
			ctx.WriteString(" IF NOT EXISTS")
		}
		ctx.WriteString(" FAMILY")
		if len(node.Family.Name) > 0 {
			ctx.WriteByte(' ')
			ctx.FormatNode(&node.Family.Name)
		}
	}
}

// NamedColumnQualification wraps a NamedColumnQualification with a name.
type NamedColumnQualification struct {
	Name          Name
	Qualification ColumnQualification
}

// ColumnQualification represents a constraint on a column.
type ColumnQualification interface {
	columnQualification()
}

func (ColumnCollation) columnQualification()         {}
func (*ColumnDefault) columnQualification()          {}
func (NotNullConstraint) columnQualification()       {}
func (NullConstraint) columnQualification()          {}
func (PrimaryKeyConstraint) columnQualification()    {}
func (UniqueConstraint) columnQualification()        {}
func (*ColumnCheckConstraint) columnQualification()  {}
func (*ColumnComputedDef) columnQualification()      {}
func (*ColumnFKConstraint) columnQualification()     {}
func (*ColumnFamilyConstraint) columnQualification() {}

// ColumnCollation represents a COLLATE clause for a column.
type ColumnCollation string

// ColumnDefault represents a DEFAULT clause for a column.
type ColumnDefault struct {
	Expr Expr
}

// NotNullConstraint represents NOT NULL on a column.
type NotNullConstraint struct{}

// NullConstraint represents NULL on a column.
type NullConstraint struct{}

// PrimaryKeyConstraint represents NULL on a column.
type PrimaryKeyConstraint struct{}

// UniqueConstraint represents UNIQUE on a column.
type UniqueConstraint struct{}

// ColumnCheckConstraint represents either a check on a column.
type ColumnCheckConstraint struct {
	Expr Expr
}

// ColumnFKConstraint represents a FK-constaint on a column.
type ColumnFKConstraint struct {
	Table   NormalizableTableName
	Col     Name // empty-string means use PK
	Actions ReferenceActions
}

// ColumnComputedDef represents the description of a computed column.
type ColumnComputedDef struct {
	Expr Expr
}

// ColumnFamilyConstraint represents FAMILY on a column.
type ColumnFamilyConstraint struct {
	Family      Name
	Create      bool
	IfNotExists bool
}

// IndexTableDef represents an index definition within a CREATE TABLE
// statement.
type IndexTableDef struct {
	Name        Name
	Columns     IndexElemList
	Storing     NameList
	Interleave  *InterleaveDef
	Inverted    bool
	PartitionBy *PartitionBy
}

// SetName implements the TableDef interface.
func (node *IndexTableDef) SetName(name Name) {
	node.Name = name
}

// Format implements the NodeFormatter interface.
func (node *IndexTableDef) Format(ctx *FmtCtx) {
	ctx.WriteString("INDEX ")
	if node.Name != "" {
		ctx.FormatNode(&node.Name)
		ctx.WriteByte(' ')
	}
	ctx.WriteByte('(')
	ctx.FormatNode(&node.Columns)
	ctx.WriteByte(')')
	if node.Storing != nil {
		ctx.WriteString(" STORING (")
		ctx.FormatNode(&node.Storing)
		ctx.WriteByte(')')
	}
	if node.Interleave != nil {
		ctx.FormatNode(node.Interleave)
	}
	if node.Inverted {
		ctx.WriteString("INVERTED ")
	}
	if node.PartitionBy != nil {
		ctx.FormatNode(node.PartitionBy)
	}
}

// ConstraintTableDef represents a constraint definition within a CREATE TABLE
// statement.
type ConstraintTableDef interface {
	TableDef
	// Placeholder function to ensure that only desired types
	// (*ConstraintTableDef) conform to the ConstraintTableDef interface.
	constraintTableDef()
}

func (*UniqueConstraintTableDef) constraintTableDef() {}

// UniqueConstraintTableDef represents a unique constraint within a CREATE
// TABLE statement.
type UniqueConstraintTableDef struct {
	IndexTableDef
	PrimaryKey bool
}

// Format implements the NodeFormatter interface.
func (node *UniqueConstraintTableDef) Format(ctx *FmtCtx) {
	if node.Name != "" {
		ctx.WriteString("CONSTRAINT ")
		ctx.FormatNode(&node.Name)
		ctx.WriteByte(' ')
	}
	if node.PrimaryKey {
		ctx.WriteString("PRIMARY KEY ")
	} else {
		ctx.WriteString("UNIQUE ")
	}
	ctx.WriteByte('(')
	ctx.FormatNode(&node.Columns)
	ctx.WriteByte(')')
	if node.Storing != nil {
		ctx.WriteString(" STORING (")
		ctx.FormatNode(&node.Storing)
		ctx.WriteByte(')')
	}
	if node.Interleave != nil {
		ctx.FormatNode(node.Interleave)
	}
	if node.PartitionBy != nil {
		ctx.FormatNode(node.PartitionBy)
	}
}

// ReferenceAction is the method used to maintain referential integrity through
// foreign keys.
type ReferenceAction int

// The values for ReferenceAction.
const (
	NoAction ReferenceAction = iota
	Restrict
	SetNull
	SetDefault
	Cascade
)

var referenceActionName = [...]string{
	NoAction:   "NO ACTION",
	Restrict:   "RESTRICT",
	SetNull:    "SET NULL",
	SetDefault: "SET DEFAULT",
	Cascade:    "CASCADE",
}

func (ra ReferenceAction) String() string {
	return referenceActionName[ra]
}

// ReferenceActions contains the actions specified to maintain referential
// integrity through foreign keys for different operations.
type ReferenceActions struct {
	Delete ReferenceAction
	Update ReferenceAction
}

// Format implements the NodeFormatter interface.
func (node *ReferenceActions) Format(ctx *FmtCtx) {
	if node.Delete != NoAction {
		ctx.WriteString(" ON DELETE ")
		ctx.WriteString(node.Delete.String())
	}
	if node.Update != NoAction {
		ctx.WriteString(" ON UPDATE ")
		ctx.WriteString(node.Update.String())
	}
}

// ForeignKeyConstraintTableDef represents a FOREIGN KEY constraint in the AST.
type ForeignKeyConstraintTableDef struct {
	Name     Name
	Table    NormalizableTableName
	FromCols NameList
	ToCols   NameList
	Actions  ReferenceActions
}

// Format implements the NodeFormatter interface.
func (node *ForeignKeyConstraintTableDef) Format(ctx *FmtCtx) {
	if node.Name != "" {
		ctx.WriteString("CONSTRAINT ")
		ctx.FormatNode(&node.Name)
		ctx.WriteByte(' ')
	}
	ctx.WriteString("FOREIGN KEY (")
	ctx.FormatNode(&node.FromCols)
	ctx.WriteString(") REFERENCES ")
	ctx.FormatNode(&node.Table)

	if len(node.ToCols) > 0 {
		ctx.WriteByte(' ')
		ctx.WriteByte('(')
		ctx.FormatNode(&node.ToCols)
		ctx.WriteByte(')')
	}

	ctx.FormatNode(&node.Actions)
}

// SetName implements the TableDef interface.
func (node *ForeignKeyConstraintTableDef) SetName(name Name) {
	node.Name = name
}

func (*ForeignKeyConstraintTableDef) tableDef()           {}
func (*ForeignKeyConstraintTableDef) constraintTableDef() {}

func (*CheckConstraintTableDef) tableDef()           {}
func (*CheckConstraintTableDef) constraintTableDef() {}

// CheckConstraintTableDef represents a check constraint within a CREATE
// TABLE statement.
type CheckConstraintTableDef struct {
	Name Name
	Expr Expr
}

// SetName implements the TableDef interface.
func (node *CheckConstraintTableDef) SetName(name Name) {
	node.Name = name
}

// Format implements the NodeFormatter interface.
func (node *CheckConstraintTableDef) Format(ctx *FmtCtx) {
	if node.Name != "" {
		ctx.WriteString("CONSTRAINT ")
		ctx.FormatNode(&node.Name)
		ctx.WriteByte(' ')
	}
	ctx.WriteString("CHECK (")
	ctx.FormatNode(node.Expr)
	ctx.WriteByte(')')
}

// FamilyTableDef represents a family definition within a CREATE TABLE
// statement.
type FamilyTableDef struct {
	Name    Name
	Columns NameList
}

// SetName implements the TableDef interface.
func (node *FamilyTableDef) SetName(name Name) {
	node.Name = name
}

// Format implements the NodeFormatter interface.
func (node *FamilyTableDef) Format(ctx *FmtCtx) {
	ctx.WriteString("FAMILY ")
	if node.Name != "" {
		ctx.FormatNode(&node.Name)
		ctx.WriteByte(' ')
	}
	ctx.WriteByte('(')
	ctx.FormatNode(&node.Columns)
	ctx.WriteByte(')')
}

// InterleaveDef represents an interleave definition within a CREATE TABLE
// or CREATE INDEX statement.
type InterleaveDef struct {
	Parent       *NormalizableTableName
	Fields       NameList
	DropBehavior DropBehavior
}

// Format implements the NodeFormatter interface.
func (node *InterleaveDef) Format(ctx *FmtCtx) {
	ctx.WriteString(" INTERLEAVE IN PARENT ")
	ctx.FormatNode(node.Parent)
	ctx.WriteString(" (")
	for i := range node.Fields {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(&node.Fields[i])
	}
	ctx.WriteString(")")
	if node.DropBehavior != DropDefault {
		ctx.WriteString(" ")
		ctx.WriteString(node.DropBehavior.String())
	}
}

// PartitionByType is an enum of each type of partitioning (LIST/RANGE).
type PartitionByType string

const (
	// PartitionByList indicates a PARTITION BY LIST clause.
	PartitionByList PartitionByType = "LIST"
	// PartitionByRange indicates a PARTITION BY LIST clause.
	PartitionByRange PartitionByType = "RANGE"
)

// PartitionBy represents an PARTITION BY definition within a CREATE/ALTER
// TABLE/INDEX statement.
type PartitionBy struct {
	Fields NameList
	// Exactly one of List or Range is required to be non-empty.
	List  []ListPartition
	Range []RangePartition
}

// Format implements the NodeFormatter interface.
func (node *PartitionBy) Format(ctx *FmtCtx) {
	if node == nil {
		ctx.WriteString(` PARTITION BY NOTHING`)
		return
	}
	if len(node.List) > 0 {
		ctx.WriteString(` PARTITION BY LIST (`)
	} else if len(node.Range) > 0 {
		ctx.WriteString(` PARTITION BY RANGE (`)
	}
	ctx.FormatNode(&node.Fields)
	ctx.WriteString(`) (`)
	for i := range node.List {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(&node.List[i])
	}
	for i := range node.Range {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(&node.Range[i])
	}
	ctx.WriteString(`)`)
}

// ListPartition represents a PARTITION definition within a PARTITION BY LIST.
type ListPartition struct {
	Name         UnrestrictedName
	Exprs        Exprs
	Subpartition *PartitionBy
}

// Format implements the NodeFormatter interface.
func (node *ListPartition) Format(ctx *FmtCtx) {
	ctx.WriteString(`PARTITION `)
	ctx.FormatNode(&node.Name)
	ctx.WriteString(` VALUES IN (`)
	ctx.FormatNode(&node.Exprs)
	ctx.WriteByte(')')
	if node.Subpartition != nil {
		ctx.FormatNode(node.Subpartition)
	}
}

// RangePartition represents a PARTITION definition within a PARTITION BY RANGE.
type RangePartition struct {
	Name         UnrestrictedName
	From         Expr
	To           Expr
	Subpartition *PartitionBy
}

// Format implements the NodeFormatter interface.
func (node *RangePartition) Format(ctx *FmtCtx) {
	ctx.WriteString(`PARTITION `)
	ctx.FormatNode(&node.Name)
	ctx.WriteString(` VALUES FROM `)
	ctx.FormatNode(node.From)
	ctx.WriteString(` TO `)
	ctx.FormatNode(node.To)
	if node.Subpartition != nil {
		ctx.FormatNode(node.Subpartition)
	}
}

// CreateTable represents a CREATE TABLE statement.
type CreateTable struct {
	IfNotExists   bool
	Table         NormalizableTableName
	Interleave    *InterleaveDef
	PartitionBy   *PartitionBy
	Defs          TableDefs
	AsSource      *Select
	AsColumnNames NameList // Only to be used in conjunction with AsSource
}

// As returns true if this table represents a CREATE TABLE ... AS statement,
// false otherwise.
func (node *CreateTable) As() bool {
	return node.AsSource != nil
}

// Format implements the NodeFormatter interface.
func (node *CreateTable) Format(ctx *FmtCtx) {
	ctx.WriteString("CREATE TABLE ")
	if node.IfNotExists {
		ctx.WriteString("IF NOT EXISTS ")
	}
	ctx.FormatNode(&node.Table)
	if node.As() {
		if len(node.AsColumnNames) > 0 {
			ctx.WriteString(" (")
			ctx.FormatNode(&node.AsColumnNames)
			ctx.WriteByte(')')
		}
		ctx.WriteString(" AS ")
		ctx.FormatNode(node.AsSource)
	} else {
		ctx.WriteString(" (")
		ctx.FormatNode(&node.Defs)
		ctx.WriteByte(')')
		if node.Interleave != nil {
			ctx.FormatNode(node.Interleave)
		}
		if node.PartitionBy != nil {
			ctx.FormatNode(node.PartitionBy)
		}
	}
}

// CreateSequence represents a CREATE SEQUENCE statement.
type CreateSequence struct {
	IfNotExists bool
	Name        NormalizableTableName
	Options     SequenceOptions
}

// Format implements the NodeFormatter interface.
func (node *CreateSequence) Format(ctx *FmtCtx) {
	ctx.WriteString("CREATE SEQUENCE ")
	if node.IfNotExists {
		ctx.WriteString("IF NOT EXISTS ")
	}
	ctx.FormatNode(&node.Name)
	ctx.FormatNode(&node.Options)
}

// SequenceOptions represents a list of sequence options.
type SequenceOptions []SequenceOption

// Format implements the NodeFormatter interface.
func (node *SequenceOptions) Format(ctx *FmtCtx) {
	for i := range *node {
		option := &(*node)[i]
		ctx.WriteByte(' ')
		switch option.Name {
		case SeqOptCycle, SeqOptNoCycle:
			ctx.WriteString(option.Name)
		case SeqOptCache:
			ctx.WriteString(option.Name)
			ctx.WriteByte(' ')
			ctx.Printf("%d", *option.IntVal)
		case SeqOptMaxValue, SeqOptMinValue:
			if option.IntVal == nil {
				ctx.WriteString("NO ")
				ctx.WriteString(option.Name)
			} else {
				ctx.WriteString(option.Name)
				ctx.WriteByte(' ')
				ctx.Printf("%d", *option.IntVal)
			}
		case SeqOptStart:
			ctx.WriteString(option.Name)
			ctx.WriteByte(' ')
			if option.OptionalWord {
				ctx.WriteString("WITH ")
			}
			ctx.Printf("%d", *option.IntVal)
		case SeqOptIncrement:
			ctx.WriteString(option.Name)
			ctx.WriteByte(' ')
			if option.OptionalWord {
				ctx.WriteString("BY ")
			}
			ctx.Printf("%d", *option.IntVal)
		default:
			panic(fmt.Sprintf("unexpected SequenceOption: %v", option))
		}
	}
}

// SequenceOption represents an option on a CREATE SEQUENCE statement.
type SequenceOption struct {
	Name string

	IntVal *int64

	OptionalWord bool
}

// Names of options on CREATE SEQUENCE.
const (
	SeqOptAs        = "AS"
	SeqOptCycle     = "CYCLE"
	SeqOptNoCycle   = "NO CYCLE"
	SeqOptOwnedBy   = "OWNED BY"
	SeqOptCache     = "CACHE"
	SeqOptIncrement = "INCREMENT"
	SeqOptMinValue  = "MINVALUE"
	SeqOptMaxValue  = "MAXVALUE"
	SeqOptStart     = "START"

	// Avoid unused warning for constants.
	_ = SeqOptAs
	_ = SeqOptOwnedBy
)

// CreateUser represents a CREATE USER statement.
type CreateUser struct {
	Name        Expr
	Password    Expr // nil if no password specified
	IfNotExists bool
}

// HasPassword returns if the CreateUser has a password.
func (node *CreateUser) HasPassword() bool {
	return node.Password != nil
}

// Format implements the NodeFormatter interface.
func (node *CreateUser) Format(ctx *FmtCtx) {
	ctx.WriteString("CREATE USER ")
	if node.IfNotExists {
		ctx.WriteString("IF NOT EXISTS ")
	}
	ctx.FormatNode(node.Name)
	if node.HasPassword() {
		ctx.WriteString(" WITH PASSWORD ")
		if ctx.flags.HasFlags(FmtShowPasswords) {
			ctx.FormatNode(node.Password)
		} else {
			ctx.WriteString("*****")
		}
	}
}

// AlterUserSetPassword represents an ALTER USER ... WITH PASSWORD statement.
type AlterUserSetPassword struct {
	Name     Expr
	Password Expr
	IfExists bool
}

// Format implements the NodeFormatter interface.
func (node *AlterUserSetPassword) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER USER ")
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(node.Name)
	ctx.WriteString(" WITH PASSWORD ")
	if ctx.flags.HasFlags(FmtShowPasswords) {
		ctx.FormatNode(node.Password)
	} else {
		ctx.WriteString("*****")
	}
}

// CreateRole represents a CREATE ROLE statement.
type CreateRole struct {
	Name        Expr
	IfNotExists bool
}

// Format implements the NodeFormatter interface.
func (node *CreateRole) Format(ctx *FmtCtx) {
	ctx.WriteString("CREATE ROLE ")
	if node.IfNotExists {
		ctx.WriteString("IF NOT EXISTS ")
	}
	ctx.FormatNode(node.Name)
}

// CreateView represents a CREATE VIEW statement.
type CreateView struct {
	Name        NormalizableTableName
	ColumnNames NameList
	AsSource    *Select
}

// Format implements the NodeFormatter interface.
func (node *CreateView) Format(ctx *FmtCtx) {
	ctx.WriteString("CREATE VIEW ")
	ctx.FormatNode(&node.Name)

	if len(node.ColumnNames) > 0 {
		ctx.WriteByte(' ')
		ctx.WriteByte('(')
		ctx.FormatNode(&node.ColumnNames)
		ctx.WriteByte(')')
	}

	ctx.WriteString(" AS ")
	ctx.FormatNode(node.AsSource)
}

// CreateStats represents a CREATE STATISTICS statement.
type CreateStats struct {
	Name        Name
	ColumnNames NameList
	Table       NormalizableTableName
}

// Format implements the NodeFormatter interface.
func (node *CreateStats) Format(ctx *FmtCtx) {
	ctx.WriteString("CREATE STATISTICS ")
	ctx.FormatNode(&node.Name)

	ctx.WriteString(" ON ")
	ctx.FormatNode(&node.ColumnNames)

	ctx.WriteString(" FROM ")
	ctx.FormatNode(&node.Table)
}
