// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in licenses/BSD-vitess.txt.

// Portions of this file are additionally subject to the following
// license and copyright.
//
// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// This code was derived from https://github.com/youtube/vitess.

package tree

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
	"golang.org/x/text/language"
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
		lex.EncodeSQLStringWithFlags(&ctx.Buffer, node.Template, ctx.flags.EncodeFlags())
	}
	if node.Encoding != "" {
		ctx.WriteString(" ENCODING = ")
		lex.EncodeSQLStringWithFlags(&ctx.Buffer, node.Encoding, ctx.flags.EncodeFlags())
	}
	if node.Collate != "" {
		ctx.WriteString(" LC_COLLATE = ")
		lex.EncodeSQLStringWithFlags(&ctx.Buffer, node.Collate, ctx.flags.EncodeFlags())
	}
	if node.CType != "" {
		ctx.WriteString(" LC_CTYPE = ")
		lex.EncodeSQLStringWithFlags(&ctx.Buffer, node.CType, ctx.flags.EncodeFlags())
	}
}

// IndexElem represents a column with a direction in a CREATE INDEX statement.
type IndexElem struct {
	Column     Name
	Direction  Direction
	NullsOrder NullsOrder
}

// Format implements the NodeFormatter interface.
func (node *IndexElem) Format(ctx *FmtCtx) {
	ctx.FormatNode(&node.Column)
	if node.Direction != DefaultDirection {
		ctx.WriteByte(' ')
		ctx.WriteString(node.Direction.String())
	}
	if node.NullsOrder != DefaultNullsOrder {
		ctx.WriteByte(' ')
		ctx.WriteString(node.NullsOrder.String())
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
	Table       TableName
	Unique      bool
	Inverted    bool
	IfNotExists bool
	Columns     IndexElemList
	Sharded     *ShardedIndexDef
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
	if node.Inverted && !ctx.HasFlags(FmtPGIndexDef) {
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
	if ctx.HasFlags(FmtPGIndexDef) {
		ctx.WriteString(" USING")
		if node.Inverted {
			ctx.WriteString(" gin")
		} else {
			ctx.WriteString(" btree")
		}
	}
	ctx.WriteString(" (")
	ctx.FormatNode(&node.Columns)
	ctx.WriteByte(')')
	if node.Sharded != nil {
		ctx.FormatNode(node.Sharded)
	}
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

func (*ColumnTableDef) tableDef()               {}
func (*IndexTableDef) tableDef()                {}
func (*FamilyTableDef) tableDef()               {}
func (*ForeignKeyConstraintTableDef) tableDef() {}
func (*CheckConstraintTableDef) tableDef()      {}

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
	Type     *types.T
	IsSerial bool
	Nullable struct {
		Nullability    Nullability
		ConstraintName Name
	}
	PrimaryKey struct {
		IsPrimaryKey bool
		Sharded      bool
		ShardBuckets Expr
	}
	Unique               bool
	UniqueConstraintName Name
	DefaultExpr          struct {
		Expr           Expr
		ConstraintName Name
	}
	CheckExprs []ColumnTableDefCheckExpr
	References struct {
		Table          *TableName
		Col            Name
		ConstraintName Name
		Actions        ReferenceActions
		Match          CompositeKeyMatchMethod
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

func processCollationOnType(name Name, typ *types.T, c ColumnCollation) (*types.T, error) {
	switch typ.Family() {
	case types.StringFamily:
		return types.MakeCollatedString(typ, string(c)), nil
	case types.CollatedStringFamily:
		return nil, pgerror.Newf(pgcode.Syntax,
			"multiple COLLATE declarations for column %q", name)
	case types.ArrayFamily:
		elemTyp, err := processCollationOnType(name, typ.ArrayContents(), c)
		if err != nil {
			return nil, err
		}
		return types.MakeArray(elemTyp), nil
	default:
		return nil, pgerror.Newf(pgcode.DatatypeMismatch,
			"COLLATE declaration for non-string-typed column %q", name)
	}
}

// NewColumnTableDef constructs a column definition for a CreateTable statement.
func NewColumnTableDef(
	name Name, typ *types.T, isSerial bool, qualifications []NamedColumnQualification,
) (*ColumnTableDef, error) {
	d := &ColumnTableDef{
		Name:     name,
		Type:     typ,
		IsSerial: isSerial,
	}
	d.Nullable.Nullability = SilentNull
	for _, c := range qualifications {
		switch t := c.Qualification.(type) {
		case ColumnCollation:
			locale := string(t)
			_, err := language.Parse(locale)
			if err != nil {
				return nil, pgerror.Wrapf(err, pgcode.Syntax, "invalid locale %s", locale)
			}
			d.Type, err = processCollationOnType(name, d.Type, t)
			if err != nil {
				return nil, err
			}
		case *ColumnDefault:
			if d.HasDefaultExpr() {
				return nil, pgerror.Newf(pgcode.Syntax,
					"multiple default values specified for column %q", name)
			}
			d.DefaultExpr.Expr = t.Expr
			d.DefaultExpr.ConstraintName = c.Name
		case NotNullConstraint:
			if d.Nullable.Nullability == Null {
				return nil, pgerror.Newf(pgcode.Syntax,
					"conflicting NULL/NOT NULL declarations for column %q", name)
			}
			d.Nullable.Nullability = NotNull
			d.Nullable.ConstraintName = c.Name
		case NullConstraint:
			if d.Nullable.Nullability == NotNull {
				return nil, pgerror.Newf(pgcode.Syntax,
					"conflicting NULL/NOT NULL declarations for column %q", name)
			}
			d.Nullable.Nullability = Null
			d.Nullable.ConstraintName = c.Name
		case PrimaryKeyConstraint:
			d.PrimaryKey.IsPrimaryKey = true
			d.UniqueConstraintName = c.Name
		case ShardedPrimaryKeyConstraint:
			d.PrimaryKey.IsPrimaryKey = true
			constraint := c.Qualification.(ShardedPrimaryKeyConstraint)
			d.PrimaryKey.Sharded = true
			d.PrimaryKey.ShardBuckets = constraint.ShardBuckets
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
				return nil, pgerror.Newf(pgcode.InvalidTableDefinition,
					"multiple foreign key constraints specified for column %q", name)
			}
			d.References.Table = &t.Table
			d.References.Col = t.Col
			d.References.ConstraintName = c.Name
			d.References.Actions = t.Actions
			d.References.Match = t.Match
		case *ColumnComputedDef:
			d.Computed.Computed = true
			d.Computed.Expr = t.Expr
		case *ColumnFamilyConstraint:
			if d.HasColumnFamily() {
				return nil, pgerror.Newf(pgcode.InvalidTableDefinition,
					"multiple column families specified for column %q", name)
			}
			d.Family.Name = t.Family
			d.Family.Create = t.Create
			d.Family.IfNotExists = t.IfNotExists
		default:
			return nil, errors.AssertionFailedf("unexpected column qualification: %T", c)
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
	return node.References.Table != nil
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

	// ColumnTableDef node type will not be specified if it represents a CREATE
	// TABLE ... AS query.
	if node.Type != nil {
		ctx.WriteByte(' ')
		ctx.WriteString(node.columnTypeString())
	}

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
	if node.PrimaryKey.IsPrimaryKey || node.Unique {
		if node.UniqueConstraintName != "" {
			ctx.WriteString(" CONSTRAINT ")
			ctx.FormatNode(&node.UniqueConstraintName)
		}
		if node.PrimaryKey.IsPrimaryKey {
			ctx.WriteString(" PRIMARY KEY")
			if node.PrimaryKey.Sharded {
				ctx.WriteString(" USING HASH WITH BUCKET_COUNT=")
				ctx.FormatNode(node.PrimaryKey.ShardBuckets)
			}
		} else if node.Unique {
			ctx.WriteString(" UNIQUE")
		}
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
		ctx.FormatNode(node.References.Table)
		if node.References.Col != "" {
			ctx.WriteString(" (")
			ctx.FormatNode(&node.References.Col)
			ctx.WriteByte(')')
		}
		if node.References.Match != MatchSimple {
			ctx.WriteByte(' ')
			ctx.WriteString(node.References.Match.String())
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
			if node.Family.IfNotExists {
				ctx.WriteString(" IF NOT EXISTS")
			}
		}
		ctx.WriteString(" FAMILY")
		if len(node.Family.Name) > 0 {
			ctx.WriteByte(' ')
			ctx.FormatNode(&node.Family.Name)
		}
	}
}

func (node *ColumnTableDef) columnTypeString() string {
	if node.IsSerial {
		// Map INT types to SERIAL keyword.
		switch node.Type.Width() {
		case 16:
			return "SERIAL2"
		case 32:
			return "SERIAL4"
		}
		return "SERIAL8"
	}
	return node.Type.SQLString()
}

// String implements the fmt.Stringer interface.
func (node *ColumnTableDef) String() string { return AsString(node) }

// NamedColumnQualification wraps a NamedColumnQualification with a name.
type NamedColumnQualification struct {
	Name          Name
	Qualification ColumnQualification
}

// ColumnQualification represents a constraint on a column.
type ColumnQualification interface {
	columnQualification()
}

func (ColumnCollation) columnQualification()             {}
func (*ColumnDefault) columnQualification()              {}
func (NotNullConstraint) columnQualification()           {}
func (NullConstraint) columnQualification()              {}
func (PrimaryKeyConstraint) columnQualification()        {}
func (ShardedPrimaryKeyConstraint) columnQualification() {}
func (UniqueConstraint) columnQualification()            {}
func (*ColumnCheckConstraint) columnQualification()      {}
func (*ColumnComputedDef) columnQualification()          {}
func (*ColumnFKConstraint) columnQualification()         {}
func (*ColumnFamilyConstraint) columnQualification()     {}

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

// PrimaryKeyConstraint represents PRIMARY KEY on a column.
type PrimaryKeyConstraint struct{}

// ShardedPrimaryKeyConstraint represents `PRIMARY KEY .. USING HASH..`
// on a column.
type ShardedPrimaryKeyConstraint struct {
	Sharded      bool
	ShardBuckets Expr
}

// UniqueConstraint represents UNIQUE on a column.
type UniqueConstraint struct{}

// ColumnCheckConstraint represents either a check on a column.
type ColumnCheckConstraint struct {
	Expr Expr
}

// ColumnFKConstraint represents a FK-constaint on a column.
type ColumnFKConstraint struct {
	Table   TableName
	Col     Name // empty-string means use PK
	Actions ReferenceActions
	Match   CompositeKeyMatchMethod
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
	Sharded     *ShardedIndexDef
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
	if node.Inverted {
		ctx.WriteString("INVERTED ")
	}
	ctx.WriteString("INDEX ")
	if node.Name != "" {
		ctx.FormatNode(&node.Name)
		ctx.WriteByte(' ')
	}
	ctx.WriteByte('(')
	ctx.FormatNode(&node.Columns)
	ctx.WriteByte(')')
	if node.Sharded != nil {
		ctx.FormatNode(node.Sharded)
	}
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

// ConstraintTableDef represents a constraint definition within a CREATE TABLE
// statement.
type ConstraintTableDef interface {
	TableDef
	// Placeholder function to ensure that only desired types
	// (*ConstraintTableDef) conform to the ConstraintTableDef interface.
	constraintTableDef()
}

func (*UniqueConstraintTableDef) constraintTableDef()     {}
func (*ForeignKeyConstraintTableDef) constraintTableDef() {}
func (*CheckConstraintTableDef) constraintTableDef()      {}

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

// CompositeKeyMatchMethod is the algorithm use when matching composite keys.
// See https://github.com/cockroachdb/cockroach/issues/20305 or
// https://www.postgresql.org/docs/11/sql-createtable.html for details on the
// different composite foreign key matching methods.
type CompositeKeyMatchMethod int

// The values for CompositeKeyMatchMethod.
const (
	MatchSimple CompositeKeyMatchMethod = iota
	MatchFull
	MatchPartial // Note: PARTIAL not actually supported at this point.
)

var compositeKeyMatchMethodName = [...]string{
	MatchSimple:  "MATCH SIMPLE",
	MatchFull:    "MATCH FULL",
	MatchPartial: "MATCH PARTIAL",
}

func (c CompositeKeyMatchMethod) String() string {
	return compositeKeyMatchMethodName[c]
}

// ForeignKeyConstraintTableDef represents a FOREIGN KEY constraint in the AST.
type ForeignKeyConstraintTableDef struct {
	Name     Name
	Table    TableName
	FromCols NameList
	ToCols   NameList
	Actions  ReferenceActions
	Match    CompositeKeyMatchMethod
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

	if node.Match != MatchSimple {
		ctx.WriteByte(' ')
		ctx.WriteString(node.Match.String())
	}

	ctx.FormatNode(&node.Actions)
}

// SetName implements the TableDef interface.
func (node *ForeignKeyConstraintTableDef) SetName(name Name) {
	node.Name = name
}

// CheckConstraintTableDef represents a check constraint within a CREATE
// TABLE statement.
type CheckConstraintTableDef struct {
	Name   Name
	Expr   Expr
	Hidden bool
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

// ShardedIndexDef represents a hash sharded secondary index definition within a CREATE
// TABLE or CREATE INDEX statement.
type ShardedIndexDef struct {
	ShardBuckets Expr
}

// EvalShardBucketCount evaluates and checks the integer argument to a `USING HASH WITH
// BUCKET_COUNT` index creation query.
func EvalShardBucketCount(shardBuckets Expr) (int32, error) {
	const invalidBucketCountMsg = `BUCKET_COUNT must be a strictly positive integer value`
	cst, ok := shardBuckets.(*NumVal)
	if !ok {
		return 0, pgerror.New(pgcode.InvalidParameterValue, invalidBucketCountMsg)
	}
	buckets, err := cst.AsInt32()
	if err != nil || buckets <= 0 {
		if err != nil {
			return 0, pgerror.Wrap(err, pgcode.InvalidParameterValue, invalidBucketCountMsg)
		}
		return 0, pgerror.New(pgcode.InvalidParameterValue, invalidBucketCountMsg)
	}
	return buckets, nil
}

// Format implements the NodeFormatter interface.
func (node *ShardedIndexDef) Format(ctx *FmtCtx) {
	ctx.WriteString(" USING HASH WITH BUCKET_COUNT=")
	ctx.FormatNode(node.ShardBuckets)
}

// InterleaveDef represents an interleave definition within a CREATE TABLE
// or CREATE INDEX statement.
type InterleaveDef struct {
	Parent       TableName
	Fields       NameList
	DropBehavior DropBehavior
}

// Format implements the NodeFormatter interface.
func (node *InterleaveDef) Format(ctx *FmtCtx) {
	ctx.WriteString(" INTERLEAVE IN PARENT ")
	ctx.FormatNode(&node.Parent)
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
	From         Exprs
	To           Exprs
	Subpartition *PartitionBy
}

// Format implements the NodeFormatter interface.
func (node *RangePartition) Format(ctx *FmtCtx) {
	ctx.WriteString(`PARTITION `)
	ctx.FormatNode(&node.Name)
	ctx.WriteString(` VALUES FROM (`)
	ctx.FormatNode(&node.From)
	ctx.WriteString(`) TO (`)
	ctx.FormatNode(&node.To)
	ctx.WriteByte(')')
	if node.Subpartition != nil {
		ctx.FormatNode(node.Subpartition)
	}
}

// CreateTable represents a CREATE TABLE statement.
type CreateTable struct {
	IfNotExists bool
	Table       TableName
	Interleave  *InterleaveDef
	PartitionBy *PartitionBy
	Temporary   bool
	// In CREATE...AS queries, Defs represents a list of ColumnTableDefs, one for
	// each column, and a ConstraintTableDef for each constraint on a subset of
	// these columns.
	Defs     TableDefs
	AsSource *Select
}

// As returns true if this table represents a CREATE TABLE ... AS statement,
// false otherwise.
func (node *CreateTable) As() bool {
	return node.AsSource != nil
}

// AsHasUserSpecifiedPrimaryKey returns true if a CREATE TABLE ... AS statement
// has a PRIMARY KEY constraint specified.
func (node *CreateTable) AsHasUserSpecifiedPrimaryKey() bool {
	if node.As() {
		for _, def := range node.Defs {
			if d, ok := def.(*ColumnTableDef); !ok {
				return false
			} else if d.PrimaryKey.IsPrimaryKey {
				return true
			}
		}
	}
	return false
}

// Format implements the NodeFormatter interface.
func (node *CreateTable) Format(ctx *FmtCtx) {
	ctx.WriteString("CREATE ")
	if node.Temporary {
		ctx.WriteString("TEMPORARY ")
	}
	ctx.WriteString("TABLE ")
	if node.IfNotExists {
		ctx.WriteString("IF NOT EXISTS ")
	}
	ctx.FormatNode(&node.Table)
	node.FormatBody(ctx)
}

// FormatBody formats the "body" of the create table definition - everything
// but the CREATE TABLE tableName part.
func (node *CreateTable) FormatBody(ctx *FmtCtx) {
	if node.As() {
		if len(node.Defs) > 0 {
			ctx.WriteString(" (")
			ctx.FormatNode(&node.Defs)
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

// HoistConstraints finds column check and foreign key constraints defined
// inline with their columns and makes them table-level constraints, stored in
// n.Defs. For example, the foreign key constraint in
//
//     CREATE TABLE foo (a INT REFERENCES bar(a))
//
// gets pulled into a top-level constraint like:
//
//     CREATE TABLE foo (a INT, FOREIGN KEY (a) REFERENCES bar(a))
//
// Similarly, the CHECK constraint in
//
//    CREATE TABLE foo (a INT CHECK (a < 1), b INT)
//
// gets pulled into a top-level constraint like:
//
//    CREATE TABLE foo (a INT, b INT, CHECK (a < 1))
//
// Note that some SQL databases require that a constraint attached to a column
// to refer only to the column it is attached to. We follow Postgres' behavior,
// however, in omitting this restriction by blindly hoisting all column
// constraints. For example, the following table definition is accepted in
// CockroachDB and Postgres, but not necessarily other SQL databases:
//
//    CREATE TABLE foo (a INT CHECK (a < b), b INT)
//
// Unique constraints are not hoisted.
//
func (node *CreateTable) HoistConstraints() {
	for _, d := range node.Defs {
		if col, ok := d.(*ColumnTableDef); ok {
			for _, checkExpr := range col.CheckExprs {
				node.Defs = append(node.Defs,
					&CheckConstraintTableDef{
						Expr: checkExpr.Expr,
						Name: checkExpr.ConstraintName,
					},
				)
			}
			col.CheckExprs = nil
			if col.HasFKConstraint() {
				var targetCol NameList
				if col.References.Col != "" {
					targetCol = append(targetCol, col.References.Col)
				}
				node.Defs = append(node.Defs, &ForeignKeyConstraintTableDef{
					Table:    *col.References.Table,
					FromCols: NameList{col.Name},
					ToCols:   targetCol,
					Name:     col.References.ConstraintName,
					Actions:  col.References.Actions,
					Match:    col.References.Match,
				})
				col.References.Table = nil
			}
		}
	}
}

// CreateSequence represents a CREATE SEQUENCE statement.
type CreateSequence struct {
	IfNotExists bool
	Name        TableName
	Temporary   bool
	Options     SequenceOptions
}

// Format implements the NodeFormatter interface.
func (node *CreateSequence) Format(ctx *FmtCtx) {
	ctx.WriteString("CREATE ")

	if node.Temporary {
		ctx.WriteString("TEMPORARY ")
	}

	ctx.WriteString("SEQUENCE ")

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
		case SeqOptVirtual:
			ctx.WriteString(option.Name)
		case SeqOptOwnedBy:
			ctx.WriteString(option.Name)
			ctx.WriteByte(' ')
			switch option.ColumnItemVal {
			case nil:
				ctx.WriteString("NONE")
			default:
				ctx.FormatNode(option.ColumnItemVal)
			}
		default:
			panic(errors.AssertionFailedf("unexpected SequenceOption: %v", option))
		}
	}
}

// SequenceOption represents an option on a CREATE SEQUENCE statement.
type SequenceOption struct {
	Name string

	IntVal *int64

	OptionalWord bool

	ColumnItemVal *ColumnItem
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
	SeqOptVirtual   = "VIRTUAL"

	// Avoid unused warning for constants.
	_ = SeqOptAs
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
	Name        TableName
	ColumnNames NameList
	AsSource    *Select
	Temporary   bool
}

// Format implements the NodeFormatter interface.
func (node *CreateView) Format(ctx *FmtCtx) {
	ctx.WriteString("CREATE ")

	if node.Temporary {
		ctx.WriteString("TEMPORARY ")
	}

	ctx.WriteString("VIEW ")
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
	Table       TableExpr
	Options     CreateStatsOptions
}

// Format implements the NodeFormatter interface.
func (node *CreateStats) Format(ctx *FmtCtx) {
	ctx.WriteString("CREATE STATISTICS ")
	ctx.FormatNode(&node.Name)

	if len(node.ColumnNames) > 0 {
		ctx.WriteString(" ON ")
		ctx.FormatNode(&node.ColumnNames)
	}

	ctx.WriteString(" FROM ")
	ctx.FormatNode(node.Table)

	if !node.Options.Empty() {
		ctx.WriteString(" WITH OPTIONS ")
		ctx.FormatNode(&node.Options)
	}
}

// CreateStatsOptions contains options for CREATE STATISTICS.
type CreateStatsOptions struct {
	// Throttling enables throttling and indicates the fraction of time we are
	// idling (between 0 and 1).
	Throttling float64

	// AsOf performs a historical read at the given timestamp.
	// Note that the timestamp will be moved up during the operation if it gets
	// too old (in order to avoid problems with TTL expiration).
	AsOf AsOfClause
}

// Empty returns true if no options were provided.
func (o *CreateStatsOptions) Empty() bool {
	return o.Throttling == 0 && o.AsOf.Expr == nil
}

// Format implements the NodeFormatter interface.
func (o *CreateStatsOptions) Format(ctx *FmtCtx) {
	sep := ""
	if o.Throttling != 0 {
		fmt.Fprintf(ctx, "THROTTLING %g", o.Throttling)
		sep = " "
	}
	if o.AsOf.Expr != nil {
		ctx.WriteString(sep)
		ctx.FormatNode(&o.AsOf)
		sep = " "
	}
}

// CombineWith combines two options, erroring out if the two options contain
// incompatible settings.
func (o *CreateStatsOptions) CombineWith(other *CreateStatsOptions) error {
	if other.Throttling != 0 {
		if o.Throttling != 0 {
			return errors.New("THROTTLING specified multiple times")
		}
		o.Throttling = other.Throttling
	}
	if other.AsOf.Expr != nil {
		if o.AsOf.Expr != nil {
			return errors.New("AS OF specified multiple times")
		}
		o.AsOf = other.AsOf
	}
	return nil
}
