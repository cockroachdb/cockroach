/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sqlparser

import (
	"encoding/hex"
	"encoding/json"
	"strings"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

// Walk calls visit on every node.
// If visit returns true, the underlying nodes
// are also visited. If it returns an error, walking
// is interrupted, and the error is returned.
func Walk(visit Visit, nodes ...SQLNode) error {
	for _, node := range nodes {
		if node == nil {
			continue
		}
		var err error
		var kontinue bool
		pre := func(cursor *Cursor) bool {
			// If we already have found an error, don't visit these nodes, just exit early
			if err != nil {
				return false
			}
			kontinue, err = visit(cursor.Node())
			if err != nil {
				return true // we have to return true here so that post gets called
			}
			return kontinue
		}
		post := func(cursor *Cursor) bool {
			return err == nil // now we can abort the traversal if an error was found
		}

		Rewrite(node, pre, post)
		if err != nil {
			return err
		}
	}
	return nil
}

// Visit defines the signature of a function that
// can be used to visit all nodes of a parse tree.
type Visit func(node SQLNode) (kontinue bool, err error)

// Append appends the SQLNode to the buffer.
func Append(buf *strings.Builder, node SQLNode) {
	tbuf := &TrackedBuffer{
		Builder: buf,
	}
	node.Format(tbuf)
}

// IndexColumn describes a column in an index definition with optional length
type IndexColumn struct {
	Column ColIdent
	Length *Literal
}

// LengthScaleOption is used for types that have an optional length
// and scale
type LengthScaleOption struct {
	Length *Literal
	Scale  *Literal
}

// IndexOption is used for trailing options for indexes: COMMENT, KEY_BLOCK_SIZE, USING
type IndexOption struct {
	Name  string
	Value *Literal
	Using string
}

// ColumnKeyOption indicates whether or not the given column is defined as an
// index element and contains the type of the option
type ColumnKeyOption int

const (
	colKeyNone ColumnKeyOption = iota
	colKeyPrimary
	colKeySpatialKey
	colKeyUnique
	colKeyUniqueKey
	colKey
)

// ReferenceAction indicates the action takes by a referential constraint e.g.
// the `CASCADE` in a `FOREIGN KEY .. ON DELETE CASCADE` table definition.
type ReferenceAction int

// These map to the SQL-defined reference actions.
// See https://dev.mysql.com/doc/refman/8.0/en/create-table-foreign-keys.html#foreign-keys-referential-actions
const (
	// DefaultAction indicates no action was explicitly specified.
	DefaultAction ReferenceAction = iota
	Restrict
	Cascade
	NoAction
	SetNull
	SetDefault
)

// ShowTablesOpt is show tables option
type ShowTablesOpt struct {
	Full   string
	DbName string
	Filter *ShowFilter
}

// ValType specifies the type for Literal.
type ValType int

// These are the possible Valtype values.
// HexNum represents a 0x... value. It cannot
// be treated as a simple value because it can
// be interpreted differently depending on the
// context.
const (
	StrVal = ValType(iota)
	IntVal
	FloatVal
	HexNum
	HexVal
	BitVal
)

// AffectedTables returns the list table names affected by the DDL.
func (node *DDL) AffectedTables() TableNames {
	if node.Action == RenameDDLAction || node.Action == DropDDLAction {
		list := make(TableNames, 0, len(node.FromTables)+len(node.ToTables))
		list = append(list, node.FromTables...)
		list = append(list, node.ToTables...)
		return list
	}
	return TableNames{node.Table}
}

// AddColumn appends the given column to the list in the spec
func (ts *TableSpec) AddColumn(cd *ColumnDefinition) {
	ts.Columns = append(ts.Columns, cd)
}

// AddIndex appends the given index to the list in the spec
func (ts *TableSpec) AddIndex(id *IndexDefinition) {
	ts.Indexes = append(ts.Indexes, id)
}

// AddConstraint appends the given index to the list in the spec
func (ts *TableSpec) AddConstraint(cd *ConstraintDefinition) {
	ts.Constraints = append(ts.Constraints, cd)
}

// DescribeType returns the abbreviated type information as required for
// describe table
func (ct *ColumnType) DescribeType() string {
	buf := NewTrackedBuffer(nil)
	buf.Myprintf("%s", ct.Type)
	if ct.Length != nil && ct.Scale != nil {
		buf.Myprintf("(%v,%v)", ct.Length, ct.Scale)
	} else if ct.Length != nil {
		buf.Myprintf("(%v)", ct.Length)
	}

	opts := make([]string, 0, 16)
	if ct.Unsigned {
		opts = append(opts, keywordStrings[UNSIGNED])
	}
	if ct.Zerofill {
		opts = append(opts, keywordStrings[ZEROFILL])
	}
	if len(opts) != 0 {
		buf.Myprintf(" %s", strings.Join(opts, " "))
	}
	return buf.String()
}

// SQLType returns the sqltypes type code for the given column
func (ct *ColumnType) SQLType() querypb.Type {
	switch strings.ToLower(ct.Type) {
	case keywordStrings[TINYINT]:
		if ct.Unsigned {
			return sqltypes.Uint8
		}
		return sqltypes.Int8
	case keywordStrings[SMALLINT]:
		if ct.Unsigned {
			return sqltypes.Uint16
		}
		return sqltypes.Int16
	case keywordStrings[MEDIUMINT]:
		if ct.Unsigned {
			return sqltypes.Uint24
		}
		return sqltypes.Int24
	case keywordStrings[INT], keywordStrings[INTEGER]:
		if ct.Unsigned {
			return sqltypes.Uint32
		}
		return sqltypes.Int32
	case keywordStrings[BIGINT]:
		if ct.Unsigned {
			return sqltypes.Uint64
		}
		return sqltypes.Int64
	case keywordStrings[BOOL], keywordStrings[BOOLEAN]:
		return sqltypes.Uint8
	case keywordStrings[TEXT]:
		return sqltypes.Text
	case keywordStrings[TINYTEXT]:
		return sqltypes.Text
	case keywordStrings[MEDIUMTEXT]:
		return sqltypes.Text
	case keywordStrings[LONGTEXT]:
		return sqltypes.Text
	case keywordStrings[BLOB]:
		return sqltypes.Blob
	case keywordStrings[TINYBLOB]:
		return sqltypes.Blob
	case keywordStrings[MEDIUMBLOB]:
		return sqltypes.Blob
	case keywordStrings[LONGBLOB]:
		return sqltypes.Blob
	case keywordStrings[CHAR]:
		return sqltypes.Char
	case keywordStrings[VARCHAR]:
		return sqltypes.VarChar
	case keywordStrings[BINARY]:
		return sqltypes.Binary
	case keywordStrings[VARBINARY]:
		return sqltypes.VarBinary
	case keywordStrings[DATE]:
		return sqltypes.Date
	case keywordStrings[TIME]:
		return sqltypes.Time
	case keywordStrings[DATETIME]:
		return sqltypes.Datetime
	case keywordStrings[TIMESTAMP]:
		return sqltypes.Timestamp
	case keywordStrings[YEAR]:
		return sqltypes.Year
	case keywordStrings[FLOAT_TYPE]:
		return sqltypes.Float32
	case keywordStrings[DOUBLE]:
		return sqltypes.Float64
	case keywordStrings[DECIMAL]:
		return sqltypes.Decimal
	case keywordStrings[BIT]:
		return sqltypes.Bit
	case keywordStrings[ENUM]:
		return sqltypes.Enum
	case keywordStrings[SET]:
		return sqltypes.Set
	case keywordStrings[JSON]:
		return sqltypes.TypeJSON
	case keywordStrings[GEOMETRY]:
		return sqltypes.Geometry
	case keywordStrings[POINT]:
		return sqltypes.Geometry
	case keywordStrings[LINESTRING]:
		return sqltypes.Geometry
	case keywordStrings[POLYGON]:
		return sqltypes.Geometry
	case keywordStrings[GEOMETRYCOLLECTION]:
		return sqltypes.Geometry
	case keywordStrings[MULTIPOINT]:
		return sqltypes.Geometry
	case keywordStrings[MULTILINESTRING]:
		return sqltypes.Geometry
	case keywordStrings[MULTIPOLYGON]:
		return sqltypes.Geometry
	}
	panic("unimplemented type " + ct.Type)
}

// ParseParams parses the vindex parameter list, pulling out the special-case
// "owner" parameter
func (node *VindexSpec) ParseParams() (string, map[string]string) {
	var owner string
	params := map[string]string{}
	for _, p := range node.Params {
		if p.Key.Lowered() == VindexOwnerStr {
			owner = p.Val
		} else {
			params[p.Key.String()] = p.Val
		}
	}
	return owner, params
}

var _ ConstraintInfo = &ForeignKeyDefinition{}

func (f *ForeignKeyDefinition) iConstraintInfo() {}

// HasOnTable returns true if the show statement has an "on" clause
func (node *Show) HasOnTable() bool {
	return node.OnTable.Name.v != ""
}

// HasTable returns true if the show statement has a parsed table name.
// Not all show statements parse table names.
func (node *Show) HasTable() bool {
	return node.Table.Name.v != ""
}

// FindColumn finds a column in the column list, returning
// the index if it exists or -1 otherwise
func (node Columns) FindColumn(col ColIdent) int {
	for i, colName := range node {
		if colName.Equal(col) {
			return i
		}
	}
	return -1
}

// RemoveHints returns a new AliasedTableExpr with the hints removed.
func (node *AliasedTableExpr) RemoveHints() *AliasedTableExpr {
	noHints := *node
	noHints.Hints = nil
	return &noHints
}

// IsEmpty returns true if TableName is nil or empty.
func (node TableName) IsEmpty() bool {
	// If Name is empty, Qualifier is also empty.
	return node.Name.IsEmpty()
}

// ToViewName returns a TableName acceptable for use as a VIEW. VIEW names are
// always lowercase, so ToViewName lowercasese the name. Databases are case-sensitive
// so Qualifier is left untouched.
func (node TableName) ToViewName() TableName {
	return TableName{
		Qualifier: node.Qualifier,
		Name:      NewTableIdent(strings.ToLower(node.Name.v)),
	}
}

// NewWhere creates a WHERE or HAVING clause out
// of a Expr. If the expression is nil, it returns nil.
func NewWhere(typ WhereType, expr Expr) *Where {
	if expr == nil {
		return nil
	}
	return &Where{Type: typ, Expr: expr}
}

// ReplaceExpr finds the from expression from root
// and replaces it with to. If from matches root,
// then to is returned.
func ReplaceExpr(root, from, to Expr) Expr {
	tmp := Rewrite(root, replaceExpr(from, to), nil)
	expr, success := tmp.(Expr)
	if !success {
		return from
	}

	return expr
}

func replaceExpr(from, to Expr) func(cursor *Cursor) bool {
	return func(cursor *Cursor) bool {
		if cursor.Node() == from {
			cursor.Replace(to)
		}
		switch cursor.Node().(type) {
		case *ExistsExpr, *Literal, *Subquery, *ValuesFuncExpr, *Default:
			return false
		}

		return true
	}
}

// IsImpossible returns true if the comparison in the expression can never evaluate to true.
// Note that this is not currently exhaustive to ALL impossible comparisons.
func (node *ComparisonExpr) IsImpossible() bool {
	var left, right *Literal
	var ok bool
	if left, ok = node.Left.(*Literal); !ok {
		return false
	}
	if right, ok = node.Right.(*Literal); !ok {
		return false
	}
	if node.Operator == NotEqualOp && left.Type == right.Type {
		if len(left.Val) != len(right.Val) {
			return false
		}

		for i := range left.Val {
			if left.Val[i] != right.Val[i] {
				return false
			}
		}
		return true
	}
	return false
}

// NewStrLiteral builds a new StrVal.
func NewStrLiteral(in []byte) *Literal {
	return &Literal{Type: StrVal, Val: in}
}

// NewIntLiteral builds a new IntVal.
func NewIntLiteral(in []byte) *Literal {
	return &Literal{Type: IntVal, Val: in}
}

// NewFloatLiteral builds a new FloatVal.
func NewFloatLiteral(in []byte) *Literal {
	return &Literal{Type: FloatVal, Val: in}
}

// NewHexNumLiteral builds a new HexNum.
func NewHexNumLiteral(in []byte) *Literal {
	return &Literal{Type: HexNum, Val: in}
}

// NewHexLiteral builds a new HexVal.
func NewHexLiteral(in []byte) *Literal {
	return &Literal{Type: HexVal, Val: in}
}

// NewBitLiteral builds a new BitVal containing a bit literal.
func NewBitLiteral(in []byte) *Literal {
	return &Literal{Type: BitVal, Val: in}
}

// NewArgument builds a new ValArg.
func NewArgument(in []byte) Argument {
	return in
}

// HexDecode decodes the hexval into bytes.
func (node *Literal) HexDecode() ([]byte, error) {
	dst := make([]byte, hex.DecodedLen(len([]byte(node.Val))))
	_, err := hex.Decode(dst, []byte(node.Val))
	if err != nil {
		return nil, err
	}
	return dst, err
}

// Equal returns true if the column names match.
func (node *ColName) Equal(c *ColName) bool {
	// Failsafe: ColName should not be empty.
	if node == nil || c == nil {
		return false
	}
	return node.Name.Equal(c.Name) && node.Qualifier == c.Qualifier
}

// Aggregates is a map of all aggregate functions.
var Aggregates = map[string]bool{
	"avg":          true,
	"bit_and":      true,
	"bit_or":       true,
	"bit_xor":      true,
	"count":        true,
	"group_concat": true,
	"max":          true,
	"min":          true,
	"std":          true,
	"stddev_pop":   true,
	"stddev_samp":  true,
	"stddev":       true,
	"sum":          true,
	"var_pop":      true,
	"var_samp":     true,
	"variance":     true,
}

// IsAggregate returns true if the function is an aggregate.
func (node *FuncExpr) IsAggregate() bool {
	return Aggregates[node.Name.Lowered()]
}

// NewColIdent makes a new ColIdent.
func NewColIdent(str string) ColIdent {
	return ColIdent{
		val: str,
	}
}

// NewColName makes a new ColName
func NewColName(str string) *ColName {
	return &ColName{
		Name: NewColIdent(str),
	}
}

//NewSelect is used to create a select statement
func NewSelect(
	comments Comments,
	exprs SelectExprs,
	selectOptions []string,
	from TableExprs,
	where *Where,
	groupBy GroupBy,
	having *Where,
) *Select {
	var cache *bool
	var distinct, straightJoinHint, sqlFoundRows bool

	for _, option := range selectOptions {
		switch strings.ToLower(option) {
		case DistinctStr:
			distinct = true
		case SQLCacheStr:
			truth := true
			cache = &truth
		case SQLNoCacheStr:
			truth := false
			cache = &truth
		case StraightJoinHint:
			straightJoinHint = true
		case SQLCalcFoundRowsStr:
			sqlFoundRows = true
		}
	}
	return &Select{
		Cache:            cache,
		Comments:         comments,
		Distinct:         distinct,
		StraightJoinHint: straightJoinHint,
		SQLCalcFoundRows: sqlFoundRows,
		SelectExprs:      exprs,
		From:             from,
		Where:            where,
		GroupBy:          groupBy,
		Having:           having,
	}
}

// NewColIdentWithAt makes a new ColIdent.
func NewColIdentWithAt(str string, at AtCount) ColIdent {
	return ColIdent{
		val: str,
		at:  at,
	}
}

// IsEmpty returns true if the name is empty.
func (node ColIdent) IsEmpty() bool {
	return node.val == ""
}

// String returns the unescaped column name. It must
// not be used for SQL generation. Use sqlparser.String
// instead. The Stringer conformance is for usage
// in templates.
func (node ColIdent) String() string {
	atStr := ""
	for i := NoAt; i < node.at; i++ {
		atStr += "@"
	}
	return atStr + node.val
}

// CompliantName returns a compliant id name
// that can be used for a bind var.
func (node ColIdent) CompliantName() string {
	return compliantName(node.val)
}

// Lowered returns a lower-cased column name.
// This function should generally be used only for optimizing
// comparisons.
func (node ColIdent) Lowered() string {
	if node.val == "" {
		return ""
	}
	if node.lowered == "" {
		node.lowered = strings.ToLower(node.val)
	}
	return node.lowered
}

// Equal performs a case-insensitive compare.
func (node ColIdent) Equal(in ColIdent) bool {
	return node.Lowered() == in.Lowered()
}

// EqualString performs a case-insensitive compare with str.
func (node ColIdent) EqualString(str string) bool {
	return node.Lowered() == strings.ToLower(str)
}

// MarshalJSON marshals into JSON.
func (node ColIdent) MarshalJSON() ([]byte, error) {
	return json.Marshal(node.val)
}

// UnmarshalJSON unmarshals from JSON.
func (node *ColIdent) UnmarshalJSON(b []byte) error {
	var result string
	err := json.Unmarshal(b, &result)
	if err != nil {
		return err
	}
	node.val = result
	return nil
}

// NewTableIdent creates a new TableIdent.
func NewTableIdent(str string) TableIdent {
	return TableIdent{v: str}
}

// IsEmpty returns true if TabIdent is empty.
func (node TableIdent) IsEmpty() bool {
	return node.v == ""
}

// String returns the unescaped table name. It must
// not be used for SQL generation. Use sqlparser.String
// instead. The Stringer conformance is for usage
// in templates.
func (node TableIdent) String() string {
	return node.v
}

// CompliantName returns a compliant id name
// that can be used for a bind var.
func (node TableIdent) CompliantName() string {
	return compliantName(node.v)
}

// MarshalJSON marshals into JSON.
func (node TableIdent) MarshalJSON() ([]byte, error) {
	return json.Marshal(node.v)
}

// UnmarshalJSON unmarshals from JSON.
func (node *TableIdent) UnmarshalJSON(b []byte) error {
	var result string
	err := json.Unmarshal(b, &result)
	if err != nil {
		return err
	}
	node.v = result
	return nil
}

func containEscapableChars(s string, at AtCount) bool {
	isDbSystemVariable := at != NoAt

	for i, c := range s {
		letter := isLetter(uint16(c))
		systemVarChar := isDbSystemVariable && isCarat(uint16(c))
		if !(letter || systemVarChar) {
			if i == 0 || !isDigit(uint16(c)) {
				return true
			}
		}
	}

	return false
}

func isKeyword(s string) bool {
	_, isKeyword := keywords[s]
	return isKeyword
}

func formatID(buf *TrackedBuffer, original, lowered string, at AtCount) {
	if containEscapableChars(original, at) || isKeyword(lowered) {
		writeEscapedString(buf, original)
	} else {
		buf.Myprintf("%s", original)
	}
}

func writeEscapedString(buf *TrackedBuffer, original string) {
	buf.WriteByte('`')
	for _, c := range original {
		buf.WriteRune(c)
		if c == '`' {
			buf.WriteByte('`')
		}
	}
	buf.WriteByte('`')
}

func compliantName(in string) string {
	var buf strings.Builder
	for i, c := range in {
		if !isLetter(uint16(c)) {
			if i == 0 || !isDigit(uint16(c)) {
				buf.WriteByte('_')
				continue
			}
		}
		buf.WriteRune(c)
	}
	return buf.String()
}

// AddOrder adds an order by element
func (node *Select) AddOrder(order *Order) {
	node.OrderBy = append(node.OrderBy, order)
}

// SetLimit sets the limit clause
func (node *Select) SetLimit(limit *Limit) {
	node.Limit = limit
}

// SetLock sets the lock clause
func (node *Select) SetLock(lock Lock) {
	node.Lock = lock
}

// AddWhere adds the boolean expression to the
// WHERE clause as an AND condition.
func (node *Select) AddWhere(expr Expr) {
	if node.Where == nil {
		node.Where = &Where{
			Type: WhereClause,
			Expr: expr,
		}
		return
	}
	node.Where.Expr = &AndExpr{
		Left:  node.Where.Expr,
		Right: expr,
	}
}

// AddHaving adds the boolean expression to the
// HAVING clause as an AND condition.
func (node *Select) AddHaving(expr Expr) {
	if node.Having == nil {
		node.Having = &Where{
			Type: HavingClause,
			Expr: expr,
		}
		return
	}
	node.Having.Expr = &AndExpr{
		Left:  node.Having.Expr,
		Right: expr,
	}
}

// AddOrder adds an order by element
func (node *ParenSelect) AddOrder(order *Order) {
	node.Select.AddOrder(order)
}

// SetLimit sets the limit clause
func (node *ParenSelect) SetLimit(limit *Limit) {
	node.Select.SetLimit(limit)
}

// SetLock sets the lock clause
func (node *ParenSelect) SetLock(lock Lock) {
	node.Select.SetLock(lock)
}

// AddOrder adds an order by element
func (node *Union) AddOrder(order *Order) {
	node.OrderBy = append(node.OrderBy, order)
}

// SetLimit sets the limit clause
func (node *Union) SetLimit(limit *Limit) {
	node.Limit = limit
}

// SetLock sets the lock clause
func (node *Union) SetLock(lock Lock) {
	node.Lock = lock
}

//Unionize returns a UNION, either creating one or adding SELECT to an existing one
func Unionize(lhs, rhs SelectStatement, typ UnionType, by OrderBy, limit *Limit, lock Lock) *Union {
	union, isUnion := lhs.(*Union)
	if isUnion {
		union.UnionSelects = append(union.UnionSelects, &UnionSelect{Type: typ, Statement: rhs})
		union.OrderBy = by
		union.Limit = limit
		union.Lock = lock
		return union
	}

	return &Union{FirstStatement: lhs, UnionSelects: []*UnionSelect{{Type: typ, Statement: rhs}}, OrderBy: by, Limit: limit, Lock: lock}
}

// ToString returns the string associated with the DDLAction Enum
func (action DDLAction) ToString() string {
	switch action {
	case CreateDDLAction:
		return CreateStr
	case AlterDDLAction:
		return AlterStr
	case DropDDLAction:
		return DropStr
	case RenameDDLAction:
		return RenameStr
	case TruncateDDLAction:
		return TruncateStr
	case FlushDDLAction:
		return FlushStr
	case CreateVindexDDLAction:
		return CreateVindexStr
	case DropVindexDDLAction:
		return DropVindexStr
	case AddVschemaTableDDLAction:
		return AddVschemaTableStr
	case DropVschemaTableDDLAction:
		return DropVschemaTableStr
	case AddColVindexDDLAction:
		return AddColVindexStr
	case DropColVindexDDLAction:
		return DropColVindexStr
	case AddSequenceDDLAction:
		return AddSequenceStr
	case AddAutoIncDDLAction:
		return AddAutoIncStr
	default:
		return "Unknown DDL Action"
	}
}

// ToString returns the string associated with the Scope enum
func (scope Scope) ToString() string {
	switch scope {
	case SessionScope:
		return SessionStr
	case GlobalScope:
		return GlobalStr
	case VitessMetadataScope:
		return VitessMetadataStr
	case VariableScope:
		return VariableStr
	case LocalScope:
		return LocalStr
	case ImplicitScope:
		return ImplicitStr
	default:
		return "Unknown Scope"
	}
}

// ToString returns the IgnoreStr if ignore is true.
func (ignore Ignore) ToString() string {
	if ignore {
		return IgnoreStr
	}
	return ""
}

// ToString returns the string associated with the type of lock
func (lock Lock) ToString() string {
	switch lock {
	case NoLock:
		return NoLockStr
	case ForUpdateLock:
		return ForUpdateStr
	case ShareModeLock:
		return ShareModeStr
	default:
		return "Unknown lock"
	}
}

// ToString returns the string associated with WhereType
func (whereType WhereType) ToString() string {
	switch whereType {
	case WhereClause:
		return WhereStr
	case HavingClause:
		return HavingStr
	default:
		return "Unknown where type"
	}
}

// ToString returns the string associated with JoinType
func (joinType JoinType) ToString() string {
	switch joinType {
	case NormalJoinType:
		return JoinStr
	case StraightJoinType:
		return StraightJoinStr
	case LeftJoinType:
		return LeftJoinStr
	case RightJoinType:
		return RightJoinStr
	case NaturalJoinType:
		return NaturalJoinStr
	case NaturalLeftJoinType:
		return NaturalLeftJoinStr
	case NaturalRightJoinType:
		return NaturalRightJoinStr
	default:
		return "Unknown join type"
	}
}

// ToString returns the operator as a string
func (op ComparisonExprOperator) ToString() string {
	switch op {
	case EqualOp:
		return EqualStr
	case LessThanOp:
		return LessThanStr
	case GreaterThanOp:
		return GreaterThanStr
	case LessEqualOp:
		return LessEqualStr
	case GreaterEqualOp:
		return GreaterEqualStr
	case NotEqualOp:
		return NotEqualStr
	case NullSafeEqualOp:
		return NullSafeEqualStr
	case InOp:
		return InStr
	case NotInOp:
		return NotInStr
	case LikeOp:
		return LikeStr
	case NotLikeOp:
		return NotLikeStr
	case RegexpOp:
		return RegexpStr
	case NotRegexpOp:
		return NotRegexpStr
	default:
		return "Unknown ComparisonExpOperator"
	}
}

// ToString returns the operator as a string
func (op RangeCondOperator) ToString() string {
	switch op {
	case BetweenOp:
		return BetweenStr
	case NotBetweenOp:
		return NotBetweenStr
	default:
		return "Unknown RangeCondOperator"
	}
}

// ToString returns the operator as a string
func (op IsExprOperator) ToString() string {
	switch op {
	case IsNullOp:
		return IsNullStr
	case IsNotNullOp:
		return IsNotNullStr
	case IsTrueOp:
		return IsTrueStr
	case IsNotTrueOp:
		return IsNotTrueStr
	case IsFalseOp:
		return IsFalseStr
	case IsNotFalseOp:
		return IsNotFalseStr
	default:
		return "Unknown IsExprOperator"
	}
}

// ToString returns the operator as a string
func (op BinaryExprOperator) ToString() string {
	switch op {
	case BitAndOp:
		return BitAndStr
	case BitOrOp:
		return BitOrStr
	case BitXorOp:
		return BitXorStr
	case PlusOp:
		return PlusStr
	case MinusOp:
		return MinusStr
	case MultOp:
		return MultStr
	case DivOp:
		return DivStr
	case IntDivOp:
		return IntDivStr
	case ModOp:
		return ModStr
	case ShiftLeftOp:
		return ShiftLeftStr
	case ShiftRightOp:
		return ShiftRightStr
	case JSONExtractOp:
		return JSONExtractOpStr
	case JSONUnquoteExtractOp:
		return JSONUnquoteExtractOpStr
	default:
		return "Unknown BinaryExprOperator"
	}
}

// ToString returns the operator as a string
func (op UnaryExprOperator) ToString() string {
	switch op {
	case UPlusOp:
		return UPlusStr
	case UMinusOp:
		return UMinusStr
	case TildaOp:
		return TildaStr
	case BangOp:
		return BangStr
	case BinaryOp:
		return BinaryStr
	case UBinaryOp:
		return UBinaryStr
	case Utf8mb4Op:
		return Utf8mb4Str
	case Utf8Op:
		return Utf8Str
	case Latin1Op:
		return Latin1Str
	default:
		return "Unknown UnaryExprOperator"
	}
}

// ToString returns the option as a string
func (option MatchExprOption) ToString() string {
	switch option {
	case NoOption:
		return NoOptionStr
	case BooleanModeOpt:
		return BooleanModeStr
	case NaturalLanguageModeOpt:
		return NaturalLanguageModeStr
	case NaturalLanguageModeWithQueryExpansionOpt:
		return NaturalLanguageModeWithQueryExpansionStr
	case QueryExpansionOpt:
		return QueryExpansionStr
	default:
		return "Unknown MatchExprOption"
	}
}

// ToString returns the direction as a string
func (dir OrderDirection) ToString() string {
	switch dir {
	case AscOrder:
		return AscScr
	case DescOrder:
		return DescScr
	default:
		return "Unknown OrderDirection"
	}
}

// ToString returns the operator as a string
func (op ConvertTypeOperator) ToString() string {
	switch op {
	case NoOperator:
		return NoOperatorStr
	case CharacterSetOp:
		return CharacterSetStr
	default:
		return "Unknown ConvertTypeOperator"
	}
}

// ToString returns the type as a string
func (ty IndexHintsType) ToString() string {
	switch ty {
	case UseOp:
		return UseStr
	case IgnoreOp:
		return IgnoreStr
	case ForceOp:
		return ForceStr
	default:
		return "Unknown IndexHintsType"
	}
}

// ToString returns the type as a string
func (ty ExplainType) ToString() string {
	switch ty {
	case EmptyType:
		return EmptyStr
	case TreeType:
		return TreeStr
	case JSONType:
		return JSONStr
	case VitessType:
		return VitessStr
	case TraditionalType:
		return TraditionalStr
	case AnalyzeType:
		return AnalyzeStr
	default:
		return "Unknown ExplainType"
	}
}

// AtCount represents the '@' count in ColIdent
type AtCount int

const (
	// NoAt represents no @
	NoAt AtCount = iota
	// SingleAt represents @
	SingleAt
	// DoubleAt represnts @@
	DoubleAt
)
