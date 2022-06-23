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

// analyzer.go contains utility analysis functions.

import (
	"fmt"
	"strings"
	"unicode"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vterrors"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// StatementType encodes the type of a SQL statement
type StatementType int

// These constants are used to identify the SQL statement type.
// Changing this list will require reviewing all calls to Preview.
const (
	StmtSelect StatementType = iota
	StmtStream
	StmtInsert
	StmtReplace
	StmtUpdate
	StmtDelete
	StmtDDL
	StmtBegin
	StmtCommit
	StmtRollback
	StmtSet
	StmtShow
	StmtUse
	StmtOther
	StmtUnknown
	StmtComment
	StmtPriv
	StmtExplain
	StmtSavepoint
	StmtSRollback
	StmtRelease
	StmtVStream
)

//ASTToStatementType returns a StatementType from an AST stmt
func ASTToStatementType(stmt Statement) StatementType {
	switch stmt.(type) {
	case *Select, *Union:
		return StmtSelect
	case *Insert:
		return StmtInsert
	case *Update:
		return StmtUpdate
	case *Delete:
		return StmtDelete
	case *Set:
		return StmtSet
	case *Show:
		return StmtShow
	case *DDL, *DBDDL:
		return StmtDDL
	case *Use:
		return StmtUse
	case *OtherRead, *OtherAdmin:
		return StmtOther
	case *Explain:
		return StmtExplain
	case *Begin:
		return StmtBegin
	case *Commit:
		return StmtCommit
	case *Rollback:
		return StmtRollback
	case *Savepoint:
		return StmtSavepoint
	case *SRollback:
		return StmtSRollback
	case *Release:
		return StmtRelease
	case *ShowTableStatus:
		return StmtShow
	default:
		return StmtUnknown
	}
}

//CanNormalize takes Statement and returns if the statement can be normalized.
func CanNormalize(stmt Statement) bool {
	switch stmt.(type) {
	case *Select, *Union, *Insert, *Update, *Delete, *Set:
		return true
	}
	return false
}

//IsSetStatement takes Statement and returns if the statement is set statement.
func IsSetStatement(stmt Statement) bool {
	switch stmt.(type) {
	case *Set:
		return true
	}
	return false
}

// Preview analyzes the beginning of the query using a simpler and faster
// textual comparison to identify the statement type.
func Preview(sql string) StatementType {
	trimmed := StripLeadingComments(sql)

	if strings.Index(trimmed, "/*!") == 0 {
		return StmtComment
	}

	isNotLetter := func(r rune) bool { return !unicode.IsLetter(r) }
	firstWord := strings.TrimLeftFunc(trimmed, isNotLetter)

	if end := strings.IndexFunc(firstWord, unicode.IsSpace); end != -1 {
		firstWord = firstWord[:end]
	}
	// Comparison is done in order of priority.
	loweredFirstWord := strings.ToLower(firstWord)
	switch loweredFirstWord {
	case "select":
		return StmtSelect
	case "stream":
		return StmtStream
	case "vstream":
		return StmtVStream
	case "insert":
		return StmtInsert
	case "replace":
		return StmtReplace
	case "update":
		return StmtUpdate
	case "delete":
		return StmtDelete
	case "savepoint":
		return StmtSavepoint
	}
	// For the following statements it is not sufficient to rely
	// on loweredFirstWord. This is because they are not statements
	// in the grammar and we are relying on Preview to parse them.
	// For instance, we don't want: "BEGIN JUNK" to be parsed
	// as StmtBegin.
	trimmedNoComments, _ := SplitMarginComments(trimmed)
	switch strings.ToLower(trimmedNoComments) {
	case "begin", "start transaction":
		return StmtBegin
	case "commit":
		return StmtCommit
	case "rollback":
		return StmtRollback
	}
	switch loweredFirstWord {
	case "create", "alter", "rename", "drop", "truncate", "flush":
		return StmtDDL
	case "set":
		return StmtSet
	case "show":
		return StmtShow
	case "use":
		return StmtUse
	case "describe", "desc", "explain":
		return StmtExplain
	case "analyze", "repair", "optimize":
		return StmtOther
	case "grant", "revoke":
		return StmtPriv
	case "release":
		return StmtRelease
	case "rollback":
		return StmtSRollback
	}
	return StmtUnknown
}

func (s StatementType) String() string {
	switch s {
	case StmtSelect:
		return "SELECT"
	case StmtStream:
		return "STREAM"
	case StmtVStream:
		return "VSTREAM"
	case StmtInsert:
		return "INSERT"
	case StmtReplace:
		return "REPLACE"
	case StmtUpdate:
		return "UPDATE"
	case StmtDelete:
		return "DELETE"
	case StmtDDL:
		return "DDL"
	case StmtBegin:
		return "BEGIN"
	case StmtCommit:
		return "COMMIT"
	case StmtRollback:
		return "ROLLBACK"
	case StmtSet:
		return "SET"
	case StmtShow:
		return "SHOW"
	case StmtUse:
		return "USE"
	case StmtOther:
		return "OTHER"
	case StmtPriv:
		return "PRIV"
	case StmtExplain:
		return "EXPLAIN"
	case StmtSavepoint:
		return "SAVEPOINT"
	case StmtSRollback:
		return "SAVEPOINT_ROLLBACK"
	case StmtRelease:
		return "RELEASE"
	default:
		return "UNKNOWN"
	}
}

// IsDML returns true if the query is an INSERT, UPDATE or DELETE statement.
func IsDML(sql string) bool {
	switch Preview(sql) {
	case StmtInsert, StmtReplace, StmtUpdate, StmtDelete:
		return true
	}
	return false
}

//IsDMLStatement returns true if the query is an INSERT, UPDATE or DELETE statement.
func IsDMLStatement(stmt Statement) bool {
	switch stmt.(type) {
	case *Insert, *Update, *Delete:
		return true
	}

	return false
}

//IsVschemaDDL returns true if the query is an Vschema alter ddl.
func IsVschemaDDL(ddl *DDL) bool {
	switch ddl.Action {
	case CreateVindexDDLAction, DropVindexDDLAction, AddVschemaTableDDLAction, DropVschemaTableDDLAction, AddColVindexDDLAction, DropColVindexDDLAction, AddSequenceDDLAction, AddAutoIncDDLAction:
		return true
	}
	return false
}

// IsOnlineSchemaDDL returns true if the query is an online schema change DDL
func IsOnlineSchemaDDL(ddl *DDL, sql string) bool {
	switch ddl.Action {
	case AlterDDLAction:
		if ddl.OnlineHint != nil {
			return ddl.OnlineHint.Strategy != ""
		}
	}
	return false
}

// SplitAndExpression breaks up the Expr into AND-separated conditions
// and appends them to filters. Outer parenthesis are removed. Precedence
// should be taken into account if expressions are recombined.
func SplitAndExpression(filters []Expr, node Expr) []Expr {
	if node == nil {
		return filters
	}
	switch node := node.(type) {
	case *AndExpr:
		filters = SplitAndExpression(filters, node.Left)
		return SplitAndExpression(filters, node.Right)
	}
	return append(filters, node)
}

// TableFromStatement returns the qualified table name for the query.
// This works only for select statements.
func TableFromStatement(sql string) (TableName, error) {
	stmt, err := Parse(sql)
	if err != nil {
		return TableName{}, err
	}
	sel, ok := stmt.(*Select)
	if !ok {
		return TableName{}, fmt.Errorf("unrecognized statement: %s", sql)
	}
	if len(sel.From) != 1 {
		return TableName{}, fmt.Errorf("table expression is complex")
	}
	aliased, ok := sel.From[0].(*AliasedTableExpr)
	if !ok {
		return TableName{}, fmt.Errorf("table expression is complex")
	}
	tableName, ok := aliased.Expr.(TableName)
	if !ok {
		return TableName{}, fmt.Errorf("table expression is complex")
	}
	return tableName, nil
}

// GetTableName returns the table name from the SimpleTableExpr
// only if it's a simple expression. Otherwise, it returns "".
func GetTableName(node SimpleTableExpr) TableIdent {
	if n, ok := node.(TableName); ok && n.Qualifier.IsEmpty() {
		return n.Name
	}
	// sub-select or '.' expression
	return NewTableIdent("")
}

// IsColName returns true if the Expr is a *ColName.
func IsColName(node Expr) bool {
	_, ok := node.(*ColName)
	return ok
}

// IsValue returns true if the Expr is a string, integral or value arg.
// NULL is not considered to be a value.
func IsValue(node Expr) bool {
	switch v := node.(type) {
	case Argument:
		return true
	case *Literal:
		switch v.Type {
		case StrVal, HexVal, IntVal:
			return true
		}
	}
	return false
}

// IsNull returns true if the Expr is SQL NULL
func IsNull(node Expr) bool {
	switch node.(type) {
	case *NullVal:
		return true
	}
	return false
}

// IsSimpleTuple returns true if the Expr is a ValTuple that
// contains simple values or if it's a list arg.
func IsSimpleTuple(node Expr) bool {
	switch vals := node.(type) {
	case ValTuple:
		for _, n := range vals {
			if !IsValue(n) {
				return false
			}
		}
		return true
	case ListArg:
		return true
	}
	// It's a subquery
	return false
}

// NewPlanValue builds a sqltypes.PlanValue from an Expr.
func NewPlanValue(node Expr) (sqltypes.PlanValue, error) {
	switch node := node.(type) {
	case Argument:
		return sqltypes.PlanValue{Key: string(node[1:])}, nil
	case *Literal:
		switch node.Type {
		case IntVal:
			n, err := sqltypes.NewIntegral(string(node.Val))
			if err != nil {
				return sqltypes.PlanValue{}, err
			}
			return sqltypes.PlanValue{Value: n}, nil
		case FloatVal:
			return sqltypes.PlanValue{Value: sqltypes.MakeTrusted(sqltypes.Float64, node.Val)}, nil
		case StrVal:
			return sqltypes.PlanValue{Value: sqltypes.MakeTrusted(sqltypes.VarBinary, node.Val)}, nil
		case HexVal:
			v, err := node.HexDecode()
			if err != nil {
				return sqltypes.PlanValue{}, err
			}
			return sqltypes.PlanValue{Value: sqltypes.MakeTrusted(sqltypes.VarBinary, v)}, nil
		}
	case ListArg:
		return sqltypes.PlanValue{ListKey: string(node[2:])}, nil
	case ValTuple:
		pv := sqltypes.PlanValue{
			Values: make([]sqltypes.PlanValue, 0, len(node)),
		}
		for _, val := range node {
			innerpv, err := NewPlanValue(val)
			if err != nil {
				return sqltypes.PlanValue{}, err
			}
			if innerpv.ListKey != "" || innerpv.Values != nil {
				return sqltypes.PlanValue{}, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: nested lists")
			}
			pv.Values = append(pv.Values, innerpv)
		}
		return pv, nil
	case *NullVal:
		return sqltypes.PlanValue{}, nil
	case *UnaryExpr:
		switch node.Operator {
		case UBinaryOp, Utf8mb4Op, Utf8Op, Latin1Op: // for some charset introducers, we can just ignore them
			return NewPlanValue(node.Expr)
		}
	}
	return sqltypes.PlanValue{}, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "expression is too complex '%v'", String(node))
}

//IsLockingFunc returns true for all functions that are used to work with mysql advisory locks
func IsLockingFunc(node Expr) bool {
	switch p := node.(type) {
	case *FuncExpr:
		_, found := lockingFunctions[p.Name.Lowered()]
		return found
	}
	return false
}

var lockingFunctions = map[string]interface{}{
	"get_lock":          nil,
	"is_free_lock":      nil,
	"is_used_lock":      nil,
	"release_all_locks": nil,
	"release_lock":      nil,
}
