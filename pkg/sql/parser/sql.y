// Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
// Portions Copyright (c) 1994, Regents of the University of California

// Portions of this file are additionally subject to the following
// license and copyright.
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

// Going to add a new statement?
// Consider taking a look at our codelab guide to learn what is needed to add a statement.
// https://github.com/cockroachdb/cockroach/blob/master/docs/codelabs/01-sql-statement.md

%{
package parser

import (
    "fmt"
    "math"
    "strings"

    "go/constant"

    "github.com/cockroachdb/cockroach/pkg/geo/geopb"
    "github.com/cockroachdb/cockroach/pkg/security/username"
    "github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
    "github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
    "github.com/cockroachdb/cockroach/pkg/sql/privilege"
    "github.com/cockroachdb/cockroach/pkg/sql/scanner"
    "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
    "github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treebin"
    "github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treecmp"
    "github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treewindow"
    "github.com/cockroachdb/cockroach/pkg/sql/sem/idxtype"
    "github.com/cockroachdb/cockroach/pkg/sql/types"
    "github.com/cockroachdb/cockroach/pkg/util/vector"
    "github.com/cockroachdb/errors"
    "github.com/lib/pq/oid"
)

// MaxUint is the maximum value of an uint.
const MaxUint = ^uint(0)
// MaxInt is the maximum value of an int.
const MaxInt = int(MaxUint >> 1)

func unimplemented(sqllex sqlLexer, feature string) int {
    sqllex.(*lexer).Unimplemented(feature)
    return 1
}

func purposelyUnimplemented(sqllex sqlLexer, feature string, reason string) int {
    sqllex.(*lexer).PurposelyUnimplemented(feature, reason)
    return 1
}

func setErr(sqllex sqlLexer, err error) int {
    sqllex.(*lexer).setErr(err)
    return 1
}

func setErrNoDetails(sqllex sqlLexer, err error) int {
    sqllex.(*lexer).setErrNoDetails(err)
    return 1
}

func unimplementedWithIssue(sqllex sqlLexer, issue int) int {
    sqllex.(*lexer).UnimplementedWithIssue(issue)
    return 1
}

func unimplementedWithIssueDetail(sqllex sqlLexer, issue int, detail string) int {
    sqllex.(*lexer).UnimplementedWithIssueDetail(issue, detail)
    return 1
}

func processBinaryQualOp(
  sqllex sqlLexer,
  op tree.Operator,
  lhs tree.Expr,
  rhs tree.Expr,
) (tree.Expr, int) {
  switch op := op.(type) {
  case treebin.BinaryOperator:
    op.IsExplicitOperator = true
    return &tree.BinaryExpr{Operator: op, Left: lhs, Right: rhs}, 0
  case treecmp.ComparisonOperator:
    op.IsExplicitOperator = true
    return &tree.ComparisonExpr{Operator: op, Left: lhs, Right: rhs}, 0
  case tree.UnaryOperator:
    // We have a unary operator which have the same symbol as the binary
    // operator, so adjust accordingly.
    switch op.Symbol {
    case tree.UnaryComplement:
      return &tree.ComparisonExpr{
        Operator: treecmp.ComparisonOperator{
          Symbol: treecmp.RegMatch,
          IsExplicitOperator: true,
        },
        Left: lhs,
        Right: rhs,
      }, 0
    default:
      sqllex.Error(fmt.Sprintf("unknown binary operator %s", op))
      return nil, -1
    }
  default:
    sqllex.Error(fmt.Sprintf("unknown binary operator %s", op))
    return nil, 1
  }
}

func processUnaryQualOp(
  sqllex sqlLexer,
  op tree.Operator,
  expr tree.Expr,
) (tree.Expr, int) {
  e, code := processUnaryQualOpInternal(sqllex, op, expr)
  if code != 0 {
    return e, code
  }
  if e, ok := e.(*tree.UnaryExpr); ok {
    e.Operator.IsExplicitOperator = true
  }
  return e, code
}

func processUnaryQualOpInternal(
  sqllex sqlLexer,
  op tree.Operator,
  expr tree.Expr,
) (tree.Expr, int) {
  switch op := op.(type) {
  case tree.UnaryOperator:
    return &tree.UnaryExpr{Operator: op, Expr: expr}, 0
  case treebin.BinaryOperator:
    // We have some binary operators which have the same symbol as the unary
    // operator, so adjust accordingly.
    switch op.Symbol {
    case treebin.Plus:
      return &tree.UnaryExpr{
        Operator: tree.MakeUnaryOperator(tree.UnaryPlus),
        Expr: expr,
      }, 0
    case treebin.Minus:
      return &tree.UnaryExpr{
        Operator: tree.MakeUnaryOperator(tree.UnaryMinus),
        Expr: expr,
      }, 0
    }
  case treecmp.ComparisonOperator:
    switch op.Symbol {
    case treecmp.RegMatch:
      return &tree.UnaryExpr{
        Operator: tree.MakeUnaryOperator(tree.UnaryComplement),
        Expr: expr,
      }, 0
    }
  }
  sqllex.Error(fmt.Sprintf("unknown unary operator %s", op))
  return nil, 1
}

%}

%{
// sqlSymType is generated by goyacc, and implements the ScanSymType interface.
var _ scanner.ScanSymType = &sqlSymType{}

func (s *sqlSymType) ID() int32 {
	return s.id
}

func (s *sqlSymType) SetID(id int32) {
	s.id = id
}

func (s *sqlSymType) Pos() int32 {
	return s.pos
}

func (s *sqlSymType) SetPos(pos int32) {
	s.pos = pos
}

func (s *sqlSymType) Str() string {
	return s.str
}

func (s *sqlSymType) SetStr(str string) {
	s.str = str
}

func (s *sqlSymType) UnionVal() interface{} {
	return s.union.val
}

func (s *sqlSymType) SetUnionVal(val interface{}) {
	s.union.val = val
}

// sqlSymUnion represents a union of types, providing accessor methods
// to retrieve the underlying type stored in the union's empty interface.
// The purpose of the sqlSymUnion struct is to reduce the memory footprint of
// the sqlSymType because only one value (of a variety of types) is ever needed
// to be stored in the union field at a time.
//
// By using an empty interface, we lose the type checking previously provided
// by yacc and the Go compiler when dealing with union values. Instead, runtime
// type assertions must be relied upon in the methods below, and as such, the
// parser should be thoroughly tested whenever new syntax is added.
//
// It is important to note that when assigning values to sqlSymUnion.val, all
// nil values should be typed so that they are stored as nil instances in the
// empty interface, instead of setting the empty interface to nil. This means
// that:
//     $$ = []String(nil)
// should be used, instead of:
//     $$ = nil
// to assign a nil string slice to the union.
type sqlSymUnion struct {
    val interface{}
}

// The following accessor methods come in three forms, depending on the
// type of the value being accessed and whether a nil value is admissible
// for the corresponding grammar rule.
// - Values and pointers are directly type asserted from the empty
//   interface, regardless of whether a nil value is admissible or
//   not. A panic occurs if the type assertion is incorrect; no panic occurs
//   if a nil is not expected but present. (TODO(knz): split this category of
//   accessor in two; with one checking for unexpected nils.)
//   Examples: bool(), tableIndexName().
//
// - Interfaces where a nil is admissible are handled differently
//   because a nil instance of an interface inserted into the empty interface
//   becomes a nil instance of the empty interface and therefore will fail a
//   direct type assertion. Instead, a guarded type assertion must be used,
//   which returns nil if the type assertion fails.
//   Examples: expr(), stmt().
//
// - Interfaces where a nil is not admissible are implemented as a direct
//   type assertion, which causes a panic to occur if an unexpected nil
//   is encountered.
//   Examples: tblDef().
//
func (u *sqlSymUnion) numVal() *tree.NumVal {
    return u.val.(*tree.NumVal)
}
func (u *sqlSymUnion) strVal() *tree.StrVal {
    if stmt, ok := u.val.(*tree.StrVal); ok {
        return stmt
    }
    return nil
}
func (u *sqlSymUnion) placeholder() *tree.Placeholder {
    return u.val.(*tree.Placeholder)
}
func (u *sqlSymUnion) auditMode() tree.AuditMode {
    return u.val.(tree.AuditMode)
}
func (u *sqlSymUnion) bool() bool {
    return u.val.(bool)
}
func (u *sqlSymUnion) strPtr() *string {
    return u.val.(*string)
}
func (u *sqlSymUnion) strs() []string {
    return u.val.([]string)
}
func (u *sqlSymUnion) roleSpec() tree.RoleSpec {
    return u.val.(tree.RoleSpec)
}
func (u *sqlSymUnion) roleSpecList() tree.RoleSpecList {
    return u.val.(tree.RoleSpecList)
}
func (u *sqlSymUnion) user() username.SQLUsername {
    return u.val.(username.SQLUsername)
}
func (u *sqlSymUnion) userPtr() *username.SQLUsername {
    return u.val.(*username.SQLUsername)
}
func (u *sqlSymUnion) users() []username.SQLUsername {
    return u.val.([]username.SQLUsername)
}
func (u *sqlSymUnion) newTableIndexName() *tree.TableIndexName {
    tn := u.val.(tree.TableIndexName)
    return &tn
}
func (u *sqlSymUnion) tableIndexName() tree.TableIndexName {
    return u.val.(tree.TableIndexName)
}
func (u *sqlSymUnion) newTableIndexNames() tree.TableIndexNames {
    return u.val.(tree.TableIndexNames)
}
func (u *sqlSymUnion) shardedIndexDef() *tree.ShardedIndexDef {
  return u.val.(*tree.ShardedIndexDef)
}
func (u *sqlSymUnion) nameList() tree.NameList {
    return u.val.(tree.NameList)
}
func (u *sqlSymUnion) enumValueList() tree.EnumValueList {
    return u.val.(tree.EnumValueList)
}
func (u *sqlSymUnion) compositeTypeList() []tree.CompositeTypeElem {
    return u.val.([]tree.CompositeTypeElem)
}
func (u *sqlSymUnion) unresolvedName() *tree.UnresolvedName {
    return u.val.(*tree.UnresolvedName)
}
func (u *sqlSymUnion) unresolvedObjectName() *tree.UnresolvedObjectName {
    return u.val.(*tree.UnresolvedObjectName)
}
func (u *sqlSymUnion) unresolvedObjectNames() []*tree.UnresolvedObjectName {
    return u.val.([]*tree.UnresolvedObjectName)
}
func (u *sqlSymUnion) tablePatterns() tree.TablePatterns {
    return u.val.(tree.TablePatterns)
}
func (u *sqlSymUnion) tableNames() tree.TableNames {
    return u.val.(tree.TableNames)
}
func (u *sqlSymUnion) indexFlags() *tree.IndexFlags {
    return u.val.(*tree.IndexFlags)
}
func (u *sqlSymUnion) arraySubscript() *tree.ArraySubscript {
    return u.val.(*tree.ArraySubscript)
}
func (u *sqlSymUnion) arraySubscripts() tree.ArraySubscripts {
    if as, ok := u.val.(tree.ArraySubscripts); ok {
        return as
    }
    return nil
}
func (u *sqlSymUnion) stmt() tree.Statement {
    if stmt, ok := u.val.(tree.Statement); ok {
        return stmt
    }
    return nil
}
func (u *sqlSymUnion) cte() *tree.CTE {
    if cte, ok := u.val.(*tree.CTE); ok {
        return cte
    }
    return nil
}
func (u *sqlSymUnion) ctes() []*tree.CTE {
    return u.val.([]*tree.CTE)
}
func (u *sqlSymUnion) with() *tree.With {
    if with, ok := u.val.(*tree.With); ok {
        return with
    }
    return nil
}
func (u *sqlSymUnion) slct() *tree.Select {
    return u.val.(*tree.Select)
}
func (u *sqlSymUnion) selectStmt() tree.SelectStatement {
    return u.val.(tree.SelectStatement)
}
func (u *sqlSymUnion) colTableDef() *tree.ColumnTableDef {
    return u.val.(*tree.ColumnTableDef)
}
func (u *sqlSymUnion) colDef() tree.ColumnDef {
    return u.val.(tree.ColumnDef)
}
func (u *sqlSymUnion) colDefList() tree.ColumnDefList {
    return u.val.(tree.ColumnDefList)
}
func (u *sqlSymUnion) constraintDef() tree.ConstraintTableDef {
    return u.val.(tree.ConstraintTableDef)
}
func (u *sqlSymUnion) tblDef() tree.TableDef {
    return u.val.(tree.TableDef)
}
func (u *sqlSymUnion) tblDefs() tree.TableDefs {
    return u.val.(tree.TableDefs)
}
func (u *sqlSymUnion) likeTableOption() tree.LikeTableOption {
    return u.val.(tree.LikeTableOption)
}
func (u *sqlSymUnion) likeTableOptionList() []tree.LikeTableOption {
    return u.val.([]tree.LikeTableOption)
}
func (u *sqlSymUnion) colQual() tree.NamedColumnQualification {
    return u.val.(tree.NamedColumnQualification)
}
func (u *sqlSymUnion) colQualElem() tree.ColumnQualification {
    return u.val.(tree.ColumnQualification)
}
func (u *sqlSymUnion) colQuals() []tree.NamedColumnQualification {
    return u.val.([]tree.NamedColumnQualification)
}
func (u *sqlSymUnion) storageParam() tree.StorageParam {
    return u.val.(tree.StorageParam)
}
func (u *sqlSymUnion) storageParams() []tree.StorageParam {
    if params, ok := u.val.([]tree.StorageParam); ok {
        return params
    }
    return nil
}
func (u *sqlSymUnion) storageParamKeys() []string {
    if params, ok := u.val.([]string); ok {
        return params
    }
    return nil
}
func (u *sqlSymUnion) tenantCapability() tree.TenantCapability {
    return u.val.(tree.TenantCapability)
}
func (u *sqlSymUnion) tenantCapabilities() []tree.TenantCapability {
    if capabilities, ok := u.val.([]tree.TenantCapability); ok {
        return capabilities
    }
    return nil
}
func (u *sqlSymUnion) persistence() tree.Persistence {
  return u.val.(tree.Persistence)
}
func (u *sqlSymUnion) colType() *types.T {
    if colType, ok := u.val.(*types.T); ok && colType != nil {
        return colType
    }
    return nil
}
func (u *sqlSymUnion) tableRefCols() []tree.ColumnID {
    if refCols, ok := u.val.([]tree.ColumnID); ok {
        return refCols
    }
    return nil
}
func (u *sqlSymUnion) colTypes() []*types.T {
    return u.val.([]*types.T)
}
func (u *sqlSymUnion) int32() int32 {
    return u.val.(int32)
}
func (u *sqlSymUnion) int64() int64 {
    return u.val.(int64)
}
func (u *sqlSymUnion) seqOpt() tree.SequenceOption {
    return u.val.(tree.SequenceOption)
}
func (u *sqlSymUnion) seqOpts() []tree.SequenceOption {
    return u.val.([]tree.SequenceOption)
}
func (u *sqlSymUnion) expr() tree.Expr {
    if expr, ok := u.val.(tree.Expr); ok {
        return expr
    }
    return nil
}
func (u *sqlSymUnion) exprs() tree.Exprs {
    return u.val.(tree.Exprs)
}
func (u *sqlSymUnion) selExpr() tree.SelectExpr {
    return u.val.(tree.SelectExpr)
}
func (u *sqlSymUnion) selExprs() tree.SelectExprs {
    return u.val.(tree.SelectExprs)
}
func (u *sqlSymUnion) retClause() tree.ReturningClause {
        return u.val.(tree.ReturningClause)
}
func (u *sqlSymUnion) aliasClause() tree.AliasClause {
    return u.val.(tree.AliasClause)
}
func (u *sqlSymUnion) asOfClause() tree.AsOfClause {
    return u.val.(tree.AsOfClause)
}
func (u *sqlSymUnion) tblExpr() tree.TableExpr {
    return u.val.(tree.TableExpr)
}
func (u *sqlSymUnion) tblExprs() tree.TableExprs {
    return u.val.(tree.TableExprs)
}
func (u *sqlSymUnion) from() tree.From {
    return u.val.(tree.From)
}
func (u *sqlSymUnion) batch() *tree.Batch {
    if batch, ok := u.val.(*tree.Batch); ok {
        return batch
    }
    return nil
}
func (u *sqlSymUnion) batchParam() tree.BatchParam {
    return u.val.(tree.BatchParam)
}
func (u *sqlSymUnion) batchParams() []tree.BatchParam {
    return u.val.([]tree.BatchParam)
}
func (u *sqlSymUnion) superRegion() tree.SuperRegion {
    return u.val.(tree.SuperRegion)
}
func (u *sqlSymUnion) int32s() []int32 {
    return u.val.([]int32)
}
func (u *sqlSymUnion) joinCond() tree.JoinCond {
    return u.val.(tree.JoinCond)
}
func (u *sqlSymUnion) when() *tree.When {
    return u.val.(*tree.When)
}
func (u *sqlSymUnion) whens() []*tree.When {
    return u.val.([]*tree.When)
}
func (u *sqlSymUnion) lockingClause() tree.LockingClause {
    return u.val.(tree.LockingClause)
}
func (u *sqlSymUnion) lockingItem() *tree.LockingItem {
    return u.val.(*tree.LockingItem)
}
func (u *sqlSymUnion) lockingStrength() tree.LockingStrength {
    return u.val.(tree.LockingStrength)
}
func (u *sqlSymUnion) lockingWaitPolicy() tree.LockingWaitPolicy {
    return u.val.(tree.LockingWaitPolicy)
}
func (u *sqlSymUnion) updateExpr() *tree.UpdateExpr {
    return u.val.(*tree.UpdateExpr)
}
func (u *sqlSymUnion) updateExprs() tree.UpdateExprs {
    return u.val.(tree.UpdateExprs)
}
func (u *sqlSymUnion) limit() *tree.Limit {
    return u.val.(*tree.Limit)
}
func (u *sqlSymUnion) backupTargetList() tree.BackupTargetList {
    return u.val.(tree.BackupTargetList)
}
func (u *sqlSymUnion) backupTargetListPtr() *tree.BackupTargetList {
    return u.val.(*tree.BackupTargetList)
}
func (u *sqlSymUnion) grantTargetList() tree.GrantTargetList {
    return u.val.(tree.GrantTargetList)
}
func (u *sqlSymUnion) grantTargetListPtr() *tree.GrantTargetList {
    return u.val.(*tree.GrantTargetList)
}
func (u *sqlSymUnion) changefeedTargets() tree.ChangefeedTargets {
    return u.val.(tree.ChangefeedTargets)
}
func (u *sqlSymUnion) changefeedTarget() tree.ChangefeedTarget {
    return u.val.(tree.ChangefeedTarget)
}
func (u *sqlSymUnion) privilegeType() privilege.Kind {
    return u.val.(privilege.Kind)
}
func (u *sqlSymUnion) privilegeList() privilege.List {
    return u.val.(privilege.List)
}
func (u *sqlSymUnion) onConflict() *tree.OnConflict {
    return u.val.(*tree.OnConflict)
}
func (u *sqlSymUnion) orderBy() tree.OrderBy {
    return u.val.(tree.OrderBy)
}
func (u *sqlSymUnion) order() *tree.Order {
    return u.val.(*tree.Order)
}
func (u *sqlSymUnion) orders() []*tree.Order {
    return u.val.([]*tree.Order)
}
func (u *sqlSymUnion) groupBy() tree.GroupBy {
    return u.val.(tree.GroupBy)
}
func (u *sqlSymUnion) windowFrame() *tree.WindowFrame {
    return u.val.(*tree.WindowFrame)
}
func (u *sqlSymUnion) windowFrameBounds() tree.WindowFrameBounds {
    return u.val.(tree.WindowFrameBounds)
}
func (u *sqlSymUnion) windowFrameBound() *tree.WindowFrameBound {
    return u.val.(*tree.WindowFrameBound)
}
func (u *sqlSymUnion) windowFrameExclusion() treewindow.WindowFrameExclusion {
    return u.val.(treewindow.WindowFrameExclusion)
}
func (u *sqlSymUnion) distinctOn() tree.DistinctOn {
    return u.val.(tree.DistinctOn)
}
func (u *sqlSymUnion) dir() tree.Direction {
    return u.val.(tree.Direction)
}
func (u *sqlSymUnion) nullsOrder() tree.NullsOrder {
    return u.val.(tree.NullsOrder)
}
func (u *sqlSymUnion) alterChangefeedCmd() tree.AlterChangefeedCmd {
    return u.val.(tree.AlterChangefeedCmd)
}
func (u *sqlSymUnion) alterChangefeedCmds() tree.AlterChangefeedCmds {
    return u.val.(tree.AlterChangefeedCmds)
}
func (u *sqlSymUnion) backupKMS() tree.BackupKMS {
    return u.val.(tree.BackupKMS)
}
func (u *sqlSymUnion) alterBackupCmd() tree.AlterBackupCmd {
    return u.val.(tree.AlterBackupCmd)
}
func (u *sqlSymUnion) alterBackupCmds() tree.AlterBackupCmds {
    return u.val.(tree.AlterBackupCmds)
}
func (u *sqlSymUnion) alterBackupScheduleCmd() tree.AlterBackupScheduleCmd {
    return u.val.(tree.AlterBackupScheduleCmd)
}
func (u *sqlSymUnion) alterBackupScheduleCmds() tree.AlterBackupScheduleCmds {
    return u.val.(tree.AlterBackupScheduleCmds)
}
func (u *sqlSymUnion) alterTableCmd() tree.AlterTableCmd {
    return u.val.(tree.AlterTableCmd)
}
func (u *sqlSymUnion) alterTableCmds() tree.AlterTableCmds {
    return u.val.(tree.AlterTableCmds)
}
func (u *sqlSymUnion) alterIndexCmd() tree.AlterIndexCmd {
    return u.val.(tree.AlterIndexCmd)
}
func (u *sqlSymUnion) alterIndexCmds() tree.AlterIndexCmds {
    return u.val.(tree.AlterIndexCmds)
}
func (u *sqlSymUnion) isoLevel() tree.IsolationLevel {
    return u.val.(tree.IsolationLevel)
}
func (u *sqlSymUnion) userPriority() tree.UserPriority {
    return u.val.(tree.UserPriority)
}
func (u *sqlSymUnion) readWriteMode() tree.ReadWriteMode {
    return u.val.(tree.ReadWriteMode)
}
func (u *sqlSymUnion) deferrableMode() tree.DeferrableMode {
    return u.val.(tree.DeferrableMode)
}
func (u *sqlSymUnion) idxElem() tree.IndexElem {
    return u.val.(tree.IndexElem)
}
func (u *sqlSymUnion) idxElems() tree.IndexElemList {
    return u.val.(tree.IndexElemList)
}
func (u *sqlSymUnion) indexInvisibility() tree.IndexInvisibility {
    return u.val.(tree.IndexInvisibility)
}
func (u *sqlSymUnion) dropBehavior() tree.DropBehavior {
    return u.val.(tree.DropBehavior)
}
func (u *sqlSymUnion) rlsTableMode() tree.TableRLSMode {
    return u.val.(tree.TableRLSMode)
}
func (u *sqlSymUnion) alterPolicy() tree.AlterPolicy {
    return u.val.(tree.AlterPolicy)
}
func (u *sqlSymUnion) createPolicy() tree.CreatePolicy {
    return u.val.(tree.CreatePolicy)
}
func (u *sqlSymUnion) policyType() tree.PolicyType {
  return u.val.(tree.PolicyType)
}
func (u *sqlSymUnion) policyCommand() tree.PolicyCommand {
  return u.val.(tree.PolicyCommand)
}
func (u *sqlSymUnion) policyExpressions() tree.PolicyExpressions {
	return u.val.(tree.PolicyExpressions)
}
func (u *sqlSymUnion) validationBehavior() tree.ValidationBehavior {
    return u.val.(tree.ValidationBehavior)
}
func (u *sqlSymUnion) partitionBy() *tree.PartitionBy {
    return u.val.(*tree.PartitionBy)
}
func (u *sqlSymUnion) partitionByTable() *tree.PartitionByTable {
    return u.val.(*tree.PartitionByTable)
}
func (u *sqlSymUnion) partitionByIndex() *tree.PartitionByIndex {
    return u.val.(*tree.PartitionByIndex)
}
func (u *sqlSymUnion) createTableOnCommitSetting() tree.CreateTableOnCommitSetting {
    return u.val.(tree.CreateTableOnCommitSetting)
}
func (u *sqlSymUnion) listPartition() tree.ListPartition {
    return u.val.(tree.ListPartition)
}
func (u *sqlSymUnion) listPartitions() []tree.ListPartition {
    return u.val.([]tree.ListPartition)
}
func (u *sqlSymUnion) rangePartition() tree.RangePartition {
    return u.val.(tree.RangePartition)
}
func (u *sqlSymUnion) rangePartitions() []tree.RangePartition {
    return u.val.([]tree.RangePartition)
}
func (u *sqlSymUnion) relocateSubject() tree.RelocateSubject {
    return u.val.(tree.RelocateSubject)
}
func (u *sqlSymUnion) setZoneConfig() *tree.SetZoneConfig {
    return u.val.(*tree.SetZoneConfig)
}
func (u *sqlSymUnion) tuples() []*tree.Tuple {
    return u.val.([]*tree.Tuple)
}
func (u *sqlSymUnion) tuple() *tree.Tuple {
    return u.val.(*tree.Tuple)
}
func (u *sqlSymUnion) windowDef() *tree.WindowDef {
    return u.val.(*tree.WindowDef)
}
func (u *sqlSymUnion) window() tree.Window {
    return u.val.(tree.Window)
}
func (u *sqlSymUnion) op() tree.Operator {
    return u.val.(tree.Operator)
}
func (u *sqlSymUnion) cmpOp() treecmp.ComparisonOperator {
    return u.val.(treecmp.ComparisonOperator)
}
func (u *sqlSymUnion) intervalTypeMetadata() types.IntervalTypeMetadata {
    return u.val.(types.IntervalTypeMetadata)
}
func (u *sqlSymUnion) kvOption() tree.KVOption {
    return u.val.(tree.KVOption)
}
func (u *sqlSymUnion) kvOptions() []tree.KVOption {
    if colType, ok := u.val.([]tree.KVOption); ok {
        return colType
    }
    return nil
}
func (u *sqlSymUnion) backupOptions() *tree.BackupOptions {
  return u.val.(*tree.BackupOptions)
}
func (u *sqlSymUnion) copyOptions() *tree.CopyOptions {
  return u.val.(*tree.CopyOptions)
}
func (u *sqlSymUnion) showJobOptions() *tree.ShowJobOptions {
  return u.val.(*tree.ShowJobOptions)
}
func (u *sqlSymUnion) showBackupDetails() tree.ShowBackupDetails {
  return u.val.(tree.ShowBackupDetails)
}
func (u *sqlSymUnion) showBackupOptions() *tree.ShowBackupOptions {
  return u.val.(*tree.ShowBackupOptions)
}
func (u *sqlSymUnion) checkExternalConnectionOptions() *tree.CheckExternalConnectionOptions {
  return u.val.(*tree.CheckExternalConnectionOptions)
}
func (u *sqlSymUnion) restoreOptions() *tree.RestoreOptions {
  return u.val.(*tree.RestoreOptions)
}
func (u *sqlSymUnion) transactionModes() tree.TransactionModes {
    return u.val.(tree.TransactionModes)
}
func (u *sqlSymUnion) compositeKeyMatchMethod() tree.CompositeKeyMatchMethod {
  return u.val.(tree.CompositeKeyMatchMethod)
}
func (u *sqlSymUnion) referenceAction() tree.ReferenceAction {
    return u.val.(tree.ReferenceAction)
}
func (u *sqlSymUnion) referenceActions() tree.ReferenceActions {
    return u.val.(tree.ReferenceActions)
}
func (u *sqlSymUnion) createStatsOptions() *tree.CreateStatsOptions {
    return u.val.(*tree.CreateStatsOptions)
}
func (u *sqlSymUnion) scrubOptions() tree.ScrubOptions {
    return u.val.(tree.ScrubOptions)
}
func (u *sqlSymUnion) scrubOption() tree.ScrubOption {
    return u.val.(tree.ScrubOption)
}
func (u *sqlSymUnion) resolvableFuncRefFromName() tree.ResolvableFunctionReference {
    return tree.ResolvableFunctionReference{FunctionReference: u.unresolvedName()}
}
func (u *sqlSymUnion) resolvableFuncRef() tree.ResolvableFunctionReference {
    return u.val.(tree.ResolvableFunctionReference)
}
func (u *sqlSymUnion) rowsFromExpr() *tree.RowsFromExpr {
    return u.val.(*tree.RowsFromExpr)
}
func (u *sqlSymUnion) stringOrPlaceholderOptList() tree.StringOrPlaceholderOptList {
    return u.val.(tree.StringOrPlaceholderOptList)
}
func (u *sqlSymUnion) listOfStringOrPlaceholderOptList() []tree.StringOrPlaceholderOptList {
    return u.val.([]tree.StringOrPlaceholderOptList)
}
func (u *sqlSymUnion) fullBackupClause() *tree.FullBackupClause {
    return u.val.(*tree.FullBackupClause)
}
func (u *sqlSymUnion) scheduleLabelSpec() *tree.LabelSpec {
    return u.val.(*tree.LabelSpec)
}
func (u *sqlSymUnion) labelSpec() *tree.LabelSpec {
    return u.val.(*tree.LabelSpec)
}

func (u *sqlSymUnion) geoShapeType() geopb.ShapeType {
  return u.val.(geopb.ShapeType)
}
func newNameFromStr(s string) *tree.Name {
    return (*tree.Name)(&s)
}
func (u *sqlSymUnion) typeReference() tree.ResolvableTypeReference {
    return u.val.(tree.ResolvableTypeReference)
}
func (u *sqlSymUnion) typeReferences() []tree.ResolvableTypeReference {
    return u.val.([]tree.ResolvableTypeReference)
}
func (u *sqlSymUnion) alterTypeAddValuePlacement() *tree.AlterTypeAddValuePlacement {
    return u.val.(*tree.AlterTypeAddValuePlacement)
}
func (u *sqlSymUnion) scheduleState() tree.ScheduleState {
  return u.val.(tree.ScheduleState)
}
func (u *sqlSymUnion) executorType() tree.ScheduledJobExecutorType {
  return u.val.(tree.ScheduledJobExecutorType)
}
func (u *sqlSymUnion) refreshDataOption() tree.RefreshDataOption {
  return u.val.(tree.RefreshDataOption)
}
func (u *sqlSymUnion) locality() *tree.Locality {
  return u.val.(*tree.Locality)
}
func (u *sqlSymUnion) survivalGoal() tree.SurvivalGoal {
  return u.val.(tree.SurvivalGoal)
}
func (u *sqlSymUnion) dataPlacement() tree.DataPlacement {
  return u.val.(tree.DataPlacement)
}
func (u *sqlSymUnion) objectNamePrefix() tree.ObjectNamePrefix {
	return u.val.(tree.ObjectNamePrefix)
}
func (u *sqlSymUnion) objectNamePrefixList() tree.ObjectNamePrefixList {
    return u.val.(tree.ObjectNamePrefixList)
}
func (u *sqlSymUnion) abbreviatedGrant() tree.AbbreviatedGrant {
  return u.val.(tree.AbbreviatedGrant)
}
func (u *sqlSymUnion) abbreviatedRevoke() tree.AbbreviatedRevoke {
  return u.val.(tree.AbbreviatedRevoke)
}
func (u *sqlSymUnion) targetObjectType() privilege.TargetObjectType {
  return u.val.(privilege.TargetObjectType)
}
func (u *sqlSymUnion) setVar() *tree.SetVar {
    return u.val.(*tree.SetVar)
}
func (u *sqlSymUnion) cursorSensitivity() tree.CursorSensitivity {
    return u.val.(tree.CursorSensitivity)
}
func (u *sqlSymUnion) cursorScrollOption() tree.CursorScrollOption {
    return u.val.(tree.CursorScrollOption)
}
func (u *sqlSymUnion) cursorStmt() tree.CursorStmt {
    return u.val.(tree.CursorStmt)
}
func (u *sqlSymUnion) asTenantClause() tree.TenantID {
    return u.val.(tree.TenantID)
}
func (u *sqlSymUnion) routineOptions() tree.RoutineOptions {
    return u.val.(tree.RoutineOptions)
}
func (u *sqlSymUnion) functionOption() tree.RoutineOption {
    return u.val.(tree.RoutineOption)
}
func (u *sqlSymUnion) routineParams() tree.RoutineParams {
    return u.val.(tree.RoutineParams)
}
func (u *sqlSymUnion) routineParam() tree.RoutineParam {
    return u.val.(tree.RoutineParam)
}
func (u *sqlSymUnion) routineParamClass() tree.RoutineParamClass {
    return u.val.(tree.RoutineParamClass)
}
func (u *sqlSymUnion) stmts() tree.Statements {
    return u.val.(tree.Statements)
}
func (u *sqlSymUnion) routineBody() *tree.RoutineBody {
    return u.val.(*tree.RoutineBody)
}
func (u *sqlSymUnion) functionObj() tree.RoutineObj {
    return u.val.(tree.RoutineObj)
}
func (u *sqlSymUnion) routineObjs() tree.RoutineObjs {
    return u.val.(tree.RoutineObjs)
}
func (u *sqlSymUnion) tenantReplicationOptions() *tree.TenantReplicationOptions {
  return u.val.(*tree.TenantReplicationOptions)
}
func (u *sqlSymUnion) showRangesOpts() *tree.ShowRangesOptions {
    return u.val.(*tree.ShowRangesOptions)
}
func (u *sqlSymUnion) tenantSpec() *tree.TenantSpec {
    return u.val.(*tree.TenantSpec)
}
func (u *sqlSymUnion) cteMaterializeClause() tree.CTEMaterializeClause {
    return u.val.(tree.CTEMaterializeClause)
}
func (u *sqlSymUnion) showTenantOpts() tree.ShowTenantOptions {
    return u.val.(tree.ShowTenantOptions)
}
func (u *sqlSymUnion) showLogicalReplicationJobsOpts() tree.ShowLogicalReplicationJobsOptions {
		return u.val.(tree.ShowLogicalReplicationJobsOptions)
}
func (u *sqlSymUnion) showCreateFormatOption() tree.ShowCreateFormatOption {
    return u.val.(tree.ShowCreateFormatOption)
}
func (u *sqlSymUnion) beginTransaction() *tree.BeginTransaction {
    return u.val.(*tree.BeginTransaction)
}
func (u *sqlSymUnion) showFingerprintOptions() *tree.ShowFingerprintOptions {
    return u.val.(*tree.ShowFingerprintOptions)
}
func (u *sqlSymUnion) logicalReplicationResources() tree.LogicalReplicationResources {
    return u.val.(tree.LogicalReplicationResources)
}
func (u *sqlSymUnion) logicalReplicationOptions() *tree.LogicalReplicationOptions {
  return u.val.(*tree.LogicalReplicationOptions)
}
func (u *sqlSymUnion) triggerActionTime() tree.TriggerActionTime {
  return u.val.(tree.TriggerActionTime)
}
func (u *sqlSymUnion) triggerEvent() *tree.TriggerEvent {
  return u.val.(*tree.TriggerEvent)
}
func (u *sqlSymUnion) triggerEvents() []*tree.TriggerEvent {
  return u.val.([]*tree.TriggerEvent)
}
func (u *sqlSymUnion) triggerTransition() *tree.TriggerTransition {
  return u.val.(*tree.TriggerTransition)
}
func (u *sqlSymUnion) triggerTransitions() []*tree.TriggerTransition {
  return u.val.([]*tree.TriggerTransition)
}
func (u *sqlSymUnion) triggerForEach() tree.TriggerForEach {
  return u.val.(tree.TriggerForEach)
}
func (u *sqlSymUnion) indexType() idxtype.T {
  return u.val.(idxtype.T)
}
func (u *sqlSymUnion) doBlockOptions() tree.DoBlockOptions {
    return u.val.(tree.DoBlockOptions)
}
func (u *sqlSymUnion) doBlockOption() tree.DoBlockOption {
    return u.val.(tree.DoBlockOption)
}
%}

// NB: the %token definitions must come before the %type definitions in this
// file to work around a bug in goyacc. See #16369 for more details.

// Non-keyword token types.
%token <str> IDENT SCONST BCONST BITCONST
%token <*tree.NumVal> ICONST FCONST
%token <*tree.Placeholder> PLACEHOLDER
%token <str> TYPECAST TYPEANNOTATE DOT_DOT
%token <str> LESS_EQUALS GREATER_EQUALS NOT_EQUALS
%token <str> NOT_REGMATCH REGIMATCH NOT_REGIMATCH
%token <str> ERROR

// If you want to make any keyword changes, add the new keyword here as well as
// to the appropriate one of the reserved-or-not-so-reserved keyword lists,
// below; search this file for "Keyword category lists".

// Ordinary key words in alphabetical order.
%token <str> ABORT ABSOLUTE ACCESS ACTION ADD ADMIN AFTER AGGREGATE
%token <str> ALL ALTER ALWAYS ANALYSE ANALYZE AND AND_AND ANY ANNOTATE_TYPE ARRAY AS ASC AS_JSON AT_AT
%token <str> ASENSITIVE ASYMMETRIC AT ATOMIC ATTRIBUTE AUTHORIZATION AUTOMATIC AVAILABILITY AVOID_FULL_SCAN

%token <str> BACKUP BACKUPS BACKWARD BATCH BEFORE BEGIN BETWEEN BIDIRECTIONAL BIGINT BIGSERIAL BINARY BIT
%token <str> BUCKET_COUNT
%token <str> BOOLEAN BOTH BOX2D BUNDLE BY BYPASSRLS

%token <str> CACHE CALL CALLED CANCEL CANCELQUERY CAPABILITIES CAPABILITY CASCADE CASE CAST CBRT CHANGEFEED CHAR
%token <str> CHARACTER CHARACTERISTICS CHECK CHECK_FILES CLOSE
%token <str> CLUSTER CLUSTERS COALESCE COLLATE COLLATION COLUMN COLUMNS COMMENT COMMENTS COMMIT
%token <str> COMMITTED COMPACT COMPLETE COMPLETIONS CONCAT CONCURRENTLY CONFIGURATION CONFIGURATIONS CONFIGURE
%token <str> CONFLICT CONNECTION CONNECTIONS CONSTRAINT CONSTRAINTS CONTAINS CONTROLCHANGEFEED CONTROLJOB
%token <str> CONVERSION CONVERT COPY COS_DISTANCE COST COVERING CREATE CREATEDB CREATELOGIN CREATEROLE
%token <str> CROSS CSV CUBE CURRENT CURRENT_CATALOG CURRENT_DATE CURRENT_SCHEMA
%token <str> CURRENT_ROLE CURRENT_TIME CURRENT_TIMESTAMP
%token <str> CURRENT_USER CURSOR CYCLE

%token <str> DATA DATABASE DATABASES DATE DAY DEBUG_IDS DEC DECIMAL DEFAULT DEFAULTS DEFINER
%token <str> DEALLOCATE DECLARE DEFERRABLE DEFERRED DELETE DELIMITER DEPENDS DESC DESTINATION DETACHED DETAILS
%token <str> DISABLE DISCARD DISTANCE DISTINCT DO DOMAIN DOUBLE DROP

%token <str> EACH ELSE ENABLE ENCODING ENCRYPTED ENCRYPTION_INFO_DIR ENCRYPTION_PASSPHRASE END ENUM ENUMS ESCAPE EXCEPT EXCLUDE EXCLUDING
%token <str> EXISTS EXECUTE EXECUTION EXPERIMENTAL
%token <str> EXPERIMENTAL_FINGERPRINTS EXPERIMENTAL_REPLICA
%token <str> EXPERIMENTAL_AUDIT EXPERIMENTAL_RELOCATE
%token <str> EXPIRATION EXPLAIN EXPORT EXTENSION EXTERNAL EXTRACT EXTRACT_DURATION EXTREMES

%token <str> FAILURE FALSE FAMILY FETCH FETCHVAL FETCHTEXT FETCHVAL_PATH FETCHTEXT_PATH
%token <str> FILES FILTER
%token <str> FIRST FLOAT FLOAT4 FLOAT8 FLOORDIV FOLLOWING FOR FORCE FORCE_INDEX FORCE_INVERTED_INDEX
%token <str> FORCE_NOT_NULL FORCE_NULL FORCE_QUOTE FORCE_ZIGZAG
%token <str> FOREIGN FORMAT FORWARD FREEZE FROM FULL FUNCTION FUNCTIONS

%token <str> GENERATED GEOGRAPHY GEOMETRY GEOMETRYM GEOMETRYZ GEOMETRYZM
%token <str> GEOMETRYCOLLECTION GEOMETRYCOLLECTIONM GEOMETRYCOLLECTIONZ GEOMETRYCOLLECTIONZM
%token <str> GLOBAL GOAL GRANT GRANTEE GRANTS GREATEST GROUP GROUPING GROUPS

%token <str> HAVING HASH HEADER HIGH HISTOGRAM HOLD HOUR

%token <str> IDENTITY
%token <str> IF IFERROR IFNULL IGNORE_FOREIGN_KEYS ILIKE IMMEDIATE IMMEDIATELY IMMUTABLE IMPORT IN INCLUDE
%token <str> INCLUDING INCLUDE_ALL_SECONDARY_TENANTS INCLUDE_ALL_VIRTUAL_CLUSTERS INCREMENT INCREMENTAL INCREMENTAL_LOCATION
%token <str> INET INET_CONTAINED_BY_OR_EQUALS
%token <str> INET_CONTAINS_OR_EQUALS INDEX INDEXES INHERITS INJECT INITIALLY
%token <str> INDEX_BEFORE_PAREN INDEX_BEFORE_NAME_THEN_PAREN INDEX_AFTER_ORDER_BY_BEFORE_AT
%token <str> INNER INOUT INPUT INSENSITIVE INSERT INSTEAD INT INTEGER
%token <str> INTERSECT INTERVAL INTO INTO_DB INVERTED INVOKER IS ISERROR ISNULL ISOLATION

%token <str> JOB JOBS JOIN JSON JSONB JSON_SOME_EXISTS JSON_ALL_EXISTS

%token <str> KEY KEYS KMS KV

%token <str> LABEL LANGUAGE LAST LATERAL LATEST LC_CTYPE LC_COLLATE
%token <str> LEADING LEASE LEAST LEAKPROOF LEFT LESS LEVEL LIKE LIMIT
%token <str> LINESTRING LINESTRINGM LINESTRINGZ LINESTRINGZM
%token <str> LIST LOCAL LOCALITY LOCALTIME LOCALTIMESTAMP LOCKED LOGICAL LOGICALLY LOGIN LOOKUP LOW LSHIFT

%token <str> MATCH MATERIALIZED MERGE MINVALUE MAXVALUE METHOD MINUTE MODIFYCLUSTERSETTING MODIFYSQLCLUSTERSETTING MODE MONTH MOVE
%token <str> MULTILINESTRING MULTILINESTRINGM MULTILINESTRINGZ MULTILINESTRINGZM
%token <str> MULTIPOINT MULTIPOINTM MULTIPOINTZ MULTIPOINTZM
%token <str> MULTIPOLYGON MULTIPOLYGONM MULTIPOLYGONZ MULTIPOLYGONZM

%token <str> NAN NAME NAMES NATURAL NEG_INNER_PRODUCT NEVER NEW NEW_DB_NAME NEW_KMS NEXT NO NOBYPASSRLS NOCANCELQUERY NOCONTROLCHANGEFEED
%token <str> NOCONTROLJOB NOCREATEDB NOCREATELOGIN NOCREATEROLE NODE NOLOGIN NOMODIFYCLUSTERSETTING NOREPLICATION
%token <str> NOSQLLOGIN NO_INDEX_JOIN NO_ZIGZAG_JOIN NO_FULL_SCAN NONE NONVOTERS NORMAL NOT
%token <str> NOTHING NOTHING_AFTER_RETURNING
%token <str> NOTNULL
%token <str> NOVIEWACTIVITY NOVIEWACTIVITYREDACTED NOVIEWCLUSTERSETTING NOWAIT NULL NULLIF NULLS NUMERIC

%token <str> OF OFF OFFSET OID OIDS OIDVECTOR OLD OLD_KMS ON ONLY OPT OPTION OPTIONS OR
%token <str> ORDER ORDINALITY OTHERS OUT OUTER OVER OVERLAPS OVERLAY OWNED OWNER OPERATOR

%token <str> PARALLEL PARENT PARTIAL PARTITION PARTITIONS PASSWORD PAUSE PAUSED PER PERMISSIVE PHYSICAL PLACEMENT PLACING
%token <str> PLAN PLANS POINT POINTM POINTZ POINTZM POLICIES POLICY POLYGON POLYGONM POLYGONZ POLYGONZM
%token <str> POSITION PRECEDING PRECISION PREPARE PREPARED PRESERVE PRIMARY PRIOR PRIORITY PRIVILEGES
%token <str> PROCEDURAL PROCEDURE PROCEDURES PUBLIC PUBLICATION

%token <str> QUERIES QUERY QUOTE

%token <str> RANGE RANGES READ REAL REASON REASSIGN RECURSIVE RECURRING REDACT REF REFERENCES REFERENCING REFRESH
%token <str> REGCLASS REGION REGIONAL REGIONS REGNAMESPACE REGPROC REGPROCEDURE REGROLE REGTYPE REINDEX
%token <str> RELATIVE RELOCATE REMOVE_PATH REMOVE_REGIONS RENAME REPEATABLE REPLACE REPLICATED REPLICATION
%token <str> RELEASE RESET RESTART RESTORE RESTRICT RESTRICTED RESTRICTIVE RESUME RETENTION RETURNING RETURN RETURNS RETRY REVISION_HISTORY
%token <str> REVOKE RIGHT ROLE ROLES ROLLBACK ROLLUP ROUTINES ROW ROWS RSHIFT RULE RUNNING

%token <str> SAVEPOINT SCANS SCATTER SCHEDULE SCHEDULES SCROLL SCHEMA SCHEMA_ONLY SCHEMAS SCRUB
%token <str> SEARCH SECOND SECONDARY SECURITY SELECT SEQUENCE SEQUENCES
%token <str> SERIALIZABLE SERVER SERVICE SESSION SESSIONS SESSION_USER SET SETOF SETS SETTING SETTINGS
%token <str> SHARE SHARED SHOW SIMILAR SIMPLE SIZE SKIP SKIP_LOCALITIES_CHECK SKIP_MISSING_FOREIGN_KEYS
%token <str> SKIP_MISSING_SEQUENCES SKIP_MISSING_SEQUENCE_OWNERS SKIP_MISSING_VIEWS SKIP_MISSING_UDFS SMALLINT SMALLSERIAL
%token <str> SNAPSHOT SOME SPLIT SQL SQLLOGIN
%token <str> STABLE START STATE STATEMENT STATISTICS STATUS STDIN STDOUT STOP STRAIGHT STREAM STRICT STRING STORAGE STORE STORED STORING SUBJECT SUBSTRING SUPER
%token <str> SUPPORT SURVIVE SURVIVAL SYMMETRIC SYNTAX SYSTEM SQRT SUBSCRIPTION STATEMENTS

%token <str> TABLE TABLES TABLESPACE TEMP TEMPLATE TEMPORARY TENANT TENANT_NAME TENANTS TESTING_RELOCATE TEXT THEN
%token <str> TIES TIME TIMETZ TIMESTAMP TIMESTAMPTZ TO THROTTLING TRAILING TRACE
%token <str> TRANSACTION TRANSACTIONS TRANSFER TRANSFORM TREAT TRIGGER TRIGGERS TRIM TRUE
%token <str> TRUNCATE TRUSTED TYPE TYPES
%token <str> TRACING

%token <str> UNBOUNDED UNCOMMITTED UNIDIRECTIONAL UNION UNIQUE UNKNOWN UNLISTEN UNLOGGED UNSAFE_RESTORE_INCOMPATIBLE_VERSION UNSPLIT
%token <str> UPDATE UPDATES_CLUSTER_MONITORING_METRICS UPSERT UNSET UNTIL USE USER USERS USING UUID

%token <str> VALID VALIDATE VALUE VALUES VARBIT VARCHAR VARIADIC VECTOR VERIFY_BACKUP_TABLE_DATA VIEW VARIABLES VARYING VIEWACTIVITY VIEWACTIVITYREDACTED VIEWDEBUG
%token <str> VIEWCLUSTERMETADATA VIEWCLUSTERSETTING VIRTUAL VISIBLE INVISIBLE VISIBILITY VOLATILE VOTERS
%token <str> VIRTUAL_CLUSTER_NAME VIRTUAL_CLUSTER

%token <str> WHEN WHERE WINDOW WITH WITHIN WITHOUT WORK WRITE

%token <str> YEAR

%token <str> ZONE

// The grammar thinks these are keywords, but they are not in any category
// and so can never be entered directly. The filter in scan.go creates these
// tokens when required (based on looking one token ahead).
// Reference: pkg/sql/parser/lexer.go
//
// - NOT_LA exists so that productions such as NOT LIKE can be given the same
// precedence as LIKE; otherwise they'd effectively have the same precedence as
// NOT, at least with respect to their left-hand subexpression.
// - WITH_LA is needed to make the grammar LALR(1).
// - GENERATED_ALWAYS is needed to support the Postgres syntax for computed
// columns along with our family related extensions (CREATE FAMILY/CREATE FAMILY
// family_name).
// - RESET_ALL is used to differentiate `RESET var` from `RESET ALL`.
// - ROLE_ALL and USER_ALL are used in ALTER ROLE and SHOW DEFAULT SESSION VARIABLES FOR ROLE statements that affect all
// roles.
// - ON_LA is needed for ON UPDATE and ON DELETE expressions for foreign key
// references.
// - TENANT_ALL is used to differentiate `ALTER TENANT <id>` from
// `ALTER TENANT ALL`. Ditto `CLUSTER_ALL` and `CLUSTER ALL`.
%token NOT_LA NULLS_LA WITH_LA AS_LA GENERATED_ALWAYS GENERATED_BY_DEFAULT RESET_ALL ROLE_ALL
%token USER_ALL ON_LA TENANT_ALL CLUSTER_ALL SET_TRACING

%union {
  id    int32
  pos   int32
  str   string
  union sqlSymUnion
}

%type <tree.Statement> stmt_block
%type <tree.Statement> stmt stmt_without_legacy_transaction


%type <tree.Statement> alter_stmt
%type <tree.Statement> alter_changefeed_stmt
%type <tree.Statement> alter_backup_stmt
%type <tree.Statement> alter_ddl_stmt
%type <tree.Statement> alter_table_stmt
%type <tree.Statement> alter_index_stmt
%type <tree.Statement> alter_view_stmt
%type <tree.Statement> alter_sequence_stmt
%type <tree.Statement> alter_database_stmt
%type <tree.Statement> alter_range_stmt
%type <tree.Statement> alter_partition_stmt
%type <tree.Statement> alter_role_stmt
%type <*tree.SetVar> set_or_reset_clause
%type <tree.Statement> alter_type_stmt
%type <tree.Statement> alter_schema_stmt
%type <tree.Statement> alter_unsupported_stmt
%type <tree.Statement> alter_func_stmt
%type <tree.Statement> alter_proc_stmt
%type <tree.Statement> alter_policy_stmt

// ALTER RANGE
%type <tree.Statement> alter_zone_range_stmt
%type <tree.Statement> alter_range_relocate_stmt

// ALTER TABLE
%type <tree.Statement> alter_onetable_stmt
%type <tree.Statement> alter_split_stmt
%type <tree.Statement> alter_unsplit_stmt
%type <tree.Statement> alter_rename_table_stmt
%type <tree.Statement> alter_scatter_stmt
%type <tree.Statement> alter_relocate_stmt
%type <tree.Statement> alter_zone_table_stmt
%type <tree.Statement> alter_table_set_schema_stmt
%type <tree.Statement> alter_table_locality_stmt
%type <tree.Statement> alter_table_owner_stmt

// ALTER VIRTUAL CLUSTER
%type <tree.Statement> alter_virtual_cluster_stmt

// ALTER VIRTUAL CLUSTER CAPABILITY
%type <tree.Statement> virtual_cluster_capability virtual_cluster_capability_list

// ALTER VIRTUAL CLUSTER SET CLUSTER SETTING
%type <tree.Statement> alter_virtual_cluster_csetting_stmt

// ALTER VIRTUAL CLUSTER CAPABILITY
%type <tree.Statement> alter_virtual_cluster_capability_stmt

// Other ALTER VIRTUAL CLUSTER statements.
%type <tree.Statement> alter_virtual_cluster_replication_stmt
%type <tree.Statement> alter_virtual_cluster_rename_stmt
%type <tree.Statement> alter_virtual_cluster_reset_stmt
%type <tree.Statement> alter_virtual_cluster_service_stmt

// ALTER PARTITION
%type <tree.Statement> alter_zone_partition_stmt

// ALTER DATABASE
%type <tree.Statement> alter_rename_database_stmt
%type <tree.Statement> alter_database_to_schema_stmt
%type <tree.Statement> alter_database_add_region_stmt
%type <tree.Statement> alter_database_drop_region_stmt
%type <tree.Statement> alter_database_survival_goal_stmt
%type <tree.Statement> alter_database_primary_region_stmt
%type <tree.Statement> alter_zone_database_stmt
%type <tree.Statement> alter_database_owner
%type <tree.Statement> alter_database_placement_stmt
%type <tree.Statement> alter_database_set_stmt
%type <tree.Statement> alter_database_add_super_region
%type <tree.Statement> alter_database_alter_super_region
%type <tree.Statement> alter_database_drop_super_region
%type <tree.Statement> alter_database_set_secondary_region_stmt
%type <tree.Statement> alter_database_drop_secondary_region
%type <tree.Statement> alter_database_set_zone_config_extension_stmt

// ALTER INDEX
%type <tree.Statement> alter_oneindex_stmt
%type <tree.Statement> alter_scatter_index_stmt
%type <tree.Statement> alter_split_index_stmt
%type <tree.Statement> alter_unsplit_index_stmt
%type <tree.Statement> alter_rename_index_stmt
%type <tree.Statement> alter_relocate_index_stmt
%type <tree.Statement> alter_zone_index_stmt
%type <tree.Statement> alter_index_visible_stmt

// ALTER VIEW
%type <tree.Statement> alter_rename_view_stmt
%type <tree.Statement> alter_view_set_schema_stmt
%type <tree.Statement> alter_view_owner_stmt

// ALTER SEQUENCE
%type <tree.Statement> alter_rename_sequence_stmt
%type <tree.Statement> alter_sequence_options_stmt
%type <tree.Statement> alter_sequence_set_schema_stmt
%type <tree.Statement> alter_sequence_owner_stmt

// ALTER DEFAULT PRIVILEGES
%type <tree.Statement> alter_default_privileges_stmt

// ALTER FUNCTION
%type <tree.Statement> alter_func_options_stmt
%type <tree.Statement> alter_func_rename_stmt
%type <tree.Statement> alter_func_set_schema_stmt
%type <tree.Statement> alter_func_owner_stmt
%type <tree.Statement> alter_func_dep_extension_stmt

// ALTER PROCEDURE
%type <tree.Statement> alter_proc_rename_stmt
%type <tree.Statement> alter_proc_set_schema_stmt
%type <tree.Statement> alter_proc_owner_stmt

%type <tree.Statement> backup_stmt
%type <tree.Statement> begin_stmt

%type <tree.Statement> call_stmt

%type <tree.Statement> cancel_stmt
%type <tree.Statement> cancel_jobs_stmt
%type <tree.Statement> cancel_queries_stmt
%type <tree.Statement> cancel_sessions_stmt
%type <tree.Statement> cancel_all_jobs_stmt

// SCRUB
%type <tree.Statement> scrub_stmt
%type <tree.Statement> scrub_database_stmt
%type <tree.Statement> scrub_table_stmt
%type <tree.ScrubOptions> opt_scrub_options_clause
%type <tree.ScrubOptions> scrub_option_list
%type <tree.ScrubOption> scrub_option

%type <tree.Statement> comment_stmt
%type <tree.Statement> commit_stmt
%type <tree.Statement> copy_stmt

%type <tree.Statement> create_stmt
%type <tree.Statement> create_schedule_stmt
%type <tree.Statement> create_changefeed_stmt create_schedule_for_changefeed_stmt
%type <tree.Statement> create_ddl_stmt
%type <tree.Statement> create_database_stmt
%type <tree.Statement> create_extension_stmt
%type <tree.Statement> create_external_connection_stmt
%type <tree.Statement> create_index_stmt
%type <tree.Statement> create_role_stmt
%type <tree.Statement> create_schedule_for_backup_stmt
%type <tree.Statement> alter_backup_schedule
%type <tree.Statement> create_schema_stmt
%type <tree.Statement> create_table_stmt
%type <tree.Statement> create_table_as_stmt
%type <tree.Statement> create_virtual_cluster_stmt
%type <tree.Statement> create_logical_replication_stream_stmt
%type <tree.Statement> create_view_stmt
%type <tree.Statement> create_sequence_stmt
%type <tree.Statement> create_func_stmt
%type <tree.Statement> create_proc_stmt
%type <tree.Statement> create_trigger_stmt
%type <tree.Statement> create_policy_stmt

%type <tree.Statement> check_stmt
%type <tree.Statement> check_external_connection_stmt

%type <tree.LogicalReplicationResources> logical_replication_resources logical_replication_resources_list
%type <*tree.LogicalReplicationOptions> opt_logical_replication_options logical_replication_options logical_replication_options_list opt_logical_replication_create_table_options logical_replication_create_table_options logical_replication_create_table_options_list

%type <tree.Statement> create_stats_stmt
%type <*tree.CreateStatsOptions> opt_create_stats_options
%type <*tree.CreateStatsOptions> create_stats_option_list
%type <*tree.CreateStatsOptions> create_stats_option
%type <*tree.CheckExternalConnectionOptions> opt_with_check_external_connection_options_list check_external_connection_options_list check_external_connection_options

%type <tree.Statement> create_type_stmt
%type <tree.Statement> delete_stmt
%type <tree.Statement> discard_stmt

%type <tree.Statement> drop_stmt
%type <tree.Statement> drop_ddl_stmt
%type <tree.Statement> drop_database_stmt
%type <tree.Statement> drop_external_connection_stmt
%type <tree.Statement> drop_index_stmt
%type <tree.Statement> drop_role_stmt
%type <tree.Statement> drop_schema_stmt
%type <tree.Statement> drop_table_stmt
%type <tree.Statement> drop_type_stmt
%type <tree.Statement> drop_view_stmt
%type <tree.Statement> drop_sequence_stmt
%type <tree.Statement> drop_func_stmt
%type <tree.Statement> drop_policy_stmt
%type <tree.Statement> drop_proc_stmt
%type <tree.Statement> drop_trigger_stmt
%type <tree.Statement> drop_virtual_cluster_stmt
%type <bool>           opt_immediate

%type <tree.Statement> analyze_stmt
%type <tree.Statement> explain_stmt
%type <tree.Statement> prepare_stmt
%type <tree.Statement> preparable_stmt
%type <tree.Statement> explainable_stmt
%type <tree.Statement> row_source_extension_stmt
%type <tree.Statement> copy_to_stmt
%type <tree.Statement> do_stmt
%type <tree.Statement> export_stmt
%type <tree.Statement> execute_stmt
%type <tree.Statement> deallocate_stmt
%type <tree.Statement> grant_stmt
%type <tree.Statement> insert_stmt
%type <tree.Statement> import_stmt
%type <tree.Statement> pause_stmt pause_jobs_stmt pause_schedules_stmt pause_all_jobs_stmt alter_job_stmt
%type <*tree.Select>   for_schedules_clause
%type <tree.Statement> reassign_owned_by_stmt
%type <tree.Statement> drop_owned_by_stmt
%type <tree.Statement> release_stmt
%type <tree.Statement> reset_stmt reset_session_stmt reset_csetting_stmt
%type <tree.Statement> resume_stmt resume_jobs_stmt resume_schedules_stmt resume_all_jobs_stmt
%type <tree.Statement> drop_schedule_stmt
%type <tree.Statement> restore_stmt
%type <tree.StringOrPlaceholderOptList> string_or_placeholder_opt_list
%type <tree.Statement> revoke_stmt
%type <tree.Statement> refresh_stmt
%type <*tree.Select> select_stmt
%type <tree.Statement> abort_stmt
%type <tree.Statement> rollback_stmt
%type <tree.Statement> savepoint_stmt
%type <tree.Statement> prepare_transaction_stmt
%type <tree.Statement> commit_prepared_stmt
%type <tree.Statement> rollback_prepared_stmt

%type <tree.Statement> preparable_set_stmt nonpreparable_set_stmt
%type <tree.Statement> set_local_stmt
%type <tree.Statement> set_session_stmt
%type <tree.Statement> set_csetting_stmt set_or_reset_csetting_stmt
%type <tree.Statement> set_transaction_stmt
%type <tree.Statement> set_exprs_internal
%type <tree.Statement> generic_set
%type <tree.Statement> set_rest_more
%type <tree.Statement> set_rest
%type <tree.Statement> set_names

%type <tree.Statement> show_stmt
%type <tree.Statement> show_backup_stmt
%type <tree.Statement> show_columns_stmt
%type <tree.Statement> show_commit_timestamp_stmt
%type <tree.Statement> show_constraints_stmt
%type <tree.Statement> show_triggers_stmt
%type <tree.Statement> show_create_stmt
%type <tree.ShowCreateFormatOption> opt_show_create_format_options
%type <tree.Statement> show_create_schedules_stmt
%type <tree.Statement> show_create_external_connections_stmt
%type <tree.Statement> show_csettings_stmt show_local_or_virtual_cluster_csettings_stmt
%type <tree.Statement> show_databases_stmt
%type <tree.Statement> show_default_privileges_stmt
%type <tree.Statement> show_enums_stmt
%type <tree.Statement> show_external_connections_stmt
%type <tree.Statement> show_fingerprints_stmt opt_with_show_fingerprints_options fingerprint_options_list fingerprint_options
%type <tree.Statement> show_functions_stmt
%type <tree.Statement> show_procedures_stmt
%type <tree.Statement> show_grants_stmt
%type <tree.Statement> show_histogram_stmt
%type <tree.Statement> show_indexes_stmt
%type <tree.Statement> show_partitions_stmt
%type <tree.Statement> show_jobs_stmt
%type <tree.Statement> show_statements_stmt
%type <tree.Statement> show_ranges_stmt
%type <tree.Statement> show_range_for_row_stmt
%type <tree.Statement> show_locality_stmt
%type <tree.Statement> show_survival_goal_stmt
%type <tree.Statement> show_regions_stmt
%type <tree.Statement> show_roles_stmt
%type <tree.Statement> show_schemas_stmt
%type <tree.Statement> show_sequences_stmt
%type <tree.Statement> show_session_stmt
%type <tree.Statement> show_sessions_stmt
%type <tree.Statement> show_savepoint_stmt
%type <tree.Statement> show_stats_stmt
%type <tree.Statement> show_syntax_stmt
%type <tree.Statement> show_last_query_stats_stmt
%type <tree.Statement> show_tables_stmt
%type <tree.Statement> show_virtual_cluster_stmt opt_show_virtual_cluster_options show_virtual_cluster_options
%type <tree.Statement> show_trace_stmt
%type <tree.Statement> show_transaction_stmt
%type <tree.Statement> show_transactions_stmt
%type <tree.Statement> show_transfer_stmt
%type <tree.Statement> show_types_stmt
%type <tree.Statement> show_users_stmt
%type <tree.Statement> show_default_session_variables_for_role_stmt
%type <tree.Statement> show_zone_stmt
%type <tree.Statement> show_schedules_stmt
%type <tree.Statement> show_full_scans_stmt
%type <tree.Statement> show_completions_stmt
%type <tree.Statement> show_logical_replication_jobs_stmt opt_show_logical_replication_jobs_options show_logical_replication_jobs_options
%type <tree.Statement> show_policies_stmt

%type <str> statements_or_queries

%type <str> session_var
%type <*string> comment_text

%type <tree.Statement> transaction_stmt legacy_transaction_stmt legacy_begin_stmt legacy_end_stmt
%type <tree.Statement> truncate_stmt
%type <tree.Statement> unlisten_stmt
%type <tree.Statement> update_stmt
%type <tree.Statement> upsert_stmt
%type <tree.Statement> use_stmt

%type <tree.Statement> close_cursor_stmt
%type <tree.Statement> declare_cursor_stmt
%type <tree.Statement> fetch_cursor_stmt
%type <tree.Statement> move_cursor_stmt
%type <tree.CursorStmt> cursor_movement_specifier
%type <bool> opt_hold opt_binary
%type <tree.CursorSensitivity> opt_sensitivity
%type <tree.CursorScrollOption> opt_scroll
%type <int64> opt_forward_backward forward_backward
%type <int64> next_prior

%type <tree.Statement> reindex_stmt

%type <tree.KVOption> kv_option
%type <[]tree.KVOption> kv_option_list opt_with_options var_set_list opt_with_schedule_options
%type <*tree.BackupOptions> opt_with_backup_options backup_options backup_options_list
%type <*tree.RestoreOptions> opt_with_restore_options restore_options restore_options_list
%type <*tree.TenantReplicationOptions> opt_with_replication_options replication_options replication_options_list
%type <tree.ShowBackupDetails> show_backup_details
%type <*tree.ShowJobOptions> show_job_options show_job_options_list
%type <*tree.ShowBackupOptions> opt_with_show_backup_options show_backup_options show_backup_options_list
%type <*tree.CopyOptions> opt_with_copy_options copy_options copy_options_list copy_generic_options copy_generic_options_list
%type <str> import_format
%type <str> storage_parameter_key
%type <[]string> storage_parameter_key_list
%type <tree.StorageParam> storage_parameter
%type <[]tree.StorageParam> storage_parameter_list opt_table_with opt_with_storage_parameter_list

%type <*tree.Select> select_no_parens
%type <tree.SelectStatement> select_clause select_with_parens simple_select values_clause table_clause simple_select_clause
%type <tree.LockingClause> for_locking_clause opt_for_locking_clause for_locking_items
%type <*tree.LockingItem> for_locking_item
%type <tree.LockingStrength> for_locking_strength
%type <tree.LockingWaitPolicy> opt_nowait_or_skip
%type <tree.SelectStatement> set_operation

%type <tree.Expr> alter_column_default
%type <tree.Expr> alter_column_on_update
%type <tree.Expr> alter_column_visible
%type <tree.Direction> opt_asc_desc
%type <tree.NullsOrder> opt_nulls_order

%type <tree.AlterChangefeedCmd> alter_changefeed_cmd
%type <tree.AlterChangefeedCmds> alter_changefeed_cmds
%type <tree.AlterBackupScheduleCmd> alter_backup_schedule_cmd
%type <tree.AlterBackupScheduleCmds> alter_backup_schedule_cmds

%type <tree.BackupKMS> backup_kms
%type <tree.AlterBackupCmd> alter_backup_cmd
%type <tree.AlterBackupCmd> alter_backup_cmds

%type <tree.AlterTableCmd> alter_table_cmd
%type <tree.AlterTableCmds> alter_table_cmds
%type <tree.AlterIndexCmd> alter_index_cmd
%type <tree.AlterIndexCmds> alter_index_cmds

%type <tree.DropBehavior> opt_drop_behavior

%type <tree.ValidationBehavior> opt_validate_behavior

%type <str> opt_template_clause opt_encoding_clause opt_lc_collate_clause opt_lc_ctype_clause
%type <tree.NameList> opt_regions_list
%type <str> region_name primary_region_clause opt_primary_region_clause secondary_region_clause opt_secondary_region_clause
%type <tree.SuperRegion> super_region_clause opt_super_region_clause
%type <tree.DataPlacement> opt_placement_clause placement_clause
%type <tree.NameList> region_name_list
%type <tree.SurvivalGoal> survival_goal_clause opt_survival_goal_clause
%type <*tree.Locality> locality opt_locality
%type <int32> opt_connection_limit

%type <tree.IsolationLevel> transaction_iso_level
%type <tree.UserPriority> transaction_user_priority
%type <tree.ReadWriteMode> transaction_read_mode
%type <tree.DeferrableMode> transaction_deferrable_mode

%type <str> name opt_name opt_name_parens
%type <str> privilege savepoint_name
%type <tree.KVOption> role_option password_clause valid_until_clause subject_clause
%type <tree.Operator> subquery_op
%type <*tree.UnresolvedName> func_name func_name_no_crdb_extra
%type <tree.ResolvableFunctionReference> func_application_name
%type <str> opt_class opt_collate

%type <str> cursor_name database_name index_name opt_index_name column_name insert_column_item statistics_name window_name opt_in_database
%type <str> family_name opt_family_name table_alias_name constraint_name target_name zone_name partition_name collation_name
%type <str> db_object_name_component
%type <*tree.UnresolvedObjectName> table_name db_name standalone_index_name sequence_name type_name
%type <*tree.UnresolvedObjectName> view_name db_object_name simple_db_object_name complex_db_object_name
%type <[]*tree.UnresolvedObjectName> type_name_list
%type <str> schema_name opt_in_schema
%type <tree.ObjectNamePrefix>  qualifiable_schema_name opt_schema_name wildcard_pattern
%type <tree.ObjectNamePrefixList> schema_name_list
%type <tree.ObjectNamePrefixList> schema_wildcard
%type <*tree.UnresolvedName> table_pattern complex_table_pattern
%type <*tree.UnresolvedName> column_path prefixed_column_path column_path_with_star
%type <tree.TableExpr> insert_target create_stats_target analyze_target

%type <*tree.TableIndexName> table_index_name
%type <tree.TableIndexNames> table_index_name_list

%type <tree.Operator> all_op qual_op operator_op

%type <tree.IsolationLevel> iso_level
%type <tree.UserPriority> user_priority

%type <tree.TableDefs> opt_table_elem_list table_elem_list create_as_opt_col_list create_as_table_defs
%type <[]tree.LikeTableOption> like_table_option_list
%type <tree.LikeTableOption> like_table_option
%type <tree.CreateTableOnCommitSetting> opt_create_table_on_commit
%type <*tree.PartitionBy> opt_partition_by partition_by partition_by_inner
%type <*tree.PartitionByTable> opt_partition_by_table partition_by_table
%type <*tree.PartitionByIndex> opt_partition_by_index partition_by_index
%type <str> partition opt_partition
%type <str> opt_create_table_inherits
%type <tree.ListPartition> list_partition
%type <[]tree.ListPartition> list_partitions
%type <tree.RangePartition> range_partition
%type <[]tree.RangePartition> range_partitions
%type <empty> opt_all_clause
%type <empty> opt_privileges_clause
%type <bool> distinct_clause opt_with_data
%type <tree.DistinctOn> distinct_on_clause
%type <tree.NameList> opt_column_list insert_column_list opt_stats_columns query_stats_cols
// Note that "no index" variants exist to disable custom ORDER BY <index> syntax
// in some places like function calls.
%type <tree.OrderBy> sort_clause sort_clause_no_index single_sort_clause opt_sort_clause opt_sort_clause_no_index
%type <[]*tree.Order> sortby_list sortby_no_index_list
%type <tree.IndexElemList> index_params create_as_params
%type <tree.IndexInvisibility> opt_index_visible alter_index_visible
%type <idxtype.T> opt_index_access_method
%type <tree.NameList> name_list privilege_list
%type <[]int32> opt_array_bounds
%type <*tree.Batch> opt_batch_clause
%type <tree.From> from_clause
%type <tree.TableExprs> from_list rowsfrom_list opt_from_list
%type <tree.TablePatterns> table_pattern_list
%type <tree.TableNames> db_object_name_list table_name_list view_name_list sequence_name_list opt_locked_rels
%type <tree.Exprs> expr_list opt_expr_list tuple1_ambiguous_values tuple1_unambiguous_values
%type <*tree.Tuple> expr_tuple1_ambiguous expr_tuple_unambiguous
%type <tree.NameList> attrs
%type <[]string> session_var_parts
%type <tree.SelectExprs> opt_target_list target_list
%type <tree.UpdateExprs> set_clause_list
%type <*tree.UpdateExpr> set_clause multiple_set_clause
%type <tree.ArraySubscripts> array_subscripts
%type <tree.GroupBy> group_clause
%type <tree.Exprs> group_by_list
%type <tree.Expr> group_by_item
%type <*tree.Limit> select_limit opt_select_limit
%type <tree.TableNames> relation_expr_list
%type <tree.ReturningClause> returning_clause
%type <tree.TableExprs> opt_using_clause
%type <tree.RefreshDataOption> opt_clear_data

%type <tree.BatchParam> batch_param
%type <[]tree.BatchParam> batch_param_list

%type <[]tree.SequenceOption> sequence_option_list opt_sequence_option_list
%type <tree.SequenceOption> sequence_option_elem

%type <[]tree.SequenceOption> identity_option_list
%type <tree.SequenceOption> identity_option_elem

%type <bool> all_or_distinct
%type <bool> with_comment
%type <empty> join_outer
%type <tree.JoinCond> join_qual
%type <str> join_type
%type <str> opt_join_hint

%type <tree.Exprs> extract_list
%type <tree.Exprs> overlay_list
%type <tree.Exprs> position_list
%type <tree.Exprs> substr_list
%type <tree.Exprs> trim_list
%type <tree.Exprs> execute_param_clause
%type <types.IntervalTypeMetadata> opt_interval_qualifier interval_qualifier interval_second
%type <tree.Expr> overlay_placing
%type <*tree.TenantSpec> virtual_cluster_spec virtual_cluster_spec_opt_all

%type <bool> opt_unique opt_concurrently opt_cluster opt_without_index

%type <*tree.Limit> limit_clause offset_clause opt_limit_clause
%type <tree.Expr> select_fetch_first_value
%type <empty> row_or_rows
%type <empty> first_or_next

%type <tree.Statement> insert_rest
%type <tree.ColumnDefList> opt_col_def_list col_def_list opt_col_def_list_no_types col_def_list_no_types
%type <tree.ColumnDef> col_def
%type <*tree.OnConflict> on_conflict

%type <tree.Statement> begin_transaction
%type <tree.TransactionModes> transaction_mode_list transaction_mode

%type <tree.Expr> opt_hash_sharded_bucket_count
%type <*tree.ShardedIndexDef> opt_hash_sharded
%type <tree.NameList> opt_storing
%type <*tree.ColumnTableDef> column_table_def
%type <tree.TableDef> table_elem
%type <tree.Expr> where_clause opt_where_clause
%type <*tree.ArraySubscript> array_subscript
%type <tree.Expr> opt_slice_bound
%type <*tree.IndexFlags> opt_index_flags
%type <*tree.IndexFlags> index_flags_param
%type <*tree.IndexFlags> index_flags_param_list
%type <tree.Expr> a_expr b_expr c_expr d_expr typed_literal
%type <tree.Expr> substr_from substr_for
%type <tree.Expr> in_expr
%type <tree.Expr> having_clause
%type <tree.Expr> array_expr
%type <tree.Expr> interval_value
%type <[]tree.ResolvableTypeReference> type_list prep_type_clause
%type <tree.Exprs> array_expr_list
%type <*tree.Tuple> row labeled_row
%type <tree.Expr> case_expr case_arg case_default
%type <*tree.When> when_clause
%type <[]*tree.When> when_clause_list
%type <treecmp.ComparisonOperator> sub_type
%type <tree.Expr> numeric_only
%type <tree.AliasClause> alias_clause opt_alias_clause func_alias_clause opt_func_alias_clause
%type <bool> opt_ordinality opt_compact
%type <*tree.Order> sortby sortby_index
%type <tree.IndexElem> index_elem index_elem_options create_as_param
%type <tree.TableExpr> table_ref numeric_table_ref func_table
%type <tree.Exprs> rowsfrom_list
%type <tree.Expr> rowsfrom_item
%type <tree.TableExpr> joined_table
%type <*tree.UnresolvedObjectName> relation_expr
%type <tree.TableExpr> table_expr_opt_alias_idx table_name_opt_idx
%type <bool> opt_only opt_descendant
%type <tree.SelectExpr> target_elem
%type <*tree.UpdateExpr> single_set_clause
%type <tree.AsOfClause> as_of_clause opt_as_of_clause
%type <tree.Expr> opt_changefeed_sink changefeed_sink
%type <str> opt_changefeed_family

%type <str> explain_option_name
%type <[]string> explain_option_list opt_enum_val_list enum_val_list
%type <[]tree.CompositeTypeElem> composite_type_list opt_composite_type_list

%type <tree.ResolvableTypeReference> typename simple_typename cast_target
%type <*types.T> const_typename
%type <*tree.AlterTypeAddValuePlacement> opt_add_val_placement
%type <bool> opt_timezone
%type <*types.T> numeric opt_numeric_modifiers
%type <*types.T> opt_float
%type <*types.T> character_with_length character_without_length
%type <*types.T> const_datetime interval_type
%type <*types.T> bit_with_length bit_without_length
%type <*types.T> character_base
%type <*types.T> geo_shape_type
%type <*types.T> const_geo
%type <*types.T> const_vector
%type <str> extract_arg
%type <bool> opt_varying

%type <*tree.NumVal> signed_iconst only_signed_iconst
%type <*tree.NumVal> signed_fconst only_signed_fconst
%type <int32> iconst32
%type <int64> signed_iconst64
%type <int64> iconst64
%type <tree.Expr> var_value
%type <tree.Exprs> var_list
%type <tree.NameList> var_name
%type <str> unrestricted_name type_function_name type_function_name_no_crdb_extra
%type <str> non_reserved_word
%type <str> non_reserved_word_or_sconst
%type <tree.RoleSpec> role_spec opt_owner_clause
%type <tree.RoleSpecList> role_spec_list
%type <tree.Expr> zone_value
%type <tree.Expr> string_or_placeholder
%type <tree.Expr> string_or_placeholder_list
%type <str> region_or_regions

%type <str> unreserved_keyword type_func_name_keyword type_func_name_no_crdb_extra_keyword type_func_name_crdb_extra_keyword
%type <str> bare_label_keywords bare_col_label
%type <str> col_name_keyword reserved_keyword cockroachdb_extra_reserved_keyword extra_var_value

%type <tree.ResolvableTypeReference> complex_type_name
%type <str> general_type_name

%type <tree.ConstraintTableDef> table_constraint constraint_elem create_as_constraint_def create_as_constraint_elem
%type <tree.TableDef> index_def
%type <tree.TableDef> family_def
%type <[]tree.NamedColumnQualification> col_qual_list create_as_col_qual_list
%type <tree.NamedColumnQualification> col_qualification create_as_col_qualification
%type <tree.ColumnQualification> col_qualification_elem create_as_col_qualification_elem
%type <tree.CompositeKeyMatchMethod> key_match
%type <tree.ReferenceActions> reference_actions
%type <tree.ReferenceAction> reference_action reference_on_delete reference_on_update

%type <tree.Expr> func_application func_expr_common_subexpr special_function
%type <tree.Expr> func_expr func_expr_windowless
%type <empty> opt_with
%type <*tree.With> with_clause opt_with_clause
%type <[]*tree.CTE> cte_list
%type <*tree.CTE> common_table_expr
%type <tree.CTEMaterializeClause> materialize_clause

%type <tree.Expr> within_group_clause
%type <tree.Expr> filter_clause
%type <tree.Exprs> opt_partition_clause
%type <tree.Window> window_clause window_definition_list
%type <*tree.WindowDef> window_definition over_clause window_specification
%type <str> opt_existing_window_name
%type <*tree.WindowFrame> opt_frame_clause
%type <tree.WindowFrameBounds> frame_extent
%type <*tree.WindowFrameBound> frame_bound
%type <treewindow.WindowFrameExclusion> opt_frame_exclusion

%type <[]tree.ColumnID> opt_tableref_col_list tableref_col_list

%type <tree.ChangefeedTargets> changefeed_targets
%type <tree.ChangefeedTarget> changefeed_target
%type <tree.BackupTargetList> backup_targets
%type <*tree.BackupTargetList> opt_backup_targets

%type <tree.GrantTargetList> grant_targets targets_roles target_types
%type <tree.TableExpr> changefeed_target_expr
%type <*tree.GrantTargetList> opt_on_targets_roles
%type <tree.RoleSpecList> for_grantee_clause
%type <privilege.List> privileges
%type <[]tree.KVOption> opt_role_options role_options
%type <tree.AuditMode> audit_mode

%type <str> relocate_kw
%type <tree.RelocateSubject> relocate_subject relocate_subject_nonlease

%type <*tree.SetZoneConfig> set_zone_config

%type <tree.Expr> opt_alter_column_using

%type <tree.Persistence> opt_temp
%type <tree.Persistence> opt_persistence_temp_table
%type <bool> role_or_group_or_user

%type <*tree.LabelSpec> schedule_label_spec
%type <tree.Expr>  cron_expr sconst_or_placeholder
%type <*tree.FullBackupClause> opt_full_backup_clause
%type <tree.ScheduleState> schedule_state
%type <tree.ScheduledJobExecutorType> opt_schedule_executor_type

%type <tree.AbbreviatedGrant> abbreviated_grant_stmt
%type <tree.AbbreviatedRevoke> abbreviated_revoke_stmt
%type <bool> opt_with_grant_option
%type <tree.NameList> opt_for_roles
%type <tree.NameList> opt_policy_roles
%type <tree.PolicyExpressions> opt_policy_exprs
%type <tree.PolicyType> opt_policy_type
%type <tree.PolicyCommand> opt_policy_command
%type <tree.TableRLSMode> table_rls_mode
%type <tree.ObjectNamePrefixList>  opt_in_schemas
%type <privilege.TargetObjectType> target_object_type

// Routine (UDF/SP) relevant components.
%type <bool> opt_or_replace opt_return_set opt_no
%type <str> param_name routine_as
%type <tree.RoutineParams> opt_routine_param_with_default_list routine_param_with_default_list
%type <tree.RoutineParams> func_params func_params_list table_func_column_list
%type <tree.RoutineParam> routine_param_with_default routine_param table_func_column
%type <tree.ResolvableTypeReference> routine_return_type routine_param_type
%type <tree.RoutineOptions> opt_create_routine_opt_list create_routine_opt_list alter_func_opt_list
%type <tree.RoutineOption> create_routine_opt_item common_routine_opt_item
%type <tree.RoutineParamClass> routine_param_class
%type <*tree.UnresolvedObjectName> routine_create_name
%type <tree.DoBlockOptions> do_stmt_opt_list
%type <tree.DoBlockOption> do_stmt_opt_item
%type <tree.Statement> routine_return_stmt routine_body_stmt
%type <tree.Statements> routine_body_stmt_list
%type <*tree.RoutineBody> opt_routine_body
%type <tree.RoutineObj> function_with_paramtypes
%type <tree.RoutineObjs> function_with_paramtypes_list
%type <empty> opt_link_sym

// Trigger relevant components.
%type <tree.TriggerActionTime> trigger_action_time
%type <*tree.TriggerEvent> trigger_event
%type <[]*tree.TriggerEvent> trigger_event_list
%type <*tree.TriggerTransition> trigger_transition
%type <[]*tree.TriggerTransition> trigger_transition_list opt_trigger_transition_list
%type <bool> transition_is_new transition_is_row
%type <tree.TriggerForEach> trigger_for_each trigger_for_type
%type <tree.Expr> trigger_when
%type <str> trigger_func_arg opt_as function_or_procedure
%type <[]string> trigger_func_args

%type <*tree.LabelSpec> label_spec

%type <*tree.ShowRangesOptions> opt_show_ranges_options show_ranges_options

// Precedence: lowest to highest
%nonassoc  VALUES              // see value_clause
%nonassoc  SET                 // see table_expr_opt_alias_idx
%left      UNION EXCEPT
%left      INTERSECT
%left      OR
%left      AND
%right     NOT
%nonassoc  IS ISNULL NOTNULL   // IS sets precedence for IS NULL, etc
%nonassoc  '<' '>' '=' LESS_EQUALS GREATER_EQUALS NOT_EQUALS
%nonassoc  '~' BETWEEN IN LIKE ILIKE SIMILAR NOT_REGMATCH REGIMATCH NOT_REGIMATCH NOT_LA
%nonassoc  ESCAPE              // ESCAPE must be just above LIKE/ILIKE/SIMILAR
%nonassoc  CONTAINS CONTAINED_BY '?' JSON_SOME_EXISTS JSON_ALL_EXISTS
%nonassoc  OVERLAPS
%left      POSTFIXOP           // dummy for postfix OP rules
// To support target_elem without AS, we must give IDENT an explicit priority
// between POSTFIXOP and OP. We can safely assign the same priority to various
// unreserved keywords as needed to resolve ambiguities (this can't have any
// bad effects since obviously the keywords will still behave the same as if
// they weren't keywords). We need to do this for PARTITION, RANGE, ROWS,
// GROUPS to support opt_existing_window_name; and for RANGE, ROWS, GROUPS so
// that they can follow a_expr without creating postfix-operator problems; and
// for NULL so that it can follow b_expr in col_qual_list without creating
// postfix-operator problems.
//
// To support CUBE and ROLLUP in GROUP BY without reserving them, we give them
// an explicit priority lower than '(', so that a rule with CUBE '(' will shift
// rather than reducing a conflicting rule that takes CUBE as a function name.
// Using the same precedence as IDENT seems right for the reasons given above.
//
// The frame_bound productions UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING are
// even messier: since UNBOUNDED is an unreserved keyword (per spec!), there is
// no principled way to distinguish these from the productions a_expr
// PRECEDING/FOLLOWING. We hack this up by giving UNBOUNDED slightly lower
// precedence than PRECEDING and FOLLOWING. At present this doesn't appear to
// cause UNBOUNDED to be treated differently from other unreserved keywords
// anywhere else in the grammar, but it's definitely risky. We can blame any
// funny behavior of UNBOUNDED on the SQL standard, though.
%nonassoc  UNBOUNDED         // ideally should have same precedence as IDENT
%nonassoc  IDENT NULL PARTITION RANGE ROWS GROUPS PRECEDING FOLLOWING CUBE ROLLUP
%left      CONCAT FETCHVAL FETCHTEXT FETCHVAL_PATH FETCHTEXT_PATH REMOVE_PATH AT_AT DISTANCE COS_DISTANCE NEG_INNER_PRODUCT // multi-character ops
%left      '|'
%left      '#'
%left      '&'
%left      LSHIFT RSHIFT INET_CONTAINS_OR_EQUALS INET_CONTAINED_BY_OR_EQUALS AND_AND SQRT CBRT
%left      OPERATOR // if changing the last token before OPERATOR, change all instances of %prec <last token>
%left      '+' '-'
%left      '*' '/' FLOORDIV '%'
%left      '^'
%left      INTERVAL_SIMPLE   // sets precedence for interval syntax
%left      TO                // sets precedence for interval syntax
// Unary Operators
%left      AT                // sets precedence for AT TIME ZONE
%left      COLLATE
%right     UMINUS
%left      '[' ']'
%left      '(' ')'
%left      TYPEANNOTATE
%left      TYPECAST
%left      '.'
// These might seem to be low-precedence, but actually they are not part
// of the arithmetic hierarchy at all in their use as JOIN operators.
// We make them high-precedence to support their use as function names.
// They wouldn't be given a precedence at all, were it not that we need
// left-associativity among the JOIN rules themselves.
%left      JOIN CROSS LEFT FULL RIGHT INNER NATURAL
%right     HELPTOKEN

%%

stmt_block:
  stmt
  {
    sqllex.(*lexer).SetStmt($1.stmt())
  }

stmt:
  HELPTOKEN { return helpWith(sqllex, "") }
| stmt_without_legacy_transaction
| legacy_transaction_stmt
| /* EMPTY */
  {
    $$.val = tree.Statement(nil)
  }

stmt_without_legacy_transaction:
  preparable_stmt            // help texts in sub-rule
| analyze_stmt               // EXTEND WITH HELP: ANALYZE
| call_stmt
| copy_stmt
| comment_stmt
| execute_stmt               // EXTEND WITH HELP: EXECUTE
| deallocate_stmt            // EXTEND WITH HELP: DEALLOCATE
| discard_stmt               // EXTEND WITH HELP: DISCARD
| do_stmt                    // EXTEND WITH HELP: DO
| grant_stmt                 // EXTEND WITH HELP: GRANT
| prepare_stmt               // EXTEND WITH HELP: PREPARE
| revoke_stmt                // EXTEND WITH HELP: REVOKE
| savepoint_stmt             // EXTEND WITH HELP: SAVEPOINT
| reassign_owned_by_stmt     // EXTEND WITH HELP: REASSIGN OWNED BY
| drop_owned_by_stmt         // EXTEND WITH HELP: DROP OWNED BY
| release_stmt               // EXTEND WITH HELP: RELEASE
| refresh_stmt               // EXTEND WITH HELP: REFRESH
| nonpreparable_set_stmt     // help texts in sub-rule
| transaction_stmt           // help texts in sub-rule
| close_cursor_stmt          // EXTEND WITH HELP: CLOSE
| declare_cursor_stmt        // EXTEND WITH HELP: DECLARE
| fetch_cursor_stmt          // EXTEND WITH HELP: FETCH
| move_cursor_stmt           // EXTEND WITH HELP: MOVE
| reindex_stmt
| unlisten_stmt
| show_commit_timestamp_stmt // EXTEND WITH HELP: SHOW COMMIT TIMESTAMP

// %Help: ALTER
// %Category: Group
// %Text: ALTER TABLE, ALTER INDEX, ALTER VIEW, ALTER SEQUENCE, ALTER DATABASE, ALTER USER, ALTER ROLE, ALTER DEFAULT PRIVILEGES
alter_stmt:
  alter_ddl_stmt      // help texts in sub-rule
| alter_role_stmt     // EXTEND WITH HELP: ALTER ROLE
| alter_virtual_cluster_stmt   /* SKIP DOC */
| alter_unsupported_stmt
| ALTER error         // SHOW HELP: ALTER

alter_ddl_stmt:
  alter_table_stmt              // EXTEND WITH HELP: ALTER TABLE
| alter_index_stmt              // EXTEND WITH HELP: ALTER INDEX
| alter_view_stmt               // EXTEND WITH HELP: ALTER VIEW
| alter_sequence_stmt           // EXTEND WITH HELP: ALTER SEQUENCE
| alter_database_stmt           // EXTEND WITH HELP: ALTER DATABASE
| alter_range_stmt              // EXTEND WITH HELP: ALTER RANGE
| alter_partition_stmt          // EXTEND WITH HELP: ALTER PARTITION
| alter_schema_stmt             // EXTEND WITH HELP: ALTER SCHEMA
| alter_type_stmt               // EXTEND WITH HELP: ALTER TYPE
| alter_default_privileges_stmt // EXTEND WITH HELP: ALTER DEFAULT PRIVILEGES
| alter_changefeed_stmt         // EXTEND WITH HELP: ALTER CHANGEFEED
| alter_backup_stmt             // EXTEND WITH HELP: ALTER BACKUP
| alter_func_stmt               // EXTEND WITH HELP: ALTER FUNCTION
| alter_proc_stmt               // EXTEND WITH HELP: ALTER PROCEDURE
| alter_backup_schedule  // EXTEND WITH HELP: ALTER BACKUP SCHEDULE
| alter_policy_stmt             // EXTEND WITH HELP: ALTER POLICY
| alter_job_stmt                // EXTEND WITH HELP: ALTER JOB

// %Help: ALTER TABLE - change the definition of a table
// %Category: DDL
// %Text:
// ALTER TABLE [IF EXISTS] <tablename> <command> [, ...]
//
// Commands:
//   ALTER TABLE ... ADD [COLUMN] [IF NOT EXISTS] <colname> <type> [<qualifiers...>]
//   ALTER TABLE ... ADD <constraint>
//   ALTER TABLE ... DROP [COLUMN] [IF EXISTS] <colname> [RESTRICT | CASCADE]
//   ALTER TABLE ... DROP CONSTRAINT [IF EXISTS] <constraintname> [RESTRICT | CASCADE]
//   ALTER TABLE ... ALTER [COLUMN] <colname> {SET DEFAULT <expr> | DROP DEFAULT}
//   ALTER TABLE ... ALTER [COLUMN] <colname> {SET ON UPDATE <expr> | DROP ON UPDATE}
//   ALTER TABLE ... ALTER [COLUMN] <colname> DROP NOT NULL
//   ALTER TABLE ... ALTER [COLUMN] <colname> DROP STORED
//   ALTER TABLE ... ALTER [COLUMN] <colname> ADD GENERATED { ALWAYS | BY DEFAULT } AS IDENTITY [ ( opt_sequence_option_list ) ]
//   ALTER TABLE ... ALTER [COLUMN] <colname> SET GENERATED { ALWAYS | BY DEFAULT }
//   ALTER TABLE ... ALTER [COLUMN] <colname> <identity_option_list>
//   ALTER TABLE ... ALTER [COLUMN] <colname> DROP IDENTITY [ IF EXISTS ]
//   ALTER TABLE ... ALTER [COLUMN] <colname> [SET DATA] TYPE <type> [COLLATE <collation>]
//   ALTER TABLE ... ALTER PRIMARY KEY USING COLUMNS ( <colnames...> )
//   ALTER TABLE ... RENAME TO <newname>
//   ALTER TABLE ... RENAME [COLUMN] <colname> TO <newname>
//   ALTER TABLE ... VALIDATE CONSTRAINT <constraintname>
//   ALTER TABLE ... SET (storage_param = value, ...)
//   ALTER TABLE ... SPLIT AT <selectclause> [WITH EXPIRATION <expr>]
//   ALTER TABLE ... UNSPLIT AT <selectclause>
//   ALTER TABLE ... UNSPLIT ALL
//   ALTER TABLE ... SCATTER [ FROM ( <exprs...> ) TO ( <exprs...> ) ]
//   ALTER TABLE ... INJECT STATISTICS ...  (experimental)
//   ALTER TABLE ... RELOCATE [ LEASE | VOTERS | NONVOTERS ] <selectclause>  (experimental)
//   ALTER TABLE ... PARTITION BY RANGE ( <name...> ) ( <rangespec> )
//   ALTER TABLE ... PARTITION BY LIST ( <name...> ) ( <listspec> )
//   ALTER TABLE ... PARTITION BY NOTHING
//   ALTER TABLE ... CONFIGURE ZONE <zoneconfig>
//   ALTER TABLE ... SET SCHEMA <newschemaname>
//   ALTER TABLE ... SET LOCALITY [REGIONAL BY [TABLE IN <region> | ROW] | GLOBAL]
//   ALTER TABLE ... {ENABLE | DISABLE | FORCE | NO FORCE} ROW LEVEL SECURITY
//
// Column qualifiers:
//   [CONSTRAINT <constraintname>] {NULL | NOT NULL | UNIQUE | PRIMARY KEY | CHECK (<expr>) | DEFAULT <expr>}
//   FAMILY <familyname>, CREATE [IF NOT EXISTS] FAMILY [<familyname>]
//   REFERENCES <tablename> [( <colnames...> )]
//   COLLATE <collationname>
//
// Zone configurations:
//   DISCARD
//   USING <var> = <expr> [, ...]
//   USING <var> = COPY FROM PARENT [, ...]
//   { TO | = } <expr>
//
// %SeeAlso: WEBDOCS/alter-table.html
alter_table_stmt:
  alter_onetable_stmt
| alter_relocate_stmt
| alter_split_stmt
| alter_unsplit_stmt
| alter_scatter_stmt
| alter_zone_table_stmt
| alter_rename_table_stmt
| alter_table_set_schema_stmt
| alter_table_locality_stmt
| alter_table_owner_stmt
// ALTER TABLE has its error help token here because the ALTER TABLE
// prefix is spread over multiple non-terminals.
| ALTER TABLE error     // SHOW HELP: ALTER TABLE

// %Help: ALTER PARTITION - apply zone configurations to a partition
// %Category: DDL
// %Text:
// ALTER PARTITION <name> <command>
//
// Commands:
//   -- Alter a single partition which exists on any of a table's indexes.
//   ALTER PARTITION <partition> OF TABLE <tablename> CONFIGURE ZONE <zoneconfig>
//
//   -- Alter a partition of a specific index.
//   ALTER PARTITION <partition> OF INDEX <tablename>@<indexname> CONFIGURE ZONE <zoneconfig>
//
//   -- Alter all partitions with the same name across a table's indexes.
//   ALTER PARTITION <partition> OF INDEX <tablename>@* CONFIGURE ZONE <zoneconfig>
//
// Zone configurations:
//   DISCARD
//   USING <var> = <expr> [, ...]
//   USING <var> = COPY FROM PARENT [, ...]
//   { TO | = } <expr>
//
// %SeeAlso: WEBDOCS/alter-partition.html
alter_partition_stmt:
  alter_zone_partition_stmt
| ALTER PARTITION error // SHOW HELP: ALTER PARTITION

// %Help: ALTER VIEW - change the definition of a view
// %Category: DDL
// %Text:
// ALTER [MATERIALIZED] VIEW [IF EXISTS] <name> RENAME TO <newname>
// ALTER [MATERIALIZED] VIEW [IF EXISTS] <name> SET SCHEMA <newschemaname>
// %SeeAlso: WEBDOCS/alter-view.html
alter_view_stmt:
  alter_rename_view_stmt
| alter_view_set_schema_stmt
| alter_view_owner_stmt
// ALTER VIEW has its error help token here because the ALTER VIEW
// prefix is spread over multiple non-terminals.
| ALTER VIEW error // SHOW HELP: ALTER VIEW

// %Help: ALTER SEQUENCE - change the definition of a sequence
// %Category: DDL
// %Text:
// ALTER SEQUENCE [IF EXISTS] <name>
//   [AS <typename>]
//   [INCREMENT <increment>]
//   [MINVALUE <minvalue> | NO MINVALUE]
//   [MAXVALUE <maxvalue> | NO MAXVALUE]
//   [START [WITH] <start>]
//   [RESTART [[WITH] <restart>]]
//   [[NO] CYCLE]
// ALTER SEQUENCE [IF EXISTS] <name> RENAME TO <newname>
// ALTER SEQUENCE [IF EXISTS] <name> SET SCHEMA <newschemaname>
alter_sequence_stmt:
  alter_rename_sequence_stmt
| alter_sequence_options_stmt
| alter_sequence_set_schema_stmt
| alter_sequence_owner_stmt
| ALTER SEQUENCE error // SHOW HELP: ALTER SEQUENCE

alter_sequence_options_stmt:
  ALTER SEQUENCE sequence_name sequence_option_list
  {
    $$.val = &tree.AlterSequence{Name: $3.unresolvedObjectName(), Options: $4.seqOpts(), IfExists: false}
  }
| ALTER SEQUENCE IF EXISTS sequence_name sequence_option_list
  {
    $$.val = &tree.AlterSequence{Name: $5.unresolvedObjectName(), Options: $6.seqOpts(), IfExists: true}
  }


// %Help: ALTER DATABASE - change the definition of a database
// %Category: DDL
// %Text:
// ALTER DATABASE <name> RENAME TO <newname>
// ALTER DATABASE <name> CONFIGURE ZONE <zone config>
// ALTER DATABASE <name> OWNER TO <newowner>
// ALTER DATABASE <name> CONVERT TO SCHEMA WITH PARENT <name>
// ALTER DATABASE <name> ADD REGION [IF NOT EXISTS] <region>
// ALTER DATABASE <name> DROP REGION [IF EXISTS] <region>
// ALTER DATABASE <name> PRIMARY REGION <region>
// ALTER DATABASE <name> SURVIVE <failure type>
// ALTER DATABASE <name> PLACEMENT { RESTRICTED | DEFAULT }
// ALTER DATABASE <name> SET var { TO | = } { value | DEFAULT }
// ALTER DATABASE <name> RESET { var | ALL }
// ALTER DATABASE <name> ALTER LOCALITY { GLOBAL | REGIONAL [IN <region>] } CONFIGURE ZONE <zone config>
// %SeeAlso: WEBDOCS/alter-database.html
alter_database_stmt:
  alter_rename_database_stmt
| alter_zone_database_stmt
| alter_database_owner
| alter_database_to_schema_stmt
| alter_database_add_region_stmt
| alter_database_drop_region_stmt
| alter_database_survival_goal_stmt
| alter_database_primary_region_stmt
| alter_database_placement_stmt
| alter_database_set_stmt
| alter_database_add_super_region
| alter_database_alter_super_region
| alter_database_drop_super_region
| alter_database_set_secondary_region_stmt
| alter_database_drop_secondary_region
| alter_database_set_zone_config_extension_stmt

// %Help: ALTER FUNCTION - change the definition of a function
// %Category: DDL
// %Text:
// ALTER FUNCTION name [ ( [ [ argmode ] [ argname ] argtype [, ...] ] ) ]
//    action [ ... ] [ RESTRICT ]
// ALTER FUNCTION name [ ( [ [ argmode ] [ argname ] argtype [, ...] ] ) ]
//    RENAME TO new_name
// ALTER FUNCTION name [ ( [ [ argmode ] [ argname ] argtype [, ...] ] ) ]
//    OWNER TO { new_owner | CURRENT_USER | SESSION_USER }
// ALTER FUNCTION name [ ( [ [ argmode ] [ argname ] argtype [, ...] ] ) ]
//    SET SCHEMA new_schema
//
// where action is one of:
//
//    CALLED ON NULL INPUT | RETURNS NULL ON NULL INPUT | STRICT
//    IMMUTABLE | STABLE | VOLATILE
//    [ NOT ] LEAKPROOF
//    [ EXTERNAL ] SECURITY { INVOKER | DEFINER }
// %SeeAlso: WEBDOCS/alter-function.html
alter_func_stmt:
  alter_func_options_stmt
| alter_func_rename_stmt
| alter_func_owner_stmt
| alter_func_set_schema_stmt
| alter_func_dep_extension_stmt
| ALTER FUNCTION error // SHOW HELP: ALTER FUNCTION

// %Help: ALTER PROCEDURE - change the definition of a procedure
// %Category: DDL
// %Text:
// ALTER PROCEDURE name [ ( [ [ argmode ] [ argname ] argtype [, ...] ] ) ]
//    RENAME TO new_name
// ALTER PROCEDURE name [ ( [ [ argmode ] [ argname ] argtype [, ...] ] ) ]
//    OWNER TO { new_owner | CURRENT_USER | SESSION_USER }
// ALTER PROCEDURE name [ ( [ [ argmode ] [ argname ] argtype [, ...] ] ) ]
//    SET SCHEMA new_schema
//
// %SeeAlso: WEBDOCS/alter-procedure.html
alter_proc_stmt:
  alter_proc_rename_stmt
| alter_proc_owner_stmt
| alter_proc_set_schema_stmt
| ALTER PROCEDURE error // SHOW HELP: ALTER PROCEDURE

// ALTER DATABASE has its error help token here because the ALTER DATABASE
// prefix is spread over multiple non-terminals.
| ALTER DATABASE error // SHOW HELP: ALTER DATABASE

alter_database_owner:
  ALTER DATABASE database_name OWNER TO role_spec
  {
    $$.val = &tree.AlterDatabaseOwner{Name: tree.Name($3), Owner: $6.roleSpec()}
  }

// This form is an alias for ALTER ROLE ALL IN DATABASE <db> SET ...
alter_database_set_stmt:
  ALTER DATABASE database_name set_or_reset_clause
  {
    $$.val = &tree.AlterRoleSet{
      AllRoles: true,
      DatabaseName: tree.Name($3),
      IsRole: true,
      SetOrReset: $4.setVar(),
    }
  }

alter_database_placement_stmt:
  ALTER DATABASE database_name placement_clause
  {
    $$.val = &tree.AlterDatabasePlacement{
      Name: tree.Name($3),
      Placement: $4.dataPlacement(),
    }
  }

alter_database_add_region_stmt:
  ALTER DATABASE database_name ADD REGION region_name
  {
    $$.val = &tree.AlterDatabaseAddRegion{
      Name: tree.Name($3),
      Region: tree.Name($6),
    }
  }
| ALTER DATABASE database_name ADD REGION IF NOT EXISTS region_name
  {
    $$.val = &tree.AlterDatabaseAddRegion{
      Name: tree.Name($3),
      Region: tree.Name($9),
      IfNotExists: true,
    }
  }

alter_database_drop_region_stmt:
  ALTER DATABASE database_name DROP REGION region_name
  {
    $$.val = &tree.AlterDatabaseDropRegion{
      Name: tree.Name($3),
      Region: tree.Name($6),
    }
  }
| ALTER DATABASE database_name DROP REGION IF EXISTS region_name
  {
    $$.val = &tree.AlterDatabaseDropRegion{
      Name: tree.Name($3),
      Region: tree.Name($8),
      IfExists: true,
    }
  }

alter_database_survival_goal_stmt:
  ALTER DATABASE database_name survival_goal_clause
  {
    $$.val = &tree.AlterDatabaseSurvivalGoal{
      Name: tree.Name($3),
      SurvivalGoal: $4.survivalGoal(),
    }
  }

alter_database_primary_region_stmt:
  ALTER DATABASE database_name primary_region_clause
  {
    $$.val = &tree.AlterDatabasePrimaryRegion{
      Name: tree.Name($3),
      PrimaryRegion: tree.Name($4),
    }
  }
| ALTER DATABASE database_name SET primary_region_clause
  {
    $$.val = &tree.AlterDatabasePrimaryRegion{
      Name: tree.Name($3),
      PrimaryRegion: tree.Name($5),
    }
  }

alter_database_add_super_region:
  ALTER DATABASE database_name ADD SUPER REGION region_name VALUES region_name_list
  {
    $$.val = &tree.AlterDatabaseAddSuperRegion{
      DatabaseName: tree.Name($3),
      SuperRegionName: tree.Name($7),
      Regions: $9.nameList(),
    }
  }

alter_database_drop_super_region:
  ALTER DATABASE database_name DROP SUPER REGION region_name
  {
    $$.val = &tree.AlterDatabaseDropSuperRegion{
      DatabaseName: tree.Name($3),
      SuperRegionName: tree.Name($7),
    }
  }

alter_database_alter_super_region:
  ALTER DATABASE database_name ALTER SUPER REGION region_name VALUES region_name_list
  {
    $$.val = &tree.AlterDatabaseAlterSuperRegion{
      DatabaseName: tree.Name($3),
      SuperRegionName: tree.Name($7),
      Regions: $9.nameList(),
    }
  }

alter_database_set_secondary_region_stmt:
   ALTER DATABASE database_name SET secondary_region_clause
   {
     $$.val = &tree.AlterDatabaseSecondaryRegion{
       DatabaseName: tree.Name($3),
       SecondaryRegion: tree.Name($5),
     }
   }

alter_database_drop_secondary_region:
    ALTER DATABASE database_name DROP SECONDARY REGION
    {
      $$.val = &tree.AlterDatabaseDropSecondaryRegion{
        DatabaseName: tree.Name($3),
        IfExists: false,
      }
    }

  | ALTER DATABASE database_name DROP SECONDARY REGION IF EXISTS
    {
      $$.val = &tree.AlterDatabaseDropSecondaryRegion{
        DatabaseName: tree.Name($3),
        IfExists: true,
      }
    }

alter_database_set_zone_config_extension_stmt:
  ALTER DATABASE database_name ALTER LOCALITY GLOBAL set_zone_config
  {
    s := $7.setZoneConfig()
    $$.val = &tree.AlterDatabaseSetZoneConfigExtension{
      DatabaseName:  tree.Name($3),
      LocalityLevel: tree.LocalityLevelGlobal,
      ZoneConfigSettings: tree.ZoneConfigSettings {
        SetDefault:    s.SetDefault,
        YAMLConfig:    s.YAMLConfig,
        Options:       s.Options,
      },
    }
  }
| ALTER DATABASE database_name ALTER LOCALITY REGIONAL set_zone_config
  {
    s := $7.setZoneConfig()
    $$.val = &tree.AlterDatabaseSetZoneConfigExtension{
      DatabaseName:  tree.Name($3),
      LocalityLevel: tree.LocalityLevelTable,
      ZoneConfigSettings: tree.ZoneConfigSettings {
        SetDefault:    s.SetDefault,
        YAMLConfig:    s.YAMLConfig,
        Options:       s.Options,
      },
    }
  }
| ALTER DATABASE database_name ALTER LOCALITY REGIONAL IN region_name set_zone_config
  {
    s := $9.setZoneConfig()
    $$.val = &tree.AlterDatabaseSetZoneConfigExtension{
      DatabaseName:  tree.Name($3),
      LocalityLevel: tree.LocalityLevelTable,
      RegionName:    tree.Name($8),
      ZoneConfigSettings: tree.ZoneConfigSettings {
        SetDefault:    s.SetDefault,
        YAMLConfig:    s.YAMLConfig,
        Options:       s.Options,
      },
    }
  }

// %Help: ALTER RANGE - change the parameters of a range
// %Category: DDL
// %Text:
// ALTER RANGE <zonename> <command>
//
// Commands:
//   ALTER RANGE ... CONFIGURE ZONE <zoneconfig>
//   ALTER RANGE   RELOCATE { VOTERS | NONVOTERS } FROM <store_id> TO <store_id> FOR <selectclause>
//   ALTER RANGE r RELOCATE { VOTERS | NONVOTERS } FROM <store_id> TO <store_id>
//   ALTER RANGE   RELOCATE LEASE                                  TO <store_id> FOR <selectclause>
//   ALTER RANGE r RELOCATE LEASE                                  TO <store_id>
//
// Zone configurations:
//   DISCARD
//   USING <var> = <expr> [, ...]
//   USING <var> = COPY FROM PARENT [, ...]
//   { TO | = } <expr>
//
// %SeeAlso: ALTER TABLE
alter_range_stmt:
  alter_zone_range_stmt
| alter_range_relocate_stmt
| ALTER RANGE error // SHOW HELP: ALTER RANGE

// %Help: ALTER INDEX - change the definition of an index
// %Category: DDL
// %Text:
// ALTER INDEX [IF EXISTS] <idxname> <command>
//
// Commands:
//   ALTER INDEX ... RENAME TO <newname>
//   ALTER INDEX ... SPLIT AT <selectclause> [WITH EXPIRATION <expr>]
//   ALTER INDEX ... UNSPLIT AT <selectclause>
//   ALTER INDEX ... UNSPLIT ALL
//   ALTER INDEX ... SCATTER [ FROM ( <exprs...> ) TO ( <exprs...> ) ]
//   ALTER INDEX ... RELOCATE [ LEASE | VOTERS | NONVOTERS ] <selectclause>
//   ALTER INDEX ... [VISIBLE | NOT VISIBLE | INVISIBLE | VISIBILITY ...]
//
// Zone configurations:
//   DISCARD
//   USING <var> = <expr> [, ...]
//   USING <var> = COPY FROM PARENT [, ...]
//   { TO | = } <expr>
//
// %SeeAlso: WEBDOCS/alter-index.html
alter_index_stmt:
  alter_oneindex_stmt
| alter_relocate_index_stmt
| alter_split_index_stmt
| alter_unsplit_index_stmt
| alter_scatter_index_stmt
| alter_rename_index_stmt
| alter_zone_index_stmt
| alter_index_visible_stmt
// ALTER INDEX has its error help token here because the ALTER INDEX
// prefix is spread over multiple non-terminals.
| ALTER INDEX error // SHOW HELP: ALTER INDEX

alter_onetable_stmt:
  ALTER TABLE relation_expr alter_table_cmds
  {
    $$.val = &tree.AlterTable{Table: $3.unresolvedObjectName(), IfExists: false, Cmds: $4.alterTableCmds()}
  }
| ALTER TABLE IF EXISTS relation_expr alter_table_cmds
  {
    $$.val = &tree.AlterTable{Table: $5.unresolvedObjectName(), IfExists: true, Cmds: $6.alterTableCmds()}
  }

alter_oneindex_stmt:
  ALTER INDEX table_index_name alter_index_cmds
  {
    $$.val = &tree.AlterIndex{Index: $3.tableIndexName(), IfExists: false, Cmds: $4.alterIndexCmds()}
  }
| ALTER INDEX IF EXISTS table_index_name alter_index_cmds
  {
    $$.val = &tree.AlterIndex{Index: $5.tableIndexName(), IfExists: true, Cmds: $6.alterIndexCmds()}
  }

alter_split_stmt:
  ALTER TABLE table_name SPLIT AT select_stmt
  {
    name := $3.unresolvedObjectName().ToTableName()
    $$.val = &tree.Split{
      TableOrIndex: tree.TableIndexName{Table: name},
      Rows: $6.slct(),
      ExpireExpr: tree.Expr(nil),
    }
  }
| ALTER TABLE table_name SPLIT AT select_stmt WITH EXPIRATION a_expr
  {
    name := $3.unresolvedObjectName().ToTableName()
    $$.val = &tree.Split{
      TableOrIndex: tree.TableIndexName{Table: name},
      Rows: $6.slct(),
      ExpireExpr: $9.expr(),
    }
  }

alter_split_index_stmt:
  ALTER INDEX table_index_name SPLIT AT select_stmt
  {
    $$.val = &tree.Split{TableOrIndex: $3.tableIndexName(), Rows: $6.slct(), ExpireExpr: tree.Expr(nil)}
  }
| ALTER INDEX table_index_name SPLIT AT select_stmt WITH EXPIRATION a_expr
  {
    $$.val = &tree.Split{TableOrIndex: $3.tableIndexName(), Rows: $6.slct(), ExpireExpr: $9.expr()}
  }

alter_unsplit_stmt:
  ALTER TABLE table_name UNSPLIT AT select_stmt
  {
    name := $3.unresolvedObjectName().ToTableName()
    $$.val = &tree.Unsplit{
      TableOrIndex: tree.TableIndexName{Table: name},
      Rows: $6.slct(),
    }
  }
| ALTER TABLE table_name UNSPLIT ALL
  {
    name := $3.unresolvedObjectName().ToTableName()
    $$.val = &tree.Unsplit {
      TableOrIndex: tree.TableIndexName{Table: name},
      All: true,
    }
  }

alter_unsplit_index_stmt:
  ALTER INDEX table_index_name UNSPLIT AT select_stmt
  {
    $$.val = &tree.Unsplit{TableOrIndex: $3.tableIndexName(), Rows: $6.slct()}
  }
| ALTER INDEX table_index_name UNSPLIT ALL
  {
    $$.val = &tree.Unsplit{TableOrIndex: $3.tableIndexName(), All: true}
  }

relocate_kw:
  TESTING_RELOCATE
| EXPERIMENTAL_RELOCATE
| RELOCATE

relocate_subject:
  relocate_subject_nonlease
| LEASE
  {
    $$.val = tree.RelocateLease
  }

relocate_subject_nonlease:
  VOTERS
  {
    $$.val = tree.RelocateVoters
  }
| /* EMPTY */
  {
    // No keyword is an alias for VOTERS.
    $$.val = tree.RelocateVoters
  }
| NONVOTERS
  {
    $$.val = tree.RelocateNonVoters
  }

alter_relocate_stmt:
  ALTER TABLE table_name relocate_kw relocate_subject select_stmt
  {
    /* SKIP DOC */
    name := $3.unresolvedObjectName().ToTableName()
    $$.val = &tree.Relocate{
      TableOrIndex: tree.TableIndexName{Table: name},
      Rows: $6.slct(),
      SubjectReplicas: $5.relocateSubject(),
    }
  }

alter_relocate_index_stmt:
  ALTER INDEX table_index_name relocate_kw relocate_subject select_stmt
  {
    /* SKIP DOC */
    $$.val = &tree.Relocate{
      TableOrIndex: $3.tableIndexName(),
      Rows: $6.slct(),
      SubjectReplicas: $5.relocateSubject(),
    }
  }

alter_index_visible_stmt:
  ALTER INDEX table_index_name alter_index_visible
  {
    $$.val = &tree.AlterIndexVisible{
      Index: $3.tableIndexName(),
      Invisibility: $4.indexInvisibility(),
      IfExists: false,
    }
  }
| ALTER INDEX IF EXISTS table_index_name alter_index_visible
  {
    $$.val = &tree.AlterIndexVisible{
      Index: $5.tableIndexName(),
      Invisibility: $6.indexInvisibility(),
      IfExists: true,
    }
  }

alter_index_visible:
  NOT VISIBLE
  {
    $$.val = tree.IndexInvisibility{Value: 1.0}
  }
| INVISIBLE
  {
    $$.val = tree.IndexInvisibility{Value: 1.0}
  }
| VISIBLE
  {
    $$.val = tree.IndexInvisibility{Value: 0.0}
  }
| VISIBILITY FCONST
  {
    visibilityConst, _ := constant.Float64Val($2.numVal().AsConstantValue())
      if visibilityConst < 0.0 || visibilityConst > 1.0 {
        sqllex.Error("index visibility must be between 0 and 1")
        return 1
      }
    invisibilityConst := 1.0 - visibilityConst
    $$.val = tree.IndexInvisibility{Value: invisibilityConst, FloatProvided: true}
  }

// Note: even though the ALTER RANGE ... CONFIGURE ZONE syntax only
// accepts unrestricted names in the 3rd position, such that we could
// write:
//     ALTER RANGE zone_name set_zone_config
// we have to parse a full a_expr there instead, for otherwise we get
// a reduce/reduce conflict with the ALTER RANGE ... RELOCATE variants
// below.
//
// TODO(knz): Would it make sense to extend the semantics to enable
// zone configurations on arbitrary range IDs?
alter_zone_range_stmt:
  ALTER RANGE a_expr set_zone_config
  {
      var zoneName string
      switch e := $3.expr().(type) {
      case *tree.UnresolvedName:
          if e.NumParts != 1 {
              return setErr(sqllex, errors.New("only simple names are supported in ALTER RANGE ... CONFIGURE ZONE"))
          }
          zoneName = e.Parts[0]
      case tree.DefaultVal:
          zoneName = "default"
      default:
          return setErr(sqllex, errors.New("only simple names are supported in ALTER RANGE ... CONFIGURE ZONE"))
     }
     s := $4.setZoneConfig()
     s.ZoneSpecifier = tree.ZoneSpecifier{NamedZone: tree.UnrestrictedName(zoneName)}
     $$.val = s
  }

alter_range_relocate_stmt:
  ALTER RANGE relocate_kw LEASE TO a_expr FOR select_stmt
  {
    $$.val = &tree.RelocateRange{
      Rows: $8.slct(),
      FromStoreID: tree.DNull,
      ToStoreID: $6.expr(),
      SubjectReplicas: tree.RelocateLease,
    }
  }
| ALTER RANGE a_expr relocate_kw LEASE TO a_expr
    {
      $$.val = &tree.RelocateRange{
        Rows: &tree.Select{
          Select: &tree.ValuesClause{Rows: []tree.Exprs{tree.Exprs{$3.expr()}}},
        },
        FromStoreID: tree.DNull,
        ToStoreID: $7.expr(),
        SubjectReplicas: tree.RelocateLease,
      }
    }
| ALTER RANGE relocate_kw relocate_subject_nonlease FROM a_expr TO a_expr FOR select_stmt
  {
    $$.val = &tree.RelocateRange{
      Rows: $10.slct(),
      FromStoreID: $6.expr(),
      ToStoreID: $8.expr(),
      SubjectReplicas: $4.relocateSubject(),
    }
  }
| ALTER RANGE a_expr relocate_kw relocate_subject_nonlease FROM a_expr TO a_expr
  {
    $$.val = &tree.RelocateRange{
      Rows: &tree.Select{
        Select: &tree.ValuesClause{Rows: []tree.Exprs{tree.Exprs{$3.expr()}}},
      },
      FromStoreID: $7.expr(),
      ToStoreID: $9.expr(),
      SubjectReplicas: $5.relocateSubject(),
    }
  }

set_zone_config:
  CONFIGURE ZONE to_or_eq a_expr
  {
    /* SKIP DOC */
    $$.val = &tree.SetZoneConfig{
      ZoneConfigSettings: tree.ZoneConfigSettings {
        YAMLConfig:    $4.expr(),
      },
    }
  }
| CONFIGURE ZONE USING var_set_list
  {
    $$.val = &tree.SetZoneConfig{
      ZoneConfigSettings: tree.ZoneConfigSettings {
        Options: $4.kvOptions(),
      },
    }
  }
| CONFIGURE ZONE USING DEFAULT
  {
    /* SKIP DOC */
    $$.val = &tree.SetZoneConfig{
      ZoneConfigSettings: tree.ZoneConfigSettings {
        SetDefault: true,
      },
    }
  }
| CONFIGURE ZONE DISCARD
  {
    $$.val = &tree.SetZoneConfig{
      ZoneConfigSettings: tree.ZoneConfigSettings {
        Discard: true,
        YAMLConfig: tree.DNull,
      },
    }
  }

alter_zone_database_stmt:
  ALTER DATABASE database_name set_zone_config
  {
     s := $4.setZoneConfig()
     s.ZoneSpecifier = tree.ZoneSpecifier{Database: tree.Name($3)}
     $$.val = s
  }

alter_zone_table_stmt:
  ALTER TABLE table_name set_zone_config
  {
    name := $3.unresolvedObjectName().ToTableName()
    s := $4.setZoneConfig()
    s.ZoneSpecifier = tree.ZoneSpecifier{
       TableOrIndex: tree.TableIndexName{Table: name},
    }
    $$.val = s
  }

alter_zone_index_stmt:
  ALTER INDEX table_index_name set_zone_config
  {
    s := $4.setZoneConfig()
    s.ZoneSpecifier = tree.ZoneSpecifier{
       TableOrIndex: $3.tableIndexName(),
    }
    $$.val = s
  }

alter_zone_partition_stmt:
  ALTER PARTITION partition_name OF TABLE table_name set_zone_config
  {
    name := $6.unresolvedObjectName().ToTableName()
    s := $7.setZoneConfig()
    s.ZoneSpecifier = tree.ZoneSpecifier{
       TableOrIndex: tree.TableIndexName{Table: name},
       Partition: tree.Name($3),
    }
    $$.val = s
  }
| ALTER PARTITION partition_name OF INDEX table_index_name set_zone_config
  {
    s := $7.setZoneConfig()
    s.ZoneSpecifier = tree.ZoneSpecifier{
       TableOrIndex: $6.tableIndexName(),
       Partition: tree.Name($3),
    }
    $$.val = s
  }
| ALTER PARTITION partition_name OF INDEX table_name '@' '*' set_zone_config
  {
    name := $6.unresolvedObjectName().ToTableName()
    s := $9.setZoneConfig()
    s.ZoneSpecifier = tree.ZoneSpecifier{
       TableOrIndex: tree.TableIndexName{Table: name},
       Partition: tree.Name($3),
       StarIndex: true,
    }
    $$.val = s
  }
| ALTER PARTITION partition_name OF TABLE table_name '@' error
  {
    err := errors.New("index name should not be specified in ALTER PARTITION ... OF TABLE")
    err = errors.WithHint(err, "try ALTER PARTITION ... OF INDEX")
    return setErr(sqllex, err)
  }
| ALTER PARTITION partition_name OF TABLE table_name '@' '*' error
  {
    err := errors.New("index wildcard unsupported in ALTER PARTITION ... OF TABLE")
    err = errors.WithHint(err, "try ALTER PARTITION <partition> OF INDEX <tablename>@*")
    return setErr(sqllex, err)
  }

var_set_list:
  var_name '=' COPY FROM PARENT
  {
    $$.val = []tree.KVOption{tree.KVOption{Key: tree.Name(strings.Join($1.strs(), "."))}}
  }
| var_name '=' var_value
  {
    $$.val = []tree.KVOption{tree.KVOption{Key: tree.Name(strings.Join($1.strs(), ".")), Value: $3.expr()}}
  }
| var_set_list ',' var_name '=' var_value
  {
    $$.val = append($1.kvOptions(), tree.KVOption{Key: tree.Name(strings.Join($3.strs(), ".")), Value: $5.expr()})
  }
| var_set_list ',' var_name '=' COPY FROM PARENT
  {
    $$.val = append($1.kvOptions(), tree.KVOption{Key: tree.Name(strings.Join($3.strs(), "."))})
  }

alter_scatter_stmt:
  ALTER TABLE table_name SCATTER
  {
    name := $3.unresolvedObjectName().ToTableName()
    $$.val = &tree.Scatter{TableOrIndex: tree.TableIndexName{Table: name}}
  }
| ALTER TABLE table_name SCATTER FROM '(' expr_list ')' TO '(' expr_list ')'
  {
    name := $3.unresolvedObjectName().ToTableName()
    $$.val = &tree.Scatter{
      TableOrIndex: tree.TableIndexName{Table: name},
      From: $7.exprs(),
      To: $11.exprs(),
    }
  }

alter_scatter_index_stmt:
  ALTER INDEX table_index_name SCATTER
  {
    $$.val = &tree.Scatter{TableOrIndex: $3.tableIndexName()}
  }
| ALTER INDEX table_index_name SCATTER FROM '(' expr_list ')' TO '(' expr_list ')'
  {
    $$.val = &tree.Scatter{TableOrIndex: $3.tableIndexName(), From: $7.exprs(), To: $11.exprs()}
  }

alter_table_cmds:
  alter_table_cmd
  {
    $$.val = tree.AlterTableCmds{$1.alterTableCmd()}
  }
| alter_table_cmds ',' alter_table_cmd
  {
    $$.val = append($1.alterTableCmds(), $3.alterTableCmd())
  }

alter_table_cmd:
  // ALTER TABLE <name> RENAME [COLUMN] <name> TO <newname>
  RENAME opt_column column_name TO column_name
  {
    $$.val = &tree.AlterTableRenameColumn{Column: tree.Name($3), NewName: tree.Name($5) }
  }
  // ALTER TABLE <name> RENAME CONSTRAINT <name> TO <newname>
| RENAME CONSTRAINT column_name TO column_name
  {
    $$.val = &tree.AlterTableRenameConstraint{Constraint: tree.Name($3), NewName: tree.Name($5) }
  }
  // ALTER TABLE <name> ADD <coldef>
| ADD column_table_def
  {
    $$.val = &tree.AlterTableAddColumn{IfNotExists: false, ColumnDef: $2.colTableDef()}
  }
  // ALTER TABLE <name> ADD IF NOT EXISTS <coldef>
| ADD IF NOT EXISTS column_table_def
  {
    $$.val = &tree.AlterTableAddColumn{IfNotExists: true, ColumnDef: $5.colTableDef()}
  }
  // ALTER TABLE <name> ADD COLUMN <coldef>
| ADD COLUMN column_table_def
  {
    $$.val = &tree.AlterTableAddColumn{IfNotExists: false, ColumnDef: $3.colTableDef()}
  }
  // ALTER TABLE <name> ADD COLUMN IF NOT EXISTS <coldef>
| ADD COLUMN IF NOT EXISTS column_table_def
  {
    $$.val = &tree.AlterTableAddColumn{IfNotExists: true, ColumnDef: $6.colTableDef()}
  }
  // ALTER TABLE <name> ALTER [COLUMN] <colname> {SET DEFAULT <expr>|DROP DEFAULT}
| ALTER opt_column column_name alter_column_default
  {
    $$.val = &tree.AlterTableSetDefault{Column: tree.Name($3), Default: $4.expr()}
  }
  // ALTER TABLE <name> ALTER [COLUMN] <colname> {SET ON UPDATE <expr>|DROP ON UPDATE}
| ALTER opt_column column_name alter_column_on_update
  {
    $$.val = &tree.AlterTableSetOnUpdate{Column: tree.Name($3), Expr: $4.expr()}
  }
  // ALTER TABLE <name> ALTER [COLUMN] <colname> SET {VISIBLE|NOT VISIBLE}
| ALTER opt_column column_name alter_column_visible
  {
    $$.val = &tree.AlterTableSetVisible{Column: tree.Name($3), Visible: $4.bool()}
  }
  // ALTER TABLE <name> ALTER [COLUMN] <colname> DROP NOT NULL
| ALTER opt_column column_name DROP NOT NULL
  {
    $$.val = &tree.AlterTableDropNotNull{Column: tree.Name($3)}
  }
  // ALTER TABLE <name> ALTER [COLUMN] <colname> ADD GENERATED ALWAYS AS IDENTITY
| ALTER opt_column column_name ADD generated_always_as IDENTITY {
    qualification := tree.GeneratedAlwaysAsIdentity{}
    $$.val = &tree.AlterTableAddIdentity{Column: tree.Name($3), Qualification: &qualification}
  }
  // ALTER TABLE <name> ALTER [COLUMN] <colname> ADD GENERATED BY DEFAULT AS IDENTITY
| ALTER opt_column column_name ADD generated_by_default_as IDENTITY {
    qualification := tree.GeneratedByDefAsIdentity{}
    $$.val = &tree.AlterTableAddIdentity{Column: tree.Name($3), Qualification: &qualification}
  }
  // ALTER TABLE <name> ALTER [COLUMN] <colname> ADD GENERATED ALWAYS AS IDENTITY ( <opt_sequence_option_list> )
| ALTER opt_column column_name ADD generated_always_as IDENTITY '(' opt_sequence_option_list ')' {
    qualification := tree.GeneratedAlwaysAsIdentity{
      SeqOptions: $8.seqOpts(),
    }
    $$.val = &tree.AlterTableAddIdentity{Column: tree.Name($3), Qualification: &qualification}
  }
  // ALTER TABLE <name> ALTER [COLUMN] <colname> ADD GENERATED BY DEFAULT AS IDENTITY ( <opt_sequence_option_list> )
| ALTER opt_column column_name ADD generated_by_default_as IDENTITY '(' opt_sequence_option_list ')' {
    qualification := tree.GeneratedByDefAsIdentity{
      SeqOptions: $8.seqOpts(),
    }
    $$.val = &tree.AlterTableAddIdentity{Column: tree.Name($3), Qualification: &qualification}
}
  // ALTER TABLE <name> ALTER [COLUMN] <colname> SET GENERATED ALWAYS
| ALTER opt_column column_name set_generated_always {
    $$.val = &tree.AlterTableSetIdentity{Column: tree.Name($3), GeneratedAsIdentityType: tree.GeneratedAlways}
  }
  // ALTER TABLE <name> ALTER [COLUMN] <colname> SET GENERATED BY DEFAULT
| ALTER opt_column column_name set_generated_default {
    $$.val = &tree.AlterTableSetIdentity{Column: tree.Name($3), GeneratedAsIdentityType: tree.GeneratedByDefault}
  }
  // ALTER TABLE <name> ALTER [COLUMN] <colname> <identity_option_list>
| ALTER opt_column column_name identity_option_list
  {
    $$.val = &tree.AlterTableIdentity{Column: tree.Name($3), SeqOptions: $4.seqOpts()}
  }
  // ALTER TABLE <name> ALTER [COLUMN] <colname> DROP IDENTITY
| ALTER opt_column column_name DROP IDENTITY
  {
    $$.val = &tree.AlterTableDropIdentity{Column: tree.Name($3), IfExists: false}
  }
  // ALTER TABLE <name> ALTER [COLUMN] <colname> DROP IDENTITY IF EXISTS
| ALTER opt_column column_name DROP IDENTITY IF EXISTS
  {
    $$.val = &tree.AlterTableDropIdentity{Column: tree.Name($3), IfExists: true}
  }
  // ALTER TABLE <name> ALTER [COLUMN] <colname> DROP STORED
| ALTER opt_column column_name DROP STORED
  {
    $$.val = &tree.AlterTableDropStored{Column: tree.Name($3)}
  }
  // ALTER TABLE <name> ALTER [COLUMN] <colname> SET NOT NULL
| ALTER opt_column column_name SET NOT NULL
  {
    $$.val = &tree.AlterTableSetNotNull{Column: tree.Name($3)}
  }
| ALTER opt_column column_name ADD error
  {
    return unimplemented(sqllex, "alter table alter column add")
  }
  // ALTER TABLE <name> DROP [COLUMN] IF EXISTS <colname> [RESTRICT|CASCADE]
| DROP opt_column IF EXISTS column_name opt_drop_behavior
  {
    $$.val = &tree.AlterTableDropColumn{
      IfExists: true,
      Column: tree.Name($5),
      DropBehavior: $6.dropBehavior(),
    }
  }
  // ALTER TABLE <name> DROP [COLUMN] <colname> [RESTRICT|CASCADE]
| DROP opt_column column_name opt_drop_behavior
  {
    $$.val = &tree.AlterTableDropColumn{
      IfExists: false,
      Column: tree.Name($3),
      DropBehavior: $4.dropBehavior(),
    }
  }
  // ALTER TABLE <name> ALTER [COLUMN] <colname>
  //     [SET DATA] TYPE <typename>
  //     [ COLLATE collation ]
  //     [ USING <expression> ]
| ALTER opt_column column_name opt_set_data TYPE typename opt_collate opt_alter_column_using
  {
    $$.val = &tree.AlterTableAlterColumnType{
      Column: tree.Name($3),
      ToType: $6.typeReference(),
      Collation: $7,
      Using: $8.expr(),
    }
  }
  // ALTER TABLE <name> ADD CONSTRAINT ...
| ADD table_constraint opt_validate_behavior
  {
    $$.val = &tree.AlterTableAddConstraint{
      ConstraintDef: $2.constraintDef(),
      ValidationBehavior: $3.validationBehavior(),
    }
  }
  // ALTER TABLE <name> ADD CONSTRAINT IF NOT EXISTS ...
| ADD CONSTRAINT IF NOT EXISTS constraint_name constraint_elem opt_validate_behavior
  {
    def := $7.constraintDef()
    def.SetName(tree.Name($6))
    def.SetIfNotExists()
    $$.val = &tree.AlterTableAddConstraint{
      ConstraintDef: def,
      ValidationBehavior: $8.validationBehavior(),
    }
  }
  // ALTER TABLE <name> ALTER CONSTRAINT ...
| ALTER CONSTRAINT constraint_name error { return unimplementedWithIssueDetail(sqllex, 31632, "alter constraint") }
  // ALTER TABLE <name> INHERITS ....
| INHERITS error
  {
    /* SKIP DOC */
    return unimplementedWithIssueDetail(sqllex, 22456, "alter table inherits")
  }
  // ALTER TABLE <name> NO INHERITS ....
| NO INHERITS error
  {
    /* SKIP DOC */
    return unimplementedWithIssueDetail(sqllex, 22456, "alter table no inherits")
  }
  // ALTER TABLE <name> ALTER PRIMARY KEY USING COLUMNS ( <colnames...> )
| ALTER PRIMARY KEY USING COLUMNS '(' index_params ')' opt_hash_sharded opt_with_storage_parameter_list
  {
    $$.val = &tree.AlterTableAlterPrimaryKey{
      Columns: $7.idxElems(),
      Sharded: $9.shardedIndexDef(),
      StorageParams: $10.storageParams(),
    }
  }
  // ALTER TABLE <name> VALIDATE CONSTRAINT ...
| VALIDATE CONSTRAINT constraint_name
  {
    $$.val = &tree.AlterTableValidateConstraint{
      Constraint: tree.Name($3),
    }
  }
  // ALTER TABLE <name> DROP CONSTRAINT IF EXISTS <name> [RESTRICT|CASCADE]
| DROP CONSTRAINT IF EXISTS constraint_name opt_drop_behavior
  {
    $$.val = &tree.AlterTableDropConstraint{
      IfExists: true,
      Constraint: tree.Name($5),
      DropBehavior: $6.dropBehavior(),
    }
  }
  // ALTER TABLE <name> DROP CONSTRAINT <name> [RESTRICT|CASCADE]
| DROP CONSTRAINT constraint_name opt_drop_behavior
  {
    $$.val = &tree.AlterTableDropConstraint{
      IfExists: false,
      Constraint: tree.Name($3),
      DropBehavior: $4.dropBehavior(),
    }
  }
  // ALTER TABLE <name> EXPERIMENTAL_AUDIT SET <mode>
| EXPERIMENTAL_AUDIT SET audit_mode
  {
    $$.val = &tree.AlterTableSetAudit{Mode: $3.auditMode()}
  }
  // ALTER TABLE <name> PARTITION BY ...
| partition_by_table
  {
    $$.val = &tree.AlterTablePartitionByTable{
      PartitionByTable: $1.partitionByTable(),
    }
  }
  // ALTER TABLE <name> INJECT STATISTICS <json>
| INJECT STATISTICS a_expr
  {
    /* SKIP DOC */
    $$.val = &tree.AlterTableInjectStats{
      Stats: $3.expr(),
    }
  }
| SET '(' storage_parameter_list ')'
  {
    $$.val = &tree.AlterTableSetStorageParams{
      StorageParams: $3.storageParams(),
    }
  }
| RESET '(' storage_parameter_key_list ')'
  {
    $$.val = &tree.AlterTableResetStorageParams{
      Params: $3.storageParamKeys(),
    }
  }
| table_rls_mode ROW LEVEL SECURITY
  {
    /* SKIP DOC */
    $$.val = &tree.AlterTableSetRLSMode{
      Mode: $1.rlsTableMode(),
    }
  }

audit_mode:
  READ WRITE { $$.val = tree.AuditModeReadWrite }
| OFF        { $$.val = tree.AuditModeDisable }

alter_index_cmds:
  alter_index_cmd
  {
    $$.val = tree.AlterIndexCmds{$1.alterIndexCmd()}
  }
| alter_index_cmds ',' alter_index_cmd
  {
    $$.val = append($1.alterIndexCmds(), $3.alterIndexCmd())
  }

alter_index_cmd:
  partition_by_index
  {
    $$.val = &tree.AlterIndexPartitionBy{
      PartitionByIndex: $1.partitionByIndex(),
    }
  }

alter_column_default:
  SET DEFAULT a_expr
  {
    $$.val = $3.expr()
  }
| DROP DEFAULT
  {
    $$.val = nil
  }

alter_column_on_update:
  SET ON UPDATE a_expr
  {
    $$.val = $4.expr()
  }
| DROP ON UPDATE
  {
    $$.val = nil
  }

alter_column_visible:
  SET VISIBLE
  {
    $$.val = true
  }
| SET NOT VISIBLE
  {
    $$.val = false
  }

opt_alter_column_using:
  USING a_expr
  {
     $$.val = $2.expr()
  }
| /* EMPTY */
  {
     $$.val = nil
  }


opt_drop_behavior:
  CASCADE
  {
    $$.val = tree.DropCascade
  }
| RESTRICT
  {
    $$.val = tree.DropRestrict
  }
| /* EMPTY */
  {
    $$.val = tree.DropDefault
  }

opt_validate_behavior:
  NOT VALID
  {
    $$.val = tree.ValidationSkip
  }
| /* EMPTY */
  {
    $$.val = tree.ValidationDefault
  }

identity_option_elem:
  SET CYCLE                        { /* SKIP DOC */
                                  $$.val = tree.SequenceOption{Name: tree.SeqOptCycle} }
  | SET NO CYCLE                     { $$.val = tree.SequenceOption{Name: tree.SeqOptNoCycle} }
  | SET CACHE signed_iconst64        { x := $3.int64()
                                  $$.val = tree.SequenceOption{Name: tree.SeqOptCache, IntVal: &x} }
  | SET INCREMENT signed_iconst64    { x := $3.int64()
                                  $$.val = tree.SequenceOption{Name: tree.SeqOptIncrement, IntVal: &x} }
  | SET INCREMENT BY signed_iconst64 { x := $4.int64()
                                  $$.val = tree.SequenceOption{Name: tree.SeqOptIncrement, IntVal: &x, OptionalWord: true} }
  | SET MINVALUE signed_iconst64     { x := $3.int64()
                                  $$.val = tree.SequenceOption{Name: tree.SeqOptMinValue, IntVal: &x} }
  | SET NO MINVALUE                  { $$.val = tree.SequenceOption{Name: tree.SeqOptMinValue} }
  | SET MAXVALUE signed_iconst64     { x := $3.int64()
                                  $$.val = tree.SequenceOption{Name: tree.SeqOptMaxValue, IntVal: &x} }
  | SET NO MAXVALUE                  { $$.val = tree.SequenceOption{Name: tree.SeqOptMaxValue} }
  | SET START signed_iconst64        { x := $3.int64()
                                  $$.val = tree.SequenceOption{Name: tree.SeqOptStart, IntVal: &x} }
  | SET START WITH signed_iconst64   { x := $4.int64()
                                  $$.val = tree.SequenceOption{Name: tree.SeqOptStart, IntVal: &x, OptionalWord: true} }
  | RESTART                      { $$.val = tree.SequenceOption{Name: tree.SeqOptRestart} }
  | RESTART signed_iconst64      { x := $2.int64()
                                  $$.val = tree.SequenceOption{Name: tree.SeqOptRestart, IntVal: &x} }
  | RESTART WITH signed_iconst64 { x := $3.int64()
                                  $$.val = tree.SequenceOption{Name: tree.SeqOptRestart, IntVal: &x, OptionalWord: true} }
  | SET sequence_option_elem error                      { return setErr(sqllex, errors.Newf("sequence option \"%s\" not supported here", $2.seqOpt().Name)) }

identity_option_list:
  identity_option_elem                       { $$.val = []tree.SequenceOption{$1.seqOpt()} }
| identity_option_list identity_option_elem  { $$.val = append($1.seqOpts(), $2.seqOpt()) }

// %Help: ALTER TYPE - change the definition of a type.
// %Category: DDL
// %Text: ALTER TYPE <typename> <command>
//
// Commands:
//   ALTER TYPE ... ADD VALUE [IF NOT EXISTS] <value> [ { BEFORE | AFTER } <value> ]
//   ALTER TYPE ... RENAME VALUE <oldname> TO <newname>
//   ALTER TYPE ... RENAME TO <newname>
//   ALTER TYPE ... SET SCHEMA <newschemaname>
//   ALTER TYPE ... OWNER TO {<newowner> | CURRENT_USER | SESSION_USER }
//   ALTER TYPE ... RENAME ATTRIBUTE <oldname> TO <newname> [ CASCADE | RESTRICT ]
//   ALTER TYPE ... <attributeaction> [, ... ]
//
// Attribute action:
//   ADD ATTRIBUTE <name> <type> [ COLLATE <collation> ] [ CASCADE | RESTRICT ]
//   DROP ATTRIBUTE [IF EXISTS] <name> [ CASCADE | RESTRICT ]
//   ALTER ATTRIBUTE <name> [ SET DATA ] TYPE <type> [ COLLATE <collation> ] [ CASCADE | RESTRICT ]
//
// %SeeAlso: WEBDOCS/alter-type.html
alter_type_stmt:
  ALTER TYPE type_name ADD VALUE SCONST opt_add_val_placement
  {
    $$.val = &tree.AlterType{
      Type: $3.unresolvedObjectName(),
      Cmd: &tree.AlterTypeAddValue{
        NewVal: tree.EnumValue($6),
        IfNotExists: false,
        Placement: $7.alterTypeAddValuePlacement(),
      },
    }
  }
| ALTER TYPE type_name ADD VALUE IF NOT EXISTS SCONST opt_add_val_placement
  {
    $$.val = &tree.AlterType{
      Type: $3.unresolvedObjectName(),
      Cmd: &tree.AlterTypeAddValue{
        NewVal: tree.EnumValue($9),
        IfNotExists: true,
        Placement: $10.alterTypeAddValuePlacement(),
      },
    }
  }
| ALTER TYPE type_name DROP VALUE SCONST
 {
   $$.val = &tree.AlterType{
     Type: $3.unresolvedObjectName(),
     Cmd: &tree.AlterTypeDropValue{
       Val: tree.EnumValue($6),
     },
   }
 }
| ALTER TYPE type_name RENAME VALUE SCONST TO SCONST
  {
    $$.val = &tree.AlterType{
      Type: $3.unresolvedObjectName(),
      Cmd: &tree.AlterTypeRenameValue{
        OldVal: tree.EnumValue($6),
        NewVal: tree.EnumValue($8),
      },
    }
  }
| ALTER TYPE type_name RENAME TO name
  {
    $$.val = &tree.AlterType{
      Type: $3.unresolvedObjectName(),
      Cmd: &tree.AlterTypeRename{
        NewName: tree.Name($6),
      },
    }
  }
| ALTER TYPE type_name SET SCHEMA schema_name
  {
    $$.val = &tree.AlterType{
      Type: $3.unresolvedObjectName(),
      Cmd: &tree.AlterTypeSetSchema{
        Schema: tree.Name($6),
      },
    }
  }
| ALTER TYPE type_name OWNER TO role_spec
  {
    $$.val = &tree.AlterType{
      Type: $3.unresolvedObjectName(),
      Cmd: &tree.AlterTypeOwner{
        Owner: $6.roleSpec(),
      },
    }
  }
| ALTER TYPE type_name RENAME ATTRIBUTE column_name TO column_name opt_drop_behavior
  {
    return unimplementedWithIssueDetail(sqllex, 48701, "ALTER TYPE ATTRIBUTE")
  }
| ALTER TYPE type_name alter_attribute_action_list
  {
    return unimplementedWithIssueDetail(sqllex, 48701, "ALTER TYPE ATTRIBUTE")
  }
| ALTER TYPE error // SHOW HELP: ALTER TYPE

opt_add_val_placement:
  BEFORE SCONST
  {
    $$.val = &tree.AlterTypeAddValuePlacement{
       Before: true,
       ExistingVal: tree.EnumValue($2),
    }
  }
| AFTER SCONST
  {
    $$.val = &tree.AlterTypeAddValuePlacement{
       Before: false,
       ExistingVal: tree.EnumValue($2),
    }
  }
| /* EMPTY */
  {
    $$.val = (*tree.AlterTypeAddValuePlacement)(nil)
  }

role_spec:
  IDENT
  {
    $$.val = tree.RoleSpec{
      RoleSpecType: tree.RoleName,
      Name: $1,
    }
  }
| unreserved_keyword
  {
    $$.val = tree.RoleSpec{
      RoleSpecType: tree.RoleName,
      Name: $1,
    }
  }
| CURRENT_USER
  {
    $$.val = tree.RoleSpec{
      RoleSpecType: tree.CurrentUser,
    }
  }
| SESSION_USER
  {
    $$.val = tree.RoleSpec{
      RoleSpecType: tree.SessionUser,
     }
  }

role_spec_list:
  role_spec
  {
    $$.val = tree.RoleSpecList{$1.roleSpec()}
  }
| role_spec_list ',' role_spec
  {
    $$.val = append($1.roleSpecList(), $3.roleSpec())
  }

alter_attribute_action_list:
  alter_attribute_action
| alter_attribute_action_list ',' alter_attribute_action

alter_attribute_action:
  ADD ATTRIBUTE column_name type_name opt_collate opt_drop_behavior
| DROP ATTRIBUTE column_name opt_drop_behavior
| DROP ATTRIBUTE IF EXISTS column_name opt_drop_behavior
| ALTER ATTRIBUTE column_name TYPE type_name opt_collate opt_drop_behavior
| ALTER ATTRIBUTE column_name SET DATA TYPE type_name opt_collate opt_drop_behavior

// %Help: REFRESH - recalculate a materialized view
// %Category: Misc
// %Text:
// REFRESH MATERIALIZED VIEW [CONCURRENTLY] view_name [WITH [NO] DATA]
refresh_stmt:
  REFRESH MATERIALIZED VIEW opt_concurrently view_name opt_clear_data
  {
    $$.val = &tree.RefreshMaterializedView{
      Name: $5.unresolvedObjectName(),
      Concurrently: $4.bool(),
      RefreshDataOption: $6.refreshDataOption(),
    }
  }
| REFRESH error // SHOW HELP: REFRESH

opt_clear_data:
  WITH DATA
  {
    $$.val = tree.RefreshDataWithData
  }
| WITH NO DATA
  {
    $$.val = tree.RefreshDataClear
  }
| /* EMPTY */
  {
    $$.val = tree.RefreshDataDefault
  }

// %Help: BACKUP - back up data to external storage
// %Category: CCL
// %Text:
//
// Create a full backup
// BACKUP <targets...> INTO <destination...>
//        [ AS OF SYSTEM TIME <expr> ]
//				[ WITH <option> [= <value>] [, ...] ]
//
// Append an incremental backup to the most recent backup added to a collection
// BACKUP <targets...> INTO LATEST IN <destination...>
//        [ AS OF SYSTEM TIME <expr> ]
//				[ WITH <option> [= <value>] [, ...] ]
//
//
// Append an incremental backup in the <subdir>. This command will create an
// incremental backup iff there is a full backup in <destination>
// BACKUP <targets...> INTO [<subdir...> IN] <destination>
//        [ AS OF SYSTEM TIME <expr> ]
//				[ WITH <option> [= <value>] [, ...] ]
//
// Targets:
//    Empty targets list: backup full cluster.
//    TABLE <pattern> [, ...]
//    DATABASE <databasename> [, ...]
//
// Destination:
//    "[scheme]://[host]/[path to backup]?[parameters]"
//
// Options:
//    revision_history: enable revision history
//    encryption_passphrase="secret": encrypt backups
//    kms="[kms_provider]://[kms_host]/[master_key_identifier]?[parameters]" : encrypt backups using KMS
//    detached: execute backup job asynchronously, without waiting for its completion
//    incremental_location: specify a different path to store the incremental backup
//    include_all_virtual_clusters: enable backups of all virtual clusters during a cluster backup
//
// %SeeAlso: RESTORE, WEBDOCS/backup.html
backup_stmt:
  BACKUP opt_backup_targets INTO sconst_or_placeholder IN string_or_placeholder_opt_list opt_as_of_clause opt_with_backup_options
  {
    $$.val = &tree.Backup{
      Targets: $2.backupTargetListPtr(),
      To: $6.stringOrPlaceholderOptList(),
      AppendToLatest: false,
      Subdir: $4.expr(),
      AsOf: $7.asOfClause(),
      Options: *$8.backupOptions(),
    }
  }
| BACKUP opt_backup_targets INTO string_or_placeholder_opt_list opt_as_of_clause opt_with_backup_options
  {
    $$.val = &tree.Backup{
      Targets: $2.backupTargetListPtr(),
      To: $4.stringOrPlaceholderOptList(),
      AsOf: $5.asOfClause(),
      Options: *$6.backupOptions(),
    }
  }
| BACKUP opt_backup_targets INTO LATEST IN string_or_placeholder_opt_list opt_as_of_clause opt_with_backup_options
  {
    $$.val = &tree.Backup{
      Targets: $2.backupTargetListPtr(),
      To: $6.stringOrPlaceholderOptList(),
      AppendToLatest: true,
      AsOf: $7.asOfClause(),
      Options: *$8.backupOptions(),
    }
  }
| BACKUP opt_backup_targets TO error
  {
    setErr(sqllex, errors.New("The `BACKUP TO` syntax is no longer supported. Please use `BACKUP INTO` to create a backup collection."))
    return helpWith(sqllex, "BACKUP")
  }
| BACKUP error // SHOW HELP: BACKUP

opt_backup_targets:
  /* EMPTY -- full cluster */
  {
    $$.val = (*tree.BackupTargetList)(nil)
  }
| backup_targets
  {
    t := $1.backupTargetList()
    $$.val = &t
  }

// Optional backup options.
opt_with_backup_options:
  WITH backup_options_list
  {
    $$.val = $2.backupOptions()
  }
| WITH OPTIONS '(' backup_options_list ')'
  {
    $$.val = $4.backupOptions()
  }
| /* EMPTY */
  {
    $$.val = &tree.BackupOptions{}
  }

backup_options_list:
  // Require at least one option
  backup_options
  {
    $$.val = $1.backupOptions()
  }
| backup_options_list ',' backup_options
  {
    if err := $1.backupOptions().CombineWith($3.backupOptions()); err != nil {
      return setErr(sqllex, err)
    }
  }

// List of valid backup options.
backup_options:
  ENCRYPTION_PASSPHRASE '=' string_or_placeholder
  {
    $$.val = &tree.BackupOptions{EncryptionPassphrase: $3.expr()}
  }
| REVISION_HISTORY
  {
    $$.val = &tree.BackupOptions{CaptureRevisionHistory: tree.MakeDBool(true)}
  }
| REVISION_HISTORY '=' a_expr
  {
    $$.val = &tree.BackupOptions{CaptureRevisionHistory: $3.expr()}
  }
| DETACHED
  {
    $$.val = &tree.BackupOptions{Detached: tree.MakeDBool(true)}
  }
| DETACHED '=' TRUE
  {
    $$.val = &tree.BackupOptions{Detached: tree.MakeDBool(true)}
  }
| DETACHED '=' FALSE
  {
    $$.val = &tree.BackupOptions{Detached: tree.MakeDBool(false)}
  }
| KMS '=' string_or_placeholder_opt_list
  {
    $$.val = &tree.BackupOptions{EncryptionKMSURI: $3.stringOrPlaceholderOptList()}
  }
| INCREMENTAL_LOCATION '=' string_or_placeholder_opt_list
  {
    $$.val = &tree.BackupOptions{IncrementalStorage: $3.stringOrPlaceholderOptList()}
  }
| EXECUTION LOCALITY '=' string_or_placeholder
  {
    $$.val = &tree.BackupOptions{ExecutionLocality: $4.expr()}
  }
| include_all_clusters
  {
    /* SKIP DOC */
    $$.val = &tree.BackupOptions{IncludeAllSecondaryTenants: tree.MakeDBool(true)}
  }
| include_all_clusters '=' a_expr
  {
    $$.val = &tree.BackupOptions{IncludeAllSecondaryTenants: $3.expr()}
  }
| UPDATES_CLUSTER_MONITORING_METRICS
  {
    $$.val = &tree.BackupOptions{UpdatesClusterMonitoringMetrics: tree.MakeDBool(true)}
  }
| UPDATES_CLUSTER_MONITORING_METRICS '=' a_expr
  {
    $$.val = &tree.BackupOptions{UpdatesClusterMonitoringMetrics: $3.expr()}
  }

include_all_clusters:
  INCLUDE_ALL_SECONDARY_TENANTS { /* SKIP DOC */ }
| INCLUDE_ALL_VIRTUAL_CLUSTERS { }

// %Help: CREATE SCHEDULE FOR BACKUP - backup data periodically
// %Category: CCL
// %Text:
// CREATE SCHEDULE [IF NOT EXISTS]
// [<description>]
// FOR BACKUP [<targets>] INTO <location...>
// [WITH <backup_option>[=<value>] [, ...]]
// RECURRING [crontab|NEVER] [FULL BACKUP <crontab|ALWAYS>]
// [WITH EXPERIMENTAL SCHEDULE OPTIONS <schedule_option>[= <value>] [, ...] ]
//
// All backups run in UTC timezone.
//
// Description:
//   Optional description (or name) for this schedule
//
// Targets:
//   empty targets: Backup entire cluster
//   DATABASE <pattern> [, ...]: comma separated list of databases to backup.
//   TABLE <pattern> [, ...]: comma separated list of tables to backup.
//
// Location:
//   "[scheme]://[host]/[path prefix to backup]?[parameters]"
//   Backup schedule will create subdirectories under this location to store
//   full and periodic backups.
//
// WITH <options>:
//   Options specific to BACKUP: See BACKUP options
//
// RECURRING <crontab>:
//   The RECURRING expression specifies when we backup.  By default these are incremental
//   backups that capture changes since the last backup, writing to the "current" backup.
//
//   Schedule specified as a string in crontab format.
//   All times in UTC.
//     "5 0 * * *": run schedule 5 minutes past midnight.
//     "@daily": run daily, at midnight
//   See https://en.wikipedia.org/wiki/Cron
//
// FULL BACKUP <crontab|ALWAYS>:
//   The optional FULL BACKUP '<cron expr>' clause specifies when we'll start a new full backup,
//   which becomes the "current" backup when complete.
//   If FULL BACKUP ALWAYS is specified, then the backups triggered by the RECURRING clause will
//   always be full backups. For free users, this is the only accepted value of FULL BACKUP.
//
//   If the FULL BACKUP clause is omitted, we will select a reasonable default:
//      * RECURRING <= 1 hour: we default to FULL BACKUP '@daily';
//      * RECURRING <= 1 day:  we default to FULL BACKUP '@weekly';
//      * Otherwise: we default to FULL BACKUP ALWAYS.
//
//  SCHEDULE OPTIONS:
//   The schedule can be modified by specifying the following options (which are considered
//   to be experimental at this time):
//   * first_run=TIMESTAMPTZ:
//     execute the schedule at the specified time. If not specified, the default is to execute
//     the scheduled based on it's next RECURRING time.
//   * on_execution_failure='[retry|reschedule|pause]':
//     If an error occurs during the execution, handle the error based as:
//     * retry: retry execution right away
//     * reschedule: retry execution by rescheduling it based on its RECURRING expression.
//       This is the default.
//     * pause: pause this schedule.  Requires manual intervention to unpause.
//   * on_previous_running='[start|skip|wait]':
//     If the previous backup started by this schedule still running, handle this as:
//     * start: start this execution anyway, even if the previous one still running.
//     * skip: skip this execution, reschedule it based on RECURRING (or change_capture_period)
//       expression.
//     * wait: wait for the previous execution to complete.  This is the default.
//   * ignore_existing_backups
//     If backups were already created in the destination in which a new schedule references,
//     this flag must be passed in to acknowledge that the new schedule may be backing up different
//     objects.
//
// %SeeAlso: BACKUP
create_schedule_for_backup_stmt:
 CREATE SCHEDULE /*$3=*/schedule_label_spec FOR BACKUP /*$6=*/opt_backup_targets INTO
  /*$8=*/string_or_placeholder_opt_list /*$9=*/opt_with_backup_options
  /*$10=*/cron_expr /*$11=*/opt_full_backup_clause /*$12=*/opt_with_schedule_options
  {
  $$.val = &tree.ScheduledBackup{
        ScheduleLabelSpec:    *($3.scheduleLabelSpec()),
        Recurrence:           $10.expr(),
        FullBackup:           $11.fullBackupClause(),
        To:                   $8.stringOrPlaceholderOptList(),
        Targets:              $6.backupTargetListPtr(),
        BackupOptions:        *($9.backupOptions()),
        ScheduleOptions:      $12.kvOptions(),
      }
  }
 | CREATE SCHEDULE schedule_label_spec FOR BACKUP error // SHOW HELP: CREATE SCHEDULE FOR BACKUP

// %Help: ALTER BACKUP SCHEDULE - alter an existing backup schedule
// %Category: CCL
// %Text:
// ALTER BACKUP SCHEDULE <id> <command> [, ...]
//
// Commands:
//   ALTER BACKUP SCHEDULE ... SET LABEL <label>
//   ALTER BACKUP SCHEDULE ... SET INTO <destination>
//   ALTER BACKUP SCHEDULE ... SET WITH <option>
//   ALTER BACKUP SCHEDULE ... SET RECURRING <crontab>
//   ALTER BACKUP SCHEDULE ... SET FULL BACKUP <crontab|ALWAYS>
//   ALTER BACKUP SCHEDULE ... SET SCHEDULE OPTION <option>
//
// See CREATE SCHEDULE FOR BACKUP for detailed option descriptions.
// %SeeAlso: CREATE SCHEDULE FOR BACKUP
alter_backup_schedule:
  ALTER BACKUP SCHEDULE iconst64 alter_backup_schedule_cmds
  {
    $$.val = &tree.AlterBackupSchedule{
			ScheduleID: uint64($4.int64()),
			Cmds:       $5.alterBackupScheduleCmds(),
    }
  }
  | ALTER BACKUP SCHEDULE error  // SHOW HELP: ALTER BACKUP SCHEDULE


alter_backup_schedule_cmds:
  alter_backup_schedule_cmd
  {
    $$.val = tree.AlterBackupScheduleCmds{$1.alterBackupScheduleCmd()}
  }
| alter_backup_schedule_cmds ',' alter_backup_schedule_cmd
  {
    $$.val = append($1.alterBackupScheduleCmds(), $3.alterBackupScheduleCmd())
  }


alter_backup_schedule_cmd:
  SET LABEL string_or_placeholder
	{
		$$.val = &tree.AlterBackupScheduleSetLabel{
		  Label: $3.expr(),
		}
	}
|	SET INTO string_or_placeholder_opt_list
  {
		$$.val = &tree.AlterBackupScheduleSetInto{
		  Into: $3.stringOrPlaceholderOptList(),
		}
  }
| SET WITH backup_options
	{
		$$.val = &tree.AlterBackupScheduleSetWith{
		  With: $3.backupOptions(),
		}
	}
| SET cron_expr
  {
		$$.val = &tree.AlterBackupScheduleSetRecurring{
		  Recurrence: $2.expr(),
		}
  }
| SET FULL BACKUP ALWAYS
  {
		$$.val = &tree.AlterBackupScheduleSetFullBackup{
		  FullBackup: tree.FullBackupClause{AlwaysFull: true},
		}
  }
| SET FULL BACKUP sconst_or_placeholder
  {
    $$.val = &tree.AlterBackupScheduleSetFullBackup{
		  FullBackup: tree.FullBackupClause{Recurrence: $4.expr()},
		}
  }
| SET SCHEDULE OPTION kv_option
  {
		$$.val = &tree.AlterBackupScheduleSetScheduleOption{
		  Option:  $4.kvOption(),
		}
  }
| EXECUTE IMMEDIATELY
  {
    $$.val = &tree.AlterBackupScheduleNextRun{}
  }
| EXECUTE FULL IMMEDIATELY
  {
    $$.val = &tree.AlterBackupScheduleNextRun{Full: true}
  }

// sconst_or_placeholder matches a simple string, or a placeholder.
sconst_or_placeholder:
  SCONST
  {
    $$.val = tree.NewStrVal($1)
  }
| PLACEHOLDER
  {
    p := $1.placeholder()
    sqllex.(*lexer).UpdateNumPlaceholders(p)
    $$.val = p
  }

cron_expr:
  RECURRING sconst_or_placeholder
  // Can't use string_or_placeholder here due to conflict on NEVER branch above
  // (is NEVER a keyword or a variable?).
  {
    $$.val = $2.expr()
  }

label_spec:
  string_or_placeholder
  {
      $$.val = &tree.LabelSpec{Label: $1.expr(), IfNotExists: false}
  }
| IF NOT EXISTS string_or_placeholder
  {
      $$.val = &tree.LabelSpec{Label: $4.expr(), IfNotExists: true}
  }

schedule_label_spec:
  label_spec
  {
      $$.val = $1.labelSpec()
  }
| /* EMPTY */
  {
      $$.val = &tree.LabelSpec{IfNotExists: false}
  }


opt_full_backup_clause:
  FULL BACKUP sconst_or_placeholder
  // Can't use string_or_placeholder here due to conflict on ALWAYS branch below
  // (is ALWAYS a keyword or a variable?).
  {
    $$.val = &tree.FullBackupClause{Recurrence: $3.expr()}
  }
| FULL BACKUP ALWAYS
  {
    $$.val = &tree.FullBackupClause{AlwaysFull: true}
  }
| /* EMPTY */
  {
    $$.val = (*tree.FullBackupClause)(nil)
  }

opt_with_schedule_options:
  WITH SCHEDULE OPTIONS kv_option_list
  {
    $$.val = $4.kvOptions()
  }
| WITH SCHEDULE OPTIONS '(' kv_option_list ')'
  {
    $$.val = $5.kvOptions()
  }
| /* EMPTY */
  {
    $$.val = nil
  }


// %Help: CREATE EXTERNAL CONNECTION - create a new external connection
// %Category: Misc
// %Text:
// CREATE EXTERNAL CONNECTION [IF NOT EXISTS] <name> AS <endpoint>
//
// Name:
//   Unique name for this external connection.
//
// Endpoint:
//   Endpoint of the resource that the external connection represents.
create_external_connection_stmt:
	CREATE EXTERNAL CONNECTION /*$4=*/label_spec AS /*$6=*/string_or_placeholder
	{
		$$.val = &tree.CreateExternalConnection{
				  ConnectionLabelSpec: *($4.labelSpec()),
		      As: $6.expr(),
		}
	}
 | CREATE EXTERNAL CONNECTION error // SHOW HELP: CREATE EXTERNAL CONNECTION

// %Help: CHECK EXTERNAL CONNECTION - check the status of an external connection
// %Category: Misc
// %Text:
// CREATE EXTERNAL CONNECTION <uri> [WITH <options>]
//
// Uri:
//   Uri for the external connection.
check_external_connection_stmt:
	CHECK EXTERNAL CONNECTION string_or_placeholder opt_with_check_external_connection_options_list
	{
	$$.val = &tree.CheckExternalConnection{
		URI: $4.expr(),
		Options: *($5.checkExternalConnectionOptions()),
	}
	}
 | CHECK EXTERNAL CONNECTION error // SHOW HELP: CHECK EXTERNAL CONNECTION

opt_with_check_external_connection_options_list:
  WITH check_external_connection_options_list
  {
    $$.val = $2.checkExternalConnectionOptions()
  }
| WITH OPTIONS '(' check_external_connection_options_list ')'
  {
    $$.val = $4.checkExternalConnectionOptions()
  }
| /* EMPTY */
  {
    $$.val = &tree.CheckExternalConnectionOptions{}
  }

check_external_connection_options_list:
  // Require at least one option
  check_external_connection_options
  {
    $$.val = $1.checkExternalConnectionOptions()
  }
| check_external_connection_options_list ',' check_external_connection_options
  {
    if err := $1.checkExternalConnectionOptions().CombineWith($3.checkExternalConnectionOptions()); err != nil {
      return setErr(sqllex, err)
    }
  }

check_external_connection_options:
  TRANSFER '=' string_or_placeholder
 {
  $$.val = &tree.CheckExternalConnectionOptions{TransferSize: $3.expr()}
 }
 | TIME '=' string_or_placeholder
 {
  $$.val = &tree.CheckExternalConnectionOptions{Duration: $3.expr()}
 }
 | CONCURRENTLY '=' a_expr
 {
  $$.val = &tree.CheckExternalConnectionOptions{Concurrency: $3.expr()}
 }

// %Help: DROP EXTERNAL CONNECTION - drop an existing external connection
// %Category: Misc
// %Text:
// DROP EXTERNAL CONNECTION <name>
//
// Name:
//   Unique name for this external connection.
drop_external_connection_stmt:
	DROP EXTERNAL CONNECTION string_or_placeholder
	{
	$$.val = &tree.DropExternalConnection{
	    ConnectionLabel: $4.expr(),
	}
	}
	| DROP EXTERNAL CONNECTION error // SHOW HELP: DROP EXTERNAL CONNECTION

// %Help: RESTORE - restore data from external storage
// %Category: CCL
// %Text:
// RESTORE <targets...> FROM <location...>
//         [ AS OF SYSTEM TIME <expr> ]
//         [ WITH <option> [= <value>] [, ...] ]
// or
// RESTORE SYSTEM USERS FROM <location...>
//         [ AS OF SYSTEM TIME <expr> ]
//         [ WITH <option> [= <value>] [, ...] ]
//
// Targets:
//    TABLE <pattern> [, ...]
//    DATABASE <databasename> [, ...]
//
// Locations:
//    "[scheme]://[host]/[path to backup]?[parameters]"
//
// Options:
//    into_db: specify target database
//    skip_missing_foreign_keys: remove foreign key constraints before restoring
//    skip_missing_sequences: ignore sequence dependencies
//    skip_missing_views: skip restoring views because of dependencies that cannot be restored
//    skip_missing_sequence_owners: remove sequence-table ownership dependencies before restoring
//    skip_missing_udfs: skip restoring
//    encryption_passphrase=passphrase: decrypt BACKUP with specified passphrase
//    kms="[kms_provider]://[kms_host]/[master_key_identifier]?[parameters]" : decrypt backups using KMS
//    detached: execute restore job asynchronously, without waiting for its completion
//    skip_localities_check: ignore difference of zone configuration between restore cluster and backup cluster
//    new_db_name: renames the restored database. only applies to database restores
//    include_all_virtual_clusters: enable backups of all virtual clusters during a cluster backup
// %SeeAlso: BACKUP, WEBDOCS/restore.html
restore_stmt:
  RESTORE FROM error
  {
    setErr(sqllex, errors.New("The `RESTORE FROM <backupURI>` syntax is no longer supported. Please use `RESTORE FROM <subdirectory> IN <collectionURI>`."))
    return helpWith(sqllex, "RESTORE")
  }
| RESTORE FROM string_or_placeholder IN string_or_placeholder_opt_list opt_as_of_clause opt_with_restore_options
  {
    $$.val = &tree.Restore{
    DescriptorCoverage: tree.AllDescriptors,
		Subdir: $3.expr(),
		From: $5.stringOrPlaceholderOptList(),
		AsOf: $6.asOfClause(),
		Options: *($7.restoreOptions()),
    }
  }
| RESTORE backup_targets FROM error
  {
    setErr(sqllex, errors.New("The `RESTORE <targets> FROM <backupURI>` syntax is no longer supported. Please use `RESTORE <targets> FROM <subdirectory> IN <collectionURI>`."))
    return helpWith(sqllex, "RESTORE")
  }
| RESTORE backup_targets FROM string_or_placeholder IN string_or_placeholder_opt_list opt_as_of_clause opt_with_restore_options
  {
    $$.val = &tree.Restore{
      Targets: $2.backupTargetList(),
      Subdir: $4.expr(),
      From: $6.stringOrPlaceholderOptList(),
      AsOf: $7.asOfClause(),
      Options: *($8.restoreOptions()),
    }
  }
| RESTORE SYSTEM USERS FROM error
  {
    setErr(sqllex, errors.New("The `RESTORE <targets> FROM <backupURI>` syntax is no longer supported. Please use `RESTORE <targets> FROM <subdirectory> IN <collectionURI>`."))
    return helpWith(sqllex, "RESTORE")
  }
| RESTORE SYSTEM USERS FROM string_or_placeholder IN string_or_placeholder_opt_list opt_as_of_clause opt_with_restore_options
  {
    $$.val = &tree.Restore{
      DescriptorCoverage: tree.SystemUsers,
      Subdir: $5.expr(),
      From: $7.stringOrPlaceholderOptList(),
      AsOf: $8.asOfClause(),
      Options: *($9.restoreOptions()),
    }
  }
| RESTORE error // SHOW HELP: RESTORE

string_or_placeholder_opt_list:
  string_or_placeholder
  {
    $$.val = tree.StringOrPlaceholderOptList{$1.expr()}
  }
| '(' string_or_placeholder_list ')'
  {
    $$.val = tree.StringOrPlaceholderOptList($2.exprs())
  }

// Optional restore options.
opt_with_restore_options:
  WITH restore_options_list
  {
    $$.val = $2.restoreOptions()
  }
| WITH OPTIONS '(' restore_options_list ')'
  {
    $$.val = $4.restoreOptions()
  }
| /* EMPTY */
  {
    $$.val = &tree.RestoreOptions{}
  }

restore_options_list:
  // Require at least one option
  restore_options
  {
    $$.val = $1.restoreOptions()
  }
| restore_options_list ',' restore_options
  {
    if err := $1.restoreOptions().CombineWith($3.restoreOptions()); err != nil {
      return setErr(sqllex, err)
    }
  }

// List of valid restore options.
restore_options:
  ENCRYPTION_PASSPHRASE '=' string_or_placeholder
  {
    $$.val = &tree.RestoreOptions{EncryptionPassphrase: $3.expr()}
  }
| KMS '=' string_or_placeholder_opt_list
	{
    $$.val = &tree.RestoreOptions{DecryptionKMSURI: $3.stringOrPlaceholderOptList()}
	}
| INTO_DB '=' string_or_placeholder
  {
    $$.val = &tree.RestoreOptions{IntoDB: $3.expr()}
  }
| SKIP_MISSING_FOREIGN_KEYS
  {
    $$.val = &tree.RestoreOptions{SkipMissingFKs: true}
  }
| SKIP_MISSING_SEQUENCES
  {
    $$.val = &tree.RestoreOptions{SkipMissingSequences: true}
  }
| SKIP_MISSING_SEQUENCE_OWNERS
  {
    $$.val = &tree.RestoreOptions{SkipMissingSequenceOwners: true}
  }
| SKIP_MISSING_VIEWS
  {
    $$.val = &tree.RestoreOptions{SkipMissingViews: true}
  }
| SKIP_MISSING_UDFS
  {
    $$.val = &tree.RestoreOptions{SkipMissingUDFs: true}
  }
| DETACHED
  {
    $$.val = &tree.RestoreOptions{Detached: true}
  }
| SKIP_LOCALITIES_CHECK
  {
    $$.val = &tree.RestoreOptions{SkipLocalitiesCheck: true}
  }
| NEW_DB_NAME '=' string_or_placeholder
  {
    $$.val = &tree.RestoreOptions{NewDBName: $3.expr()}
  }
| INCREMENTAL_LOCATION '=' string_or_placeholder_opt_list
	{
		$$.val = &tree.RestoreOptions{IncrementalStorage: $3.stringOrPlaceholderOptList()}
	}
| virtual_cluster_name '=' string_or_placeholder
  {
    $$.val = &tree.RestoreOptions{AsTenant: $3.expr()}
  }
| virtual_cluster_opt '=' string_or_placeholder
  {
    $$.val = &tree.RestoreOptions{ForceTenantID: $3.expr()}
  }
| SCHEMA_ONLY
	{
		$$.val = &tree.RestoreOptions{SchemaOnly: true}
	}
| VERIFY_BACKUP_TABLE_DATA
	{
		$$.val = &tree.RestoreOptions{VerifyData: true}
	}
| UNSAFE_RESTORE_INCOMPATIBLE_VERSION
  {
    $$.val = &tree.RestoreOptions{UnsafeRestoreIncompatibleVersion: true}
  }
| EXECUTION LOCALITY '=' string_or_placeholder
  {
    $$.val = &tree.RestoreOptions{ExecutionLocality: $4.expr()}
  }
| EXPERIMENTAL DEFERRED COPY
  {
    $$.val = &tree.RestoreOptions{ExperimentalOnline: true}
  }
| REMOVE_REGIONS
  {
    $$.val = &tree.RestoreOptions{RemoveRegions: true, SkipLocalitiesCheck: true}
  }

virtual_cluster_opt:
  TENANT  { /* SKIP DOC */ }
| VIRTUAL_CLUSTER { }

virtual_cluster_name:
  TENANT_NAME { /* SKIP DOC */ }
| VIRTUAL_CLUSTER_NAME { }

import_format:
  name
  {
    $$ = strings.ToUpper($1)
  }

alter_unsupported_stmt:
  ALTER DOMAIN error
  {
    return unimplemented(sqllex, "alter domain")
  }
| ALTER AGGREGATE error
  {
    return unimplementedWithIssueDetail(sqllex, 74775, "alter aggregate")
  }

// %Help: IMPORT - load data from file in a distributed manner
// %Category: CCL
// %Text:
// -- Import both schema and table data:
// IMPORT [ TABLE <tablename> FROM ]
//        <format> <datafile>
//        [ WITH <option> [= <value>] [, ...] ]
//
// Formats:
//    MYSQLDUMP
//    PGDUMP
//
// Options:
//    distributed = '...'
//    sstsize = '...'
//    temp = '...'
//
// Use CREATE TABLE followed by IMPORT INTO to create and import into a table
// from external files that only have table data.
//
// %SeeAlso: CREATE TABLE, WEBDOCS/import-into.html
import_stmt:
 IMPORT import_format '(' string_or_placeholder ')' opt_with_options
  {
    /* SKIP DOC */
    $$.val = &tree.Import{Bundle: true, FileFormat: $2, Files: tree.Exprs{$4.expr()}, Options: $6.kvOptions()}
  }
| IMPORT import_format string_or_placeholder opt_with_options
  {
    $$.val = &tree.Import{Bundle: true, FileFormat: $2, Files: tree.Exprs{$3.expr()}, Options: $4.kvOptions()}
  }
| IMPORT TABLE table_name FROM import_format '(' string_or_placeholder ')' opt_with_options
  {
    /* SKIP DOC */
    name := $3.unresolvedObjectName().ToTableName()
    $$.val = &tree.Import{Bundle: true, Table: &name, FileFormat: $5, Files: tree.Exprs{$7.expr()}, Options: $9.kvOptions()}
  }
| IMPORT TABLE table_name FROM import_format string_or_placeholder opt_with_options
  {
    name := $3.unresolvedObjectName().ToTableName()
    $$.val = &tree.Import{Bundle: true, Table: &name, FileFormat: $5, Files: tree.Exprs{$6.expr()}, Options: $7.kvOptions()}
  }
| IMPORT INTO table_name '(' insert_column_list ')' import_format DATA '(' string_or_placeholder_list ')' opt_with_options
  {
    name := $3.unresolvedObjectName().ToTableName()
    $$.val = &tree.Import{Table: &name, Into: true, IntoCols: $5.nameList(), FileFormat: $7, Files: $10.exprs(), Options: $12.kvOptions()}
  }
| IMPORT INTO table_name import_format DATA '(' string_or_placeholder_list ')' opt_with_options
  {
    name := $3.unresolvedObjectName().ToTableName()
    $$.val = &tree.Import{Table: &name, Into: true, IntoCols: nil, FileFormat: $4, Files: $7.exprs(), Options: $9.kvOptions()}
  }
| IMPORT error // SHOW HELP: IMPORT

// %Help: EXPORT - export data to file in a distributed manner
// %Category: CCL
// %Text:
// EXPORT INTO <format> <datafile> [WITH <option> [= value] [,...]] FROM <query>
//
// Formats:
//    CSV
//    Parquet
//
// Options:
//    delimiter = '...'   [CSV-specific]
//
// %SeeAlso: SELECT
export_stmt:
  EXPORT INTO import_format string_or_placeholder opt_with_options FROM select_stmt
  {
    $$.val = &tree.Export{Query: $7.slct(), FileFormat: $3, File: $4.expr(), Options: $5.kvOptions()}
  }
| EXPORT error // SHOW HELP: EXPORT

string_or_placeholder:
  non_reserved_word_or_sconst
  {
    $$.val = tree.NewStrVal($1)
  }
| PLACEHOLDER
  {
    p := $1.placeholder()
    sqllex.(*lexer).UpdateNumPlaceholders(p)
    $$.val = p
  }

string_or_placeholder_list:
  string_or_placeholder
  {
    $$.val = tree.Exprs{$1.expr()}
  }
| string_or_placeholder_list ',' string_or_placeholder
  {
    $$.val = append($1.exprs(), $3.expr())
  }

kv_option:
  name '=' string_or_placeholder
  {
    $$.val = tree.KVOption{Key: tree.Name($1), Value: $3.expr()}
  }
|  name
  {
    $$.val = tree.KVOption{Key: tree.Name($1)}
  }
|  SCONST '=' string_or_placeholder
  {
    $$.val = tree.KVOption{Key: tree.Name($1), Value: $3.expr()}
  }
|  SCONST
  {
    $$.val = tree.KVOption{Key: tree.Name($1)}
  }

kv_option_list:
  kv_option
  {
    $$.val = []tree.KVOption{$1.kvOption()}
  }
|  kv_option_list ',' kv_option
  {
    $$.val = append($1.kvOptions(), $3.kvOption())
  }

opt_with_options:
  WITH kv_option_list
  {
    $$.val = $2.kvOptions()
  }
| WITH OPTIONS '(' kv_option_list ')'
  {
    $$.val = $4.kvOptions()
  }
| /* EMPTY */
  {
    $$.val = nil
  }

// %Help: CALL - invoke a procedure
// %Category: Misc
// %Text: CALL <name> ( [ <expr> [, ...] ] )
// %SeeAlso: CREATE PROCEDURE
call_stmt:
  CALL func_application
  {
    p := $2.expr().(*tree.FuncExpr)
    p.InCall = true
    $$.val = &tree.Call{Proc: p}
  }

// The COPY grammar in postgres has 3 different versions, all of which are supported by postgres:
// 1) The "really old" syntax from v7.2 and prior
// 2) Pre 9.0 using hard-wired, space-separated options
// 3) The current and preferred options using comma-separated generic identifiers instead of keywords.
// We currently support only the #2 format.
// See the comment for CopyStmt in https://github.com/postgres/postgres/blob/master/src/backend/parser/gram.y.
copy_stmt:
  COPY table_name opt_column_list FROM STDIN opt_with_copy_options opt_where_clause
  {
    /* FORCE DOC */
    name := $2.unresolvedObjectName().ToTableName()
    if $7.expr() != nil {
      return unimplementedWithIssue(sqllex, 54580)
    }
    $$.val = &tree.CopyFrom{
       Table: name,
       Columns: $3.nameList(),
       Stdin: true,
       Options: *$6.copyOptions(),
    }
  }
| COPY table_name opt_column_list FROM error
  {
    return unimplemented(sqllex, "copy from unsupported format")
  }
| COPY table_name opt_column_list TO STDOUT opt_with_copy_options
  {
    /* FORCE DOC */
    name := $2.unresolvedObjectName().ToTableName()
    $$.val = &tree.CopyTo{
       Table: name,
       Columns: $3.nameList(),
       Options: *$6.copyOptions(),
    }
  }
| COPY table_name opt_column_list TO error
  {
    return unimplementedWithIssue(sqllex, 97181)
  }
| COPY '(' copy_to_stmt ')' TO STDOUT opt_with_copy_options
   {
     /* FORCE DOC */
     $$.val = &tree.CopyTo{
        Statement: $3.stmt(),
        Options: *$7.copyOptions(),
     }
   }
| COPY '(' copy_to_stmt ')' TO error
   {
     return unimplementedWithIssue(sqllex, 96590)
   }

opt_with_copy_options:
  opt_with copy_options_list
  {
    $$.val = $2.copyOptions()
  }
| opt_with '(' copy_generic_options_list ')'
  {
    $$.val = $3.copyOptions()
  }
| /* EMPTY */
  {
    $$.val = &tree.CopyOptions{}
  }

copy_options_list:
  copy_options
  {
    $$.val = $1.copyOptions()
  }
| copy_options_list copy_options
  {
    if err := $1.copyOptions().CombineWith($2.copyOptions()); err != nil {
      return setErr(sqllex, err)
    }
  }

copy_generic_options_list:
  copy_generic_options
  {
    $$.val = $1.copyOptions()
  }
| copy_generic_options_list ',' copy_generic_options
  {
    if err := $1.copyOptions().CombineWith($3.copyOptions()); err != nil {
      return setErr(sqllex, err)
    }
  }

copy_options:
  DESTINATION '=' string_or_placeholder
  {
    $$.val = &tree.CopyOptions{Destination: $3.expr()}
  }
| BINARY
  {
    $$.val = &tree.CopyOptions{CopyFormat: tree.CopyFormatBinary, HasFormat: true}
  }
| CSV
  {
    $$.val = &tree.CopyOptions{CopyFormat: tree.CopyFormatCSV, HasFormat: true}
  }
| DELIMITER string_or_placeholder
  {
    $$.val = &tree.CopyOptions{Delimiter: $2.expr()}
  }
| NULL string_or_placeholder
  {
    $$.val = &tree.CopyOptions{Null: $2.expr()}
  }
| OIDS error
  {
    return unimplementedWithIssueDetail(sqllex, 41608, "oids")
  }
| FREEZE error
  {
    return unimplementedWithIssueDetail(sqllex, 41608, "freeze")
  }
| HEADER
  {
    $$.val = &tree.CopyOptions{Header: true, HasHeader: true}
  }
| QUOTE SCONST
  {
    $$.val = &tree.CopyOptions{Quote: tree.NewStrVal($2)}
  }
| ESCAPE SCONST
  {
    $$.val = &tree.CopyOptions{Escape: tree.NewStrVal($2)}
  }
| FORCE QUOTE error
  {
    return unimplementedWithIssueDetail(sqllex, 41608, "force_quote")
  }
| FORCE NOT NULL error
  {
    return unimplementedWithIssueDetail(sqllex, 41608, "force_not_null")
  }
| FORCE NULL error
  {
    return unimplementedWithIssueDetail(sqllex, 41608, "force_null")
  }
| ENCODING SCONST
  {
    $$.val = &tree.CopyOptions{Encoding: tree.NewStrVal($2)}
  }

copy_generic_options:
  DESTINATION string_or_placeholder
  {
    $$.val = &tree.CopyOptions{Destination: $2.expr()}
  }
| FORMAT BINARY
  {
    $$.val = &tree.CopyOptions{CopyFormat: tree.CopyFormatBinary, HasFormat: true}
  }
| FORMAT CSV
  {
    $$.val = &tree.CopyOptions{CopyFormat: tree.CopyFormatCSV, HasFormat: true}
  }
| FORMAT TEXT
  {
    $$.val = &tree.CopyOptions{CopyFormat: tree.CopyFormatText, HasFormat: true}
  }
| FORMAT SCONST
  {
    format := $2
    switch format {
    case "csv":
      $$.val = &tree.CopyOptions{CopyFormat: tree.CopyFormatCSV, HasFormat: true}
    case "binary":
      $$.val = &tree.CopyOptions{CopyFormat: tree.CopyFormatBinary, HasFormat: true}
    case "text":
      $$.val = &tree.CopyOptions{CopyFormat: tree.CopyFormatText, HasFormat: true}
    default:
      sqllex.Error("COPY format \"" + format + "\" not recognized")
      return 1
    }
  }
| DELIMITER string_or_placeholder
  {
    $$.val = &tree.CopyOptions{Delimiter: $2.expr()}
  }
| NULL string_or_placeholder
  {
    $$.val = &tree.CopyOptions{Null: $2.expr()}
  }
| OIDS error
  {
    return unimplementedWithIssueDetail(sqllex, 41608, "oids")
  }
| FREEZE error
  {
    return unimplementedWithIssueDetail(sqllex, 41608, "freeze")
  }
| HEADER
  {
    $$.val = &tree.CopyOptions{Header: true, HasHeader: true}
  }
| HEADER TRUE
  {
    $$.val = &tree.CopyOptions{Header: true, HasHeader: true}
  }
| HEADER FALSE
  {
    $$.val = &tree.CopyOptions{Header: false, HasHeader: true}
  }
| QUOTE SCONST
  {
    $$.val = &tree.CopyOptions{Quote: tree.NewStrVal($2)}
  }
| ESCAPE SCONST
  {
    $$.val = &tree.CopyOptions{Escape: tree.NewStrVal($2)}
  }
| FORCE_QUOTE error
  {
    return unimplementedWithIssueDetail(sqllex, 41608, "force_quote")
  }
| FORCE_NOT_NULL error
  {
    return unimplementedWithIssueDetail(sqllex, 41608, "force_not_null")
  }
| FORCE_NULL error
  {
    return unimplementedWithIssueDetail(sqllex, 41608, "force_null")
  }
| ENCODING SCONST
  {
    $$.val = &tree.CopyOptions{Encoding: tree.NewStrVal($2)}
  }

// %Help: CANCEL
// %Category: Group
// %Text: CANCEL JOBS, CANCEL QUERIES, CANCEL SESSIONS
cancel_stmt:
  cancel_jobs_stmt      // EXTEND WITH HELP: CANCEL JOBS
| cancel_queries_stmt   // EXTEND WITH HELP: CANCEL QUERIES
| cancel_sessions_stmt  // EXTEND WITH HELP: CANCEL SESSIONS
| cancel_all_jobs_stmt  // EXTEND WITH HELP: CANCEL ALL JOBS
| CANCEL error          // SHOW HELP: CANCEL

// %Help: CANCEL JOBS - cancel background jobs
// %Category: Misc
// %Text:
// CANCEL JOBS <selectclause>
// CANCEL JOB <jobid>
// %SeeAlso: SHOW JOBS, PAUSE JOBS, RESUME JOBS
cancel_jobs_stmt:
  CANCEL JOB a_expr
  {
    $$.val = &tree.ControlJobs{
      Jobs: &tree.Select{
        Select: &tree.ValuesClause{Rows: []tree.Exprs{tree.Exprs{$3.expr()}}},
      },
      Command: tree.CancelJob,
    }
  }
| CANCEL JOB error // SHOW HELP: CANCEL JOBS
| CANCEL JOBS select_stmt
  {
    $$.val = &tree.ControlJobs{Jobs: $3.slct(), Command: tree.CancelJob}
  }
| CANCEL JOBS for_schedules_clause
  {
    $$.val = &tree.ControlJobsForSchedules{Schedules: $3.slct(), Command: tree.CancelJob}
  }
| CANCEL JOBS error // SHOW HELP: CANCEL JOBS

// %Help: CANCEL QUERIES - cancel running queries
// %Category: Misc
// %Text:
// CANCEL QUERIES [IF EXISTS] <selectclause>
// CANCEL QUERY [IF EXISTS] <expr>
// %SeeAlso: SHOW STATEMENTS
cancel_queries_stmt:
  CANCEL QUERY a_expr
  {
    $$.val = &tree.CancelQueries{
      Queries: &tree.Select{
        Select: &tree.ValuesClause{Rows: []tree.Exprs{tree.Exprs{$3.expr()}}},
      },
      IfExists: false,
    }
  }
| CANCEL QUERY IF EXISTS a_expr
  {
    $$.val = &tree.CancelQueries{
      Queries: &tree.Select{
        Select: &tree.ValuesClause{Rows: []tree.Exprs{tree.Exprs{$5.expr()}}},
      },
      IfExists: true,
    }
  }
| CANCEL QUERY error // SHOW HELP: CANCEL QUERIES
| CANCEL QUERIES select_stmt
  {
    $$.val = &tree.CancelQueries{Queries: $3.slct(), IfExists: false}
  }
| CANCEL QUERIES IF EXISTS select_stmt
  {
    $$.val = &tree.CancelQueries{Queries: $5.slct(), IfExists: true}
  }
| CANCEL QUERIES error // SHOW HELP: CANCEL QUERIES

// %Help: CANCEL SESSIONS - cancel open sessions
// %Category: Misc
// %Text:
// CANCEL SESSIONS [IF EXISTS] <selectclause>
// CANCEL SESSION [IF EXISTS] <sessionid>
// %SeeAlso: SHOW SESSIONS
cancel_sessions_stmt:
  CANCEL SESSION a_expr
  {
   $$.val = &tree.CancelSessions{
      Sessions: &tree.Select{
        Select: &tree.ValuesClause{Rows: []tree.Exprs{tree.Exprs{$3.expr()}}},
      },
      IfExists: false,
    }
  }
| CANCEL SESSION IF EXISTS a_expr
  {
   $$.val = &tree.CancelSessions{
      Sessions: &tree.Select{
        Select: &tree.ValuesClause{Rows: []tree.Exprs{tree.Exprs{$5.expr()}}},
      },
      IfExists: true,
    }
  }
| CANCEL SESSION error // SHOW HELP: CANCEL SESSIONS
| CANCEL SESSIONS select_stmt
  {
    $$.val = &tree.CancelSessions{Sessions: $3.slct(), IfExists: false}
  }
| CANCEL SESSIONS IF EXISTS select_stmt
  {
    $$.val = &tree.CancelSessions{Sessions: $5.slct(), IfExists: true}
  }
| CANCEL SESSIONS error // SHOW HELP: CANCEL SESSIONS

// %Help: CANCEL ALL JOBS - cancel all background jobs
// %Category: Misc
// %Text:
// CANCEL ALL {BACKUP|CHANGEFEED|IMPORT|RESTORE} JOBS
cancel_all_jobs_stmt:
  CANCEL ALL name JOBS
  {
    $$.val = &tree.ControlJobsOfType{Type: $3, Command: tree.CancelJob}
  }
| CANCEL ALL error // SHOW HELP: CANCEL ALL JOBS

comment_stmt:
  COMMENT ON DATABASE database_name IS comment_text
  {
    $$.val = &tree.CommentOnDatabase{Name: tree.Name($4), Comment: $6.strPtr()}
  }
| COMMENT ON SCHEMA qualifiable_schema_name IS comment_text
  {
    $$.val = &tree.CommentOnSchema{Name: $4.objectNamePrefix(), Comment: $6.strPtr()}
  }
| COMMENT ON TYPE type_name IS comment_text
  {
    $$.val = &tree.CommentOnType{Name: $4.unresolvedObjectName(), Comment: $6.strPtr()}
  }
| COMMENT ON TABLE table_name IS comment_text
  {
    $$.val = &tree.CommentOnTable{Table: $4.unresolvedObjectName(), Comment: $6.strPtr()}
  }
| COMMENT ON COLUMN column_path IS comment_text
  {
    varName, err := $4.unresolvedName().NormalizeVarName()
    if err != nil {
      return setErr(sqllex, err)
    }
    columnItem, ok := varName.(*tree.ColumnItem)
    if !ok {
      sqllex.Error(fmt.Sprintf("invalid column name: %q", tree.ErrString($4.unresolvedName())))
            return 1
    }
    if columnItem != nil && columnItem.TableName != nil {
      aIdx := sqllex.(*lexer).NewAnnotation()
      columnItem.TableName.AnnotatedNode = tree.AnnotatedNode{AnnIdx: aIdx}
    }
    $$.val = &tree.CommentOnColumn{ColumnItem: columnItem, Comment: $6.strPtr()}
  }
| COMMENT ON INDEX table_index_name IS comment_text
  {
    $$.val = &tree.CommentOnIndex{Index: $4.tableIndexName(), Comment: $6.strPtr()}
  }

| COMMENT ON CONSTRAINT constraint_name ON table_name IS comment_text
  {
    $$.val = &tree.CommentOnConstraint{Constraint:tree.Name($4), Table: $6.unresolvedObjectName(), Comment: $8.strPtr()}
  }
| COMMENT ON EXTENSION error { return unimplementedWithIssueDetail(sqllex, 74777, "comment on extension") }
| COMMENT ON FUNCTION error { return unimplementedWithIssueDetail(sqllex, 17511, "comment on function") }

comment_text:
  SCONST
  {
    t := $1
    $$.val = &t
  }
| NULL
  {
    var str *string
    $$.val = str
  }

// %Help: CREATE
// %Category: Group
// %Text:
// CREATE DATABASE, CREATE TABLE, CREATE INDEX, CREATE TABLE AS,
// CREATE USER, CREATE VIEW, CREATE SEQUENCE, CREATE STATISTICS,
// CREATE ROLE, CREATE TYPE, CREATE EXTENSION, CREATE SCHEDULE
create_stmt:
  create_role_stmt       // EXTEND WITH HELP: CREATE ROLE
| create_ddl_stmt        // help texts in sub-rule
| create_stats_stmt      // EXTEND WITH HELP: CREATE STATISTICS
| create_changefeed_stmt // EXTEND WITH HELP: CREATE CHANGEFEED
| create_extension_stmt  // EXTEND WITH HELP: CREATE EXTENSION
| create_external_connection_stmt // EXTEND WITH HELP: CREATE EXTERNAL CONNECTION
| create_virtual_cluster_stmt     // EXTEND WITH HELP: CREATE VIRTUAL CLUSTER
| create_logical_replication_stream_stmt     // EXTEND WITH HELP: CREATE LOGICAL REPLICATION STREAM
| create_schedule_stmt   // help texts in sub-rule
| create_unsupported     {}
| CREATE error           // SHOW HELP: CREATE

// %Help: CHECK
// %Category: Group
// %Text:
// CHECK EXTERNAL CONNECTION
check_stmt:
  check_external_connection_stmt // EXTEND WITH HELP: CHECK EXTERNAL CONNECTION
| CHECK error // SHOW HELP: CHECK

// %Help: CREATE LOGICAL REPLICATION STREAM - create a new logical replication stream
// %Category: Experimental
// %Text:
// CREATE LOGICAL REPLICATION STREAM
//  FROM <TABLE remote_name | TABLES (remote_name, ...) | DATABASE remote_name>
//  ON 'stream_uri'
//  INTO <TABLE remote_name | TABLES (remote_name, ...) | DATABASE remote_name>
//  [WITH
//  < MODE = immediate | validated > |
//  < CURSOR = start_time > |
//  < DEFAULT FUNCTION = lww | dlq | udf
//  < FUNCTION 'udf' FOR TABLE local_name  , ... > |
//  < DISCARD = 'ttl-deletes' >
// ]
create_logical_replication_stream_stmt:
  CREATE LOGICAL REPLICATION STREAM FROM logical_replication_resources ON string_or_placeholder INTO logical_replication_resources opt_logical_replication_options
  {
    /* SKIP DOC */
    $$.val = &tree.CreateLogicalReplicationStream{
      From: $6.logicalReplicationResources(),
      PGURL: $8.expr(),
      Into: $10.logicalReplicationResources(),
      Options: *$11.logicalReplicationOptions(),
    }
  }
| CREATE LOGICALLY REPLICATED logical_replication_resources FROM logical_replication_resources ON string_or_placeholder opt_logical_replication_create_table_options
  {
    $$.val = &tree.CreateLogicalReplicationStream{
      Into: $4.logicalReplicationResources(),
      From: $6.logicalReplicationResources(),
      PGURL: $8.expr(),
      CreateTable: true,
      Options: *$9.logicalReplicationOptions(),
    }
  }
| CREATE LOGICAL REPLICATION STREAM error // SHOW HELP: CREATE LOGICAL REPLICATION STREAM

logical_replication_resources:
  TABLE db_object_name
  {
    $$.val = tree.LogicalReplicationResources{
      Tables: []*tree.UnresolvedName{
        $2.unresolvedObjectName().ToUnresolvedName(),
      },
    }
  }
| TABLES '(' logical_replication_resources_list ')'
  {
    $$.val = $3.logicalReplicationResources()
  }
| DATABASE database_name
  {
    $$.val = tree.LogicalReplicationResources{
      Database: tree.Name($2),
    }
  }

logical_replication_resources_list:
  db_object_name
  {
    $$.val = tree.LogicalReplicationResources{
      Tables: []*tree.UnresolvedName{
        $1.unresolvedObjectName().ToUnresolvedName(),
      },
    }
  }
| logical_replication_resources_list ',' db_object_name
  {
    $$.val = tree.LogicalReplicationResources{
      Tables: append($1.logicalReplicationResources().Tables, $3.unresolvedObjectName().ToUnresolvedName()),
    }
  }

// Optional logical replication options.
opt_logical_replication_options:
  WITH logical_replication_options_list
  {
    $$.val = $2.logicalReplicationOptions()
  }
| WITH OPTIONS '(' logical_replication_options_list ')'
  {
    $$.val = $4.logicalReplicationOptions()
  }
| /* EMPTY */
  {
    $$.val = &tree.LogicalReplicationOptions{}
  }

opt_logical_replication_create_table_options:
  WITH logical_replication_create_table_options_list
  {
    $$.val = $2.logicalReplicationOptions()
  }
| WITH OPTIONS '(' logical_replication_create_table_options_list ')'
  {
    $$.val = $4.logicalReplicationOptions()
  }
| /* EMPTY */
  {
    $$.val = &tree.LogicalReplicationOptions{}
  }

logical_replication_options_list:
  // Require at least one option
  logical_replication_options
  {
    $$.val = $1.logicalReplicationOptions()
  }
| logical_replication_options_list ',' logical_replication_options
  {
    if err := $1.logicalReplicationOptions().CombineWith($3.logicalReplicationOptions()); err != nil {
      return setErr(sqllex, err)
    }
  }

logical_replication_create_table_options_list:
  // Require at least one option
  logical_replication_create_table_options
  {
    $$.val = $1.logicalReplicationOptions()
  }
| logical_replication_create_table_options_list ',' logical_replication_create_table_options
  {
    if err := $1.logicalReplicationOptions().CombineWith($3.logicalReplicationOptions()); err != nil {
      return setErr(sqllex, err)
    }
  }

// List of valid logical replication options.
logical_replication_options:
  CURSOR '=' string_or_placeholder
  {
    $$.val = &tree.LogicalReplicationOptions{Cursor: $3.expr()}
  }
|
  MODE '=' string_or_placeholder
  {
    $$.val = &tree.LogicalReplicationOptions{Mode: $3.expr()}
  }
| DEFAULT FUNCTION '=' string_or_placeholder
  {
    $$.val = &tree.LogicalReplicationOptions{DefaultFunction: $4.expr()}
  }
| FUNCTION db_object_name FOR TABLE db_object_name
  {
     $$.val = &tree.LogicalReplicationOptions{UserFunctions: map[tree.UnresolvedName]tree.RoutineName{*$5.unresolvedObjectName().ToUnresolvedName():$2.unresolvedObjectName().ToRoutineName()}}
  }
 | DISCARD '=' string_or_placeholder
  {
    $$.val = &tree.LogicalReplicationOptions{Discard: $3.expr()}
  }
| SKIP SCHEMA CHECK
  {
    $$.val = &tree.LogicalReplicationOptions{SkipSchemaCheck: tree.MakeDBool(true)}
  }
| LABEL '=' string_or_placeholder
  {
    $$.val = &tree.LogicalReplicationOptions{MetricsLabel: $3.expr()}
  }
| PARENT '=' string_or_placeholder
  /* SKIP DOC */
  {
    $$.val = &tree.LogicalReplicationOptions{ParentID: $3.expr()}
  }


logical_replication_create_table_options:
  MODE '=' string_or_placeholder
  {
    $$.val = &tree.LogicalReplicationOptions{Mode: $3.expr()}
  }
 | DISCARD '=' string_or_placeholder
  {
    $$.val = &tree.LogicalReplicationOptions{Discard: $3.expr()}
  }
| LABEL '=' string_or_placeholder
  {
    $$.val = &tree.LogicalReplicationOptions{MetricsLabel: $3.expr()}
  }
| UNIDIRECTIONAL
  {
   $$.val = &tree.LogicalReplicationOptions{Unidirectional: tree.MakeDBool(true)} 
  }
| BIDIRECTIONAL ON string_or_placeholder
  {
   $$.val = &tree.LogicalReplicationOptions{BidirectionalURI: $3.expr()} 
  }


// %Help: CREATE VIRTUAL CLUSTER - create a new virtual cluster
// %Category: Experimental
// %Text:
// CREATE VIRTUAL CLUSTER [ IF NOT EXISTS ] name [ <replication> ]
//
// Replication option:
//    FROM REPLICATION OF name ON <location> [ WITH OPTIONS ... ]
create_virtual_cluster_stmt:
  CREATE virtual_cluster virtual_cluster_spec
  {
    /* SKIP DOC */
    $$.val = &tree.CreateTenant{
      TenantSpec: $3.tenantSpec(),
    }
  }
| CREATE virtual_cluster IF NOT EXISTS virtual_cluster_spec
  {
    /* SKIP DOC */
    $$.val = &tree.CreateTenant{
      IfNotExists: true,
      TenantSpec: $6.tenantSpec(),
    }
  }
| CREATE virtual_cluster virtual_cluster_spec FROM REPLICATION OF d_expr ON d_expr opt_with_replication_options
  {
    /* SKIP DOC */
    $$.val = &tree.CreateTenantFromReplication{
      TenantSpec: $3.tenantSpec(),
      ReplicationSourceTenantName: &tree.TenantSpec{IsName: true, Expr: $7.expr()},
      ReplicationSourceConnUri: $9.expr(),
      Options: *$10.tenantReplicationOptions(),
    }
  }
| CREATE virtual_cluster IF NOT EXISTS virtual_cluster_spec FROM REPLICATION OF d_expr ON d_expr opt_with_replication_options
  {
    /* SKIP DOC */
    $$.val = &tree.CreateTenantFromReplication{
      IfNotExists: true,
      TenantSpec: $6.tenantSpec(),
      ReplicationSourceTenantName: &tree.TenantSpec{IsName: true, Expr: $10.expr()},
      ReplicationSourceConnUri: $12.expr(),
      Options: *$13.tenantReplicationOptions(),
    }
  }
| CREATE virtual_cluster error // SHOW HELP: CREATE VIRTUAL CLUSTER

virtual_cluster:
  TENANT { /* SKIP DOC */ }
| VIRTUAL CLUSTER

// Optional tenant replication options.
opt_with_replication_options:
  WITH replication_options_list
  {
    $$.val = $2.tenantReplicationOptions()
  }
| WITH OPTIONS '(' replication_options_list ')'
  {
    $$.val = $4.tenantReplicationOptions()
  }
| /* EMPTY */
  {
    $$.val = &tree.TenantReplicationOptions{}
  }

replication_options_list:
  // Require at least one option
  replication_options
  {
    $$.val = $1.tenantReplicationOptions()
  }
| replication_options_list ',' replication_options
  {
    if err := $1.tenantReplicationOptions().CombineWith($3.tenantReplicationOptions()); err != nil {
      return setErr(sqllex, err)
    }
  }

// List of valid tenant replication options.
replication_options:
  RETENTION '=' d_expr
  {
    $$.val = &tree.TenantReplicationOptions{Retention: $3.expr()}
  }
|
  EXPIRATION WINDOW '=' d_expr
  {
      $$.val = &tree.TenantReplicationOptions{ExpirationWindow: $4.expr()}
  }
| READ VIRTUAL CLUSTER
  {
    $$.val = &tree.TenantReplicationOptions{EnableReaderTenant: tree.MakeDBool(true)}
  }

// %Help: CREATE SCHEDULE
// %Category: Group
// %Text:
// CREATE SCHEDULE FOR BACKUP,
// CREATE SCHEDULE FOR CHANGEFEED
create_schedule_stmt:
  create_schedule_for_changefeed_stmt // EXTEND WITH HELP: CREATE SCHEDULE FOR CHANGEFEED
| create_schedule_for_backup_stmt     // EXTEND WITH HELP: CREATE SCHEDULE FOR BACKUP
| CREATE SCHEDULE error               // SHOW HELP: CREATE SCHEDULE

// %Help: CREATE EXTENSION - pseudo-statement for PostgreSQL compatibility
// %Category: Cfg
// %Text: CREATE EXTENSION [IF NOT EXISTS] name
create_extension_stmt:
  CREATE EXTENSION IF NOT EXISTS name
  {
    $$.val = &tree.CreateExtension{IfNotExists: true, Name: tree.Name($6)}
  }
| CREATE EXTENSION name {
    $$.val = &tree.CreateExtension{Name: tree.Name($3)}
  }
| CREATE EXTENSION IF NOT EXISTS name WITH error
  {
    return unimplementedWithIssueDetail(sqllex, 74777, "create extension if not exists with")
  }
| CREATE EXTENSION name WITH error {
    return unimplementedWithIssueDetail(sqllex, 74777, "create extension with")
  }
| CREATE EXTENSION error // SHOW HELP: CREATE EXTENSION

// %Help: ALTER POLICY - alter an existing row-level security policy
// %Category: DDL
// %Text:
// ALTER POLICY name ON table_name RENAME TO new_policy_name
//
// ALTER POLICY name ON table_name
//       [ TO { role_name | PUBLIC | CURRENT_USER | SESSION_USER } [, ...] ]
//       [ USING ( using_expression ) ]
//       [ WITH CHECK ( check_expression ) ]
//
// %SeeAlso: CREATE POLICY, DROP POLICY
alter_policy_stmt:
  ALTER POLICY name ON table_name RENAME TO name
  {
    /* SKIP DOC */
    $$.val = &tree.AlterPolicy{
      PolicyName: tree.Name($3),
      TableName: $5.unresolvedObjectName(),
      NewPolicyName: tree.Name($8),
    }
  }
| ALTER POLICY name ON table_name opt_policy_roles opt_policy_exprs
  {
    /* SKIP DOC */
    $$.val = &tree.AlterPolicy{
      PolicyName: tree.Name($3),
      TableName: $5.unresolvedObjectName(),
      Roles: $6.roleSpecList(),
      Exprs: $7.policyExpressions(),
    }
  }
| ALTER POLICY error // SHOW HELP: ALTER POLICY

// %Help: CREATE POLICY - define a new row-level security policy for a table
// %Category: DDL
// %Text:
// CREATE POLICY name ON table_name
//     [ AS { PERMISSIVE | RESTRICTIVE } ]
//     [ FOR { ALL | SELECT | INSERT | UPDATE | DELETE } ]
//     [ TO { role_name | PUBLIC | CURRENT_USER | SESSION_USER } [, ...] ]
//     [ USING ( using_expression ) ]
//     [ WITH CHECK ( check_expression ) ]
//
// %SeeAlso: ALTER POLICY, DROP POLICY
create_policy_stmt:
  CREATE POLICY name ON table_name opt_policy_type opt_policy_command opt_policy_roles opt_policy_exprs
  {
    /* SKIP DOC */
    $$.val = &tree.CreatePolicy{
      PolicyName: tree.Name($3),
      TableName: $5.unresolvedObjectName(),
      Type: $6.policyType(),
      Cmd: $7.policyCommand(),
      Roles: $8.roleSpecList(),
      Exprs: $9.policyExpressions(),
    }
  }
 | CREATE POLICY error // SHOW HELP: CREATE POLICY

// %Help: DROP POLICY - remove an existing row-level security policy from a table
// %Category: DDL
// %Text:
// DROP POLICY [ IF EXISTS ] name ON table_name [ CASCADE | RESTRICT ]
//
// %SeeAlso: CREATE POLICY, ALTER POLICY
drop_policy_stmt:
  DROP POLICY name ON table_name opt_drop_behavior
  {
    /* SKIP DOC */
    $$.val = &tree.DropPolicy{
      PolicyName: tree.Name($3),
      TableName: $5.unresolvedObjectName(),
      DropBehavior: $6.dropBehavior(),
      IfExists: false,
    }
  }
| DROP POLICY IF EXISTS name ON table_name opt_drop_behavior
  {
    /* SKIP DOC */
    $$.val = &tree.DropPolicy{
      PolicyName: tree.Name($5),
      TableName: $7.unresolvedObjectName(),
      DropBehavior: $8.dropBehavior(),
      IfExists: true,
    }
  }
| DROP POLICY error // SHOW HELP: DROP POLICY

opt_policy_type:
  AS PERMISSIVE
  {
    $$.val = tree.PolicyTypePermissive
  }
| AS RESTRICTIVE
  {
    $$.val = tree.PolicyTypeRestrictive
  }
| /* EMPTY */
  {
    $$.val = tree.PolicyTypeDefault
  }

opt_policy_command:
  FOR ALL
  {
    $$.val = tree.PolicyCommandAll
  }
| FOR SELECT
  {
    $$.val = tree.PolicyCommandSelect
  }
| FOR INSERT
  {
    $$.val = tree.PolicyCommandInsert
  }
| FOR UPDATE
  {
    $$.val = tree.PolicyCommandUpdate
  }
| FOR DELETE
  {
    $$.val = tree.PolicyCommandDelete
  }
| /* EMPTY */
  {
    $$.val = tree.PolicyCommandDefault
  }

opt_policy_roles:
 TO role_spec_list
 {
   $$.val = $2.roleSpecList()
 }
| /* EMPTY */ {
   $$.val = tree.RoleSpecList(nil)
}

opt_policy_exprs:
  USING '(' a_expr ')' WITH CHECK '(' a_expr ')'
  {
    $$.val = tree.PolicyExpressions{
      Using: $3.expr(),
      WithCheck: $8.expr(),
    }
  }
| WITH CHECK '(' a_expr ')' USING '(' a_expr ')'
  {
    $$.val = tree.PolicyExpressions{
      Using: $8.expr(),
      WithCheck: $4.expr(),
    }
  }
| WITH CHECK '(' a_expr ')'
  {
    $$.val = tree.PolicyExpressions{
      WithCheck: $4.expr(),
    }
 	}
| USING '(' a_expr ')'
  {
    $$.val = tree.PolicyExpressions{
      Using: $3.expr(),
    }
 	}
| /* EMPTY */ {
   $$.val = tree.PolicyExpressions{}
}

table_rls_mode:
  ENABLE
  {
    $$.val = tree.TableRLSEnable
  }
| DISABLE
  {
    $$.val = tree.TableRLSDisable
  }
| FORCE
  {
    $$.val = tree.TableRLSForce
  }
| NO FORCE
  {
    $$.val = tree.TableRLSNoForce
  }

// %Help: CREATE FUNCTION - define a new function
// %Category: DDL
// %Text:
// CREATE [ OR REPLACE ] FUNCTION
//    name ( [ [ argmode ] [ argname ] argtype [, ...] ] )
//    [ RETURNS rettype ]
//  { LANGUAGE lang_name
//    | { IMMUTABLE | STABLE | VOLATILE }
//    | [ NOT ] LEAKPROOF
//    | { CALLED ON NULL INPUT | RETURNS NULL ON NULL INPUT | STRICT }
//    | AS 'definition'
//    | { [ EXTERNAL ] SECURITY { INVOKER | DEFINER } }
//  } ...
// %SeeAlso: WEBDOCS/create-function.html
create_func_stmt:
  CREATE opt_or_replace FUNCTION routine_create_name '(' opt_routine_param_with_default_list ')'
  RETURNS opt_return_set routine_return_type opt_create_routine_opt_list opt_routine_body
  {
    name := $4.unresolvedObjectName().ToRoutineName()
    $$.val = &tree.CreateRoutine{
      IsProcedure: false,
      Replace: $2.bool(),
      Name: name,
      Params: $6.routineParams(),
      ReturnType: &tree.RoutineReturnType{
        Type: $10.typeReference(),
        SetOf: $9.bool(),
      },
      Options: $11.routineOptions(),
      RoutineBody: $12.routineBody(),
    }
  }
| CREATE opt_or_replace FUNCTION routine_create_name '(' opt_routine_param_with_default_list ')'
  RETURNS TABLE '(' table_func_column_list ')' opt_create_routine_opt_list opt_routine_body
  {
    // RETURNS TABLE is syntactic sugar for RETURNS SETOF with:
    // - RECORD if there are multiple TABLE parameters, or
    // - the type of the single TABLE parameter.
    // The TABLE parameters are added to the list of routine parameters.
    tableParams := $11.routineParams()
    returnType := tree.ResolvableTypeReference(types.AnyTuple)
    if len(tableParams) == 1 {
      returnType = tableParams[0].Type
    }
    routineParams := $6.routineParams()
    for i := range routineParams {
      // OUT parameters are not allowed in table functions.
      if tree.IsOutParamClass(routineParams[i].Class) {
        return setErrNoDetails(sqllex, errors.New("OUT and INOUT arguments aren't allowed in TABLE functions"))
      }
    }
    $$.val = &tree.CreateRoutine{
      IsProcedure: false,
      Replace: $2.bool(),
      Name: $4.unresolvedObjectName().ToRoutineName(),
      Params: append(routineParams, tableParams...),
      ReturnType: &tree.RoutineReturnType{
        Type: returnType,
        SetOf: true,
      },
      Options: $13.routineOptions(),
      RoutineBody: $14.routineBody(),
    }
  }
| CREATE opt_or_replace FUNCTION routine_create_name '(' opt_routine_param_with_default_list ')'
    opt_create_routine_opt_list opt_routine_body
    {
        name := $4.unresolvedObjectName().ToRoutineName()
        $$.val = &tree.CreateRoutine{
          IsProcedure: false,
          Replace: $2.bool(),
          Name: name,
          Params: $6.routineParams(),
          ReturnType: (*tree.RoutineReturnType)(nil),
          Options: $8.routineOptions(),
          RoutineBody: $9.routineBody(),
        }
    }
| CREATE opt_or_replace FUNCTION error // SHOW HELP: CREATE FUNCTION

// %Help: CREATE PROCEDURE - define a new procedure
// %Category: DDL
// %Text:
// CREATE [ OR REPLACE ] PROCEDURE
//    name ( [ [ argmode ] [ argname ] argtype [, ...] ] )
//  { LANGUAGE lang_name
//    | AS 'definition'
//  } ...
// %SeeAlso: WEBDOCS/create-procedure.html
create_proc_stmt:
  CREATE opt_or_replace PROCEDURE routine_create_name '(' opt_routine_param_with_default_list ')'
  opt_create_routine_opt_list opt_routine_body
  {
    name := $4.unresolvedObjectName().ToRoutineName()
    $$.val = &tree.CreateRoutine{
      IsProcedure: true,
      Replace: $2.bool(),
      Name: name,
      Params: $6.routineParams(),
      Options: $8.routineOptions(),
      RoutineBody: $9.routineBody(),
      ReturnType: (*tree.RoutineReturnType)(nil),
    }
  }
| CREATE opt_or_replace PROCEDURE error // SHOW HELP: CREATE PROCEDURE

opt_or_replace:
  OR REPLACE { $$.val = true }
| /* EMPTY */ { $$.val = false }

opt_return_set:
  SETOF { $$.val = true}
| /* EMPTY */ { $$.val = false }

routine_create_name:
  db_object_name

opt_routine_param_with_default_list:
  routine_param_with_default_list { $$.val = $1.routineParams() }
| /* Empty */ { $$.val = tree.RoutineParams{} }

routine_param_with_default_list:
  routine_param_with_default { $$.val = tree.RoutineParams{$1.routineParam()} }
| routine_param_with_default_list ',' routine_param_with_default
  {
    $$.val = append($1.routineParams(), $3.routineParam())
  }

routine_param_with_default:
  routine_param
| routine_param DEFAULT a_expr
  {
    arg := $1.routineParam()
    arg.DefaultVal = $3.expr()
    $$.val = arg
  }
| routine_param '=' a_expr
  {
    arg := $1.routineParam()
    arg.DefaultVal = $3.expr()
    $$.val = arg
  }

routine_param:
  routine_param_class param_name routine_param_type
  {
    $$.val = tree.RoutineParam{
      Name: tree.Name($2),
      Type: $3.typeReference(),
      Class: $1.routineParamClass(),
    }
  }
| param_name routine_param_class routine_param_type
  {
    $$.val = tree.RoutineParam{
      Name: tree.Name($1),
      Type: $3.typeReference(),
      Class: $2.routineParamClass(),
    }
  }
| param_name routine_param_type
  {
    $$.val = tree.RoutineParam{
      Name: tree.Name($1),
      Type: $2.typeReference(),
      Class: tree.RoutineParamDefault,
    }
  }
| routine_param_class routine_param_type
  {
    $$.val = tree.RoutineParam{
      Type: $2.typeReference(),
      Class: $1.routineParamClass(),
    }
  }
| routine_param_type
  {
    $$.val = tree.RoutineParam{
      Type: $1.typeReference(),
      Class: tree.RoutineParamDefault,
    }
  }

routine_param_class:
  IN { $$.val = tree.RoutineParamIn }
| OUT { $$.val = tree.RoutineParamOut }
| INOUT { $$.val = tree.RoutineParamInOut }
| IN OUT { $$.val = tree.RoutineParamInOut }
| VARIADIC { return unimplementedWithIssueDetail(sqllex, 88947, "variadic user-defined functions") }

routine_param_type:
  typename

routine_return_type:
  routine_param_type

table_func_column: param_name routine_param_type
  {
    $$.val = tree.RoutineParam{
      Name: tree.Name($1),
      Type: $2.typeReference(),
      Class: tree.RoutineParamOut,
    }
  }

table_func_column_list:
  table_func_column
  {
    $$.val = tree.RoutineParams{$1.routineParam()}
  }
| table_func_column_list ',' table_func_column
  {
    $$.val = append($1.routineParams(), $3.routineParam())
  }

opt_create_routine_opt_list:
  create_routine_opt_list { $$.val = $1.routineOptions() }
| /* EMPTY */ { $$.val = tree.RoutineOptions{} }

create_routine_opt_list:
  create_routine_opt_item { $$.val = tree.RoutineOptions{$1.functionOption()} }
| create_routine_opt_list create_routine_opt_item
  {
    $$.val = append($1.routineOptions(), $2.functionOption())
  }

create_routine_opt_item:
  AS routine_as opt_link_sym
  {
    $$.val = tree.RoutineBodyStr($2)
  }
| LANGUAGE non_reserved_word_or_sconst
  {
    lang, err := tree.AsRoutineLanguage($2)
    if err != nil {
      return setErr(sqllex, err)
    }
    $$.val = lang
  }
| TRANSFORM { return unimplemented(sqllex, "create transform function") }
| WINDOW { return unimplemented(sqllex, "create window function") }
| common_routine_opt_item
  {
    $$.val = $1.functionOption()
  }

common_routine_opt_item:
  CALLED ON NULL INPUT
  {
    $$.val = tree.RoutineCalledOnNullInput
  }
| RETURNS NULL ON NULL INPUT
  {
    $$.val = tree.RoutineReturnsNullOnNullInput
  }
| STRICT
  {
    $$.val = tree.RoutineStrict
  }
| IMMUTABLE
  {
    $$.val = tree.RoutineImmutable
  }
| STABLE
  {
    $$.val = tree.RoutineStable
  }
| VOLATILE
  {
    $$.val = tree.RoutineVolatile
  }
| EXTERNAL SECURITY DEFINER
  {
    $$.val = tree.RoutineDefiner
  }
| EXTERNAL SECURITY INVOKER
  {
    $$.val = tree.RoutineInvoker
  }
| SECURITY DEFINER
  {
    $$.val = tree.RoutineDefiner
  }
| SECURITY INVOKER
  {
    $$.val = tree.RoutineInvoker
  }
| LEAKPROOF
  {
    $$.val = tree.RoutineLeakproof(true)
  }
| NOT LEAKPROOF
  {
    $$.val = tree.RoutineLeakproof(false)
  }
| COST numeric_only
  {
    return unimplemented(sqllex, "create function/procedure ... cost")
  }
| ROWS numeric_only
  {
    return unimplemented(sqllex, "create function/procedure ... rows")
  }
| SUPPORT name
  {
    return unimplemented(sqllex, "create function/procedure ... support")
  }

// In theory we should parse the a whole set/reset statement here. But it's fine
// to just return fast on SET/RESET keyword for now since it's not supported
// yet.
| SET { return unimplemented(sqllex, "create function/procedure ... set") }
| PARALLEL { return unimplemented(sqllex, "create function/procedure ... parallel") }

routine_as:
  SCONST

routine_return_stmt:
  RETURN a_expr
{
  $$.val = &tree.RoutineReturn{
    ReturnVal: $2.expr(),
  }
}

routine_body_stmt:
  stmt_without_legacy_transaction
| routine_return_stmt

routine_body_stmt_list:
  routine_body_stmt_list routine_body_stmt ';'
  {
    $$.val = append($1.stmts(), $2.stmt())
  }
| /* Empty */
  {
    $$.val = tree.Statements{}
  }

opt_routine_body:
  routine_return_stmt
  {
    $$.val = &tree.RoutineBody{
      Stmts: tree.Statements{$1.stmt()},
    }
  }
| BEGIN ATOMIC routine_body_stmt_list END
  {
    $$.val = &tree.RoutineBody{
        Stmts: $3.stmts(),
    }
  }
| /* Empty */
  {
    $$.val = (*tree.RoutineBody)(nil)
  }

opt_link_sym:
  ',' SCONST
  {
  }
| /* Empty */
  {
  }

// %Help: DO - execute an anonymous code block
// %Category: Misc
// %Text:
// DO [LANGUAGE lang_name] code
do_stmt:
  DO do_stmt_opt_list
  {
    doBlockBody, err := ParseDoBlockFn($2.doBlockOptions())
    if err != nil {
      return setErrNoDetails(sqllex, err)
    }
    $$.val = &tree.DoBlock{Code: doBlockBody}
  }
| DO error // SHOW HELP: DO

do_stmt_opt_list:
  do_stmt_opt_item
  {
    $$.val = tree.DoBlockOptions{$1.doBlockOption()}
  }
| do_stmt_opt_list do_stmt_opt_item
  {
    $$.val = append($1.doBlockOptions(), $2.doBlockOption())
  }

do_stmt_opt_item:
  SCONST
  {
    $$.val = tree.RoutineBodyStr($1)
  }
| LANGUAGE non_reserved_word_or_sconst
  {
    lang, err := tree.AsRoutineLanguage($2)
    if err != nil {
      return setErr(sqllex, err)
    }
    $$.val = lang
  }

// %Help: DROP FUNCTION - remove a function
// %Category: DDL
// %Text:
// DROP FUNCTION [ IF EXISTS ] name [ ( [ [ argmode ] [ argname ] argtype [, ...] ] ) ] [, ...]
//    [ CASCADE | RESTRICT ]
// %SeeAlso: WEBDOCS/drop-function.html
drop_func_stmt:
  DROP FUNCTION function_with_paramtypes_list opt_drop_behavior
  {
    $$.val = &tree.DropRoutine{
      Routines: $3.routineObjs(),
      DropBehavior: $4.dropBehavior(),
    }
  }
| DROP FUNCTION IF EXISTS function_with_paramtypes_list opt_drop_behavior
  {
    $$.val = &tree.DropRoutine{
      IfExists: true,
      Routines: $5.routineObjs(),
      DropBehavior: $6.dropBehavior(),
    }
  }
| DROP FUNCTION error // SHOW HELP: DROP FUNCTION

// %Help: DROP PROCEDURE - remove a procedure
// %Category: DDL
// %Text:
// DROP PROCEDURE [ IF EXISTS ] name [ ( [ [ argmode ] [ argname ] argtype [, ...] ] ) ] [, ...]
//    [ CASCADE | RESTRICT ]
// %SeeAlso: WEBDOCS/drop-procedure.html
drop_proc_stmt:
  DROP PROCEDURE function_with_paramtypes_list opt_drop_behavior
  {
    $$.val = &tree.DropRoutine{
      Procedure: true,
      Routines: $3.routineObjs(),
      DropBehavior: $4.dropBehavior(),
    }
  }
| DROP PROCEDURE IF EXISTS function_with_paramtypes_list opt_drop_behavior
  {
    $$.val = &tree.DropRoutine{
      IfExists: true,
      Procedure: true,
      Routines: $5.routineObjs(),
      DropBehavior: $6.dropBehavior(),
    }
  }
| DROP PROCEDURE error // SHOW HELP: DROP PROCEDURE

function_with_paramtypes_list:
  function_with_paramtypes
  {
    $$.val = tree.RoutineObjs{$1.functionObj()}
  }
  | function_with_paramtypes_list ',' function_with_paramtypes
  {
    $$.val = append($1.routineObjs(), $3.functionObj())
  }

function_with_paramtypes:
  db_object_name func_params
  {
    $$.val = tree.RoutineObj{
      FuncName: $1.unresolvedObjectName().ToRoutineName(),
      Params: $2.routineParams(),
    }
  }
  | db_object_name
  {
    $$.val = tree.RoutineObj{
      FuncName: $1.unresolvedObjectName().ToRoutineName(),
    }
  }

func_params:
  '(' func_params_list ')'
  {
    $$.val = $2.routineParams()
  }
  | '(' ')'
  {
    $$.val = tree.RoutineParams{}
  }

func_params_list:
  routine_param
  {
    $$.val = tree.RoutineParams{$1.routineParam()}
  }
  | func_params_list ',' routine_param
  {
    $$.val = append($1.routineParams(), $3.routineParam())
  }

alter_func_options_stmt:
  ALTER FUNCTION function_with_paramtypes alter_func_opt_list opt_restrict
  {
    $$.val = &tree.AlterFunctionOptions{
      Function: $3.functionObj(),
      Options: $4.routineOptions(),
    }
  }

alter_func_opt_list:
  common_routine_opt_item
  {
    $$.val = tree.RoutineOptions{$1.functionOption()}
  }
| alter_func_opt_list common_routine_opt_item
  {
    $$.val = append($1.routineOptions(), $2.functionOption())
  }

opt_restrict:
  RESTRICT {}
| /* EMPTY */ {}

alter_func_rename_stmt:
  ALTER FUNCTION function_with_paramtypes RENAME TO name
  {
    $$.val = &tree.AlterRoutineRename{
      Function: $3.functionObj(),
      NewName: tree.Name($6),
    }
  }

alter_func_set_schema_stmt:
  ALTER FUNCTION function_with_paramtypes SET SCHEMA schema_name
  {
    $$.val = &tree.AlterRoutineSetSchema{
      Function: $3.functionObj(),
      NewSchemaName: tree.Name($6),
    }
  }

alter_func_owner_stmt:
  ALTER FUNCTION function_with_paramtypes OWNER TO role_spec
  {
    $$.val = &tree.AlterRoutineSetOwner{
      Function: $3.functionObj(),
      NewOwner: $6.roleSpec(),
    }
  }

alter_func_dep_extension_stmt:
  ALTER FUNCTION function_with_paramtypes opt_no DEPENDS ON EXTENSION name
  {
    $$.val = &tree.AlterFunctionDepExtension{
      Function: $3.functionObj(),
      Remove: $4.bool(),
      Extension: tree.Name($8),
    }
  }

alter_proc_rename_stmt:
  ALTER PROCEDURE function_with_paramtypes RENAME TO name
  {
    $$.val = &tree.AlterRoutineRename{
      Function: $3.functionObj(),
      NewName: tree.Name($6),
      Procedure: true,
    }
  }

alter_proc_set_schema_stmt:
  ALTER PROCEDURE function_with_paramtypes SET SCHEMA schema_name
  {
    $$.val = &tree.AlterRoutineSetSchema{
      Function: $3.functionObj(),
      NewSchemaName: tree.Name($6),
      Procedure: true,
    }
  }

alter_proc_owner_stmt:
  ALTER PROCEDURE function_with_paramtypes OWNER TO role_spec
  {
    $$.val = &tree.AlterRoutineSetOwner{
      Function: $3.functionObj(),
      NewOwner: $6.roleSpec(),
      Procedure: true,
    }
  }

opt_no:
  NO
  {
    $$.val = true
  }
| /* EMPTY */
  {
    $$.val = false
  }

// %Help: CREATE TRIGGER - define a new trigger
// %Category: DDL
// %Text:
// CREATE [ OR REPLACE ] TRIGGER name { BEFORE | AFTER | INSTEAD OF }
//  { INSERT | DELETE | UPDATE [ OF column_name [, ...] ] | TRUNCATE | UPSERT }
//  ON table_name [ REFERENCING { NEW | OLD } TABLE [ AS ] table_alias_name [...] ]
//  [ FOR { EACH ROW | EACH STATEMENT } ]
//  [ WHEN a_expr ]
//  EXECUTE FUNCTION func_name ( trigger_func_args )
create_trigger_stmt:
  CREATE opt_or_replace TRIGGER name trigger_action_time trigger_event_list
  ON table_name opt_trigger_transition_list trigger_for_each trigger_when
  EXECUTE function_or_procedure func_name '(' trigger_func_args ')'
  {
    $$.val = &tree.CreateTrigger{
      Replace: $2.bool(),
      Name: tree.Name($4),
      ActionTime: $5.triggerActionTime(),
      Events: $6.triggerEvents(),
      TableName: $8.unresolvedObjectName(),
      Transitions: $9.triggerTransitions(),
      ForEach: $10.triggerForEach(),
      When: $11.expr(),
      FuncName: $14.unresolvedName(),
      FuncArgs: $16.strs(),
    }
  }
| CREATE opt_or_replace TRIGGER error // SHOW HELP: CREATE TRIGGER

trigger_action_time:
  BEFORE { $$.val = tree.TriggerActionTimeBefore }
| AFTER { $$.val = tree.TriggerActionTimeAfter }
| INSTEAD OF { $$.val = tree.TriggerActionTimeInsteadOf }

trigger_event_list:
  trigger_event
  {
    $$.val = []*tree.TriggerEvent{$1.triggerEvent()}
  }
| trigger_event_list OR trigger_event
  {
    events := append($1.triggerEvents(), $3.triggerEvent())

    // Validate that the trigger events are unique.
    var seenEvents tree.TriggerEventTypeSet
    for i := range events {
      if seenEvents.Contains(events[i].EventType) {
        return setErr(sqllex, errors.New("duplicate trigger events specified"))
      }
      seenEvents.Add(events[i].EventType)
    }
    $$.val = events
  }

trigger_event:
  INSERT
  {
    $$.val = &tree.TriggerEvent{EventType: tree.TriggerEventInsert}
  }
| DELETE
  {
    $$.val = &tree.TriggerEvent{EventType: tree.TriggerEventDelete}
  }
| UPDATE
  {
    $$.val = &tree.TriggerEvent{EventType: tree.TriggerEventUpdate}
  }
| UPDATE OF name_list
  {
    $$.val = &tree.TriggerEvent{EventType: tree.TriggerEventUpdate, Columns: $3.nameList()}
  }
| TRUNCATE
  {
    $$.val = &tree.TriggerEvent{EventType: tree.TriggerEventTruncate}
  }

opt_trigger_transition_list:
  REFERENCING trigger_transition_list
  {
    $$.val = $2.triggerTransitions()
  }
| /* EMPTY */
  {
    $$.val = []*tree.TriggerTransition{}
  }

trigger_transition_list:
  trigger_transition
  {
    $$.val = []*tree.TriggerTransition{$1.triggerTransition()}
  }
| trigger_transition_list trigger_transition
  {
    $$.val = append($1.triggerTransitions(), $2.triggerTransition())
  }

trigger_transition:
  transition_is_new transition_is_row opt_as table_alias_name
  {
    $$.val = &tree.TriggerTransition{
      Name: tree.Name($4),
      IsNew: $1.bool(),
      IsRow: $2.bool(),
    }
  }

opt_as:
  AS {}
| /* EMPTY */ {}

transition_is_new:
  NEW { $$.val = true }
| OLD { $$.val = false }

transition_is_row:
  ROW { $$.val = true }
| TABLE { $$.val = false }

trigger_for_each:
  FOR trigger_for_opt_each trigger_for_type
  {
    $$.val = $3.triggerForEach()
  }
| /* EMPTY */
  {
    // FOR EACH STATEMENT is the default.
    $$.val = tree.TriggerForEachStatement
  }

trigger_for_opt_each:
  EACH {}
| /* EMPTY */ {}

trigger_for_type:
  ROW { $$.val = tree.TriggerForEachRow }
| STATEMENT { $$.val = tree.TriggerForEachStatement }

trigger_when:
  WHEN a_expr
  {
    $$.val = $2.expr()
  }
| /* EMPTY */
  {
    $$.val = nil
  }

function_or_procedure:
  FUNCTION {}
| PROCEDURE {}

trigger_func_args:
  trigger_func_arg
  {
    $$.val = []string{$1}
  }
| trigger_func_args ',' trigger_func_arg
  {
    $$.val = append($1.strs(), $3)
  }
| /* EMPTY */
  {
    $$.val = []string{}
  }

trigger_func_arg:
  ICONST
  {
    // NOTE: Calling OrigString() ignores the sign for signed constants.
    // However, this doesn't matter because postgres doesn't parse a signed
    // numerical constant, and so neither do we. This applies to the FCONST
    // case too.
    $$ = $1.numVal().OrigString()
  }
| FCONST
  {
    $$ = $1.numVal().OrigString()
  }
| SCONST
  {
    $$ = $1
  }
| unrestricted_name
  {
    $$ = $1
  }

// %Help: DROP TRIGGER - remove a trigger
// %Category: DDL
// %Text:
// DROP TRIGGER [ IF EXISTS ] name ON table_name [ CASCADE | RESTRICT ]
drop_trigger_stmt:
  DROP TRIGGER name ON table_name opt_drop_behavior
  {
    $$.val = &tree.DropTrigger{
      Trigger: tree.Name($3),
      Table: $5.unresolvedObjectName(),
      DropBehavior: $6.dropBehavior(),
    }
  }
| DROP TRIGGER IF EXISTS name ON table_name opt_drop_behavior
  {
    $$.val = &tree.DropTrigger{
      IfExists: true,
      Trigger: tree.Name($5),
      Table: $7.unresolvedObjectName(),
      DropBehavior: $8.dropBehavior(),
    }
  }
| DROP TRIGGER error // SHOW HELP: DROP TRIGGER

create_unsupported:
  CREATE ACCESS METHOD error { return unimplemented(sqllex, "create access method") }
| CREATE AGGREGATE error { return unimplementedWithIssueDetail(sqllex, 74775, "create aggregate") }
| CREATE CAST error { return unimplemented(sqllex, "create cast") }
| CREATE CONSTRAINT TRIGGER error { return unimplementedWithIssueDetail(sqllex, 28296, "create constraint") }
| CREATE CONVERSION error { return unimplemented(sqllex, "create conversion") }
| CREATE DEFAULT CONVERSION error { return unimplemented(sqllex, "create def conv") }
| CREATE FOREIGN TABLE error { return unimplemented(sqllex, "create foreign table") }
| CREATE FOREIGN DATA error { return unimplemented(sqllex, "create fdw") }
| CREATE opt_or_replace opt_trusted opt_procedural LANGUAGE name error { return unimplementedWithIssueDetail(sqllex, 17511, "create language " + $6) }
| CREATE OPERATOR error { return unimplementedWithIssue(sqllex, 65017) }
| CREATE PUBLICATION error { return unimplemented(sqllex, "create publication") }
| CREATE opt_or_replace RULE error { return unimplemented(sqllex, "create rule") }
| CREATE SERVER error { return unimplemented(sqllex, "create server") }
| CREATE SUBSCRIPTION error { return unimplemented(sqllex, "create subscription") }
| CREATE TABLESPACE error { return unimplementedWithIssueDetail(sqllex, 54113, "create tablespace") }
| CREATE TEXT error { return unimplementedWithIssueDetail(sqllex, 7821, "create text") }

opt_trusted:
  TRUSTED {}
| /* EMPTY */ {}

opt_procedural:
  PROCEDURAL {}
| /* EMPTY */ {}

drop_unsupported:
  DROP ACCESS METHOD error { return unimplemented(sqllex, "drop access method") }
| DROP AGGREGATE error { return unimplementedWithIssueDetail(sqllex, 74775, "drop aggregate") }
| DROP CAST error { return unimplemented(sqllex, "drop cast") }
| DROP COLLATION error { return unimplemented(sqllex, "drop collation") }
| DROP CONVERSION error { return unimplemented(sqllex, "drop conversion") }
| DROP DOMAIN error { return unimplementedWithIssueDetail(sqllex, 27796, "drop") }
| DROP EXTENSION IF EXISTS name error { return unimplementedWithIssueDetail(sqllex, 74777, "drop extension if exists") }
| DROP EXTENSION name error { return unimplementedWithIssueDetail(sqllex, 74777, "drop extension") }
| DROP FOREIGN TABLE error { return unimplemented(sqllex, "drop foreign table") }
| DROP FOREIGN DATA error { return unimplemented(sqllex, "drop fdw") }
| DROP opt_procedural LANGUAGE name error { return unimplementedWithIssueDetail(sqllex, 17511, "drop language " + $4) }
| DROP OPERATOR error { return unimplemented(sqllex, "drop operator") }
| DROP PUBLICATION error { return unimplemented(sqllex, "drop publication") }
| DROP RULE error { return unimplemented(sqllex, "drop rule") }
| DROP SERVER error { return unimplemented(sqllex, "drop server") }
| DROP SUBSCRIPTION error { return unimplemented(sqllex, "drop subscription") }
| DROP TEXT error { return unimplementedWithIssueDetail(sqllex, 7821, "drop text") }

create_ddl_stmt:
  create_database_stmt // EXTEND WITH HELP: CREATE DATABASE
| create_index_stmt    // EXTEND WITH HELP: CREATE INDEX
| create_schema_stmt   // EXTEND WITH HELP: CREATE SCHEMA
| create_table_stmt    // EXTEND WITH HELP: CREATE TABLE
| create_table_as_stmt // EXTEND WITH HELP: CREATE TABLE
// Error case for both CREATE TABLE and CREATE TABLE ... AS in one
| CREATE opt_persistence_temp_table TABLE error   // SHOW HELP: CREATE TABLE
| create_type_stmt     // EXTEND WITH HELP: CREATE TYPE
| create_view_stmt     // EXTEND WITH HELP: CREATE VIEW
| create_sequence_stmt // EXTEND WITH HELP: CREATE SEQUENCE
| create_func_stmt     // EXTEND WITH HELP: CREATE FUNCTION
| create_proc_stmt     // EXTEND WITH HELP: CREATE PROCEDURE
| create_trigger_stmt  // EXTEND WITH HELP: CREATE TRIGGER
| create_policy_stmt   // EXTEND WITH HELP: CREATE POLICY

// %Help: CREATE STATISTICS - create a new table statistic
// %Category: Misc
// %Text:
// CREATE STATISTICS <statisticname>
//   [ON <colname> [, ...]]
//   FROM <tablename> [AS OF SYSTEM TIME <expr>]
create_stats_stmt:
  CREATE STATISTICS statistics_name opt_stats_columns FROM create_stats_target opt_create_stats_options
  {
    $$.val = &tree.CreateStats{
      Name: tree.Name($3),
      ColumnNames: $4.nameList(),
      Table: $6.tblExpr(),
      Options: *$7.createStatsOptions(),
    }
  }
| CREATE STATISTICS error // SHOW HELP: CREATE STATISTICS

opt_stats_columns:
  ON name_list
  {
    $$.val = $2.nameList()
  }
| /* EMPTY */
  {
    $$.val = tree.NameList(nil)
  }

create_stats_target:
  table_name
  {
    $$.val = $1.unresolvedObjectName()
  }
| '[' iconst64 ']'
  {
    /* SKIP DOC */
    $$.val = &tree.TableRef{
      TableID: $2.int64(),
    }
  }

opt_create_stats_options:
  create_stats_option_list
  {
    $$.val = $1.createStatsOptions()
  }
| WITH OPTIONS create_stats_option_list
  {
    $$.val = $3.createStatsOptions()
  }
| /* EMPTY */
  {
    $$.val = &tree.CreateStatsOptions{}
  }

create_stats_option_list:
  create_stats_option
  {
    $$.val = $1.createStatsOptions()
  }
| create_stats_option_list create_stats_option
  {
    a := $1.createStatsOptions()
    b := $2.createStatsOptions()
    if err := a.CombineWith(b); err != nil {
      return setErr(sqllex, err)
    }
    $$.val = a
  }

create_stats_option:
  THROTTLING FCONST
  {
    /* SKIP DOC */
    value, _ := constant.Float64Val($2.numVal().AsConstantValue())
    if value < 0.0 || value >= 1.0 {
      sqllex.Error("THROTTLING fraction must be between 0 and 1")
      return 1
    }
    $$.val = &tree.CreateStatsOptions{
      Throttling: value,
    }
  }
| as_of_clause
  {
    $$.val = &tree.CreateStatsOptions{
      AsOf: $1.asOfClause(),
    }
  }
| USING EXTREMES
  {
    $$.val = &tree.CreateStatsOptions{
      UsingExtremes: true,
    }
  }
| where_clause
  {
    $$.val = &tree.CreateStatsOptions{
      Where: tree.NewWhere(tree.AstWhere, $1.expr()),
    }
  }

// %Help: CREATE CHANGEFEED  - create change data capture
// %Category: CCL
// %Text:
// CREATE CHANGEFEED
// FOR <targets> [INTO sink] [WITH <options>]
//
// sink: data capture stream destination (Enterprise only)
create_changefeed_stmt:
  CREATE CHANGEFEED FOR changefeed_targets opt_changefeed_sink opt_with_options
  {
    $$.val = &tree.CreateChangefeed{
      Targets: $4.changefeedTargets(),
      SinkURI: $5.expr(),
      Options: $6.kvOptions(),
    }
  }
| CREATE CHANGEFEED /*$3=*/ opt_changefeed_sink /*$4=*/ opt_with_options
  AS SELECT /*$7=*/target_list FROM /*$9=*/changefeed_target_expr /*$10=*/opt_where_clause
  {
    target, err := tree.ChangefeedTargetFromTableExpr($9.tblExpr())
    if err != nil {
      return setErr(sqllex, err)
    }

    $$.val = &tree.CreateChangefeed{
      SinkURI: $3.expr(),
      Options: $4.kvOptions(),
      Targets: tree.ChangefeedTargets{target},
      Select:  &tree.SelectClause{
         Exprs: $7.selExprs(),
         From:  tree.From{Tables: tree.TableExprs{$9.tblExpr()}},
         Where: tree.NewWhere(tree.AstWhere, $10.expr()),
      },
    }
  }
| EXPERIMENTAL CHANGEFEED FOR changefeed_targets opt_with_options
  {
    /* SKIP DOC */
    $$.val = &tree.CreateChangefeed{
      Targets: $4.changefeedTargets(),
      Options: $5.kvOptions(),
    }
  }

// %Help: CREATE SCHEDULE FOR CHANGEFEED - create changefeed periodically
// %Category: CCL
// %Text:
// CREATE SCHEDULE [IF NOT EXISTS]
// [<description>]
// FOR CHANGEFEED
// <targets> INTO <sink> [WITH <options>]
// RECURRING [crontab|NEVER]
// [WITH EXPERIMENTAL SCHEDULE OPTIONS <schedule_option>[= <value>] [, ...] ]
//
// All changefeeds run in UTC timezone.
//
// Description:
//   Optional description (or name) for this schedule
//
// RECURRING <crontab>:
//   The RECURRING expression specifies when export runs
//   Schedule specified as a string in crontab format.
//   All times in UTC.
//     "5 0 * * *": run schedule 5 minutes past midnight.
//     "@daily": run daily, at midnight
//   See https://en.wikipedia.org/wiki/Cron
//
// sink: data capture stream destination (Enterprise only)
// %SeeAlso: CREATE CHANGEFEED
create_schedule_for_changefeed_stmt:
  CREATE SCHEDULE /*$3=*/schedule_label_spec FOR CHANGEFEED
  /* $6=*/changefeed_targets /*$7=*/changefeed_sink
  /*$8=*/opt_with_options /*$9=*/cron_expr /*$10=*/opt_with_schedule_options
  {
     $$.val = &tree.ScheduledChangefeed{
        CreateChangefeed:   &tree.CreateChangefeed{
          Targets:    $6.changefeedTargets(),
          SinkURI:    $7.expr(),
          Options:    $8.kvOptions(),
        },
        ScheduleLabelSpec:  *($3.scheduleLabelSpec()),
        Recurrence:         $9.expr(),
				ScheduleOptions:    $10.kvOptions(),
     }
  }
| CREATE SCHEDULE /*$3=*/schedule_label_spec FOR CHANGEFEED /*$6=*/changefeed_sink
  /*$7=*/opt_with_options AS SELECT /*$10=*/target_list FROM /*$12=*/changefeed_target_expr /*$13=*/opt_where_clause
  /*$14=*/cron_expr /*$15=*/opt_with_schedule_options
  {
    target, err := tree.ChangefeedTargetFromTableExpr($12.tblExpr())
    if err != nil {
      return setErr(sqllex, err)
    }

    createChangefeedNode := &tree.CreateChangefeed{
      SinkURI: $6.expr(),
      Options: $7.kvOptions(),
      Targets: tree.ChangefeedTargets{target},
      Select:  &tree.SelectClause{
         Exprs: $10.selExprs(),
         From:  tree.From{Tables: tree.TableExprs{$12.tblExpr()}},
         Where: tree.NewWhere(tree.AstWhere, $13.expr()),
      },
    }

    $$.val = &tree.ScheduledChangefeed{
			CreateChangefeed:  	createChangefeedNode,
			ScheduleLabelSpec:  *($3.scheduleLabelSpec()),
			Recurrence:         $14.expr(),
			ScheduleOptions:    $15.kvOptions(),
	 }
  }
 | CREATE SCHEDULE schedule_label_spec FOR CHANGEFEED error  // SHOW HELP: CREATE SCHEDULE FOR CHANGEFEED

changefeed_targets:
  changefeed_target
  {
    $$.val = tree.ChangefeedTargets{$1.changefeedTarget()}
  }
| changefeed_targets ',' changefeed_target
  {
    $$.val = append($1.changefeedTargets(), $3.changefeedTarget())
  }

changefeed_target:
  opt_table_prefix table_name opt_changefeed_family
  {
    $$.val = tree.ChangefeedTarget{
      TableName:  $2.unresolvedObjectName().ToUnresolvedName(),
      FamilyName: tree.Name($3),
    }
  }

changefeed_target_expr: insert_target

opt_table_prefix:
  TABLE
  {}
| /* EMPTY */
  {}

opt_changefeed_family:
  FAMILY family_name
  {
    $$ = $2
  }
| /* EMPTY */
  {
    $$ = ""
  }

opt_changefeed_sink:
  INTO string_or_placeholder
  {
    $$.val = $2.expr()
  }
| /* EMPTY */
  {
    /* SKIP DOC */
    $$.val = nil
  }

changefeed_sink:
  INTO string_or_placeholder
  {
    $$.val = $2.expr()
  }
// %Help: DELETE - delete rows from a table
// %Category: DML
// %Text:
// DELETE
//    [BATCH [SIZE <expr>]]
//    FROM <tablename>
//    [WHERE <expr>]
//    [ORDER BY <exprs...>]
//    [USING <exprs...>]
//    [LIMIT <expr>]
//    [RETURNING <exprs...>]
// %SeeAlso: WEBDOCS/delete.html
delete_stmt:
  opt_with_clause DELETE opt_batch_clause FROM table_expr_opt_alias_idx opt_using_clause opt_where_clause opt_sort_clause opt_limit_clause returning_clause
  {
    $$.val = &tree.Delete{
      With: $1.with(),
      Batch: $3.batch(),
      Table: $5.tblExpr(),
      Using: $6.tblExprs(),
      Where: tree.NewWhere(tree.AstWhere, $7.expr()),
      OrderBy: $8.orderBy(),
      Limit: $9.limit(),
      Returning: $10.retClause(),
    }
  }
| opt_with_clause DELETE error // SHOW HELP: DELETE

opt_batch_clause:
  BATCH
  {
    $$.val = &tree.Batch{}
  }
| BATCH '(' batch_param_list ')'
  {
    $$.val = &tree.Batch{Params: $3.batchParams()}
  }
| /* EMPTY */
  {
    $$.val = (*tree.Batch)(nil)
  }

batch_param_list:
  batch_param
  {
    $$.val = []tree.BatchParam{$1.batchParam()}
  }
| batch_param_list ',' batch_param
  {
    $$.val = append($1.batchParams(), $3.batchParam())
  }

batch_param:
  SIZE a_expr
  {
    $$.val = &tree.SizeBatchParam{Size: $2.expr()}
  }

opt_using_clause:
  USING from_list
  {
    $$.val = $2.tblExprs()
  }
| /* EMPTY */
  {
    $$.val = tree.TableExprs{}
  }


// %Help: DISCARD - reset the session to its initial state
// %Category: Cfg
// %Text: DISCARD ALL
discard_stmt:
  DISCARD ALL
  {
    $$.val = &tree.Discard{Mode: tree.DiscardModeAll}
  }
| DISCARD PLANS { return unimplemented(sqllex, "discard plans") }
| DISCARD SEQUENCES
  {
    $$.val = &tree.Discard{Mode: tree.DiscardModeSequences}
  }
| DISCARD TEMP
  {
    $$.val = &tree.Discard{Mode: tree.DiscardModeTemp}
  }
| DISCARD TEMPORARY
  {
    $$.val = &tree.Discard{Mode: tree.DiscardModeTemp}
  }
| DISCARD error // SHOW HELP: DISCARD

// %Help: DROP
// %Category: Group
// %Text:
// DROP DATABASE, DROP INDEX, DROP TABLE, DROP VIEW, DROP SEQUENCE,
// DROP USER, DROP ROLE, DROP TYPE
drop_stmt:
  drop_ddl_stmt                 // help texts in sub-rule
| drop_role_stmt                // EXTEND WITH HELP: DROP ROLE
| drop_schedule_stmt            // EXTEND WITH HELP: DROP SCHEDULES
| drop_external_connection_stmt // EXTEND WITH HELP: DROP EXTERNAL CONNECTION
| drop_virtual_cluster_stmt     // EXTEND WITH HELP: DROP VIRTUAL CLUSTER
| drop_unsupported   {}
| DROP error                    // SHOW HELP: DROP

drop_ddl_stmt:
  drop_database_stmt // EXTEND WITH HELP: DROP DATABASE
| drop_index_stmt    // EXTEND WITH HELP: DROP INDEX
| drop_table_stmt    // EXTEND WITH HELP: DROP TABLE
| drop_view_stmt     // EXTEND WITH HELP: DROP VIEW
| drop_sequence_stmt // EXTEND WITH HELP: DROP SEQUENCE
| drop_schema_stmt   // EXTEND WITH HELP: DROP SCHEMA
| drop_type_stmt     // EXTEND WITH HELP: DROP TYPE
| drop_func_stmt     // EXTEND WITH HELP: DROP FUNCTION
| drop_proc_stmt     // EXTEND WITH HELP: DROP FUNCTION
| drop_trigger_stmt  // EXTEND WITH HELP: DROP TRIGGER
| drop_policy_stmt   // EXTEND WITH HELP: DROP POLICY

// %Help: DROP VIEW - remove a view
// %Category: DDL
// %Text: DROP [MATERIALIZED] VIEW [IF EXISTS] <tablename> [, ...] [CASCADE | RESTRICT]
// %SeeAlso: WEBDOCS/drop-index.html
drop_view_stmt:
  DROP VIEW view_name_list opt_drop_behavior
  {
    $$.val = &tree.DropView{Names: $3.tableNames(), IfExists: false, DropBehavior: $4.dropBehavior()}
  }
| DROP VIEW IF EXISTS view_name_list opt_drop_behavior
  {
    $$.val = &tree.DropView{Names: $5.tableNames(), IfExists: true, DropBehavior: $6.dropBehavior()}
  }
| DROP MATERIALIZED VIEW view_name_list opt_drop_behavior
  {
    $$.val = &tree.DropView{
      Names: $4.tableNames(),
      IfExists: false,
      DropBehavior: $5.dropBehavior(),
      IsMaterialized: true,
    }
  }
| DROP MATERIALIZED VIEW IF EXISTS view_name_list opt_drop_behavior
  {
    $$.val = &tree.DropView{
      Names: $6.tableNames(),
      IfExists: true,
      DropBehavior: $7.dropBehavior(),
      IsMaterialized: true,
    }
  }
| DROP VIEW error // SHOW HELP: DROP VIEW

// %Help: DROP SEQUENCE - remove a sequence
// %Category: DDL
// %Text: DROP SEQUENCE [IF EXISTS] <sequenceName> [, ...] [CASCADE | RESTRICT]
// %SeeAlso: DROP
drop_sequence_stmt:
  DROP SEQUENCE sequence_name_list opt_drop_behavior
  {
    $$.val = &tree.DropSequence{Names: $3.tableNames(), IfExists: false, DropBehavior: $4.dropBehavior()}
  }
| DROP SEQUENCE IF EXISTS sequence_name_list opt_drop_behavior
  {
    $$.val = &tree.DropSequence{Names: $5.tableNames(), IfExists: true, DropBehavior: $6.dropBehavior()}
  }
| DROP SEQUENCE error // SHOW HELP: DROP VIEW

// %Help: DROP TABLE - remove a table
// %Category: DDL
// %Text: DROP TABLE [IF EXISTS] <tablename> [, ...] [CASCADE | RESTRICT]
// %SeeAlso: WEBDOCS/drop-table.html
drop_table_stmt:
  DROP TABLE table_name_list opt_drop_behavior
  {
    $$.val = &tree.DropTable{Names: $3.tableNames(), IfExists: false, DropBehavior: $4.dropBehavior()}
  }
| DROP TABLE IF EXISTS table_name_list opt_drop_behavior
  {
    $$.val = &tree.DropTable{Names: $5.tableNames(), IfExists: true, DropBehavior: $6.dropBehavior()}
  }
| DROP TABLE error // SHOW HELP: DROP TABLE

// %Help: DROP INDEX - remove an index
// %Category: DDL
// %Text: DROP INDEX [CONCURRENTLY] [IF EXISTS] <idxname> [, ...] [CASCADE | RESTRICT]
// %SeeAlso: WEBDOCS/drop-index.html
drop_index_stmt:
  DROP INDEX opt_concurrently table_index_name_list opt_drop_behavior
  {
    $$.val = &tree.DropIndex{
      IndexList: $4.newTableIndexNames(),
      IfExists: false,
      DropBehavior: $5.dropBehavior(),
      Concurrently: $3.bool(),
    }
  }
| DROP INDEX opt_concurrently IF EXISTS table_index_name_list opt_drop_behavior
  {
    $$.val = &tree.DropIndex{
      IndexList: $6.newTableIndexNames(),
      IfExists: true,
      DropBehavior: $7.dropBehavior(),
      Concurrently: $3.bool(),
    }
  }
| DROP INDEX error // SHOW HELP: DROP INDEX

// %Help: DROP DATABASE - remove a database
// %Category: DDL
// %Text: DROP DATABASE [IF EXISTS] <databasename> [CASCADE | RESTRICT]
// %SeeAlso: WEBDOCS/drop-database.html
drop_database_stmt:
  DROP DATABASE database_name opt_drop_behavior
  {
    $$.val = &tree.DropDatabase{
      Name: tree.Name($3),
      IfExists: false,
      DropBehavior: $4.dropBehavior(),
    }
  }
| DROP DATABASE IF EXISTS database_name opt_drop_behavior
  {
    $$.val = &tree.DropDatabase{
      Name: tree.Name($5),
      IfExists: true,
      DropBehavior: $6.dropBehavior(),
    }
  }
| DROP DATABASE error // SHOW HELP: DROP DATABASE

// %Help: DROP TYPE - remove a type
// %Category: DDL
// %Text: DROP TYPE [IF EXISTS] <type_name> [, ...] [CASCASE | RESTRICT]
drop_type_stmt:
  DROP TYPE type_name_list opt_drop_behavior
  {
    $$.val = &tree.DropType{
      Names: $3.unresolvedObjectNames(),
      IfExists: false,
      DropBehavior: $4.dropBehavior(),
    }
  }
| DROP TYPE IF EXISTS type_name_list opt_drop_behavior
  {
    $$.val = &tree.DropType{
      Names: $5.unresolvedObjectNames(),
      IfExists: true,
      DropBehavior: $6.dropBehavior(),
    }
  }
| DROP TYPE error // SHOW HELP: DROP TYPE

// %Help: DROP VIRTUAL CLUSTER - remove a virtual cluster
// %Category: Experimental
// %Text: DROP VIRTUAL CLUSTER [IF EXISTS] <virtual_cluster_spec> [IMMEDIATE]
drop_virtual_cluster_stmt:
  DROP virtual_cluster virtual_cluster_spec opt_immediate
  {
   /* SKIP DOC */
   $$.val = &tree.DropTenant{
      TenantSpec: $3.tenantSpec(),
      IfExists: false,
      Immediate: $4.bool(),
    }
  }
| DROP virtual_cluster IF EXISTS virtual_cluster_spec opt_immediate
  {
    /* SKIP DOC */
    $$.val = &tree.DropTenant{
      TenantSpec: $5.tenantSpec(),
      IfExists: true,
      Immediate: $6.bool(),
    }
  }
| DROP virtual_cluster error // SHOW HELP: DROP VIRTUAL CLUSTER

opt_immediate:
  /* EMPTY */
  { $$.val = false }
| IMMEDIATE
  { $$.val = true }

target_types:
  type_name_list
  {
    $$.val = tree.GrantTargetList{Types: $1.unresolvedObjectNames()}
  }

type_name_list:
  type_name
  {
    $$.val = []*tree.UnresolvedObjectName{$1.unresolvedObjectName()}
  }
| type_name_list ',' type_name
  {
    $$.val = append($1.unresolvedObjectNames(), $3.unresolvedObjectName())
  }

// %Help: DROP SCHEMA - remove a schema
// %Category: DDL
// %Text: DROP SCHEMA [IF EXISTS] <schema_name> [, ...] [CASCADE | RESTRICT]
drop_schema_stmt:
  DROP SCHEMA schema_name_list opt_drop_behavior
  {
    $$.val = &tree.DropSchema{
      Names: $3.objectNamePrefixList(),
      IfExists: false,
      DropBehavior: $4.dropBehavior(),
    }
  }
| DROP SCHEMA IF EXISTS schema_name_list opt_drop_behavior
  {
    $$.val = &tree.DropSchema{
      Names: $5.objectNamePrefixList(),
      IfExists: true,
      DropBehavior: $6.dropBehavior(),
    }
  }
| DROP SCHEMA error // SHOW HELP: DROP SCHEMA

// %Help: DROP ROLE - remove a user
// %Category: Priv
// %Text: DROP ROLE [IF EXISTS] <user> [, ...]
// %SeeAlso: CREATE ROLE, SHOW ROLE
drop_role_stmt:
  DROP role_or_group_or_user role_spec_list
  {
    $$.val = &tree.DropRole{Names: $3.roleSpecList(), IfExists: false, IsRole: $2.bool()}
  }
| DROP role_or_group_or_user IF EXISTS role_spec_list
  {
    $$.val = &tree.DropRole{Names: $5.roleSpecList(), IfExists: true, IsRole: $2.bool()}
  }
| DROP role_or_group_or_user error // SHOW HELP: DROP ROLE

db_object_name_list:
  db_object_name
  {
    name := $1.unresolvedObjectName().ToTableName()
    $$.val = tree.TableNames{name}
  }
| db_object_name_list ',' db_object_name
  {
    name := $3.unresolvedObjectName().ToTableName()
    $$.val = append($1.tableNames(), name)
  }

table_name_list:
  db_object_name_list

sequence_name_list:
  db_object_name_list

view_name_list:
  db_object_name_list

// %Help: ANALYZE - collect table statistics
// %Category: Misc
// %Text:
// ANALYZE <tablename>
//
// %SeeAlso: CREATE STATISTICS
analyze_stmt:
  ANALYZE analyze_target
  {
    $$.val = &tree.Analyze{
      Table: $2.tblExpr(),
    }
  }
| ANALYZE error // SHOW HELP: ANALYZE
| ANALYSE analyze_target
  {
    $$.val = &tree.Analyze{
      Table: $2.tblExpr(),
    }
  }
| ANALYSE error // SHOW HELP: ANALYZE

analyze_target:
  table_name
  {
    $$.val = $1.unresolvedObjectName()
  }

// %Help: EXPLAIN - show the logical plan of a query
// %Category: Misc
// %Text:
// EXPLAIN <statement>
// EXPLAIN ([PLAN ,] <planoptions...> ) <statement>
// EXPLAIN (DISTSQL) <statement>
// EXPLAIN ANALYZE [(DISTSQL)] <statement>
// EXPLAIN ANALYZE (PLAN <planoptions...>) <statement>
//
// Explainable statements:
//     SELECT, CREATE, DROP, ALTER, INSERT, UPSERT, UPDATE, DELETE,
//     SHOW, EXPLAIN
//
// Plan options:
//     TYPES, VERBOSE, OPT
//
// %SeeAlso: WEBDOCS/explain.html
explain_stmt:
  EXPLAIN explainable_stmt
  {
    var err error
    $$.val, err = tree.MakeExplain(nil /* options */, $2.stmt())
    if err != nil {
      return setErr(sqllex, err)
    }
  }
| EXPLAIN error // SHOW HELP: EXPLAIN
| EXPLAIN '(' explain_option_list ')' explainable_stmt
  {
    var err error
    $$.val, err = tree.MakeExplain($3.strs(), $5.stmt())
    if err != nil {
      return setErr(sqllex, err)
    }
  }
| EXPLAIN ANALYZE explainable_stmt
  {
    var err error
    $$.val, err = tree.MakeExplain([]string{"ANALYZE"}, $3.stmt())
    if err != nil {
      return setErr(sqllex, err)
    }
  }
| EXPLAIN ANALYSE explainable_stmt
  {
    var err error
    $$.val, err = tree.MakeExplain([]string{"ANALYZE"}, $3.stmt())
    if err != nil {
      return setErr(sqllex, err)
    }
  }
| EXPLAIN ANALYZE '(' explain_option_list ')' explainable_stmt
  {
    var err error
    $$.val, err = tree.MakeExplain(append($4.strs(), "ANALYZE"), $6.stmt())
    if err != nil {
      return setErr(sqllex, err)
    }
  }
| EXPLAIN ANALYSE '(' explain_option_list ')' explainable_stmt
  {
    var err error
    $$.val, err = tree.MakeExplain(append($4.strs(), "ANALYZE"), $6.stmt())
    if err != nil {
      return setErr(sqllex, err)
    }
  }
// This second error rule is necessary, because otherwise
// preparable_stmt also provides "selectclause := '(' error ..." and
// cause a help text for the select clause, which will be confusing in
// the context of EXPLAIN.
| EXPLAIN '(' error // SHOW HELP: EXPLAIN

explainable_stmt:
  preparable_stmt
| comment_stmt
| execute_stmt
| call_stmt
| do_stmt

preparable_stmt:
  alter_stmt     // help texts in sub-rule
| backup_stmt    // EXTEND WITH HELP: BACKUP
| cancel_stmt    // help texts in sub-rule
| create_stmt    // help texts in sub-rule
| check_stmt	 // help texts in sub-rule
| delete_stmt    // EXTEND WITH HELP: DELETE
| drop_stmt      // help texts in sub-rule
| explain_stmt   // EXTEND WITH HELP: EXPLAIN
| import_stmt    // EXTEND WITH HELP: IMPORT
| insert_stmt    // EXTEND WITH HELP: INSERT
| pause_stmt     // help texts in sub-rule
| reset_stmt     // help texts in sub-rule
| restore_stmt   // EXTEND WITH HELP: RESTORE
| resume_stmt    // help texts in sub-rule
| export_stmt    // EXTEND WITH HELP: EXPORT
| scrub_stmt     // help texts in sub-rule
| select_stmt    // help texts in sub-rule
  {
    $$.val = $1.slct()
  }
| preparable_set_stmt // help texts in sub-rule
| show_stmt         // help texts in sub-rule
| truncate_stmt     // EXTEND WITH HELP: TRUNCATE
| update_stmt       // EXTEND WITH HELP: UPDATE
| upsert_stmt       // EXTEND WITH HELP: UPSERT

// These are statements that can be used as a data source using the special
// syntax with brackets. These are a subset of preparable_stmt.
row_source_extension_stmt:
  delete_stmt       // EXTEND WITH HELP: DELETE
| explain_stmt      // EXTEND WITH HELP: EXPLAIN
| insert_stmt       // EXTEND WITH HELP: INSERT
| select_stmt       // help texts in sub-rule
  {
    $$.val = $1.slct()
  }
| show_stmt         // help texts in sub-rule
| update_stmt       // EXTEND WITH HELP: UPDATE
| upsert_stmt       // EXTEND WITH HELP: UPSERT

copy_to_stmt:
  delete_stmt       // EXTEND WITH HELP: DELETE
| insert_stmt       // EXTEND WITH HELP: INSERT
| select_stmt       // help texts in sub-rule
  {
    $$.val = $1.slct()
  }
| update_stmt       // EXTEND WITH HELP: UPDATE
| upsert_stmt       // EXTEND WITH HELP: UPSERT

explain_option_list:
  explain_option_name
  {
    $$.val = []string{$1}
  }
| explain_option_list ',' explain_option_name
  {
    $$.val = append($1.strs(), $3)
  }

// %Help: ALTER JOB - alter an existing job
// %Category: Misc
// %Text:
// ALTER JOB <jobid> <cmd>
// %SeeAlso: SHOW JOBS, CANCEL JOBS, RESUME JOBS
alter_job_stmt:
  ALTER JOB a_expr OWNER TO role_spec
  {
    $$.val = &tree.AlterJobOwner{
      Job: $3.expr(),
      Owner: $6.roleSpec(),
    }
  }
| ALTER JOB error // SHOW HELP: ALTER JOB


// %Help: ALTER CHANGEFEED - alter an existing changefeed
// %Category: CCL
// %Text:
// ALTER CHANGEFEED <job_id> {{ADD|DROP <targets...>} | SET <options...>}...
alter_changefeed_stmt:
  ALTER CHANGEFEED a_expr alter_changefeed_cmds
  {
    $$.val = &tree.AlterChangefeed{
      Jobs: $3.expr(),
      Cmds: $4.alterChangefeedCmds(),
    }
  }
| ALTER CHANGEFEED error // SHOW HELP: ALTER CHANGEFEED

alter_changefeed_cmds:
  alter_changefeed_cmd
  {
    $$.val = tree.AlterChangefeedCmds{$1.alterChangefeedCmd()}
  }
| alter_changefeed_cmds alter_changefeed_cmd
  {
    $$.val = append($1.alterChangefeedCmds(), $2.alterChangefeedCmd())
  }

alter_changefeed_cmd:
  // ALTER CHANGEFEED <job_id> ADD [TABLE] ...
  ADD changefeed_targets opt_with_options
  {
    $$.val = &tree.AlterChangefeedAddTarget{
      Targets: $2.changefeedTargets(),
      Options: $3.kvOptions(),
    }
  }
  // ALTER CHANGEFEED <job_id> DROP [TABLE] ...
| DROP changefeed_targets
  {
    $$.val = &tree.AlterChangefeedDropTarget{
      Targets: $2.changefeedTargets(),
    }
  }
| SET kv_option_list
  {
    $$.val = &tree.AlterChangefeedSetOptions{
      Options: $2.kvOptions(),
    }
  }
| UNSET name_list
  {
    $$.val = &tree.AlterChangefeedUnsetOptions{
      Options: $2.nameList(),
    }
  }

// %Help: ALTER BACKUP - alter an existing backup's encryption keys
// %Category: CCL
// %Text:
// ALTER BACKUP <location...>
//        [ ADD NEW_KMS = <kms...> ]
//        [ WITH OLD_KMS = <kms...> ]
// Locations:
//    "[scheme]://[host]/[path to backup]?[parameters]"
//
// KMS:
//    "[kms_provider]://[kms_host]/[master_key_identifier]?[parameters]" : add new kms keys to backup
alter_backup_stmt:
  ALTER BACKUP string_or_placeholder alter_backup_cmds
  {
    $$.val = &tree.AlterBackup {
      Backup:	$3.expr(),
      Cmds:	$4.alterBackupCmds(),
    }
  }
| ALTER BACKUP string_or_placeholder IN string_or_placeholder alter_backup_cmds
	{
    $$.val = &tree.AlterBackup {
      Subdir:	$3.expr(),
      Backup:	$5.expr(),
      Cmds:	$6.alterBackupCmds(),
    }
	}
| ALTER BACKUP error // SHOW HELP: ALTER BACKUP

alter_backup_cmds:
	alter_backup_cmd
	{
    $$.val = tree.AlterBackupCmds{$1.alterBackupCmd()}
	}
|	alter_backup_cmds alter_backup_cmd
	{
    $$.val = append($1.alterBackupCmds(), $2.alterBackupCmd())
	}

alter_backup_cmd:
	ADD backup_kms
	{
    $$.val = &tree.AlterBackupKMS{
      KMSInfo:	$2.backupKMS(),
    }
	}

backup_kms:
	NEW_KMS '=' string_or_placeholder_opt_list WITH OLD_KMS '=' string_or_placeholder_opt_list
	{
    $$.val = tree.BackupKMS{
      NewKMSURI:	$3.stringOrPlaceholderOptList(),
      OldKMSURI:	$7.stringOrPlaceholderOptList(),
    }
	}

// %Help: SHOW VIRTUAL CLUSTER - display metadata about virtual clusters
// %Category: Experimental
// %Text:
// SHOW VIRTUAL CLUSTER { <virtual_cluster_spec> | ALL } [ WITH <options> ]
// SHOW VIRTUAL CLUSTERS                                 [ WITH <options> ]
//
// Options:
//     REPLICATION STATUS
//     CAPABILITIES
show_virtual_cluster_stmt:
  SHOW virtual_cluster_spec_opt_all opt_show_virtual_cluster_options
  {
    /* SKIP DOC */
    $$.val = &tree.ShowTenant{
      TenantSpec: $2.tenantSpec(),
      ShowTenantOptions: $3.showTenantOpts(),
    }
  }
| SHOW virtual_cluster error // SHOW HELP: SHOW VIRTUAL CLUSTER

virtual_cluster_spec_opt_all:
  TENANT_ALL ALL
  {
    /* SKIP DOC */
    $$.val = &tree.TenantSpec{All: true}
  }
| TENANTS
  {
    /* SKIP DOC */
    $$.val = &tree.TenantSpec{All: true}
  }
| TENANT virtual_cluster_spec
  {
    /* SKIP DOC */
    $$.val = $2.tenantSpec()
  }
| VIRTUAL CLUSTER_ALL ALL
  {
    $$.val = &tree.TenantSpec{All: true}
  }
| VIRTUAL CLUSTERS
  {
    $$.val = &tree.TenantSpec{All: true}
  }
| VIRTUAL CLUSTER virtual_cluster_spec
  {
    $$.val = $3.tenantSpec()
  }

opt_show_virtual_cluster_options:
  /* EMPTY */
  {
    /* SKIP DOC */
    $$.val = tree.ShowTenantOptions{}
  }
| WITH show_virtual_cluster_options
  {
    /* SKIP DOC */
    $$.val = $2.showTenantOpts()
  }

show_virtual_cluster_options:
  REPLICATION STATUS
  {
    /* SKIP DOC */
    $$.val = tree.ShowTenantOptions{WithReplication: true}
  }
| CAPABILITIES
  {
    /* SKIP DOC */
    $$.val = tree.ShowTenantOptions{WithCapabilities: true}
  }
| PRIOR REPLICATION DETAILS
  {
    /* SKIP DOC */
    $$.val = tree.ShowTenantOptions{WithPriorReplication: true}
  }
| show_virtual_cluster_options ',' REPLICATION STATUS
  {
    /* SKIP DOC */
    o := $1.showTenantOpts()
    o.WithReplication = true
    $$.val = o
  }
| show_virtual_cluster_options ',' CAPABILITIES
  {
    /* SKIP DOC */
    o := $1.showTenantOpts()
    o.WithCapabilities = true
    $$.val = o
  }
| show_virtual_cluster_options ',' PRIOR REPLICATION DETAILS
  {
    /* SKIP DOC */
    o := $1.showTenantOpts()
    o.WithPriorReplication = true
    $$.val = o
  }

// %Help: SHOW LOGICAL REPLICATION JOBS - display metadata about logical replication jobs
// %Category: Experimental
// %Text: SHOW LOGICAL REPLICATION JOBS [ WITH <options> ]
//
// Options:
//     DETAILS
show_logical_replication_jobs_stmt:
	SHOW LOGICAL REPLICATION JOBS opt_show_logical_replication_jobs_options
	{
		/* SKIP DOC */
		$$.val = &tree.ShowLogicalReplicationJobs{
			ShowLogicalReplicationJobsOptions: $5.showLogicalReplicationJobsOpts(),
		}
	}
| SHOW LOGICAL REPLICATION JOBS error // SHOW HELP: SHOW LOGICAL REPLICATION JOBS

opt_show_logical_replication_jobs_options:
  /* EMPTY */
  {
    /* SKIP DOC */
    $$.val = tree.ShowLogicalReplicationJobsOptions{}
  }
| WITH show_logical_replication_jobs_options
  {
    /* SKIP DOC */
    $$.val = $2.showLogicalReplicationJobsOpts()
  }

show_logical_replication_jobs_options:
  DETAILS
  {
    /* SKIP DOC */
    $$.val = tree.ShowLogicalReplicationJobsOptions{WithDetails: true}
  }

// %Help: PREPARE - prepare a statement for later execution
// %Category: Misc
// %Text: PREPARE <name> [ ( <types...> ) ] AS <query>
// %SeeAlso: EXECUTE, DEALLOCATE, DISCARD
prepare_stmt:
  PREPARE table_alias_name prep_type_clause AS preparable_stmt
  {
    $$.val = &tree.Prepare{
      Name: tree.Name($2),
      Types: $3.typeReferences(),
      Statement: $5.stmt(),
    }
  }
| PREPARE table_alias_name prep_type_clause AS OPT PLAN SCONST
  {
    /* SKIP DOC */
    $$.val = &tree.Prepare{
      Name: tree.Name($2),
      Types: $3.typeReferences(),
      Statement: &tree.CannedOptPlan{Plan: $7},
    }
  }
| PREPARE error // SHOW HELP: PREPARE

prep_type_clause:
  '(' type_list ')'
  {
    $$.val = $2.typeReferences();
  }
| /* EMPTY */
  {
    $$.val = []tree.ResolvableTypeReference(nil)
  }

// %Help: EXECUTE - execute a statement prepared previously
// %Category: Misc
// %Text: EXECUTE <name> [ ( <exprs...> ) ]
// %SeeAlso: PREPARE, DEALLOCATE, DISCARD
execute_stmt:
  EXECUTE table_alias_name execute_param_clause
  {
    $$.val = &tree.Execute{
      Name: tree.Name($2),
      Params: $3.exprs(),
    }
  }
| EXECUTE table_alias_name execute_param_clause DISCARD ROWS
  {
    /* SKIP DOC */
    $$.val = &tree.Execute{
      Name: tree.Name($2),
      Params: $3.exprs(),
      DiscardRows: true,
    }
  }
| EXECUTE error // SHOW HELP: EXECUTE

execute_param_clause:
  '(' expr_list ')'
  {
    $$.val = $2.exprs()
  }
| /* EMPTY */
  {
    $$.val = tree.Exprs(nil)
  }

// %Help: DEALLOCATE - remove a prepared statement
// %Category: Misc
// %Text: DEALLOCATE [PREPARE] { <name> | ALL }
// %SeeAlso: PREPARE, EXECUTE, DISCARD
deallocate_stmt:
  DEALLOCATE name
  {
    $$.val = &tree.Deallocate{Name: tree.Name($2)}
  }
| DEALLOCATE PREPARE name
  {
    $$.val = &tree.Deallocate{Name: tree.Name($3)}
  }
| DEALLOCATE ALL
  {
    $$.val = &tree.Deallocate{}
  }
| DEALLOCATE PREPARE ALL
  {
    $$.val = &tree.Deallocate{}
  }
| DEALLOCATE error // SHOW HELP: DEALLOCATE

// %Help: GRANT - define access privileges and role memberships
// %Category: Priv
// %Text:
// Grant privileges:
//   GRANT {ALL [PRIVILEGES] | <privileges...> } ON <targets...> TO <grantees...>
// Grant role membership:
//   GRANT <roles...> TO <grantees...> [WITH ADMIN OPTION]
//
// Privileges:
//   CREATE, DROP, GRANT, SELECT, INSERT, DELETE, UPDATE, USAGE, EXECUTE
//
// Targets:
//   DATABASE <databasename> [, ...]
//   [TABLE] [<databasename> .] { <tablename> | * } [, ...]
//   TYPE <typename> [, <typename>]...
//   FUNCTION <functionname> [ ( [ [ argmode ] [ argname ] argtype [, ...] ] ) ] [, ...]
//   SCHEMA [<databasename> .]<schemaname> [, [<databasename> .]<schemaname>]...
//   ALL TABLES IN SCHEMA schema_name [, ...]
//
// %SeeAlso: REVOKE, WEBDOCS/grant.html
grant_stmt:
  GRANT privileges ON grant_targets TO role_spec_list opt_with_grant_option
  {
    $$.val = &tree.Grant{Privileges: $2.privilegeList(), Grantees: $6.roleSpecList(), Targets: $4.grantTargetList(), WithGrantOption: $7.bool(),}
  }
| GRANT privilege_list TO role_spec_list
  {
    $$.val = &tree.GrantRole{Roles: $2.nameList(), Members: $4.roleSpecList(), AdminOption: false}
  }
| GRANT privilege_list TO role_spec_list WITH ADMIN OPTION
  {
    $$.val = &tree.GrantRole{Roles: $2.nameList(), Members: $4.roleSpecList(), AdminOption: true}
  }
| GRANT privileges ON TYPE target_types TO role_spec_list opt_with_grant_option
  {
    $$.val = &tree.Grant{Privileges: $2.privilegeList(), Targets: $5.grantTargetList(), Grantees: $7.roleSpecList(), WithGrantOption: $8.bool(),}
  }
| GRANT privileges ON SCHEMA schema_name_list TO role_spec_list opt_with_grant_option
  {
    $$.val = &tree.Grant{
      Privileges: $2.privilegeList(),
      Targets: tree.GrantTargetList{
        Schemas: $5.objectNamePrefixList(),
      },
      Grantees: $7.roleSpecList(),
      WithGrantOption: $8.bool(),
    }
  }
| GRANT privileges ON SCHEMA schema_name_list TO role_spec_list WITH error
  {
    return unimplemented(sqllex, "grant privileges on schema with")
  }
| GRANT privileges ON ALL SEQUENCES IN SCHEMA schema_name_list TO role_spec_list opt_with_grant_option
  {
    $$.val = &tree.Grant{
      Privileges: $2.privilegeList(),
      Targets: tree.GrantTargetList{
        Schemas: $8.objectNamePrefixList(),
        AllSequencesInSchema: true,
      },
      Grantees: $10.roleSpecList(),
      WithGrantOption: $11.bool(),
    }
  }
| GRANT privileges ON ALL TABLES IN SCHEMA schema_name_list TO role_spec_list opt_with_grant_option
  {
    $$.val = &tree.Grant{
      Privileges: $2.privilegeList(),
      Targets: tree.GrantTargetList{
        Schemas: $8.objectNamePrefixList(),
        AllTablesInSchema: true,
      },
      Grantees: $10.roleSpecList(),
      WithGrantOption: $11.bool(),
    }
  }
| GRANT privileges ON ALL FUNCTIONS IN SCHEMA schema_name_list TO role_spec_list opt_with_grant_option
  {
    $$.val = &tree.Grant{
      Privileges: $2.privilegeList(),
      Targets: tree.GrantTargetList{
        Schemas: $8.objectNamePrefixList(),
        AllFunctionsInSchema: true,
      },
      Grantees: $10.roleSpecList(),
      WithGrantOption: $11.bool(),
    }
  }
| GRANT privileges ON ALL PROCEDURES IN SCHEMA schema_name_list TO role_spec_list opt_with_grant_option
  {
    $$.val = &tree.Grant{
      Privileges: $2.privilegeList(),
      Targets: tree.GrantTargetList{
        Schemas: $8.objectNamePrefixList(),
        AllProceduresInSchema: true,
      },
      Grantees: $10.roleSpecList(),
      WithGrantOption: $11.bool(),
    }
  }
| GRANT SYSTEM privileges TO role_spec_list opt_with_grant_option
  {
    $$.val = &tree.Grant{
      Privileges: $3.privilegeList(),
      Targets: tree.GrantTargetList{
        System: true,
      },
      Grantees: $5.roleSpecList(),
      WithGrantOption: $6.bool(),
    }
  }
| GRANT error // SHOW HELP: GRANT

// %Help: REVOKE - remove access privileges and role memberships
// %Category: Priv
// %Text:
// Revoke privileges:
//   REVOKE {ALL | <privileges...> } ON <targets...> FROM <grantees...>
// Revoke role membership:
//   REVOKE [ADMIN OPTION FOR] <roles...> FROM <grantees...>
//
// Privileges:
//   CREATE, DROP, GRANT, SELECT, INSERT, DELETE, UPDATE, USAGE, EXECUTE
//
// Targets:
//   DATABASE <databasename> [, <databasename>]...
//   [TABLE] [<databasename> .] { <tablename> | * } [, ...]
//   TYPE <typename> [, <typename>]...
//   FUNCTION <functionname> [ ( [ [ argmode ] [ argname ] argtype [, ...] ] ) ] [, ...]
//   SCHEMA [<databasename> .]<schemaname> [, [<databasename> .]<schemaname]...
//   ALL TABLES IN SCHEMA schema_name [, ...]
//
// %SeeAlso: GRANT, WEBDOCS/revoke.html
revoke_stmt:
  REVOKE privileges ON grant_targets FROM role_spec_list
  {
    $$.val = &tree.Revoke{Privileges: $2.privilegeList(), Grantees: $6.roleSpecList(), Targets: $4.grantTargetList(), GrantOptionFor: false}
  }
| REVOKE GRANT OPTION FOR privileges ON grant_targets FROM role_spec_list
  {
    $$.val = &tree.Revoke{Privileges: $5.privilegeList(), Grantees: $9.roleSpecList(), Targets: $7.grantTargetList(), GrantOptionFor: true}
  }
| REVOKE privilege_list FROM role_spec_list
  {
    $$.val = &tree.RevokeRole{Roles: $2.nameList(), Members: $4.roleSpecList(), AdminOption: false }
  }
| REVOKE ADMIN OPTION FOR privilege_list FROM role_spec_list
  {
    $$.val = &tree.RevokeRole{Roles: $5.nameList(), Members: $7.roleSpecList(), AdminOption: true }
  }
| REVOKE privileges ON TYPE target_types FROM role_spec_list
  {
    $$.val = &tree.Revoke{Privileges: $2.privilegeList(), Targets: $5.grantTargetList(), Grantees: $7.roleSpecList(), GrantOptionFor: false}
  }
| REVOKE GRANT OPTION FOR privileges ON TYPE target_types FROM role_spec_list
  {
    $$.val = &tree.Revoke{Privileges: $5.privilegeList(), Targets: $8.grantTargetList(), Grantees: $10.roleSpecList(), GrantOptionFor: true}
  }
| REVOKE privileges ON SCHEMA schema_name_list FROM role_spec_list
  {
    $$.val = &tree.Revoke{
      Privileges: $2.privilegeList(),
      Targets: tree.GrantTargetList{
        Schemas: $5.objectNamePrefixList(),
      },
      Grantees: $7.roleSpecList(),
      GrantOptionFor: false,
    }
  }
| REVOKE GRANT OPTION FOR privileges ON SCHEMA schema_name_list FROM role_spec_list
  {
    $$.val = &tree.Revoke{
      Privileges: $5.privilegeList(),
      Targets: tree.GrantTargetList{
        Schemas: $8.objectNamePrefixList(),
      },
      Grantees: $10.roleSpecList(),
      GrantOptionFor: true,
    }
  }
| REVOKE privileges ON ALL TABLES IN SCHEMA schema_name_list FROM role_spec_list
  {
    $$.val = &tree.Revoke{
      Privileges: $2.privilegeList(),
      Targets: tree.GrantTargetList{
        Schemas: $8.objectNamePrefixList(),
        AllTablesInSchema: true,
      },
      Grantees: $10.roleSpecList(),
      GrantOptionFor: false,
    }
  }
| REVOKE privileges ON ALL SEQUENCES IN SCHEMA schema_name_list FROM role_spec_list
  {
    $$.val = &tree.Revoke{
      Privileges: $2.privilegeList(),
      Targets: tree.GrantTargetList{
        Schemas: $8.objectNamePrefixList(),
        AllSequencesInSchema: true,
      },
      Grantees: $10.roleSpecList(),
      GrantOptionFor: false,
    }
  }
| REVOKE GRANT OPTION FOR privileges ON ALL TABLES IN SCHEMA schema_name_list FROM role_spec_list
  {
    $$.val = &tree.Revoke{
      Privileges: $5.privilegeList(),
      Targets: tree.GrantTargetList{
        Schemas: $11.objectNamePrefixList(),
        AllTablesInSchema: true,
      },
      Grantees: $13.roleSpecList(),
      GrantOptionFor: true,
    }
  }
| REVOKE privileges ON ALL FUNCTIONS IN SCHEMA schema_name_list FROM role_spec_list
  {
    $$.val = &tree.Revoke{
      Privileges: $2.privilegeList(),
      Targets: tree.GrantTargetList{
        Schemas: $8.objectNamePrefixList(),
        AllFunctionsInSchema: true,
      },
      Grantees: $10.roleSpecList(),
      GrantOptionFor: false,
    }
  }
| REVOKE GRANT OPTION FOR privileges ON ALL FUNCTIONS IN SCHEMA schema_name_list FROM role_spec_list
  {
    $$.val = &tree.Revoke{
      Privileges: $5.privilegeList(),
      Targets: tree.GrantTargetList{
        Schemas: $11.objectNamePrefixList(),
        AllFunctionsInSchema: true,
      },
      Grantees: $13.roleSpecList(),
      GrantOptionFor: true,
    }
  }
| REVOKE privileges ON ALL PROCEDURES IN SCHEMA schema_name_list FROM role_spec_list
  {
    $$.val = &tree.Revoke{
      Privileges: $2.privilegeList(),
      Targets: tree.GrantTargetList{
        Schemas: $8.objectNamePrefixList(),
        AllProceduresInSchema: true,
      },
      Grantees: $10.roleSpecList(),
      GrantOptionFor: false,
    }
  }
| REVOKE GRANT OPTION FOR privileges ON ALL PROCEDURES IN SCHEMA schema_name_list FROM role_spec_list
  {
    $$.val = &tree.Revoke{
      Privileges: $5.privilegeList(),
      Targets: tree.GrantTargetList{
        Schemas: $11.objectNamePrefixList(),
        AllProceduresInSchema: true,
      },
      Grantees: $13.roleSpecList(),
      GrantOptionFor: true,
    }
  }
| REVOKE SYSTEM privileges FROM role_spec_list
  {
    $$.val = &tree.Revoke{
      Privileges: $3.privilegeList(),
      Targets: tree.GrantTargetList{
        System: true,
      },
      Grantees: $5.roleSpecList(),
    }
  }
| REVOKE GRANT OPTION FOR SYSTEM privileges FROM role_spec_list
  {
    $$.val = &tree.Revoke{
      Privileges: $6.privilegeList(),
      Targets: tree.GrantTargetList{
        System: true,
      },
      Grantees: $8.roleSpecList(),
      GrantOptionFor: true,
    }
  }
| REVOKE privileges ON SEQUENCE error
  {
    return unimplemented(sqllex, "revoke privileges on sequence")
  }
| REVOKE error // SHOW HELP: REVOKE


// ALL can either be by itself, or with the optional PRIVILEGES keyword (which no-ops)
privileges:
  ALL opt_privileges_clause
  {
    $$.val = privilege.List{privilege.ALL}
  }
| privilege_list
  {
     privList, err := privilege.ListFromStrings($1.nameList().ToStrings(), privilege.OriginFromUserInput)
     if err != nil {
       return setErr(sqllex, err)
     }
     $$.val = privList
  }

privilege_list:
  privilege
  {
    $$.val = tree.NameList{tree.Name($1)}
  }
| privilege_list ',' privilege
  {
    $$.val = append($1.nameList(), tree.Name($3))
  }

// Privileges are parsed at execution time to avoid having to make them reserved.
// Any privileges above `col_name_keyword` should be listed here.
// The full list is in sql/privilege/privilege.go.
privilege:
  name
| CREATE
| GRANT
| SELECT

reset_stmt:
  reset_session_stmt  // EXTEND WITH HELP: RESET
| reset_csetting_stmt // EXTEND WITH HELP: RESET CLUSTER SETTING

// %Help: RESET - reset a session variable to its default value
// %Category: Cfg
// %Text: RESET [SESSION] <var>
// %SeeAlso: RESET CLUSTER SETTING, WEBDOCS/set-vars.html
reset_session_stmt:
  RESET session_var
  {
    $$.val = &tree.SetVar{Name: $2, Values:tree.Exprs{tree.DefaultVal{}}, Reset: true}
  }
| RESET SESSION session_var
  {
    $$.val = &tree.SetVar{Name: $3, Values:tree.Exprs{tree.DefaultVal{}}, Reset: true}
  }
| RESET_ALL ALL
  {
    $$.val = &tree.SetVar{ResetAll: true, Reset: true}
  }
| RESET error // SHOW HELP: RESET

// %Help: RESET CLUSTER SETTING - reset a cluster setting to its default value
// %Category: Cfg
// %Text: RESET CLUSTER SETTING <var>
// %SeeAlso: SET CLUSTER SETTING, RESET
reset_csetting_stmt:
  RESET CLUSTER SETTING var_name
  {
    $$.val = &tree.SetClusterSetting{Name: strings.Join($4.strs(), "."), Value:tree.DefaultVal{}}
  }
| RESET CLUSTER error // SHOW HELP: RESET CLUSTER SETTING

// USE is the MSSQL/MySQL equivalent of SET DATABASE. Alias it for convenience.
// %Help: USE - set the current database
// %Category: Cfg
// %Text: USE <dbname>
//
// "USE <dbname>" is an alias for "SET [SESSION] database = <dbname>".
// %SeeAlso: SET SESSION, WEBDOCS/set-vars.html
use_stmt:
  USE var_value
  {
    $$.val = &tree.SetVar{Name: "database", Values: tree.Exprs{$2.expr()}}
  }
| USE error // SHOW HELP: USE

// SET remainder, e.g. SET TRANSACTION
nonpreparable_set_stmt:
  set_transaction_stmt // EXTEND WITH HELP: SET TRANSACTION
| set_exprs_internal   { /* SKIP DOC */ }
| SET CONSTRAINTS error { return unimplemented(sqllex, "set constraints") }

// SET SESSION / SET LOCAL / SET CLUSTER SETTING
preparable_set_stmt:
  set_session_stmt     // EXTEND WITH HELP: SET SESSION
| set_local_stmt       // EXTEND WITH HELP: SET LOCAL
| set_csetting_stmt    // EXTEND WITH HELP: SET CLUSTER SETTING
| use_stmt             // EXTEND WITH HELP: USE

// %Help: SCRUB - run checks against databases or tables
// %Category: Experimental
// %Text:
// EXPERIMENTAL SCRUB TABLE <table> ...
// EXPERIMENTAL SCRUB DATABASE <database>
//
// The various checks that ca be run with SCRUB includes:
//   - Physical table data (encoding)
//   - Secondary index integrity
//   - Constraint integrity (NOT NULL, CHECK, FOREIGN KEY, UNIQUE)
// %SeeAlso: SCRUB TABLE, SCRUB DATABASE
scrub_stmt:
  scrub_table_stmt
| scrub_database_stmt
| EXPERIMENTAL SCRUB error // SHOW HELP: SCRUB

// %Help: SCRUB DATABASE - run scrub checks on a database
// %Category: Experimental
// %Text:
// EXPERIMENTAL SCRUB DATABASE <database>
//                             [AS OF SYSTEM TIME <expr>]
//
// All scrub checks will be run on the database. This includes:
//   - Physical table data (encoding)
//   - Secondary index integrity
//   - Constraint integrity (NOT NULL, CHECK, FOREIGN KEY, UNIQUE)
// %SeeAlso: SCRUB TABLE, SCRUB
scrub_database_stmt:
  EXPERIMENTAL SCRUB DATABASE database_name opt_as_of_clause
  {
    $$.val = &tree.Scrub{Typ: tree.ScrubDatabase, Database: tree.Name($4), AsOf: $5.asOfClause()}
  }
| EXPERIMENTAL SCRUB DATABASE error // SHOW HELP: SCRUB DATABASE

// %Help: SCRUB TABLE - run scrub checks on a table
// %Category: Experimental
// %Text:
// SCRUB TABLE <tablename>
//             [AS OF SYSTEM TIME <expr>]
//             [WITH OPTIONS <option> [, ...]]
//
// Options:
//   EXPERIMENTAL SCRUB TABLE ... WITH OPTIONS INDEX ALL
//   EXPERIMENTAL SCRUB TABLE ... WITH OPTIONS INDEX (<index>...)
//   EXPERIMENTAL SCRUB TABLE ... WITH OPTIONS CONSTRAINT ALL
//   EXPERIMENTAL SCRUB TABLE ... WITH OPTIONS CONSTRAINT (<constraint>...)
//   EXPERIMENTAL SCRUB TABLE ... WITH OPTIONS PHYSICAL
// %SeeAlso: SCRUB DATABASE, SRUB
scrub_table_stmt:
  EXPERIMENTAL SCRUB TABLE table_name opt_as_of_clause opt_scrub_options_clause
  {
    $$.val = &tree.Scrub{
      Typ: tree.ScrubTable,
      Table: $4.unresolvedObjectName(),
      AsOf: $5.asOfClause(),
      Options: $6.scrubOptions(),
    }
  }
| EXPERIMENTAL SCRUB TABLE error // SHOW HELP: SCRUB TABLE

opt_scrub_options_clause:
  WITH OPTIONS scrub_option_list
  {
    $$.val = $3.scrubOptions()
  }
| /* EMPTY */
  {
    $$.val = tree.ScrubOptions{}
  }

scrub_option_list:
  scrub_option
  {
    $$.val = tree.ScrubOptions{$1.scrubOption()}
  }
| scrub_option_list ',' scrub_option
  {
    $$.val = append($1.scrubOptions(), $3.scrubOption())
  }

scrub_option:
  INDEX ALL
  {
    $$.val = &tree.ScrubOptionIndex{}
  }
| INDEX_BEFORE_PAREN '(' name_list ')'
  {
    $$.val = &tree.ScrubOptionIndex{IndexNames: $3.nameList()}
  }
| CONSTRAINT ALL
  {
    $$.val = &tree.ScrubOptionConstraint{}
  }
| CONSTRAINT '(' name_list ')'
  {
    $$.val = &tree.ScrubOptionConstraint{ConstraintNames: $3.nameList()}
  }
| PHYSICAL
  {
    $$.val = &tree.ScrubOptionPhysical{}
  }

// %Help: SET CLUSTER SETTING - change a cluster setting
// %Category: Cfg
// %Text: SET CLUSTER SETTING <var> { TO | = } <value>
// %SeeAlso: SHOW CLUSTER SETTING, RESET CLUSTER SETTING, SET SESSION, SET LOCAL
// WEBDOCS/cluster-settings.html
set_csetting_stmt:
  SET CLUSTER SETTING var_name to_or_eq var_value
  {
    $$.val = &tree.SetClusterSetting{Name: strings.Join($4.strs(), "."), Value: $6.expr()}
  }
| SET CLUSTER error // SHOW HELP: SET CLUSTER SETTING


// %Help: ALTER VIRTUAL CLUSTER - alter configuration of virtual clusters
// %Category: Group
// %Text:
// ALTER VIRTUAL CLUSTER REPLICATION, ALTER VIRTUAL CLUSTER SETTING,
// ALTER VIRTUAL CLUSTER CAPABILITY, ALTER VIRTUAL CLUSTER RENAME,
// ALTER VIRTUAL CLUSTER RESET, ALTER VIRTUAL CLUSTER SERVICE
alter_virtual_cluster_stmt:
  alter_virtual_cluster_replication_stmt // EXTEND WITH HELP: ALTER VIRTUAL CLUSTER REPLICATION
| alter_virtual_cluster_csetting_stmt    // EXTEND WITH HELP: ALTER VIRTUAL CLUSTER SETTING
| alter_virtual_cluster_capability_stmt  // EXTEND WITH HELP: ALTER VIRTUAL CLUSTER CAPABILITY
| alter_virtual_cluster_rename_stmt      // EXTEND WITH HELP: ALTER VIRTUAL CLUSTER RENAME
| alter_virtual_cluster_reset_stmt       // EXTEND WITH HELP: ALTER VIRTUAL CLUSTER RESET
| alter_virtual_cluster_service_stmt     // EXTEND WITH HELP: ALTER VIRTUAL CLUSTER SERVICE
| ALTER virtual_cluster error   // SHOW HELP: ALTER VIRTUAL CLUSTER

virtual_cluster_spec:
  d_expr
  { $$.val = &tree.TenantSpec{IsName: true, Expr: $1.expr()} }
| '[' a_expr ']'
  { $$.val = &tree.TenantSpec{IsName: false, Expr: $2.expr()} }


// %Help: ALTER VIRTUAL CLUSTER RESET - reset data state of a virtual cluster
// %Category: Experimental
// %Text:
// ALTER VIRTUAL CLUSTER <virtual_cluster_spec> RESET DATA TO SYSTEM TIME <time>
alter_virtual_cluster_reset_stmt:
  ALTER virtual_cluster virtual_cluster_spec  RESET DATA TO SYSTEM TIME a_expr
  {
    /* SKIP DOC */
    $$.val = &tree.AlterTenantReset{
      TenantSpec: $3.tenantSpec(),
      Timestamp: $9.expr(),
    }
  }

// %Help: ALTER VIRTUAL CLUSTER RENAME - rename a virtual cluster
// %Category: Experimental
// %Text:
// ALTER VIRTUAL CLUSTER <virtual_cluster_spec> RENAME TO <name>
alter_virtual_cluster_rename_stmt:
  ALTER virtual_cluster virtual_cluster_spec RENAME TO d_expr
  {
    /* SKIP DOC */
    $$.val = &tree.AlterTenantRename{
      TenantSpec: $3.tenantSpec(),
      NewName: &tree.TenantSpec{IsName: true, Expr: $6.expr()},
    }
  }

// %Help: ALTER VIRTUAL CLUSTER SERVICE - alter service mode of a virtual cluster
// %Category: Experimental
// %Text:
// ALTER VIRTUAL CLUSTER <virtual_cluster_spec> START SERVICE EXTERNAL
// ALTER VIRTUAL CLUSTER <virtual_cluster_spec> START SERVICE SHARED
// ALTER VIRTUAL CLUSTER <virtual_cluster_spec> STOP SERVICE
alter_virtual_cluster_service_stmt:
  ALTER virtual_cluster virtual_cluster_spec START SERVICE EXTERNAL
  {
    /* SKIP DOC */
    $$.val = &tree.AlterTenantService{
      TenantSpec: $3.tenantSpec(),
      Command: tree.TenantStartServiceExternal,
    }
  }
| ALTER virtual_cluster virtual_cluster_spec START SERVICE SHARED
  {
    /* SKIP DOC */
    $$.val = &tree.AlterTenantService{
      TenantSpec: $3.tenantSpec(),
      Command: tree.TenantStartServiceShared,
    }
  }
| ALTER virtual_cluster virtual_cluster_spec STOP SERVICE
  {
    /* SKIP DOC */
    $$.val = &tree.AlterTenantService{
      TenantSpec: $3.tenantSpec(),
      Command: tree.TenantStopService,
    }
  }
| ALTER virtual_cluster virtual_cluster_spec START error // SHOW HELP: ALTER VIRTUAL CLUSTER SERVICE
| ALTER virtual_cluster virtual_cluster_spec STOP error // SHOW HELP: ALTER VIRTUAL CLUSTER SERVICE


// %Help: ALTER VIRTUAL CLUSTER REPLICATION - alter replication stream between virtual clusters
// %Category: Experimental
// %Text:
// ALTER VIRTUAL CLUSTER <virtual_cluster_spec> PAUSE REPLICATION
// ALTER VIRTUAL CLUSTER <virtual_cluster_spec> RESUME REPLICATION
// ALTER VIRTUAL CLUSTER <virtual_cluster_spec> COMPLETE REPLICATION TO LATEST
// ALTER VIRTUAL CLUSTER <virtual_cluster_spec> COMPLETE REPLICATION TO SYSTEM TIME 'time'
// ALTER VIRTUAL CLUSTER <virtual_cluster_spec> SET REPLICATION opt=value,...
alter_virtual_cluster_replication_stmt:
  ALTER virtual_cluster virtual_cluster_spec PAUSE REPLICATION
  {
    /* SKIP DOC */
    $$.val = &tree.AlterTenantReplication{
      TenantSpec: $3.tenantSpec(),
      Command: tree.PauseJob,
    }
  }
| ALTER virtual_cluster virtual_cluster_spec RESUME REPLICATION
  {
    /* SKIP DOC */
    $$.val = &tree.AlterTenantReplication{
      TenantSpec: $3.tenantSpec(),
      Command: tree.ResumeJob,
    }
  }
| ALTER virtual_cluster virtual_cluster_spec COMPLETE REPLICATION TO SYSTEM TIME a_expr
  {
    /* SKIP DOC */
    $$.val = &tree.AlterTenantReplication{
      TenantSpec: $3.tenantSpec(),
      Cutover: &tree.ReplicationCutoverTime{
        Timestamp: $9.expr(),
      },
    }
  }
| ALTER virtual_cluster virtual_cluster_spec COMPLETE REPLICATION TO LATEST
  {
    /* SKIP DOC */
    $$.val = &tree.AlterTenantReplication{
      TenantSpec: $3.tenantSpec(),
      Cutover: &tree.ReplicationCutoverTime{
        Latest: true,
      },
    }
  }
| ALTER virtual_cluster virtual_cluster_spec SET REPLICATION replication_options_list
  {
    /* SKIP DOC */
    $$.val = &tree.AlterTenantReplication{
      TenantSpec: $3.tenantSpec(),
      Options: *$6.tenantReplicationOptions(),
    }
  }
| ALTER virtual_cluster virtual_cluster_spec START REPLICATION OF d_expr ON d_expr opt_with_replication_options
  {
    /* SKIP DOC */
    $$.val = &tree.AlterTenantReplication{
      TenantSpec: $3.tenantSpec(),
      ReplicationSourceTenantName: &tree.TenantSpec{IsName: true, Expr: $7.expr()},
      ReplicationSourceConnUri: $9.expr(),
      Options: *$10.tenantReplicationOptions(),
    }
  }


// %Help: ALTER VIRTUAL CLUSTER SETTING - alter cluster setting overrides for virtual clusters
// %Category: Group
// %Text:
// ALTER VIRTUAL CLUSTER { <virtual_cluster_spec> | ALL } SET CLUSTER SETTING <var> { TO | = } <value>
// ALTER VIRTUAL CLUSTER { <virtual_cluster_spec> | ALL } RESET CLUSTER SETTING <var>
// %SeeAlso: SET CLUSTER SETTING
alter_virtual_cluster_csetting_stmt:
  ALTER virtual_cluster virtual_cluster_spec set_or_reset_csetting_stmt
  {
    /* SKIP DOC */
    csettingStmt := $4.stmt().(*tree.SetClusterSetting)
    $$.val = &tree.AlterTenantSetClusterSetting{
      SetClusterSetting: *csettingStmt,
      TenantSpec: $3.tenantSpec(),
    }
  }
| ALTER TENANT_ALL ALL set_or_reset_csetting_stmt
  {
    /* SKIP DOC */
    csettingStmt := $4.stmt().(*tree.SetClusterSetting)
    $$.val = &tree.AlterTenantSetClusterSetting{
      SetClusterSetting: *csettingStmt,
      TenantSpec: &tree.TenantSpec{All: true},
    }
  }
| ALTER VIRTUAL CLUSTER_ALL ALL set_or_reset_csetting_stmt
  {
    /* SKIP DOC */
    csettingStmt := $5.stmt().(*tree.SetClusterSetting)
    $$.val = &tree.AlterTenantSetClusterSetting{
      SetClusterSetting: *csettingStmt,
      TenantSpec: &tree.TenantSpec{All: true},
    }
  }
| ALTER VIRTUAL CLUSTER_ALL ALL error // SHOW HELP: ALTER VIRTUAL CLUSTER SETTING
| ALTER TENANT_ALL ALL error // SHOW HELP: ALTER VIRTUAL CLUSTER SETTING

set_or_reset_csetting_stmt:
  reset_csetting_stmt
| set_csetting_stmt

to_or_eq:
  '='
| TO

// %Help: ALTER VIRTUAL CLUSTER CAPABILITY - alter system capability of virtual cluster
// %Category: Group
// %Text:
// ALTER VIRTUAL CLUSTER <tenant_id> GRANT CAPABILITY <var> { TO | = } <value>
// ALTER VIRTUAL CLUSTER <tenant_id> REVOKE CAPABILITY <var>
alter_virtual_cluster_capability_stmt:
  ALTER virtual_cluster virtual_cluster_spec GRANT CAPABILITY virtual_cluster_capability_list
  {
    /* SKIP DOC */
    $$.val = &tree.AlterTenantCapability{
      TenantSpec: $3.tenantSpec(),
      Capabilities: $6.tenantCapabilities(),
    }
  }
| ALTER virtual_cluster virtual_cluster_spec GRANT ALL CAPABILITIES
  {
    /* SKIP DOC */
    $$.val = &tree.AlterTenantCapability{
      TenantSpec: $3.tenantSpec(),
      AllCapabilities: true,
    }
  }
| ALTER virtual_cluster virtual_cluster_spec REVOKE CAPABILITY virtual_cluster_capability_list
  {
    /* SKIP DOC */
    $$.val = &tree.AlterTenantCapability{
      TenantSpec: $3.tenantSpec(),
      Capabilities: $6.tenantCapabilities(),
      IsRevoke: true,
    }
  }
| ALTER virtual_cluster virtual_cluster_spec REVOKE ALL CAPABILITIES
  {
    /* SKIP DOC */
    $$.val = &tree.AlterTenantCapability{
      TenantSpec: $3.tenantSpec(),
      AllCapabilities: true,
      IsRevoke: true,
    }
  }
| ALTER virtual_cluster virtual_cluster_spec GRANT error // SHOW HELP: ALTER VIRTUAL CLUSTER CAPABILITY
| ALTER virtual_cluster virtual_cluster_spec REVOKE error // SHOW HELP: ALTER VIRTUAL CLUSTER CAPABILITY

virtual_cluster_capability:
  var_name
  {
    /* SKIP DOC */
    $$.val = tree.TenantCapability{
      Name: strings.Join($1.strs(), "."),
    }
  }
| var_name to_or_eq var_value
  {
    /* SKIP DOC */
    $$.val = tree.TenantCapability{
      Name: strings.Join($1.strs(), "."),
      Value: $3.expr(),
    }
  }

virtual_cluster_capability_list:
  virtual_cluster_capability
  {
    /* SKIP DOC */
    $$.val = []tree.TenantCapability{$1.tenantCapability()}
  }
| virtual_cluster_capability_list ',' virtual_cluster_capability
  {
    /* SKIP DOC */
    $$.val = append($1.tenantCapabilities(), $3.tenantCapability())
  }

set_exprs_internal:
  /* SET ROW serves to accelerate parser.parseExprs().
     It cannot be used by clients. */
  SET ROW '(' expr_list ')'
  {
    $$.val = &tree.SetVar{Values: $4.exprs(), SetRow: true}
  }

// %Help: SET SESSION - change a session variable
// %Category: Cfg
// %Text:
// SET [SESSION] <var> { TO | = } <values...>
// SET [SESSION] TIME ZONE <tz>
// SET [SESSION] CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL { READ COMMITTED | REPEATABLE READ | SERIALIZABLE }
// SET [SESSION] TRACING { TO | = } { on | off | cluster | kv | results } [,...]
//
// %SeeAlso: SHOW SESSION, RESET, DISCARD, SHOW, SET CLUSTER SETTING, SET TRANSACTION, SET LOCAL
// WEBDOCS/set-vars.html
set_session_stmt:
  SET_TRACING TRACING to_or_eq var_list
	{
    /* SKIP DOC */
    // We need to recognize the "set tracing" specially here using syntax lookahead.
    $$.val = &tree.SetTracing{Values: $4.exprs()}
	}
| SET_TRACING SESSION TRACING to_or_eq var_list
	{
    /* SKIP DOC */
    // We need to recognize the "set tracing" specially here using syntax lookahead.
    $$.val = &tree.SetTracing{Values: $5.exprs()}
	}
| SET SESSION set_rest_more
  {
    $$.val = $3.stmt()
  }
| SET SESSION error  // SHOW HELP: SET SESSION
| SET set_rest_more
  {
    $$.val = $2.stmt()
  }
| SET error  // SHOW HELP: SET SESSION
// Special form for pg compatibility:
| SET SESSION CHARACTERISTICS AS TRANSACTION transaction_mode_list
  {
    $$.val = &tree.SetSessionCharacteristics{Modes: $6.transactionModes()}
  }

// %Help: SET LOCAL - change a session variable scoped to the current transaction
// %Category: Cfg
// %Text:
// SET LOCAL <var> { TO | = } <values...>
// SET LOCAL TIME ZONE <tz>
//
// %SeeAlso: SHOW SESSION, RESET, DISCARD, SHOW, SET CLUSTER SETTING, SET TRANSACTION, SET SESSION
// WEBDOCS/set-vars.html
set_local_stmt:
  SET LOCAL set_rest
  {
    ret := $3.setVar()
    ret.Local = true
    $$.val = ret
  }
| SET LOCAL error  // SHOW HELP: SET LOCAL

// %Help: SET TRANSACTION - configure the transaction settings
// %Category: Txn
// %Text:
// SET [SESSION] TRANSACTION <txnparameters...>
//
// Transaction parameters:
//    ISOLATION LEVEL { READ COMMITTED | REPEATABLE READ | SERIALIZABLE }
//    PRIORITY { LOW | NORMAL | HIGH }
//    AS OF SYSTEM TIME <expr>
//    [NOT] DEFERRABLE
//
// %SeeAlso: SHOW TRANSACTION, SET SESSION, SET LOCAL
// WEBDOCS/set-transaction.html
set_transaction_stmt:
  SET TRANSACTION transaction_mode_list
  {
    $$.val = &tree.SetTransaction{Modes: $3.transactionModes()}
  }
| SET TRANSACTION error // SHOW HELP: SET TRANSACTION
| SET SESSION TRANSACTION transaction_mode_list
  {
    $$.val = &tree.SetTransaction{Modes: $4.transactionModes()}
  }
| SET SESSION TRANSACTION error // SHOW HELP: SET TRANSACTION

generic_set:
  var_name to_or_eq var_list
  {
    $$.val = &tree.SetVar{Name: strings.Join($1.strs(), "."), Values: $3.exprs()}
  }

set_rest:
// Generic SET syntaxes:
   generic_set
// Special SET syntax forms in addition to the generic form.
// See: https://www.postgresql.org/docs/10/static/sql-set.html
//
// "SET TIME ZONE value is an alias for SET timezone TO value."
| TIME ZONE zone_value
  {
    /* SKIP DOC */
    $$.val = &tree.SetVar{Name: "timezone", Values: tree.Exprs{$3.expr()}}
  }
| var_name FROM CURRENT { return unimplemented(sqllex, "set from current") }
// "SET SCHEMA 'value' is an alias for SET search_path TO value. Only
// one schema can be specified using this syntax."
| SCHEMA var_value
  {
    /* SKIP DOC */
    $$.val = &tree.SetVar{Name: "search_path", Values: tree.Exprs{$2.expr()}}
  }
| ROLE var_value
	{
		/* SKIP DOC */
    $$.val = &tree.SetVar{Name: "role", Values: tree.Exprs{$2.expr()}}
	}

set_rest_more:
// SET syntaxes supported as a clause of other statements:
  set_rest
| SESSION AUTHORIZATION DEFAULT
  {
    /* SKIP DOC */
    $$.val = &tree.SetSessionAuthorizationDefault{}
  }
| SESSION AUTHORIZATION IDENT
  {
    return unimplementedWithIssue(sqllex, 40283)
  }
| SESSION AUTHORIZATION SCONST
  {
    return unimplementedWithIssue(sqllex, 40283)
  }
// See comment for the non-terminal for SET NAMES below.
| set_names

// SET NAMES is the SQL standard syntax for SET client_encoding.
// "SET NAMES value is an alias for SET client_encoding TO value."
// See https://www.postgresql.org/docs/10/static/sql-set.html
// Also see https://www.postgresql.org/docs/9.6/static/multibyte.html#AEN39236
set_names:
  NAMES var_value
  {
    /* SKIP DOC */
    $$.val = &tree.SetVar{Name: "client_encoding", Values: tree.Exprs{$2.expr()}}
  }
| NAMES
  {
    /* SKIP DOC */
    $$.val = &tree.SetVar{Name: "client_encoding", Values: tree.Exprs{tree.DefaultVal{}}}
  }

var_name:
  name
  {
    $$.val = []string{$1}
  }
| name attrs
  {
    $$.val = append([]string{$1}, $2.strs()...)
  }

attrs:
  '.' unrestricted_name
  {
    $$.val = []string{$2}
  }
| attrs '.' unrestricted_name
  {
    $$.val = append($1.strs(), $3)
  }

var_value:
  a_expr
| extra_var_value
  {
    $$.val = tree.Expr(&tree.UnresolvedName{NumParts: 1, Parts: tree.NameParts{$1}})
  }

// The RHS of a SET statement can contain any valid expression, which
// themselves can contain identifiers like TRUE, FALSE. These are parsed
// as column names (via a_expr) and later during semantic analysis
// assigned their special value.
//
// In addition, for compatibility with CockroachDB we need to support
// the reserved keyword ON (to go along OFF, which is a valid column name).
// Similarly, NONE is specially allowed here.
//
// Finally, in PostgreSQL the CockroachDB-reserved words "index",
// "nothing", etc. are not special and are valid in SET. These need to
// be allowed here too.
extra_var_value:
  ON
| NONE
| cockroachdb_extra_reserved_keyword

var_list:
  var_value
  {
    $$.val = tree.Exprs{$1.expr()}
  }
| var_list ',' var_value
  {
    $$.val = append($1.exprs(), $3.expr())
  }

iso_level:
  READ UNCOMMITTED
  {
    $$.val = tree.ReadUncommittedIsolation
  }
| READ COMMITTED
  {
    $$.val = tree.ReadCommittedIsolation
  }
| SNAPSHOT
  {
    $$.val = tree.SnapshotIsolation
  }
| REPEATABLE READ
  {
    $$.val = tree.RepeatableReadIsolation
  }
| SERIALIZABLE
  {
    $$.val = tree.SerializableIsolation
  }

user_priority:
  LOW
  {
    $$.val = tree.Low
  }
| NORMAL
  {
    $$.val = tree.Normal
  }
| HIGH
  {
    $$.val = tree.High
  }

// Timezone values can be:
// - a string such as 'pst8pdt'
// - an identifier such as "pst8pdt"
// - an integer or floating point number
// - a time interval per SQL99
zone_value:
  SCONST
  {
    $$.val = tree.NewStrVal($1)
  }
| IDENT
  {
    $$.val = tree.NewStrVal($1)
  }
| interval_value
  {
    $$.val = $1.expr()
  }
| numeric_only
| DEFAULT
  {
    $$.val = tree.DefaultVal{}
  }
| LOCAL
  {
    $$.val = tree.NewStrVal($1)
  }

// %Help: SHOW
// %Category: Group
// %Text:
// SHOW BACKUP, SHOW CLUSTER SETTING, SHOW COLUMNS, SHOW CONSTRAINTS, SHOW TRIGGERS,
// SHOW CREATE, SHOW CREATE SCHEDULES, SHOW DATABASES, SHOW DEFAULT SESSION VARIABLES,
// SHOW ENUMS, SHOW FUNCTION, SHOW FUNCTIONS, SHOW HISTOGRAM, SHOW INDEXES, SHOW PARTITIONS,
// SHOW JOBS, SHOW STATEMENTS, SHOW RANGE, SHOW RANGES, SHOW REGIONS, SHOW SURVIVAL GOAL,
// SHOW ROLES, SHOW SCHEMAS, SHOW SEQUENCES, SHOW SESSION, SHOW SESSIONS,
// SHOW STATISTICS, SHOW SYNTAX, SHOW TABLES, SHOW TRACE, SHOW TRANSACTION,
// SHOW TRANSACTIONS, SHOW TRANSFER, SHOW TYPES, SHOW USERS, SHOW LAST QUERY STATISTICS,
// SHOW SCHEDULES, SHOW LOCALITY, SHOW ZONE CONFIGURATION, SHOW COMMIT TIMESTAMP,
// SHOW FULL TABLE SCANS, SHOW CREATE EXTERNAL CONNECTIONS, SHOW EXTERNAL CONNECTIONS
show_stmt:
  show_backup_stmt           // EXTEND WITH HELP: SHOW BACKUP
| show_columns_stmt          // EXTEND WITH HELP: SHOW COLUMNS
| show_constraints_stmt      // EXTEND WITH HELP: SHOW CONSTRAINTS
| show_triggers_stmt         // EXTEND WITH HELP: SHOW TRIGGERS
| show_create_stmt           // EXTEND WITH HELP: SHOW CREATE
| show_create_schedules_stmt // EXTEND WITH HELP: SHOW CREATE SCHEDULES
| show_create_external_connections_stmt // EXTEND WITH HELP: SHOW CREATE EXTERNAL CONNECTIONS
| show_local_or_virtual_cluster_csettings_stmt // EXTEND WITH HELP: SHOW CLUSTER SETTING
| show_logical_replication_jobs_stmt	// EXTEND WITH HELP: SHOW LOGICAL REPLICATION JOBS
| show_databases_stmt        // EXTEND WITH HELP: SHOW DATABASES
| show_enums_stmt            // EXTEND WITH HELP: SHOW ENUMS
| show_external_connections_stmt // EXTEND WITH HELP: SHOW EXTERNAL CONNECTIONS
| show_types_stmt            // EXTEND WITH HELP: SHOW TYPES
| show_fingerprints_stmt
| show_functions_stmt        // EXTEND WITH HELP: SHOW FUNCTIONS
| show_procedures_stmt       // EXTEND WITH HELP: SHOW PROCEDURES
| show_grants_stmt           // EXTEND WITH HELP: SHOW GRANTS
| show_histogram_stmt        // EXTEND WITH HELP: SHOW HISTOGRAM
| show_indexes_stmt          // EXTEND WITH HELP: SHOW INDEXES
| show_partitions_stmt       // EXTEND WITH HELP: SHOW PARTITIONS
| show_jobs_stmt             // EXTEND WITH HELP: SHOW JOBS
| show_locality_stmt
| show_schedules_stmt        // EXTEND WITH HELP: SHOW SCHEDULES
| show_statements_stmt       // EXTEND WITH HELP: SHOW STATEMENTS
| show_ranges_stmt           // EXTEND WITH HELP: SHOW RANGES
| show_range_for_row_stmt
| show_regions_stmt          // EXTEND WITH HELP: SHOW REGIONS
| show_survival_goal_stmt    // EXTEND_WITH_HELP: SHOW SURVIVAL GOAL
| show_roles_stmt            // EXTEND WITH HELP: SHOW ROLES
| show_savepoint_stmt        // EXTEND WITH HELP: SHOW SAVEPOINT
| show_schemas_stmt          // EXTEND WITH HELP: SHOW SCHEMAS
| show_sequences_stmt        // EXTEND WITH HELP: SHOW SEQUENCES
| show_session_stmt          // EXTEND WITH HELP: SHOW SESSION
| show_sessions_stmt         // EXTEND WITH HELP: SHOW SESSIONS
| show_stats_stmt            // EXTEND WITH HELP: SHOW STATISTICS
| show_syntax_stmt           // EXTEND WITH HELP: SHOW SYNTAX
| show_tables_stmt           // EXTEND WITH HELP: SHOW TABLES
| show_virtual_cluster_stmt  // EXTEND WITH HELP: SHOW VIRTUAL CLUSTER
| show_trace_stmt            // EXTEND WITH HELP: SHOW TRACE
| show_transaction_stmt      // EXTEND WITH HELP: SHOW TRANSACTION
| show_transactions_stmt     // EXTEND WITH HELP: SHOW TRANSACTIONS
| show_transfer_stmt         // EXTEND WITH HELP: SHOW TRANSFER
| show_users_stmt            // EXTEND WITH HELP: SHOW USERS
| show_default_session_variables_for_role_stmt // EXTEND WITH HELP: SHOW DEFAULT SESSION VARIABLES FOR ROLE
| show_zone_stmt             // EXTEND WITH HELP: SHOW ZONE CONFIGURATION
| show_policies_stmt         // EXTEND WITH HELP: SHOW POLICIES
| SHOW error                 // SHOW HELP: SHOW
| show_last_query_stats_stmt
| show_full_scans_stmt
| show_default_privileges_stmt // EXTEND WITH HELP: SHOW DEFAULT PRIVILEGES
| show_completions_stmt

// %Help: CLOSE - close SQL cursor
// %Category: Misc
// %Text: CLOSE [ ALL | <name> ]
// %SeeAlso: DECLARE, FETCH
close_cursor_stmt:
	CLOSE ALL
	{
	  $$.val = &tree.CloseCursor{
	    All: true,
	  }
	}
| CLOSE cursor_name
  {
	  $$.val = &tree.CloseCursor{
	    Name: tree.Name($2),
	  }
	}
| CLOSE error // SHOW HELP: CLOSE

// %Help: DECLARE - declare SQL cursor
// %Category: Misc
// %Text: DECLARE <name> [ options ] CURSOR p [ WITH | WITHOUT HOLD ] FOR <query>
// %SeeAlso: CLOSE, FETCH
declare_cursor_stmt:
  // TODO(jordan): the options here should be supported in any order, not just
  // the fixed one here.
	DECLARE cursor_name opt_binary opt_sensitivity opt_scroll CURSOR opt_hold FOR select_stmt
	{
	  $$.val = &tree.DeclareCursor{
	    Binary: $3.bool(),
	    Name: tree.Name($2),
	    Sensitivity: $4.cursorSensitivity(),
	    Scroll: $5.cursorScrollOption(),
	    Hold: $7.bool(),
	    Select: $9.slct(),
	  }
  }
| DECLARE error // SHOW HELP: DECLARE

opt_binary:
  BINARY
  {
    $$.val = true
  }
| /* EMPTY */
  {
    $$.val = false
  }

opt_sensitivity:
  INSENSITIVE
  {
    $$.val = tree.Insensitive
  }
| ASENSITIVE
  {
    $$.val = tree.Asensitive
  }
| /* EMPTY */
  {
    $$.val = tree.UnspecifiedSensitivity
  }

opt_scroll:
  SCROLL
  {
    $$.val = tree.Scroll
  }
| NO SCROLL
  {
    $$.val = tree.NoScroll
  }
| /* EMPTY */
  {
    $$.val = tree.UnspecifiedScroll
  }

opt_hold:
  WITH HOLD
  {
    $$.val = true
  }
| WITHOUT HOLD
  {
    $$.val = false
  }
| /* EMPTY */
  {
    $$.val = false
  }

// %Help: FETCH - fetch rows from a SQL cursor
// %Category: Misc
// %Text: FETCH [ direction [ FROM | IN ] ] <name>
// %SeeAlso: MOVE, CLOSE, DECLARE
fetch_cursor_stmt:
  FETCH cursor_movement_specifier
  {
    $$.val = &tree.FetchCursor{
      CursorStmt: $2.cursorStmt(),
    }
  }
| FETCH error // SHOW HELP: FETCH

// %Help: MOVE - move a SQL cursor without fetching rows
// %Category: Misc
// %Text: MOVE [ direction [ FROM | IN ] ] <name>
// %SeeAlso: FETCH, CLOSE, DECLARE
move_cursor_stmt:
  MOVE cursor_movement_specifier
  {
    $$.val = &tree.MoveCursor{
      CursorStmt: $2.cursorStmt(),
    }
  }
| MOVE error // SHOW HELP: MOVE

cursor_movement_specifier:
  cursor_name
  {
    $$.val = tree.CursorStmt{
      Name: tree.Name($1),
      Count: 1,
    }
  }
| from_or_in cursor_name
  {
    $$.val = tree.CursorStmt{
      Name: tree.Name($2),
      Count: 1,
    }
  }
| next_prior opt_from_or_in cursor_name
  {
    $$.val = tree.CursorStmt{
      Name: tree.Name($3),
      Count: $1.int64(),
    }
  }
| forward_backward opt_from_or_in cursor_name
  {
    $$.val = tree.CursorStmt{
      Name: tree.Name($3),
      Count: $1.int64(),
    }
  }
| opt_forward_backward signed_iconst64 opt_from_or_in cursor_name
  {
    $$.val = tree.CursorStmt{
      Name: tree.Name($4),
      Count: $2.int64() * $1.int64(),
    }
  }
| opt_forward_backward ALL opt_from_or_in cursor_name
  {
    fetchType := tree.FetchAll
    count := $1.int64()
    if count < 0 {
      fetchType = tree.FetchBackwardAll
    }
    $$.val = tree.CursorStmt{
      Name: tree.Name($4),
      FetchType: fetchType,
    }
  }
| ABSOLUTE signed_iconst64 opt_from_or_in cursor_name
  {
    $$.val = tree.CursorStmt{
      Name: tree.Name($4),
      FetchType: tree.FetchAbsolute,
      Count: $2.int64(),
    }
  }
| RELATIVE signed_iconst64 opt_from_or_in cursor_name
  {
    $$.val = tree.CursorStmt{
      Name: tree.Name($4),
      FetchType: tree.FetchRelative,
      Count: $2.int64(),
    }
  }
| FIRST opt_from_or_in cursor_name
  {
    $$.val = tree.CursorStmt{
      Name: tree.Name($3),
      FetchType: tree.FetchFirst,
    }
  }
| LAST opt_from_or_in cursor_name
  {
    $$.val = tree.CursorStmt{
      Name: tree.Name($3),
      FetchType: tree.FetchLast,
    }
  }

next_prior:
  NEXT  { $$.val = int64(1) }
| PRIOR { $$.val = int64(-1) }

opt_forward_backward:
  forward_backward { $$.val = $1.int64() }
| /* EMPTY */ { $$.val = int64(1) }

forward_backward:
  FORWARD  { $$.val = int64(1) }
| BACKWARD { $$.val = int64(-1) }

opt_from_or_in:
  from_or_in { }
| /* EMPTY */ { }

from_or_in:
  FROM { }
| IN { }

reindex_stmt:
  REINDEX TABLE error
  {
    /* SKIP DOC */
    return purposelyUnimplemented(sqllex, "reindex table", "CockroachDB does not require reindexing.")
  }
| REINDEX INDEX error
  {
    /* SKIP DOC */
    return purposelyUnimplemented(sqllex, "reindex index", "CockroachDB does not require reindexing.")
  }
| REINDEX SCHEMA error
  {
    /* SKIP DOC */
    return purposelyUnimplemented(sqllex, "reindex schema", "CockroachDB does not require reindexing.")
  }
| REINDEX DATABASE error
  {
    /* SKIP DOC */
    return purposelyUnimplemented(sqllex, "reindex database", "CockroachDB does not require reindexing.")
  }
| REINDEX SYSTEM error
  {
    /* SKIP DOC */
    return purposelyUnimplemented(sqllex, "reindex system", "CockroachDB does not require reindexing.")
  }

// %Help: SHOW SESSION - display session variables
// %Category: Cfg
// %Text: SHOW [SESSION] { <var> | ALL }
// %SeeAlso: WEBDOCS/show-vars.html
show_session_stmt:
  SHOW session_var         { $$.val = &tree.ShowVar{Name: $2} }
| SHOW SESSION session_var { $$.val = &tree.ShowVar{Name: $3} }
| SHOW SESSION error // SHOW HELP: SHOW SESSION

session_var:
  IDENT
| IDENT session_var_parts
  {
    $$ = $1 + "." + strings.Join($2.strs(), ".")
  }
// Although ALL, SESSION_USER, DATABASE, LC_COLLATE, LC_CTYPE, and TRACING are
// identifiers for the purpose of SHOW, they lex as separate token types, so
// they need separate rules.
| ALL
| DATABASE
// SET NAMES is standard SQL for SET client_encoding.
// See https://www.postgresql.org/docs/9.6/static/multibyte.html#AEN39236
| NAMES { $$ = "client_encoding" }
| ROLE
| SESSION_USER
| LC_COLLATE
| LC_CTYPE
| TRACING { /* SKIP DOC */ }
| TRACING session_var_parts
  {
    /* SKIP DOC */
    $$ = $1 + "." + strings.Join($2.strs(), ".")
  }
// TIME ZONE is special: it is two tokens, but is really the identifier "TIME ZONE".
| TIME ZONE { $$ = "timezone" }
| TIME error // SHOW HELP: SHOW SESSION
| VIRTUAL_CLUSTER_NAME

session_var_parts:
  '.' IDENT
  {
    $$.val = []string{$2}
  }
| session_var_parts '.' IDENT
  {
    $$.val = append($1.strs(), $3)
  }

// %Help: SHOW STATISTICS - display table statistics
// %Category: Misc
// %Text: SHOW STATISTICS [USING JSON] FOR TABLE <table_name> [WITH FORECAST]
//
// Returns the available statistics for a table. The statistics can include a
// histogram ID, which can be used with SHOW HISTOGRAM.
//
// If USING JSON is specified, the statistics and histograms are encoded in JSON
// format.
//
// If WITH FORECAST is specified, forecasted statistics are included if
// available.
//
// %SeeAlso: SHOW HISTOGRAM
show_stats_stmt:
  SHOW STATISTICS FOR TABLE table_name opt_with_options
  {
      $$.val = &tree.ShowTableStats{
        Table:   $5.unresolvedObjectName(),
        Options: $6.kvOptions(),
      }
  }
| SHOW STATISTICS USING JSON FOR TABLE table_name opt_with_options
  {
    /* SKIP DOC */
    $$.val = &tree.ShowTableStats{
      Table:     $7.unresolvedObjectName(),
      UsingJSON: true,
      Options:   $8.kvOptions(),
    }
  }
| SHOW STATISTICS error // SHOW HELP: SHOW STATISTICS



// %Help: SHOW HISTOGRAM - display histogram (experimental)
// %Category: Experimental
// %Text: SHOW HISTOGRAM <histogram_id>
//
// Returns the data in the histogram with the
// given ID (as returned by SHOW STATISTICS).
// %SeeAlso: SHOW STATISTICS
show_histogram_stmt:
  SHOW HISTOGRAM ICONST
  {
    /* SKIP DOC */
    id, err := $3.numVal().AsInt64()
    if err != nil {
      return setErr(sqllex, err)
    }
    $$.val = &tree.ShowHistogram{HistogramID: id}
  }
| SHOW HISTOGRAM error // SHOW HELP: SHOW HISTOGRAM

// %Help: SHOW BACKUP - list backup contents
// %Category: CCL
// %Text: SHOW BACKUP [SCHEMAS|FILES|RANGES] <location>
// %SeeAlso: WEBDOCS/show-backup.html
show_backup_stmt:
  SHOW BACKUPS IN string_or_placeholder_opt_list
 {
    $$.val = &tree.ShowBackup{
      InCollection:    $4.stringOrPlaceholderOptList(),
    }
  }
| SHOW BACKUP show_backup_details FROM string_or_placeholder IN string_or_placeholder_opt_list opt_with_show_backup_options
	{
		$$.val = &tree.ShowBackup{
			From:    true,
			Details:    $3.showBackupDetails(),
			Path:    $5.expr(),
			InCollection: $7.stringOrPlaceholderOptList(),
			Options: *$8.showBackupOptions(),
		}
	}
| SHOW BACKUP string_or_placeholder IN string_or_placeholder_opt_list opt_with_show_backup_options
	{
		$$.val = &tree.ShowBackup{
			Details:  tree.BackupDefaultDetails,
			Path:    $3.expr(),
			InCollection: $5.stringOrPlaceholderOptList(),
			Options: *$6.showBackupOptions(),
		}
	}
| SHOW BACKUP string_or_placeholder opt_with_show_backup_options error
	{
    setErr(sqllex, errors.New("The `SHOW BACKUP` syntax without the `IN` keyword is no longer supported. Please use `SHOW BACKUP FROM <subdirectory> IN <collectionURI>`."))
    return helpWith(sqllex, "SHOW BACKUP")
	}
| SHOW BACKUP SCHEMAS string_or_placeholder opt_with_show_backup_options error
	{
    setErr(sqllex, errors.New("The `SHOW BACKUP SCHEMAS` syntax without the `IN` keyword is no longer supported. Please use `SHOW BACKUP SCHEMAS FROM <subdirectory> IN <collectionURI>`."))
    return helpWith(sqllex, "SHOW BACKUP")
	}
| SHOW BACKUP FILES string_or_placeholder opt_with_show_backup_options error
	{
    /* SKIP DOC */
    setErr(sqllex, errors.New("The `SHOW BACKUP FILES` syntax without the `IN` keyword is no longer supported. Please use `SHOW BACKUP FILES FROM <subdirectory> IN <collectionURI>`."))
    return helpWith(sqllex, "SHOW BACKUP")
	}
| SHOW BACKUP RANGES string_or_placeholder opt_with_show_backup_options error
	{
    /* SKIP DOC */
    setErr(sqllex, errors.New("The `SHOW BACKUP RANGES` syntax without the `IN` keyword is no longer supported. Please use `SHOW BACKUP RANGES FROM <subdirectory> IN <collectionURI>`."))
    return helpWith(sqllex, "SHOW BACKUP")
	}
| SHOW BACKUP VALIDATE string_or_placeholder opt_with_show_backup_options error
	{
		/* SKIP DOC */
		setErr(sqllex, errors.New("The `SHOW BACKUP VALIDATE` syntax without the `IN` keyword is no longer supported. Please use `SHOW BACKUP VALIDATE FROM <subdirectory> IN <collectionURI>`."))
    return helpWith(sqllex, "SHOW BACKUP")
	}
| SHOW BACKUP CONNECTION string_or_placeholder error
	{
		/* SKIP DOC */
		setErr(sqllex, errors.New("The `SHOW BACKUP CONNECTION` syntax is no longer supported. Please use `CHECK EXTERNAL CONNECTION`."))
		return helpWith(sqllex, "CHECK EXTERNAL CONNECTION")
	}
| SHOW BACKUP error // SHOW HELP: SHOW BACKUP

show_backup_details:
  /* EMPTY -- default */
  {
    $$.val = tree.BackupDefaultDetails
  }
| SCHEMAS
  {
    $$.val = tree.BackupSchemaDetails
  }
| FILES
	{
    /* SKIP DOC */
	$$.val = tree.BackupFileDetails
	}
| RANGES
	{
    /* SKIP DOC */
	$$.val = tree.BackupRangeDetails
	}
| VALIDATE
	{
    /* SKIP DOC */
	$$.val = tree.BackupValidateDetails
	}

opt_with_show_backup_options:
  WITH show_backup_options_list
  {
    $$.val = $2.showBackupOptions()
  }
| WITH OPTIONS '(' show_backup_options_list ')'
  {
    $$.val = $4.showBackupOptions()
  }
| /* EMPTY */
  {
    $$.val = &tree.ShowBackupOptions{}
  }

show_backup_options_list:
  // Require at least one option
  show_backup_options
  {
    $$.val = $1.showBackupOptions()
  }
| show_backup_options_list ',' show_backup_options
  {
    if err := $1.showBackupOptions().CombineWith($3.showBackupOptions()); err != nil {
      return setErr(sqllex, err)
    }
  }

show_backup_options:
 AS_JSON
 {
 $$.val = &tree.ShowBackupOptions{AsJson: true}
 }
 | CHECK_FILES
 {
 $$.val = &tree.ShowBackupOptions{CheckFiles: true}
 }
 | SKIP SIZE
 {
 $$.val = &tree.ShowBackupOptions{SkipSize: true}
 }
 | NOWAIT
 {
 /* SKIP DOC */
 $$.val = &tree.ShowBackupOptions{SkipSize: true}
 }
 | DEBUG_IDS
 {
 $$.val = &tree.ShowBackupOptions{DebugIDs: true}
 }
 | INCREMENTAL_LOCATION '=' string_or_placeholder_opt_list
 {
 $$.val = &tree.ShowBackupOptions{IncrementalStorage: $3.stringOrPlaceholderOptList()}
 }
 | KMS '=' string_or_placeholder_opt_list
 {
 $$.val = &tree.ShowBackupOptions{DecryptionKMSURI: $3.stringOrPlaceholderOptList()}
 }
 | ENCRYPTION_PASSPHRASE '=' string_or_placeholder
 {
 $$.val = &tree.ShowBackupOptions{EncryptionPassphrase: $3.expr()}
 }
 | PRIVILEGES
 {
 $$.val = &tree.ShowBackupOptions{Privileges: true}
 }
 | ENCRYPTION_INFO_DIR '=' string_or_placeholder
 {
 $$.val = &tree.ShowBackupOptions{EncryptionInfoDir: $3.expr()}
 }

// %Help: SHOW CLUSTER SETTING - display cluster settings
// %Category: Cfg
// %Text:
// SHOW CLUSTER SETTING <var> [ FOR VIRTUAL CLUSTER <virtual_cluster_spec> ]
// SHOW [ PUBLIC | ALL ] CLUSTER SETTINGS [ FOR VIRTUAL CLUSTER <virtual_cluster_spec> ]
// %SeeAlso: WEBDOCS/cluster-settings.html
show_csettings_stmt:
  SHOW CLUSTER SETTING var_name
  {
    $$.val = &tree.ShowClusterSetting{Name: strings.Join($4.strs(), ".")}
  }
| SHOW CLUSTER SETTING ALL
  {
    $$.val = &tree.ShowClusterSettingList{All: true}
  }
| SHOW CLUSTER error // SHOW HELP: SHOW CLUSTER SETTING
| SHOW ALL CLUSTER SETTINGS
  {
    $$.val = &tree.ShowClusterSettingList{All: true}
  }
| SHOW ALL CLUSTER error // SHOW HELP: SHOW CLUSTER SETTING
| SHOW CLUSTER SETTINGS
  {
    $$.val = &tree.ShowClusterSettingList{}
  }
| SHOW PUBLIC CLUSTER SETTINGS
  {
    $$.val = &tree.ShowClusterSettingList{}
  }
| SHOW PUBLIC CLUSTER error // SHOW HELP: SHOW CLUSTER SETTING

show_local_or_virtual_cluster_csettings_stmt:
  show_csettings_stmt
  {
    /* SKIP DOC */
    $$.val = $1.stmt()
  }
| show_csettings_stmt FOR virtual_cluster virtual_cluster_spec
  {
    /* SKIP DOC */
    switch t := $1.stmt().(type) {
    case *tree.ShowClusterSetting:
       $$.val = &tree.ShowTenantClusterSetting{
          ShowClusterSetting: t,
          TenantSpec: $4.tenantSpec(),
       }
    case *tree.ShowClusterSettingList:
       $$.val = &tree.ShowTenantClusterSettingList{
          ShowClusterSettingList: t,
          TenantSpec: $4.tenantSpec(),
       }
    }
  }
| show_csettings_stmt FOR virtual_cluster error // SHOW HELP: SHOW CLUSTER SETTING

// %Help: SHOW COLUMNS - list columns in relation
// %Category: DDL
// %Text: SHOW COLUMNS FROM <tablename>
// %SeeAlso: WEBDOCS/show-columns.html
show_columns_stmt:
  SHOW COLUMNS FROM table_name with_comment
  {
    $$.val = &tree.ShowColumns{Table: $4.unresolvedObjectName(), WithComment: $5.bool()}
  }
| SHOW COLUMNS error // SHOW HELP: SHOW COLUMNS

// %Help: SHOW PARTITIONS - list partition information
// %Category: DDL
// %Text: SHOW PARTITIONS FROM { TABLE <table> | INDEX <index> | DATABASE <database> }
// %SeeAlso: WEBDOCS/show-partitions.html
show_partitions_stmt:
  SHOW PARTITIONS FROM TABLE table_name
  {
    $$.val = &tree.ShowPartitions{IsTable: true, Table: $5.unresolvedObjectName()}
  }
| SHOW PARTITIONS FROM DATABASE database_name
  {
    $$.val = &tree.ShowPartitions{IsDB: true, Database: tree.Name($5)}
  }
| SHOW PARTITIONS FROM INDEX table_index_name
  {
    $$.val = &tree.ShowPartitions{IsIndex: true, Index: $5.tableIndexName()}
  }
| SHOW PARTITIONS FROM INDEX table_name '@' '*'
  {
    $$.val = &tree.ShowPartitions{IsTable: true, Table: $5.unresolvedObjectName()}
  }
| SHOW PARTITIONS error // SHOW HELP: SHOW PARTITIONS

// %Help: SHOW DATABASES - list databases
// %Category: DDL
// %Text: SHOW DATABASES
// %SeeAlso: WEBDOCS/show-databases.html
show_databases_stmt:
  SHOW DATABASES with_comment
  {
    $$.val = &tree.ShowDatabases{WithComment: $3.bool()}
  }
| SHOW DATABASES error // SHOW HELP: SHOW DATABASES

// %Help: SHOW DEFAULT PRIVILEGES - list default privileges
// %Category: DDL
// %Text: SHOW DEFAULT PRIVILEGES
// %SeeAlso: WEBDOCS/show-default-privileges
show_default_privileges_stmt:
  SHOW DEFAULT PRIVILEGES opt_for_roles opt_in_schema {
    $$.val = &tree.ShowDefaultPrivileges{
      Roles: $4.roleSpecList(),
      Schema: tree.Name($5),
    }
  }
| SHOW DEFAULT PRIVILEGES FOR GRANTEE role_spec_list opt_in_schema {
    $$.val = &tree.ShowDefaultPrivileges{
      Roles: $6.roleSpecList(),
      ForGrantee: true,
      Schema: tree.Name($7),
    }
  }
| SHOW DEFAULT PRIVILEGES FOR ALL ROLES opt_in_schema {
    $$.val = &tree.ShowDefaultPrivileges{
      ForAllRoles: true,
      Schema: tree.Name($7),
    }
  }
| SHOW DEFAULT PRIVILEGES error // SHOW HELP: SHOW DEFAULT PRIVILEGES

// %Help: SHOW ENUMS - list enums
// %Category: Misc
// %Text: SHOW ENUMS
show_enums_stmt:
  SHOW ENUMS
  {
    $$.val = &tree.ShowEnums{}
  }
| SHOW ENUMS FROM name '.' name
  {
    $$.val = &tree.ShowEnums{ObjectNamePrefix:tree.ObjectNamePrefix{
        CatalogName: tree.Name($4),
        ExplicitCatalog: true,
        SchemaName: tree.Name($6),
        ExplicitSchema: true,
      },
    }
  }
| SHOW ENUMS FROM name
{
    $$.val = &tree.ShowEnums{ObjectNamePrefix:tree.ObjectNamePrefix{
        // Note: the schema name may be interpreted as database name,
        // see name_resolution.go.
        SchemaName: tree.Name($4),
        ExplicitSchema: true,
      },
    }
}
| SHOW ENUMS error // SHOW HELP: SHOW ENUMS

// %Help: SHOW EXTERNAL CONNECTIONS - list external connections
// %Category: Misc
// %Text:
// SHOW EXTERNAL CONNECTIONS
// SHOW EXTERNAL CONNECTION <connection_name>
show_external_connections_stmt:
  SHOW EXTERNAL CONNECTIONS
  {
    $$.val = &tree.ShowExternalConnections{}
  }
| SHOW EXTERNAL CONNECTIONS error // SHOW HELP: SHOW EXTERNAL CONNECTIONS
| SHOW EXTERNAL CONNECTION string_or_placeholder
 {
   $$.val = &tree.ShowExternalConnections{ConnectionLabel: $4.expr()}
 }
| SHOW EXTERNAL CONNECTION error // SHOW HELP: SHOW EXTERNAL CONNECTIONS

// %Help: SHOW TYPES - list user defined types
// %Category: Misc
// %Text: SHOW TYPES [WITH_COMMENT]
show_types_stmt:
  SHOW TYPES with_comment
  {
    $$.val = &tree.ShowTypes{WithComment: $3.bool()}
  }
| SHOW TYPES error // SHOW HELP: SHOW TYPES

// %Help: SHOW GRANTS - list grants
// %Category: Priv
// %Text:
// Show privilege grants:
//   SHOW GRANTS [ON <targets...>] [FOR <users...>]
// Show role grants:
//   SHOW GRANTS ON ROLE [<roles...>] [FOR <grantees...>]
//
// %SeeAlso: WEBDOCS/show-grants.html
show_grants_stmt:
  SHOW GRANTS opt_on_targets_roles for_grantee_clause
  {
    lst := $3.grantTargetListPtr()
    if lst != nil && lst.ForRoles {
      $$.val = &tree.ShowRoleGrants{Roles: lst.Roles, Grantees: $4.roleSpecList()}
    } else {
      $$.val = &tree.ShowGrants{Targets: lst, Grantees: $4.roleSpecList()}
    }
  }
| SHOW SYSTEM GRANTS for_grantee_clause
  {
    $$.val = &tree.ShowGrants{
      Targets: &tree.GrantTargetList{System: true},
        Grantees: $4.roleSpecList(),
      }
  }
| SHOW GRANTS error // SHOW HELP: SHOW GRANTS

// %Help: SHOW INDEXES - list indexes
// %Category: DDL
// %Text: SHOW INDEXES FROM { <tablename> | DATABASE <database_name> } [WITH COMMENT]
// %SeeAlso: WEBDOCS/show-index.html
show_indexes_stmt:
  SHOW INDEX FROM table_name with_comment
  {
    $$.val = &tree.ShowIndexes{Table: $4.unresolvedObjectName(), WithComment: $5.bool()}
  }
| SHOW INDEX error // SHOW HELP: SHOW INDEXES
| SHOW INDEX FROM DATABASE database_name with_comment
  {
    $$.val = &tree.ShowDatabaseIndexes{Database: tree.Name($5), WithComment: $6.bool()}
  }
| SHOW INDEXES FROM table_name with_comment
  {
    $$.val = &tree.ShowIndexes{Table: $4.unresolvedObjectName(), WithComment: $5.bool()}
  }
| SHOW INDEXES FROM DATABASE database_name with_comment
  {
    $$.val = &tree.ShowDatabaseIndexes{Database: tree.Name($5), WithComment: $6.bool()}
  }
| SHOW INDEXES error // SHOW HELP: SHOW INDEXES
| SHOW KEYS FROM table_name with_comment
  {
    $$.val = &tree.ShowIndexes{Table: $4.unresolvedObjectName(), WithComment: $5.bool()}
  }
| SHOW KEYS FROM DATABASE database_name with_comment
  {
    $$.val = &tree.ShowDatabaseIndexes{Database: tree.Name($5), WithComment: $6.bool()}
  }
| SHOW KEYS error // SHOW HELP: SHOW INDEXES

// %Help: SHOW COMMIT TIMESTAMP - show timestamp commit timestamp of last transaction
// %Category: Misc
// %Text: SHOW COMMIT TIMESTAMP
//
// Shows the commit timestamp of the last committed transaction if not currently
// in a transaction. If currently in a transaction, implicitly commits the
// transaction, returning any errors which may have occurred during the commit.
// The transaction state will remain open from the perspective of the client,
// meaning that a COMMIT must be issued to move the connection back to a state
// where new statements may be issued.
show_commit_timestamp_stmt:
  SHOW COMMIT TIMESTAMP
  {
    $$.val = &tree.ShowCommitTimestamp{}
  }

// %Help: SHOW CONSTRAINTS - list constraints
// %Category: DDL
// %Text: SHOW CONSTRAINTS FROM <tablename>
// %SeeAlso: WEBDOCS/show-constraints.html
show_constraints_stmt:
  SHOW CONSTRAINT FROM table_name with_comment
  {
    $$.val = &tree.ShowConstraints{Table: $4.unresolvedObjectName(), WithComment: $5.bool()}
  }
| SHOW CONSTRAINT error // SHOW HELP: SHOW CONSTRAINTS
| SHOW CONSTRAINTS FROM table_name with_comment
  {
    $$.val = &tree.ShowConstraints{Table: $4.unresolvedObjectName(), WithComment: $5.bool()}
  }
| SHOW CONSTRAINTS error // SHOW HELP: SHOW CONSTRAINTS

// %Help: SHOW TRIGGERS - list triggers on a table
// %Category: DDL
// %Text: SHOW TRIGGERS FROM <tablename>
// TODO(drewk): Add docs link.
show_triggers_stmt:
  SHOW TRIGGERS FROM table_name
  {
    $$.val = &tree.ShowTriggers{Table: $4.unresolvedObjectName()}
  }
| SHOW TRIGGER error // SHOW HELP: SHOW TRIGGERS
| SHOW TRIGGERS error // SHOW HELP: SHOW TRIGGERS

// %Help: SHOW STATEMENTS - list running statements
// %Category: Misc
// %Text: SHOW [ALL] [CLUSTER | LOCAL] STATEMENTS
// %SeeAlso: CANCEL QUERIES
show_statements_stmt:
  SHOW opt_cluster statements_or_queries
  {
    $$.val = &tree.ShowQueries{All: false, Cluster: $2.bool()}
  }
| SHOW opt_cluster statements_or_queries error // SHOW HELP: SHOW STATEMENTS
| SHOW ALL opt_cluster statements_or_queries
  {
    $$.val = &tree.ShowQueries{All: true, Cluster: $3.bool()}
  }
| SHOW ALL opt_cluster statements_or_queries error // SHOW HELP: SHOW STATEMENTS

opt_cluster:
  /* EMPTY */
  { $$.val = true }
| CLUSTER
  { $$.val = true }
| LOCAL
  { $$.val = false }

// SHOW QUERIES is now an alias for SHOW STATEMENTS
// https://github.com/cockroachdb/cockroach/issues/56240
statements_or_queries:
  STATEMENTS
| QUERIES

// %Help: SHOW JOBS - list background jobs
// %Category: Misc
// %Text:
// SHOW [AUTOMATIC | CHANGEFEED] JOBS [select clause] [WITH EXECUTION DETAILS]
// SHOW JOBS FOR SCHEDULES [select clause]
// SHOW [CHANGEFEED] JOB <jobid> [WITH EXECUTION DETAILS]
// %SeeAlso: CANCEL JOBS, PAUSE JOBS, RESUME JOBS
show_jobs_stmt:
  SHOW AUTOMATIC JOBS
  {
    $$.val = &tree.ShowJobs{Automatic: true}
  }
| SHOW JOBS
  {
    $$.val = &tree.ShowJobs{
      Automatic: false,
    }
  }
| SHOW JOBS WITH show_job_options_list
  {
    $$.val = &tree.ShowJobs{
      Automatic: false,
      Options: $4.showJobOptions(),
    }
  }
| SHOW CHANGEFEED JOBS
  {
    $$.val = &tree.ShowChangefeedJobs{}
  }
| SHOW AUTOMATIC JOBS error // SHOW HELP: SHOW JOBS
| SHOW JOBS error // SHOW HELP: SHOW JOBS
| SHOW CHANGEFEED JOBS error // SHOW HELP: SHOW JOBS
| SHOW JOBS select_stmt
  {
    $$.val = &tree.ShowJobs{Jobs: $3.slct()}
  }
| SHOW JOBS select_stmt WITH show_job_options_list
  {
    $$.val = &tree.ShowJobs{
      Jobs: $3.slct(),
      Options: $5.showJobOptions(),
    }
  }
| SHOW JOBS WHEN COMPLETE select_stmt
  {
    $$.val = &tree.ShowJobs{Jobs: $5.slct(), Block: true}
  }
| SHOW JOBS for_schedules_clause
  {
    $$.val = &tree.ShowJobs{Schedules: $3.slct()}
  }
| SHOW CHANGEFEED JOBS select_stmt
  {
    $$.val = &tree.ShowChangefeedJobs{Jobs: $4.slct()}
  }
| SHOW JOBS select_stmt error // SHOW HELP: SHOW JOBS
| SHOW JOB a_expr
  {
    $$.val = &tree.ShowJobs{
      Jobs: &tree.Select{
        Select: &tree.ValuesClause{Rows: []tree.Exprs{tree.Exprs{$3.expr()}}},
      },
    }
  }
| SHOW JOB a_expr WITH show_job_options_list
  {
    $$.val = &tree.ShowJobs{
      Jobs: &tree.Select{
        Select: &tree.ValuesClause{Rows: []tree.Exprs{tree.Exprs{$3.expr()}}},
      },
      Options: $5.showJobOptions(),
    }
  }
| SHOW CHANGEFEED JOB a_expr
  {
    $$.val = &tree.ShowChangefeedJobs{
      Jobs: &tree.Select{
        Select: &tree.ValuesClause{Rows: []tree.Exprs{tree.Exprs{$4.expr()}}},
      },
    }
  }
| SHOW JOB WHEN COMPLETE a_expr
  {
    $$.val = &tree.ShowJobs{
      Jobs: &tree.Select{
        Select: &tree.ValuesClause{Rows: []tree.Exprs{tree.Exprs{$5.expr()}}},
      },
      Block: true,
    }
  }
| SHOW JOB error // SHOW HELP: SHOW JOBS
| SHOW CHANGEFEED JOB error // SHOW HELP: SHOW JOBS


show_job_options_list:
  // Require at least one option
  show_job_options
  {
    $$.val = $1.showJobOptions()
  }
| show_job_options_list ',' show_job_options
  {
    if err := $1.showJobOptions().CombineWith($3.showJobOptions()); err != nil {
      return setErr(sqllex, err)
    }
  }

// List of valid SHOW JOB options.
show_job_options:
  EXECUTION DETAILS
  {
    $$.val = &tree.ShowJobOptions{
      ExecutionDetails: true,
    }
  }

// %Help: SHOW SCHEDULES - list periodic schedules
// %Category: Misc
// %Text:
// SHOW [RUNNING | PAUSED] SCHEDULES [FOR BACKUP]
// SHOW SCHEDULE <schedule_id>
// %SeeAlso: PAUSE SCHEDULES, RESUME SCHEDULES, DROP SCHEDULES
show_schedules_stmt:
  SHOW SCHEDULES opt_schedule_executor_type
  {
    $$.val = &tree.ShowSchedules{
      WhichSchedules: tree.SpecifiedSchedules,
      ExecutorType: $3.executorType(),
    }
  }
| SHOW SCHEDULES opt_schedule_executor_type error // SHOW HELP: SHOW SCHEDULES
| SHOW schedule_state SCHEDULES opt_schedule_executor_type
  {
    $$.val = &tree.ShowSchedules{
      WhichSchedules: $2.scheduleState(),
      ExecutorType: $4.executorType(),
    }
  }
| SHOW schedule_state SCHEDULES opt_schedule_executor_type error // SHOW HELP: SHOW SCHEDULES
| SHOW SCHEDULE a_expr
  {
    $$.val = &tree.ShowSchedules{
      WhichSchedules: tree.SpecifiedSchedules,
      ScheduleID:  $3.expr(),
    }
  }
| SHOW SCHEDULE error  // SHOW HELP: SHOW SCHEDULES

schedule_state:
  RUNNING
  {
    $$.val = tree.ActiveSchedules
  }
| PAUSED
  {
    $$.val = tree.PausedSchedules
  }

opt_schedule_executor_type:
  /* Empty */
  {
    $$.val = tree.InvalidExecutor
  }
| FOR BACKUP
  {
    $$.val = tree.ScheduledBackupExecutor
  }
| FOR SQL STATISTICS
  {
    $$.val = tree.ScheduledSQLStatsCompactionExecutor
  }
| FOR CHANGEFEED
	{
		$$.val = tree.ScheduledChangefeedExecutor
	}

// %Help: SHOW TRACE - display an execution trace
// %Category: Misc
// %Text:
// SHOW [COMPACT] [KV] TRACE FOR SESSION
// %SeeAlso: EXPLAIN
show_trace_stmt:
  SHOW opt_compact TRACE FOR SESSION
  {
    $$.val = &tree.ShowTraceForSession{TraceType: tree.ShowTraceRaw, Compact: $2.bool()}
  }
| SHOW opt_compact TRACE error // SHOW HELP: SHOW TRACE
| SHOW opt_compact KV TRACE FOR SESSION
  {
    $$.val = &tree.ShowTraceForSession{TraceType: tree.ShowTraceKV, Compact: $2.bool()}
  }
| SHOW opt_compact KV error // SHOW HELP: SHOW TRACE
| SHOW opt_compact EXPERIMENTAL_REPLICA TRACE FOR SESSION
  {
    /* SKIP DOC */
    $$.val = &tree.ShowTraceForSession{TraceType: tree.ShowTraceReplica, Compact: $2.bool()}
  }
| SHOW opt_compact EXPERIMENTAL_REPLICA error // SHOW HELP: SHOW TRACE

opt_compact:
  COMPACT { $$.val = true }
| /* EMPTY */ { $$.val = false }

// %Help: SHOW SESSIONS - list open client sessions
// %Category: Misc
// %Text: SHOW [ALL] [CLUSTER | LOCAL] SESSIONS
// %SeeAlso: CANCEL SESSIONS
show_sessions_stmt:
  SHOW opt_cluster SESSIONS
  {
    $$.val = &tree.ShowSessions{Cluster: $2.bool()}
  }
| SHOW opt_cluster SESSIONS error // SHOW HELP: SHOW SESSIONS
| SHOW ALL opt_cluster SESSIONS
  {
    $$.val = &tree.ShowSessions{All: true, Cluster: $3.bool()}
  }
| SHOW ALL opt_cluster SESSIONS error // SHOW HELP: SHOW SESSIONS

// %Help: SHOW TABLES - list tables
// %Category: DDL
// %Text: SHOW TABLES [FROM <databasename> [ . <schemaname> ] ] [WITH COMMENT]
// %SeeAlso: WEBDOCS/show-tables.html
show_tables_stmt:
  SHOW TABLES FROM name '.' name with_comment
  {
    $$.val = &tree.ShowTables{ObjectNamePrefix:tree.ObjectNamePrefix{
        CatalogName: tree.Name($4),
        ExplicitCatalog: true,
        SchemaName: tree.Name($6),
        ExplicitSchema: true,
    },
    WithComment: $7.bool()}
  }
| SHOW TABLES FROM name with_comment
  {
    $$.val = &tree.ShowTables{ObjectNamePrefix:tree.ObjectNamePrefix{
        // Note: the schema name may be interpreted as database name,
        // see name_resolution.go.
        SchemaName: tree.Name($4),
        ExplicitSchema: true,
    },
    WithComment: $5.bool()}
  }
| SHOW TABLES with_comment
  {
    $$.val = &tree.ShowTables{WithComment: $3.bool()}
  }
| SHOW TABLES error // SHOW HELP: SHOW TABLES

// %Help: SHOW FUNCTIONS - list functions
// %Category: DDL
// %Text: SHOW FUNCTIONS [FROM <databasename> [ . <schemaname> ] ]
show_functions_stmt:
  SHOW FUNCTIONS FROM name '.' name
  {
    $$.val = &tree.ShowRoutines{ObjectNamePrefix:tree.ObjectNamePrefix{
        CatalogName: tree.Name($4),
        ExplicitCatalog: true,
        SchemaName: tree.Name($6),
        ExplicitSchema: true,
    }}
  }
| SHOW FUNCTIONS FROM name
  {
    $$.val = &tree.ShowRoutines{ObjectNamePrefix:tree.ObjectNamePrefix{
        // Note: the schema name may be interpreted as database name,
        // see name_resolution.go.
        SchemaName: tree.Name($4),
        ExplicitSchema: true,
    }}
  }
| SHOW FUNCTIONS
  {
    $$.val = &tree.ShowRoutines{}
  }
| SHOW FUNCTIONS error // SHOW HELP: SHOW FUNCTIONS

// %Help: SHOW PROCEDURES - list procedures
// %Category: DDL
// %Text: SHOW PROCEDURES [FROM <databasename> [ . <schemaname> ] ]
show_procedures_stmt:
  SHOW PROCEDURES FROM name '.' name
  {
    $$.val = &tree.ShowRoutines{ObjectNamePrefix:tree.ObjectNamePrefix{
        CatalogName: tree.Name($4),
        ExplicitCatalog: true,
        SchemaName: tree.Name($6),
        ExplicitSchema: true,
    }, Procedure: true}
  }
| SHOW PROCEDURES FROM name
  {
    $$.val = &tree.ShowRoutines{ObjectNamePrefix:tree.ObjectNamePrefix{
        // Note: the schema name may be interpreted as database name,
        // see name_resolution.go.
        SchemaName: tree.Name($4),
        ExplicitSchema: true,
    }, Procedure: true}
  }
| SHOW PROCEDURES
  {
    $$.val = &tree.ShowRoutines{Procedure: true}
  }
| SHOW PROCEDURES error // SHOW HELP: SHOW PROCEDURES

// %Help: SHOW TRANSACTIONS - list open client transactions across the cluster
// %Category: Misc
// %Text: SHOW [ALL] [CLUSTER | LOCAL] TRANSACTIONS
show_transactions_stmt:
  SHOW opt_cluster TRANSACTIONS
  {
    $$.val = &tree.ShowTransactions{Cluster: $2.bool()}
  }
| SHOW opt_cluster TRANSACTIONS error // SHOW HELP: SHOW TRANSACTIONS
| SHOW ALL opt_cluster TRANSACTIONS
  {
    $$.val = &tree.ShowTransactions{All: true, Cluster: $3.bool()}
  }
| SHOW ALL opt_cluster TRANSACTIONS error // SHOW HELP: SHOW TRANSACTIONS

with_comment:
  WITH COMMENT { $$.val = true }
| /* EMPTY */  { $$.val = false }

// %Help: SHOW SCHEMAS - list schemas
// %Category: DDL
// %Text: SHOW SCHEMAS [FROM <databasename> ] [WITH COMMENT]
show_schemas_stmt:
  SHOW SCHEMAS FROM name with_comment
  {
    $$.val = &tree.ShowSchemas{Database: tree.Name($4), WithComment: $5.bool()}
  }
| SHOW SCHEMAS with_comment
  {
    $$.val = &tree.ShowSchemas{WithComment: $3.bool()}
  }
| SHOW SCHEMAS error // SHOW HELP: SHOW SCHEMAS

// %Help: SHOW SEQUENCES - list sequences
// %Category: DDL
// %Text: SHOW SEQUENCES [FROM <databasename> ]
show_sequences_stmt:
  SHOW SEQUENCES FROM name
  {
    $$.val = &tree.ShowSequences{Database: tree.Name($4)}
  }
| SHOW SEQUENCES
  {
    $$.val = &tree.ShowSequences{}
  }
| SHOW SEQUENCES error // SHOW HELP: SHOW SEQUENCES

// %Help: SHOW SYNTAX - analyze SQL syntax
// %Category: Misc
// %Text: SHOW SYNTAX <string>
show_syntax_stmt:
  SHOW SYNTAX SCONST
  {
    /* SKIP DOC */
    $$.val = &tree.ShowSyntax{Statement: $3}
  }
| SHOW SYNTAX error // SHOW HELP: SHOW SYNTAX

show_completions_stmt:
  SHOW COMPLETIONS AT OFFSET ICONST FOR SCONST
  {
    /* SKIP DOC */
    $$.val = &tree.ShowCompletions{
        Statement: tree.NewStrVal($7),
        Offset: $5.numVal(),
    }
  }

// %Help: SHOW POLICIES - list row-level security policies defined for a table
// %Category: DDL
// %Text: SHOW POLICIES FOR <table_name>
show_policies_stmt:
  SHOW POLICIES FOR table_name
  {
    /* SKIP DOC */
    $$.val = &tree.ShowPolicies{
      Table: $4.unresolvedObjectName(),
    }
  }
| SHOW POLICIES error // SHOW HELP: SHOW POLICIES

show_last_query_stats_stmt:
  SHOW LAST QUERY STATISTICS query_stats_cols
  {
   /* SKIP DOC */
   $$.val = &tree.ShowLastQueryStatistics{Columns: $5.nameList()}
  }

query_stats_cols:
  RETURNING name_list
  {
    $$.val = $2.nameList()
  }
| /* EMPTY */
  {
    // Note: the form that does not specify the RETURNING clause is deprecated.
    // Remove it when there are no more clients using it (22.1 or later).
    $$.val = tree.ShowLastQueryStatisticsDefaultColumns
  }

// %Help: SHOW SAVEPOINT - display current savepoint properties
// %Category: Cfg
// %Text: SHOW SAVEPOINT STATUS
show_savepoint_stmt:
  SHOW SAVEPOINT STATUS
  {
    $$.val = &tree.ShowSavepointStatus{}
  }
| SHOW SAVEPOINT error // SHOW HELP: SHOW SAVEPOINT

// %Help: SHOW TRANSACTION - display current transaction properties
// %Category: Cfg
// %Text: SHOW TRANSACTION {ISOLATION LEVEL | PRIORITY | STATUS}
// %SeeAlso: WEBDOCS/show-vars.html
show_transaction_stmt:
  SHOW TRANSACTION ISOLATION LEVEL
  {
    /* SKIP DOC */
    $$.val = &tree.ShowVar{Name: "transaction_isolation"}
  }
| SHOW TRANSACTION PRIORITY
  {
    /* SKIP DOC */
    $$.val = &tree.ShowVar{Name: "transaction_priority"}
  }
| SHOW TRANSACTION STATUS
  {
    /* SKIP DOC */
    $$.val = &tree.ShowTransactionStatus{}
  }
| SHOW TRANSACTION error // SHOW HELP: SHOW TRANSACTION

// %Help: SHOW TRANSFER - display session state for connection migration
// %Category: Misc
// %Text: SHOW TRANSFER STATE [ WITH '<transfer_key>' ]
show_transfer_stmt:
  SHOW TRANSFER STATE WITH SCONST
  {
     $$.val = &tree.ShowTransferState{TransferKey: tree.NewStrVal($5)}
  }
| SHOW TRANSFER STATE
  {
     $$.val = &tree.ShowTransferState{}
  }
| SHOW TRANSFER error // SHOW HELP: SHOW TRANSFER

// %Help: SHOW CREATE - display the CREATE statement for a table, sequence, view, or database
// %Category: DDL
// %Text:
// SHOW CREATE [ TABLE | SEQUENCE | VIEW | DATABASE ] <object_name>
// SHOW CREATE [ SECONDARY ] INDEXES FROM <table_name>
// SHOW CREATE ALL SCHEMAS
// SHOW CREATE ALL TABLES
// SHOW CREATE ALL TYPES
// %SeeAlso: WEBDOCS/show-create.html
show_create_stmt:
  SHOW CREATE table_name opt_show_create_format_options
  {
    $$.val = &tree.ShowCreate{
      Name: $3.unresolvedObjectName(), FmtOpt: $4.showCreateFormatOption(),
    }
  }
| SHOW CREATE TABLE table_name opt_show_create_format_options
  {
    /* SKIP DOC */
    $$.val = &tree.ShowCreate{
      Mode: tree.ShowCreateModeTable,
      Name: $4.unresolvedObjectName(),
      FmtOpt: $5.showCreateFormatOption(),
    }
  }
| SHOW CREATE VIEW table_name opt_show_create_format_options
  {
    /* SKIP DOC */
    $$.val = &tree.ShowCreate{
      Mode: tree.ShowCreateModeView,
      Name: $4.unresolvedObjectName(),
      FmtOpt: $5.showCreateFormatOption(),
    }
  }
| SHOW CREATE SEQUENCE sequence_name
  {
    /* SKIP DOC */
    $$.val = &tree.ShowCreate{Mode: tree.ShowCreateModeSequence, Name: $4.unresolvedObjectName()}
  }
| SHOW CREATE DATABASE db_name
  {
    /* SKIP DOC */
    $$.val = &tree.ShowCreate{Mode: tree.ShowCreateModeDatabase, Name: $4.unresolvedObjectName()}
  }
| SHOW CREATE INDEXES FROM table_name
  {
    /* SKIP DOC */
    $$.val = &tree.ShowCreate{Mode: tree.ShowCreateModeIndexes, Name: $5.unresolvedObjectName()}
  }
| SHOW CREATE SECONDARY INDEXES FROM table_name
  {
    /* SKIP DOC */
    $$.val = &tree.ShowCreate{Mode: tree.ShowCreateModeSecondaryIndexes, Name: $6.unresolvedObjectName()}
  }
| SHOW CREATE FUNCTION db_object_name
  {
    /* SKIP DOC */
    $$.val = &tree.ShowCreateRoutine{
      Name: tree.ResolvableFunctionReference{
        FunctionReference: $4.unresolvedObjectName().ToUnresolvedName(),
      },
    }
  }
| SHOW CREATE PROCEDURE db_object_name
  {
    /* SKIP DOC */
    $$.val = &tree.ShowCreateRoutine{
      Name: tree.ResolvableFunctionReference{
        FunctionReference: $4.unresolvedObjectName().ToUnresolvedName(),
      },
      Procedure: true,
    }
  }
| SHOW CREATE TRIGGER name ON table_name
  {
    /* SKIP DOC */
    $$.val = &tree.ShowCreateTrigger{
      Name: tree.Name($4),
      TableName: $6.unresolvedObjectName(),
    }
  }
| SHOW CREATE ALL SCHEMAS
  {
    $$.val = &tree.ShowCreateAllSchemas{}
  }
| SHOW CREATE ALL TABLES
  {
    $$.val = &tree.ShowCreateAllTables{}
  }
| SHOW CREATE ALL TYPES
  {
    $$.val = &tree.ShowCreateAllTypes{}
  }
| SHOW CREATE error // SHOW HELP: SHOW CREATE

opt_show_create_format_options:
  /* EMPTY */
  {
    $$.val = tree.ShowCreateFormatOptionNone
  }
| WITH REDACT
  {
    $$.val = tree.ShowCreateFormatOptionRedactedValues
  }
| WITH IGNORE_FOREIGN_KEYS
  {
    $$.val = tree.ShowCreateFormatOptionIgnoreFKs
  }

// %Help: SHOW CREATE SCHEDULES - list CREATE statements for scheduled jobs
// %Category: DDL
// %Text:
// SHOW CREATE ALL SCHEDULES
// SHOW CREATE SCHEDULE <schedule_id>
// %SeeAlso: SHOW SCHEDULES, PAUSE SCHEDULES, RESUME SCHEDULES, DROP SCHEDULES
show_create_schedules_stmt:
  SHOW CREATE ALL SCHEDULES
  {
    $$.val = &tree.ShowCreateSchedules{}
  }
| SHOW CREATE ALL SCHEDULES error // SHOW HELP: SHOW CREATE SCHEDULES
| SHOW CREATE SCHEDULE a_expr
  {
    $$.val = &tree.ShowCreateSchedules{ScheduleID: $4.expr()}
  }
| SHOW CREATE SCHEDULE error // SHOW HELP: SHOW CREATE SCHEDULES

// %Help: SHOW CREATE EXTERNAL CONNECTIONS - list CREATE statements for external connections
// %Category: DDL
// %Text:
// SHOW CREATE ALL EXTERNAL CONNECTIONS
// SHOW CREATE EXTERNAL CONNECTION <connection_name>
show_create_external_connections_stmt:
  SHOW CREATE ALL EXTERNAL CONNECTIONS
  {
    $$.val = &tree.ShowCreateExternalConnections{}
  }
| SHOW CREATE ALL EXTERNAL CONNECTIONS error // SHOW HELP: SHOW CREATE EXTERNAL CONNECTIONS
| SHOW CREATE EXTERNAL CONNECTION string_or_placeholder
 {
   $$.val = &tree.ShowCreateExternalConnections{ConnectionLabel: $5.expr()}
 }
| SHOW CREATE EXTERNAL CONNECTION error // SHOW HELP: SHOW CREATE EXTERNAL CONNECTIONS

// %Help: SHOW USERS - list defined users
// %Category: Priv
// %Text: SHOW USERS
// %SeeAlso: CREATE USER, DROP USER, WEBDOCS/show-users.html
show_users_stmt:
  SHOW USERS
  {
    $$.val = &tree.ShowUsers{}
  }
| SHOW USERS error // SHOW HELP: SHOW USERS

// %Help: SHOW DEFAULT SESSION VARIABLES FOR ROLE - list default session variables for role
// %Category: Priv
// %Text: SHOW DEFAULT SESSION VARIABLES FOR ROLE <name>
show_default_session_variables_for_role_stmt:
 SHOW DEFAULT SESSION VARIABLES FOR role_or_group_or_user role_spec
    {
      $$.val = &tree.ShowDefaultSessionVariablesForRole{Name: $7.roleSpec(), IsRole: $6.bool()}
    }
| SHOW DEFAULT SESSION VARIABLES FOR ROLE_ALL ALL
     {
       $$.val = &tree.ShowDefaultSessionVariablesForRole{All: true, IsRole: true}
     }
| SHOW DEFAULT SESSION VARIABLES FOR USER_ALL ALL
     {
       $$.val = &tree.ShowDefaultSessionVariablesForRole{All: true, IsRole: false}
     }
| SHOW DEFAULT SESSION VARIABLES FOR role_or_group_or_user error // SHOW HELP: SHOW DEFAULT SESSION VARIABLES FOR ROLE

// %Help: SHOW ROLES - list defined roles
// %Category: Priv
// %Text: SHOW ROLES
// %SeeAlso: CREATE ROLE, ALTER ROLE, DROP ROLE
show_roles_stmt:
  SHOW ROLES
  {
    $$.val = &tree.ShowRoles{}
  }
| SHOW ROLES error // SHOW HELP: SHOW ROLES

// %Help: SHOW ZONE CONFIGURATION - display current zone configuration
// %Category: Cfg
// %Text: SHOW ZONE CONFIGURATION FROM [ RANGE | DATABASE | TABLE | INDEX ] <name>
// SHOW ZONE CONFIGURATION FROM PARTITION OF [ INDEX | TABLE ] <name>
// SHOW [ALL] ZONE CONFIGURATIONS
// %SeeAlso: WEBDOCS/show-zone-configurations.html
show_zone_stmt:
  SHOW ZONE CONFIGURATION from_with_implicit_for_alias RANGE zone_name
  {
    $$.val = &tree.ShowZoneConfig{ZoneSpecifier: tree.ZoneSpecifier{NamedZone: tree.UnrestrictedName($6)}}
  }
| SHOW ZONE CONFIGURATION from_with_implicit_for_alias DATABASE database_name
  {
    $$.val = &tree.ShowZoneConfig{ZoneSpecifier: tree.ZoneSpecifier{Database: tree.Name($6)}}
  }
| SHOW ZONE CONFIGURATION from_with_implicit_for_alias TABLE table_name opt_partition
  {
    name := $6.unresolvedObjectName().ToTableName()
    $$.val = &tree.ShowZoneConfig{ZoneSpecifier: tree.ZoneSpecifier{
        TableOrIndex: tree.TableIndexName{Table: name},
        Partition: tree.Name($7),
    }}
  }
| SHOW ZONE CONFIGURATION from_with_implicit_for_alias PARTITION partition_name OF TABLE table_name
  {
    name := $9.unresolvedObjectName().ToTableName()
    $$.val = &tree.ShowZoneConfig{ZoneSpecifier: tree.ZoneSpecifier{
      TableOrIndex: tree.TableIndexName{Table: name},
      Partition: tree.Name($6),
    }}
  }
| SHOW ZONE CONFIGURATION from_with_implicit_for_alias INDEX table_index_name opt_partition
  {
    $$.val = &tree.ShowZoneConfig{ZoneSpecifier: tree.ZoneSpecifier{
      TableOrIndex: $6.tableIndexName(),
      Partition: tree.Name($7),
    }}
  }
| SHOW ZONE CONFIGURATION from_with_implicit_for_alias PARTITION partition_name OF INDEX table_index_name
  {
    $$.val = &tree.ShowZoneConfig{ZoneSpecifier: tree.ZoneSpecifier{
      TableOrIndex: $9.tableIndexName(),
      Partition: tree.Name($6),
    }}
  }
| SHOW ZONE CONFIGURATION error // SHOW HELP: SHOW ZONE CONFIGURATION
| SHOW ZONE CONFIGURATIONS
  {
    $$.val = &tree.ShowZoneConfig{}
  }
| SHOW ZONE CONFIGURATIONS error // SHOW HELP: SHOW ZONE CONFIGURATION
| SHOW ALL ZONE CONFIGURATIONS
  {
    $$.val = &tree.ShowZoneConfig{}
  }
| SHOW ALL ZONE CONFIGURATIONS error // SHOW HELP: SHOW ZONE CONFIGURATION

from_with_implicit_for_alias:
  FROM
| FOR { /* SKIP DOC */ }

// %Help: SHOW RANGE - show range information for a row
// %Category: Misc
// %Text:
// SHOW RANGE FROM TABLE <tablename> FOR ROW (value1, value2, ...)
// SHOW RANGE FROM INDEX [ <tablename> @ ] <indexname> FOR ROW (value1, value2, ...)
show_range_for_row_stmt:
  SHOW RANGE FROM TABLE table_name FOR ROW '(' expr_list ')'
  {
    name := $5.unresolvedObjectName().ToTableName()
    $$.val = &tree.ShowRangeForRow{
      Row: $9.exprs(),
      TableOrIndex: tree.TableIndexName{Table: name},
    }
  }
| SHOW RANGE FROM INDEX table_index_name FOR ROW '(' expr_list ')'
  {
    $$.val = &tree.ShowRangeForRow{
      Row: $9.exprs(),
      TableOrIndex: $5.tableIndexName(),
    }
  }
| SHOW RANGE error // SHOW HELP: SHOW RANGE

// %Help: SHOW RANGES - list ranges
// %Category: Misc
// %Text:
// SHOW CLUSTER RANGES                                  [ WITH <options...> ]
// SHOW RANGES FROM DATABASE <databasename>             [ WITH <options...> ]
// SHOW RANGES FROM CURRENT_CATALOG                     [ WITH <options...> ]
// SHOW RANGES FROM TABLE   <tablename>                 [ WITH <options...> ]
// SHOW RANGES FROM INDEX [ <tablename> @ ] <indexname> [ WITH <options...> ]
//
// Options:
//   INDEXES: list indexes contained per range
//   TABLES:  list tables contained per range
//   DETAILS: add range size, leaseholder and other details
//   KEYS:    include binary start/end keys
//   EXPLAIN: show the SQL queries that produces the result
//
// Note: the availability of some of the options listed above is subject
// to cluster configuration. See the documentation for details.
//
// %SeeAlso: WEBDOCS/show-ranges.html
show_ranges_stmt:
  SHOW RANGES FROM INDEX table_index_name opt_show_ranges_options
  {
    $$.val = &tree.ShowRanges{Source: tree.ShowRangesIndex, TableOrIndex: $5.tableIndexName(), Options: $6.showRangesOpts()}
  }
| SHOW RANGES FROM TABLE table_name opt_show_ranges_options
  {
    name := $5.unresolvedObjectName().ToTableName()
    $$.val = &tree.ShowRanges{Source: tree.ShowRangesTable, TableOrIndex: tree.TableIndexName{Table: name}, Options: $6.showRangesOpts()}
  }
| SHOW RANGES FROM DATABASE database_name opt_show_ranges_options
  {
    $$.val = &tree.ShowRanges{Source: tree.ShowRangesDatabase, DatabaseName: tree.Name($5), Options: $6.showRangesOpts()}
  }
| SHOW RANGES FROM CURRENT_CATALOG opt_show_ranges_options
  {
    $$.val = &tree.ShowRanges{Source: tree.ShowRangesCurrentDatabase, Options: $5.showRangesOpts()}
  }
| SHOW RANGES opt_show_ranges_options
  {
    $$.val = &tree.ShowRanges{Source: tree.ShowRangesCurrentDatabase, Options: $3.showRangesOpts()}
  }
| SHOW RANGES error // SHOW HELP: SHOW RANGES
| SHOW CLUSTER RANGES opt_show_ranges_options
  {
    $$.val = &tree.ShowRanges{Source: tree.ShowRangesCluster, Options: $4.showRangesOpts()}
  }
| SHOW CLUSTER RANGES error // SHOW HELP: SHOW RANGES

opt_show_ranges_options:
  /* EMPTY */
  { $$.val = &tree.ShowRangesOptions{} }
| WITH show_ranges_options
  { $$.val = $2.showRangesOpts() }

show_ranges_options:
  TABLES  {  $$.val = &tree.ShowRangesOptions{Mode: tree.ExpandTables} }
| INDEXES {  $$.val = &tree.ShowRangesOptions{Mode: tree.ExpandIndexes} }
| DETAILS {  $$.val = &tree.ShowRangesOptions{Details: true} }
| KEYS    {  $$.val = &tree.ShowRangesOptions{Keys: true} }
| EXPLAIN {  $$.val = &tree.ShowRangesOptions{Explain: true} }
| show_ranges_options ',' TABLES
  {
    o := $1.showRangesOpts()
    if o.Mode != 0 { return setErr(sqllex, errors.New("conflicting modes")) }
    o.Mode = tree.ExpandTables
    $$.val = o
  }
| show_ranges_options ',' INDEXES
  {
    o := $1.showRangesOpts()
    if o.Mode != 0 { return setErr(sqllex, errors.New("conflicting modes")) }
    o.Mode = tree.ExpandIndexes
    $$.val = o
  }
| show_ranges_options ',' DETAILS
  {
    o := $1.showRangesOpts()
    o.Details = true
    $$.val = o
  }
| show_ranges_options ',' EXPLAIN
  {
    o := $1.showRangesOpts()
    o.Explain = true
    $$.val = o
  }
| show_ranges_options ',' KEYS
  {
    o := $1.showRangesOpts()
    o.Keys = true
    $$.val = o
  }


// %Help: SHOW SURVIVAL GOAL - list survival goals
// %Category: DDL
// %Text:
// SHOW SURVIVAL GOAL FROM DATABASE
// SHOW SURVIVAL GOAL FROM DATABASE <database>
show_survival_goal_stmt:
  SHOW SURVIVAL GOAL FROM DATABASE
  {
    $$.val = &tree.ShowSurvivalGoal{}
  }
| SHOW SURVIVAL GOAL FROM DATABASE database_name
  {
    $$.val = &tree.ShowSurvivalGoal{
      DatabaseName: tree.Name($6),
    }
  }

// %Help: SHOW REGIONS - list regions
// %Category: DDL
// %Text:
// SHOW REGIONS
// SHOW REGIONS FROM ALL DATABASES
// SHOW REGIONS FROM CLUSTER
// SHOW REGIONS FROM DATABASE
// SHOW REGIONS FROM DATABASE <database>
show_regions_stmt:
  SHOW REGIONS FROM CLUSTER
  {
    $$.val = &tree.ShowRegions{
      ShowRegionsFrom: tree.ShowRegionsFromCluster,
    }
  }
| SHOW REGIONS FROM DATABASE
  {
    $$.val = &tree.ShowRegions{
      ShowRegionsFrom: tree.ShowRegionsFromDatabase,
    }
  }
| SHOW REGIONS FROM ALL DATABASES
  {
    $$.val = &tree.ShowRegions{
      ShowRegionsFrom: tree.ShowRegionsFromAllDatabases,
    }
  }
| SHOW REGIONS FROM DATABASE database_name
  {
    $$.val = &tree.ShowRegions{
      ShowRegionsFrom: tree.ShowRegionsFromDatabase,
      DatabaseName: tree.Name($5),
    }
  }
| SHOW REGIONS
  {
    $$.val = &tree.ShowRegions{
      ShowRegionsFrom: tree.ShowRegionsFromDefault,
    }
  }
| SHOW SUPER REGIONS FROM DATABASE database_name
  {
    $$.val = &tree.ShowRegions{
      ShowRegionsFrom: tree.ShowSuperRegionsFromDatabase,
      DatabaseName: tree.Name($6),
    }
  }
| SHOW REGIONS error // SHOW HELP: SHOW REGIONS

show_locality_stmt:
  SHOW LOCALITY
  {
    $$.val = &tree.ShowVar{Name: "locality"}
  }

show_fingerprints_stmt:
  SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE table_name opt_with_show_fingerprints_options
  {
    /* SKIP DOC */
    $$.val = &tree.ShowFingerprints{Table: $5.unresolvedObjectName(), Options: *$6.showFingerprintOptions()}
  }
| SHOW EXPERIMENTAL_FINGERPRINTS FROM virtual_cluster virtual_cluster_spec opt_with_show_fingerprints_options
  {
    /* SKIP DOC */
    $$.val = &tree.ShowFingerprints{TenantSpec: $5.tenantSpec(), Options: *$6.showFingerprintOptions()}
  }

opt_with_show_fingerprints_options:
  WITH fingerprint_options_list
  {
    $$.val = $2.showFingerprintOptions()
  }
| WITH OPTIONS '(' fingerprint_options_list ')'
  {
    $$.val = $4.showFingerprintOptions()
  }
| /* EMPTY */
  {
    $$.val = &tree.ShowFingerprintOptions{}
  }

fingerprint_options_list:
  // Require at least one option
  fingerprint_options
  {
    $$.val = $1.showFingerprintOptions()
  }
| fingerprint_options_list ',' fingerprint_options
  {
    if err := $1.showFingerprintOptions().CombineWith($3.showFingerprintOptions()); err != nil {
      return setErr(sqllex, err)
    }
  }

// List of valid backup options.
fingerprint_options:
  START TIMESTAMP '=' d_expr
  {
    $$.val = &tree.ShowFingerprintOptions{StartTimestamp: $4.expr()}
  }
| EXCLUDE COLUMNS '=' string_or_placeholder_opt_list
  {
    $$.val = &tree.ShowFingerprintOptions{ExcludedUserColumns: $4.stringOrPlaceholderOptList()}
  }



show_full_scans_stmt:
  SHOW FULL TABLE SCANS
  {
    $$.val = &tree.ShowFullTableScans{}
  }

opt_on_targets_roles:
  ON targets_roles
  {
    tmp := $2.grantTargetList()
    $$.val = &tmp
  }
| /* EMPTY */
  {
    $$.val = (*tree.GrantTargetList)(nil)
  }

// grant_targets is a non-terminal for a list of privilege targets, either a
// list of databases or a list of tables.
//
// This rule is complex and cannot be decomposed as a tree of
// non-terminals because it must resolve syntax ambiguities in the
// SHOW GRANTS ON ROLE statement. It was constructed as follows.
//
// 1. Start with the desired definition of targets:
//
//    targets ::=
//        table_pattern_list
//        TABLE table_pattern_list
//        DATABASE name_list
//
// 2. Now we must disambiguate the first rule "table_pattern_list"
//    between one that recognizes ROLE and one that recognizes
//    "<some table pattern list>". So first, inline the definition of
//    table_pattern_list.
//
//    targets ::=
//        table_pattern                          # <- here
//        table_pattern_list ',' table_pattern   # <- here
//        TABLE table_pattern_list
//        DATABASE name_list
//
// 3. We now must disambiguate the "ROLE" inside the prefix "table_pattern".
//    However having "table_pattern_list" as prefix is cumbersome, so swap it.
//
//    targets ::=
//        table_pattern
//        table_pattern ',' table_pattern_list   # <- here
//        TABLE table_pattern_list
//        DATABASE name_list
//
// 4. The rule that has table_pattern followed by a comma is now
//    non-problematic, because it will never match "ROLE" followed
//    by an optional name list (neither "ROLE;" nor "ROLE <ident>"
//    would match). We just need to focus on the first one "table_pattern".
//    This needs to tweak "table_pattern".
//
//    Here we could inline table_pattern but now we do not have to any
//    more, we just need to create a variant of it which is
//    unambiguous with a single ROLE keyword. That is, we need a
//    table_pattern which cannot contain a single name. We do
//    this as follows.
//
//    targets ::=
//        complex_table_pattern                  # <- here
//        table_pattern ',' table_pattern_list
//        TABLE table_pattern_list
//        DATABASE name_list
//    complex_table_pattern ::=
//        name '.' unrestricted_name
//        name '.' unrestricted_name '.' unrestricted_name
//        name '.' unrestricted_name '.' '*'
//        name '.' '*'
//        '*'
//
// 5. At this point the rule cannot start with a simple identifier any
//    more, keyword or not. But more importantly, any token sequence
//    that starts with ROLE cannot be matched by any of these remaining
//    rules. This means that the prefix is now free to use, without
//    ambiguity. We do this as follows, to gain a syntax rule for "ROLE
//    <namelist>". (We will handle a ROLE with no name list below.)
//
//    targets ::=
//        ROLE name_list                        # <- here
//        complex_table_pattern
//        table_pattern ',' table_pattern_list
//        TABLE table_pattern_list
//        DATABASE name_list
//
// 6. Now on to the finishing touches. First we would like to regain the
//    ability to use "<tablename>" when the table name is a simple
//    identifier. This is done as follows:
//
//    targets ::=
//        ROLE name_list
//        name                                  # <- here
//        complex_table_pattern
//        table_pattern ',' table_pattern_list
//        TABLE table_pattern_list
//        DATABASE name_list
//
// 7. Then, we want to recognize "ROLE" without any subsequent name
//    list. This requires some care: we can not add "ROLE" to the set of
//    rules above, because "name" would then overlap. To disambiguate,
//    we must first inline "name" as follows:
//
//    targets ::=
//        ROLE name_list
//        IDENT                    # <- here, always <table>
//        col_name_keyword         # <- here, always <table>
//        unreserved_keyword       # <- here, either ROLE or <table>
//        complex_table_pattern
//        table_pattern ',' table_pattern_list
//        TABLE table_pattern_list
//        DATABASE name_list
//
// 8. And now the rule is sufficiently simple that we can disambiguate
//    in the action, like this:
//
//    targets ::=
//        ...
//        unreserved_keyword {
//             if $1 == "role" { /* handle ROLE */ }
//             else { /* handle ON <tablename> */ }
//        }
//        ...
//
//   (but see the comment on the action of this sub-rule below for
//   more nuance.)
//
// Tada!
// TODO(knz): This should learn how to parse more complex expressions
// and placeholders.
grant_targets:
  IDENT
  {
    $$.val = tree.GrantTargetList{Tables: tree.TableAttrs{SequenceOnly: false, TablePatterns: tree.TablePatterns{&tree.UnresolvedName{NumParts:1, Parts: tree.NameParts{$1}}}}}
  }
| col_name_keyword
  {
    $$.val = tree.GrantTargetList{Tables: tree.TableAttrs{SequenceOnly: false, TablePatterns: tree.TablePatterns{&tree.UnresolvedName{NumParts:1, Parts: tree.NameParts{$1}}}}}
  }
| unreserved_keyword
  {
    // This sub-rule is meant to support both ROLE and other keywords
    // used as table name without the TABLE prefix. The keyword ROLE
    // here can have two meanings:
    //
    // - for all statements except SHOW GRANTS, it must be interpreted
    //   as a plain table name.
    // - for SHOW GRANTS specifically, it must be handled as an ON ROLE
    //   specifier without a name list (the rule with a name list is separate,
    //   see above).
    //
    // Yet we want to use a single "targets" non-terminal for all
    // statements that use targets, to share the code. This action
    // achieves this as follows:
    //
    // - for all statements (including SHOW GRANTS), it populates the
    //   Tables list in TargetList{} with the given name. This will
    //   include the given keyword as table pattern in all cases,
    //   including when the keyword was ROLE.
    //
    // - if ROLE was specified, it remembers this fact in the ForRoles
    //   field. This distinguishes `ON ROLE` (where "role" is
    //   specified as keyword), which triggers the special case in
    //   SHOW GRANTS, from `ON "role"` (where "role" is specified as
    //   identifier), which is always handled as a table name.
    //
    //   Both `ON ROLE` and `ON "role"` populate the Tables list in the same way,
    //   so that other statements than SHOW GRANTS don't observe any difference.
    //
    // Arguably this code is a bit too clever. Future work should aim
    // to remove the special casing of SHOW GRANTS altogether instead
    // of increasing (or attempting to modify) the grey magic occurring
    // here.
    $$.val = tree.GrantTargetList{
      Tables: tree.TableAttrs{SequenceOnly: false, TablePatterns:tree.TablePatterns{&tree.UnresolvedName{NumParts:1, Parts: tree.NameParts{$1}}}},
      ForRoles: $1 == "role", // backdoor for "SHOW GRANTS ON ROLE" (no name list)
    }
  }
| complex_table_pattern
  {
    $$.val = tree.GrantTargetList{Tables: tree.TableAttrs{SequenceOnly: false, TablePatterns: tree.TablePatterns{$1.unresolvedName()}}}
  }
| SEQUENCE table_pattern_list
  {
    $$.val = tree.GrantTargetList{Tables: tree.TableAttrs{SequenceOnly: true, TablePatterns: $2.tablePatterns()}}
  }
| table_pattern ',' table_pattern_list
  {
    remainderPats := $3.tablePatterns()
    $$.val = tree.GrantTargetList{Tables: tree.TableAttrs{SequenceOnly: false, TablePatterns: append(tree.TablePatterns{$1.unresolvedName()}, remainderPats...)}}
  }
| TABLE table_pattern_list
  {
    $$.val = tree.GrantTargetList{Tables: tree.TableAttrs{SequenceOnly: false, TablePatterns: $2.tablePatterns()}}
  }
| DATABASE name_list
  {
    $$.val = tree.GrantTargetList{Databases: $2.nameList()}
  }
| EXTERNAL CONNECTION name_list
  {
    $$.val = tree.GrantTargetList{ExternalConnections: $3.nameList()}
  }
| FUNCTION function_with_paramtypes_list
  {
    $$.val = tree.GrantTargetList{Functions: $2.routineObjs()}
  }
| PROCEDURE function_with_paramtypes_list
  {
    $$.val = tree.GrantTargetList{Procedures: $2.routineObjs()}
  }

// backup_targets is similar to grant_targets but used by backup and restore, and thus
// supports tenants, but does not support sequences, types, or other SQL nouns
// that grants support, but rather just things which hold rows that can be backed
// up.
backup_targets:
  IDENT
  {
    $$.val = tree.BackupTargetList{Tables: tree.TableAttrs{SequenceOnly: false, TablePatterns: tree.TablePatterns{&tree.UnresolvedName{NumParts:1, Parts: tree.NameParts{$1}}}}}
  }
| col_name_keyword
  {
    $$.val = tree.BackupTargetList{Tables: tree.TableAttrs{SequenceOnly: false, TablePatterns: tree.TablePatterns{&tree.UnresolvedName{NumParts:1, Parts: tree.NameParts{$1}}}}}
  }
| unreserved_keyword
  {
      $$.val = tree.BackupTargetList{
      Tables: tree.TableAttrs{SequenceOnly: false, TablePatterns:tree.TablePatterns{&tree.UnresolvedName{NumParts:1, Parts: tree.NameParts{$1}}}},
    }
  }
| complex_table_pattern
  {
    $$.val = tree.BackupTargetList{Tables: tree.TableAttrs{SequenceOnly: false, TablePatterns: tree.TablePatterns{$1.unresolvedName()}}}
  }
| table_pattern ',' table_pattern_list
  {
    remainderPats := $3.tablePatterns()
    $$.val = tree.BackupTargetList{Tables: tree.TableAttrs{SequenceOnly: false, TablePatterns: append(tree.TablePatterns{$1.unresolvedName()}, remainderPats...)}}
  }
| TABLE table_pattern_list
  {
    $$.val = tree.BackupTargetList{Tables: tree.TableAttrs{SequenceOnly: false, TablePatterns: $2.tablePatterns()}}
  }
// TODO(knz): This should learn how to parse more complex expressions
// and placeholders.
| virtual_cluster iconst64
  {
    /* SKIP DOC */
    tenID := uint64($2.int64())
    if tenID == 0 {
      return setErr(sqllex, errors.New("invalid tenant ID"))
    }
    $$.val = tree.BackupTargetList{TenantID: tree.TenantID{Specified: true, ID: tenID}}
  }
| virtual_cluster IDENT
  {
    /* SKIP DOC */
    // TODO(knz): This rule can go away once the main clause above supports
    // arbitrary expressions.
    if $2 != "_" {
       return setErr(sqllex, errors.New("invalid syntax"))
    }
    $$.val = tree.BackupTargetList{TenantID: tree.TenantID{Specified: true}}
  }
| DATABASE name_list
  {
    $$.val = tree.BackupTargetList{Databases: $2.nameList()}
  }

// target_roles is the variant of targets which recognizes ON ROLES
// with a name list. This cannot be included in targets directly
// because some statements must not recognize this syntax.
targets_roles:
  ROLE role_spec_list
  {
     $$.val = tree.GrantTargetList{ForRoles: true, Roles: $2.roleSpecList()}
  }
| SCHEMA schema_name_list
  {
     $$.val = tree.GrantTargetList{Schemas: $2.objectNamePrefixList()}
  }
| SCHEMA schema_wildcard
   {
     $$.val = tree.GrantTargetList{Schemas: $2.objectNamePrefixList()}
   }
| TYPE type_name_list
  {
    $$.val = tree.GrantTargetList{Types: $2.unresolvedObjectNames()}
  }
| grant_targets


for_grantee_clause:
  FOR role_spec_list
  {
    $$.val = $2.roleSpecList()
  }
| /* EMPTY */
  {
    $$.val = tree.RoleSpecList(nil)
  }

// %Help: PAUSE - pause background tasks
// %Category: Group
// %Text: PAUSE JOBS, PAUSE SCHEDULES, PAUSE ALL JOBS
pause_stmt:
  pause_jobs_stmt       // EXTEND WITH HELP: PAUSE JOBS
| pause_schedules_stmt  // EXTEND WITH HELP: PAUSE SCHEDULES
| pause_all_jobs_stmt  // EXTEND WITH HELP: PAUSE ALL JOBS
| PAUSE error           // SHOW HELP: PAUSE

// %Help: RESUME - resume background tasks
// %Category: Group
// %Text: RESUME JOBS, RESUME SCHEDULES, RESUME ALL BACKUP JOBS
resume_stmt:
  resume_jobs_stmt       // EXTEND WITH HELP: RESUME JOBS
| resume_schedules_stmt  // EXTEND WITH HELP: RESUME SCHEDULES
| resume_all_jobs_stmt  // EXTEND WITH HELP: RESUME ALL JOBS
| RESUME error           // SHOW HELP: RESUME

// %Help: RESUME ALL JOBS - resume background jobs
// %Category: Misc
// %Text:
// RESUME ALL {BACKUP|CHANGEFEED|IMPORT|RESTORE} JOBS
resume_all_jobs_stmt:
  RESUME ALL name JOBS
  {
    $$.val = &tree.ControlJobsOfType{Type: $3, Command: tree.ResumeJob}
  }
| RESUME ALL error // SHOW HELP: RESUME ALL JOBS

// %Help: PAUSE JOBS - pause selected background jobs
// %Category: Misc
// %Text:
// PAUSE JOBS <selectclause>
// PAUSE JOB <jobid>
// %SeeAlso: SHOW JOBS, CANCEL JOBS, RESUME JOBS
pause_jobs_stmt:
  PAUSE JOB a_expr
  {
    $$.val = &tree.ControlJobs{
      Jobs: &tree.Select{
        Select: &tree.ValuesClause{Rows: []tree.Exprs{tree.Exprs{$3.expr()}}},
      },
      Command: tree.PauseJob,
    }
  }
| PAUSE JOB a_expr WITH REASON '=' string_or_placeholder
  {
    $$.val = &tree.ControlJobs{
      Jobs: &tree.Select{
        Select: &tree.ValuesClause{Rows: []tree.Exprs{tree.Exprs{$3.expr()}}},
      },
      Command: tree.PauseJob,
      Reason: $7.expr(),
    }
  }
| PAUSE JOB error // SHOW HELP: PAUSE JOBS
| PAUSE JOBS select_stmt
  {
    $$.val = &tree.ControlJobs{Jobs: $3.slct(), Command: tree.PauseJob}
  }
| PAUSE JOBS select_stmt WITH REASON '=' string_or_placeholder
  {
    $$.val = &tree.ControlJobs{Jobs: $3.slct(), Command: tree.PauseJob, Reason: $7.expr()}
  }
| PAUSE JOBS for_schedules_clause
  {
    $$.val = &tree.ControlJobsForSchedules{Schedules: $3.slct(), Command: tree.PauseJob}
  }
| PAUSE JOBS error // SHOW HELP: PAUSE JOBS


for_schedules_clause:
  FOR SCHEDULES select_stmt
  {
    $$.val = $3.slct()
  }
| FOR SCHEDULE a_expr
  {
   $$.val = &tree.Select{
     Select: &tree.ValuesClause{Rows: []tree.Exprs{tree.Exprs{$3.expr()}}},
   }
  }

// %Help: PAUSE SCHEDULES - pause scheduled jobs
// %Category: Misc
// %Text:
// PAUSE SCHEDULES <selectclause>
//   select clause: select statement returning schedule id to pause.
// PAUSE SCHEDULE <scheduleID>
// %SeeAlso: RESUME SCHEDULES, SHOW JOBS, CANCEL JOBS
pause_schedules_stmt:
  PAUSE SCHEDULE a_expr
  {
    $$.val = &tree.ControlSchedules{
      Schedules: &tree.Select{
        Select: &tree.ValuesClause{Rows: []tree.Exprs{tree.Exprs{$3.expr()}}},
      },
      Command: tree.PauseSchedule,
    }
  }
| PAUSE SCHEDULE error // SHOW HELP: PAUSE SCHEDULES
| PAUSE SCHEDULES select_stmt
  {
    $$.val = &tree.ControlSchedules{
      Schedules: $3.slct(),
      Command: tree.PauseSchedule,
    }
  }
| PAUSE SCHEDULES error // SHOW HELP: PAUSE SCHEDULES

// %Help: PAUSE ALL JOBS - pause all background jobs
// %Category: Misc
// %Text:
// PAUSE ALL {BACKUP|CHANGEFEED|IMPORT|RESTORE} JOBS
pause_all_jobs_stmt:
  PAUSE ALL name JOBS
  {
    $$.val = &tree.ControlJobsOfType{Type: $3, Command: tree.PauseJob}
  }
| PAUSE ALL error // SHOW HELP: PAUSE ALL JOBS


// %Help: CREATE SCHEMA - create a new schema
// %Category: DDL
// %Text:
// CREATE SCHEMA [IF NOT EXISTS] { [<databasename>.]<schemaname> | [[<databasename>.]<schemaname>] AUTHORIZATION <rolename> }
create_schema_stmt:
  CREATE SCHEMA qualifiable_schema_name
  {
    $$.val = &tree.CreateSchema{
      Schema: $3.objectNamePrefix(),
    }
  }
| CREATE SCHEMA IF NOT EXISTS qualifiable_schema_name
  {
    $$.val = &tree.CreateSchema{
      Schema: $6.objectNamePrefix(),
      IfNotExists: true,
    }
  }
| CREATE SCHEMA opt_schema_name AUTHORIZATION role_spec
  {
    $$.val = &tree.CreateSchema{
      Schema: $3.objectNamePrefix(),
      AuthRole: $5.roleSpec(),
    }
  }
| CREATE SCHEMA IF NOT EXISTS opt_schema_name AUTHORIZATION role_spec
  {
    $$.val = &tree.CreateSchema{
      Schema: $6.objectNamePrefix(),
      IfNotExists: true,
      AuthRole: $8.roleSpec(),
    }
  }
| CREATE SCHEMA error // SHOW HELP: CREATE SCHEMA

// %Help: ALTER SCHEMA - alter an existing schema
// %Category: DDL
// %Text:
//
// Commands:
//   ALTER SCHEMA ... RENAME TO <newschemaname>
//   ALTER SCHEMA ... OWNER TO {<newowner> | CURRENT_USER | SESSION_USER }
alter_schema_stmt:
  ALTER SCHEMA qualifiable_schema_name RENAME TO schema_name
  {
    $$.val = &tree.AlterSchema{
      Schema: $3.objectNamePrefix(),
      Cmd: &tree.AlterSchemaRename{
        NewName: tree.Name($6),
      },
    }
  }
| ALTER SCHEMA qualifiable_schema_name OWNER TO role_spec
  {
    $$.val = &tree.AlterSchema{
      Schema: $3.objectNamePrefix(),
      Cmd: &tree.AlterSchemaOwner{
        Owner: $6.roleSpec(),
      },
    }
  }
| ALTER SCHEMA error // SHOW HELP: ALTER SCHEMA

// %Help: CREATE TABLE - create a new table
// %Category: DDL
// %Text:
// CREATE [[GLOBAL | LOCAL] {TEMPORARY | TEMP}] TABLE [IF NOT EXISTS] <tablename> ( <elements...> ) [<on_commit>]
// CREATE [[GLOBAL | LOCAL] {TEMPORARY | TEMP}] TABLE [IF NOT EXISTS] <tablename> [( <colnames...> )] AS <source> [<on commit>]
//
// Table elements:
//    <name> <type> [<qualifiers...>]
//    [UNIQUE | INVERTED | VECTOR] INDEX [<name>] ( <colname> [ASC | DESC] [, ...] )
//                            [USING HASH] [{STORING | INCLUDE | COVERING} ( <colnames...> )]
//    FAMILY [<name>] ( <colnames...> )
//    [CONSTRAINT <name>] <constraint>
//
// Table constraints:
//    PRIMARY KEY ( <colnames...> ) [USING HASH]
//    FOREIGN KEY ( <colnames...> ) REFERENCES <tablename> [( <colnames...> )] [ON DELETE {NO ACTION | RESTRICT}] [ON UPDATE {NO ACTION | RESTRICT}]
//    UNIQUE ( <colnames...> ) [{STORING | INCLUDE | COVERING} ( <colnames...> )]
//    CHECK ( <expr> )
//
// Column qualifiers:
//   [CONSTRAINT <constraintname>] {NULL | NOT NULL | NOT VISIBLE | UNIQUE | PRIMARY KEY | CHECK (<expr>) | DEFAULT <expr> | ON UPDATE <expr> | GENERATED { ALWAYS | BY DEFAULT } AS IDENTITY [( <opt_sequence_option_list> )]}
//   FAMILY <familyname>, CREATE [IF NOT EXISTS] FAMILY [<familyname>]
//   REFERENCES <tablename> [( <colnames...> )] [ON DELETE {NO ACTION | RESTRICT}] [ON UPDATE {NO ACTION | RESTRICT}]
//   COLLATE <collationname>
//   AS ( <expr> ) { STORED | VIRTUAL }
//
// On commit clause:
//    ON COMMIT {PRESERVE ROWS | DROP | DELETE ROWS}
//
// %SeeAlso: SHOW TABLES, CREATE VIEW, SHOW CREATE,
// WEBDOCS/create-table.html
// WEBDOCS/create-table-as.html
create_table_stmt:
  CREATE opt_persistence_temp_table TABLE table_name '(' opt_table_elem_list ')' opt_create_table_inherits opt_partition_by_table opt_table_with opt_create_table_on_commit opt_locality
  {
    name := $4.unresolvedObjectName().ToTableName()
    $$.val = &tree.CreateTable{
      Table: name,
      IfNotExists: false,
      Defs: $6.tblDefs(),
      AsSource: nil,
      PartitionByTable: $9.partitionByTable(),
      Persistence: $2.persistence(),
      StorageParams: $10.storageParams(),
      OnCommit: $11.createTableOnCommitSetting(),
      Locality: $12.locality(),
    }
  }
| CREATE opt_persistence_temp_table TABLE IF NOT EXISTS table_name '(' opt_table_elem_list ')' opt_create_table_inherits opt_partition_by_table opt_table_with opt_create_table_on_commit opt_locality
  {
    name := $7.unresolvedObjectName().ToTableName()
    $$.val = &tree.CreateTable{
      Table: name,
      IfNotExists: true,
      Defs: $9.tblDefs(),
      AsSource: nil,
      PartitionByTable: $12.partitionByTable(),
      Persistence: $2.persistence(),
      StorageParams: $13.storageParams(),
      OnCommit: $14.createTableOnCommitSetting(),
      Locality: $15.locality(),
    }
  }

opt_locality:
  locality
  {
    $$.val = $1.locality()
  }
| /* EMPTY */
  {
    $$.val = (*tree.Locality)(nil)
  }

opt_table_with:
  opt_with_storage_parameter_list
| WITHOUT OIDS
  {
    /* SKIP DOC */
    /* this is also the default in CockroachDB */
    $$.val = nil
  }
| WITH OIDS error
  {
    return unimplemented(sqllex, "create table with oids")
  }

opt_create_table_inherits:
  /* EMPTY */
  {
    $$ = ""
  }
| INHERITS error
  {
    /* SKIP DOC */
    return unimplementedWithIssueDetail(sqllex, 22456, "create table inherits")
  }

opt_with_storage_parameter_list:
  {
    $$.val = nil
  }
| WITH '(' storage_parameter_list ')'
  {
    $$.val = $3.storageParams()
  }

opt_create_table_on_commit:
  {
    $$.val = tree.CreateTableOnCommitUnset
  }
| ON COMMIT PRESERVE ROWS
  {
    $$.val = tree.CreateTableOnCommitPreserveRows
  }
| ON COMMIT DELETE ROWS error
  {
    return unimplementedWithIssueDetail(sqllex, 46556, "delete rows")
  }
| ON COMMIT DROP error
  {
    return unimplementedWithIssueDetail(sqllex, 46556, "drop")
  }

storage_parameter_key:
  name
| SCONST

storage_parameter_key_list:
  storage_parameter_key
  {
    $$.val = []string{$1}
  }
| storage_parameter_key_list ',' storage_parameter_key
  {
    $$.val = append($1.storageParamKeys(), $3)
  }

storage_parameter:
  storage_parameter_key '=' var_value
  {
    $$.val = tree.StorageParam{Key: $1, Value: $3.expr()}
  }

storage_parameter_list:
  storage_parameter
  {
    $$.val = []tree.StorageParam{$1.storageParam()}
  }
|  storage_parameter_list ',' storage_parameter
  {
    $$.val = append($1.storageParams(), $3.storageParam())
  }

create_table_as_stmt:
  CREATE opt_persistence_temp_table TABLE table_name create_as_opt_col_list opt_table_with AS select_stmt opt_create_as_data opt_create_table_on_commit
  {
    name := $4.unresolvedObjectName().ToTableName()
    $$.val = &tree.CreateTable{
      Table: name,
      IfNotExists: false,
      Defs: $5.tblDefs(),
      AsSource: $8.slct(),
      StorageParams: $6.storageParams(),
      OnCommit: $10.createTableOnCommitSetting(),
      Persistence: $2.persistence(),
    }
  }
| CREATE opt_persistence_temp_table TABLE IF NOT EXISTS table_name create_as_opt_col_list opt_table_with AS select_stmt opt_create_as_data opt_create_table_on_commit
  {
    name := $7.unresolvedObjectName().ToTableName()
    $$.val = &tree.CreateTable{
      Table: name,
      IfNotExists: true,
      Defs: $8.tblDefs(),
      AsSource: $11.slct(),
      StorageParams: $9.storageParams(),
      OnCommit: $13.createTableOnCommitSetting(),
      Persistence: $2.persistence(),
    }
  }

opt_create_as_data:
  /* EMPTY */  { /* no error */ }
| WITH DATA    { /* SKIP DOC */ /* This is the default */ }
| WITH NO DATA { return unimplemented(sqllex, "create table as with no data") }

/*
 * Redundancy here is needed to avoid shift/reduce conflicts,
 * since TEMP is not a reserved word.  See also OptTempTableName.
 *
 * NOTE: we accept both GLOBAL and LOCAL options.  They currently do nothing,
 * but future versions might consider GLOBAL to request SQL-spec-compliant
 * temp table behavior.  Since we have no modules the
 * LOCAL keyword is really meaningless; furthermore, some other products
 * implement LOCAL as meaning the same as our default temp table behavior,
 * so we'll probably continue to treat LOCAL as a noise word.
 *
 * NOTE: PG only accepts GLOBAL/LOCAL keywords for temp tables -- not sequences
 * and views. These keywords are no-ops in PG. This behavior is replicated by
 * making the distinction between opt_temp and opt_persistence_temp_table.
 */
 opt_temp:
  TEMPORARY         { $$.val = tree.PersistenceTemporary }
| TEMP              { $$.val = tree.PersistenceTemporary }
| /*EMPTY*/         { $$.val = tree.PersistencePermanent }

opt_persistence_temp_table:
  opt_temp
| LOCAL TEMPORARY   { $$.val = tree.PersistenceTemporary }
| LOCAL TEMP        { $$.val = tree.PersistenceTemporary }
| GLOBAL TEMPORARY  { $$.val = tree.PersistenceTemporary }
| GLOBAL TEMP       { $$.val = tree.PersistenceTemporary }
| UNLOGGED          { $$.val = tree.PersistenceUnlogged }

opt_table_elem_list:
  table_elem_list
| /* EMPTY */
  {
    $$.val = tree.TableDefs(nil)
  }

table_elem_list:
  table_elem
  {
    $$.val = tree.TableDefs{$1.tblDef()}
  }
| table_elem_list ',' table_elem
  {
    $$.val = append($1.tblDefs(), $3.tblDef())
  }

table_elem:
  column_table_def
  {
    $$.val = $1.colTableDef()
  }
| index_def
| family_def
| table_constraint opt_validate_behavior
  {
    def := $1.constraintDef()
    valBehavior := $2.validationBehavior()
    if u, ok := def.(*tree.UniqueConstraintTableDef); ok && valBehavior == tree.ValidationSkip && !u.WithoutIndex {
      typ := "PRIMARY KEY"
      if !u.PrimaryKey {
        typ = "UNIQUE"
      }
      return purposelyUnimplemented(sqllex, "table constraint", typ + " constraints cannot be marked NOT VALID")
    }
    $$.val = def
  }
| LIKE table_name like_table_option_list
  {
    $$.val = &tree.LikeTableDef{
      Name: $2.unresolvedObjectName().ToTableName(),
      Options: $3.likeTableOptionList(),
    }
  }

like_table_option_list:
  like_table_option_list INCLUDING like_table_option
  {
    $$.val = append($1.likeTableOptionList(), $3.likeTableOption())
  }
| like_table_option_list EXCLUDING like_table_option
  {
    opt := $3.likeTableOption()
    opt.Excluded = true
    $$.val = append($1.likeTableOptionList(), opt)
  }
| /* EMPTY */
  {
    $$.val = []tree.LikeTableOption(nil)
  }

like_table_option:
  COMMENTS			{ return unimplementedWithIssueDetail(sqllex, 47071, "like table in/excluding comments") }
| CONSTRAINTS		{ $$.val = tree.LikeTableOption{Opt: tree.LikeTableOptConstraints} }
| DEFAULTS			{ $$.val = tree.LikeTableOption{Opt: tree.LikeTableOptDefaults} }
| IDENTITY	  	{ return unimplementedWithIssueDetail(sqllex, 47071, "like table in/excluding identity") }
| GENERATED			{ $$.val = tree.LikeTableOption{Opt: tree.LikeTableOptGenerated} }
| INDEXES			{ $$.val = tree.LikeTableOption{Opt: tree.LikeTableOptIndexes} }
| STATISTICS		{ return unimplementedWithIssueDetail(sqllex, 47071, "like table in/excluding statistics") }
| STORAGE			{ return unimplementedWithIssueDetail(sqllex, 47071, "like table in/excluding storage") }
| ALL				{ $$.val = tree.LikeTableOption{Opt: tree.LikeTableOptAll} }


partition:
  PARTITION partition_name
  {
    $$ = $2
  }

opt_partition:
  partition
| /* EMPTY */
  {
    $$ = ""
  }

opt_partition_by:
  partition_by
| /* EMPTY */
  {
    $$.val = (*tree.PartitionBy)(nil)
  }

partition_by_index:
  partition_by
  {
    $$.val = &tree.PartitionByIndex{
      PartitionBy: $1.partitionBy(),
    }
  }

opt_partition_by_index:
  partition_by
  {
    $$.val = &tree.PartitionByIndex{
      PartitionBy: $1.partitionBy(),
    }
  }
| /* EMPTY */
  {
    $$.val = (*tree.PartitionByIndex)(nil)
  }

partition_by_table:
  partition_by
  {
    $$.val = &tree.PartitionByTable{
      PartitionBy: $1.partitionBy(),
    }
  }
| PARTITION ALL BY partition_by_inner
  {
    $$.val = &tree.PartitionByTable{
      All: true,
      PartitionBy: $4.partitionBy(),
    }
  }

opt_partition_by_table:
  partition_by_table
| /* EMPTY */
  {
    $$.val = (*tree.PartitionByTable)(nil)
  }

partition_by:
  PARTITION BY partition_by_inner
  {
    $$.val = $3.partitionBy()
  }

partition_by_inner:
  LIST '(' name_list ')' '(' list_partitions ')'
  {
    $$.val = &tree.PartitionBy{
      Fields: $3.nameList(),
      List: $6.listPartitions(),
    }
  }
| RANGE '(' name_list ')' '(' range_partitions ')'
  {
    $$.val = &tree.PartitionBy{
      Fields: $3.nameList(),
      Range: $6.rangePartitions(),
    }
  }
| NOTHING
  {
    $$.val = (*tree.PartitionBy)(nil)
  }

list_partitions:
  list_partition
  {
    $$.val = []tree.ListPartition{$1.listPartition()}
  }
| list_partitions ',' list_partition
  {
    $$.val = append($1.listPartitions(), $3.listPartition())
  }

list_partition:
  partition VALUES IN '(' expr_list ')' opt_partition_by
  {
    $$.val = tree.ListPartition{
      Name: tree.Name($1),
      Exprs: $5.exprs(),
      Subpartition: $7.partitionBy(),
    }
  }

range_partitions:
  range_partition
  {
    $$.val = []tree.RangePartition{$1.rangePartition()}
  }
| range_partitions ',' range_partition
  {
    $$.val = append($1.rangePartitions(), $3.rangePartition())
  }

range_partition:
  partition VALUES FROM '(' expr_list ')' TO '(' expr_list ')' opt_partition_by
  {
    $$.val = tree.RangePartition{
      Name: tree.Name($1),
      From: $5.exprs(),
      To: $9.exprs(),
      Subpartition: $11.partitionBy(),
    }
  }

// Treat SERIAL pseudo-types as separate case so that types.T does not have to
// support them as first-class types (e.g. they should not be supported as CAST
// target types).
column_table_def:
  column_name typename col_qual_list
  {
    typ := $2.typeReference()
    tableDef, err := tree.NewColumnTableDef(tree.Name($1), typ, tree.IsReferenceSerialType(typ), $3.colQuals())
    if err != nil {
      return setErr(sqllex, err)
    }
    $$.val = tableDef
  }

col_qual_list:
  col_qual_list col_qualification
  {
    $$.val = append($1.colQuals(), $2.colQual())
  }
| /* EMPTY */
  {
    $$.val = []tree.NamedColumnQualification(nil)
  }

col_qualification:
  CONSTRAINT constraint_name col_qualification_elem
  {
    $$.val = tree.NamedColumnQualification{Name: tree.Name($2), Qualification: $3.colQualElem()}
  }
| col_qualification_elem
  {
    $$.val = tree.NamedColumnQualification{Qualification: $1.colQualElem()}
  }
| COLLATE collation_name
  {
    $$.val = tree.NamedColumnQualification{Qualification: tree.ColumnCollation($2)}
  }
| FAMILY family_name
  {
    $$.val = tree.NamedColumnQualification{Qualification: &tree.ColumnFamilyConstraint{Family: tree.Name($2)}}
  }
| CREATE FAMILY family_name
  {
    $$.val = tree.NamedColumnQualification{Qualification: &tree.ColumnFamilyConstraint{Family: tree.Name($3), Create: true}}
  }
| CREATE FAMILY
  {
    $$.val = tree.NamedColumnQualification{Qualification: &tree.ColumnFamilyConstraint{Create: true}}
  }
| CREATE IF NOT EXISTS FAMILY family_name
  {
    $$.val = tree.NamedColumnQualification{Qualification: &tree.ColumnFamilyConstraint{Family: tree.Name($6), Create: true, IfNotExists: true}}
  }

// DEFAULT NULL is already the default for Postgres. But define it here and
// carry it forward into the system to make it explicit.
// - thomas 1998-09-13
//
// WITH NULL and NULL are not SQL-standard syntax elements, so leave them
// out. Use DEFAULT NULL to explicitly indicate that a column may have that
// value. WITH NULL leads to shift/reduce conflicts with WITH TIME ZONE anyway.
// - thomas 1999-01-08
//
// DEFAULT expression must be b_expr not a_expr to prevent shift/reduce
// conflict on NOT (since NOT might start a subsequent NOT NULL constraint, or
// be part of a_expr NOT LIKE or similar constructs).
col_qualification_elem:
  NOT NULL
  {
    $$.val = tree.NotNullConstraint{}
  }
| NULL
  {
    $$.val = tree.NullConstraint{}
  }
| NOT VISIBLE
  {
    $$.val = tree.HiddenConstraint{}
  }
| UNIQUE opt_without_index
  {
    $$.val = tree.UniqueConstraint{
      WithoutIndex: $2.bool(),
    }
  }
| PRIMARY KEY opt_with_storage_parameter_list
  {
    $$.val = tree.PrimaryKeyConstraint{
      StorageParams: $3.storageParams(),
    }
  }
| PRIMARY KEY USING HASH opt_hash_sharded_bucket_count opt_with_storage_parameter_list
{
  $$.val = tree.ShardedPrimaryKeyConstraint{
    Sharded: true,
    ShardBuckets: $5.expr(),
    StorageParams: $6.storageParams(),
  }
}
| CHECK '(' a_expr ')'
  {
    $$.val = &tree.ColumnCheckConstraint{Expr: $3.expr()}
  }
| DEFAULT b_expr
  {
    $$.val = &tree.ColumnDefault{Expr: $2.expr()}
  }
| ON UPDATE b_expr
  {
    $$.val = &tree.ColumnOnUpdate{Expr: $3.expr()}
  }
| REFERENCES table_name opt_name_parens key_match reference_actions
  {
    name := $2.unresolvedObjectName().ToTableName()
    $$.val = &tree.ColumnFKConstraint{
      Table: name,
      Col: tree.Name($3),
      Actions: $5.referenceActions(),
      Match: $4.compositeKeyMatchMethod(),
    }
  }
| generated_as '(' a_expr ')' STORED
  {
    $$.val = &tree.ColumnComputedDef{Expr: $3.expr(), Virtual: false}
  }
| generated_as '(' a_expr ')' VIRTUAL
  {
    $$.val = &tree.ColumnComputedDef{Expr: $3.expr(), Virtual: true}
  }
| generated_as error
  {
    sqllex.Error("use AS ( <expr> ) STORED or AS ( <expr> ) VIRTUAL")
    return 1
  }
| generated_always_as IDENTITY '(' opt_sequence_option_list ')'
  {
    $$.val = &tree.GeneratedAlwaysAsIdentity{
       SeqOptions: $4.seqOpts(),
    }
  }
| generated_by_default_as IDENTITY '(' opt_sequence_option_list ')'
  {
    $$.val = &tree.GeneratedByDefAsIdentity{
        SeqOptions: $4.seqOpts(),
    }
  }
| generated_always_as IDENTITY
  {
    $$.val = &tree.GeneratedAlwaysAsIdentity{}
  }
| generated_by_default_as IDENTITY
  {
    $$.val = &tree.GeneratedByDefAsIdentity{}
  }

opt_without_index:
  WITHOUT INDEX
  {
    /* SKIP DOC */
    $$.val = true
  }
| /* EMPTY */
  {
    $$.val = false
  }

generated_as:
  AS {}
| generated_always_as

set_generated_always:
  SET GENERATED_ALWAYS ALWAYS

set_generated_default:
  SET GENERATED_BY_DEFAULT BY DEFAULT

generated_always_as:
  GENERATED_ALWAYS ALWAYS AS {}

generated_by_default_as:
  GENERATED_BY_DEFAULT BY DEFAULT AS {}

index_def:
  INDEX_BEFORE_PAREN '(' index_params ')' opt_hash_sharded opt_storing opt_partition_by_index opt_with_storage_parameter_list opt_where_clause opt_index_visible
  {
    $$.val = &tree.IndexTableDef{
      Name:             "",
      Columns:          $3.idxElems(),
      Sharded:          $5.shardedIndexDef(),
      Storing:          $6.nameList(),
      PartitionByIndex: $7.partitionByIndex(),
      StorageParams:    $8.storageParams(),
      Predicate:        $9.expr(),
      Invisibility:     $10.indexInvisibility(),
    }
  }
| INDEX_BEFORE_NAME_THEN_PAREN name '(' index_params ')' opt_hash_sharded opt_storing opt_partition_by_index opt_with_storage_parameter_list opt_where_clause opt_index_visible
  {
    $$.val = &tree.IndexTableDef{
      Name:             tree.Name($2),
      Columns:          $4.idxElems(),
      Sharded:          $6.shardedIndexDef(),
      Storing:          $7.nameList(),
      PartitionByIndex: $8.partitionByIndex(),
      StorageParams:    $9.storageParams(),
      Predicate:        $10.expr(),
      Invisibility:     $11.indexInvisibility(),
    }
  }
| UNIQUE INDEX opt_index_name '(' index_params ')' opt_hash_sharded opt_storing opt_partition_by_index opt_with_storage_parameter_list opt_where_clause opt_index_visible
  {
    $$.val = &tree.UniqueConstraintTableDef{
      IndexTableDef: tree.IndexTableDef {
        Name:             tree.Name($3),
        Columns:          $5.idxElems(),
        Sharded:          $7.shardedIndexDef(),
        Storing:          $8.nameList(),
        PartitionByIndex: $9.partitionByIndex(),
        StorageParams:    $10.storageParams(),
        Predicate:        $11.expr(),
        Invisibility:     $12.indexInvisibility(),
      },
    }
  }
| INVERTED INDEX_BEFORE_PAREN '(' index_params ')' opt_partition_by_index opt_with_storage_parameter_list opt_where_clause opt_index_visible
  {
    $$.val = &tree.IndexTableDef{
      Name:             "",
      Columns:          $4.idxElems(),
      Type:             idxtype.INVERTED,
      PartitionByIndex: $6.partitionByIndex(),
      StorageParams:    $7.storageParams(),
      Predicate:        $8.expr(),
      Invisibility:     $9.indexInvisibility(),
    }
  }
| INVERTED INDEX_BEFORE_NAME_THEN_PAREN name '(' index_params ')' opt_partition_by_index opt_with_storage_parameter_list opt_where_clause opt_index_visible
  {
    $$.val = &tree.IndexTableDef{
      Name:             tree.Name($3),
      Columns:          $5.idxElems(),
      Type:             idxtype.INVERTED,
      PartitionByIndex: $7.partitionByIndex(),
      StorageParams:    $8.storageParams(),
      Predicate:        $9.expr(),
      Invisibility:     $10.indexInvisibility(),
    }
  }
| VECTOR INDEX_BEFORE_PAREN '(' index_params ')' opt_partition_by_index opt_with_storage_parameter_list opt_where_clause opt_index_visible
  {
    $$.val = &tree.IndexTableDef{
      Name:             "",
      Columns:          $4.idxElems(),
      Type:             idxtype.VECTOR,
      PartitionByIndex: $6.partitionByIndex(),
      StorageParams:    $7.storageParams(),
      Predicate:        $8.expr(),
      Invisibility:     $9.indexInvisibility(),
    }
  }
| VECTOR INDEX_BEFORE_NAME_THEN_PAREN name '(' index_params ')' opt_partition_by_index opt_with_storage_parameter_list opt_where_clause opt_index_visible
  {
    $$.val = &tree.IndexTableDef{
      Name:             tree.Name($3),
      Columns:          $5.idxElems(),
      Type:             idxtype.VECTOR,
      PartitionByIndex: $7.partitionByIndex(),
      StorageParams:    $8.storageParams(),
      Predicate:        $9.expr(),
      Invisibility:     $10.indexInvisibility(),
    }
  }

family_def:
  FAMILY opt_family_name '(' name_list ')'
  {
    $$.val = &tree.FamilyTableDef{
      Name: tree.Name($2),
      Columns: $4.nameList(),
    }
  }

// constraint_elem specifies constraint syntax which is not embedded into a
// column definition. col_qualification_elem specifies the embedded form.
// - thomas 1997-12-03
table_constraint:
  CONSTRAINT constraint_name constraint_elem
  {
    $$.val = $3.constraintDef()
    $$.val.(tree.ConstraintTableDef).SetName(tree.Name($2))
  }
| constraint_elem
  {
    $$.val = $1.constraintDef()
  }

constraint_elem:
  CHECK '(' a_expr ')' opt_deferrable
  {
    $$.val = &tree.CheckConstraintTableDef{
      Expr: $3.expr(),
    }
  }
| UNIQUE opt_without_index '(' index_params ')'
    opt_storing opt_partition_by_index opt_deferrable opt_where_clause
  {
    $$.val = &tree.UniqueConstraintTableDef{
      WithoutIndex: $2.bool(),
      IndexTableDef: tree.IndexTableDef{
        Columns: $4.idxElems(),
        Storing: $6.nameList(),
        PartitionByIndex: $7.partitionByIndex(),
        Predicate: $9.expr(),
      },
    }
  }
| PRIMARY KEY '(' index_params ')' opt_hash_sharded opt_with_storage_parameter_list
  {
    $$.val = &tree.UniqueConstraintTableDef{
      IndexTableDef: tree.IndexTableDef{
        Columns: $4.idxElems(),
        Sharded: $6.shardedIndexDef(),
        StorageParams: $7.storageParams(),
      },
      PrimaryKey: true,
    }
  }
| FOREIGN KEY '(' name_list ')' REFERENCES table_name
    opt_column_list key_match reference_actions opt_deferrable
  {
    name := $7.unresolvedObjectName().ToTableName()
    $$.val = &tree.ForeignKeyConstraintTableDef{
      Table: name,
      FromCols: $4.nameList(),
      ToCols: $8.nameList(),
      Match: $9.compositeKeyMatchMethod(),
      Actions: $10.referenceActions(),
    }
  }
| EXCLUDE USING error
  {
    return unimplementedWithIssueDetail(sqllex, 46657, "add constraint exclude using")
  }


create_as_opt_col_list:
  '(' create_as_table_defs ')'
  {
    $$.val = $2.val
  }
| /* EMPTY */
  {
    $$.val = tree.TableDefs(nil)
  }

create_as_table_defs:
  column_name create_as_col_qual_list
  {
    tableDef, err := tree.NewColumnTableDef(tree.Name($1), nil, false, $2.colQuals())
    if err != nil {
      return setErr(sqllex, err)
    }

    var colToTableDef tree.TableDef = tableDef
    $$.val = tree.TableDefs{colToTableDef}
  }
| create_as_table_defs ',' column_name create_as_col_qual_list
  {
    tableDef, err := tree.NewColumnTableDef(tree.Name($3), nil, false, $4.colQuals())
    if err != nil {
      return setErr(sqllex, err)
    }

    var colToTableDef tree.TableDef = tableDef

    $$.val = append($1.tblDefs(), colToTableDef)
  }
| create_as_table_defs ',' family_def
  {
    $$.val = append($1.tblDefs(), $3.tblDef())
  }
| create_as_table_defs ',' create_as_constraint_def
{
  var constraintToTableDef tree.TableDef = $3.constraintDef()
  $$.val = append($1.tblDefs(), constraintToTableDef)
}

create_as_constraint_def:
  create_as_constraint_elem
  {
    $$.val = $1.constraintDef()
  }

create_as_constraint_elem:
  PRIMARY KEY '(' create_as_params ')' opt_with_storage_parameter_list
  {
    $$.val = &tree.UniqueConstraintTableDef{
      IndexTableDef: tree.IndexTableDef{
        Columns: $4.idxElems(),
        StorageParams: $6.storageParams(),
      },
      PrimaryKey:    true,
    }
  }

create_as_params:
  create_as_param
  {
    $$.val = tree.IndexElemList{$1.idxElem()}
  }
| create_as_params ',' create_as_param
  {
    $$.val = append($1.idxElems(), $3.idxElem())
  }

create_as_param:
  column_name
  {
    $$.val = tree.IndexElem{Column: tree.Name($1)}
  }

create_as_col_qual_list:
  create_as_col_qual_list create_as_col_qualification
  {
    $$.val = append($1.colQuals(), $2.colQual())
  }
| /* EMPTY */
  {
    $$.val = []tree.NamedColumnQualification(nil)
  }

create_as_col_qualification:
  create_as_col_qualification_elem
  {
    $$.val = tree.NamedColumnQualification{Qualification: $1.colQualElem()}
  }
| FAMILY family_name
  {
    $$.val = tree.NamedColumnQualification{Qualification: &tree.ColumnFamilyConstraint{Family: tree.Name($2)}}
  }

create_as_col_qualification_elem:
  PRIMARY KEY opt_with_storage_parameter_list
  {
    $$.val = tree.PrimaryKeyConstraint{
      StorageParams: $3.storageParams(),
    }
  }

opt_deferrable:
  /* EMPTY */ { /* no error */ }
| DEFERRABLE { return unimplementedWithIssueDetail(sqllex, 31632, "deferrable") }
| DEFERRABLE INITIALLY DEFERRED { return unimplementedWithIssueDetail(sqllex, 31632, "def initially deferred") }
| DEFERRABLE INITIALLY IMMEDIATE { return unimplementedWithIssueDetail(sqllex, 31632, "def initially immediate") }
| INITIALLY DEFERRED { return unimplementedWithIssueDetail(sqllex, 31632, "initially deferred") }
| INITIALLY IMMEDIATE { return unimplementedWithIssueDetail(sqllex, 31632, "initially immediate") }

storing:
  COVERING
| STORING
| INCLUDE

// TODO(pmattis): It would be nice to support a syntax like STORING
// ALL or STORING (*). The syntax addition is straightforward, but we
// need to be careful with the rest of the implementation. In
// particular, columns stored at indexes are currently encoded in such
// a way that adding a new column would require rewriting the existing
// index values. We will need to change the storage format so that it
// is a list of <columnID, value> pairs which will allow both adding
// and dropping columns without rewriting indexes that are storing the
// adjusted column.
opt_storing:
  storing '(' name_list ')'
  {
    $$.val = $3.nameList()
  }
| /* EMPTY */
  {
    $$.val = tree.NameList(nil)
  }

opt_hash_sharded:
  USING HASH opt_hash_sharded_bucket_count
  {
    $$.val = &tree.ShardedIndexDef{
      ShardBuckets: $3.expr(),
    }
  }
  | /* EMPTY */
  {
    $$.val = (*tree.ShardedIndexDef)(nil)
  }

opt_hash_sharded_bucket_count:
  WITH_LA BUCKET_COUNT '=' a_expr
  {
    $$.val = $4.expr()
  }
  |
  {
    $$.val = tree.DefaultVal{}
  }

opt_column_list:
  '(' name_list ')'
  {
    $$.val = $2.nameList()
  }
| /* EMPTY */
  {
    $$.val = tree.NameList(nil)
  }

// https://www.postgresql.org/docs/10/sql-createtable.html
//
// "A value inserted into the referencing column(s) is matched against
// the values of the referenced table and referenced columns using the
// given match type. There are three match types: MATCH FULL, MATCH
// PARTIAL, and MATCH SIMPLE (which is the default). MATCH FULL will
// not allow one column of a multicolumn foreign key to be null unless
// all foreign key columns are null; if they are all null, the row is
// not required to have a match in the referenced table. MATCH SIMPLE
// allows any of the foreign key columns to be null; if any of them
// are null, the row is not required to have a match in the referenced
// table. MATCH PARTIAL is not yet implemented. (Of course, NOT NULL
// constraints can be applied to the referencing column(s) to prevent
// these cases from arising.)"
key_match:
  MATCH SIMPLE
  {
    $$.val = tree.MatchSimple
  }
| MATCH FULL
  {
    $$.val = tree.MatchFull
  }
| MATCH PARTIAL
  {
    return unimplementedWithIssueDetail(sqllex, 20305, "match partial")
  }
| /* EMPTY */
  {
    $$.val = tree.MatchSimple
  }

// We combine the update and delete actions into one value temporarily for
// simplicity of parsing, and then break them down again in the calling
// production.
reference_actions:
  reference_on_update
  {
     $$.val = tree.ReferenceActions{Update: $1.referenceAction()}
  }
| reference_on_delete
  {
     $$.val = tree.ReferenceActions{Delete: $1.referenceAction()}
  }
| reference_on_update reference_on_delete
  {
    $$.val = tree.ReferenceActions{Update: $1.referenceAction(), Delete: $2.referenceAction()}
  }
| reference_on_delete reference_on_update
  {
    $$.val = tree.ReferenceActions{Delete: $1.referenceAction(), Update: $2.referenceAction()}
  }
| /* EMPTY */
  {
    $$.val = tree.ReferenceActions{}
  }

reference_on_update:
  ON_LA UPDATE reference_action
  {
    $$.val = $3.referenceAction()
  }

reference_on_delete:
  ON_LA DELETE reference_action
  {
    $$.val = $3.referenceAction()
  }

reference_action:
// NO ACTION is currently the default behavior. It is functionally the same as
// RESTRICT.
  NO ACTION
  {
    $$.val = tree.NoAction
  }
| RESTRICT
  {
    $$.val = tree.Restrict
  }
| CASCADE
  {
    $$.val = tree.Cascade
  }
| SET NULL
  {
    $$.val = tree.SetNull
  }
| SET DEFAULT
  {
    $$.val = tree.SetDefault
  }

// %Help: CREATE SEQUENCE - create a new sequence
// %Category: DDL
// %Text:
// CREATE [TEMPORARY | TEMP] SEQUENCE <seqname>
//   [AS <typename>]
//   [INCREMENT <increment>]
//   [MINVALUE <minvalue> | NO MINVALUE]
//   [MAXVALUE <maxvalue> | NO MAXVALUE]
//   [START [WITH] <start>]
//   [CACHE <cache>]
//   [NO CYCLE]
//   [VIRTUAL]
//
// %SeeAlso: CREATE TABLE
create_sequence_stmt:
  CREATE opt_temp SEQUENCE sequence_name opt_sequence_option_list
  {
    name := $4.unresolvedObjectName().ToTableName()
    $$.val = &tree.CreateSequence {
      Name: name,
      Persistence: $2.persistence(),
      Options: $5.seqOpts(),
    }
  }
| CREATE opt_temp SEQUENCE IF NOT EXISTS sequence_name opt_sequence_option_list
  {
    name := $7.unresolvedObjectName().ToTableName()
    $$.val = &tree.CreateSequence {
      Name: name, Options: $8.seqOpts(),
      Persistence: $2.persistence(),
      IfNotExists: true,
    }
  }
| CREATE opt_temp SEQUENCE error // SHOW HELP: CREATE SEQUENCE

opt_sequence_option_list:
  sequence_option_list
| /* EMPTY */          { $$.val = []tree.SequenceOption(nil) }

sequence_option_list:
  sequence_option_elem                       { $$.val = []tree.SequenceOption{$1.seqOpt()} }
| sequence_option_list sequence_option_elem  { $$.val = append($1.seqOpts(), $2.seqOpt()) }

sequence_option_elem:
  AS typename                  {
                                  // Valid option values must be integer types (ex. int2, bigint)
                                  parsedType := $2.colType()
                                  if parsedType == nil {
                                      sqllex.(*lexer).lastError = pgerror.Newf(pgcode.UndefinedObject, "type %q does not exist", $2.val)
                                      sqllex.(*lexer).populateErrorDetails()
                                      return 1
                                  }
                                  if parsedType.Family() != types.IntFamily {
                                      sqllex.Error(fmt.Sprintf("invalid integer type: %s", parsedType.SQLString()))
                                      return 1
                                  }
                                  $$.val = tree.SequenceOption{Name: tree.SeqOptAs, AsIntegerType: parsedType}
                                }
| CYCLE                        { /* SKIP DOC */
                                 $$.val = tree.SequenceOption{Name: tree.SeqOptCycle} }
| NO CYCLE                     { $$.val = tree.SequenceOption{Name: tree.SeqOptNoCycle} }
| OWNED BY NONE                { $$.val = tree.SequenceOption{Name: tree.SeqOptOwnedBy, ColumnItemVal: nil} }
| OWNED BY column_path         { varName, err := $3.unresolvedName().NormalizeVarName()
                                     if err != nil {
                                       return setErr(sqllex, err)
                                     }
                                     columnItem, ok := varName.(*tree.ColumnItem)
                                     if !ok {
                                       sqllex.Error(fmt.Sprintf("invalid column name: %q", tree.ErrString($3.unresolvedName())))
                                             return 1
                                     }
                                 $$.val = tree.SequenceOption{Name: tree.SeqOptOwnedBy, ColumnItemVal: columnItem} }
| CACHE signed_iconst64        { x := $2.int64()
                                 $$.val = tree.SequenceOption{Name: tree.SeqOptCache, IntVal: &x} }
| PER NODE CACHE signed_iconst64  { x := $4.int64()
                                    $$.val = tree.SequenceOption{Name: tree.SeqOptCacheNode, IntVal: &x} }
| INCREMENT signed_iconst64    { x := $2.int64()
                                 $$.val = tree.SequenceOption{Name: tree.SeqOptIncrement, IntVal: &x} }
| INCREMENT BY signed_iconst64 { x := $3.int64()
                                 $$.val = tree.SequenceOption{Name: tree.SeqOptIncrement, IntVal: &x, OptionalWord: true} }
| MINVALUE signed_iconst64     { x := $2.int64()
                                 $$.val = tree.SequenceOption{Name: tree.SeqOptMinValue, IntVal: &x} }
| NO MINVALUE                  { $$.val = tree.SequenceOption{Name: tree.SeqOptMinValue} }
| MAXVALUE signed_iconst64     { x := $2.int64()
                                 $$.val = tree.SequenceOption{Name: tree.SeqOptMaxValue, IntVal: &x} }
| NO MAXVALUE                  { $$.val = tree.SequenceOption{Name: tree.SeqOptMaxValue} }
| START signed_iconst64        { x := $2.int64()
                                 $$.val = tree.SequenceOption{Name: tree.SeqOptStart, IntVal: &x} }
| START WITH signed_iconst64   { x := $3.int64()
                                 $$.val = tree.SequenceOption{Name: tree.SeqOptStart, IntVal: &x, OptionalWord: true} }
| RESTART                      { $$.val = tree.SequenceOption{Name: tree.SeqOptRestart} }
| RESTART signed_iconst64      { x := $2.int64()
                                 $$.val = tree.SequenceOption{Name: tree.SeqOptRestart, IntVal: &x} }
| RESTART WITH signed_iconst64 { x := $3.int64()
                                 $$.val = tree.SequenceOption{Name: tree.SeqOptRestart, IntVal: &x, OptionalWord: true} }

| VIRTUAL                      { $$.val = tree.SequenceOption{Name: tree.SeqOptVirtual} }

// %Help: TRUNCATE - empty one or more tables
// %Category: DML
// %Text: TRUNCATE [TABLE] <tablename> [, ...] [CASCADE | RESTRICT]
// %SeeAlso: WEBDOCS/truncate.html
truncate_stmt:
  TRUNCATE opt_table relation_expr_list opt_drop_behavior
  {
    $$.val = &tree.Truncate{Tables: $3.tableNames(), DropBehavior: $4.dropBehavior()}
  }
| TRUNCATE error // SHOW HELP: TRUNCATE

password_clause:
  ENCRYPTED PASSWORD sconst_or_placeholder
  {
    /* SKIP DOC */
    // This is a legacy postgres syntax.
    $$.val = tree.KVOption{Key: tree.Name($2), Value: $3.expr()}
  }
| PASSWORD sconst_or_placeholder
  {
    $$.val = tree.KVOption{Key: tree.Name($1), Value: $2.expr()}
  }
| PASSWORD NULL
  {
    $$.val = tree.KVOption{Key: tree.Name($1), Value: tree.DNull}
  }

// %Help: CREATE ROLE - define a new role
// %Category: Priv
// %Text: CREATE ROLE [IF NOT EXISTS] <name> [ [WITH] <OPTIONS...> ]
// %SeeAlso: ALTER ROLE, DROP ROLE, SHOW ROLES
create_role_stmt:
  CREATE role_or_group_or_user role_spec opt_role_options
  {
    $$.val = &tree.CreateRole{Name: $3.roleSpec(), KVOptions: $4.kvOptions(), IsRole: $2.bool()}
  }
| CREATE role_or_group_or_user IF NOT EXISTS role_spec opt_role_options
  {
    $$.val = &tree.CreateRole{Name: $6.roleSpec(), IfNotExists: true, KVOptions: $7.kvOptions(), IsRole: $2.bool()}
  }
| CREATE role_or_group_or_user error // SHOW HELP: CREATE ROLE

// %Help: ALTER ROLE - alter a role
// %Category: Priv
// %Text:
// ALTER ROLE <name> [WITH] <options...>
// ALTER ROLE { name | ALL } [ IN DATABASE database_name ] SET var { TO | = } { value | DEFAULT }
// ALTER ROLE { name | ALL } [ IN DATABASE database_name ] RESET { var | ALL }
// %SeeAlso: CREATE ROLE, DROP ROLE, SHOW ROLES
alter_role_stmt:
  ALTER role_or_group_or_user role_spec opt_role_options
{
  $$.val = &tree.AlterRole{Name: $3.roleSpec(), KVOptions: $4.kvOptions(), IsRole: $2.bool()}
}
| ALTER role_or_group_or_user IF EXISTS role_spec opt_role_options
{
  $$.val = &tree.AlterRole{Name: $5.roleSpec(), IfExists: true, KVOptions: $6.kvOptions(), IsRole: $2.bool()}
}
| ALTER role_or_group_or_user role_spec opt_in_database set_or_reset_clause
  {
    $$.val = &tree.AlterRoleSet{RoleName: $3.roleSpec(), DatabaseName: tree.Name($4), IsRole: $2.bool(), SetOrReset: $5.setVar()}
  }
| ALTER role_or_group_or_user IF EXISTS role_spec opt_in_database set_or_reset_clause
  {
    $$.val = &tree.AlterRoleSet{RoleName: $5.roleSpec(), IfExists: true, DatabaseName: tree.Name($6), IsRole: $2.bool(), SetOrReset: $7.setVar()}
  }
| ALTER ROLE_ALL ALL opt_in_database set_or_reset_clause
  {
    $$.val = &tree.AlterRoleSet{AllRoles: true, DatabaseName: tree.Name($4), IsRole: true, SetOrReset: $5.setVar()}
  }
| ALTER USER_ALL ALL opt_in_database set_or_reset_clause
  {
    $$.val = &tree.AlterRoleSet{AllRoles: true, DatabaseName: tree.Name($4), IsRole: false, SetOrReset: $5.setVar()}
  }
| ALTER role_or_group_or_user error // SHOW HELP: ALTER ROLE

opt_in_database:
  IN DATABASE database_name
  {
    $$ = $3
  }
| /* EMPTY */
  {
    $$ = ""
  }

// This rule is used when SET is used as a clause in another statement,
// like ALTER ROLE ... SET.
set_or_reset_clause:
  SET set_rest
  {
    $$.val = $2.setVar()
  }
| SET_TRACING set_rest
  {
    /* SKIP DOC */
    // We need to recognize the "set tracing" specially here since we do a
    // syntax lookahead and use a different token.
    $$.val = $2.setVar()
  }
| RESET_ALL ALL
  {
    $$.val = &tree.SetVar{ResetAll: true}
  }
| RESET session_var
  {
    $$.val = &tree.SetVar{Name: $2, Values:tree.Exprs{tree.DefaultVal{}}}
  }

// "CREATE GROUP is now an alias for CREATE ROLE"
// https://www.postgresql.org/docs/10/static/sql-creategroup.html
role_or_group_or_user:
  ROLE
  {
    $$.val = true
  }
| GROUP
  {
    /* SKIP DOC */
    $$.val = true
  }
| USER
  {
    $$.val = false
  }

// %Help: CREATE VIEW - create a new view
// %Category: DDL
// %Text:
// CREATE [TEMPORARY | TEMP] VIEW [IF NOT EXISTS] <viewname> [( <colnames...> )] AS <source>
// CREATE [TEMPORARY | TEMP] MATERIALIZED VIEW [IF NOT EXISTS] <viewname> [( <colnames...> )] AS <source> [WITH [NO] DATA]
// %SeeAlso: CREATE TABLE, SHOW CREATE, WEBDOCS/create-view.html
create_view_stmt:
  CREATE opt_temp opt_view_recursive VIEW view_name opt_column_list AS select_stmt
  {
    name := $5.unresolvedObjectName().ToTableName()
    $$.val = &tree.CreateView{
      Name: name,
      ColumnNames: $6.nameList(),
      AsSource: $8.slct(),
      Persistence: $2.persistence(),
      IfNotExists: false,
      Replace: false,
    }
  }
// We cannot use a rule like opt_or_replace here as that would cause a conflict
// with the opt_temp rule.
| CREATE OR REPLACE opt_temp opt_view_recursive VIEW view_name opt_column_list AS select_stmt
  {
    name := $7.unresolvedObjectName().ToTableName()
    $$.val = &tree.CreateView{
      Name: name,
      ColumnNames: $8.nameList(),
      AsSource: $10.slct(),
      Persistence: $4.persistence(),
      IfNotExists: false,
      Replace: true,
    }
  }
| CREATE opt_temp opt_view_recursive VIEW IF NOT EXISTS view_name opt_column_list AS select_stmt
  {
    name := $8.unresolvedObjectName().ToTableName()
    $$.val = &tree.CreateView{
      Name: name,
      ColumnNames: $9.nameList(),
      AsSource: $11.slct(),
      Persistence: $2.persistence(),
      IfNotExists: true,
      Replace: false,
    }
  }
| CREATE MATERIALIZED VIEW view_name opt_column_list AS select_stmt opt_with_data
  {
    name := $4.unresolvedObjectName().ToTableName()
    $$.val = &tree.CreateView{
      Name: name,
      ColumnNames: $5.nameList(),
      AsSource: $7.slct(),
      Materialized: true,
      WithData: $8.bool(),
    }
  }
| CREATE MATERIALIZED VIEW IF NOT EXISTS view_name opt_column_list AS select_stmt opt_with_data
  {
    name := $7.unresolvedObjectName().ToTableName()
    $$.val = &tree.CreateView{
      Name: name,
      ColumnNames: $8.nameList(),
      AsSource: $10.slct(),
      Materialized: true,
      IfNotExists: true,
      WithData: $11.bool(),
    }
  }
| CREATE opt_temp opt_view_recursive VIEW error // SHOW HELP: CREATE VIEW

opt_with_data:
  WITH NO DATA error
  {
    $$.val = false
  }
| WITH DATA
  {
    $$.val = true
  }
| /* EMPTY */
  {
    $$.val = true
  }

role_option:
  CREATEROLE
  {
    $$.val = tree.KVOption{Key: tree.Name($1), Value: nil}
  }
| NOCREATEROLE
  {
    $$.val = tree.KVOption{Key: tree.Name($1), Value: nil}
  }
| LOGIN
  {
    $$.val = tree.KVOption{Key: tree.Name($1), Value: nil}
  }
| NOLOGIN
  {
    $$.val = tree.KVOption{Key: tree.Name($1), Value: nil}
  }
| CONTROLJOB
  {
    $$.val = tree.KVOption{Key: tree.Name($1), Value: nil}
  }
| NOCONTROLJOB
  {
   $$.val = tree.KVOption{Key: tree.Name($1), Value: nil}
  }
| CONTROLCHANGEFEED
  {
    $$.val = tree.KVOption{Key: tree.Name($1), Value: nil}
  }
| NOCONTROLCHANGEFEED
  {
    $$.val = tree.KVOption{Key: tree.Name($1), Value: nil}
  }
| CREATEDB
  {
    $$.val = tree.KVOption{Key: tree.Name($1), Value: nil}
  }
| NOCREATEDB
  {
    $$.val = tree.KVOption{Key: tree.Name($1), Value: nil}
  }
| CREATELOGIN
  {
    $$.val = tree.KVOption{Key: tree.Name($1), Value: nil}
  }
| NOCREATELOGIN
  {
    $$.val = tree.KVOption{Key: tree.Name($1), Value: nil}
  }
| VIEWACTIVITY
  {
    $$.val = tree.KVOption{Key: tree.Name($1), Value: nil}
  }
| NOVIEWACTIVITY
  {
    $$.val = tree.KVOption{Key: tree.Name($1), Value: nil}
  }
| VIEWACTIVITYREDACTED
  {
    $$.val = tree.KVOption{Key: tree.Name($1), Value: nil}
  }
| NOVIEWACTIVITYREDACTED
  {
    $$.val = tree.KVOption{Key: tree.Name($1), Value: nil}
  }
| CANCELQUERY
  {
    $$.val = tree.KVOption{Key: tree.Name($1), Value: nil}
  }
| NOCANCELQUERY
  {
    $$.val = tree.KVOption{Key: tree.Name($1), Value: nil}
  }
| MODIFYCLUSTERSETTING
  {
    $$.val = tree.KVOption{Key: tree.Name($1), Value: nil}
  }
| NOMODIFYCLUSTERSETTING
  {
    $$.val = tree.KVOption{Key: tree.Name($1), Value: nil}
  }
| SQLLOGIN
  {
    $$.val = tree.KVOption{Key: tree.Name($1), Value: nil}
  }
| NOSQLLOGIN
  {
    $$.val = tree.KVOption{Key: tree.Name($1), Value: nil}
  }
| VIEWCLUSTERSETTING
  {
    $$.val = tree.KVOption{Key: tree.Name($1), Value: nil}
  }
| NOVIEWCLUSTERSETTING
  {
    $$.val = tree.KVOption{Key: tree.Name($1), Value: nil}
  }
| password_clause
| valid_until_clause
| subject_clause
| REPLICATION
  {
    $$.val = tree.KVOption{Key: tree.Name($1), Value: nil}
  }
| NOREPLICATION
  {
    $$.val = tree.KVOption{Key: tree.Name($1), Value: nil}
  }
| BYPASSRLS
  {
    $$.val = tree.KVOption{Key: tree.Name($1), Value: nil}
  }
| NOBYPASSRLS
  {
    $$.val = tree.KVOption{Key: tree.Name($1), Value: nil}
  }

role_options:
  role_option
  {
    $$.val = []tree.KVOption{$1.kvOption()}
  }
| role_options role_option
  {
    $$.val = append($1.kvOptions(), $2.kvOption())
  }

opt_role_options:
  opt_with role_options
  {
    $$.val = $2.kvOptions()
  }
| /* EMPTY */
  {
    $$.val = nil
  }

valid_until_clause:
  VALID UNTIL string_or_placeholder
  {
    $$.val = tree.KVOption{Key: tree.Name("valid until"), Value: $3.expr()}
  }
| VALID UNTIL NULL
  {
    $$.val = tree.KVOption{Key: tree.Name("valid until"), Value: tree.DNull}
  }

subject_clause:
  SUBJECT string_or_placeholder
  {
    $$.val = tree.KVOption{Key: tree.Name("subject"), Value: $2.expr()}
  }
| SUBJECT NULL
  {
    $$.val = tree.KVOption{Key: tree.Name("subject"), Value: tree.DNull}
  }

opt_view_recursive:
  /* EMPTY */ { /* no error */ }
| RECURSIVE { return unimplemented(sqllex, "create recursive view") }


// %Help: CREATE TYPE - create a type
// %Category: DDL
// %Text: CREATE TYPE [IF NOT EXISTS] <type_name> AS ENUM (...)
create_type_stmt:
  // Enum types.
  CREATE TYPE type_name AS ENUM '(' opt_enum_val_list ')'
  {
    $$.val = &tree.CreateType{
      TypeName: $3.unresolvedObjectName(),
      Variety: tree.Enum,
      EnumLabels: $7.enumValueList(),
    }
  }
| CREATE TYPE IF NOT EXISTS type_name AS ENUM '(' opt_enum_val_list ')'
  {
    $$.val = &tree.CreateType{
      TypeName: $6.unresolvedObjectName(),
      Variety: tree.Enum,
      EnumLabels: $10.enumValueList(),
      IfNotExists: true,
    }
  }
| CREATE TYPE error // SHOW HELP: CREATE TYPE
  // Record/Composite types.
| CREATE TYPE type_name AS '(' opt_composite_type_list ')'
  {
    $$.val = &tree.CreateType{
      TypeName: $3.unresolvedObjectName(),
      Variety: tree.Composite,
      CompositeTypeList: $6.compositeTypeList(),
    }
  }
| CREATE TYPE IF NOT EXISTS type_name AS '(' opt_composite_type_list ')'
  {
    $$.val = &tree.CreateType{
      TypeName: $6.unresolvedObjectName(),
      Variety: tree.Composite,
      IfNotExists: true,
      CompositeTypeList: $9.compositeTypeList(),
    }
  }
  // Range types.
| CREATE TYPE type_name AS RANGE error    { return unimplementedWithIssue(sqllex, 27791) }
  // Base (primitive) types.
| CREATE TYPE type_name '(' error         { return unimplementedWithIssueDetail(sqllex, 27793, "base") }
  // Shell types, gateway to define base types using the previous syntax.
| CREATE TYPE type_name                   { return unimplementedWithIssueDetail(sqllex, 27793, "shell") }
  // Domain types.
| CREATE DOMAIN type_name error           { return unimplementedWithIssueDetail(sqllex, 27796, "create") }

opt_enum_val_list:
  enum_val_list
  {
    $$.val = $1.enumValueList()
  }
| /* EMPTY */
  {
    $$.val = tree.EnumValueList(nil)
  }

enum_val_list:
  SCONST
  {
    $$.val = tree.EnumValueList{tree.EnumValue($1)}
  }
| enum_val_list ',' SCONST
  {
    $$.val = append($1.enumValueList(), tree.EnumValue($3))
  }

opt_composite_type_list:
  composite_type_list
  {
    $$.val = $1.compositeTypeList()
  }
| /* EMPTY */
  {
    $$.val = []tree.CompositeTypeElem{}
  }

composite_type_list:
  name simple_typename
  {
    $$.val = []tree.CompositeTypeElem{
        tree.CompositeTypeElem{
            Label: tree.Name($1),
            Type: $2.typeReference(),
        },
    }
  }
| composite_type_list ',' name simple_typename
  {
    $$.val = append($1.compositeTypeList(),
        tree.CompositeTypeElem{
            Label: tree.Name($3),
            Type: $4.typeReference(),
        },
    )
  }

// %Help: CREATE INDEX - create a new index
// %Category: DDL
// %Text:
// CREATE [UNIQUE | INVERTED | VECTOR] INDEX [CONCURRENTLY] [IF NOT EXISTS] [<idxname>]
//        ON <tablename> ( <colname> [ASC | DESC] [, ...] )
//        [USING HASH] [STORING ( <colnames...> )]
//        [PARTITION BY <partition params>]
//        [WITH <storage_parameter_list] [WHERE <where_conds...>]
//
// %SeeAlso: CREATE TABLE, SHOW INDEXES, SHOW CREATE,
// WEBDOCS/create-index.html
create_index_stmt:
  CREATE opt_unique INDEX opt_concurrently opt_index_name ON table_name opt_index_access_method '(' index_params ')' opt_hash_sharded opt_storing opt_partition_by_index opt_with_storage_parameter_list opt_where_clause opt_index_visible
  {
    table := $7.unresolvedObjectName().ToTableName()
    indexType := $8.indexType()
    $$.val = &tree.CreateIndex{
      Name:             tree.Name($5),
      Table:            table,
      Unique:           $2.bool(),
      Columns:          $10.idxElems(),
      Sharded:          $12.shardedIndexDef(),
      Storing:          $13.nameList(),
      PartitionByIndex: $14.partitionByIndex(),
      StorageParams:    $15.storageParams(),
      Predicate:        $16.expr(),
      Type:             indexType,
      Concurrently:     $4.bool(),
      Invisibility:     $17.indexInvisibility(),
    }
  }
| CREATE opt_unique INDEX opt_concurrently IF NOT EXISTS index_name ON table_name opt_index_access_method '(' index_params ')' opt_hash_sharded opt_storing opt_partition_by_index opt_with_storage_parameter_list opt_where_clause opt_index_visible
  {
    table := $10.unresolvedObjectName().ToTableName()
    indexType := $11.indexType()
    $$.val = &tree.CreateIndex{
      Name:             tree.Name($8),
      Table:            table,
      Unique:           $2.bool(),
      IfNotExists:      true,
      Columns:          $13.idxElems(),
      Sharded:          $15.shardedIndexDef(),
      Storing:          $16.nameList(),
      PartitionByIndex: $17.partitionByIndex(),
      Type:             indexType,
      StorageParams:    $18.storageParams(),
      Predicate:        $19.expr(),
      Concurrently:     $4.bool(),
      Invisibility:     $20.indexInvisibility(),
    }
  }
| CREATE opt_unique INVERTED INDEX opt_concurrently opt_index_name ON table_name '(' index_params ')' opt_storing opt_partition_by_index opt_with_storage_parameter_list opt_where_clause opt_index_visible
  {
    table := $8.unresolvedObjectName().ToTableName()
    $$.val = &tree.CreateIndex{
      Name:             tree.Name($6),
      Table:            table,
      Unique:           $2.bool(),
      Type:             idxtype.INVERTED,
      Columns:          $10.idxElems(),
      Storing:          $12.nameList(),
      PartitionByIndex: $13.partitionByIndex(),
      StorageParams:    $14.storageParams(),
      Predicate:        $15.expr(),
      Concurrently:     $5.bool(),
      Invisibility:     $16.indexInvisibility(),
    }
  }
| CREATE opt_unique INVERTED INDEX opt_concurrently IF NOT EXISTS index_name ON table_name '(' index_params ')' opt_storing opt_partition_by_index opt_with_storage_parameter_list opt_where_clause opt_index_visible
  {
    table := $11.unresolvedObjectName().ToTableName()
    $$.val = &tree.CreateIndex{
      Name:             tree.Name($9),
      Table:            table,
      Unique:           $2.bool(),
      Type:             idxtype.INVERTED,
      IfNotExists:      true,
      Columns:          $13.idxElems(),
      Storing:          $15.nameList(),
      PartitionByIndex: $16.partitionByIndex(),
      StorageParams:    $17.storageParams(),
      Predicate:        $18.expr(),
      Concurrently:     $5.bool(),
      Invisibility:     $19.indexInvisibility(),
    }
  }
| CREATE opt_unique VECTOR INDEX opt_concurrently opt_index_name ON table_name '(' index_params ')' opt_storing opt_partition_by_index opt_with_storage_parameter_list opt_where_clause opt_index_visible
  {
    table := $8.unresolvedObjectName().ToTableName()
    $$.val = &tree.CreateIndex{
      Name:             tree.Name($6),
      Table:            table,
      Unique:           $2.bool(),
      Type:             idxtype.VECTOR,
      Columns:          $10.idxElems(),
      Storing:          $12.nameList(),
      PartitionByIndex: $13.partitionByIndex(),
      StorageParams:    $14.storageParams(),
      Predicate:        $15.expr(),
      Concurrently:     $5.bool(),
      Invisibility:     $16.indexInvisibility(),
    }
  }
| CREATE opt_unique VECTOR INDEX opt_concurrently IF NOT EXISTS index_name ON table_name '(' index_params ')' opt_storing opt_partition_by_index opt_with_storage_parameter_list opt_where_clause opt_index_visible
  {
    table := $11.unresolvedObjectName().ToTableName()
    $$.val = &tree.CreateIndex{
      Name:             tree.Name($9),
      Table:            table,
      Unique:           $2.bool(),
      Type:             idxtype.VECTOR,
      IfNotExists:      true,
      Columns:          $13.idxElems(),
      Storing:          $15.nameList(),
      PartitionByIndex: $16.partitionByIndex(),
      StorageParams:    $17.storageParams(),
      Predicate:        $18.expr(),
      Concurrently:     $5.bool(),
      Invisibility:     $19.indexInvisibility(),
    }
  }
| CREATE opt_unique INDEX error // SHOW HELP: CREATE INDEX

opt_index_access_method:
  USING name
  {
    /* FORCE DOC */
    var val idxtype.T
    switch $2 {
      case "gin", "gist":
        val = idxtype.INVERTED
      case "btree":
        val = idxtype.FORWARD
      case "cspann":
        val = idxtype.VECTOR
      case "hash", "spgist", "brin":
        return unimplemented(sqllex, "index using " + $2)
      default:
        sqllex.Error("unrecognized access method: " + $2)
        return 1
    }
    $$.val = val
  }
| /* EMPTY */
  {
    $$.val = idxtype.FORWARD
  }

opt_concurrently:
  CONCURRENTLY
  {
    $$.val = true
  }
| /* EMPTY */
  {
    $$.val = false
  }

opt_unique:
  UNIQUE
  {
    $$.val = true
  }
| /* EMPTY */
  {
    $$.val = false
  }

index_params:
  index_elem
  {
    $$.val = tree.IndexElemList{$1.idxElem()}
  }
| index_params ',' index_elem
  {
    $$.val = append($1.idxElems(), $3.idxElem())
  }

// Index attributes can be either simple column references, or arbitrary
// expressions in parens. For backwards-compatibility reasons, we allow an
// expression that is just a function call to be written without parens.
index_elem:
  func_expr_windowless index_elem_options
  {
    e := $2.idxElem()
    e.Expr = $1.expr()
    $$.val = e
  }
| '(' a_expr ')' index_elem_options
  {
    e := $4.idxElem()
    e.Expr = $2.expr()
    $$.val = e
  }
| name index_elem_options
  {
    e := $2.idxElem()
    e.Column = tree.Name($1)
    $$.val = e
  }

index_elem_options:
  opt_class opt_asc_desc opt_nulls_order
  {
    /* FORCE DOC */
    opClass := $1
    dir := $2.dir()
    nullsOrder := $3.nullsOrder()
    // We currently only support the opposite of Postgres defaults.
    if nullsOrder != tree.DefaultNullsOrder {
      if dir == tree.Descending && nullsOrder == tree.NullsFirst {
        return unimplementedWithIssue(sqllex, 6224)
      }
      if dir != tree.Descending && nullsOrder == tree.NullsLast {
        return unimplementedWithIssue(sqllex, 6224)
      }
    }
    $$.val = tree.IndexElem{Direction: dir, NullsOrder: nullsOrder, OpClass: tree.Name(opClass)}
  }

opt_class:
  name { $$ = $1 }
| /* EMPTY */ { $$ = "" }

opt_collate:
  COLLATE collation_name { $$ = $2 }
| /* EMPTY */ { $$ = "" }

opt_asc_desc:
  ASC
  {
    $$.val = tree.Ascending
  }
| DESC
  {
    $$.val = tree.Descending
  }
| /* EMPTY */
  {
    $$.val = tree.DefaultDirection
  }

opt_index_visible:
  NOT VISIBLE
  {
    $$.val = tree.IndexInvisibility{Value: 1.0}
  }
| INVISIBLE
  {
    $$.val = tree.IndexInvisibility{Value: 1.0}
  }
| VISIBLE
  {
    $$.val = tree.IndexInvisibility{Value: 0.0}
  }
| VISIBILITY FCONST
  {
    visibilityConst, _ := constant.Float64Val($2.numVal().AsConstantValue())
      if visibilityConst < 0.0 || visibilityConst > 1.0 {
        sqllex.Error("index visibility must be between 0 and 1")
        return 1
      }
    invisibilityConst := 1.0 - visibilityConst
    $$.val = tree.IndexInvisibility{Value: invisibilityConst, FloatProvided: true}
  }
| /* EMPTY */
  {
    $$.val = tree.IndexInvisibility{Value: 0.0}
  }

alter_database_to_schema_stmt:
  ALTER DATABASE database_name CONVERT TO SCHEMA WITH PARENT database_name
  {
    $$.val = &tree.ReparentDatabase{Name: tree.Name($3), Parent: tree.Name($9)}
  }

alter_rename_database_stmt:
  ALTER DATABASE database_name RENAME TO database_name
  {
    $$.val = &tree.RenameDatabase{Name: tree.Name($3), NewName: tree.Name($6)}
  }

alter_rename_table_stmt:
  ALTER TABLE relation_expr RENAME TO table_name
  {
    name := $3.unresolvedObjectName()
    newName := $6.unresolvedObjectName()
    $$.val = &tree.RenameTable{Name: name, NewName: newName, IfExists: false, IsView: false}
  }
| ALTER TABLE IF EXISTS relation_expr RENAME TO table_name
  {
    name := $5.unresolvedObjectName()
    newName := $8.unresolvedObjectName()
    $$.val = &tree.RenameTable{Name: name, NewName: newName, IfExists: true, IsView: false}
  }

alter_table_set_schema_stmt:
  ALTER TABLE relation_expr SET SCHEMA schema_name
   {
     $$.val = &tree.AlterTableSetSchema{
       Name: $3.unresolvedObjectName(), Schema: tree.Name($6), IfExists: false,
     }
   }
| ALTER TABLE IF EXISTS relation_expr SET SCHEMA schema_name
  {
    $$.val = &tree.AlterTableSetSchema{
      Name: $5.unresolvedObjectName(), Schema: tree.Name($8), IfExists: true,
    }
  }

alter_table_locality_stmt:
  ALTER TABLE relation_expr SET locality
  {
    $$.val = &tree.AlterTableLocality{
      Name: $3.unresolvedObjectName(),
      Locality: $5.locality(),
      IfExists: false,
    }
  }
| ALTER TABLE IF EXISTS relation_expr SET locality
  {
    $$.val = &tree.AlterTableLocality{
      Name: $5.unresolvedObjectName(),
      Locality: $7.locality(),
      IfExists: true,
    }
  }

locality:
  LOCALITY GLOBAL
  {
    $$.val = &tree.Locality{
      LocalityLevel: tree.LocalityLevelGlobal,
    }
  }
| LOCALITY REGIONAL BY TABLE IN region_name
  {
    $$.val = &tree.Locality{
      TableRegion: tree.Name($6),
      LocalityLevel: tree.LocalityLevelTable,
    }
  }
| LOCALITY REGIONAL BY TABLE IN PRIMARY REGION
  {
    $$.val = &tree.Locality{
      LocalityLevel: tree.LocalityLevelTable,
    }
  }
| LOCALITY REGIONAL BY TABLE
  {
    $$.val = &tree.Locality{
      LocalityLevel: tree.LocalityLevelTable,
    }
  }
| LOCALITY REGIONAL IN region_name
  {
    $$.val = &tree.Locality{
      TableRegion: tree.Name($4),
      LocalityLevel: tree.LocalityLevelTable,
    }
  }
| LOCALITY REGIONAL IN PRIMARY REGION
  {
    $$.val = &tree.Locality{
      LocalityLevel: tree.LocalityLevelTable,
    }
  }
| LOCALITY REGIONAL
  {
    $$.val = &tree.Locality{
      LocalityLevel: tree.LocalityLevelTable,
    }
  }
| LOCALITY REGIONAL BY ROW
  {
    $$.val = &tree.Locality{
      LocalityLevel: tree.LocalityLevelRow,
    }
  }
| LOCALITY REGIONAL BY ROW AS name
  {
    $$.val = &tree.Locality{
      LocalityLevel: tree.LocalityLevelRow,
      RegionalByRowColumn: tree.Name($6),
    }
  }

alter_table_owner_stmt:
  ALTER TABLE relation_expr OWNER TO role_spec
  {
    $$.val = &tree.AlterTableOwner{
      Name: $3.unresolvedObjectName(),
      Owner: $6.roleSpec(),
      IfExists: false,
    }
  }
| ALTER TABLE IF EXISTS relation_expr OWNER TO role_spec
  {
    $$.val = &tree.AlterTableOwner{
      Name: $5.unresolvedObjectName(),
      Owner: $8.roleSpec(),
      IfExists: true,
    }
  }

alter_view_set_schema_stmt:
	ALTER VIEW relation_expr SET SCHEMA schema_name
	 {
		 $$.val = &tree.AlterTableSetSchema{
			 Name: $3.unresolvedObjectName(), Schema: tree.Name($6), IfExists: false, IsView: true,
		 }
	 }
| ALTER MATERIALIZED VIEW relation_expr SET SCHEMA schema_name
	 {
		 $$.val = &tree.AlterTableSetSchema{
			 Name: $4.unresolvedObjectName(),
			 Schema: tree.Name($7),
			 IfExists: false,
			 IsView: true,
			 IsMaterialized: true,
		 }
	 }
| ALTER VIEW IF EXISTS relation_expr SET SCHEMA schema_name
	{
		$$.val = &tree.AlterTableSetSchema{
			Name: $5.unresolvedObjectName(), Schema: tree.Name($8), IfExists: true, IsView: true,
		}
	}
| ALTER MATERIALIZED VIEW IF EXISTS relation_expr SET SCHEMA schema_name
	{
		$$.val = &tree.AlterTableSetSchema{
			Name: $6.unresolvedObjectName(),
			Schema: tree.Name($9),
			IfExists: true,
			IsView: true,
			IsMaterialized: true,
		}
	}

alter_view_owner_stmt:
	ALTER VIEW relation_expr OWNER TO role_spec
  {
    $$.val = &tree.AlterTableOwner{
      Name: $3.unresolvedObjectName(),
      Owner: $6.roleSpec(),
      IfExists: false,
      IsView: true,
    }
  }
| ALTER MATERIALIZED VIEW relation_expr OWNER TO role_spec
  {
    $$.val = &tree.AlterTableOwner{
      Name: $4.unresolvedObjectName(),
      Owner: $7.roleSpec(),
      IfExists: false,
      IsView: true,
      IsMaterialized: true,
    }
  }
| ALTER VIEW IF EXISTS relation_expr OWNER TO role_spec
  {
    $$.val = &tree.AlterTableOwner{
      Name: $5.unresolvedObjectName(),
      Owner: $8.roleSpec(),
      IfExists: true,
      IsView: true,
    }
  }
| ALTER MATERIALIZED VIEW IF EXISTS relation_expr OWNER TO role_spec
  {
    $$.val = &tree.AlterTableOwner{
      Name: $6.unresolvedObjectName(),
      Owner: $9.roleSpec(),
      IfExists: true,
      IsView: true,
      IsMaterialized: true,
    }
  }

alter_sequence_set_schema_stmt:
	ALTER SEQUENCE relation_expr SET SCHEMA schema_name
	 {
		 $$.val = &tree.AlterTableSetSchema{
			 Name: $3.unresolvedObjectName(), Schema: tree.Name($6), IfExists: false, IsSequence: true,
		 }
	 }
| ALTER SEQUENCE IF EXISTS relation_expr SET SCHEMA schema_name
	{
		$$.val = &tree.AlterTableSetSchema{
			Name: $5.unresolvedObjectName(), Schema: tree.Name($8), IfExists: true, IsSequence: true,
		}
	}

alter_sequence_owner_stmt:
	ALTER SEQUENCE relation_expr OWNER TO role_spec
	{
		$$.val = &tree.AlterTableOwner{
			Name: $3.unresolvedObjectName(),
			Owner: $6.roleSpec(),
			IfExists: false,
			IsSequence: true,
		}
	}
| ALTER SEQUENCE IF EXISTS relation_expr OWNER TO role_spec
	{
		$$.val = &tree.AlterTableOwner{
			Name: $5.unresolvedObjectName(),
			Owner: $8.roleSpec(),
			IfExists: true,
			IsSequence: true,
		}
	}

alter_rename_view_stmt:
  ALTER VIEW relation_expr RENAME TO view_name
  {
    name := $3.unresolvedObjectName()
    newName := $6.unresolvedObjectName()
    $$.val = &tree.RenameTable{Name: name, NewName: newName, IfExists: false, IsView: true}
  }
| ALTER MATERIALIZED VIEW relation_expr RENAME TO view_name
  {
    name := $4.unresolvedObjectName()
    newName := $7.unresolvedObjectName()
    $$.val = &tree.RenameTable{
      Name: name,
      NewName: newName,
      IfExists: false,
      IsView: true,
      IsMaterialized: true,
    }
  }
| ALTER VIEW IF EXISTS relation_expr RENAME TO view_name
  {
    name := $5.unresolvedObjectName()
    newName := $8.unresolvedObjectName()
    $$.val = &tree.RenameTable{Name: name, NewName: newName, IfExists: true, IsView: true}
  }
| ALTER MATERIALIZED VIEW IF EXISTS relation_expr RENAME TO view_name
  {
    name := $6.unresolvedObjectName()
    newName := $9.unresolvedObjectName()
    $$.val = &tree.RenameTable{
      Name: name,
      NewName: newName,
      IfExists: true,
      IsView: true,
      IsMaterialized: true,
    }
  }

alter_rename_sequence_stmt:
  ALTER SEQUENCE relation_expr RENAME TO sequence_name
  {
    name := $3.unresolvedObjectName()
    newName := $6.unresolvedObjectName()
    $$.val = &tree.RenameTable{Name: name, NewName: newName, IfExists: false, IsSequence: true}
  }
| ALTER SEQUENCE IF EXISTS relation_expr RENAME TO sequence_name
  {
    name := $5.unresolvedObjectName()
    newName := $8.unresolvedObjectName()
    $$.val = &tree.RenameTable{Name: name, NewName: newName, IfExists: true, IsSequence: true}
  }

alter_rename_index_stmt:
  ALTER INDEX table_index_name RENAME TO index_name
  {
    $$.val = &tree.RenameIndex{Index: $3.newTableIndexName(), NewName: tree.UnrestrictedName($6), IfExists: false}
  }
| ALTER INDEX IF EXISTS table_index_name RENAME TO index_name
  {
    $$.val = &tree.RenameIndex{Index: $5.newTableIndexName(), NewName: tree.UnrestrictedName($8), IfExists: true}
  }

// %Help: ALTER DEFAULT PRIVILEGES - alter default privileges on an object
// %Category: DDL
// %Text:
//
// Commands:
//   ALTER DEFAULT PRIVILEGES [ FOR { ROLE | USER } target_roles... ] [ IN SCHEMA schema_name...] abbreviated_grant_or_revoke
alter_default_privileges_stmt:
 ALTER DEFAULT PRIVILEGES opt_for_roles opt_in_schemas abbreviated_grant_stmt
 {
   $$.val = &tree.AlterDefaultPrivileges{
     Roles: $4.roleSpecList(),
     Schemas: $5.objectNamePrefixList(),
     Grant: $6.abbreviatedGrant(),
     IsGrant: true,
   }
 }
| ALTER DEFAULT PRIVILEGES opt_for_roles opt_in_schemas abbreviated_revoke_stmt
 {
   $$.val = &tree.AlterDefaultPrivileges{
     Roles: $4.roleSpecList(),
     Schemas: $5.objectNamePrefixList(),
     Revoke: $6.abbreviatedRevoke(),
     IsGrant: false,
   }
 }
| ALTER DEFAULT PRIVILEGES FOR ALL ROLES opt_in_schemas abbreviated_grant_stmt
 {
   $$.val = &tree.AlterDefaultPrivileges{
     ForAllRoles: true,
     Schemas: $7.objectNamePrefixList(),
     Grant: $8.abbreviatedGrant(),
     IsGrant: true,
  }
 }
| ALTER DEFAULT PRIVILEGES FOR ALL ROLES opt_in_schemas abbreviated_revoke_stmt
 {
   $$.val = &tree.AlterDefaultPrivileges{
     ForAllRoles: true,
     Schemas: $7.objectNamePrefixList(),
     Revoke: $8.abbreviatedRevoke(),
     IsGrant: false,
  }
 }
| ALTER DEFAULT PRIVILEGES error // SHOW HELP: ALTER DEFAULT PRIVILEGES

abbreviated_grant_stmt:
  GRANT privileges ON target_object_type TO role_spec_list opt_with_grant_option
  {
    $$.val = tree.AbbreviatedGrant{
      Privileges: $2.privilegeList(),
      Target: $4.targetObjectType(),
      Grantees: $6.roleSpecList(),
      WithGrantOption: $7.bool(),
    }
  }

opt_with_grant_option:
 WITH GRANT OPTION
  {
    $$.val = true
  }
| /* EMPTY */
  {
    $$.val = false
  }

abbreviated_revoke_stmt:
  REVOKE privileges ON target_object_type FROM role_spec_list opt_drop_behavior
  {
    $$.val = tree.AbbreviatedRevoke{
      Privileges: $2.privilegeList(),
      Target: $4.targetObjectType(),
      Grantees: $6.roleSpecList(),
    }
  }
| REVOKE GRANT OPTION FOR privileges ON target_object_type FROM role_spec_list opt_drop_behavior
  {
    $$.val = tree.AbbreviatedRevoke{
      Privileges: $5.privilegeList(),
      Target: $7.targetObjectType(),
      Grantees: $9.roleSpecList(),
      GrantOptionFor: true,
    }
  }

target_object_type:
  TABLES
  {
    $$.val = privilege.Tables
  }
| SEQUENCES
  {
    $$.val = privilege.Sequences
  }
| TYPES
  {
    $$.val = privilege.Types
  }
| SCHEMAS
  {
    $$.val = privilege.Schemas
  }
| FUNCTIONS
  {
    $$.val = privilege.Routines
  }
| ROUTINES error
  {
    return unimplemented(sqllex, "ALTER DEFAULT PRIVILEGES ... ON ROUTINES ...")
  }

opt_for_roles:
 FOR role_or_group_or_user role_spec_list
 {
   $$.val = $3.roleSpecList()
 }
| /* EMPTY */ {
   $$.val = tree.RoleSpecList(nil)
}

opt_in_schema:
 IN SCHEMA schema_name
 {
   $$ = $3
 }
| /* EMPTY */
 {
   $$ = ""
 }


opt_in_schemas:
 IN SCHEMA schema_name_list
 {
   $$.val = $3.objectNamePrefixList()
 }
| /* EMPTY */
 {
   $$.val = tree.ObjectNamePrefixList{}
 }

opt_column:
  COLUMN {}
| /* EMPTY */ {}

opt_set_data:
  SET DATA {}
| /* EMPTY */ {}

// %Help: RELEASE - complete a sub-transaction
// %Category: Txn
// %Text: RELEASE [SAVEPOINT] <savepoint name>
// %SeeAlso: SAVEPOINT, WEBDOCS/savepoint.html
release_stmt:
  RELEASE savepoint_name
  {
    $$.val = &tree.ReleaseSavepoint{Savepoint: tree.Name($2)}
  }
| RELEASE error // SHOW HELP: RELEASE

// %Help: RESUME JOBS - resume selected background jobs
// %Category: Misc
// %Text:
// RESUME JOBS <selectclause>
// RESUME JOB <jobid>
// %SeeAlso: SHOW JOBS, CANCEL JOBS, PAUSE JOBS
resume_jobs_stmt:
  RESUME JOB a_expr
  {
    $$.val = &tree.ControlJobs{
      Jobs: &tree.Select{
        Select: &tree.ValuesClause{Rows: []tree.Exprs{tree.Exprs{$3.expr()}}},
      },
      Command: tree.ResumeJob,
    }
  }
| RESUME JOB error // SHOW HELP: RESUME JOBS
| RESUME JOBS select_stmt
  {
    $$.val = &tree.ControlJobs{Jobs: $3.slct(), Command: tree.ResumeJob}
  }
| RESUME JOBS for_schedules_clause
  {
    $$.val = &tree.ControlJobsForSchedules{Schedules: $3.slct(), Command: tree.ResumeJob}
  }
| RESUME JOBS error // SHOW HELP: RESUME JOBS

// %Help: RESUME SCHEDULES - resume executing scheduled jobs
// %Category: Misc
// %Text:
// RESUME SCHEDULES <selectclause>
//  selectclause: select statement returning schedule IDs to resume.
//
// RESUME SCHEDULE <scheduleID>
//
// %SeeAlso: PAUSE SCHEDULES, SHOW JOBS, RESUME JOBS
resume_schedules_stmt:
  RESUME SCHEDULE a_expr
  {
    $$.val = &tree.ControlSchedules{
      Schedules: &tree.Select{
        Select: &tree.ValuesClause{Rows: []tree.Exprs{tree.Exprs{$3.expr()}}},
      },
      Command: tree.ResumeSchedule,
    }
  }
| RESUME SCHEDULE error // SHOW HELP: RESUME SCHEDULES
| RESUME SCHEDULES select_stmt
  {
    $$.val = &tree.ControlSchedules{
      Schedules: $3.slct(),
      Command: tree.ResumeSchedule,
    }
  }
| RESUME SCHEDULES error // SHOW HELP: RESUME SCHEDULES

// %Help: DROP SCHEDULES - destroy specified schedules
// %Category: Misc
// %Text:
// DROP SCHEDULES <selectclause>
//  selectclause: select statement returning schedule IDs to resume.
//
// DROP SCHEDULE <scheduleID>
//
// %SeeAlso: PAUSE SCHEDULES, SHOW JOBS, CANCEL JOBS
drop_schedule_stmt:
  DROP SCHEDULE a_expr
  {
    $$.val = &tree.ControlSchedules{
      Schedules: &tree.Select{
        Select: &tree.ValuesClause{Rows: []tree.Exprs{tree.Exprs{$3.expr()}}},
      },
      Command: tree.DropSchedule,
    }
  }
| DROP SCHEDULE error // SHOW HELP: DROP SCHEDULES
| DROP SCHEDULES select_stmt
  {
    $$.val = &tree.ControlSchedules{
      Schedules: $3.slct(),
      Command: tree.DropSchedule,
    }
  }
| DROP SCHEDULES error // SHOW HELP: DROP SCHEDULES

// %Help: SAVEPOINT - start a sub-transaction
// %Category: Txn
// %Text: SAVEPOINT <savepoint name>
// %SeeAlso: RELEASE, WEBDOCS/savepoint.html
savepoint_stmt:
  SAVEPOINT name
  {
    $$.val = &tree.Savepoint{Name: tree.Name($2)}
  }
| SAVEPOINT error // SHOW HELP: SAVEPOINT

// BEGIN / START / COMMIT / END / ROLLBACK / PREPARE TRANSACTION / COMMIT PREPARED / ROLLBACK PREPARED / ...
transaction_stmt:
  begin_stmt               // EXTEND WITH HELP: BEGIN
| commit_stmt              // EXTEND WITH HELP: COMMIT
| rollback_stmt            // EXTEND WITH HELP: ROLLBACK
| abort_stmt               /* SKIP DOC */
| prepare_transaction_stmt // EXTEND WITH HELP: PREPARE TRANSACTION
| commit_prepared_stmt     // EXTEND WITH HELP: COMMIT PREPARED
| rollback_prepared_stmt   // EXTEND WITH HELP: ROLLBACK PREPARED

// %Help: BEGIN - start a transaction
// %Category: Txn
// %Text:
// BEGIN [TRANSACTION] [ <txnparameter> [[,] ...] ]
// START TRANSACTION [ <txnparameter> [[,] ...] ]
//
// Transaction parameters:
//    ISOLATION LEVEL { READ COMMITTED | REPEATABLE READ | SERIALIZABLE }
//    PRIORITY { LOW | NORMAL | HIGH }
//
// %SeeAlso: COMMIT, ROLLBACK, WEBDOCS/begin-transaction.html
begin_stmt:
  START TRANSACTION begin_transaction
  {
    s := $3.beginTransaction()
    s.FormatWithStart = true
    $$.val = s
  }
| START error // SHOW HELP: BEGIN

// %Help: COMMIT - commit the current transaction
// %Category: Txn
// %Text:
// COMMIT [TRANSACTION]
// END [TRANSACTION]
// %SeeAlso: BEGIN, ROLLBACK, WEBDOCS/commit-transaction.html
commit_stmt:
  COMMIT opt_transaction
  {
    $$.val = &tree.CommitTransaction{}
  }
| COMMIT error // SHOW HELP: COMMIT

abort_stmt:
  ABORT opt_abort_mod
  {
    $$.val = &tree.RollbackTransaction{}
  }

opt_abort_mod:
  TRANSACTION {}
| WORK        {}
| /* EMPTY */ {}

// %Help: ROLLBACK - abort the current (sub-)transaction
// %Category: Txn
// %Text:
// ROLLBACK [TRANSACTION]
// ROLLBACK [TRANSACTION] TO [SAVEPOINT] <savepoint name>
// %SeeAlso: BEGIN, COMMIT, SAVEPOINT, WEBDOCS/rollback-transaction.html
rollback_stmt:
  ROLLBACK opt_transaction
  {
     $$.val = &tree.RollbackTransaction{}
  }
| ROLLBACK opt_transaction TO savepoint_name
  {
     $$.val = &tree.RollbackToSavepoint{Savepoint: tree.Name($4)}
  }
| ROLLBACK error // SHOW HELP: ROLLBACK

// "legacy" here doesn't mean we're deprecating the syntax. We inherit this
// concept from postgres. The idea is to avoid conflicts in "CREATE FUNCTION"'s
// "BEGIN ATOMIC...END" function body context.
legacy_transaction_stmt:
  legacy_begin_stmt // EXTEND WITH HELP: BEGIN
| legacy_end_stmt // EXTEND WITH HELP: COMMIT

legacy_begin_stmt:
  BEGIN opt_transaction begin_transaction
  {
    $$.val = $3.stmt()
  }
| BEGIN error // SHOW HELP: BEGIN

legacy_end_stmt:
  END opt_transaction
  {
    $$.val = &tree.CommitTransaction{}
  }
| END error // SHOW HELP: COMMIT


opt_transaction:
  TRANSACTION {}
| /* EMPTY */ {}

savepoint_name:
  SAVEPOINT name
  {
    $$ = $2
  }
| name
  {
    $$ = $1
  }

begin_transaction:
  transaction_mode_list
  {
    $$.val = &tree.BeginTransaction{Modes: $1.transactionModes()}
  }
| /* EMPTY */
  {
    $$.val = &tree.BeginTransaction{}
  }

transaction_mode_list:
  transaction_mode
  {
    $$.val = $1.transactionModes()
  }
| transaction_mode_list opt_comma transaction_mode
  {
    a := $1.transactionModes()
    b := $3.transactionModes()
    err := a.Merge(b)
    if err != nil { return setErr(sqllex, err) }
    $$.val = a
  }

// The transaction mode list after BEGIN should use comma-separated
// modes as per the SQL standard, but PostgreSQL historically allowed
// them to be listed without commas too.
opt_comma:
  ','
  { }
| /* EMPTY */
  { }

transaction_mode:
  transaction_iso_level
  {
    $$.val = tree.TransactionModes{Isolation: $1.isoLevel()}
  }
| transaction_user_priority
  {
    $$.val = tree.TransactionModes{UserPriority: $1.userPriority()}
  }
| transaction_read_mode
  {
    $$.val = tree.TransactionModes{ReadWriteMode: $1.readWriteMode()}
  }
| as_of_clause
  {
    $$.val = tree.TransactionModes{AsOf: $1.asOfClause()}
  }
| transaction_deferrable_mode
  {
    $$.val = tree.TransactionModes{Deferrable: $1.deferrableMode()}
  }

transaction_user_priority:
  PRIORITY user_priority
  {
    $$.val = $2.userPriority()
  }

transaction_iso_level:
  ISOLATION LEVEL iso_level
  {
    $$.val = $3.isoLevel()
  }

transaction_read_mode:
  READ ONLY
  {
    $$.val = tree.ReadOnly
  }
| READ WRITE
  {
    $$.val = tree.ReadWrite
  }

transaction_deferrable_mode:
  DEFERRABLE
  {
    $$.val = tree.Deferrable
  }
| NOT DEFERRABLE
  {
    $$.val = tree.NotDeferrable
  }

// %Help: PREPARE TRANSACTION - prepare the current transaction for two-phase commit
// %Category: Txn
// %Text: PREPARE TRANSACTION <transaction-id>
// %SeeAlso: COMMIT PREPARED, ROLLBACK PREPARED
prepare_transaction_stmt:
  PREPARE TRANSACTION SCONST
  {
    $$.val = &tree.PrepareTransaction{Transaction: tree.NewStrVal($3)}
  }

// %Help: COMMIT PREPARED - commit the named transaction as part of two-phase commit
// %Category: Txn
// %Text: COMMIT PREPARED <transaction-id>
// %SeeAlso: PREPARE TRANSACTION, ROLLBACK PREPARED
commit_prepared_stmt:
  COMMIT PREPARED SCONST
  {
    $$.val = &tree.CommitPrepared{Transaction: tree.NewStrVal($3)}
  }

// %Help: ROLLBACK PREPARED - rollback the named transaction as part of two-phase commit
// %Category: Txn
// %Text: ROLLBACK PREPARED <transaction-id>
// %SeeAlso: PREPARE TRANSACTION, COMMIT PREPARED
rollback_prepared_stmt:
  ROLLBACK PREPARED SCONST
  {
    $$.val = &tree.RollbackPrepared{Transaction: tree.NewStrVal($3)}
  }

// %Help: CREATE DATABASE - create a new database
// %Category: DDL
// %Text: CREATE DATABASE [IF NOT EXISTS] <name>
// %SeeAlso: WEBDOCS/create-database.html
create_database_stmt:
  CREATE DATABASE database_name opt_with opt_template_clause opt_encoding_clause opt_lc_collate_clause opt_lc_ctype_clause opt_connection_limit opt_primary_region_clause opt_regions_list opt_survival_goal_clause opt_placement_clause opt_owner_clause opt_super_region_clause opt_secondary_region_clause
  {
    $$.val = &tree.CreateDatabase{
      Name: tree.Name($3),
      Template: $5,
      Encoding: $6,
      Collate: $7,
      CType: $8,
      ConnectionLimit: $9.int32(),
      PrimaryRegion: tree.Name($10),
      Regions: $11.nameList(),
      SurvivalGoal: $12.survivalGoal(),
      Placement: $13.dataPlacement(),
      Owner: $14.roleSpec(),
      SuperRegion: $15.superRegion(),
      SecondaryRegion: tree.Name($16),
    }
  }
| CREATE DATABASE IF NOT EXISTS database_name opt_with opt_template_clause opt_encoding_clause opt_lc_collate_clause opt_lc_ctype_clause opt_connection_limit opt_primary_region_clause opt_regions_list opt_survival_goal_clause opt_placement_clause opt_owner_clause opt_super_region_clause opt_secondary_region_clause
  {
    $$.val = &tree.CreateDatabase{
      IfNotExists: true,
      Name: tree.Name($6),
      Template: $8,
      Encoding: $9,
      Collate: $10,
      CType: $11,
      ConnectionLimit: $12.int32(),
      PrimaryRegion: tree.Name($13),
      Regions: $14.nameList(),
      SurvivalGoal: $15.survivalGoal(),
      Placement: $16.dataPlacement(),
      Owner: $17.roleSpec(),
      SuperRegion: $18.superRegion(),
      SecondaryRegion: tree.Name($19),
    }
  }
| CREATE DATABASE error // SHOW HELP: CREATE DATABASE

opt_primary_region_clause:
  primary_region_clause
| /* EMPTY */
  {
    $$ = ""
  }

primary_region_clause:
  PRIMARY REGION opt_equal region_name {
    $$ = $4
  }

opt_secondary_region_clause:
  secondary_region_clause
| /* EMPTY */
  {
    $$ = ""
  }

secondary_region_clause:
  SECONDARY REGION opt_equal region_name {
    $$ = $4
  }

opt_super_region_clause:
  super_region_clause
| /* EMPTY */
{
  $$.val = tree.SuperRegion{}
}

super_region_clause:
SUPER REGION region_name VALUES region_name_list
{
  $$.val = tree.SuperRegion{Name: tree.Name($3), Regions: $5.nameList()}
}

opt_placement_clause:
  placement_clause
| /* EMPTY */
  {
    $$.val = tree.DataPlacementUnspecified
  }

placement_clause:
  PLACEMENT RESTRICTED
  {
    $$.val = tree.DataPlacementRestricted
  }
| PLACEMENT DEFAULT
  {
    $$.val = tree.DataPlacementDefault
  }

opt_regions_list:
  region_or_regions opt_equal region_name_list
  {
    $$.val = $3.nameList()
  }
| /* EMPTY */
  {
    $$.val = tree.NameList(nil)
  }

region_or_regions:
  REGION
  {
    /* SKIP DOC */
  }
| REGIONS

survival_goal_clause:
  SURVIVE opt_equal REGION FAILURE
  {
    $$.val = tree.SurvivalGoalRegionFailure
  }
| SURVIVE opt_equal ZONE FAILURE
  {
    $$.val = tree.SurvivalGoalZoneFailure
  }
| SURVIVE opt_equal AVAILABILITY ZONE FAILURE
  {
    /* SKIP DOC */
    $$.val = tree.SurvivalGoalZoneFailure
  }


opt_survival_goal_clause:
  survival_goal_clause
| /* EMPTY */
  {
    $$.val = tree.SurvivalGoalDefault
  }

opt_template_clause:
  TEMPLATE opt_equal non_reserved_word_or_sconst
  {
    $$ = $3
  }
| /* EMPTY */
  {
    $$ = ""
  }

opt_encoding_clause:
  ENCODING opt_equal non_reserved_word_or_sconst
  {
    $$ = $3
  }
| /* EMPTY */
  {
    $$ = ""
  }

opt_lc_collate_clause:
  LC_COLLATE opt_equal non_reserved_word_or_sconst
  {
    $$ = $3
  }
| /* EMPTY */
  {
    $$ = ""
  }

opt_lc_ctype_clause:
  LC_CTYPE opt_equal non_reserved_word_or_sconst
  {
    $$ = $3
  }
| /* EMPTY */
  {
    $$ = ""
  }

opt_connection_limit:
  CONNECTION LIMIT opt_equal signed_iconst
  {
    ret, err := $4.numVal().AsInt32()
    if err != nil {
      return setErr(sqllex, err)
    }
    $$.val = ret
  }
| /* EMPTY */
  {
    $$.val = int32(-1)
  }

opt_owner_clause:
  OWNER opt_equal role_spec
  {
    $$ = $3
  }
| /* EMPTY */
   {
		 $$.val = tree.RoleSpec{
		   RoleSpecType: tree.CurrentUser,
		 }
   }

opt_equal:
  '=' {}
| /* EMPTY */ {}

// %Help: INSERT - create new rows in a table
// %Category: DML
// %Text:
// INSERT INTO <tablename> [[AS] <name>] [( <colnames...> )]
//        <selectclause>
//        [ON CONFLICT {
//          [( <colnames...> )] [WHERE <arbiter_predicate>] DO NOTHING |
//          ( <colnames...> ) [WHERE <index_predicate>] DO UPDATE SET ... [WHERE <expr>]
//        }
//        [RETURNING <exprs...>]
// %SeeAlso: UPSERT, UPDATE, DELETE, WEBDOCS/insert.html
insert_stmt:
  opt_with_clause INSERT INTO insert_target insert_rest returning_clause
  {
    $$.val = $5.stmt()
    $$.val.(*tree.Insert).With = $1.with()
    $$.val.(*tree.Insert).Table = $4.tblExpr()
    $$.val.(*tree.Insert).Returning = $6.retClause()
  }
| opt_with_clause INSERT INTO insert_target insert_rest on_conflict returning_clause
  {
    $$.val = $5.stmt()
    $$.val.(*tree.Insert).With = $1.with()
    $$.val.(*tree.Insert).Table = $4.tblExpr()
    $$.val.(*tree.Insert).OnConflict = $6.onConflict()
    $$.val.(*tree.Insert).Returning = $7.retClause()
  }
| opt_with_clause INSERT error // SHOW HELP: INSERT

// %Help: UPSERT - create or replace rows in a table
// %Category: DML
// %Text:
// UPSERT INTO <tablename> [AS <name>] [( <colnames...> )]
//        <selectclause>
//        [RETURNING <exprs...>]
// %SeeAlso: INSERT, UPDATE, DELETE, WEBDOCS/upsert.html
upsert_stmt:
  opt_with_clause UPSERT INTO insert_target insert_rest returning_clause
  {
    $$.val = $5.stmt()
    $$.val.(*tree.Insert).With = $1.with()
    $$.val.(*tree.Insert).Table = $4.tblExpr()
    $$.val.(*tree.Insert).OnConflict = &tree.OnConflict{}
    $$.val.(*tree.Insert).Returning = $6.retClause()
  }
| opt_with_clause UPSERT error // SHOW HELP: UPSERT

insert_target:
  table_name_opt_idx
  {
    $$.val = $1.tblExpr()
  }
// Can't easily make AS optional here, because VALUES in insert_rest would have
// a shift/reduce conflict with VALUES as an optional alias. We could easily
// allow unreserved_keywords as optional aliases, but that'd be an odd
// divergence from other places. So just require AS for now.
| table_name_opt_idx AS table_alias_name
  {
    alias := $1.tblExpr().(*tree.AliasedTableExpr)
    alias.As = tree.AliasClause{Alias: tree.Name($3)}
    $$.val = alias
  }
| numeric_table_ref opt_index_flags
  {
    /* SKIP DOC */
    $$.val = &tree.AliasedTableExpr{
      Expr: $1.tblExpr(),
      IndexFlags: $2.indexFlags(),
    }
  }

insert_rest:
  select_stmt
  {
    $$.val = &tree.Insert{Rows: $1.slct()}
  }
| '(' insert_column_list ')' select_stmt
  {
    $$.val = &tree.Insert{Columns: $2.nameList(), Rows: $4.slct()}
  }
| DEFAULT VALUES
  {
    $$.val = &tree.Insert{Rows: &tree.Select{}}
  }

insert_column_list:
  insert_column_item
  {
    $$.val = tree.NameList{tree.Name($1)}
  }
| insert_column_list ',' insert_column_item
  {
    $$.val = append($1.nameList(), tree.Name($3))
  }

// insert_column_item represents the target of an INSERT/UPSERT or one
// of the LHS operands in an UPDATE SET statement.
//
//    INSERT INTO foo (x, y) VALUES ...
//                     ^^^^ here
//
//    UPDATE foo SET x = 1+2, (y, z) = (4, 5)
//                   ^^ here   ^^^^ here
//
// Currently CockroachDB only supports simple column names in this
// position. The rule below can be extended to support a sequence of
// field subscript or array indexing operators to designate a part of
// a field, when partial updates are to be supported. This likely will
// be needed together with support for composite types (#27792).
insert_column_item:
  column_name
| column_name '.' error { return unimplementedWithIssue(sqllex, 27792) }

on_conflict:
  ON CONFLICT DO NOTHING
  {
    $$.val = &tree.OnConflict{
      Columns: tree.NameList(nil),
      DoNothing: true,
    }
  }
| ON CONFLICT '(' name_list ')' opt_where_clause DO NOTHING
  {
    $$.val = &tree.OnConflict{
      Columns: $4.nameList(),
      ArbiterPredicate: $6.expr(),
      DoNothing: true,
    }
  }
| ON CONFLICT '(' name_list ')' opt_where_clause DO UPDATE SET set_clause_list opt_where_clause
  {
    $$.val = &tree.OnConflict{
      Columns: $4.nameList(),
      ArbiterPredicate: $6.expr(),
      Exprs: $10.updateExprs(),
      Where: tree.NewWhere(tree.AstWhere, $11.expr()),
    }
  }
| ON CONFLICT ON CONSTRAINT constraint_name DO NOTHING
  {
    $$.val = &tree.OnConflict{
      Constraint: tree.Name($5),
      DoNothing: true,
    }
  }
| ON CONFLICT ON CONSTRAINT constraint_name DO UPDATE SET set_clause_list opt_where_clause
  {
    $$.val = &tree.OnConflict{
      Constraint: tree.Name($5),
      Exprs: $9.updateExprs(),
      Where: tree.NewWhere(tree.AstWhere, $10.expr()),
    }
  }

returning_clause:
  RETURNING target_list
  {
    ret := tree.ReturningExprs($2.selExprs())
    $$.val = &ret
  }
| RETURNING NOTHING_AFTER_RETURNING
  {
    $$.val = tree.ReturningNothingClause
  }
| /* EMPTY */
  {
    $$.val = tree.AbsentReturningClause
  }

// %Help: UPDATE - update rows of a table
// %Category: DML
// %Text:
// UPDATE <tablename> [[AS] <name>]
//        SET ...
//        [FROM <source>]
//        [WHERE <expr>]
//        [ORDER BY <exprs...>]
//        [LIMIT <expr>]
//        [RETURNING <exprs...>]
// %SeeAlso: INSERT, UPSERT, DELETE, WEBDOCS/update.html
update_stmt:
  opt_with_clause UPDATE table_expr_opt_alias_idx
    SET set_clause_list opt_from_list opt_where_clause opt_sort_clause opt_limit_clause returning_clause
  {
    $$.val = &tree.Update{
      With: $1.with(),
      Table: $3.tblExpr(),
      Exprs: $5.updateExprs(),
      From: $6.tblExprs(),
      Where: tree.NewWhere(tree.AstWhere, $7.expr()),
      OrderBy: $8.orderBy(),
      Limit: $9.limit(),
      Returning: $10.retClause(),
    }
  }
| opt_with_clause UPDATE error // SHOW HELP: UPDATE

opt_from_list:
  FROM from_list {
    $$.val = $2.tblExprs()
  }
| /* EMPTY */ {
    $$.val = tree.TableExprs{}
}

set_clause_list:
  set_clause
  {
    $$.val = tree.UpdateExprs{$1.updateExpr()}
  }
| set_clause_list ',' set_clause
  {
    $$.val = append($1.updateExprs(), $3.updateExpr())
  }

// TODO(knz): The LHS in these can be extended to support
// a path to a field member when compound types are supported.
// Keep it simple for now.
set_clause:
  single_set_clause
| multiple_set_clause

single_set_clause:
  column_name '=' a_expr
  {
    $$.val = &tree.UpdateExpr{Names: tree.NameList{tree.Name($1)}, Expr: $3.expr()}
  }
| column_name '.' error { return unimplementedWithIssue(sqllex, 27792) }

multiple_set_clause:
  '(' insert_column_list ')' '=' in_expr
  {
    $$.val = &tree.UpdateExpr{Tuple: true, Names: $2.nameList(), Expr: $5.expr()}
  }

// %Help: REASSIGN OWNED BY - change ownership of all objects
// %Category: Priv
// %Text: REASSIGN OWNED BY {<name> | CURRENT_USER | SESSION_USER}[,...]
// TO {<name> | CURRENT_USER | SESSION_USER}
// %SeeAlso: DROP OWNED BY
reassign_owned_by_stmt:
  REASSIGN OWNED BY role_spec_list TO role_spec
  {
    $$.val = &tree.ReassignOwnedBy{
      OldRoles: $4.roleSpecList(),
      NewRole: $6.roleSpec(),
    }
  }
| REASSIGN OWNED BY error // SHOW HELP: REASSIGN OWNED BY

// %Help: DROP OWNED BY - remove database objects owned by role(s).
// %Category: Priv
// %Text: DROP OWNED BY {<name> | CURRENT_USER | SESSION_USER}[,...]
// [RESTRICT | CASCADE]
// %SeeAlso: REASSIGN OWNED BY
drop_owned_by_stmt:
  DROP OWNED BY role_spec_list opt_drop_behavior
  {
    $$.val = &tree.DropOwnedBy{
      Roles: $4.roleSpecList(),
      DropBehavior: $5.dropBehavior(),
    }
  }
| DROP OWNED BY error // SHOW HELP: DROP OWNED BY

// A complete SELECT statement looks like this.
//
// The rule returns either a single select_stmt node or a tree of them,
// representing a set-operation tree.
//
// There is an ambiguity when a sub-SELECT is within an a_expr and there are
// excess parentheses: do the parentheses belong to the sub-SELECT or to the
// surrounding a_expr?  We don't really care, but bison wants to know. To
// resolve the ambiguity, we are careful to define the grammar so that the
// decision is staved off as long as possible: as long as we can keep absorbing
// parentheses into the sub-SELECT, we will do so, and only when it's no longer
// possible to do that will we decide that parens belong to the expression. For
// example, in "SELECT (((SELECT 2)) + 3)" the extra parentheses are treated as
// part of the sub-select. The necessity of doing it that way is shown by
// "SELECT (((SELECT 2)) UNION SELECT 2)". Had we parsed "((SELECT 2))" as an
// a_expr, it'd be too late to go back to the SELECT viewpoint when we see the
// UNION.
//
// This approach is implemented by defining a nonterminal select_with_parens,
// which represents a SELECT with at least one outer layer of parentheses, and
// being careful to use select_with_parens, never '(' select_stmt ')', in the
// expression grammar. We will then have shift-reduce conflicts which we can
// resolve in favor of always treating '(' <select> ')' as a
// select_with_parens. To resolve the conflicts, the productions that conflict
// with the select_with_parens productions are manually given precedences lower
// than the precedence of ')', thereby ensuring that we shift ')' (and then
// reduce to select_with_parens) rather than trying to reduce the inner
// <select> nonterminal to something else. We use UMINUS precedence for this,
// which is a fairly arbitrary choice.
//
// To be able to define select_with_parens itself without ambiguity, we need a
// nonterminal select_no_parens that represents a SELECT structure with no
// outermost parentheses. This is a little bit tedious, but it works.
//
// In non-expression contexts, we use select_stmt which can represent a SELECT
// with or without outer parentheses.
select_stmt:
  select_no_parens %prec UMINUS
| select_with_parens %prec UMINUS
  {
    $$.val = &tree.Select{Select: $1.selectStmt()}
  }

select_with_parens:
  '(' select_no_parens ')'
  {
    $$.val = &tree.ParenSelect{Select: $2.slct()}
  }
| '(' select_with_parens ')'
  {
    $$.val = &tree.ParenSelect{Select: &tree.Select{Select: $2.selectStmt()}}
  }

// This rule parses the equivalent of the standard's <query expression>. The
// duplicative productions are annoying, but hard to get rid of without
// creating shift/reduce conflicts.
//
//      The locking clause (FOR UPDATE etc) may be before or after
//      LIMIT/OFFSET. In <=7.2.X, LIMIT/OFFSET had to be after FOR UPDATE We
//      now support both orderings, but prefer LIMIT/OFFSET before the locking
//      clause.
//      - 2002-08-28 bjm
select_no_parens:
  simple_select
  {
    $$.val = &tree.Select{Select: $1.selectStmt()}
  }
| select_clause sort_clause
  {
    $$.val = &tree.Select{Select: $1.selectStmt(), OrderBy: $2.orderBy()}
  }
| select_clause opt_sort_clause for_locking_clause opt_select_limit
  {
    $$.val = &tree.Select{Select: $1.selectStmt(), OrderBy: $2.orderBy(), Limit: $4.limit(), Locking: $3.lockingClause()}
  }
| select_clause opt_sort_clause select_limit opt_for_locking_clause
  {
    $$.val = &tree.Select{Select: $1.selectStmt(), OrderBy: $2.orderBy(), Limit: $3.limit(), Locking: $4.lockingClause()}
  }
| with_clause select_clause
  {
    $$.val = &tree.Select{With: $1.with(), Select: $2.selectStmt()}
  }
| with_clause select_clause sort_clause
  {
    $$.val = &tree.Select{With: $1.with(), Select: $2.selectStmt(), OrderBy: $3.orderBy()}
  }
| with_clause select_clause opt_sort_clause for_locking_clause opt_select_limit
  {
    $$.val = &tree.Select{With: $1.with(), Select: $2.selectStmt(), OrderBy: $3.orderBy(), Limit: $5.limit(), Locking: $4.lockingClause()}
  }
| with_clause select_clause opt_sort_clause select_limit opt_for_locking_clause
  {
    $$.val = &tree.Select{With: $1.with(), Select: $2.selectStmt(), OrderBy: $3.orderBy(), Limit: $4.limit(), Locking: $5.lockingClause()}
  }

for_locking_clause:
  for_locking_items { $$.val = $1.lockingClause() }
| FOR READ ONLY     { $$.val = (tree.LockingClause)(nil) }

opt_for_locking_clause:
  for_locking_clause { $$.val = $1.lockingClause() }
| /* EMPTY */        { $$.val = (tree.LockingClause)(nil) }

for_locking_items:
  for_locking_item
  {
    $$.val = tree.LockingClause{$1.lockingItem()}
  }
| for_locking_items for_locking_item
  {
    $$.val = append($1.lockingClause(), $2.lockingItem())
  }

for_locking_item:
  for_locking_strength opt_locked_rels opt_nowait_or_skip
  {
    $$.val = &tree.LockingItem{
      Strength:   $1.lockingStrength(),
      Targets:    $2.tableNames(),
      WaitPolicy: $3.lockingWaitPolicy(),
    }
  }

for_locking_strength:
  FOR UPDATE        { $$.val = tree.ForUpdate }
| FOR NO KEY UPDATE { $$.val = tree.ForNoKeyUpdate }
| FOR SHARE         { $$.val = tree.ForShare }
| FOR KEY SHARE     { $$.val = tree.ForKeyShare }

opt_locked_rels:
  /* EMPTY */        { $$.val = tree.TableNames{} }
| OF table_name_list { $$.val = $2.tableNames() }

opt_nowait_or_skip:
  /* EMPTY */ { $$.val = tree.LockWaitBlock }
| SKIP LOCKED { $$.val = tree.LockWaitSkipLocked }
| NOWAIT      { $$.val = tree.LockWaitError }

select_clause:
// We only provide help if an open parenthesis is provided, because
// otherwise the rule is ambiguous with the top-level statement list.
  '(' error // SHOW HELP: <SELECTCLAUSE>
| simple_select
| select_with_parens

// This rule parses SELECT statements that can appear within set operations,
// including UNION, INTERSECT and EXCEPT. '(' and ')' can be used to specify
// the ordering of the set operations. Without '(' and ')' we want the
// operations to be ordered per the precedence specs at the head of this file.
//
// As with select_no_parens, simple_select cannot have outer parentheses, but
// can have parenthesized subclauses.
//
// Note that sort clauses cannot be included at this level --- SQL requires
//       SELECT foo UNION SELECT bar ORDER BY baz
// to be parsed as
//       (SELECT foo UNION SELECT bar) ORDER BY baz
// not
//       SELECT foo UNION (SELECT bar ORDER BY baz)
//
// Likewise for WITH, FOR UPDATE and LIMIT. Therefore, those clauses are
// described as part of the select_no_parens production, not simple_select.
// This does not limit functionality, because you can reintroduce these clauses
// inside parentheses.
//
// NOTE: only the leftmost component select_stmt should have INTO. However,
// this is not checked by the grammar; parse analysis must check it.
//
// %Help: <SELECTCLAUSE> - access tabular data
// %Category: DML
// %Text:
// Select clause:
//   TABLE <tablename>
//   VALUES ( <exprs...> ) [ , ... ]
//   SELECT ... [ { INTERSECT | UNION | EXCEPT } [ ALL | DISTINCT ] <selectclause> ]
simple_select:
  simple_select_clause // EXTEND WITH HELP: SELECT
| values_clause        // EXTEND WITH HELP: VALUES
| table_clause         // EXTEND WITH HELP: TABLE
| set_operation

// %Help: SELECT - retrieve rows from a data source and compute a result
// %Category: DML
// %Text:
// SELECT [DISTINCT [ ON ( <expr> [ , ... ] ) ] ]
//        { <expr> [[AS] <name>] | [ [<dbname>.] <tablename>. ] * } [, ...]
//        [ FROM <source> ]
//        [ WHERE <expr> ]
//        [ GROUP BY <expr> [ , ... ] ]
//        [ HAVING <expr> ]
//        [ WINDOW <name> AS ( <definition> ) ]
//        [ { UNION | INTERSECT | EXCEPT } [ ALL | DISTINCT ] <selectclause> ]
//        [ ORDER BY <expr> [ ASC | DESC ] [, ...] ]
//        [ LIMIT { <expr> | ALL } ]
//        [ OFFSET <expr> [ ROW | ROWS ] ]
// %SeeAlso: WEBDOCS/select-clause.html
simple_select_clause:
  SELECT opt_all_clause opt_target_list
    from_clause opt_where_clause
    group_clause having_clause window_clause
  {
    $$.val = &tree.SelectClause{
      Exprs:   $3.selExprs(),
      From:    $4.from(),
      Where:   tree.NewWhere(tree.AstWhere, $5.expr()),
      GroupBy: $6.groupBy(),
      Having:  tree.NewWhere(tree.AstHaving, $7.expr()),
      Window:  $8.window(),
    }
  }
| SELECT distinct_clause target_list
    from_clause opt_where_clause
    group_clause having_clause window_clause
  {
    $$.val = &tree.SelectClause{
      Distinct: $2.bool(),
      Exprs:    $3.selExprs(),
      From:     $4.from(),
      Where:    tree.NewWhere(tree.AstWhere, $5.expr()),
      GroupBy:  $6.groupBy(),
      Having:   tree.NewWhere(tree.AstHaving, $7.expr()),
      Window:   $8.window(),
    }
  }
| SELECT distinct_on_clause target_list
    from_clause opt_where_clause
    group_clause having_clause window_clause
  {
    $$.val = &tree.SelectClause{
      Distinct:   true,
      DistinctOn: $2.distinctOn(),
      Exprs:      $3.selExprs(),
      From:       $4.from(),
      Where:      tree.NewWhere(tree.AstWhere, $5.expr()),
      GroupBy:    $6.groupBy(),
      Having:     tree.NewWhere(tree.AstHaving, $7.expr()),
      Window:     $8.window(),
    }
  }

set_operation:
  select_clause UNION all_or_distinct select_clause
  {
    $$.val = &tree.UnionClause{
      Type:  tree.UnionOp,
      Left:  &tree.Select{Select: $1.selectStmt()},
      Right: &tree.Select{Select: $4.selectStmt()},
      All:   $3.bool(),
    }
  }
| select_clause INTERSECT all_or_distinct select_clause
  {
    $$.val = &tree.UnionClause{
      Type:  tree.IntersectOp,
      Left:  &tree.Select{Select: $1.selectStmt()},
      Right: &tree.Select{Select: $4.selectStmt()},
      All:   $3.bool(),
    }
  }
| select_clause EXCEPT all_or_distinct select_clause
  {
    $$.val = &tree.UnionClause{
      Type:  tree.ExceptOp,
      Left:  &tree.Select{Select: $1.selectStmt()},
      Right: &tree.Select{Select: $4.selectStmt()},
      All:   $3.bool(),
    }
  }

// %Help: TABLE - select an entire table
// %Category: DML
// %Text: TABLE <tablename>
// %SeeAlso: SELECT, VALUES, WEBDOCS/table-expressions.html
table_clause:
  TABLE table_ref
  {
    $$.val = &tree.SelectClause{
      Exprs:       tree.SelectExprs{tree.StarSelectExpr()},
      From:        tree.From{Tables: tree.TableExprs{$2.tblExpr()}},
      TableSelect: true,
    }
  }
| TABLE error // SHOW HELP: TABLE

// SQL standard WITH clause looks like:
//
// WITH [ RECURSIVE ] <query name> [ (<column> [, ...]) ]
//        AS [ [ NOT ] MATERIALIZED ] (query) [ SEARCH or CYCLE clause ]
//
// We don't currently support the SEARCH or CYCLE clause.
//
// Recognizing WITH_LA here allows a CTE to be named TIME or ORDINALITY.
with_clause:
  WITH cte_list
  {
    $$.val = &tree.With{CTEList: $2.ctes()}
  }
| WITH_LA cte_list
  {
    /* SKIP DOC */
    $$.val = &tree.With{CTEList: $2.ctes()}
  }
| WITH RECURSIVE cte_list
  {
    $$.val = &tree.With{Recursive: true, CTEList: $3.ctes()}
  }

cte_list:
  common_table_expr
  {
    $$.val = []*tree.CTE{$1.cte()}
  }
| cte_list ',' common_table_expr
  {
    $$.val = append($1.ctes(), $3.cte())
  }

materialize_clause:
  MATERIALIZED
  {
    $$.val = tree.CTEMaterializeAlways
  }
| NOT MATERIALIZED
  {
    $$.val = tree.CTEMaterializeNever
  }
| /* EMPTY */ {
    $$.val = tree.CTEMaterializeDefault
}

common_table_expr:
  table_alias_name opt_col_def_list_no_types AS materialize_clause '(' preparable_stmt ')'
    {
      $$.val = &tree.CTE{
        Name: tree.AliasClause{Alias: tree.Name($1), Cols: $2.colDefList() },
        Mtr: $4.cteMaterializeClause(),
        Stmt: $6.stmt(),
      }
    }

opt_with:
  WITH {}
| /* EMPTY */ {}

opt_with_clause:
  with_clause
  {
    $$.val = $1.with()
  }
| /* EMPTY */
  {
    $$.val = nil
  }

opt_table:
  TABLE {}
| /* EMPTY */ {}

all_or_distinct:
  ALL
  {
    $$.val = true
  }
| DISTINCT
  {
    $$.val = false
  }
| /* EMPTY */
  {
    $$.val = false
  }

distinct_clause:
  DISTINCT
  {
    $$.val = true
  }

distinct_on_clause:
  DISTINCT ON '(' expr_list ')'
  {
    $$.val = tree.DistinctOn($4.exprs())
  }

opt_all_clause:
  ALL {}
| /* EMPTY */ {}

opt_privileges_clause:
  PRIVILEGES {}
| /* EMPTY */ {}

opt_sort_clause:
  sort_clause
  {
    $$.val = $1.orderBy()
  }
| /* EMPTY */
  {
    $$.val = tree.OrderBy(nil)
  }

opt_sort_clause_no_index:
  sort_clause_no_index
  {
    $$.val = $1.orderBy()
  }
| /* EMPTY */
  {
    $$.val = tree.OrderBy(nil)
  }

sort_clause:
  ORDER BY sortby_list
  {
    $$.val = tree.OrderBy($3.orders())
  }

sort_clause_no_index:
  ORDER BY sortby_no_index_list
  {
    $$.val = tree.OrderBy($3.orders())
  }

single_sort_clause:
  ORDER BY sortby
  {
    $$.val = tree.OrderBy([]*tree.Order{$3.order()})
  }
| ORDER BY sortby_index
  {
    return unimplementedWithIssueDetail(sqllex, 109847, "order by index")
  }
| ORDER BY sortby ',' sortby_list
  {
    sqllex.Error("multiple ORDER BY clauses are not supported in this function")
    return 1
  }
| ORDER BY sortby_index ',' sortby_list
  {
    sqllex.Error("multiple ORDER BY clauses are not supported in this function")
    return 1
  }

sortby_list:
  sortby
  {
    $$.val = []*tree.Order{$1.order()}
  }
| sortby_index
  {
    $$.val = []*tree.Order{$1.order()}
  }
| sortby_list ',' sortby
  {
    $$.val = append($1.orders(), $3.order())
  }
| sortby_list ',' sortby_index
  {
    $$.val = append($1.orders(), $3.order())
  }

sortby_no_index_list:
  sortby
  {
    $$.val = []*tree.Order{$1.order()}
  }
| sortby_no_index_list ',' sortby
  {
    $$.val = append($1.orders(), $3.order())
  }
| sortby_no_index_list ',' sortby_index
  {
    $$.val = append($1.orders(), $3.order())
  }

sortby_index:
  PRIMARY KEY table_name opt_asc_desc
  {
    name := $3.unresolvedObjectName().ToTableName()
    $$.val = &tree.Order{OrderType: tree.OrderByIndex, Direction: $4.dir(), Table: name}
  }
| INDEX_AFTER_ORDER_BY_BEFORE_AT table_name '@' index_name opt_asc_desc
  {
    name := $2.unresolvedObjectName().ToTableName()
    $$.val = &tree.Order{
      OrderType: tree.OrderByIndex,
      Direction: $5.dir(),
      Table:     name,
      Index:     tree.UnrestrictedName($4),
    }
  }

sortby:
  a_expr opt_asc_desc opt_nulls_order
  {
    /* FORCE DOC */
    dir := $2.dir()
    nullsOrder := $3.nullsOrder()
    $$.val = &tree.Order{
      OrderType:  tree.OrderByColumn,
      Expr:       $1.expr(),
      Direction:  dir,
      NullsOrder: nullsOrder,
    }
  }

opt_nulls_order:
  NULLS_LA FIRST
  {
    $$.val = tree.NullsFirst
  }
| NULLS_LA LAST
  {
    $$.val = tree.NullsLast
  }
| /* EMPTY */
  {
    $$.val = tree.DefaultNullsOrder
  }

select_limit:
  limit_clause offset_clause
  {
    if $1.limit() == nil {
      $$.val = $2.limit()
    } else {
      $$.val = $1.limit()
      $$.val.(*tree.Limit).Offset = $2.limit().Offset
    }
  }
| offset_clause limit_clause
  {
    $$.val = $1.limit()
    if $2.limit() != nil {
      $$.val.(*tree.Limit).Count = $2.limit().Count
      $$.val.(*tree.Limit).LimitAll = $2.limit().LimitAll
    }
  }
| limit_clause
| offset_clause

opt_select_limit:
  select_limit { $$.val = $1.limit() }
| /* EMPTY */  { $$.val = (*tree.Limit)(nil) }

opt_limit_clause:
  limit_clause
| /* EMPTY */ { $$.val = (*tree.Limit)(nil) }

limit_clause:
  LIMIT ALL
  {
    $$.val = &tree.Limit{LimitAll: true}
  }
| LIMIT a_expr
  {
    if $2.expr() == nil {
      $$.val = (*tree.Limit)(nil)
    } else {
      $$.val = &tree.Limit{Count: $2.expr()}
    }
  }
// SQL:2008 syntax
// To avoid shift/reduce conflicts, handle the optional value with
// a separate production rather than an opt_ expression. The fact
// that ONLY is fully reserved means that this way, we defer any
// decision about what rule reduces ROW or ROWS to the point where
// we can see the ONLY token in the lookahead slot.
| FETCH first_or_next select_fetch_first_value row_or_rows ONLY
  {
    $$.val = &tree.Limit{Count: $3.expr()}
  }
| FETCH first_or_next row_or_rows ONLY
	{
    $$.val = &tree.Limit{
      Count: tree.NewNumVal(constant.MakeInt64(1), "" /* origString */, false /* negative */),
    }
  }

offset_clause:
  OFFSET a_expr
  {
    $$.val = &tree.Limit{Offset: $2.expr()}
  }
  // SQL:2008 syntax
  // The trailing ROW/ROWS in this case prevent the full expression
  // syntax. c_expr is the best we can do.
| OFFSET select_fetch_first_value row_or_rows
  {
    $$.val = &tree.Limit{Offset: $2.expr()}
  }

// Allowing full expressions without parentheses causes various parsing
// problems with the trailing ROW/ROWS key words. SQL spec only calls for
// <simple value specification>, which is either a literal or a parameter (but
// an <SQL parameter reference> could be an identifier, bringing up conflicts
// with ROW/ROWS). We solve this by leveraging the presence of ONLY (see above)
// to determine whether the expression is missing rather than trying to make it
// optional in this rule.
//
// c_expr covers almost all the spec-required cases (and more), but it doesn't
// cover signed numeric literals, which are allowed by the spec. So we include
// those here explicitly.
select_fetch_first_value:
  c_expr
| only_signed_iconst
| only_signed_fconst

// noise words
row_or_rows:
  ROW {}
| ROWS {}

first_or_next:
  FIRST {}
| NEXT {}

// This syntax for group_clause tries to follow the spec quite closely.
// However, the spec allows only column references, not expressions,
// which introduces an ambiguity between implicit row constructors
// (a,b) and lists of column references.
//
// We handle this by using the a_expr production for what the spec calls
// <ordinary grouping set>, which in the spec represents either one column
// reference or a parenthesized list of column references. Then, we check the
// top node of the a_expr to see if it's an RowExpr, and if so, just grab and
// use the list, discarding the node. (this is done in parse analysis, not here)
//
// Each item in the group_clause list is either an expression tree or a
// GroupingSet node of some type.
group_clause:
  GROUP BY group_by_list
  {
    $$.val = tree.GroupBy($3.exprs())
  }
| /* EMPTY */
  {
    $$.val = tree.GroupBy(nil)
  }

group_by_list:
  group_by_item { $$.val = tree.Exprs{$1.expr()} }
| group_by_list ',' group_by_item { $$.val = append($1.exprs(), $3.expr()) }

// Note the '(' is required as CUBE and ROLLUP rely on setting precedence
// of CUBE and ROLLUP below that of '(', so that they shift in these rules
// rather than reducing the conflicting unreserved_keyword rule.
group_by_item:
  a_expr { $$.val = $1.expr() }
| ROLLUP '(' error { return unimplementedWithIssueDetail(sqllex, 46280, "rollup") }
| CUBE '(' error { return unimplementedWithIssueDetail(sqllex, 46280, "cube") }
| GROUPING SETS error { return unimplementedWithIssueDetail(sqllex, 46280, "grouping sets") }

having_clause:
  HAVING a_expr
  {
    $$.val = $2.expr()
  }
| /* EMPTY */
  {
    $$.val = tree.Expr(nil)
  }

// Given "VALUES (a, b)" in a table expression context, we have to
// decide without looking any further ahead whether VALUES is the
// values clause or a set-generating function. Since VALUES is allowed
// as a function name both interpretations are feasible. We resolve
// the shift/reduce conflict by giving the first values_clause
// production a higher precedence than the VALUES token has, causing
// the parser to prefer to reduce, in effect assuming that the VALUES
// is not a function name.
//
// %Help: VALUES - select a given set of values
// %Category: DML
// %Text: VALUES ( <exprs...> ) [, ...]
// %SeeAlso: SELECT, TABLE, WEBDOCS/table-expressions.html
values_clause:
  VALUES '(' expr_list ')' %prec UMINUS
  {
    $$.val = &tree.ValuesClause{Rows: []tree.Exprs{$3.exprs()}}
  }
| VALUES error // SHOW HELP: VALUES
| values_clause ',' '(' expr_list ')'
  {
    valNode := $1.selectStmt().(*tree.ValuesClause)
    valNode.Rows = append(valNode.Rows, $4.exprs())
    $$.val = valNode
  }

// clauses common to all optimizable statements:
//  from_clause   - allow list of both JOIN expressions and table names
//  where_clause  - qualifications for joins or restrictions

from_clause:
  FROM from_list opt_as_of_clause
  {
    $$.val = tree.From{Tables: $2.tblExprs(), AsOf: $3.asOfClause()}
  }
| FROM error // SHOW HELP: <SOURCE>
| /* EMPTY */
  {
    $$.val = tree.From{}
  }

from_list:
  table_ref
  {
    $$.val = tree.TableExprs{$1.tblExpr()}
  }
| from_list ',' table_ref
  {
    $$.val = append($1.tblExprs(), $3.tblExpr())
  }

index_flags_param:
  FORCE_INDEX '=' index_name
  {
     $$.val = &tree.IndexFlags{Index: tree.UnrestrictedName($3)}
  }
| FORCE_INDEX '=' '[' iconst64 ']'
  {
    /* SKIP DOC */
    $$.val = &tree.IndexFlags{IndexID: tree.IndexID($4.int64())}
  }
| ASC
  {
    /* SKIP DOC */
    $$.val = &tree.IndexFlags{Direction: tree.Ascending}
  }
| DESC
  {
    /* SKIP DOC */
    $$.val = &tree.IndexFlags{Direction: tree.Descending}
  }
|
  NO_INDEX_JOIN
  {
    $$.val = &tree.IndexFlags{NoIndexJoin: true}
  }
| NO_ZIGZAG_JOIN
  {
    $$.val = &tree.IndexFlags{NoZigzagJoin: true}
  }
| NO_FULL_SCAN
  {
    $$.val = &tree.IndexFlags{NoFullScan: true}
  }
| AVOID_FULL_SCAN
  {
    $$.val = &tree.IndexFlags{AvoidFullScan: true}
  }
| IGNORE_FOREIGN_KEYS
  {
    /* SKIP DOC */
    $$.val = &tree.IndexFlags{IgnoreForeignKeys: true}
  }
| FORCE_INVERTED_INDEX
  {
    /* SKIP DOC */
     $$.val = &tree.IndexFlags{ForceInvertedIndex: true}
  }
| FORCE_ZIGZAG
  {
     $$.val = &tree.IndexFlags{ForceZigzag: true}
  }
| FORCE_ZIGZAG '=' index_name
  {
     $$.val = &tree.IndexFlags{ZigzagIndexes: []tree.UnrestrictedName{tree.UnrestrictedName($3)}}
  }
| FORCE_ZIGZAG '=' '[' iconst64 ']'
  {
    /* SKIP DOC */
     $$.val = &tree.IndexFlags{ZigzagIndexIDs: []tree.IndexID{tree.IndexID($4.int64())}}
  }
| FAMILY '=' '[' iconst64 ']'
  {
    /* SKIP DOC */
    id := tree.FamilyID(uint32($4.int64()))
     $$.val = &tree.IndexFlags{FamilyID: &id}
  }

index_flags_param_list:
  index_flags_param
  {
    $$.val = $1.indexFlags()
  }
|
  index_flags_param_list ',' index_flags_param
  {
    a := $1.indexFlags()
    b := $3.indexFlags()
    if err := a.CombineWith(b); err != nil {
      return setErr(sqllex, err)
    }
    $$.val = a
  }

opt_index_flags:
  '@' index_name
  {
    $$.val = &tree.IndexFlags{Index: tree.UnrestrictedName($2)}
  }
| '@' '[' iconst64 ']'
  {
    $$.val = &tree.IndexFlags{IndexID: tree.IndexID($3.int64())}
  }
| '@' '{' index_flags_param_list '}'
  {
    flags := $3.indexFlags()
    if err := flags.Check(); err != nil {
      return setErr(sqllex, err)
    }
    $$.val = flags
  }
| /* EMPTY */
  {
    $$.val = (*tree.IndexFlags)(nil)
  }

// %Help: <SOURCE> - define a data source for SELECT
// %Category: DML
// %Text:
// Data sources:
//   <tablename> [ @ { <idxname> | <indexflags> } ]
//   <tablefunc> ( <exprs...> )
//   ( { <selectclause> | <source> } )
//   <source> [AS] <alias> [( <colnames...> )]
//   <source> [ <jointype> ] JOIN <source> ON <expr>
//   <source> [ <jointype> ] JOIN <source> USING ( <colnames...> )
//   <source> NATURAL [ <jointype> ] JOIN <source>
//   <source> CROSS JOIN <source>
//   <source> WITH ORDINALITY
//   '[' EXPLAIN ... ']'
//   '[' SHOW ... ']'
//
// Index flags:
//   '{' FORCE_INDEX = <idxname> [, ...] '}'
//   '{' NO_INDEX_JOIN [, ...] '}'
//   '{' NO_ZIGZAG_JOIN [, ...] '}'
//   '{' NO_FULL_SCAN [, ...] '}'
//   '{' AVOID_FULL_SCAN [, ...] '}'
//   '{' IGNORE_FOREIGN_KEYS [, ...] '}'
//   '{' FORCE_ZIGZAG = <idxname> [, ...]  '}'
//
// Join types:
//   { INNER | { LEFT | RIGHT | FULL } [OUTER] } [ { HASH | MERGE | LOOKUP | INVERTED | STRAIGHT } ]
//
// %SeeAlso: WEBDOCS/table-expressions.html
table_ref:
  numeric_table_ref opt_index_flags opt_ordinality opt_alias_clause
  {
    /* SKIP DOC */
    $$.val = &tree.AliasedTableExpr{
        Expr:       $1.tblExpr(),
        IndexFlags: $2.indexFlags(),
        Ordinality: $3.bool(),
        As:         $4.aliasClause(),
    }
  }
| relation_expr opt_index_flags opt_ordinality opt_alias_clause
  {
    name := $1.unresolvedObjectName().ToTableName()
    $$.val = &tree.AliasedTableExpr{
      Expr:       &name,
      IndexFlags: $2.indexFlags(),
      Ordinality: $3.bool(),
      As:         $4.aliasClause(),
    }
  }
| select_with_parens opt_ordinality opt_alias_clause
  {
    $$.val = &tree.AliasedTableExpr{
      Expr:       &tree.Subquery{Select: $1.selectStmt()},
      Ordinality: $2.bool(),
      As:         $3.aliasClause(),
    }
  }
| LATERAL select_with_parens opt_ordinality opt_alias_clause
  {
    $$.val = &tree.AliasedTableExpr{
      Expr:       &tree.Subquery{Select: $2.selectStmt()},
      Ordinality: $3.bool(),
      Lateral:    true,
      As:         $4.aliasClause(),
    }
  }
| joined_table
  {
    $$.val = $1.tblExpr()
  }
| '(' joined_table ')' opt_ordinality alias_clause
  {
    $$.val = &tree.AliasedTableExpr{Expr: &tree.ParenTableExpr{Expr: $2.tblExpr()}, Ordinality: $4.bool(), As: $5.aliasClause()}
  }
| func_table opt_ordinality opt_func_alias_clause
  {
    f := $1.tblExpr()
    $$.val = &tree.AliasedTableExpr{
      Expr: f,
      Ordinality: $2.bool(),
      // Technically, LATERAL is always implied on an SRF, but including it
      // here makes re-printing the AST slightly tricky.
      As: $3.aliasClause(),
    }
  }
| LATERAL func_table opt_ordinality opt_alias_clause
  {
    f := $2.tblExpr()
    $$.val = &tree.AliasedTableExpr{
      Expr: f,
      Ordinality: $3.bool(),
      Lateral: true,
      As: $4.aliasClause(),
    }
  }
// The following syntax is a CockroachDB extension:
//     SELECT ... FROM [ EXPLAIN .... ] WHERE ...
//     SELECT ... FROM [ SHOW .... ] WHERE ...
//     SELECT ... FROM [ INSERT ... RETURNING ... ] WHERE ...
// A statement within square brackets can be used as a table expression (data source).
// We use square brackets for two reasons:
// - the grammar would be terribly ambiguous if we used simple
//   parentheses or no parentheses at all.
// - it carries visual semantic information, by marking the table
//   expression as radically different from the other things.
//   If a user does not know this and encounters this syntax, they
//   will know from the unusual choice that something rather different
//   is going on and may be pushed by the unusual syntax to
//   investigate further in the docs.
| '[' row_source_extension_stmt ']' opt_ordinality opt_alias_clause
  {
    $$.val = &tree.AliasedTableExpr{Expr: &tree.StatementSource{ Statement: $2.stmt() }, Ordinality: $4.bool(), As: $5.aliasClause() }
  }

numeric_table_ref:
  '[' iconst64 opt_tableref_col_list alias_clause ']'
  {
    /* SKIP DOC */
    $$.val = &tree.TableRef{
      TableID: $2.int64(),
      Columns: $3.tableRefCols(),
      As:      $4.aliasClause(),
    }
  }

func_table:
  func_expr_windowless
  {
    $$.val = &tree.RowsFromExpr{Items: tree.Exprs{$1.expr()}}
  }
| ROWS FROM '(' rowsfrom_list ')'
  {
    $$.val = &tree.RowsFromExpr{Items: $4.exprs()}
  }

rowsfrom_list:
  rowsfrom_item
  { $$.val = tree.Exprs{$1.expr()} }
| rowsfrom_list ',' rowsfrom_item
  { $$.val = append($1.exprs(), $3.expr()) }

rowsfrom_item:
  func_expr_windowless opt_func_alias_clause
  {
    $$.val = $1.expr()
  }

opt_col_def_list_no_types:
  '(' col_def_list_no_types ')'
  {
    $$.val = $2.colDefList()
  }
| /* EMPTY */
  {
    $$.val = tree.ColumnDefList(nil)
  }

col_def_list_no_types:
  name
  {
    $$.val = tree.ColumnDefList{tree.ColumnDef{Name: tree.Name($1)}}
  }
| col_def_list_no_types ',' name
  {
    $$.val = append($1.colDefList(), tree.ColumnDef{Name: tree.Name($3)})
  }


opt_col_def_list:
  /* EMPTY */
  {
    $$.val = tree.ColumnDefList(nil)
  }
| '(' col_def_list ')'
  {
    $$.val = $2.colDefList()
  }

col_def_list:
  col_def
  {
    $$.val = tree.ColumnDefList{$1.colDef()}
  }
| col_def_list ',' col_def
  {
    $$.val = append($1.colDefList(), $3.colDef())
  }

col_def:
  name
  {
    $$.val = tree.ColumnDef{Name: tree.Name($1)}
  }
| name typename
  {
    $$.val = tree.ColumnDef{Name: tree.Name($1), Type: $2.typeReference()}
  }

opt_tableref_col_list:
  /* EMPTY */               { $$.val = nil }
| '(' ')'                   { $$.val = []tree.ColumnID{} }
| '(' tableref_col_list ')' { $$.val = $2.tableRefCols() }

tableref_col_list:
  iconst64
  {
    $$.val = []tree.ColumnID{tree.ColumnID($1.int64())}
  }
| tableref_col_list ',' iconst64
  {
    $$.val = append($1.tableRefCols(), tree.ColumnID($3.int64()))
  }

opt_ordinality:
  WITH_LA ORDINALITY
  {
    $$.val = true
  }
| /* EMPTY */
  {
    $$.val = false
  }

// It may seem silly to separate joined_table from table_ref, but there is
// method in SQL's madness: if you don't do it this way you get reduce- reduce
// conflicts, because it's not clear to the parser generator whether to expect
// alias_clause after ')' or not. For the same reason we must treat 'JOIN' and
// 'join_type JOIN' separately, rather than allowing join_type to expand to
// empty; if we try it, the parser generator can't figure out when to reduce an
// empty join_type right after table_ref.
//
// Note that a CROSS JOIN is the same as an unqualified INNER JOIN, and an
// INNER JOIN/ON has the same shape but a qualification expression to limit
// membership. A NATURAL JOIN implicitly matches column names between tables
// and the shape is determined by which columns are in common. We'll collect
// columns during the later transformations.

joined_table:
  '(' joined_table ')'
  {
    $$.val = &tree.ParenTableExpr{Expr: $2.tblExpr()}
  }
| table_ref CROSS opt_join_hint JOIN table_ref
  {
    $$.val = &tree.JoinTableExpr{JoinType: tree.AstCross, Left: $1.tblExpr(), Right: $5.tblExpr(), Hint: $3}
  }
| table_ref join_type opt_join_hint JOIN table_ref join_qual
  {
    $$.val = &tree.JoinTableExpr{JoinType: $2, Left: $1.tblExpr(), Right: $5.tblExpr(), Cond: $6.joinCond(), Hint: $3}
  }
| table_ref JOIN table_ref join_qual
  {
    $$.val = &tree.JoinTableExpr{Left: $1.tblExpr(), Right: $3.tblExpr(), Cond: $4.joinCond()}
  }
| table_ref NATURAL join_type opt_join_hint JOIN table_ref
  {
    $$.val = &tree.JoinTableExpr{JoinType: $3, Left: $1.tblExpr(), Right: $6.tblExpr(), Cond: tree.NaturalJoinCond{}, Hint: $4}
  }
| table_ref NATURAL JOIN table_ref
  {
    $$.val = &tree.JoinTableExpr{Left: $1.tblExpr(), Right: $4.tblExpr(), Cond: tree.NaturalJoinCond{}}
  }

alias_clause:
  AS table_alias_name opt_col_def_list_no_types
  {
    $$.val = tree.AliasClause{Alias: tree.Name($2), Cols: $3.colDefList()}
  }
| table_alias_name opt_col_def_list_no_types
  {
    $$.val = tree.AliasClause{Alias: tree.Name($1), Cols: $2.colDefList()}
  }

opt_alias_clause:
  alias_clause
| /* EMPTY */
  {
    $$.val = tree.AliasClause{}
  }

func_alias_clause:
  AS table_alias_name opt_col_def_list
  {
    $$.val = tree.AliasClause{Alias: tree.Name($2), Cols: $3.colDefList()}
  }
| table_alias_name opt_col_def_list
  {
    $$.val = tree.AliasClause{Alias: tree.Name($1), Cols: $2.colDefList()}
  }

opt_func_alias_clause:
  func_alias_clause
| /* EMPTY */
  {
    $$.val = tree.AliasClause{}
  }

as_of_clause:
  AS_LA OF SYSTEM TIME a_expr
  {
    $$.val = tree.AsOfClause{Expr: $5.expr()}
  }

opt_as_of_clause:
  as_of_clause
| /* EMPTY */
  {
    $$.val = tree.AsOfClause{}
  }

join_type:
  FULL join_outer
  {
    $$ = tree.AstFull
  }
| LEFT join_outer
  {
    $$ = tree.AstLeft
  }
| RIGHT join_outer
  {
    $$ = tree.AstRight
  }
| INNER
  {
    $$ = tree.AstInner
  }

// OUTER is just noise...
join_outer:
  OUTER {}
| /* EMPTY */ {}

// Join hint specifies that the join in the query should use a
// specific method.

// The semantics are as follows:
//  - HASH forces a hash join; in other words, it disables merge and lookup
//    join. A hash join is always possible; even if there are no equality
//    columns - we consider cartesian join a degenerate case of the hash join
//    (one bucket).
//  - MERGE forces a merge join, even if it requires resorting both sides of
//    the join.
//  - LOOKUP forces a lookup join into the right side; the right side must be
//    a table with a suitable index. `LOOKUP` can only be used with INNER and
//    LEFT joins (though this is not enforced by the syntax).
//  - INVERTED forces an inverted join into the right side; the right side must
//    be a table with a suitable inverted index. `INVERTED` can only be used
//    with INNER and LEFT joins (though this is not enforced by the syntax).
//  - STRAIGHT forces the join order, but not the algorithm.
//  - If it is not possible to use the algorithm in the hint, an error is
//    returned.
//  - When a join hint is specified, the two tables will not be reordered
//    by the optimizer.
opt_join_hint:
  HASH
  {
    $$ = tree.AstHash
  }
| MERGE
  {
    $$ = tree.AstMerge
  }
| LOOKUP
  {
    $$ = tree.AstLookup
  }
| INVERTED
  {
    $$ = tree.AstInverted
  }
| STRAIGHT
  {
    $$ = tree.AstStraight
  }
| /* EMPTY */
  {
    $$ = ""
  }

// JOIN qualification clauses
// Possibilities are:
//      USING ( column list ) allows only unqualified column names,
//          which must match between tables.
//      ON expr allows more general qualifications.
//
// We return USING as a List node, while an ON-expr will not be a List.
join_qual:
  USING '(' name_list ')'
  {
    $$.val = &tree.UsingJoinCond{Cols: $3.nameList()}
  }
| ON a_expr
  {
    $$.val = &tree.OnJoinCond{Expr: $2.expr()}
  }

relation_expr:
  table_name              { $$.val = $1.unresolvedObjectName() }
| table_name '*'          { $$.val = $1.unresolvedObjectName() }
| ONLY table_name         { $$.val = $2.unresolvedObjectName() }
| ONLY '(' table_name ')' { $$.val = $3.unresolvedObjectName() }

relation_expr_list:
  relation_expr
  {
    name := $1.unresolvedObjectName().ToTableName()
    $$.val = tree.TableNames{name}
  }
| relation_expr_list ',' relation_expr
  {
    name := $3.unresolvedObjectName().ToTableName()
    $$.val = append($1.tableNames(), name)
  }

// UNLISTEN
unlisten_stmt:
   UNLISTEN type_name
    {
        $$.val = &tree.Unlisten{ ChannelName: $2.unresolvedObjectName(), Star: false}
    }
|  UNLISTEN '*'
      {
          $$.val = &tree.Unlisten{ ChannelName:nil, Star: true}
      }


// Given "UPDATE foo set set ...", we have to decide without looking any
// further ahead whether the first "set" is an alias or the UPDATE's SET
// keyword. Since "set" is allowed as a column name both interpretations are
// feasible. We resolve the shift/reduce conflict by giving the first
// table_expr_opt_alias_idx production a higher precedence than the SET token
// has, causing the parser to prefer to reduce, in effect assuming that the SET
// is not an alias.
table_expr_opt_alias_idx:
  table_name_opt_idx %prec UMINUS
  {
     $$.val = $1.tblExpr()
  }
| table_name_opt_idx table_alias_name
  {
     alias := $1.tblExpr().(*tree.AliasedTableExpr)
     alias.As = tree.AliasClause{Alias: tree.Name($2)}
     $$.val = alias
  }
| table_name_opt_idx AS table_alias_name
  {
     alias := $1.tblExpr().(*tree.AliasedTableExpr)
     alias.As = tree.AliasClause{Alias: tree.Name($3)}
     $$.val = alias
  }
| numeric_table_ref opt_index_flags
  {
    /* SKIP DOC */
    $$.val = &tree.AliasedTableExpr{
      Expr: $1.tblExpr(),
      IndexFlags: $2.indexFlags(),
    }
  }

table_name_opt_idx:
  opt_only table_name opt_index_flags opt_descendant
  {
    name := $2.unresolvedObjectName().ToTableName()
    $$.val = &tree.AliasedTableExpr{
      Expr: &name,
      IndexFlags: $3.indexFlags(),
    }
  }

opt_only:
	ONLY
  {
    $$.val = true
  }
| /* EMPTY */
  {
    $$.val = false
  }

opt_descendant:
	'*'
  {
    $$.val = true
  }
| /* EMPTY */
  {
    $$.val = false
  }

where_clause:
  WHERE a_expr
  {
    $$.val = $2.expr()
  }

opt_where_clause:
  where_clause
| /* EMPTY */
  {
    $$.val = tree.Expr(nil)
  }

// Type syntax
//   SQL introduces a large amount of type-specific syntax.
//   Define individual clauses to handle these cases, and use
//   the generic case to handle regular type-extensible Postgres syntax.
//   - thomas 1997-10-10

typename:
  simple_typename opt_array_bounds
  {
    if bounds := $2.int32s(); bounds != nil {
      var err error
      $$.val, err = arrayOf($1.typeReference(), bounds)
      if err != nil {
        return setErr(sqllex, err)
      }
    } else {
      $$.val = $1.typeReference()
    }
  }
  // SQL standard syntax, currently only one-dimensional
  // Undocumented but support for potential Postgres compat
| simple_typename ARRAY '[' ICONST ']' {
    /* SKIP DOC */
    var err error
    $$.val, err = arrayOf($1.typeReference(), nil)
    if err != nil {
      return setErr(sqllex, err)
    }
  }
| simple_typename ARRAY '[' ICONST ']' '[' error { return unimplementedWithIssue(sqllex, 32552) }
| simple_typename ARRAY {
    var err error
    $$.val, err = arrayOf($1.typeReference(), nil)
    if err != nil {
      return setErr(sqllex, err)
    }
  }

cast_target:
  typename
  {
    $$.val = $1.typeReference()
  }

opt_array_bounds:
  // TODO(justin): reintroduce multiple array bounds
  // opt_array_bounds '[' ']' { $$.val = append($1.int32s(), -1) }
  '[' ']' { $$.val = []int32{-1} }
| '[' ']' '[' error { return unimplementedWithIssue(sqllex, 32552) }
| '[' ICONST ']'
  {
    /* SKIP DOC */
    bound, err := $2.numVal().AsInt32()
    if err != nil {
      return setErr(sqllex, err)
    }
    $$.val = []int32{bound}
  }
| '[' ICONST ']' '[' error { return unimplementedWithIssue(sqllex, 32552) }
| /* EMPTY */ { $$.val = []int32(nil) }

// general_type_name is a variant of type_or_function_name but does not
// include some extra keywords (like FAMILY) which cause ambiguity with
// parsing of typenames in certain contexts.
general_type_name:
  type_function_name_no_crdb_extra

// complex_type_name mirrors the rule for complex_db_object_name, but uses
// general_type_name rather than db_object_name_component to avoid conflicts.
complex_type_name:
  general_type_name '.' unrestricted_name
  {
    aIdx := sqllex.(*lexer).NewAnnotation()
    res, err := tree.NewUnresolvedObjectName(2, [3]string{$3, $1}, aIdx)
    if err != nil { return setErr(sqllex, err) }
    $$.val = res
  }
| general_type_name '.' unrestricted_name '.' unrestricted_name
  {
    aIdx := sqllex.(*lexer).NewAnnotation()
    res, err := tree.NewUnresolvedObjectName(3, [3]string{$5, $3, $1}, aIdx)
    if err != nil { return setErr(sqllex, err) }
    $$.val = res
  }

simple_typename:
  general_type_name
  {
    /* FORCE DOC */
    // See https://www.postgresql.org/docs/9.1/static/datatype-character.html
    // Postgres supports a special character type named "char" (with the quotes)
    // that is a single-character column type. It's used by system tables.
    // Eventually this clause will be used to parse user-defined types as well,
    // since their names can be quoted.
    if $1 == "char" {
      $$.val = types.QChar
    } else if $1 == "serial" {
        switch sqllex.(*lexer).nakedIntType.Width() {
        case 32:
          $$.val = &types.Serial4Type
        default:
          $$.val = &types.Serial8Type
        }
    } else {
      // Check the the type is one of our "non-keyword" type names.
      // Otherwise, package it up as a type reference for later.
      var ok bool
      var err error
      var unimp int
      $$.val, ok, unimp = types.TypeForNonKeywordTypeName($1)
      if !ok {
        switch unimp {
          case 0:
            // In this case, we don't think this type is one of our
            // known unsupported types, so make a type reference for it.
            aIdx := sqllex.(*lexer).NewAnnotation()
            $$.val, err = tree.NewUnresolvedObjectName(1, [3]string{$1}, aIdx)
            if err != nil { return setErr(sqllex, err) }
          case -1:
            return unimplemented(sqllex, "type name " + $1)
          default:
            return unimplementedWithIssueDetail(sqllex, unimp, $1)
        }
      }
    }
  }
| '@' iconst32
  {
    id := $2.int32()
    $$.val = &tree.OIDTypeReference{OID: oid.Oid(id)}
  }
| complex_type_name
  {
    $$.val = $1.typeReference()
  }
| const_typename
| interval_type
| POINT error { return unimplementedWithIssueDetail(sqllex, 21286, "point") } // needed or else it generates a syntax error.
| POLYGON error { return unimplementedWithIssueDetail(sqllex, 21286, "polygon") } // needed or else it generates a syntax error.

geo_shape_type:
  POINT { $$.val = geopb.ShapeType_Point }
| POINTM { $$.val = geopb.ShapeType_PointM }
| POINTZ { $$.val = geopb.ShapeType_PointZ }
| POINTZM { $$.val = geopb.ShapeType_PointZM }
| LINESTRING { $$.val = geopb.ShapeType_LineString }
| LINESTRINGM { $$.val = geopb.ShapeType_LineStringM }
| LINESTRINGZ { $$.val = geopb.ShapeType_LineStringZ }
| LINESTRINGZM { $$.val = geopb.ShapeType_LineStringZM }
| POLYGON { $$.val = geopb.ShapeType_Polygon }
| POLYGONM { $$.val = geopb.ShapeType_PolygonM }
| POLYGONZ { $$.val = geopb.ShapeType_PolygonZ }
| POLYGONZM { $$.val = geopb.ShapeType_PolygonZM }
| MULTIPOINT { $$.val = geopb.ShapeType_MultiPoint }
| MULTIPOINTM { $$.val = geopb.ShapeType_MultiPointM }
| MULTIPOINTZ { $$.val = geopb.ShapeType_MultiPointZ }
| MULTIPOINTZM { $$.val = geopb.ShapeType_MultiPointZM }
| MULTILINESTRING { $$.val = geopb.ShapeType_MultiLineString }
| MULTILINESTRINGM { $$.val = geopb.ShapeType_MultiLineStringM }
| MULTILINESTRINGZ { $$.val = geopb.ShapeType_MultiLineStringZ }
| MULTILINESTRINGZM { $$.val = geopb.ShapeType_MultiLineStringZM }
| MULTIPOLYGON { $$.val = geopb.ShapeType_MultiPolygon }
| MULTIPOLYGONM { $$.val = geopb.ShapeType_MultiPolygonM }
| MULTIPOLYGONZ { $$.val = geopb.ShapeType_MultiPolygonZ }
| MULTIPOLYGONZM { $$.val = geopb.ShapeType_MultiPolygonZM }
| GEOMETRYCOLLECTION { $$.val = geopb.ShapeType_GeometryCollection }
| GEOMETRYCOLLECTIONM { $$.val = geopb.ShapeType_GeometryCollectionM }
| GEOMETRYCOLLECTIONZ { $$.val = geopb.ShapeType_GeometryCollectionZ }
| GEOMETRYCOLLECTIONZM { $$.val = geopb.ShapeType_GeometryCollectionZM }
| GEOMETRY { $$.val = geopb.ShapeType_Geometry }
| GEOMETRYM { $$.val = geopb.ShapeType_GeometryM }
| GEOMETRYZ { $$.val = geopb.ShapeType_GeometryZ }
| GEOMETRYZM { $$.val = geopb.ShapeType_GeometryZM }

const_geo:
  GEOGRAPHY { $$.val = types.Geography }
| GEOMETRY  { $$.val = types.Geometry }
| BOX2D     { $$.val = types.Box2D }
| GEOMETRY '(' geo_shape_type ')'
  {
    $$.val = types.MakeGeometry($3.geoShapeType(), 0)
  }
| GEOGRAPHY '(' geo_shape_type ')'
  {
    $$.val = types.MakeGeography($3.geoShapeType(), 0)
  }
| GEOMETRY '(' geo_shape_type ',' signed_iconst ')'
  {
    val, err := $5.numVal().AsInt32()
    if err != nil {
      return setErr(sqllex, err)
    }
    $$.val = types.MakeGeometry($3.geoShapeType(), geopb.SRID(val))
  }
| GEOGRAPHY '(' geo_shape_type ',' signed_iconst ')'
  {
    val, err := $5.numVal().AsInt32()
    if err != nil {
      return setErr(sqllex, err)
    }
    $$.val = types.MakeGeography($3.geoShapeType(), geopb.SRID(val))
  }

const_vector:
  VECTOR { $$.val = types.PGVector }
| VECTOR '(' iconst32 ')'
  {
    dims := $3.int32()
    if dims <= 0 {
      sqllex.Error("dimensions for type vector must be at least 1")
      return 1
    } else if dims > vector.MaxDim {
      sqllex.Error(fmt.Sprintf("dimensions for type vector cannot exceed %d", vector.MaxDim))
      return 1
    }
    $$.val = types.MakePGVector(dims)
  }

// We have a separate const_typename to allow defaulting fixed-length types such
// as CHAR() and BIT() to an unspecified length. SQL9x requires that these
// default to a length of one, but this makes no sense for constructs like CHAR
// 'hi' and BIT '0101', where there is an obvious better choice to make. This
// rule *also* supports length-specified types like BIT(1), CHAR(3), etc. Note
// that interval_type is not included here since it must be pushed up higher in
// the rules to accommodate the postfix options (e.g. INTERVAL '1' YEAR).
// Likewise, we have to handle the generic-type-name case in a_expr_const to
// avoid premature reduce/reduce conflicts against function names.
const_typename:
  numeric
| bit_without_length
| bit_with_length
| character_without_length
| character_with_length
| const_datetime
| const_geo
| const_vector

opt_numeric_modifiers:
  '(' iconst32 ')'
  {
    dec, err := newDecimal($2.int32(), 0)
    if err != nil {
      return setErr(sqllex, err)
    }
    $$.val = dec
  }
| '(' iconst32 ',' iconst32 ')'
  {
    dec, err := newDecimal($2.int32(), $4.int32())
    if err != nil {
      return setErr(sqllex, err)
    }
    $$.val = dec
  }
| /* EMPTY */
  {
    $$.val = nil
  }

// SQL numeric data types
numeric:
  INT
  {
    $$.val = sqllex.(*lexer).nakedIntType
  }
| INTEGER
  {
    $$.val = sqllex.(*lexer).nakedIntType
  }
| SMALLINT
  {
    $$.val = types.Int2
  }
| BIGINT
  {
    $$.val = types.Int
  }
| REAL
  {
    $$.val = types.Float4
  }
| FLOAT opt_float
  {
    $$.val = $2.colType()
  }
| DOUBLE PRECISION
  {
    $$.val = types.Float
  }
| DECIMAL opt_numeric_modifiers
  {
    typ := $2.colType()
    if typ == nil {
      typ = types.Decimal
    }
    $$.val = typ
  }
| DEC opt_numeric_modifiers
  {
    typ := $2.colType()
    if typ == nil {
      typ = types.Decimal
    }
    $$.val = typ
  }
| NUMERIC opt_numeric_modifiers
  {
    typ := $2.colType()
    if typ == nil {
      typ = types.Decimal
    }
    $$.val = typ
  }
| BOOLEAN
  {
    $$.val = types.Bool
  }

opt_float:
  '(' ICONST ')'
  {
    nv := $2.numVal()
    prec, err := nv.AsInt64()
    if err != nil {
      return setErr(sqllex, err)
    }
    typ, err := newFloat(prec)
    if err != nil {
      return setErr(sqllex, err)
    }
    $$.val = typ
  }
| /* EMPTY */
  {
    $$.val = types.Float
  }

bit_with_length:
  BIT opt_varying '(' iconst32 ')'
  {
    bit, err := newBitType($4.int32(), $2.bool())
    if err != nil { return setErr(sqllex, err) }
    $$.val = bit
  }
| VARBIT '(' iconst32 ')'
  {
    bit, err := newBitType($3.int32(), true)
    if err != nil { return setErr(sqllex, err) }
    $$.val = bit
  }

bit_without_length:
  BIT
  {
    $$.val = types.MakeBit(1)
  }
| BIT VARYING
  {
    $$.val = types.VarBit
  }
| VARBIT
  {
    $$.val = types.VarBit
  }

character_with_length:
  character_base '(' iconst32 ')'
  {
    colTyp := *$1.colType()
    n := $3.int32()
    if n == 0 {
      sqllex.Error(fmt.Sprintf("length for type %s must be at least 1", colTyp.SQLString()))
      return 1
    }
    $$.val = types.MakeScalar(types.StringFamily, colTyp.Oid(), colTyp.Precision(), n, colTyp.Locale())
  }

character_without_length:
  character_base
  {
    $$.val = $1.colType()
  }

character_base:
  char_aliases
  {
    $$.val = types.MakeChar(1)
  }
| char_aliases VARYING
  {
    $$.val = types.VarChar
  }
| VARCHAR
  {
    $$.val = types.VarChar
  }
| STRING
  {
    $$.val = types.String
  }

char_aliases:
  CHAR
| CHARACTER

opt_varying:
  VARYING     { $$.val = true }
| /* EMPTY */ { $$.val = false }

// SQL date/time types
const_datetime:
  DATE
  {
    $$.val = types.Date
  }
| TIME opt_timezone
  {
    if $2.bool() {
      $$.val = types.TimeTZ
    } else {
      $$.val = types.Time
    }
  }
| TIME '(' iconst32 ')' opt_timezone
  {
    prec := $3.int32()
    if prec < 0 || prec > 6 {
      sqllex.Error(fmt.Sprintf("precision %d out of range", prec))
      return 1
    }
    if $5.bool() {
      $$.val = types.MakeTimeTZ(prec)
    } else {
      $$.val = types.MakeTime(prec)
    }
  }
| TIMETZ                             { $$.val = types.TimeTZ }
| TIMETZ '(' iconst32 ')'
  {
    prec := $3.int32()
    if prec < 0 || prec > 6 {
      sqllex.Error(fmt.Sprintf("precision %d out of range", prec))
      return 1
    }
    $$.val = types.MakeTimeTZ(prec)
  }
| TIMESTAMP opt_timezone
  {
    if $2.bool() {
      $$.val = types.TimestampTZ
    } else {
      $$.val = types.Timestamp
    }
  }
| TIMESTAMP '(' iconst32 ')' opt_timezone
  {
    prec := $3.int32()
    if prec < 0 || prec > 6 {
      sqllex.Error(fmt.Sprintf("precision %d out of range", prec))
      return 1
    }
    if $5.bool() {
      $$.val = types.MakeTimestampTZ(prec)
    } else {
      $$.val = types.MakeTimestamp(prec)
    }
  }
| TIMESTAMPTZ
  {
    $$.val = types.TimestampTZ
  }
| TIMESTAMPTZ '(' iconst32 ')'
  {
    prec := $3.int32()
    if prec < 0 || prec > 6 {
      sqllex.Error(fmt.Sprintf("precision %d out of range", prec))
      return 1
    }
    $$.val = types.MakeTimestampTZ(prec)
  }

opt_timezone:
  WITH_LA TIME ZONE { $$.val = true; }
| WITHOUT TIME ZONE { $$.val = false; }
| /*EMPTY*/         { $$.val = false; }

interval_type:
  INTERVAL
  {
    $$.val = types.Interval
  }
| INTERVAL interval_qualifier
  {
    $$.val = types.MakeInterval($2.intervalTypeMetadata())
  }
| INTERVAL '(' iconst32 ')'
  {
    prec := $3.int32()
    if prec < 0 || prec > 6 {
      sqllex.Error(fmt.Sprintf("precision %d out of range", prec))
      return 1
    }
    $$.val = types.MakeInterval(types.IntervalTypeMetadata{Precision: prec, PrecisionIsSet: true})
  }

interval_qualifier:
  YEAR %prec INTERVAL_SIMPLE
  {
    $$.val = types.IntervalTypeMetadata{
      DurationField: types.IntervalDurationField{
        DurationType: types.IntervalDurationType_YEAR,
      },
    }
  }
| MONTH %prec INTERVAL_SIMPLE
  {
    $$.val = types.IntervalTypeMetadata{
      DurationField: types.IntervalDurationField{
        DurationType: types.IntervalDurationType_MONTH,
      },
    }
  }
| DAY %prec INTERVAL_SIMPLE
  {
    $$.val = types.IntervalTypeMetadata{
      DurationField: types.IntervalDurationField{
        DurationType: types.IntervalDurationType_DAY,
      },
    }
  }
| HOUR %prec INTERVAL_SIMPLE
  {
    $$.val = types.IntervalTypeMetadata{
      DurationField: types.IntervalDurationField{
        DurationType: types.IntervalDurationType_HOUR,
      },
    }
  }
| MINUTE %prec INTERVAL_SIMPLE
  {
    $$.val = types.IntervalTypeMetadata{
      DurationField: types.IntervalDurationField{
        DurationType: types.IntervalDurationType_MINUTE,
      },
    }
  }
| interval_second
  {
    $$.val = $1.intervalTypeMetadata()
  }
// Like Postgres, we ignore the left duration field. See explanation:
// https://www.postgresql.org/message-id/20110510040219.GD5617%40tornado.gateway.2wire.net
| YEAR TO MONTH %prec TO
  {
    $$.val = types.IntervalTypeMetadata{
      DurationField: types.IntervalDurationField{
        FromDurationType: types.IntervalDurationType_YEAR,
        DurationType: types.IntervalDurationType_MONTH,
      },
    }
  }
| DAY TO HOUR %prec TO
  {
    $$.val = types.IntervalTypeMetadata{
      DurationField: types.IntervalDurationField{
        FromDurationType: types.IntervalDurationType_DAY,
        DurationType: types.IntervalDurationType_HOUR,
      },
    }
  }
| DAY TO MINUTE %prec TO
  {
    $$.val = types.IntervalTypeMetadata{
      DurationField: types.IntervalDurationField{
        FromDurationType: types.IntervalDurationType_DAY,
        DurationType: types.IntervalDurationType_MINUTE,
      },
    }
  }
| DAY TO interval_second %prec TO
  {
    ret := $3.intervalTypeMetadata()
    ret.DurationField.FromDurationType = types.IntervalDurationType_DAY
    $$.val = ret
  }
| HOUR TO MINUTE %prec TO
  {
    $$.val = types.IntervalTypeMetadata{
      DurationField: types.IntervalDurationField{
        FromDurationType: types.IntervalDurationType_HOUR,
        DurationType: types.IntervalDurationType_MINUTE,
      },
    }
  }
| HOUR TO interval_second %prec TO
  {
    ret := $3.intervalTypeMetadata()
    ret.DurationField.FromDurationType = types.IntervalDurationType_HOUR
    $$.val = ret
  }
| MINUTE TO interval_second %prec TO
  {
    $$.val = $3.intervalTypeMetadata()
    ret := $3.intervalTypeMetadata()
    ret.DurationField.FromDurationType = types.IntervalDurationType_MINUTE
    $$.val = ret
  }

opt_interval_qualifier:
  interval_qualifier
| /* EMPTY */
  {
    $$.val = nil
  }

interval_second:
  SECOND
  {
    $$.val = types.IntervalTypeMetadata{
      DurationField: types.IntervalDurationField{
        DurationType: types.IntervalDurationType_SECOND,
      },
    }
  }
| SECOND '(' iconst32 ')'
  {
    prec := $3.int32()
    if prec < 0 || prec > 6 {
      sqllex.Error(fmt.Sprintf("precision %d out of range", prec))
      return 1
    }
    $$.val = types.IntervalTypeMetadata{
      DurationField: types.IntervalDurationField{
        DurationType: types.IntervalDurationType_SECOND,
      },
      PrecisionIsSet: true,
      Precision: prec,
    }
  }

// General expressions. This is the heart of the expression syntax.
//
// We have two expression types: a_expr is the unrestricted kind, and b_expr is
// a subset that must be used in some places to avoid shift/reduce conflicts.
// For example, we can't do BETWEEN as "BETWEEN a_expr AND a_expr" because that
// use of AND conflicts with AND as a boolean operator. So, b_expr is used in
// BETWEEN and we remove boolean keywords from b_expr.
//
// Note that '(' a_expr ')' is a b_expr, so an unrestricted expression can
// always be used by surrounding it with parens.
//
// c_expr is all the productions that are common to a_expr and b_expr; it's
// factored out just to eliminate redundant coding.
//
// Be careful of productions involving more than one terminal token. By
// default, bison will assign such productions the precedence of their last
// terminal, but in nearly all cases you want it to be the precedence of the
// first terminal instead; otherwise you will not get the behavior you expect!
// So we use %prec annotations freely to set precedences.
a_expr:
  c_expr
| a_expr TYPECAST cast_target
  {
    $$.val = &tree.CastExpr{Expr: $1.expr(), Type: $3.typeReference(), SyntaxMode: tree.CastShort}
  }
| a_expr TYPEANNOTATE typename
  {
    $$.val = &tree.AnnotateTypeExpr{Expr: $1.expr(), Type: $3.typeReference(), SyntaxMode: tree.AnnotateShort}
  }
| a_expr COLLATE collation_name
  {
    $$.val = &tree.CollateExpr{Expr: $1.expr(), Locale: $3}
  }
| a_expr AT TIME ZONE a_expr %prec AT
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction("timezone"), Exprs: tree.Exprs{$5.expr(), $1.expr()}}
  }
  // These operators must be called out explicitly in order to make use of
  // bison's automatic operator-precedence handling. All other operator names
  // are handled by the generic productions using "OP", below; and all those
  // operators will have the same precedence.
  //
  // If you add more explicitly-known operators, be sure to add them also to
  // b_expr and to the all_op list below.
| '+' a_expr %prec UMINUS
  {
    // Unary plus is a no-op. Desugar immediately.
    $$.val = $2.expr()
  }
| '-' a_expr %prec UMINUS
  {
    $$.val = unaryNegation($2.expr())
  }
| '~' a_expr %prec UMINUS
  {
    $$.val = &tree.UnaryExpr{Operator: tree.MakeUnaryOperator(tree.UnaryComplement), Expr: $2.expr()}
  }
| SQRT a_expr
  {
    $$.val = &tree.UnaryExpr{Operator: tree.MakeUnaryOperator(tree.UnarySqrt), Expr: $2.expr()}
  }
| CBRT a_expr
  {
    $$.val = &tree.UnaryExpr{Operator: tree.MakeUnaryOperator(tree.UnaryCbrt), Expr: $2.expr()}
  }
| a_expr '+' a_expr
  {
    $$.val = &tree.BinaryExpr{Operator: treebin.MakeBinaryOperator(treebin.Plus), Left: $1.expr(), Right: $3.expr()}
  }
| a_expr '-' a_expr
  {
    $$.val = &tree.BinaryExpr{Operator: treebin.MakeBinaryOperator(treebin.Minus), Left: $1.expr(), Right: $3.expr()}
  }
| a_expr '*' a_expr
  {
    $$.val = &tree.BinaryExpr{Operator: treebin.MakeBinaryOperator(treebin.Mult), Left: $1.expr(), Right: $3.expr()}
  }
| a_expr '/' a_expr
  {
    $$.val = &tree.BinaryExpr{Operator: treebin.MakeBinaryOperator(treebin.Div), Left: $1.expr(), Right: $3.expr()}
  }
| a_expr FLOORDIV a_expr
  {
    $$.val = &tree.BinaryExpr{Operator: treebin.MakeBinaryOperator(treebin.FloorDiv), Left: $1.expr(), Right: $3.expr()}
  }
| a_expr '%' a_expr
  {
    $$.val = &tree.BinaryExpr{Operator: treebin.MakeBinaryOperator(treebin.Mod), Left: $1.expr(), Right: $3.expr()}
  }
| a_expr '^' a_expr
  {
    $$.val = &tree.BinaryExpr{Operator: treebin.MakeBinaryOperator(treebin.Pow), Left: $1.expr(), Right: $3.expr()}
  }
| a_expr '#' a_expr
  {
    $$.val = &tree.BinaryExpr{Operator: treebin.MakeBinaryOperator(treebin.Bitxor), Left: $1.expr(), Right: $3.expr()}
  }
| a_expr '&' a_expr
  {
    $$.val = &tree.BinaryExpr{Operator: treebin.MakeBinaryOperator(treebin.Bitand), Left: $1.expr(), Right: $3.expr()}
  }
| a_expr '|' a_expr
  {
    $$.val = &tree.BinaryExpr{Operator: treebin.MakeBinaryOperator(treebin.Bitor), Left: $1.expr(), Right: $3.expr()}
  }
| a_expr '<' a_expr
  {
    $$.val = &tree.ComparisonExpr{Operator: treecmp.MakeComparisonOperator(treecmp.LT), Left: $1.expr(), Right: $3.expr()}
  }
| a_expr '>' a_expr
  {
    $$.val = &tree.ComparisonExpr{Operator: treecmp.MakeComparisonOperator(treecmp.GT), Left: $1.expr(), Right: $3.expr()}
  }
| a_expr '?' a_expr
  {
    $$.val = &tree.ComparisonExpr{Operator: treecmp.MakeComparisonOperator(treecmp.JSONExists), Left: $1.expr(), Right: $3.expr()}
  }
| a_expr JSON_SOME_EXISTS a_expr
  {
    $$.val = &tree.ComparisonExpr{Operator: treecmp.MakeComparisonOperator(treecmp.JSONSomeExists), Left: $1.expr(), Right: $3.expr()}
  }
| a_expr JSON_ALL_EXISTS a_expr
  {
    $$.val = &tree.ComparisonExpr{Operator: treecmp.MakeComparisonOperator(treecmp.JSONAllExists), Left: $1.expr(), Right: $3.expr()}
  }
| a_expr CONTAINS a_expr
  {
    $$.val = &tree.ComparisonExpr{Operator: treecmp.MakeComparisonOperator(treecmp.Contains), Left: $1.expr(), Right: $3.expr()}
  }
| a_expr CONTAINED_BY a_expr
  {
    $$.val = &tree.ComparisonExpr{Operator: treecmp.MakeComparisonOperator(treecmp.ContainedBy), Left: $1.expr(), Right: $3.expr()}
  }
| a_expr '=' a_expr
  {
    $$.val = &tree.ComparisonExpr{Operator: treecmp.MakeComparisonOperator(treecmp.EQ), Left: $1.expr(), Right: $3.expr()}
  }
| a_expr CONCAT a_expr
  {
    $$.val = &tree.BinaryExpr{Operator: treebin.MakeBinaryOperator(treebin.Concat), Left: $1.expr(), Right: $3.expr()}
  }
| a_expr LSHIFT a_expr
  {
    $$.val = &tree.BinaryExpr{Operator: treebin.MakeBinaryOperator(treebin.LShift), Left: $1.expr(), Right: $3.expr()}
  }
| a_expr RSHIFT a_expr
  {
    $$.val = &tree.BinaryExpr{Operator: treebin.MakeBinaryOperator(treebin.RShift), Left: $1.expr(), Right: $3.expr()}
  }
| a_expr FETCHVAL a_expr
  {
    $$.val = &tree.BinaryExpr{Operator: treebin.MakeBinaryOperator(treebin.JSONFetchVal), Left: $1.expr(), Right: $3.expr()}
  }
| a_expr FETCHTEXT a_expr
  {
    $$.val = &tree.BinaryExpr{Operator: treebin.MakeBinaryOperator(treebin.JSONFetchText), Left: $1.expr(), Right: $3.expr()}
  }
| a_expr FETCHVAL_PATH a_expr
  {
    $$.val = &tree.BinaryExpr{Operator: treebin.MakeBinaryOperator(treebin.JSONFetchValPath), Left: $1.expr(), Right: $3.expr()}
  }
| a_expr FETCHTEXT_PATH a_expr
  {
    $$.val = &tree.BinaryExpr{Operator: treebin.MakeBinaryOperator(treebin.JSONFetchTextPath), Left: $1.expr(), Right: $3.expr()}
  }
| a_expr REMOVE_PATH a_expr
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction("json_remove_path"), Exprs: tree.Exprs{$1.expr(), $3.expr()}}
  }
| a_expr INET_CONTAINED_BY_OR_EQUALS a_expr
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction("inet_contained_by_or_equals"), Exprs: tree.Exprs{$1.expr(), $3.expr()}}
  }
| a_expr AND_AND a_expr
  {
    $$.val = &tree.ComparisonExpr{Operator: treecmp.MakeComparisonOperator(treecmp.Overlaps), Left: $1.expr(), Right: $3.expr()}
  }
| a_expr AT_AT a_expr
  {
    $$.val = &tree.ComparisonExpr{Operator: treecmp.MakeComparisonOperator(treecmp.TSMatches), Left: $1.expr(), Right: $3.expr()}
  }
| a_expr DISTANCE a_expr
  {
    $$.val = &tree.BinaryExpr{Operator: treebin.MakeBinaryOperator(treebin.Distance), Left: $1.expr(), Right: $3.expr()}
  }
| a_expr COS_DISTANCE a_expr
  {
    $$.val = &tree.BinaryExpr{Operator: treebin.MakeBinaryOperator(treebin.CosDistance), Left: $1.expr(), Right: $3.expr()}
  }
| a_expr NEG_INNER_PRODUCT a_expr
  {
    $$.val = &tree.BinaryExpr{Operator: treebin.MakeBinaryOperator(treebin.NegInnerProduct), Left: $1.expr(), Right: $3.expr()}
  }
| a_expr INET_CONTAINS_OR_EQUALS a_expr
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction("inet_contains_or_equals"), Exprs: tree.Exprs{$1.expr(), $3.expr()}}
  }
| a_expr LESS_EQUALS a_expr
  {
    $$.val = &tree.ComparisonExpr{Operator: treecmp.MakeComparisonOperator(treecmp.LE), Left: $1.expr(), Right: $3.expr()}
  }
| a_expr GREATER_EQUALS a_expr
  {
    $$.val = &tree.ComparisonExpr{Operator: treecmp.MakeComparisonOperator(treecmp.GE), Left: $1.expr(), Right: $3.expr()}
  }
| a_expr NOT_EQUALS a_expr
  {
    $$.val = &tree.ComparisonExpr{Operator: treecmp.MakeComparisonOperator(treecmp.NE), Left: $1.expr(), Right: $3.expr()}
  }
| qual_op a_expr %prec CBRT
  {
    var retCode int
    $$.val, retCode = processUnaryQualOp(sqllex, $1.op(), $2.expr())
    if retCode != 0 {
      return retCode
    }
  }
| a_expr qual_op a_expr %prec CBRT
  {
    {
      var retCode int
      $$.val, retCode = processBinaryQualOp(sqllex, $2.op(), $1.expr(), $3.expr())
      if retCode != 0 {
        return retCode
      }
    }
  }
| a_expr AND a_expr
  {
    $$.val = &tree.AndExpr{Left: $1.expr(), Right: $3.expr()}
  }
| a_expr OR a_expr
  {
    $$.val = &tree.OrExpr{Left: $1.expr(), Right: $3.expr()}
  }
| NOT a_expr
  {
    $$.val = &tree.NotExpr{Expr: $2.expr()}
  }
| NOT_LA a_expr %prec NOT
  {
    $$.val = &tree.NotExpr{Expr: $2.expr()}
  }
| a_expr LIKE a_expr
  {
    $$.val = &tree.ComparisonExpr{Operator: treecmp.MakeComparisonOperator(treecmp.Like), Left: $1.expr(), Right: $3.expr()}
  }
| a_expr LIKE a_expr ESCAPE a_expr %prec ESCAPE
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction("like_escape"), Exprs: tree.Exprs{$1.expr(), $3.expr(), $5.expr()}}
  }
| a_expr NOT_LA LIKE a_expr %prec NOT_LA
  {
    $$.val = &tree.ComparisonExpr{Operator: treecmp.MakeComparisonOperator(treecmp.NotLike), Left: $1.expr(), Right: $4.expr()}
  }
| a_expr NOT_LA LIKE a_expr ESCAPE a_expr %prec ESCAPE
 {
   $$.val = &tree.FuncExpr{Func: tree.WrapFunction("not_like_escape"), Exprs: tree.Exprs{$1.expr(), $4.expr(), $6.expr()}}
 }
| a_expr ILIKE a_expr
  {
    $$.val = &tree.ComparisonExpr{Operator: treecmp.MakeComparisonOperator(treecmp.ILike), Left: $1.expr(), Right: $3.expr()}
  }
| a_expr ILIKE a_expr ESCAPE a_expr %prec ESCAPE
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction("ilike_escape"), Exprs: tree.Exprs{$1.expr(), $3.expr(), $5.expr()}}
  }
| a_expr NOT_LA ILIKE a_expr %prec NOT_LA
  {
    $$.val = &tree.ComparisonExpr{Operator: treecmp.MakeComparisonOperator(treecmp.NotILike), Left: $1.expr(), Right: $4.expr()}
  }
| a_expr NOT_LA ILIKE a_expr ESCAPE a_expr %prec ESCAPE
 {
   $$.val = &tree.FuncExpr{Func: tree.WrapFunction("not_ilike_escape"), Exprs: tree.Exprs{$1.expr(), $4.expr(), $6.expr()}}
 }
| a_expr SIMILAR TO a_expr %prec SIMILAR
  {
    $$.val = &tree.ComparisonExpr{Operator: treecmp.MakeComparisonOperator(treecmp.SimilarTo), Left: $1.expr(), Right: $4.expr()}
  }
| a_expr SIMILAR TO a_expr ESCAPE a_expr %prec ESCAPE
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction("similar_to_escape"), Exprs: tree.Exprs{$1.expr(), $4.expr(), $6.expr()}}
  }
| a_expr NOT_LA SIMILAR TO a_expr %prec NOT_LA
  {
    $$.val = &tree.ComparisonExpr{Operator: treecmp.MakeComparisonOperator(treecmp.NotSimilarTo), Left: $1.expr(), Right: $5.expr()}
  }
| a_expr NOT_LA SIMILAR TO a_expr ESCAPE a_expr %prec ESCAPE
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction("not_similar_to_escape"), Exprs: tree.Exprs{$1.expr(), $5.expr(), $7.expr()}}
  }
| a_expr '~' a_expr
  {
    $$.val = &tree.ComparisonExpr{Operator: treecmp.MakeComparisonOperator(treecmp.RegMatch), Left: $1.expr(), Right: $3.expr()}
  }
| a_expr NOT_REGMATCH a_expr
  {
    $$.val = &tree.ComparisonExpr{Operator: treecmp.MakeComparisonOperator(treecmp.NotRegMatch), Left: $1.expr(), Right: $3.expr()}
  }
| a_expr REGIMATCH a_expr
  {
    $$.val = &tree.ComparisonExpr{Operator: treecmp.MakeComparisonOperator(treecmp.RegIMatch), Left: $1.expr(), Right: $3.expr()}
  }
| a_expr NOT_REGIMATCH a_expr
  {
    $$.val = &tree.ComparisonExpr{Operator: treecmp.MakeComparisonOperator(treecmp.NotRegIMatch), Left: $1.expr(), Right: $3.expr()}
  }
| a_expr IS NAN %prec IS
  {
    $$.val = &tree.ComparisonExpr{
      Operator: treecmp.MakeComparisonOperator(treecmp.EQ),
      Left: $1.expr(),
      Right: tree.NewNumVal(constant.MakeFloat64(math.NaN()), "NaN", false /*negative*/),
    }
  }
| a_expr IS NOT NAN %prec IS
  {
    $$.val = &tree.ComparisonExpr{
      Operator: treecmp.MakeComparisonOperator(treecmp.NE),
      Left: $1.expr(),
      Right: tree.NewNumVal(constant.MakeFloat64(math.NaN()), "NaN", false /*negative*/),
    }
  }
| a_expr IS NULL %prec IS
  {
    $$.val = &tree.IsNullExpr{Expr: $1.expr()}
  }
| a_expr ISNULL %prec IS
  {
    $$.val = &tree.IsNullExpr{Expr: $1.expr()}
  }
| a_expr IS NOT NULL %prec IS
  {
    $$.val = &tree.IsNotNullExpr{Expr: $1.expr()}
  }
| a_expr NOTNULL %prec IS
  {
    $$.val = &tree.IsNotNullExpr{Expr: $1.expr()}
  }
| row OVERLAPS row
  {
   t1, t2 := $1.tuple(), $3.tuple()
   if len(t1.Exprs) != 2 {
     sqllex.Error("wrong number of parameters on left side of OVERLAPS expression")
     return 1
   }
   if len(t2.Exprs) != 2 {
     sqllex.Error("wrong number of parameters on right side of OVERLAPS expression")
     return 1
   }
   $$.val = &tree.FuncExpr{Func: tree.WrapFunction("overlaps"), Exprs: tree.Exprs{t1.Exprs[0], t1.Exprs[1], t2.Exprs[0], t2.Exprs[1]}}
  }
| a_expr IS TRUE %prec IS
  {
    $$.val = &tree.ComparisonExpr{Operator: treecmp.MakeComparisonOperator(treecmp.IsNotDistinctFrom), Left: $1.expr(), Right: tree.MakeDBool(true)}
  }
| a_expr IS NOT TRUE %prec IS
  {
    $$.val = &tree.ComparisonExpr{Operator: treecmp.MakeComparisonOperator(treecmp.IsDistinctFrom), Left: $1.expr(), Right: tree.MakeDBool(true)}
  }
| a_expr IS FALSE %prec IS
  {
    $$.val = &tree.ComparisonExpr{Operator: treecmp.MakeComparisonOperator(treecmp.IsNotDistinctFrom), Left: $1.expr(), Right: tree.MakeDBool(false)}
  }
| a_expr IS NOT FALSE %prec IS
  {
    $$.val = &tree.ComparisonExpr{Operator: treecmp.MakeComparisonOperator(treecmp.IsDistinctFrom), Left: $1.expr(), Right: tree.MakeDBool(false)}
  }
| a_expr IS UNKNOWN %prec IS
  {
    $$.val = &tree.ComparisonExpr{Operator: treecmp.MakeComparisonOperator(treecmp.IsNotDistinctFrom), Left: $1.expr(), Right: tree.DNull}
  }
| a_expr IS NOT UNKNOWN %prec IS
  {
    $$.val = &tree.ComparisonExpr{Operator: treecmp.MakeComparisonOperator(treecmp.IsDistinctFrom), Left: $1.expr(), Right: tree.DNull}
  }
| a_expr IS DISTINCT FROM a_expr %prec IS
  {
    $$.val = &tree.ComparisonExpr{Operator: treecmp.MakeComparisonOperator(treecmp.IsDistinctFrom), Left: $1.expr(), Right: $5.expr()}
  }
| a_expr IS NOT DISTINCT FROM a_expr %prec IS
  {
    $$.val = &tree.ComparisonExpr{Operator: treecmp.MakeComparisonOperator(treecmp.IsNotDistinctFrom), Left: $1.expr(), Right: $6.expr()}
  }
| a_expr IS OF '(' type_list ')' %prec IS
  {
    $$.val = &tree.IsOfTypeExpr{Expr: $1.expr(), Types: $5.typeReferences()}
  }
| a_expr IS NOT OF '(' type_list ')' %prec IS
  {
    $$.val = &tree.IsOfTypeExpr{Not: true, Expr: $1.expr(), Types: $6.typeReferences()}
  }
| a_expr BETWEEN opt_asymmetric b_expr AND a_expr %prec BETWEEN
  {
    $$.val = &tree.RangeCond{Left: $1.expr(), From: $4.expr(), To: $6.expr()}
  }
| a_expr NOT_LA BETWEEN opt_asymmetric b_expr AND a_expr %prec NOT_LA
  {
    $$.val = &tree.RangeCond{Not: true, Left: $1.expr(), From: $5.expr(), To: $7.expr()}
  }
| a_expr BETWEEN SYMMETRIC b_expr AND a_expr %prec BETWEEN
  {
    $$.val = &tree.RangeCond{Symmetric: true, Left: $1.expr(), From: $4.expr(), To: $6.expr()}
  }
| a_expr NOT_LA BETWEEN SYMMETRIC b_expr AND a_expr %prec NOT_LA
  {
    $$.val = &tree.RangeCond{Not: true, Symmetric: true, Left: $1.expr(), From: $5.expr(), To: $7.expr()}
  }
| a_expr IN in_expr
  {
    $$.val = &tree.ComparisonExpr{Operator: treecmp.MakeComparisonOperator(treecmp.In), Left: $1.expr(), Right: $3.expr()}
  }
| a_expr NOT_LA IN in_expr %prec NOT_LA
  {
    $$.val = &tree.ComparisonExpr{Operator: treecmp.MakeComparisonOperator(treecmp.NotIn), Left: $1.expr(), Right: $4.expr()}
  }
| a_expr subquery_op sub_type a_expr %prec CONCAT
  {
    op := $3.cmpOp()
    subOp := $2.op()
    subOpCmp, ok := subOp.(treecmp.ComparisonOperator)
    if !ok {
      // It is possible that we found `~` operator which was incorrectly typed
      // as "unary complement" math operator. Check whether that's the case and
      // override it to the correct "reg match" comparison operator.
      if tree.IsUnaryComplement(subOp) {
        subOp = treecmp.MakeComparisonOperator(treecmp.RegMatch)
      } else {
        sqllex.Error(fmt.Sprintf("%s %s <array> is invalid because %q is not a boolean operator",
          subOp, op, subOp))
        return 1
      }
    }
    $$.val = &tree.ComparisonExpr{
      Operator: op,
      SubOperator: subOpCmp,
      Left: $1.expr(),
      Right: $4.expr(),
    }
  }
| DEFAULT
  {
    $$.val = tree.DefaultVal{}
  }
// The UNIQUE predicate is a standard SQL feature but not yet implemented
// in PostgreSQL (as of 10.5).
| UNIQUE '(' error { return unimplemented(sqllex, "UNIQUE predicate") }

// Restricted expressions
//
// b_expr is a subset of the complete expression syntax defined by a_expr.
//
// Presently, AND, NOT, IS, and IN are the a_expr keywords that would cause
// trouble in the places where b_expr is used. For simplicity, we just
// eliminate all the boolean-keyword-operator productions from b_expr.
b_expr:
  c_expr
| b_expr TYPECAST cast_target
  {
    $$.val = &tree.CastExpr{Expr: $1.expr(), Type: $3.typeReference(), SyntaxMode: tree.CastShort}
  }
| b_expr TYPEANNOTATE typename
  {
    $$.val = &tree.AnnotateTypeExpr{Expr: $1.expr(), Type: $3.typeReference(), SyntaxMode: tree.AnnotateShort}
  }
| '+' b_expr %prec UMINUS
  {
    $$.val = $2.expr()
  }
| '-' b_expr %prec UMINUS
  {
    $$.val = unaryNegation($2.expr())
  }
| '~' b_expr %prec UMINUS
  {
    $$.val = &tree.UnaryExpr{Operator: tree.MakeUnaryOperator(tree.UnaryComplement), Expr: $2.expr()}
  }
| b_expr '+' b_expr
  {
    $$.val = &tree.BinaryExpr{Operator: treebin.MakeBinaryOperator(treebin.Plus), Left: $1.expr(), Right: $3.expr()}
  }
| b_expr '-' b_expr
  {
    $$.val = &tree.BinaryExpr{Operator: treebin.MakeBinaryOperator(treebin.Minus), Left: $1.expr(), Right: $3.expr()}
  }
| b_expr '*' b_expr
  {
    $$.val = &tree.BinaryExpr{Operator: treebin.MakeBinaryOperator(treebin.Mult), Left: $1.expr(), Right: $3.expr()}
  }
| b_expr '/' b_expr
  {
    $$.val = &tree.BinaryExpr{Operator: treebin.MakeBinaryOperator(treebin.Div), Left: $1.expr(), Right: $3.expr()}
  }
| b_expr FLOORDIV b_expr
  {
    $$.val = &tree.BinaryExpr{Operator: treebin.MakeBinaryOperator(treebin.FloorDiv), Left: $1.expr(), Right: $3.expr()}
  }
| b_expr '%' b_expr
  {
    $$.val = &tree.BinaryExpr{Operator: treebin.MakeBinaryOperator(treebin.Mod), Left: $1.expr(), Right: $3.expr()}
  }
| b_expr '^' b_expr
  {
    $$.val = &tree.BinaryExpr{Operator: treebin.MakeBinaryOperator(treebin.Pow), Left: $1.expr(), Right: $3.expr()}
  }
| b_expr '#' b_expr
  {
    $$.val = &tree.BinaryExpr{Operator: treebin.MakeBinaryOperator(treebin.Bitxor), Left: $1.expr(), Right: $3.expr()}
  }
| b_expr '&' b_expr
  {
    $$.val = &tree.BinaryExpr{Operator: treebin.MakeBinaryOperator(treebin.Bitand), Left: $1.expr(), Right: $3.expr()}
  }
| b_expr '|' b_expr
  {
    $$.val = &tree.BinaryExpr{Operator: treebin.MakeBinaryOperator(treebin.Bitor), Left: $1.expr(), Right: $3.expr()}
  }
| b_expr '<' b_expr
  {
    $$.val = &tree.ComparisonExpr{Operator: treecmp.MakeComparisonOperator(treecmp.LT), Left: $1.expr(), Right: $3.expr()}
  }
| b_expr '>' b_expr
  {
    $$.val = &tree.ComparisonExpr{Operator: treecmp.MakeComparisonOperator(treecmp.GT), Left: $1.expr(), Right: $3.expr()}
  }
| b_expr '=' b_expr
  {
    $$.val = &tree.ComparisonExpr{Operator: treecmp.MakeComparisonOperator(treecmp.EQ), Left: $1.expr(), Right: $3.expr()}
  }
| b_expr CONCAT b_expr
  {
    $$.val = &tree.BinaryExpr{Operator: treebin.MakeBinaryOperator(treebin.Concat), Left: $1.expr(), Right: $3.expr()}
  }
| b_expr LSHIFT b_expr
  {
    $$.val = &tree.BinaryExpr{Operator: treebin.MakeBinaryOperator(treebin.LShift), Left: $1.expr(), Right: $3.expr()}
  }
| b_expr RSHIFT b_expr
  {
    $$.val = &tree.BinaryExpr{Operator: treebin.MakeBinaryOperator(treebin.RShift), Left: $1.expr(), Right: $3.expr()}
  }
| b_expr LESS_EQUALS b_expr
  {
    $$.val = &tree.ComparisonExpr{Operator: treecmp.MakeComparisonOperator(treecmp.LE), Left: $1.expr(), Right: $3.expr()}
  }
| b_expr GREATER_EQUALS b_expr
  {
    $$.val = &tree.ComparisonExpr{Operator: treecmp.MakeComparisonOperator(treecmp.GE), Left: $1.expr(), Right: $3.expr()}
  }
| b_expr NOT_EQUALS b_expr
  {
    $$.val = &tree.ComparisonExpr{Operator: treecmp.MakeComparisonOperator(treecmp.NE), Left: $1.expr(), Right: $3.expr()}
  }
| qual_op b_expr %prec CBRT
  {
    var retCode int
    $$.val, retCode = processUnaryQualOp(sqllex, $1.op(), $2.expr())
    if retCode != 0 {
      return retCode
    }
  }
| b_expr qual_op b_expr %prec CBRT
  {
    {
      var retCode int
      $$.val, retCode = processBinaryQualOp(sqllex, $2.op(), $1.expr(), $3.expr())
      if retCode != 0 {
        return retCode
      }
    }
  }
| b_expr IS DISTINCT FROM b_expr %prec IS
  {
    $$.val = &tree.ComparisonExpr{Operator: treecmp.MakeComparisonOperator(treecmp.IsDistinctFrom), Left: $1.expr(), Right: $5.expr()}
  }
| b_expr IS NOT DISTINCT FROM b_expr %prec IS
  {
    $$.val = &tree.ComparisonExpr{Operator: treecmp.MakeComparisonOperator(treecmp.IsNotDistinctFrom), Left: $1.expr(), Right: $6.expr()}
  }
| b_expr IS OF '(' type_list ')' %prec IS
  {
    $$.val = &tree.IsOfTypeExpr{Expr: $1.expr(), Types: $5.typeReferences()}
  }
| b_expr IS NOT OF '(' type_list ')' %prec IS
  {
    $$.val = &tree.IsOfTypeExpr{Not: true, Expr: $1.expr(), Types: $6.typeReferences()}
  }

// Productions that can be used in both a_expr and b_expr.
//
// Note: productions that refer recursively to a_expr or b_expr mostly cannot
// appear here. However, it's OK to refer to a_exprs that occur inside
// parentheses, such as function arguments; that cannot introduce ambiguity to
// the b_expr syntax.
//
c_expr:
  d_expr
| d_expr array_subscripts
  {
    $$.val = &tree.IndirectionExpr{
      Expr: $1.expr(),
      Indirection: $2.arraySubscripts(),
    }
  }
| case_expr
| EXISTS select_with_parens
  {
    $$.val = &tree.Subquery{Select: $2.selectStmt(), Exists: true}
  }

// Productions that can be followed by a postfix operator.
//
// Currently we support array indexing (see c_expr above).
//
// TODO(knz/jordan): this is the rule that can be extended to support
// composite types (#27792) with e.g.:
//
//     | '(' a_expr ')' field_access_ops
//
//     [...]
//
//     // field_access_ops supports the notations:
//     // - .a
//     // - .a[123]
//     // - .a.b[123][5456].c.d
//     // NOT [123] directly, this is handled in c_expr above.
//
//     field_access_ops:
//       field_access_op
//     | field_access_op other_subscripts
//
//     field_access_op:
//       '.' name
//     other_subscripts:
//       other_subscript
//     | other_subscripts other_subscript
//     other_subscript:
//        field_access_op
//     |  array_subscripts

d_expr:
  ICONST
  {
    $$.val = $1.numVal()
  }
| FCONST
  {
    $$.val = $1.numVal()
  }
| SCONST
  {
    $$.val = tree.NewStrVal($1)
  }
| BCONST
  {
    $$.val = tree.NewBytesStrVal($1)
  }
| BITCONST
  {
    d, err := tree.ParseDBitArray($1)
    if err != nil { return setErr(sqllex, err) }
    $$.val = d
  }
| func_application_name '(' expr_list opt_sort_clause_no_index ')' SCONST { return unimplemented(sqllex, $1.resolvableFuncRef().String() + "(...) SCONST") }
| typed_literal
  {
    $$.val = $1.expr()
  }
| interval_value
  {
    $$.val = $1.expr()
  }
| TRUE
  {
    $$.val = tree.MakeDBool(true)
  }
| FALSE
  {
    $$.val = tree.MakeDBool(false)
  }
| NULL
  {
    $$.val = tree.DNull
  }
| column_path_with_star
  {
    $$.val = tree.Expr($1.unresolvedName())
  }
| '@' iconst64
  {
    colNum := $2.int64()
    if colNum < 1 || colNum > int64(MaxInt) {
      sqllex.Error(fmt.Sprintf("invalid column ordinal: @%d", colNum))
      return 1
    }
    $$.val = tree.NewOrdinalReference(int(colNum-1))
  }
| PLACEHOLDER
  {
    p := $1.placeholder()
    sqllex.(*lexer).UpdateNumPlaceholders(p)
    $$.val = p
  }
// TODO(knz/jordan): extend this for compound types. See explanation above.
| '(' a_expr ')' '.' '*'
  {
    $$.val = &tree.TupleStar{Expr: $2.expr()}
  }
| '(' a_expr ')' '.' unrestricted_name
  {
    $$.val = &tree.ColumnAccessExpr{Expr: $2.expr(), ColName: tree.Name($5) }
  }
| '(' a_expr ')' '.' '@' ICONST
  {
    idx, err := $6.numVal().AsInt32()
    if err != nil { return setErr(sqllex, err) }
    if idx <= 0 {
      err := errors.New("invalid numeric tuple index: indexes must be > 0")
      return setErr(sqllex, err)
    }
    $$.val = &tree.ColumnAccessExpr{Expr: $2.expr(), ByIndex: true, ColIndex: int(idx-1)}
  }
| '(' a_expr ')'
  {
    $$.val = &tree.ParenExpr{Expr: $2.expr()}
  }
| func_expr
| select_with_parens %prec UMINUS
  {
    $$.val = &tree.Subquery{Select: $1.selectStmt()}
  }
| labeled_row
  {
    $$.val = $1.tuple()
  }
| ARRAY select_with_parens %prec UMINUS
  {
    $$.val = &tree.ArrayFlatten{Subquery: &tree.Subquery{Select: $2.selectStmt()}}
  }
| ARRAY row
  {
    $$.val = &tree.Array{Exprs: $2.tuple().Exprs}
  }
| ARRAY array_expr
  {
    $$.val = $2.expr()
  }
| GROUPING '(' expr_list ')' { return unimplemented(sqllex, "d_expr grouping") }

func_application:
  func_application_name '(' ')'
  {
    $$.val = &tree.FuncExpr{Func: $1.resolvableFuncRef()}
  }
| func_application_name '(' expr_list opt_sort_clause_no_index ')'
  {
    $$.val = &tree.FuncExpr{Func: $1.resolvableFuncRef(), Exprs: $3.exprs(), OrderBy: $4.orderBy(), AggType: tree.GeneralAgg}
  }
| func_application_name '(' VARIADIC a_expr opt_sort_clause_no_index ')' { return unimplemented(sqllex, "variadic") }
| func_application_name '(' expr_list ',' VARIADIC a_expr opt_sort_clause_no_index ')' { return unimplemented(sqllex, "variadic") }
| func_application_name '(' ALL expr_list opt_sort_clause_no_index ')'
  {
    $$.val = &tree.FuncExpr{Func: $1.resolvableFuncRef(), Type: tree.AllFuncType, Exprs: $4.exprs(), OrderBy: $5.orderBy(), AggType: tree.GeneralAgg}
  }
// TODO(ridwanmsharif): Once DISTINCT is supported by window aggregates,
// allow ordering to be specified below.
| func_application_name '(' DISTINCT expr_list ')'
  {
    $$.val = &tree.FuncExpr{Func: $1.resolvableFuncRef(), Type: tree.DistinctFuncType, Exprs: $4.exprs()}
  }
| func_application_name '(' '*' ')'
  {
    $$.val = &tree.FuncExpr{Func: $1.resolvableFuncRef(), Exprs: tree.Exprs{tree.StarExpr()}}
  }
| func_application_name '(' error { return helpWithFunction(sqllex, $1.resolvableFuncRef()) }

func_application_name:
  func_name
  {
    $$.val = $1.resolvableFuncRefFromName()
  }
| '[' FUNCTION iconst32 ']'
  {
    id := $3.int32()
    $$.val = tree.ResolvableFunctionReference{
      FunctionReference: &tree.FunctionOID{OID: oid.Oid(id)},
    }
  }

// typed_literal represents expressions like INT '4', or generally <TYPE> SCONST.
// This rule handles both the case of qualified and non-qualified typenames.
typed_literal:
  // The key here is that none of the keywords in the func_name_no_crdb_extra
  // production can overlap with the type rules in const_typename, otherwise
  // we will have conflicts between this rule and the one below.
  func_name_no_crdb_extra SCONST
  {
    name := $1.unresolvedName()
    if name.NumParts == 1 {
      typName := name.Parts[0]
      /* FORCE DOC */
      // See https://www.postgresql.org/docs/9.1/static/datatype-character.html
      // Postgres supports a special character type named "char" (with the quotes)
      // that is a single-character column type. It's used by system tables.
      // Eventually this clause will be used to parse user-defined types as well,
      // since their names can be quoted.
      if typName == "char" {
        $$.val = &tree.CastExpr{Expr: tree.NewStrVal($2), Type: types.QChar, SyntaxMode: tree.CastPrepend}
      } else if typName == "serial" {
        switch sqllex.(*lexer).nakedIntType.Width() {
        case 32:
          $$.val = &tree.CastExpr{Expr: tree.NewStrVal($2), Type: &types.Serial4Type, SyntaxMode: tree.CastPrepend}
        default:
          $$.val = &tree.CastExpr{Expr: tree.NewStrVal($2), Type: &types.Serial8Type, SyntaxMode: tree.CastPrepend}
        }
      } else {
        // Check the the type is one of our "non-keyword" type names.
        // Otherwise, package it up as a type reference for later.
        // However, if the type name is one of our known unsupported
        // types, return an unimplemented error message.
        var typ tree.ResolvableTypeReference
        var ok bool
        var unimp int
        typ, ok, unimp = types.TypeForNonKeywordTypeName(typName)
        if !ok {
          switch unimp {
            case 0:
              // In this case, we don't think this type is one of our
              // known unsupported types, so make a type reference for it.
              aIdx := sqllex.(*lexer).NewAnnotation()
              un, err := name.ToUnresolvedObjectName(aIdx)
              if err != nil { return setErr(sqllex, err) }
              typ = &un
            case -1:
              return unimplemented(sqllex, "type name " + typName)
            default:
              return unimplementedWithIssueDetail(sqllex, unimp, typName)
          }
        }
      $$.val = &tree.CastExpr{Expr: tree.NewStrVal($2), Type: typ, SyntaxMode: tree.CastPrepend}
      }
    } else {
      aIdx := sqllex.(*lexer).NewAnnotation()
      res, err := name.ToUnresolvedObjectName(aIdx)
      if err != nil { return setErr(sqllex, err) }
      $$.val = &tree.CastExpr{Expr: tree.NewStrVal($2), Type: &res, SyntaxMode: tree.CastPrepend}
    }
  }
| const_typename SCONST
  {
    $$.val = &tree.CastExpr{Expr: tree.NewStrVal($2), Type: $1.colType(), SyntaxMode: tree.CastPrepend}
  }

// func_expr and its cousin func_expr_windowless are split out from c_expr just
// so that we have classifications for "everything that is a function call or
// looks like one". This isn't very important, but it saves us having to
// document which variants are legal in places like "FROM function()" or the
// backwards-compatible functional-index syntax for CREATE INDEX. (Note that
// many of the special SQL functions wouldn't actually make any sense as
// functional index entries, but we ignore that consideration here.)
func_expr:
  func_application within_group_clause filter_clause over_clause
  {
    f := $1.expr().(*tree.FuncExpr)
    w := $2.expr().(*tree.FuncExpr)
    if w.AggType != 0 {
      f.AggType = w.AggType
      f.OrderBy = w.OrderBy
    }
    f.Filter = $3.expr()
    f.WindowDef = $4.windowDef()
    $$.val = f
  }
| func_expr_common_subexpr
  {
    $$.val = $1.expr()
  }

// As func_expr but does not accept WINDOW functions directly (but they can
// still be contained in arguments for functions etc). Use this when window
// expressions are not allowed, where needed to disambiguate the grammar
// (e.g. in CREATE INDEX).
func_expr_windowless:
  func_application { $$.val = $1.expr() }
| func_expr_common_subexpr { $$.val = $1.expr() }

// Special expressions that are considered to be functions.
func_expr_common_subexpr:
  COLLATION FOR '(' a_expr ')'
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction("pg_collation_for"), Exprs: tree.Exprs{$4.expr()}}
  }
| CURRENT_DATE
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction($1)}
  }
| CURRENT_SCHEMA
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction($1)}
  }
// Special identifier current_catalog is equivalent to current_database().
// https://www.postgresql.org/docs/10/static/functions-info.html
| CURRENT_CATALOG
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction("current_database")}
  }
| CURRENT_TIMESTAMP
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction($1)}
  }
| CURRENT_TIME
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction($1)}
  }
| LOCALTIMESTAMP
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction($1)}
  }
| LOCALTIME
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction($1)}
  }
| CURRENT_USER
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction($1)}
  }
// Special identifier current_role is equivalent to current_user.
// https://www.postgresql.org/docs/10/static/functions-info.html
| CURRENT_ROLE
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction("current_user")}
  }
| SESSION_USER
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction("session_user")}
  }
| USER
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction("current_user")}
  }
| CAST '(' a_expr AS cast_target ')'
  {
    $$.val = &tree.CastExpr{Expr: $3.expr(), Type: $5.typeReference(), SyntaxMode: tree.CastExplicit}
  }
| ANNOTATE_TYPE '(' a_expr ',' typename ')'
  {
    $$.val = &tree.AnnotateTypeExpr{Expr: $3.expr(), Type: $5.typeReference(), SyntaxMode: tree.AnnotateExplicit}
  }
| IF '(' a_expr ',' a_expr ',' a_expr ')'
  {
    $$.val = &tree.IfExpr{Cond: $3.expr(), True: $5.expr(), Else: $7.expr()}
  }
| IFERROR '(' a_expr ',' a_expr ',' a_expr ')'
  {
    $$.val = &tree.IfErrExpr{Cond: $3.expr(), Else: $5.expr(), ErrCode: $7.expr()}
  }
| IFERROR '(' a_expr ',' a_expr ')'
  {
    $$.val = &tree.IfErrExpr{Cond: $3.expr(), Else: $5.expr()}
  }
| ISERROR '(' a_expr ')'
  {
    $$.val = &tree.IfErrExpr{Cond: $3.expr()}
  }
| ISERROR '(' a_expr ',' a_expr ')'
  {
    $$.val = &tree.IfErrExpr{Cond: $3.expr(), ErrCode: $5.expr()}
  }
| NULLIF '(' a_expr ',' a_expr ')'
  {
    $$.val = &tree.NullIfExpr{Expr1: $3.expr(), Expr2: $5.expr()}
  }
| IFNULL '(' a_expr ',' a_expr ')'
  {
    $$.val = &tree.CoalesceExpr{Name: "IFNULL", Exprs: tree.Exprs{$3.expr(), $5.expr()}}
  }
| COALESCE '(' expr_list ')'
  {
    $$.val = &tree.CoalesceExpr{Name: "COALESCE", Exprs: $3.exprs()}
  }
| special_function

special_function:
  CURRENT_DATE '(' ')'
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction($1)}
  }
| CURRENT_DATE '(' error { return helpWithFunctionByName(sqllex, $1) }
| CURRENT_SCHEMA '(' ')'
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction($1)}
  }
| CURRENT_SCHEMA '(' error { return helpWithFunctionByName(sqllex, $1) }
| CURRENT_TIMESTAMP '(' ')'
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction($1)}
  }
| CURRENT_TIMESTAMP '(' a_expr ')'
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction($1), Exprs: tree.Exprs{$3.expr()}}
  }
| CURRENT_TIMESTAMP '(' error { return helpWithFunctionByName(sqllex, $1) }
| CURRENT_TIME '(' ')'
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction($1)}
  }
| CURRENT_TIME '(' a_expr ')'
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction($1), Exprs: tree.Exprs{$3.expr()}}
  }
| CURRENT_TIME '(' error { return helpWithFunctionByName(sqllex, $1) }
| LOCALTIMESTAMP '(' ')'
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction($1)}
  }
| LOCALTIMESTAMP '(' a_expr ')'
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction($1), Exprs: tree.Exprs{$3.expr()}}
  }
| LOCALTIMESTAMP '(' error { return helpWithFunctionByName(sqllex, $1) }
| LOCALTIME '(' ')'
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction($1)}
  }
| LOCALTIME '(' a_expr ')'
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction($1), Exprs: tree.Exprs{$3.expr()}}
  }
| LOCALTIME '(' error { return helpWithFunctionByName(sqllex, $1) }
| CURRENT_USER '(' ')'
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction($1)}
  }
| CURRENT_USER '(' error { return helpWithFunctionByName(sqllex, $1) }
| SESSION_USER '(' ')'
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction($1)}
  }
| SESSION_USER '(' error { return helpWithFunctionByName(sqllex, $1) }
| EXTRACT '(' extract_list ')'
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction($1), Exprs: $3.exprs()}
  }
| EXTRACT '(' error { return helpWithFunctionByName(sqllex, $1) }
| EXTRACT_DURATION '(' extract_list ')'
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction($1), Exprs: $3.exprs()}
  }
| EXTRACT_DURATION '(' error { return helpWithFunctionByName(sqllex, $1) }
| OVERLAY '(' overlay_list ')'
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction($1), Exprs: $3.exprs()}
  }
| OVERLAY '(' error { return helpWithFunctionByName(sqllex, $1) }
| POSITION '(' position_list ')'
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction("strpos"), Exprs: $3.exprs()}
  }
| SUBSTRING '(' substr_list ')'
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction($1), Exprs: $3.exprs()}
  }
| SUBSTRING '(' error { return helpWithFunctionByName(sqllex, $1) }
| TREAT '(' a_expr AS typename ')' { return unimplemented(sqllex, "treat") }
| TRIM '(' BOTH trim_list ')'
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction("btrim"), Exprs: $4.exprs()}
  }
| TRIM '(' LEADING trim_list ')'
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction("ltrim"), Exprs: $4.exprs()}
  }
| TRIM '(' TRAILING trim_list ')'
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction("rtrim"), Exprs: $4.exprs()}
  }
| TRIM '(' trim_list ')'
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction("btrim"), Exprs: $3.exprs()}
  }
| GREATEST '(' expr_list ')'
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction($1), Exprs: $3.exprs()}
  }
| GREATEST '(' error { return helpWithFunctionByName(sqllex, $1) }
| LEAST '(' expr_list ')'
  {
    $$.val = &tree.FuncExpr{Func: tree.WrapFunction($1), Exprs: $3.exprs()}
  }
| LEAST '(' error { return helpWithFunctionByName(sqllex, $1) }


// Aggregate decoration clauses
within_group_clause:
  WITHIN GROUP '(' single_sort_clause ')'
  {
    $$.val = &tree.FuncExpr{OrderBy: $4.orderBy(), AggType: tree.OrderedSetAgg}
  }
| /* EMPTY */
  {
    $$.val = &tree.FuncExpr{}
  }

filter_clause:
  FILTER '(' WHERE a_expr ')'
  {
    $$.val = $4.expr()
  }
| /* EMPTY */
  {
    $$.val = tree.Expr(nil)
  }

// Window Definitions
window_clause:
  WINDOW window_definition_list
  {
    $$.val = $2.window()
  }
| /* EMPTY */
  {
    $$.val = tree.Window(nil)
  }

window_definition_list:
  window_definition
  {
    $$.val = tree.Window{$1.windowDef()}
  }
| window_definition_list ',' window_definition
  {
    $$.val = append($1.window(), $3.windowDef())
  }

window_definition:
  window_name AS window_specification
  {
    n := $3.windowDef()
    n.Name = tree.Name($1)
    $$.val = n
  }

over_clause:
  OVER window_specification
  {
    $$.val = $2.windowDef()
  }
| OVER window_name
  {
    $$.val = &tree.WindowDef{Name: tree.Name($2)}
  }
| /* EMPTY */
  {
    $$.val = (*tree.WindowDef)(nil)
  }

window_specification:
  '(' opt_existing_window_name opt_partition_clause
    opt_sort_clause_no_index opt_frame_clause ')'
  {
    $$.val = &tree.WindowDef{
      RefName: tree.Name($2),
      Partitions: $3.exprs(),
      OrderBy: $4.orderBy(),
      Frame: $5.windowFrame(),
    }
  }

// If we see PARTITION, RANGE, ROWS, or GROUPS as the first token after the '('
// of a window_specification, we want the assumption to be that there is no
// existing_window_name; but those keywords are unreserved and so could be
// names. We fix this by making them have the same precedence as IDENT and
// giving the empty production here a slightly higher precedence, so that the
// shift/reduce conflict is resolved in favor of reducing the rule. These
// keywords are thus precluded from being an existing_window_name but are not
// reserved for any other purpose.
opt_existing_window_name:
  name
| /* EMPTY */ %prec CONCAT
  {
    $$ = ""
  }

opt_partition_clause:
  PARTITION BY expr_list
  {
    $$.val = $3.exprs()
  }
| /* EMPTY */
  {
    $$.val = tree.Exprs(nil)
  }

opt_frame_clause:
  RANGE frame_extent opt_frame_exclusion
  {
    $$.val = &tree.WindowFrame{
      Mode: treewindow.RANGE,
      Bounds: $2.windowFrameBounds(),
      Exclusion: $3.windowFrameExclusion(),
    }
  }
| ROWS frame_extent opt_frame_exclusion
  {
    $$.val = &tree.WindowFrame{
      Mode: treewindow.ROWS,
      Bounds: $2.windowFrameBounds(),
      Exclusion: $3.windowFrameExclusion(),
    }
  }
| GROUPS frame_extent opt_frame_exclusion
  {
    $$.val = &tree.WindowFrame{
      Mode: treewindow.GROUPS,
      Bounds: $2.windowFrameBounds(),
      Exclusion: $3.windowFrameExclusion(),
    }
  }
| /* EMPTY */
  {
    $$.val = (*tree.WindowFrame)(nil)
  }

frame_extent:
  frame_bound
  {
    startBound := $1.windowFrameBound()
    switch {
    case startBound.BoundType == treewindow.UnboundedFollowing:
      sqllex.Error("frame start cannot be UNBOUNDED FOLLOWING")
      return 1
    case startBound.BoundType == treewindow.OffsetFollowing:
      sqllex.Error("frame starting from following row cannot end with current row")
      return 1
    }
    $$.val = tree.WindowFrameBounds{StartBound: startBound}
  }
| BETWEEN frame_bound AND frame_bound
  {
    startBound := $2.windowFrameBound()
    endBound := $4.windowFrameBound()
    switch {
    case startBound.BoundType == treewindow.UnboundedFollowing:
      sqllex.Error("frame start cannot be UNBOUNDED FOLLOWING")
      return 1
    case endBound.BoundType == treewindow.UnboundedPreceding:
      sqllex.Error("frame end cannot be UNBOUNDED PRECEDING")
      return 1
    case startBound.BoundType == treewindow.CurrentRow && endBound.BoundType == treewindow.OffsetPreceding:
      sqllex.Error("frame starting from current row cannot have preceding rows")
      return 1
    case startBound.BoundType == treewindow.OffsetFollowing && endBound.BoundType == treewindow.OffsetPreceding:
      sqllex.Error("frame starting from following row cannot have preceding rows")
      return 1
    case startBound.BoundType == treewindow.OffsetFollowing && endBound.BoundType == treewindow.CurrentRow:
      sqllex.Error("frame starting from following row cannot have preceding rows")
      return 1
    }
    $$.val = tree.WindowFrameBounds{StartBound: startBound, EndBound: endBound}
  }

// This is used for both frame start and frame end, with output set up on the
// assumption it's frame start; the frame_extent productions must reject
// invalid cases.
frame_bound:
  UNBOUNDED PRECEDING
  {
    $$.val = &tree.WindowFrameBound{BoundType: treewindow.UnboundedPreceding}
  }
| UNBOUNDED FOLLOWING
  {
    $$.val = &tree.WindowFrameBound{BoundType: treewindow.UnboundedFollowing}
  }
| CURRENT ROW
  {
    $$.val = &tree.WindowFrameBound{BoundType: treewindow.CurrentRow}
  }
| a_expr PRECEDING
  {
    $$.val = &tree.WindowFrameBound{
      OffsetExpr: $1.expr(),
      BoundType: treewindow.OffsetPreceding,
    }
  }
| a_expr FOLLOWING
  {
    $$.val = &tree.WindowFrameBound{
      OffsetExpr: $1.expr(),
      BoundType: treewindow.OffsetFollowing,
    }
  }

opt_frame_exclusion:
  EXCLUDE CURRENT ROW
  {
    $$.val = treewindow.ExcludeCurrentRow
  }
| EXCLUDE GROUP
  {
    $$.val = treewindow.ExcludeGroup
  }
| EXCLUDE TIES
  {
    $$.val = treewindow.ExcludeTies
  }
| EXCLUDE NO OTHERS
  {
    // EXCLUDE NO OTHERS is equivalent to omitting the frame exclusion clause.
    $$.val = treewindow.NoExclusion
  }
| /* EMPTY */
  {
    $$.val = treewindow.NoExclusion
  }

// Supporting nonterminals for expressions.

// Explicit row production.
//
// SQL99 allows an optional ROW keyword, so we can now do single-element rows
// without conflicting with the parenthesized a_expr production. Without the
// ROW keyword, there must be more than one a_expr inside the parens.
row:
  ROW '(' opt_expr_list ')'
  {
    $$.val = &tree.Tuple{Exprs: $3.exprs(), Row: true}
  }
| expr_tuple_unambiguous
  {
    $$.val = $1.tuple()
  }

labeled_row:
  row
| '(' row AS name_list ')'
  {
    t := $2.tuple()
    labels := $4.nameList()
    t.Labels = make([]string, len(labels))
    for i, l := range labels {
      t.Labels[i] = string(l)
    }
    $$.val = t
  }

sub_type:
  ANY
  {
    $$.val = treecmp.MakeComparisonOperator(treecmp.Any)
  }
| SOME
  {
    $$.val = treecmp.MakeComparisonOperator(treecmp.Some)
  }
| ALL
  {
    $$.val = treecmp.MakeComparisonOperator(treecmp.All)
  }

// We combine mathOp and Op from PostgreSQL's gram.y there.
// In PostgreSQL, mathOp have an order of operations as defined by the
// assoc rules.
//
// In CockroachDB, we have defined further operations that have a defined
// order of operations (e.g. <@, |, &, ~, #, FLOORDIV) which is broken
// if we split them up to math_ops and ops, which breaks compatibility
// with PostgreSQL's qual_op.
//
// Ensure you also update process.*QualOp above when adding to this.
all_op:
  // exactly from MathOp
  '+' { $$.val = treebin.MakeBinaryOperator(treebin.Plus)  }
| '-' { $$.val = treebin.MakeBinaryOperator(treebin.Minus) }
| '*' { $$.val = treebin.MakeBinaryOperator(treebin.Mult)  }
| '/' { $$.val = treebin.MakeBinaryOperator(treebin.Div)   }
| '%' { $$.val = treebin.MakeBinaryOperator(treebin.Mod)   }
| '^' { $$.val = treebin.MakeBinaryOperator(treebin.Pow) }
| '<' { $$.val = treecmp.MakeComparisonOperator(treecmp.LT) }
| '>' { $$.val = treecmp.MakeComparisonOperator(treecmp.GT) }
| '=' { $$.val = treecmp.MakeComparisonOperator(treecmp.EQ) }
| LESS_EQUALS    { $$.val = treecmp.MakeComparisonOperator(treecmp.LE) }
| GREATER_EQUALS { $$.val = treecmp.MakeComparisonOperator(treecmp.GE) }
| NOT_EQUALS     { $$.val = treecmp.MakeComparisonOperator(treecmp.NE) }
  // partial set of operators from from Op
| '?' { $$.val = treecmp.MakeComparisonOperator(treecmp.JSONExists) }
| '&' { $$.val = treebin.MakeBinaryOperator(treebin.Bitand) }
| '|' { $$.val = treebin.MakeBinaryOperator(treebin.Bitor)  }
| '#' { $$.val = treebin.MakeBinaryOperator(treebin.Bitxor) }
| FLOORDIV { $$.val = treebin.MakeBinaryOperator(treebin.FloorDiv) }
| CONTAINS { $$.val = treecmp.MakeComparisonOperator(treecmp.Contains) }
| CONTAINED_BY { $$.val = treecmp.MakeComparisonOperator(treecmp.ContainedBy) }
| LSHIFT { $$.val = treebin.MakeBinaryOperator(treebin.LShift) }
| RSHIFT { $$.val = treebin.MakeBinaryOperator(treebin.RShift) }
| CONCAT { $$.val = treebin.MakeBinaryOperator(treebin.Concat) }
| FETCHVAL { $$.val = treebin.MakeBinaryOperator(treebin.JSONFetchVal) }
| FETCHTEXT { $$.val = treebin.MakeBinaryOperator(treebin.JSONFetchText) }
| FETCHVAL_PATH { $$.val = treebin.MakeBinaryOperator(treebin.JSONFetchValPath) }
| FETCHTEXT_PATH { $$.val = treebin.MakeBinaryOperator(treebin.JSONFetchTextPath) }
| JSON_SOME_EXISTS { $$.val = treecmp.MakeComparisonOperator(treecmp.JSONSomeExists) }
| JSON_ALL_EXISTS { $$.val = treecmp.MakeComparisonOperator(treecmp.JSONAllExists) }
| NOT_REGMATCH { $$.val = treecmp.MakeComparisonOperator(treecmp.NotRegMatch) }
| REGIMATCH { $$.val = treecmp.MakeComparisonOperator(treecmp.RegIMatch) }
| NOT_REGIMATCH { $$.val = treecmp.MakeComparisonOperator(treecmp.NotRegIMatch) }
| AND_AND { $$.val = treecmp.MakeComparisonOperator(treecmp.Overlaps) }
| AT_AT { $$.val = treecmp.MakeComparisonOperator(treecmp.TSMatches) }
| DISTANCE { $$.val = treebin.MakeBinaryOperator(treebin.Distance) }
| COS_DISTANCE { $$.val = treebin.MakeBinaryOperator(treebin.CosDistance) }
| NEG_INNER_PRODUCT { $$.val = treebin.MakeBinaryOperator(treebin.NegInnerProduct) }
| '~' { $$.val = tree.MakeUnaryOperator(tree.UnaryComplement) }
| SQRT { $$.val = tree.MakeUnaryOperator(tree.UnarySqrt) }
| CBRT { $$.val = tree.MakeUnaryOperator(tree.UnaryCbrt) }

operator_op:
  all_op
| name '.' all_op
  {
    // Only support operators on pg_catalog.
    if $1 != "pg_catalog" {
      return unimplementedWithIssue(sqllex, 65017)
    }
    $$ = $3
  }

// qual_op partially matches qualOp PostgreSQL's gram.y.
// In an ideal circumstance, we also include non math_ops in this
// definition. However, this would break cross compatibility as we
// need %prec (keyword before OPERATOR) in a_expr/b_expr, which
// breaks order of operations for older versions of CockroachDB.
// See #64699 for the attempt.
qual_op:
  OPERATOR '(' operator_op ')'
  {
    $$ = $3
  }

subquery_op:
  all_op
| qual_op
| LIKE         { $$.val = treecmp.MakeComparisonOperator(treecmp.Like)     }
| NOT_LA LIKE  { $$.val = treecmp.MakeComparisonOperator(treecmp.NotLike)  }
| ILIKE        { $$.val = treecmp.MakeComparisonOperator(treecmp.ILike)    }
| NOT_LA ILIKE { $$.val = treecmp.MakeComparisonOperator(treecmp.NotILike) }
  // cannot put SIMILAR TO here, because SIMILAR TO is a hack.
  // the regular expression is preprocessed by a function (similar_escape),
  // and the ~ operator for posix regular expressions is used.
  //        x SIMILAR TO y     ->    x ~ similar_escape(y)
  // this transformation is made on the fly by the parser upwards.
  // however the SubLink structure which handles any/some/all stuff
  // is not ready for such a thing.

// expr_tuple1_ambiguous is a tuple expression with at least one expression.
// The allowable syntax is:
// ( )         -- empty tuple.
// ( E )       -- just one value, this is potentially ambiguous with
//             -- grouping parentheses. The ambiguity is resolved
//             -- by only allowing expr_tuple1_ambiguous on the RHS
//             -- of a IN expression.
// ( E, E, E ) -- comma-separated values, no trailing comma allowed.
// ( E, )      -- just one value with a comma, makes the syntax unambiguous
//             -- with grouping parentheses. This is not usually produced
//             -- by SQL clients, but can be produced by pretty-printing
//             -- internally in CockroachDB.
expr_tuple1_ambiguous:
  '(' ')'
  {
    $$.val = &tree.Tuple{}
  }
| '(' tuple1_ambiguous_values ')'
  {
    $$.val = &tree.Tuple{Exprs: $2.exprs()}
  }

tuple1_ambiguous_values:
  a_expr
  {
    $$.val = tree.Exprs{$1.expr()}
  }
| a_expr ','
  {
    $$.val = tree.Exprs{$1.expr()}
  }
| a_expr ',' expr_list
  {
     $$.val = append(tree.Exprs{$1.expr()}, $3.exprs()...)
  }

// expr_tuple_unambiguous is a tuple expression with zero or more
// expressions. The allowable syntax is:
// ( )         -- zero values
// ( E, )      -- just one value. This is unambiguous with the (E) grouping syntax.
// ( E, E, E ) -- comma-separated values, more than 1.
expr_tuple_unambiguous:
  '(' ')'
  {
    $$.val = &tree.Tuple{}
  }
| '(' tuple1_unambiguous_values ')'
  {
    $$.val = &tree.Tuple{Exprs: $2.exprs()}
  }

tuple1_unambiguous_values:
  a_expr ','
  {
    $$.val = tree.Exprs{$1.expr()}
  }
| a_expr ',' expr_list
  {
     $$.val = append(tree.Exprs{$1.expr()}, $3.exprs()...)
  }

opt_expr_list:
  expr_list
| /* EMPTY */
  {
    $$.val = tree.Exprs(nil)
  }

expr_list:
  a_expr
  {
    $$.val = tree.Exprs{$1.expr()}
  }
| expr_list ',' a_expr
  {
    $$.val = append($1.exprs(), $3.expr())
  }

type_list:
  typename
  {
    $$.val = []tree.ResolvableTypeReference{$1.typeReference()}
  }
| type_list ',' typename
  {
    $$.val = append($1.typeReferences(), $3.typeReference())
  }

array_expr:
  '[' opt_expr_list ']'
  {
    $$.val = &tree.Array{Exprs: $2.exprs()}
  }
| '[' array_expr_list ']'
  {
    $$.val = &tree.Array{Exprs: $2.exprs()}
  }

array_expr_list:
  array_expr
  {
    $$.val = tree.Exprs{$1.expr()}
  }
| array_expr_list ',' array_expr
  {
    $$.val = append($1.exprs(), $3.expr())
  }

extract_list:
  extract_arg FROM a_expr
  {
    $$.val = tree.Exprs{tree.NewStrVal(strings.ToLower($1)), $3.expr()}
  }
| expr_list
  {
    $$.val = $1.exprs()
  }

// TODO(vivek): Narrow down to just IDENT once the other
// terms are not keywords.
extract_arg:
  IDENT
| YEAR
| MONTH
| DAY
| HOUR
| MINUTE
| SECOND
| SCONST

// OVERLAY() arguments
// SQL99 defines the OVERLAY() function:
//   - overlay(text placing text from int for int)
//   - overlay(text placing text from int)
// and similarly for binary strings
overlay_list:
  a_expr overlay_placing substr_from substr_for
  {
    $$.val = tree.Exprs{$1.expr(), $2.expr(), $3.expr(), $4.expr()}
  }
| a_expr overlay_placing substr_from
  {
    $$.val = tree.Exprs{$1.expr(), $2.expr(), $3.expr()}
  }
| expr_list
  {
    $$.val = $1.exprs()
  }

overlay_placing:
  PLACING a_expr
  {
    $$.val = $2.expr()
  }

// position_list uses b_expr not a_expr to avoid conflict with general IN
position_list:
  b_expr IN b_expr
  {
    $$.val = tree.Exprs{$3.expr(), $1.expr()}
  }
| /* EMPTY */
  {
    $$.val = tree.Exprs(nil)
  }

// SUBSTRING() arguments
// SQL9x defines a specific syntax for arguments to SUBSTRING():
//   - substring(text from int for int)
//   - substring(text from int) get entire string from starting point "int"
//   - substring(text for int) get first "int" characters of string
//   - substring(text from pattern) get entire string matching pattern
//   - substring(text from pattern for escape) same with specified escape char
// We also want to support generic substring functions which accept
// the usual generic list of arguments. So we will accept both styles
// here, and convert the SQL9x style to the generic list for further
// processing. - thomas 2000-11-28
substr_list:
  a_expr substr_from substr_for
  {
    $$.val = tree.Exprs{$1.expr(), $2.expr(), $3.expr()}
  }
| a_expr substr_for substr_from
  {
    $$.val = tree.Exprs{$1.expr(), $3.expr(), $2.expr()}
  }
| a_expr substr_from
  {
    $$.val = tree.Exprs{$1.expr(), $2.expr()}
  }
| a_expr substr_for
  {
    $$.val = tree.Exprs{$1.expr(), tree.NewDInt(1), $2.expr()}
  }
| opt_expr_list
  {
    $$.val = $1.exprs()
  }

substr_from:
  FROM a_expr
  {
    $$.val = $2.expr()
  }

substr_for:
  FOR a_expr
  {
    $$.val = $2.expr()
  }

trim_list:
  a_expr FROM expr_list
  {
    $$.val = append($3.exprs(), $1.expr())
  }
| FROM expr_list
  {
    $$.val = $2.exprs()
  }
| expr_list
  {
    $$.val = $1.exprs()
  }

in_expr:
  select_with_parens
  {
    $$.val = &tree.Subquery{Select: $1.selectStmt()}
  }
| expr_tuple1_ambiguous

// Define SQL-style CASE clause.
// - Full specification
//      CASE WHEN a = b THEN c ... ELSE d END
// - Implicit argument
//      CASE a WHEN b THEN c ... ELSE d END
case_expr:
  CASE case_arg when_clause_list case_default END
  {
    $$.val = &tree.CaseExpr{Expr: $2.expr(), Whens: $3.whens(), Else: $4.expr()}
  }

when_clause_list:
  // There must be at least one
  when_clause
  {
    $$.val = []*tree.When{$1.when()}
  }
| when_clause_list when_clause
  {
    $$.val = append($1.whens(), $2.when())
  }

when_clause:
  WHEN a_expr THEN a_expr
  {
    $$.val = &tree.When{Cond: $2.expr(), Val: $4.expr()}
  }

case_default:
  ELSE a_expr
  {
    $$.val = $2.expr()
  }
| /* EMPTY */
  {
    $$.val = tree.Expr(nil)
  }

case_arg:
  a_expr
| /* EMPTY */
  {
    $$.val = tree.Expr(nil)
  }

array_subscript:
  '[' a_expr ']'
  {
    $$.val = &tree.ArraySubscript{Begin: $2.expr()}
  }
| '[' opt_slice_bound ':' opt_slice_bound ']'
  {
    $$.val = &tree.ArraySubscript{Begin: $2.expr(), End: $4.expr(), Slice: true}
  }

opt_slice_bound:
  a_expr
| /*EMPTY*/
  {
    $$.val = tree.Expr(nil)
  }

array_subscripts:
  array_subscript
  {
    $$.val = tree.ArraySubscripts{$1.arraySubscript()}
  }
| array_subscripts array_subscript
  {
    $$.val = append($1.arraySubscripts(), $2.arraySubscript())
  }

opt_asymmetric:
  ASYMMETRIC {}
| /* EMPTY */ {}

target_list:
  target_elem
  {
    $$.val = tree.SelectExprs{$1.selExpr()}
  }
| target_list ',' target_elem
  {
    $$.val = append($1.selExprs(), $3.selExpr())
  }

opt_target_list:
  target_list
  {
    $$.val = $1.selExprs()
  }
| /* EMPTY */
  {
    $$.val = tree.SelectExprs{}
  }

target_elem:
  a_expr AS target_name
  {
    $$.val = tree.SelectExpr{Expr: $1.expr(), As: tree.UnrestrictedName($3)}
  }
  // We support omitting AS only for column labels that aren't any known
  // keyword. There is an ambiguity against postfix operators: is "a ! b" an
  // infix expression, or a postfix expression and a column label?  We prefer
  // to resolve this as an infix expression, which we accomplish by assigning
  // IDENT a precedence higher than POSTFIXOP.
| a_expr bare_col_label
  {
    $$.val = tree.SelectExpr{Expr: $1.expr(), As: tree.UnrestrictedName($2)}
  }
| a_expr
  {
    $$.val = tree.SelectExpr{Expr: $1.expr()}
  }
| '*'
  {
    $$.val = tree.StarSelectExpr()
  }

bare_col_label:
  IDENT
| bare_label_keywords

// Names and constants.

table_index_name_list:
  table_index_name
  {
    $$.val = tree.TableIndexNames{$1.newTableIndexName()}
  }
| table_index_name_list ',' table_index_name
  {
    $$.val = append($1.newTableIndexNames(), $3.newTableIndexName())
  }

table_pattern_list:
  table_pattern
  {
    $$.val = tree.TablePatterns{$1.unresolvedName()}
  }
| table_pattern_list ',' table_pattern
  {
    $$.val = append($1.tablePatterns(), $3.unresolvedName())
  }

// An index can be specified in a few different ways:
//
//   - with explicit table name:
//       <table>@<index>
//       <schema>.<table>@<index>
//       <catalog/db>.<table>@<index>
//       <catalog/db>.<schema>.<table>@<index>
//
//   - without explicit table name:
//       <index>
//       <schema>.<index>
//       <catalog/db>.<index>
//       <catalog/db>.<schema>.<index>
table_index_name:
  table_name '@' index_name
  {
    name := $1.unresolvedObjectName().ToTableName()
    $$.val = tree.TableIndexName{
       Table: name,
       Index: tree.UnrestrictedName($3),
    }
  }
| standalone_index_name
  {
    // Treat it as a table name, then pluck out the ObjectName.
    name := $1.unresolvedObjectName().ToTableName()
    indexName := tree.UnrestrictedName(name.ObjectName)
    name.ObjectName = ""
    $$.val = tree.TableIndexName{
        Table: name,
        Index: indexName,
    }
  }

// table_pattern selects zero or more tables using a wildcard.
// Accepted patterns:
// - Patterns accepted by db_object_name
//   <table>
//   <schema>.<table>
//   <catalog/db>.<schema>.<table>
// - Wildcards:
//   <db/catalog>.<schema>.*
//   <schema>.*
//   *
table_pattern:
  simple_db_object_name
  {
     $$.val = $1.unresolvedObjectName().ToUnresolvedName()
  }
| complex_table_pattern

// complex_table_pattern is the part of table_pattern which recognizes
// every pattern not composed of a single identifier.
complex_table_pattern:
  complex_db_object_name
  {
     $$.val = $1.unresolvedObjectName().ToUnresolvedName()
  }
| db_object_name_component '.' unrestricted_name '.' '*'
  {
     $$.val = &tree.UnresolvedName{Star: true, NumParts: 3, Parts: tree.NameParts{"", $3, $1}}
  }
| db_object_name_component '.' '*'
  {
     $$.val = &tree.UnresolvedName{Star: true, NumParts: 2, Parts: tree.NameParts{"", $1}}
  }
| '*'
  {
     $$.val = &tree.UnresolvedName{Star: true, NumParts: 1}
  }

name_list:
  name
  {
    $$.val = tree.NameList{tree.Name($1)}
  }
| name_list ',' name
  {
    $$.val = append($1.nameList(), tree.Name($3))
  }

// Constants
numeric_only:
  signed_iconst
| signed_fconst

signed_iconst:
  ICONST
| only_signed_iconst

only_signed_iconst:
  '+' ICONST
  {
    $$.val = $2.numVal()
  }
| '-' ICONST
  {
    n := $2.numVal()
    n.SetNegative()
    $$.val = n
  }

signed_fconst:
  FCONST
| only_signed_fconst

only_signed_fconst:
  '+' FCONST
  {
    $$.val = $2.numVal()
  }
| '-' FCONST
  {
    n := $2.numVal()
    n.SetNegative()
    $$.val = n
  }

// iconst32 accepts only unsigned integer literals that fit in an int32.
iconst32:
  ICONST
  {
    val, err := $1.numVal().AsInt32()
    if err != nil { return setErr(sqllex, err) }
    $$.val = val
  }

// signed_iconst64 is a variant of signed_iconst which only accepts (signed) integer literals that fit in an int64.
// If you use signed_iconst, you have to call AsInt64(), which returns an error if the value is too big.
// This rule just doesn't match in that case.
signed_iconst64:
  signed_iconst
  {
    val, err := $1.numVal().AsInt64()
    if err != nil { return setErr(sqllex, err) }
    $$.val = val
  }

// iconst64 accepts only unsigned integer literals that fit in an int64.
iconst64:
  ICONST
  {
    val, err := $1.numVal().AsInt64()
    if err != nil { return setErr(sqllex, err) }
    $$.val = val
  }

interval_value:
  INTERVAL SCONST opt_interval_qualifier
  {
    var t *types.T
    if $3.val == nil {
      t = types.Interval
    } else {
      t = types.MakeInterval($3.intervalTypeMetadata())
    }
    $$.val = &tree.CastExpr{
      Expr: tree.NewStrVal($2),
      Type: t,
      // TODO(#sql-sessions): This should be CastPrepend, but
      // that does not work with parenthesized expressions
      // (using FmtAlwaysGroupExprs).
      SyntaxMode: tree.CastShort,
    }
  }
| INTERVAL '(' iconst32 ')' SCONST
  {
    prec := $3.int32()
    if prec < 0 || prec > 6 {
      sqllex.Error(fmt.Sprintf("precision %d out of range", prec))
      return 1
    }
    $$.val = &tree.CastExpr{
      Expr: tree.NewStrVal($5),
      Type: types.MakeInterval(
        types.IntervalTypeMetadata{Precision: prec, PrecisionIsSet: true},
      ),
      // TODO(#sql-sessions): This should be CastPrepend, but
      // that does not work with parenthesized expressions
      // (using FmtAlwaysGroupExprs).
      SyntaxMode: tree.CastShort,
    }
  }

// Name classification hierarchy.
//
// IDENT is the lexeme returned by the lexer for identifiers that match no
// known keyword. In most cases, we can accept certain keywords as names, not
// only IDENTs. We prefer to accept as many such keywords as possible to
// minimize the impact of "reserved words" on programmers. So, we divide names
// into several possible classes. The classification is chosen in part to make
// keywords acceptable as names wherever possible.

// Names specific to syntactic positions.
//
// The non-terminals "name", "unrestricted_name", "non_reserved_word",
// "unreserved_keyword", "non_reserved_word_or_sconst" etc. defined
// below are low-level, structural constructs.
//
// They are separate only because having them all as one rule would
// make the rest of the grammar ambiguous. However, because they are
// separate the question is then raised throughout the rest of the
// grammar: which of the name non-terminals should one use when
// defining a grammar rule?  Is an index a "name" or
// "unrestricted_name"? A partition? What about an index option?
//
// To make the decision easier, this section of the grammar creates
// meaningful, purpose-specific aliases to the non-terminals. These
// both make it easier to decide "which one should I use in this
// context" and also improves the readability of
// automatically-generated syntax diagrams.

// Note: newlines between non-terminals matter to the doc generator.

collation_name:        unrestricted_name

partition_name:        unrestricted_name

index_name:            unrestricted_name

opt_index_name:        opt_name

zone_name:             unrestricted_name

target_name:           unrestricted_name

constraint_name:       name

database_name:         name

column_name:           name

family_name:           name

opt_family_name:       opt_name

table_alias_name:      name

statistics_name:       name

window_name:           name

view_name:             table_name

type_name:             db_object_name

sequence_name:         db_object_name

region_name:
  name
| SCONST

region_name_list:
  region_name
  {
    $$.val = tree.NameList{tree.Name($1)}
  }
| region_name_list ',' region_name
  {
    $$.val = append($1.nameList(), tree.Name($3))
  }

schema_name:           name

qualifiable_schema_name:
	name
	{
		$$.val = tree.ObjectNamePrefix{SchemaName: tree.Name($1), ExplicitSchema: true}
	}
| name '.' name
	{
		$$.val = tree.ObjectNamePrefix{CatalogName: tree.Name($1), SchemaName: tree.Name($3), ExplicitCatalog: true, ExplicitSchema: true}
	}

schema_name_list:
  qualifiable_schema_name
  {
    $$.val = tree.ObjectNamePrefixList{$1.objectNamePrefix()}
  }
| schema_name_list ',' qualifiable_schema_name
  {
    $$.val = append($1.objectNamePrefixList(), $3.objectNamePrefix())
  }

schema_wildcard:
	wildcard_pattern
	{
    $$.val = tree.ObjectNamePrefixList{$1.objectNamePrefix()}
	}

wildcard_pattern:
	name '.' '*'
	{
		$$.val = tree.ObjectNamePrefix{CatalogName: tree.Name($1), SchemaName: tree.Name('*'), ExplicitCatalog: true, ExplicitSchema: true}
	}

opt_schema_name:
	qualifiable_schema_name
| /* EMPTY */
	{
		$$.val = tree.ObjectNamePrefix{ExplicitSchema: false}
	}

table_name:            db_object_name

db_name:               db_object_name

standalone_index_name: db_object_name

explain_option_name:   non_reserved_word

cursor_name:           name

// Names for column references.
// Accepted patterns:
// <colname>
// <table>.<colname>
// <schema>.<table>.<colname>
// <catalog/db>.<schema>.<table>.<colname>
//
// Note: the rule for accessing compound types, if those are ever
// supported, is not to be handled here. The syntax `a.b.c.d....y.z`
// in `select a.b.c.d from t` *always* designates a column `z` in a
// table `y`, regardless of the meaning of what's before.
column_path:
  name
  {
      $$.val = &tree.UnresolvedName{NumParts:1, Parts: tree.NameParts{$1}}
  }
| prefixed_column_path

prefixed_column_path:
  db_object_name_component '.' unrestricted_name
  {
      $$.val = &tree.UnresolvedName{NumParts:2, Parts: tree.NameParts{$3,$1}}
  }
| db_object_name_component '.' unrestricted_name '.' unrestricted_name
  {
      $$.val = &tree.UnresolvedName{NumParts:3, Parts: tree.NameParts{$5,$3,$1}}
  }
| db_object_name_component '.' unrestricted_name '.' unrestricted_name '.' unrestricted_name
  {
      $$.val = &tree.UnresolvedName{NumParts:4, Parts: tree.NameParts{$7,$5,$3,$1}}
  }

// Names for column references and wildcards.
// Accepted patterns:
// - those from column_path
// - <table>.*
// - <schema>.<table>.*
// - <catalog/db>.<schema>.<table>.*
// The single unqualified star is handled separately by target_elem.
column_path_with_star:
  column_path
| db_object_name_component '.' unrestricted_name '.' unrestricted_name '.' '*'
  {
    $$.val = &tree.UnresolvedName{Star:true, NumParts:4, Parts: tree.NameParts{"",$5,$3,$1}}
  }
| db_object_name_component '.' unrestricted_name '.' '*'
  {
    $$.val = &tree.UnresolvedName{Star:true, NumParts:3, Parts: tree.NameParts{"",$3,$1}}
  }
| db_object_name_component '.' '*'
  {
    $$.val = &tree.UnresolvedName{Star:true, NumParts:2, Parts: tree.NameParts{"",$1}}
  }

// Names for functions.
// The production for a qualified func_name has to exactly match the production
// for a column_path, because we cannot tell which we are parsing until
// we see what comes after it ('(' or SCONST for a func_name, anything else for
// a name).
// However we cannot use column_path directly, because for a single function name
// we allow more possible tokens than a simple column name.
func_name:
  type_function_name
  {
    $$.val = &tree.UnresolvedName{NumParts:1, Parts: tree.NameParts{$1}}
  }
| prefixed_column_path
| INDEX_BEFORE_PAREN
  {
    $$.val = &tree.UnresolvedName{NumParts:1, Parts: tree.NameParts{$1}}
  }

// func_name_no_crdb_extra is the same rule as func_name, but does not
// contain some CRDB specific keywords like FAMILY.
func_name_no_crdb_extra:
  type_function_name_no_crdb_extra
  {
    $$.val = &tree.UnresolvedName{NumParts:1, Parts: tree.NameParts{$1}}
  }
| prefixed_column_path

// Names for database objects (tables, sequences, views, stored functions).
// Accepted patterns:
// <table>
// <schema>.<table>
// <catalog/db>.<schema>.<table>
db_object_name:
  simple_db_object_name
| complex_db_object_name

// simple_db_object_name is the part of db_object_name that recognizes
// simple identifiers.
simple_db_object_name:
  db_object_name_component
  {
    aIdx := sqllex.(*lexer).NewAnnotation()
    res, err := tree.NewUnresolvedObjectName(1, [3]string{$1}, aIdx)
    if err != nil { return setErr(sqllex, err) }
    $$.val = res
  }

// complex_db_object_name is the part of db_object_name that recognizes
// composite names (not simple identifiers).
// It is split away from db_object_name in order to enable the definition
// of table_pattern.
complex_db_object_name:
  db_object_name_component '.' unrestricted_name
  {
    aIdx := sqllex.(*lexer).NewAnnotation()
    res, err := tree.NewUnresolvedObjectName(2, [3]string{$3, $1}, aIdx)
    if err != nil { return setErr(sqllex, err) }
    $$.val = res
  }
| db_object_name_component '.' unrestricted_name '.' unrestricted_name
  {
    aIdx := sqllex.(*lexer).NewAnnotation()
    res, err := tree.NewUnresolvedObjectName(3, [3]string{$5, $3, $1}, aIdx)
    if err != nil { return setErr(sqllex, err) }
    $$.val = res
  }

// DB object name component -- this cannot not include any reserved
// keyword because of ambiguity after FROM, but we've been too lax
// with reserved keywords and made INDEX and FAMILY reserved, so we're
// trying to gain them back here.
db_object_name_component:
  name
| type_func_name_crdb_extra_keyword
| cockroachdb_extra_reserved_keyword

// General name --- names that can be column, table, etc names.
name:
  IDENT
| unreserved_keyword
| col_name_keyword

opt_name:
  name
| /* EMPTY */
  {
    $$ = ""
  }

opt_name_parens:
  '(' name ')'
  {
    $$ = $2
  }
| /* EMPTY */
  {
    $$ = ""
  }

// Structural, low-level names

// Non-reserved word and also string literal constants.
non_reserved_word_or_sconst:
  non_reserved_word
| SCONST

// Type/function identifier --- names that can be type or function names.
type_function_name:
  IDENT
| unreserved_keyword
| type_func_name_keyword

// Type/function identifier without CRDB extra reserved keywords.
type_function_name_no_crdb_extra:
  IDENT
| unreserved_keyword
| type_func_name_no_crdb_extra_keyword

param_name:
  type_function_name

// Any not-fully-reserved word --- these names can be, eg, variable names.
non_reserved_word:
  IDENT
| unreserved_keyword
| col_name_keyword
| type_func_name_keyword

// Unrestricted name --- allowable names when there is no ambiguity with even
// reserved keywords, like in "AS" clauses. This presently includes *all*
// Postgres keywords.
unrestricted_name:
  IDENT
| unreserved_keyword
| col_name_keyword
| type_func_name_keyword
| reserved_keyword

// Keyword category lists. Generally, every keyword present in the Postgres
// grammar should appear in exactly one of these "x_keyword" lists.
//
// Put a new keyword into the first list that it can go into without causing
// shift or reduce conflicts. The earlier lists define "less reserved"
// categories of keywords.
//
// Note: also add the **new** keyword to `bare_label_keywords` list to not break
// user queries using column label without `AS`.
// "Unreserved" keywords --- available for use as any kind of name.
unreserved_keyword:
  ABORT
| ABSOLUTE
| ACTION
| ACCESS
| ADD
| ADMIN
| AFTER
| AGGREGATE
| ALTER
| ALWAYS
| ASENSITIVE
| AS_JSON
| AT
| ATOMIC
| ATTRIBUTE
| AUTOMATIC
| AVAILABILITY
| AVOID_FULL_SCAN
| BACKUP
| BACKUPS
| BACKWARD
| BATCH
| BEFORE
| BEGIN
| BIDIRECTIONAL
| BINARY
| BUCKET_COUNT
| BUNDLE
| BY
| BYPASSRLS
| CACHE
| CALL
| CALLED
| CANCEL
| CANCELQUERY
| CAPABILITIES
| CAPABILITY
| CASCADE
| CHANGEFEED
| CHECK_FILES
| CLOSE
| CLUSTER
| CLUSTERS
| COLUMNS
| COMMENT
| COMMENTS
| COMMIT
| COMMITTED
| COMPACT
| COMPLETE
| COMPLETIONS
| CONFLICT
| CONFIGURATION
| CONFIGURATIONS
| CONFIGURE
| CONNECTION
| CONNECTIONS
| CONSTRAINTS
| CONTROLCHANGEFEED
| CONTROLJOB
| CONVERSION
| CONVERT
| COPY
| COST
| COVERING
| CREATEDB
| CREATELOGIN
| CREATEROLE
| CSV
| CUBE
| CURRENT
| CURSOR
| CYCLE
| DATA
| DATABASE
| DATABASES
| DAY
| DEALLOCATE
| DEBUG_IDS
| DECLARE
| DELETE
| DEFAULTS
| DEFERRED
| DEFINER
| DELIMITER
| DEPENDS
| DESTINATION
| DETACHED
| DETAILS
| DISABLE
| DISCARD
| DOMAIN
| DOUBLE
| DROP
| EACH
| ENABLE
| ENCODING
| ENCRYPTED
| ENCRYPTION_PASSPHRASE
| ENCRYPTION_INFO_DIR
| ENUM
| ENUMS
| ESCAPE
| EXCLUDE
| EXCLUDING
| EXECUTE
| EXECUTION
| EXPERIMENTAL
| EXPERIMENTAL_AUDIT
| EXPERIMENTAL_FINGERPRINTS
| EXPERIMENTAL_RELOCATE
| EXPERIMENTAL_REPLICA
| EXPIRATION
| EXPLAIN
| EXPORT
| EXTENSION
| EXTERNAL
| EXTREMES
| FAILURE
| FILES
| FILTER
| FIRST
| FOLLOWING
| FORMAT
| FORCE
| FORCE_NOT_NULL
| FORCE_NULL
| FORCE_QUOTE
| FORCE_INDEX
| FORCE_INVERTED_INDEX
| FORCE_ZIGZAG
| FORWARD
| FREEZE
| FUNCTION
| FUNCTIONS
| GENERATED
| GEOMETRYM
| GEOMETRYZ
| GEOMETRYZM
| GEOMETRYCOLLECTION
| GEOMETRYCOLLECTIONM
| GEOMETRYCOLLECTIONZ
| GEOMETRYCOLLECTIONZM
| GLOBAL
| GOAL
| GRANTEE
| GRANTS
| GROUPS
| HASH
| HEADER
| HIGH
| HISTOGRAM
| HOLD
| HOUR
| IDENTITY
| IMMEDIATE
| IMMEDIATELY
| IMMUTABLE
| IMPORT
| INCLUDE
| INCLUDING
| INCLUDE_ALL_SECONDARY_TENANTS
| INCLUDE_ALL_VIRTUAL_CLUSTERS
| INCREMENT
| INCREMENTAL
| INCREMENTAL_LOCATION
| INDEX
| INDEXES
| INHERITS
| INJECT
| INPUT
| INSERT
| INSTEAD
| INTO_DB
| INVERTED
| INVISIBLE
| ISOLATION
| INVOKER
| JOB
| JOBS
| JSON
| KEY
| KEYS
| KMS
| KV
| LABEL
| LANGUAGE
| LAST
| LATEST
| LC_COLLATE
| LC_CTYPE
| LEAKPROOF
| LEASE
| LESS
| LEVEL
| LINESTRING
| LINESTRINGM
| LINESTRINGZ
| LINESTRINGZM
| LIST
| LOCAL
| LOCKED
| LOGICAL
| LOGICALLY
| LOGIN
| LOCALITY
| LOOKUP
| LOW
| MATCH
| MATERIALIZED
| MAXVALUE
| MERGE
| METHOD
| MINUTE
| MINVALUE
| MODIFYCLUSTERSETTING
| MODIFYSQLCLUSTERSETTING
| MULTILINESTRING
| MULTILINESTRINGM
| MULTILINESTRINGZ
| MULTILINESTRINGZM
| MULTIPOINT
| MULTIPOINTM
| MULTIPOINTZ
| MULTIPOINTZM
| MULTIPOLYGON
| MULTIPOLYGONM
| MULTIPOLYGONZ
| MULTIPOLYGONZM
| MODE
| MONTH
| MOVE
| NAMES
| NAN
| NEVER
| NEW
| NEW_DB_NAME
| NEW_KMS
| NEXT
| NO
| NORMAL
| NOTHING
| NO_INDEX_JOIN
| NO_ZIGZAG_JOIN
| NO_FULL_SCAN
| NOBYPASSRLS
| NOCREATEDB
| NOCREATELOGIN
| NOCANCELQUERY
| NOCREATEROLE
| NOCONTROLCHANGEFEED
| NOCONTROLJOB
| NODE
| NOLOGIN
| NOMODIFYCLUSTERSETTING
| NONVOTERS
| NOREPLICATION
| NOSQLLOGIN
| NOVIEWACTIVITY
| NOVIEWACTIVITYREDACTED
| NOVIEWCLUSTERSETTING
| NOWAIT
| NULLS
| IGNORE_FOREIGN_KEYS
| INSENSITIVE
| OF
| OFF
| OIDS
| OLD
| OLD_KMS
| OPERATOR
| OPT
| OPTION
| OPTIONS
| ORDINALITY
| OTHERS
| OVER
| OWNED
| OWNER
| PARALLEL
| PARENT
| PARTIAL
| PARTITION
| PARTITIONS
| PASSWORD
| PAUSE
| PAUSED
| PER
| PERMISSIVE
| PHYSICAL
| PLACEMENT
| PLAN
| PLANS
| POINTM
| POINTZ
| POINTZM
| POLICIES
| POLICY
| POLYGONM
| POLYGONZ
| POLYGONZM
| PRECEDING
| PREPARE
| PREPARED
| PRESERVE
| PRIOR
| PRIORITY
| PRIVILEGES
| PROCEDURE
| PROCEDURES
| PUBLIC
| PUBLICATION
| QUERIES
| QUERY
| QUOTE
| RANGE
| RANGES
| READ
| REASON
| REASSIGN
| RECURRING
| RECURSIVE
| REDACT
| REF
| REFERENCING
| REFRESH
| REGION
| REGIONAL
| REGIONS
| REINDEX
| RELATIVE
| RELEASE
| RELOCATE
| REMOVE_REGIONS
| RENAME
| REPEATABLE
| REPLACE
| REPLICATED
| REPLICATION
| RESET
| RESTART
| RESTORE
| RESTRICT
| RESTRICTED
| RESTRICTIVE
| RESUME
| RETENTION
| RETRY
| RETURN
| RETURNS
| REVISION_HISTORY
| REVOKE
| ROLE
| ROLES
| ROLLBACK
| ROLLUP
| ROUTINES
| ROWS
| RULE
| RUNNING
| SCHEDULE
| SCHEDULES
| SCHEMA_ONLY
| SCROLL
| SETTING
| SETTINGS
| STATUS
| SAVEPOINT
| SCANS
| SCATTER
| SCHEMA
| SCHEMAS
| SCRUB
| SEARCH
| SECOND
| SECURITY
| SECONDARY
| SERIALIZABLE
| SEQUENCE
| SEQUENCES
| SERVER
| SERVICE
| SESSION
| SESSIONS
| SET
| SETS
| SHARE
| SHARED
| SHOW
| SIMPLE
| SIZE
| SKIP
| SKIP_LOCALITIES_CHECK
| SKIP_MISSING_FOREIGN_KEYS
| SKIP_MISSING_SEQUENCES
| SKIP_MISSING_SEQUENCE_OWNERS
| SKIP_MISSING_VIEWS
| SKIP_MISSING_UDFS
| SNAPSHOT
| SPLIT
| SQL
| SQLLOGIN
| STABLE
| START
| STATE
| STATEMENT
| STATEMENTS
| STATISTICS
| STDIN
| STDOUT
| STOP
| STORAGE
| STORE
| STORED
| STORING
| STRAIGHT
| STREAM
| STRICT
| SUBSCRIPTION
| SUBJECT
| SUPER
| SUPPORT
| SURVIVE
| SURVIVAL
| SYNTAX
| SYSTEM
| TABLES
| TABLESPACE
| TEMP
| TEMPLATE
| TEMPORARY
| TENANT
| TENANT_NAME
| TENANTS
| TESTING_RELOCATE
| TEXT
| TIES
| TRACE
| TRACING
| TRANSACTION
| TRANSACTIONS
| TRANSFER
| TRANSFORM
| TRIGGER
| TRIGGERS
| TRUNCATE
| TRUSTED
| TYPE
| TYPES
| THROTTLING
| UNIDIRECTIONAL
| UNBOUNDED
| UNCOMMITTED
| UNKNOWN
| UNLISTEN
| UNLOGGED
| UNSAFE_RESTORE_INCOMPATIBLE_VERSION
| UNSET
| UNSPLIT
| UNTIL
| UPDATE
| UPDATES_CLUSTER_MONITORING_METRICS
| UPSERT
| USE
| USERS
| VALID
| VALIDATE
| VALUE
| VARIABLES
| VARYING
| VERIFY_BACKUP_TABLE_DATA
| VIEW
| VIEWACTIVITY
| VIEWACTIVITYREDACTED
| VIEWCLUSTERMETADATA
| VIEWCLUSTERSETTING
| VIEWDEBUG
| VIRTUAL_CLUSTER_NAME
| VIRTUAL_CLUSTER
| VISIBLE
| VISIBILITY
| VOLATILE
| VOTERS
| WITHIN
| WITHOUT
| WRITE
| YEAR
| ZONE

// Column label --- keywords that can be column label that doesn't use "AS"
// before it. This is to guarantee that any new keyword won't break user
// query like "SELECT col label FROM tab" where "label" is a new keyword.
// Any new keyword should be added to this list.
bare_label_keywords:
  ABORT
| ABSOLUTE
| ACCESS
| ACTION
| ADD
| ADMIN
| AFTER
| AGGREGATE
| ALL
| ALTER
| ALWAYS
| ANALYSE
| ANALYZE
| AND
| ANNOTATE_TYPE
| ANY
| ASC
| ASENSITIVE
| ASYMMETRIC
| AS_JSON
| AT
| ATOMIC
| ATTRIBUTE
| AUTHORIZATION
| AUTOMATIC
| AVAILABILITY
| AVOID_FULL_SCAN
| BACKUP
| BACKUPS
| BACKWARD
| BATCH
| BEFORE
| BEGIN
| BETWEEN
| BIDIRECTIONAL
| BIGINT
| BINARY
| BIT
| BOOLEAN
| BOTH
| BOX2D
| BUCKET_COUNT
| BUNDLE
| BY
| BYPASSRLS
| CACHE
| CALL
| CALLED
| CANCEL
| CANCELQUERY
| CAPABILITIES
| CAPABILITY
| CASCADE
| CASE
| CAST
| CHANGEFEED
| CHARACTERISTICS
| CHECK
| CHECK_FILES
| CLOSE
| CLUSTER
| CLUSTERS
| COALESCE
| COLLATION
| COLUMN
| COLUMNS
| COMMENT
| COMMENTS
| COMMIT
| COMMITTED
| COMPACT
| COMPLETE
| COMPLETIONS
| CONCURRENTLY
| CONFIGURATION
| CONFIGURATIONS
| CONFIGURE
| CONFLICT
| CONNECTION
| CONNECTIONS
| CONSTRAINT
| CONSTRAINTS
| CONTROLCHANGEFEED
| CONTROLJOB
| CONVERSION
| CONVERT
| COPY
| COST
| COVERING
| CREATEDB
| CREATELOGIN
| CREATEROLE
| CROSS
| CSV
| CUBE
| CURRENT
| CURRENT_CATALOG
| CURRENT_DATE
| CURRENT_ROLE
| CURRENT_SCHEMA
| CURRENT_TIME
| CURRENT_TIMESTAMP
| CURRENT_USER
| CURSOR
| CYCLE
| DATA
| DATABASE
| DATABASES
| DEALLOCATE
| DEBUG_IDS
| DEC
| DECIMAL
| DECLARE
| DEFAULT
| DEFAULTS
| DEFERRABLE
| DEFERRED
| DEFINER
| DELETE
| DELIMITER
| DEPENDS
| DESC
| DESTINATION
| DETACHED
| DETAILS
| DISABLE
| DISCARD
| DISTINCT
| DO
| DOMAIN
| DOUBLE
| DROP
| EACH
| ELSE
| ENABLE
| ENCODING
| ENCRYPTED
| ENCRYPTION_INFO_DIR
| ENCRYPTION_PASSPHRASE
| END
| ENUM
| ENUMS
| ESCAPE
| EXCLUDE
| EXCLUDING
| EXECUTE
| EXECUTION
| EXISTS
| EXPERIMENTAL
| EXPERIMENTAL_AUDIT
| EXPERIMENTAL_FINGERPRINTS
| EXPERIMENTAL_RELOCATE
| EXPERIMENTAL_REPLICA
| EXPIRATION
| EXPLAIN
| EXPORT
| EXTENSION
| EXTERNAL
| EXTRACT
| EXTRACT_DURATION
| EXTREMES
| FAILURE
| FALSE
| FAMILY
| FILES
| FIRST
| FLOAT
| FOLLOWING
| FORCE
| FORCE_NOT_NULL
| FORCE_NULL
| FORCE_QUOTE
| FORCE_INDEX
| FORCE_INVERTED_INDEX
| FORCE_ZIGZAG
| FOREIGN
| FORMAT
| FORWARD
| FREEZE
| FULL
| FUNCTION
| FUNCTIONS
| GENERATED
| GEOGRAPHY
| GEOMETRY
| GEOMETRYCOLLECTION
| GEOMETRYCOLLECTIONM
| GEOMETRYCOLLECTIONZ
| GEOMETRYCOLLECTIONZM
| GEOMETRYM
| GEOMETRYZ
| GEOMETRYZM
| GLOBAL
| GOAL
| GRANTEE
| GRANTS
| GREATEST
| GROUPING
| GROUPS
| HASH
| HEADER
| HIGH
| HISTOGRAM
| HOLD
| IDENTITY
| IF
| IFERROR
| IFNULL
| IGNORE_FOREIGN_KEYS
| ILIKE
| IMMEDIATE
| IMMEDIATELY
| IMMUTABLE
| IMPORT
| IN
| INCLUDE
| INCLUDE_ALL_SECONDARY_TENANTS
| INCLUDE_ALL_VIRTUAL_CLUSTERS
| INCLUDING
| INCREMENT
| INCREMENTAL
| INCREMENTAL_LOCATION
| INDEX
| INDEXES
| INDEX_AFTER_ORDER_BY_BEFORE_AT
| INDEX_BEFORE_NAME_THEN_PAREN
| INDEX_BEFORE_PAREN
| INHERITS
| INITIALLY
| INJECT
| INNER
| INOUT
| INPUT
| INSENSITIVE
| INSERT
| INSTEAD
| INT
| INTEGER
| INTERVAL
| INTO_DB
| INVERTED
| INVISIBLE
| INVOKER
| IS
| ISERROR
| ISOLATION
| JOB
| JOBS
| JOIN
| JSON
| KEY
| KEYS
| KMS
| KV
| LABEL
| LANGUAGE
| LAST
| LATERAL
| LATEST
| LC_COLLATE
| LC_CTYPE
| LEADING
| LEAKPROOF
| LEASE
| LEAST
| LEFT
| LESS
| LEVEL
| LIKE
| LINESTRING
| LINESTRINGM
| LINESTRINGZ
| LINESTRINGZM
| LIST
| LOCAL
| LOCALITY
| LOCALTIME
| LOCALTIMESTAMP
| LOCKED
| LOGICAL
| LOGICALLY
| LOGIN
| LOOKUP
| LOW
| MATCH
| MATERIALIZED
| MAXVALUE
| MERGE
| METHOD
| MINVALUE
| MODE
| MODIFYCLUSTERSETTING
| MODIFYSQLCLUSTERSETTING
| MOVE
| MULTILINESTRING
| MULTILINESTRINGM
| MULTILINESTRINGZ
| MULTILINESTRINGZM
| MULTIPOINT
| MULTIPOINTM
| MULTIPOINTZ
| MULTIPOINTZM
| MULTIPOLYGON
| MULTIPOLYGONM
| MULTIPOLYGONZ
| MULTIPOLYGONZM
| NAMES
| NAN
| NATURAL
| NEVER
| NEW
| NEW_DB_NAME
| NEW_KMS
| NEXT
| NO
| NOBYPASSRLS
| NOCANCELQUERY
| NOCONTROLCHANGEFEED
| NOCONTROLJOB
| NOCREATEDB
| NOCREATELOGIN
| NOCREATEROLE
| NODE
| NOLOGIN
| NOMODIFYCLUSTERSETTING
| NONE
| NONVOTERS
| NORMAL
| NOREPLICATION
| NOSQLLOGIN
| NOT
| NOTHING
| NOTHING_AFTER_RETURNING
| NOVIEWACTIVITY
| NOVIEWACTIVITYREDACTED
| NOVIEWCLUSTERSETTING
| NOWAIT
| NO_FULL_SCAN
| NO_INDEX_JOIN
| NO_ZIGZAG_JOIN
| NULL
| NULLIF
| NULLS
| NUMERIC
| OF
| OFF
| OIDS
| OLD
| OLD_KMS
| ONLY
| OPERATOR
| OPT
| OPTION
| OPTIONS
| OR
| ORDINALITY
| OTHERS
| OUT
| OUTER
| OVERLAY
| OWNED
| OWNER
| PARALLEL
| PARENT
| PARTIAL
| PARTITION
| PARTITIONS
| PASSWORD
| PAUSE
| PAUSED
| PER
| PERMISSIVE
| PHYSICAL
| PLACEMENT
| PLACING
| PLAN
| PLANS
| POINT
| POINTM
| POINTZ
| POINTZM
| POLICIES
| POLICY
| POLYGON
| POLYGONM
| POLYGONZ
| POLYGONZM
| POSITION
| PRECEDING
| PREPARE
| PREPARED
| PRESERVE
| PRIMARY
| PRIOR
| PRIORITY
| PRIVILEGES
| PROCEDURE
| PROCEDURES
| PUBLIC
| PUBLICATION
| QUERIES
| QUERY
| QUOTE
| RANGE
| RANGES
| READ
| REAL
| REASON
| REASSIGN
| RECURRING
| RECURSIVE
| REDACT
| REF
| REFERENCES
| REFERENCING
| REFRESH
| REGION
| REGIONAL
| REGIONS
| REINDEX
| RELATIVE
| RELEASE
| RELOCATE
| REMOVE_REGIONS
| RENAME
| REPEATABLE
| REPLACE
| REPLICATED
| REPLICATION
| RESET
| RESTART
| RESTORE
| RESTRICT
| RESTRICTED
| RESTRICTIVE
| RESUME
| RETENTION
| RETRY
| RETURN
| RETURNS
| REVISION_HISTORY
| REVOKE
| RIGHT
| ROLE
| ROLES
| ROLLBACK
| ROLLUP
| ROUTINES
| ROW
| ROWS
| RULE
| RUNNING
| SAVEPOINT
| SCANS
| SCATTER
| SCHEDULE
| SCHEDULES
| SCHEMA
| SCHEMAS
| SCHEMA_ONLY
| SCROLL
| SCRUB
| SEARCH
| SECONDARY
| SECURITY
| SELECT
| SEQUENCE
| SEQUENCES
| SERIALIZABLE
| SERVER
| SERVICE
| SESSION
| SESSIONS
| SESSION_USER
| SET
| SETOF
| SETS
| SETTING
| SETTINGS
| SHARE
| SHARED
| SHOW
| SIMILAR
| SIMPLE
| SIZE
| SKIP
| SKIP_LOCALITIES_CHECK
| SKIP_MISSING_FOREIGN_KEYS
| SKIP_MISSING_SEQUENCES
| SKIP_MISSING_SEQUENCE_OWNERS
| SKIP_MISSING_UDFS
| SKIP_MISSING_VIEWS
| SMALLINT
| SNAPSHOT
| SOME
| SPLIT
| SQL
| SQLLOGIN
| STABLE
| START
| STATE
| STATEMENT
| STATEMENTS
| STATISTICS
| STATUS
| STDIN
| STDOUT
| STOP
| STORAGE
| STORE
| STORED
| STORING
| STRAIGHT
| STREAM
| STRICT
| STRING
| SUBSCRIPTION
| SUBSTRING
| SUBJECT
| SUPER
| SUPPORT
| SURVIVAL
| SURVIVE
| SYMMETRIC
| SYNTAX
| SYSTEM
| TABLE
| TABLES
| TABLESPACE
| TEMP
| TEMPLATE
| TEMPORARY
| TENANT
| TENANTS
| TENANT_NAME
| TESTING_RELOCATE
| TEXT
| THEN
| THROTTLING
| TIES
| TIME
| TIMESTAMP
| TIMESTAMPTZ
| TIMETZ
| TRACE
| TRACING
| TRAILING
| TRANSACTION
| TRANSACTIONS
| TRANSFER
| TRANSFORM
| TREAT
| TRIGGER
| TRIGGERS
| TRIM
| TRUE
| TRUNCATE
| TRUSTED
| TYPE
| TYPES
| UNBOUNDED
| UNCOMMITTED
| UNIDIRECTIONAL
| UNIQUE
| UNKNOWN
| UNLISTEN
| UNLOGGED
| UNSAFE_RESTORE_INCOMPATIBLE_VERSION
| UNSET
| UNSPLIT
| UNTIL
| UPDATE
| UPDATES_CLUSTER_MONITORING_METRICS
| UPSERT
| USE
| USER
| USERS
| USING
| VALID
| VALIDATE
| VALUE
| VALUES
| VARBIT
| VARCHAR
| VARIABLES
| VARIADIC
| VECTOR
| VERIFY_BACKUP_TABLE_DATA
| VIEW
| VIEWACTIVITY
| VIEWACTIVITYREDACTED
| VIEWCLUSTERMETADATA
| VIEWCLUSTERSETTING
| VIEWDEBUG
| VIRTUAL
| VIRTUAL_CLUSTER_NAME
| VIRTUAL_CLUSTER
| VISIBLE
| VISIBILITY
| VOLATILE
| VOTERS
| WHEN
| WORK
| WRITE
| ZONE


// Column identifier --- keywords that can be column, table, etc names.
//
// Many of these keywords will in fact be recognized as type or function names
// too; but they have special productions for the purpose, and so can't be
// treated as "generic" type or function names.
//
// The type names appearing here are not usable as function names because they
// can be followed by '(' in typename productions, which looks too much like a
// function call for an LR(1) parser.
//
// Note: also add the new keyword to `bare_label` list to not break
// user queries using column label without `AS`.
col_name_keyword:
  ANNOTATE_TYPE
| BETWEEN
| BIGINT
| BIT
| BOOLEAN
| BOX2D
| CHAR
| CHARACTER
| CHARACTERISTICS
| COALESCE
| DEC
| DECIMAL
| EXISTS
| EXTRACT
| EXTRACT_DURATION
| FLOAT
| GEOGRAPHY
| GEOMETRY
| GREATEST
| GROUPING
| IF
| IFERROR
| IFNULL
| INOUT
| INT
| INTEGER
| INTERVAL
| ISERROR
| LEAST
| NULLIF
| NUMERIC
| OUT
| OVERLAY
| POINT
| POLYGON
| POSITION
| PRECISION
| REAL
| ROW
| SETOF
| SMALLINT
| STRING
| SUBSTRING
| TIME
| TIMETZ
| TIMESTAMP
| TIMESTAMPTZ
| TREAT
| TRIM
| VALUES
| VARBIT
| VARCHAR
| VECTOR
| VIRTUAL
| WORK

// type_func_name_keyword contains both the standard set of
// type_func_name_keyword's along with the set of CRDB extensions.
type_func_name_keyword:
  type_func_name_no_crdb_extra_keyword
| type_func_name_crdb_extra_keyword

// Type/function identifier --- keywords that can be type or function names.
//
// Most of these are keywords that are used as operators in expressions; in
// general such keywords can't be column names because they would be ambiguous
// with variables, but they are unambiguous as function identifiers.
//
// Do not include POSITION, SUBSTRING, etc here since they have explicit
// productions in a_expr to support the goofy SQL9x argument syntax.
// - thomas 2000-11-28
//
// *** DO NOT ADD COCKROACHDB-SPECIFIC KEYWORDS HERE ***
//
// See type_func_name_crdb_extra_keyword below.
type_func_name_no_crdb_extra_keyword:
  AUTHORIZATION
| COLLATION
| CROSS
| FULL
| INNER
| ILIKE
| IS
| ISNULL
| JOIN
| LEFT
| LIKE
| NATURAL
| NONE
| NOTNULL
| OUTER
| OVERLAPS
| RIGHT
| SIMILAR

// CockroachDB-specific keywords that can be used in type/function
// identifiers.
//
// *** REFRAIN FROM ADDING KEYWORDS HERE ***
//
// Adding keywords here creates non-resolvable incompatibilities with
// postgres clients.
//
type_func_name_crdb_extra_keyword:
  FAMILY

// Reserved keyword --- these keywords are usable only as an unrestricted_name.
//
// Keywords appear here if they could not be distinguished from variable, type,
// or function names in some contexts.
//
// *** NEVER ADD KEYWORDS HERE ***
//
// See cockroachdb_extra_reserved_keyword below.
//
// Note: also add the new keyword to `bare_label` list to not break
// user queries using column label without `AS`.
reserved_keyword:
  ALL
| ANALYSE
| ANALYZE
| AND
| ANY
| ARRAY
| AS
| ASC
| ASYMMETRIC
| BOTH
| CASE
| CAST
| CHECK
| COLLATE
| COLUMN
| CONCURRENTLY
| CONSTRAINT
| CREATE
| CURRENT_CATALOG
| CURRENT_DATE
| CURRENT_ROLE
| CURRENT_SCHEMA
| CURRENT_TIME
| CURRENT_TIMESTAMP
| CURRENT_USER
| DEFAULT
| DEFERRABLE
| DESC
| DISTINCT
| DO
| ELSE
| END
| EXCEPT
| FALSE
| FETCH
| FOR
| FOREIGN
| FROM
| GRANT
| GROUP
| HAVING
| IN
| INITIALLY
| INTERSECT
| INTO
| LATERAL
| LEADING
| LIMIT
| LOCALTIME
| LOCALTIMESTAMP
| NOT
| NULL
| OFFSET
| ON
| ONLY
| OR
| ORDER
| PLACING
| PRIMARY
| REFERENCES
| RETURNING
| SELECT
| SESSION_USER
| SOME
| SYMMETRIC
| TABLE
| THEN
| TO
| TRAILING
| TRUE
| UNION
| UNIQUE
| USER
| USING
| VARIADIC
| WHEN
| WHERE
| WINDOW
| WITH
| cockroachdb_extra_reserved_keyword

// Reserved keywords in CockroachDB, in addition to those reserved in
// PostgreSQL.
//
// *** REFRAIN FROM ADDING KEYWORDS HERE ***
//
// Adding keywords here creates non-resolvable incompatibilities with
// postgres clients.
cockroachdb_extra_reserved_keyword:
  INDEX_BEFORE_NAME_THEN_PAREN
| INDEX_BEFORE_PAREN
| INDEX_AFTER_ORDER_BY_BEFORE_AT
| NOTHING_AFTER_RETURNING

%%
