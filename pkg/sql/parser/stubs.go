// Copyright 2017 The Cockroach Authors.
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

package parser

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// AsStringWithFlags is a temporary alias.
var AsStringWithFlags = tree.AsStringWithFlags

// MakeDBool is a temporary alias.
var MakeDBool = tree.MakeDBool

// NewDInt is a temporary alias.
var NewDInt = tree.NewDInt

// NewOrdinalReference is a temporary alias.
var NewOrdinalReference = tree.NewOrdinalReference

// NewPlaceholder is a temporary alias.
var NewPlaceholder = tree.NewPlaceholder

// NewStrVal is a temporary alias.
var NewStrVal = tree.NewStrVal

// ParseDInterval is a temporary alias.
var ParseDInterval = tree.ParseDInterval

// ParseDIntervalWithField is a temporary alias.
var ParseDIntervalWithField = tree.ParseDIntervalWithField

// StarExpr is a temporary alias.
var StarExpr = tree.StarExpr

// AliasClause is a temporary alias.
type AliasClause = tree.AliasClause

// AliasedTableExpr is a temporary alias.
type AliasedTableExpr = tree.AliasedTableExpr

// AlterTable is a temporary alias.
type AlterTable = tree.AlterTable

// AlterTableAddColumn is a temporary alias.
type AlterTableAddColumn = tree.AlterTableAddColumn

// AlterTableAddConstraint is a temporary alias.
type AlterTableAddConstraint = tree.AlterTableAddConstraint

// AlterTableCmd is a temporary alias.
type AlterTableCmd = tree.AlterTableCmd

// AlterTableCmds is a temporary alias.
type AlterTableCmds = tree.AlterTableCmds

// AlterTableDropColumn is a temporary alias.
type AlterTableDropColumn = tree.AlterTableDropColumn

// AlterTableDropConstraint is a temporary alias.
type AlterTableDropConstraint = tree.AlterTableDropConstraint

// AlterTableDropNotNull is a temporary alias.
type AlterTableDropNotNull = tree.AlterTableDropNotNull

// AlterTableSetDefault is a temporary alias.
type AlterTableSetDefault = tree.AlterTableSetDefault

// AlterTableValidateConstraint is a temporary alias.
type AlterTableValidateConstraint = tree.AlterTableValidateConstraint

// AlterUserSetPassword is a temporary alias.
type AlterUserSetPassword = tree.AlterUserSetPassword

// AndExpr is a temporary alias.
type AndExpr = tree.AndExpr

// AnnotateTypeExpr is a temporary alias.
type AnnotateTypeExpr = tree.AnnotateTypeExpr

// Array is a temporary alias.
type Array = tree.Array

// ArrayFlatten is a temporary alias.
type ArrayFlatten = tree.ArrayFlatten

// ArraySubscript is a temporary alias.
type ArraySubscript = tree.ArraySubscript

// ArraySubscripts is a temporary alias.
type ArraySubscripts = tree.ArraySubscripts

// AsOfClause is a temporary alias.
type AsOfClause = tree.AsOfClause

// Backup is a temporary alias.
type Backup = tree.Backup

// BeginTransaction is a temporary alias.
type BeginTransaction = tree.BeginTransaction

// BinaryExpr is a temporary alias.
type BinaryExpr = tree.BinaryExpr

// CancelJob is a temporary alias.
type CancelJob = tree.CancelJob

// CancelQuery is a temporary alias.
type CancelQuery = tree.CancelQuery

// CaseExpr is a temporary alias.
type CaseExpr = tree.CaseExpr

// CastExpr is a temporary alias.
type CastExpr = tree.CastExpr

// CheckConstraintTableDef is a temporary alias.
type CheckConstraintTableDef = tree.CheckConstraintTableDef

// CoalesceExpr is a temporary alias.
type CoalesceExpr = tree.CoalesceExpr

// CollateExpr is a temporary alias.
type CollateExpr = tree.CollateExpr

// ColumnCheckConstraint is a temporary alias.
type ColumnCheckConstraint = tree.ColumnCheckConstraint

// ColumnDefault is a temporary alias.
type ColumnDefault = tree.ColumnDefault

// ColumnFKConstraint is a temporary alias.
type ColumnFKConstraint = tree.ColumnFKConstraint

// ColumnFamilyConstraint is a temporary alias.
type ColumnFamilyConstraint = tree.ColumnFamilyConstraint

// ColumnID is a temporary alias.
type ColumnID = tree.ColumnID

// ColumnQualification is a temporary alias.
type ColumnQualification = tree.ColumnQualification

// ColumnTableDef is a temporary alias.
type ColumnTableDef = tree.ColumnTableDef

// CommitTransaction is a temporary alias.
type CommitTransaction = tree.CommitTransaction

// ComparisonExpr is a temporary alias.
type ComparisonExpr = tree.ComparisonExpr

// ComparisonOperator is a temporary alias.
type ComparisonOperator = tree.ComparisonOperator

// ConstraintTableDef is a temporary alias.
type ConstraintTableDef = tree.ConstraintTableDef

// CopyFrom is a temporary alias.
type CopyFrom = tree.CopyFrom

// CreateDatabase is a temporary alias.
type CreateDatabase = tree.CreateDatabase

// CreateIndex is a temporary alias.
type CreateIndex = tree.CreateIndex

// CreateTable is a temporary alias.
type CreateTable = tree.CreateTable

// CreateUser is a temporary alias.
type CreateUser = tree.CreateUser

// CreateView is a temporary alias.
type CreateView = tree.CreateView

// Deallocate is a temporary alias.
type Deallocate = tree.Deallocate

// DefaultVal is a temporary alias.
type DefaultVal = tree.DefaultVal

// Delete is a temporary alias.
type Delete = tree.Delete

// Direction is a temporary alias.
type Direction = tree.Direction

// Discard is a temporary alias.
type Discard = tree.Discard

// DropBehavior is a temporary alias.
type DropBehavior = tree.DropBehavior

// DropDatabase is a temporary alias.
type DropDatabase = tree.DropDatabase

// DropIndex is a temporary alias.
type DropIndex = tree.DropIndex

// DropTable is a temporary alias.
type DropTable = tree.DropTable

// DropUser is a temporary alias.
type DropUser = tree.DropUser

// DropView is a temporary alias.
type DropView = tree.DropView

// Execute is a temporary alias.
type Execute = tree.Execute

// ExistsExpr is a temporary alias.
type ExistsExpr = tree.ExistsExpr

// Explain is a temporary alias.
type Explain = tree.Explain

// Expr is a temporary alias.
type Expr = tree.Expr

// Exprs is a temporary alias.
type Exprs = tree.Exprs

// FamilyTableDef is a temporary alias.
type FamilyTableDef = tree.FamilyTableDef

// ForeignKeyConstraintTableDef is a temporary alias.
type ForeignKeyConstraintTableDef = tree.ForeignKeyConstraintTableDef

// From is a temporary alias.
type From = tree.From

// FuncExpr is a temporary alias.
type FuncExpr = tree.FuncExpr

// FunctionReference is a temporary alias.
type FunctionReference = tree.FunctionReference

// Grant is a temporary alias.
type Grant = tree.Grant

// GroupBy is a temporary alias.
type GroupBy = tree.GroupBy

// IfExpr is a temporary alias.
type IfExpr = tree.IfExpr

// Import is a temporary alias.
type Import = tree.Import

// IndexElem is a temporary alias.
type IndexElem = tree.IndexElem

// IndexElemList is a temporary alias.
type IndexElemList = tree.IndexElemList

// IndexHints is a temporary alias.
type IndexHints = tree.IndexHints

// IndexTableDef is a temporary alias.
type IndexTableDef = tree.IndexTableDef

// IndirectionExpr is a temporary alias.
type IndirectionExpr = tree.IndirectionExpr

// Insert is a temporary alias.
type Insert = tree.Insert

// InterleaveDef is a temporary alias.
type InterleaveDef = tree.InterleaveDef

// IsOfTypeExpr is a temporary alias.
type IsOfTypeExpr = tree.IsOfTypeExpr

// IsolationLevel is a temporary alias.
type IsolationLevel = tree.IsolationLevel

// JoinCond is a temporary alias.
type JoinCond = tree.JoinCond

// JoinTableExpr is a temporary alias.
type JoinTableExpr = tree.JoinTableExpr

// KVOption is a temporary alias.
type KVOption = tree.KVOption

// Limit is a temporary alias.
type Limit = tree.Limit

// ListPartition is a temporary alias.
type ListPartition = tree.ListPartition

// NameList is a temporary alias.
type NameList = tree.NameList

// NamePart is a temporary alias.
type NamePart = tree.NamePart

// NamedColumnQualification is a temporary alias.
type NamedColumnQualification = tree.NamedColumnQualification

// NaturalJoinCond is a temporary alias.
type NaturalJoinCond = tree.NaturalJoinCond

// NormalizableTableName is a temporary alias.
type NormalizableTableName = tree.NormalizableTableName

// NotExpr is a temporary alias.
type NotExpr = tree.NotExpr

// NotNullConstraint is a temporary alias.
type NotNullConstraint = tree.NotNullConstraint

// NullConstraint is a temporary alias.
type NullConstraint = tree.NullConstraint

// NullIfExpr is a temporary alias.
type NullIfExpr = tree.NullIfExpr

// NumVal is a temporary alias.
type NumVal = tree.NumVal

// OnConflict is a temporary alias.
type OnConflict = tree.OnConflict

// OnJoinCond is a temporary alias.
type OnJoinCond = tree.OnJoinCond

// OrExpr is a temporary alias.
type OrExpr = tree.OrExpr

// Order is a temporary alias.
type Order = tree.Order

// OrderBy is a temporary alias.
type OrderBy = tree.OrderBy

// ParenExpr is a temporary alias.
type ParenExpr = tree.ParenExpr

// ParenSelect is a temporary alias.
type ParenSelect = tree.ParenSelect

// ParenTableExpr is a temporary alias.
type ParenTableExpr = tree.ParenTableExpr

// PartitionBy is a temporary alias.
type PartitionBy = tree.PartitionBy

// MaxVal is a temporary alias.
type MaxVal = tree.MaxVal

// PauseJob is a temporary alias.
type PauseJob = tree.PauseJob

// Prepare is a temporary alias.
type Prepare = tree.Prepare

// PrimaryKeyConstraint is a temporary alias.
type PrimaryKeyConstraint = tree.PrimaryKeyConstraint

// RangeCond is a temporary alias.
type RangeCond = tree.RangeCond

// RangePartition is a temporary alias.
type RangePartition = tree.RangePartition

// ReadWriteMode is a temporary alias.
type ReadWriteMode = tree.ReadWriteMode

// ReferenceAction is a temporary alias.
type ReferenceAction = tree.ReferenceAction

// ReferenceActions is a temporary alias.
type ReferenceActions = tree.ReferenceActions

// ReleaseSavepoint is a temporary alias.
type ReleaseSavepoint = tree.ReleaseSavepoint

// RenameColumn is a temporary alias.
type RenameColumn = tree.RenameColumn

// RenameDatabase is a temporary alias.
type RenameDatabase = tree.RenameDatabase

// RenameIndex is a temporary alias.
type RenameIndex = tree.RenameIndex

// RenameTable is a temporary alias.
type RenameTable = tree.RenameTable

// ResolvableFunctionReference is a temporary alias.
type ResolvableFunctionReference = tree.ResolvableFunctionReference

// Restore is a temporary alias.
type Restore = tree.Restore

// ResumeJob is a temporary alias.
type ResumeJob = tree.ResumeJob

// ReturningClause is a temporary alias.
type ReturningClause = tree.ReturningClause

// Revoke is a temporary alias.
type Revoke = tree.Revoke

// RollbackToSavepoint is a temporary alias.
type RollbackToSavepoint = tree.RollbackToSavepoint

// RollbackTransaction is a temporary alias.
type RollbackTransaction = tree.RollbackTransaction

// Savepoint is a temporary alias.
type Savepoint = tree.Savepoint

// Scatter is a temporary alias.
type Scatter = tree.Scatter

// Scrub is a temporary alias.
type Scrub = tree.Scrub

// ScrubOption is a temporary alias.
type ScrubOption = tree.ScrubOption

// ScrubOptionIndex is a temporary alias.
type ScrubOptionIndex = tree.ScrubOptionIndex

// ScrubOptionPhysical is a temporary alias.
type ScrubOptionPhysical = tree.ScrubOptionPhysical

// ScrubOptions is a temporary alias.
type ScrubOptions = tree.ScrubOptions

// Select is a temporary alias.
type Select = tree.Select

// SelectClause is a temporary alias.
type SelectClause = tree.SelectClause

// SelectExpr is a temporary alias.
type SelectExpr = tree.SelectExpr

// SelectExprs is a temporary alias.
type SelectExprs = tree.SelectExprs

// SelectStatement is a temporary alias.
type SelectStatement = tree.SelectStatement

// SetClusterSetting is a temporary alias.
type SetClusterSetting = tree.SetClusterSetting

// SetDefaultIsolation is a temporary alias.
type SetDefaultIsolation = tree.SetDefaultIsolation

// SetTransaction is a temporary alias.
type SetTransaction = tree.SetTransaction

// SetVar is a temporary alias.
type SetVar = tree.SetVar

// SetZoneConfig is a temporary alias.
type SetZoneConfig = tree.SetZoneConfig

// ShowBackup is a temporary alias.
type ShowBackup = tree.ShowBackup

// ShowClusterSetting is a temporary alias.
type ShowClusterSetting = tree.ShowClusterSetting

// ShowColumns is a temporary alias.
type ShowColumns = tree.ShowColumns

// ShowConstraints is a temporary alias.
type ShowConstraints = tree.ShowConstraints

// ShowCreateTable is a temporary alias.
type ShowCreateTable = tree.ShowCreateTable

// ShowCreateView is a temporary alias.
type ShowCreateView = tree.ShowCreateView

// ShowDatabases is a temporary alias.
type ShowDatabases = tree.ShowDatabases

// ShowFingerprints is a temporary alias.
type ShowFingerprints = tree.ShowFingerprints

// ShowGrants is a temporary alias.
type ShowGrants = tree.ShowGrants

// ShowIndex is a temporary alias.
type ShowIndex = tree.ShowIndex

// ShowJobs is a temporary alias.
type ShowJobs = tree.ShowJobs

// ShowQueries is a temporary alias.
type ShowQueries = tree.ShowQueries

// ShowRanges is a temporary alias.
type ShowRanges = tree.ShowRanges

// ShowSessions is a temporary alias.
type ShowSessions = tree.ShowSessions

// ShowTables is a temporary alias.
type ShowTables = tree.ShowTables

// ShowTrace is a temporary alias.
type ShowTrace = tree.ShowTrace

// ShowTransactionStatus is a temporary alias.
type ShowTransactionStatus = tree.ShowTransactionStatus

// ShowUsers is a temporary alias.
type ShowUsers = tree.ShowUsers

// ShowVar is a temporary alias.
type ShowVar = tree.ShowVar

// ShowZoneConfig is a temporary alias.
type ShowZoneConfig = tree.ShowZoneConfig

// Split is a temporary alias.
type Split = tree.Split

// Statement is a temporary alias.
type Statement = tree.Statement

// StatementSource is a temporary alias.
type StatementSource = tree.StatementSource

// StrVal is a temporary alias.
type StrVal = tree.StrVal

// Subquery is a temporary alias.
type Subquery = tree.Subquery

// TableDef is a temporary alias.
type TableDef = tree.TableDef

// TableDefs is a temporary alias.
type TableDefs = tree.TableDefs

// TableExpr is a temporary alias.
type TableExpr = tree.TableExpr

// TableExprs is a temporary alias.
type TableExprs = tree.TableExprs

// TableNameReferences is a temporary alias.
type TableNameReferences = tree.TableNameReferences

// TableNameWithIndex is a temporary alias.
type TableNameWithIndex = tree.TableNameWithIndex

// TableNameWithIndexList is a temporary alias.
type TableNameWithIndexList = tree.TableNameWithIndexList

// TablePatterns is a temporary alias.
type TablePatterns = tree.TablePatterns

// TableRef is a temporary alias.
type TableRef = tree.TableRef

// TargetList is a temporary alias.
type TargetList = tree.TargetList

// TestingRelocate is a temporary alias.
type TestingRelocate = tree.TestingRelocate

// TransactionModes is a temporary alias.
type TransactionModes = tree.TransactionModes

// Truncate is a temporary alias.
type Truncate = tree.Truncate

// Tuple is a temporary alias.
type Tuple = tree.Tuple

// UnaryExpr is a temporary alias.
type UnaryExpr = tree.UnaryExpr

// UnionClause is a temporary alias.
type UnionClause = tree.UnionClause

// UniqueConstraint is a temporary alias.
type UniqueConstraint = tree.UniqueConstraint

// UniqueConstraintTableDef is a temporary alias.
type UniqueConstraintTableDef = tree.UniqueConstraintTableDef

// UnqualifiedStar is a temporary alias.
type UnqualifiedStar = tree.UnqualifiedStar

// UnresolvedName is a temporary alias.
type UnresolvedName = tree.UnresolvedName

// UnresolvedNames is a temporary alias.
type UnresolvedNames = tree.UnresolvedNames

// Update is a temporary alias.
type Update = tree.Update

// UpdateExpr is a temporary alias.
type UpdateExpr = tree.UpdateExpr

// UpdateExprs is a temporary alias.
type UpdateExprs = tree.UpdateExprs

// UserPriority is a temporary alias.
type UserPriority = tree.UserPriority

// UsingJoinCond is a temporary alias.
type UsingJoinCond = tree.UsingJoinCond

// ValidationBehavior is a temporary alias.
type ValidationBehavior = tree.ValidationBehavior

// ValuesClause is a temporary alias.
type ValuesClause = tree.ValuesClause

// When is a temporary alias.
type When = tree.When

// Window is a temporary alias.
type Window = tree.Window

// WindowDef is a temporary alias.
type WindowDef = tree.WindowDef

// ZoneSpecifier is a temporary alias.
type ZoneSpecifier = tree.ZoneSpecifier

// SearchPath is a temporary alias.
type SearchPath = tree.SearchPath

// Builtin is a temporary alias.
type Builtin = tree.Builtin

// StatementList is a temporary alias.
type StatementList = tree.StatementList

// EvalContext is a temporary alias.
type EvalContext = tree.EvalContext

// Datum is a temporary alias.
type Datum = tree.Datum

// ParseDBool is a temporary alias.
var ParseDBool = tree.ParseDBool

// AbsentReturningClause is a temporary alias.
var AbsentReturningClause = tree.AbsentReturningClause

// All is a temporary alias.
const All = tree.All

// AllExistence is a temporary alias.
const AllExistence = tree.AllExistence

// AllFuncType is a temporary alias.
const AllFuncType = tree.AllFuncType

// Any is a temporary alias.
const Any = tree.Any

// Ascending is a temporary alias.
const Ascending = tree.Ascending

// Bitand is a temporary alias.
const Bitand = tree.Bitand

// Bitor is a temporary alias.
const Bitor = tree.Bitor

// Bitxor is a temporary alias.
const Bitxor = tree.Bitxor

// Cascade is a temporary alias.
const Cascade = tree.Cascade

// ColumnCollation is a temporary alias.
type ColumnCollation = tree.ColumnCollation

// Concat is a temporary alias.
const Concat = tree.Concat

// ContainedBy is a temporary alias.
const ContainedBy = tree.ContainedBy

// Contains is a temporary alias.
const Contains = tree.Contains

// DBytes is a temporary alias.
type DBytes = tree.DBytes

// DNull is a temporary alias.
var DNull = tree.DNull

// DefaultDirection is a temporary alias.
const DefaultDirection = tree.DefaultDirection

// Descending is a temporary alias.
const Descending = tree.Descending

// DiscardModeAll is a temporary alias.
const DiscardModeAll = tree.DiscardModeAll

// DistinctFuncType is a temporary alias.
const DistinctFuncType = tree.DistinctFuncType

// Div is a temporary alias.
const Div = tree.Div

// DropCascade is a temporary alias.
const DropCascade = tree.DropCascade

// DropDefault is a temporary alias.
const DropDefault = tree.DropDefault

// DropRestrict is a temporary alias.
const DropRestrict = tree.DropRestrict

// EQ is a temporary alias.
const EQ = tree.EQ

// ExceptOp is a temporary alias.
const ExceptOp = tree.ExceptOp

// Existence is a temporary alias.
const Existence = tree.Existence

// FetchText is a temporary alias.
const FetchText = tree.FetchText

// FetchTextPath is a temporary alias.
const FetchTextPath = tree.FetchTextPath

// FetchVal is a temporary alias.
const FetchVal = tree.FetchVal

// FetchValPath is a temporary alias.
const FetchValPath = tree.FetchValPath

// FloorDiv is a temporary alias.
const FloorDiv = tree.FloorDiv

// FmtBareIdentifiers is a temporary alias.
var FmtBareIdentifiers = tree.FmtBareIdentifiers

// GE is a temporary alias.
const GE = tree.GE

// GT is a temporary alias.
const GT = tree.GT

// High is a temporary alias.
const High = tree.High

// ILike is a temporary alias.
const ILike = tree.ILike

// In is a temporary alias.
const In = tree.In

// IndexID is a temporary alias.
type IndexID = tree.IndexID

// IntersectOp is a temporary alias.
const IntersectOp = tree.IntersectOp

// Is is a temporary alias.
const Is = tree.Is

// IsDistinctFrom is a temporary alias.
const IsDistinctFrom = tree.IsDistinctFrom

// IsNot is a temporary alias.
const IsNot = tree.IsNot

// IsNotDistinctFrom is a temporary alias.
const IsNotDistinctFrom = tree.IsNotDistinctFrom

// LE is a temporary alias.
const LE = tree.LE

// LShift is a temporary alias.
const LShift = tree.LShift

// LT is a temporary alias.
const LT = tree.LT

// Like is a temporary alias.
const Like = tree.Like

// Low is a temporary alias.
const Low = tree.Low

// Minus is a temporary alias.
const Minus = tree.Minus

// Mod is a temporary alias.
const Mod = tree.Mod

// Mult is a temporary alias.
const Mult = tree.Mult

// NE is a temporary alias.
const NE = tree.NE

// Name is a temporary alias.
type Name = tree.Name

// NewDBytes is a temporary alias.
var NewDBytes = tree.NewDBytes

// NewDString is a temporary alias.
var NewDString = tree.NewDString

// NoAction is a temporary alias.
const NoAction = tree.NoAction

// Normal is a temporary alias.
const Normal = tree.Normal

// NotILike is a temporary alias.
const NotILike = tree.NotILike

// NotIn is a temporary alias.
const NotIn = tree.NotIn

// NotLike is a temporary alias.
const NotLike = tree.NotLike

// NotRegIMatch is a temporary alias.
const NotRegIMatch = tree.NotRegIMatch

// NotRegMatch is a temporary alias.
const NotRegMatch = tree.NotRegMatch

// NotSimilarTo is a temporary alias.
const NotSimilarTo = tree.NotSimilarTo

// OrderByColumn is a temporary alias.
const OrderByColumn = tree.OrderByColumn

// OrderByIndex is a temporary alias.
const OrderByIndex = tree.OrderByIndex

// ParseDArrayFromString is a temporary alias.
var ParseDArrayFromString = tree.ParseDArrayFromString

// ParseDDate is a temporary alias.
var ParseDDate = tree.ParseDDate

// ParseDDecimal is a temporary alias.
var ParseDDecimal = tree.ParseDDecimal

// ParseDFloat is a temporary alias.
var ParseDFloat = tree.ParseDFloat

// ParseDIPAddrFromINetString is a temporary alias.
var ParseDIPAddrFromINetString = tree.ParseDIPAddrFromINetString

// ParseDInt is a temporary alias.
var ParseDInt = tree.ParseDInt

// ParseDTimestamp is a temporary alias.
var ParseDTimestamp = tree.ParseDTimestamp

// ParseDTimestampTZ is a temporary alias.
var ParseDTimestampTZ = tree.ParseDTimestampTZ

// ParseDUuidFromString is a temporary alias.
var ParseDUuidFromString = tree.ParseDUuidFromString

// Plus is a temporary alias.
const Plus = tree.Plus

// Pow is a temporary alias.
const Pow = tree.Pow

// RShift is a temporary alias.
const RShift = tree.RShift

// ReadOnly is a temporary alias.
const ReadOnly = tree.ReadOnly

// ReadWrite is a temporary alias.
const ReadWrite = tree.ReadWrite

// RegIMatch is a temporary alias.
const RegIMatch = tree.RegIMatch

// RegMatch is a temporary alias.
const RegMatch = tree.RegMatch

// Restrict is a temporary alias.
const Restrict = tree.Restrict

// ReturningExprs is a temporary alias.
type ReturningExprs = tree.ReturningExprs

// ScrubDatabase is a temporary alias.
const ScrubDatabase = tree.ScrubDatabase

// ScrubTable is a temporary alias.
const ScrubTable = tree.ScrubTable

// SerializableIsolation is a temporary alias.
const SerializableIsolation = tree.SerializableIsolation

// SetDefault is a temporary alias.
const SetDefault = tree.SetDefault

// SetNull is a temporary alias.
const SetNull = tree.SetNull

// SimilarTo is a temporary alias.
const SimilarTo = tree.SimilarTo

// SnapshotIsolation is a temporary alias.
const SnapshotIsolation = tree.SnapshotIsolation

// Some is a temporary alias.
const Some = tree.Some

// SomeExistence is a temporary alias.
const SomeExistence = tree.SomeExistence

// TableNameReference is a temporary alias.
type TableNameReference = tree.TableNameReference

// UnaryComplement is a temporary alias.
const UnaryComplement = tree.UnaryComplement

// UnaryMinus is a temporary alias.
const UnaryMinus = tree.UnaryMinus

// UnaryPlus is a temporary alias.
const UnaryPlus = tree.UnaryPlus

// UnionOp is a temporary alias.
const UnionOp = tree.UnionOp

// UnrestrictedName is a temporary alias.
type UnrestrictedName = tree.UnrestrictedName

// ValidationDefault is a temporary alias.
const ValidationDefault = tree.ValidationDefault

// ValidationSkip is a temporary alias.
const ValidationSkip = tree.ValidationSkip

// NewBytesStrVal is a temporary alias.
var NewBytesStrVal = tree.NewBytesStrVal

// TypedExpr is a temporary alias.
type TypedExpr = tree.TypedExpr

// TableName is a temporary alias.
type TableName = tree.TableName

// PlaceholderTypes is a temporary alias
type PlaceholderTypes = tree.PlaceholderTypes

// DInt is a temporary alias
type DInt = tree.DInt

// BinOp is a temporary alias.
type BinOp = tree.BinOp

// CmpOp is a temporary alias.
type CmpOp = tree.CmpOp

// DDecimal is a temporary alias.
type DDecimal = tree.DDecimal

// DFloat is a temporary alias.
type DFloat = tree.DFloat

// MakeDJSON is a temporary alias.
var MakeDJSON = tree.MakeDJSON

// MakeSemaContext is a temporary alias.
var MakeSemaContext = tree.MakeSemaContext

// MockNameTypes is a temporary alias.
var MockNameTypes = tree.MockNameTypes

// NewDFloat is a temporary alias.
var NewDFloat = tree.NewDFloat

// NewTestingEvalContext is a temporary alias.
var NewTestingEvalContext = tree.NewTestingEvalContext

// ParseDJSON is a temporary alias.
var ParseDJSON = tree.ParseDJSON

// Placeholder is a temporary alias.
type Placeholder = tree.Placeholder

// SemaContext is a temporary alias.
type SemaContext = tree.SemaContext

// Serialize is a temporary alias.
var Serialize = tree.Serialize

// StripParens is a temporary alias.
var StripParens = tree.StripParens

// TypeCheck is a temporary alias.
var TypeCheck = tree.TypeCheck

// UnaryOp is a temporary alias.
type UnaryOp = tree.UnaryOp

// VarName is a temporary alias.
type VarName = tree.VarName

// WalkExpr is a temporary alias.
var WalkExpr = tree.WalkExpr

// Datums is a temporary alias.
type Datums = tree.Datums

// AggregateFunc is a temporary alias.
type AggregateFunc = tree.AggregateFunc

// ReturnTyper is a temporary alias.
type ReturnTyper = tree.ReturnTyper

// DArray is a temporary alias.
type DArray = tree.DArray

// AddWithOverflow is a temporary alias.
var AddWithOverflow = tree.AddWithOverflow

// AggregateClass is a temporary alias.
const AggregateClass = tree.AggregateClass

// AppendToMaybeNullArray is a temporary alias.
var AppendToMaybeNullArray = tree.AppendToMaybeNullArray

// ArgTypes is a temporary alias.
type ArgTypes = tree.ArgTypes

// AsDArray is a temporary alias.
var AsDArray = tree.AsDArray

// ConcatArrays is a temporary alias.
var ConcatArrays = tree.ConcatArrays

// DBool is a temporary alias.
type DBool = tree.DBool

// DBoolTrue is a temporary alias.
var DBoolTrue = tree.DBoolTrue

// DDate is a temporary alias.
type DDate = tree.DDate

// DIPAddr is a temporary alias.
type DIPAddr = tree.DIPAddr

// DInterval is a temporary alias.
type DInterval = tree.DInterval

// DOid is a temporary alias.
type DOid = tree.DOid

// DString is a temporary alias.
type DString = tree.DString

// DTable is a temporary alias.
type DTable = tree.DTable

// DTimestamp is a temporary alias.
type DTimestamp = tree.DTimestamp

// DTimestampTZ is a temporary alias.
type DTimestampTZ = tree.DTimestampTZ

// DUuid is a temporary alias.
type DUuid = tree.DUuid

// DZero is a temporary alias.
var DZero = tree.DZero

// DecimalCtx is a temporary alias.
var DecimalCtx = tree.DecimalCtx

// ErrDivByZero is a temporary alias.
var ErrDivByZero = tree.ErrDivByZero

// ErrZeroModulus is a temporary alias.
var ErrZeroModulus = tree.ErrZeroModulus

// ExactCtx is a temporary alias.
var ExactCtx = tree.ExactCtx

// FixedReturnType is a temporary alias.
var FixedReturnType = tree.FixedReturnType

// FunctionDefinition is a temporary alias.
type FunctionDefinition = tree.FunctionDefinition

// GeneratorClass is a temporary alias.
const GeneratorClass = tree.GeneratorClass

// HighPrecisionCtx is a temporary alias.
var HighPrecisionCtx = tree.HighPrecisionCtx

// HomogeneousType is a temporary alias.
type HomogeneousType = tree.HomogeneousType

// IdentityReturnType is a temporary alias.
var IdentityReturnType = tree.IdentityReturnType

// IntPow is a temporary alias.
var IntPow = tree.IntPow

// IntermediateCtx is a temporary alias.
var IntermediateCtx = tree.IntermediateCtx

// MakeDTimestamp is a temporary alias.
var MakeDTimestamp = tree.MakeDTimestamp

// MakeDTimestampTZ is a temporary alias.
var MakeDTimestampTZ = tree.MakeDTimestampTZ

// MakeDTimestampTZFromDate is a temporary alias.
var MakeDTimestampTZFromDate = tree.MakeDTimestampTZFromDate

// MustBeDArray is a temporary alias.
var MustBeDArray = tree.MustBeDArray

// MustBeDIPAddr is a temporary alias.
var MustBeDIPAddr = tree.MustBeDIPAddr

// MustBeDInt is a temporary alias.
var MustBeDInt = tree.MustBeDInt

// MustBeDString is a temporary alias.
var MustBeDString = tree.MustBeDString

// NewDArray is a temporary alias.
var NewDArray = tree.NewDArray

// NewDDateFromTime is a temporary alias.
var NewDDateFromTime = tree.NewDDateFromTime

// NewDOid is a temporary alias.
var NewDOid = tree.NewDOid

// NewDUuid is a temporary alias.
var NewDUuid = tree.NewDUuid

// NewFunctionDefinition is a temporary alias.
var NewFunctionDefinition = tree.NewFunctionDefinition

// PickFromTuple is a temporary alias.
var PickFromTuple = tree.PickFromTuple

// PrependToMaybeNullArray is a temporary alias.
var PrependToMaybeNullArray = tree.PrependToMaybeNullArray

// RoundCtx is a temporary alias.
var RoundCtx = tree.RoundCtx

// SecondsInDay is a temporary alias.
const SecondsInDay = tree.SecondsInDay

// TimestampDifference is a temporary alias.
var TimestampDifference = tree.TimestampDifference

// TypeList is a temporary alias.
type TypeList = tree.TypeList

// UnknownReturnType is a temporary alias.
var UnknownReturnType = tree.UnknownReturnType

// ValueGenerator is a temporary alias.
type ValueGenerator = tree.ValueGenerator

// VariadicType is a temporary alias.
type VariadicType = tree.VariadicType

// WindowClass is a temporary alias.
const WindowClass = tree.WindowClass

// WindowFrame is a temporary alias.
type WindowFrame = tree.WindowFrame

// WindowFunc is a temporary alias.
type WindowFunc = tree.WindowFunc

// NormalizeVisitor is a temporary alias.
type NormalizeVisitor = tree.NormalizeVisitor

// Visitor is a temporary alias.
type Visitor = tree.Visitor

// MakeNormalizeVisitor is a temporary alias.
var MakeNormalizeVisitor = tree.MakeNormalizeVisitor

// WalkExprConst is a temporary alias.
var WalkExprConst = tree.WalkExprConst

// ID is a temporary alias.
type ID = tree.ID

// CollationEnvironment is a temporary alias.
type CollationEnvironment = tree.CollationEnvironment

// AsDInt is a temporary alias.
var AsDInt = tree.AsDInt

// AsDString is a temporary alias.
var AsDString = tree.AsDString

// AsString is a temporary alias.
var AsString = tree.AsString

// ColumnItem is a temporary alias.
type ColumnItem = tree.ColumnItem

// CompositeDatum is a temporary alias.
type CompositeDatum = tree.CompositeDatum

// ContainsVars is a temporary alias.
var ContainsVars = tree.ContainsVars

// DCollatedString is a temporary alias.
type DCollatedString = tree.DCollatedString

// DTuple is a temporary alias.
type DTuple = tree.DTuple

// DatumTypeSize is a temporary alias.
var DatumTypeSize = tree.DatumTypeSize

// ErrString is a temporary alias.
var ErrString = tree.ErrString

// LimitDecimalWidth is a temporary alias.
var LimitDecimalWidth = tree.LimitDecimalWidth

// MakeDOid is a temporary alias.
var MakeDOid = tree.MakeDOid

// NewDCollatedString is a temporary alias.
var NewDCollatedString = tree.NewDCollatedString

// NewDDate is a temporary alias.
var NewDDate = tree.NewDDate

// NewDIPAddr is a temporary alias.
var NewDIPAddr = tree.NewDIPAddr

// NewDName is a temporary alias.
var NewDName = tree.NewDName

// NewDNameFromDString is a temporary alias.
var NewDNameFromDString = tree.NewDNameFromDString

// NodeFormatter is a temporary alias.
type NodeFormatter = tree.NodeFormatter

// NotNull is a temporary alias.
var NotNull = tree.NotNull

// PlaceholderInfo is a temporary alias.
type PlaceholderInfo = tree.PlaceholderInfo

// UnwrapDatum is a temporary alias.
var UnwrapDatum = tree.UnwrapDatum

// IndexedVarHelper is a temporary alias.
type IndexedVarHelper = tree.IndexedVarHelper

// FmtFlags is a temporary alias.
type FmtFlags = tree.FmtFlags

// RegexpCache is a temporary alias.
type RegexpCache = tree.RegexpCache

// IndexedVar is a temporary alias.
type IndexedVar = tree.IndexedVar

// IndexedVarContainer is a temporary alias.
type IndexedVarContainer = tree.IndexedVarContainer

// MakeIndexedVarHelper is a temporary alias.
var MakeIndexedVarHelper = tree.MakeIndexedVarHelper

// NewRegexpCache is a temporary alias.
var NewRegexpCache = tree.NewRegexpCache

// MakeSearchPath is a temporary alias.
var MakeSearchPath = tree.MakeSearchPath

// ExprDebugString is a temporary alias.
var ExprDebugString = tree.ExprDebugString

// FmtIndexedVarFormat is a temporary alias.
var FmtIndexedVarFormat = tree.FmtIndexedVarFormat

// FmtParsable is a temporary alias.
var FmtParsable = tree.FmtParsable

// FmtPlaceholderFormat is a temporary alias.
var FmtPlaceholderFormat = tree.FmtPlaceholderFormat

// FormatNode is a temporary alias.
var FormatNode = tree.FormatNode

// QueryArguments is a temporary alias.
type QueryArguments = tree.QueryArguments

// StatementType is a temporary alias.
type StatementType = tree.StatementType

// ColumnMutationCmd is a temporary alias.
type ColumnMutationCmd = tree.ColumnMutationCmd

// IndexedRow is a temporary alias.
type IndexedRow = tree.IndexedRow

// InvalidColIdx is a temporary alias.
const InvalidColIdx = tree.InvalidColIdx

// KVOptions is a temporary alias.
type KVOptions = tree.KVOptions

// PartitionByType is a temporary alias.
type PartitionByType = tree.PartitionByType

// PgCatalogName is a temporary alias.
const PgCatalogName = tree.PgCatalogName

// TableNames is a temporary alias.
type TableNames = tree.TableNames

// TablePattern is a temporary alias.
type TablePattern = tree.TablePattern

// TypedExprs is a temporary alias.
type TypedExprs = tree.TypedExprs

// VariableExpr is a temporary alias.
type VariableExpr = tree.VariableExpr

// Where is a temporary alias.
type Where = tree.Where

// AllColumnsSelector is a temporary alias.
type AllColumnsSelector = tree.AllColumnsSelector

// AllTablesSelector is a temporary alias.
type AllTablesSelector = tree.AllTablesSelector

// DBoolFalse is a temporary alias.
var DBoolFalse = tree.DBoolFalse

// DDL is a temporary alias.
const DDL = tree.DDL

// FindEqualComparisonFunction is a temporary alias.
var FindEqualComparisonFunction = tree.FindEqualComparisonFunction

// FmtAnonymize is a temporary alias.
var FmtAnonymize = tree.FmtAnonymize

// FmtBareStrings is a temporary alias.
var FmtBareStrings = tree.FmtBareStrings

// FmtCheckEquivalence is a temporary alias.
var FmtCheckEquivalence = tree.FmtCheckEquivalence

// FmtExpr is a temporary alias.
var FmtExpr = tree.FmtExpr

// FmtHideConstants is a temporary alias.
var FmtHideConstants = tree.FmtHideConstants

// FmtReformatTableNames is a temporary alias.
var FmtReformatTableNames = tree.FmtReformatTableNames

// FmtSimple is a temporary alias.
var FmtSimple = tree.FmtSimple

// FmtSimpleQualified is a temporary alias.
var FmtSimpleQualified = tree.FmtSimpleQualified

// GetBool is a temporary alias.
var GetBool = tree.GetBool

// HiddenFromShowQueries is a temporary alias.
type HiddenFromShowQueries = tree.HiddenFromShowQueries

// HiddenFromStats is a temporary alias.
type HiddenFromStats = tree.HiddenFromStats

// IndependentFromParallelizedPriors is a temporary alias.
type IndependentFromParallelizedPriors = tree.IndependentFromParallelizedPriors

// MultipleResultsError is a temporary alias.
type MultipleResultsError = tree.MultipleResultsError

// NewDIntVectorFromDArray is a temporary alias.
var NewDIntVectorFromDArray = tree.NewDIntVectorFromDArray

// NewDTuple is a temporary alias.
var NewDTuple = tree.NewDTuple

// NewDTupleWithLen is a temporary alias.
var NewDTupleWithLen = tree.NewDTupleWithLen

// NewIndexedVar is a temporary alias.
var NewIndexedVar = tree.NewIndexedVar

// NewInvalidNameErrorf is a temporary alias.
var NewInvalidNameErrorf = tree.NewInvalidNameErrorf

// NewTypedAndExpr is a temporary alias.
var NewTypedAndExpr = tree.NewTypedAndExpr

// NewTypedComparisonExpr is a temporary alias.
var NewTypedComparisonExpr = tree.NewTypedComparisonExpr

// NewTypedNotExpr is a temporary alias.
var NewTypedNotExpr = tree.NewTypedNotExpr

// NewTypedOrExpr is a temporary alias.
var NewTypedOrExpr = tree.NewTypedOrExpr

// NoReturningClause is a temporary alias.
type NoReturningClause = tree.NoReturningClause

// PartitionByList is a temporary alias.
const PartitionByList = tree.PartitionByList

// PartitionByRange is a temporary alias.
const PartitionByRange = tree.PartitionByRange

// RestartSavepointName is a temporary alias.
const RestartSavepointName = tree.RestartSavepointName

// ReturningNothing is a temporary alias.
type ReturningNothing = tree.ReturningNothing

// Rows is a temporary alias.
const Rows = tree.Rows

// RowsAffected is a temporary alias.
const RowsAffected = tree.RowsAffected

// SilentNull is a temporary alias.
const SilentNull = tree.SilentNull

// SimilarEscape is a temporary alias.
var SimilarEscape = tree.SimilarEscape

// SimpleVisit is a temporary alias.
var SimpleVisit = tree.SimpleVisit

// StripTypeFormatting is a temporary alias.
var StripTypeFormatting = tree.StripTypeFormatting

// TimestampToDecimal is a temporary alias.
var TimestampToDecimal = tree.TimestampToDecimal

// TypeCheckAndRequire is a temporary alias.
var TypeCheckAndRequire = tree.TypeCheckAndRequire

// UnspecifiedIsolation is a temporary alias.
const UnspecifiedIsolation = tree.UnspecifiedIsolation

// UnspecifiedReadWriteMode is a temporary alias.
const UnspecifiedReadWriteMode = tree.UnspecifiedReadWriteMode

// UnspecifiedUserPriority is a temporary alias.
const UnspecifiedUserPriority = tree.UnspecifiedUserPriority

// ValidateRestartCheckpoint is a temporary alias.
var ValidateRestartCheckpoint = tree.ValidateRestartCheckpoint

// DJSON is a temporary alias.
type DJSON = tree.DJSON

// FmtArrays is a temporary alias.
var FmtArrays = tree.FmtArrays

// ParseDUuidFromBytes is a temporary alias.
var ParseDUuidFromBytes = tree.ParseDUuidFromBytes

// Ack is a temporary alias.
const Ack = tree.Ack

// CopyIn is a temporary alias.
const CopyIn = tree.CopyIn

// MakePlaceholderInfo is a temporary alias.
var MakePlaceholderInfo = tree.MakePlaceholderInfo

// HasReturningClause is a temporary alias.
var HasReturningClause = tree.HasReturningClause

// Constant is a temporary alias.
type Constant = tree.Constant

// StringToColType is a temporary alias.
var StringToColType = tree.StringToColType

// TimestampOutputFormat is a temporary alias.
const TimestampOutputFormat = tree.TimestampOutputFormat

// UnaryOps is a temporary alias
var UnaryOps = tree.UnaryOps

// BinOps is a temporary alias.
var BinOps = tree.BinOps

// CmpOps is a temporary alias.
var CmpOps = tree.CmpOps

// MakeTestingEvalContext is a temporary alias.
var MakeTestingEvalContext = tree.MakeTestingEvalContext

// BinaryOperator is a temporary alias.
type BinaryOperator = tree.BinaryOperator

// FmtAlwaysGroupExprs is a temporary alias.
var FmtAlwaysGroupExprs = tree.FmtAlwaysGroupExprs

// FmtShowTypes is a temporary alias.
var FmtShowTypes = tree.FmtShowTypes

// FmtSimpleWithPasswords is a temporary alias.
var FmtSimpleWithPasswords = tree.FmtSimpleWithPasswords

// UnaryOperator is a temporary alias.
type UnaryOperator = tree.UnaryOperator
