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

package tests

import "github.com/cockroachdb/cockroach/pkg/sql/parser"

// AddWithOverflow is a temporary alias.
var AddWithOverflow = parser.AddWithOverflow

// AggregateClass is a temporary alias.
var AggregateClass = parser.AggregateClass

// AggregateFunc is a temporary alias.
type AggregateFunc = parser.AggregateFunc

// AndExpr is a temporary alias.
type AndExpr = parser.AndExpr

// AppendToMaybeNullArray is a temporary alias.
var AppendToMaybeNullArray = parser.AppendToMaybeNullArray

// ArgTypes is a temporary alias.
type ArgTypes = parser.ArgTypes

// AsDArray is a temporary alias.
var AsDArray = parser.AsDArray

// BinaryExpr is a temporary alias.
type BinaryExpr = parser.BinaryExpr

// BinaryOperator is a temporary alias.
type BinaryOperator = parser.BinaryOperator

// Builtin is a temporary alias.
type Builtin = parser.Builtin

// ComparisonOperator is a temporary alias.
type ComparisonOperator = parser.ComparisonOperator

// ComparisonExpr is a temporary alias.
type ComparisonExpr = parser.ComparisonExpr

// ConcatArrays is a temporary alias.
var ConcatArrays = parser.ConcatArrays

// DArray is a temporary alias.
type DArray = parser.DArray

// DBool is a temporary alias.
type DBool = parser.DBool

// DBoolTrue is a temporary alias.
var DBoolTrue = parser.DBoolTrue

// DBytes is a temporary alias.
type DBytes = parser.DBytes

// DDate is a temporary alias.
type DDate = parser.DDate

// DDecimal is a temporary alias.
type DDecimal = parser.DDecimal

// DFloat is a temporary alias.
type DFloat = parser.DFloat

// DIPAddr is a temporary alias.
type DIPAddr = parser.DIPAddr

// DInt is a temporary alias.
type DInt = parser.DInt

// DInterval is a temporary alias.
type DInterval = parser.DInterval

// DNull is a temporary alias.
var DNull = parser.DNull

// DOid is a temporary alias.
type DOid = parser.DOid

// DString is a temporary alias.
type DString = parser.DString

// DTable is a temporary alias.
type DTable = parser.DTable

// DTimestamp is a temporary alias.
type DTimestamp = parser.DTimestamp

// DTimestampTZ is a temporary alias.
type DTimestampTZ = parser.DTimestampTZ

// DUuid is a temporary alias.
type DUuid = parser.DUuid

// DZero is a temporary alias.
var DZero = parser.DZero

// Datum is a temporary alias.
type Datum = parser.Datum

// Datums is a temporary alias.
type Datums = parser.Datums

// DecimalCtx is a temporary alias.
var DecimalCtx = parser.DecimalCtx

// ErrDivByZero is a temporary alias.
var ErrDivByZero = parser.ErrDivByZero

// ErrZeroModulus is a temporary alias.
var ErrZeroModulus = parser.ErrZeroModulus

// EvalContext is a temporary alias.
type EvalContext = parser.EvalContext

// ExactCtx is a temporary alias.
var ExactCtx = parser.ExactCtx

// Expr is a temporary alias.
type Expr = parser.Expr

// FixedReturnType is a temporary alias.
var FixedReturnType = parser.FixedReturnType

// FunctionDefinition is a temporary alias.
type FunctionDefinition = parser.FunctionDefinition

// GeneratorClass is a temporary alias.
var GeneratorClass = parser.GeneratorClass

// HighPrecisionCtx is a temporary alias.
var HighPrecisionCtx = parser.HighPrecisionCtx

// HomogeneousType is a temporary alias.
type HomogeneousType = parser.HomogeneousType

// IdentityReturnType is a temporary alias.
var IdentityReturnType = parser.IdentityReturnType

// IntPow is a temporary alias.
var IntPow = parser.IntPow

// IntermediateCtx is a temporary alias.
var IntermediateCtx = parser.IntermediateCtx

// MakeDBool is a temporary alias.
var MakeDBool = parser.MakeDBool

// MakeDTimestamp is a temporary alias.
var MakeDTimestamp = parser.MakeDTimestamp

// MakeDTimestampTZ is a temporary alias.
var MakeDTimestampTZ = parser.MakeDTimestampTZ

// MakeDTimestampTZFromDate is a temporary alias.
var MakeDTimestampTZFromDate = parser.MakeDTimestampTZFromDate

// MustBeDArray is a temporary alias.
var MustBeDArray = parser.MustBeDArray

// MustBeDIPAddr is a temporary alias.
var MustBeDIPAddr = parser.MustBeDIPAddr

// MustBeDInt is a temporary alias.
var MustBeDInt = parser.MustBeDInt

// MustBeDString is a temporary alias.
var MustBeDString = parser.MustBeDString

// NewDArray is a temporary alias.
var NewDArray = parser.NewDArray

// NewDBytes is a temporary alias.
var NewDBytes = parser.NewDBytes

// NewDDateFromTime is a temporary alias.
var NewDDateFromTime = parser.NewDDateFromTime

// NewDFloat is a temporary alias.
var NewDFloat = parser.NewDFloat

// NewDInt is a temporary alias.
var NewDInt = parser.NewDInt

// NewDOid is a temporary alias.
var NewDOid = parser.NewDOid

// NewDString is a temporary alias.
var NewDString = parser.NewDString

// NewDUuid is a temporary alias.
var NewDUuid = parser.NewDUuid

// NewFunctionDefinition is a temporary alias.
var NewFunctionDefinition = parser.NewFunctionDefinition

// NewTestingEvalContext is a temporary alias.
var NewTestingEvalContext = parser.NewTestingEvalContext

// NumVal is a temporary alias.
type NumVal = parser.NumVal

// NotExpr is a temporary alias.
type NotExpr = parser.NotExpr

// OrExpr is a temporary alias.
type OrExpr = parser.OrExpr

// PickFromTuple is a temporary alias.
var PickFromTuple = parser.PickFromTuple

// PrependToMaybeNullArray is a temporary alias.
var PrependToMaybeNullArray = parser.PrependToMaybeNullArray

// ReturnTyper is a temporary alias.
type ReturnTyper = parser.ReturnTyper

// RoundCtx is a temporary alias.
var RoundCtx = parser.RoundCtx

// SecondsInDay is a temporary alias.
const SecondsInDay = parser.SecondsInDay

// StrVal is a temporary alias.
type StrVal = parser.StrVal

// TimestampDifference is a temporary alias.
var TimestampDifference = parser.TimestampDifference

// TypeList is a temporary alias.
type TypeList = parser.TypeList

// TypedExpr is a temporary alias.
type TypedExpr = parser.TypedExpr

// UnaryExpr is a temporary alias.
type UnaryExpr = parser.UnaryExpr

// UnaryOperator is a temporary alias.
type UnaryOperator = parser.UnaryOperator

// UnknownReturnType is a temporary alias.
var UnknownReturnType = parser.UnknownReturnType

// ValueGenerator is a temporary alias.
type ValueGenerator = parser.ValueGenerator

// VariadicType is a temporary alias.
type VariadicType = parser.VariadicType

// WindowClass is a temporary alias.
var WindowClass = parser.WindowClass

// WindowFrame is a temporary alias.
type WindowFrame = parser.WindowFrame

// WindowFunc is a temporary alias.
type WindowFunc = parser.WindowFunc

// EQ is a temporary alias.
const EQ = parser.EQ

// LT is a temporary alias.
const LT = parser.LT

// GT is a temporary alias.
const GT = parser.GT

// LE is a temporary alias.
const LE = parser.LE

// GE is a temporary alias.
const GE = parser.GE

// NE is a temporary alias.
const NE = parser.NE

// In is a temporary alias.
const In = parser.In

// NotIn is a temporary alias.
const NotIn = parser.NotIn

// Like is a temporary alias.
const Like = parser.Like

// NotLike is a temporary alias.
const NotLike = parser.NotLike

// ILike is a temporary alias.
const ILike = parser.ILike

// NotILike is a temporary alias.
const NotILike = parser.NotILike

// SimilarTo is a temporary alias.
const SimilarTo = parser.SimilarTo

// NotSimilarTo is a temporary alias.
const NotSimilarTo = parser.NotSimilarTo

// RegMatch is a temporary alias.
const RegMatch = parser.RegMatch

// NotRegMatch is a temporary alias.
const NotRegMatch = parser.NotRegMatch

// RegIMatch is a temporary alias.
const RegIMatch = parser.RegIMatch

// NotRegIMatch is a temporary alias.
const NotRegIMatch = parser.NotRegIMatch

// IsDistinctFrom is a temporary alias.
const IsDistinctFrom = parser.IsDistinctFrom

// IsNotDistinctFrom is a temporary alias.
const IsNotDistinctFrom = parser.IsNotDistinctFrom

// Is is a temporary alias.
const Is = parser.Is

// IsNot is a temporary alias.
const IsNot = parser.IsNot

// Contains is a temporary alias.
const Contains = parser.Contains

// ContainedBy is a temporary alias.
const ContainedBy = parser.ContainedBy

// Existence is a temporary alias.
const Existence = parser.Existence

// SomeExistence is a temporary alias.
const SomeExistence = parser.SomeExistence

// AllExistence is a temporary alias.
const AllExistence = parser.AllExistence

// Bitand is a temporary alias.
const Bitand = parser.Bitand

// Bitor is a temporary alias.
const Bitor = parser.Bitor

// Bitxor is a temporary alias.
const Bitxor = parser.Bitxor

// Plus is a temporary alias.
const Plus = parser.Plus

// Minus is a temporary alias.
const Minus = parser.Minus

// Mult is a temporary alias.
const Mult = parser.Mult

// Div is a temporary alias.
const Div = parser.Div

// FloorDiv is a temporary alias.
const FloorDiv = parser.FloorDiv

// Mod is a temporary alias.
const Mod = parser.Mod

// Pow is a temporary alias.
const Pow = parser.Pow

// Concat is a temporary alias.
const Concat = parser.Concat

// LShift is a temporary alias.
const LShift = parser.LShift

// RShift is a temporary alias.
const RShift = parser.RShift

// FetchVal is a temporary alias.
const FetchVal = parser.FetchVal

// FetchText is a temporary alias.
const FetchText = parser.FetchText

// FetchValPath is a temporary alias.
const FetchValPath = parser.FetchValPath

// FetchTextPath is a temporary alias.
const FetchTextPath = parser.FetchTextPath

// RemovePath is a temporary alias.
const RemovePath = parser.RemovePath

// UnaryPlus is a temporary alias.
const UnaryPlus = parser.UnaryPlus

// UnaryMinus is a temporary alias.
const UnaryMinus = parser.UnaryMinus

// UnaryComplement is a temporary alias.
const UnaryComplement = parser.UnaryComplement
