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

// AsStringWithFlags is a temporary alias.
var AsStringWithFlags = parser.AsStringWithFlags

// FmtAnonymize is a temporary alias.
var FmtAnonymize = parser.FmtAnonymize

// FmtFlags is a temporary alias.
type FmtFlags = parser.FmtFlags

// FmtSimpleWithPasswords is a temporary alias.
var FmtSimpleWithPasswords = parser.FmtSimpleWithPasswords

// FmtShowTypes is a temporary alias.
var FmtShowTypes = parser.FmtShowTypes

// FmtBareStrings is a temporary alias.
var FmtBareStrings = parser.FmtBareStrings

// FmtArrays is a temporary alias.
var FmtArrays = parser.FmtArrays

// FmtBareIdentifiers is a temporary alias.
var FmtBareIdentifiers = parser.FmtBareIdentifiers

// FmtParsable is a temporary alias.
var FmtParsable = parser.FmtParsable

// FmtCheckEquivalence is a temporary alias.
var FmtCheckEquivalence = parser.FmtCheckEquivalence

// FmtHideConstants is a temporary alias.
var FmtHideConstants = parser.FmtHideConstants

// FmtSimple is a temporary alias.
var FmtSimple = parser.FmtSimpleQualified

// FmtSimpleQualified is a temporary alias.
var FmtSimpleQualified = parser.FmtSimpleQualified

// FmtAlwaysGroupExprs is a temporary alias.
var FmtAlwaysGroupExprs = parser.FmtAlwaysGroupExprs

// FmtReformatTableNames is a temporary alias.
var FmtReformatTableNames = parser.FmtReformatTableNames

// MakeSemaContext is a temporary alias.
var MakeSemaContext = parser.MakeSemaContext

// NormalizableTableName is a temporary alias.
type NormalizableTableName = parser.NormalizableTableName

// ParseExpr is a temporary alias.
var ParseExpr = parser.ParseExpr

// ParseOne is a temporary alias.
var ParseOne = parser.ParseOne

// TypeCheck is a temporary alias.
var TypeCheck = parser.TypeCheck

// MakeSearchPath is a temporary alias.
var MakeSearchPath = parser.MakeSearchPath

// Select is a temporary alias.
type Select = parser.Select

// SelectClause is a temporary alias.
type SelectClause = parser.SelectClause

// FuncExpr is a temporary alias.
type FuncExpr = parser.FuncExpr

// NewTestingEvalContext is a temporary alias.
var NewTestingEvalContext = parser.NewTestingEvalContext

// Parse is a temporary alias.
var Parse = parser.Parse

// UnaryExpr is a temporary alias.
type UnaryExpr = parser.UnaryExpr

// UnaryOperator is a temporary alias.
type UnaryOperator = parser.UnaryOperator

// BinaryExpr is a temporary alias.
type BinaryExpr = parser.BinaryExpr

// BinaryOperator is a temporary alias.
type BinaryOperator = parser.BinaryOperator

// Expr is a temporary alias.
type Expr = parser.Expr

// ComparisonOperator is a temporary alias.
type ComparisonOperator = parser.ComparisonOperator

// ComparisonExpr is a temporary alias.
type ComparisonExpr = parser.ComparisonExpr

// AndExpr is a temporary alias.
type AndExpr = parser.AndExpr

// OrExpr is a temporary alias.
type OrExpr = parser.OrExpr

// Concat is a temporary alias.
const Concat = parser.Concat

// NumVal is a temporary alias.
type NumVal = parser.NumVal

// NotExpr is a temporary alias.
type NotExpr = parser.NotExpr

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

// StrVal is a temporary alias.
type StrVal = parser.StrVal

// Update is a temporary alias.
type Update = parser.Update

// NewStrVal is a temporary alias.
var NewStrVal = parser.NewStrVal

// MockNameTypes is a temporary alias.
var MockNameTypes = parser.MockNameTypes

// DTimestampTZ is a temporary alias.
type DTimestampTZ = parser.DTimestampTZ

// DString is a temporary alias.
type DString = parser.DString

// Serialize is a temporary alias.
var Serialize = parser.Serialize
