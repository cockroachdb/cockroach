// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package redactcheck defines an Analyzer that checks registered redact-safe
// types against an allow-list.
package redactcheck

import (
	"fmt"
	"go/ast"
	"go/token"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

// Analyzer is an analysis.Analyzer that checks for unused or discarded
// roachpb.Error objects from function calls.
var Analyzer = &analysis.Analyzer{
	Name:     "redactcheck",
	Doc:      "checks registered redact-safe types against an allow-list",
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      runAnalyzer,
}

func runAnalyzer(pass *analysis.Pass) (interface{}, error) {
	inspect := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)
	inspect.Preorder([]ast.Node{
		(*ast.ExprStmt)(nil),
		(*ast.FuncDecl)(nil),
	}, func(n ast.Node) {
		switch stmt := n.(type) {
		case *ast.FuncDecl:
			if stmt.Recv == nil {
				return
			}
			// Most types are marked redact-safe by the presence of
			// the SafeValue method.
			if stmt.Name.Name == "SafeValue" {
				recv := stmt.Recv.List
				if len(recv) != 1 {
					pass.Report(analysis.Diagnostic{
						Pos:     stmt.Recv.Opening,
						Message: "expected only one item in receiver list for method SafeValue",
					})
					return
				}
				var allowlist = map[string]map[string]struct{}{
					"github.com/cockroachdb/cockroach/pkg/base": {
						"NodeIDContainer":  {},
						"SQLIDContainer":   {},
						"StoreIDContainer": {},
					},
					"github.com/cockroachdb/cockroach/pkg/cli/exit": {
						"Code": {},
					},
					"github.com/cockroachdb/cockroach/pkg/kv/bulk/bulkpb": {
						"sz":     {},
						"timing": {},
					},
					"github.com/cockroachdb/cockroach/pkg/jobs/jobspb": {
						"Type": {},
					},
					"github.com/cockroachdb/cockroach/pkg/kv/bulk": {
						"sz":     {},
						"timing": {},
					},
					"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/ctpb": {
						"LAI":    {},
						"SeqNum": {},
					},
					"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock": {
						"Durability": {},
						"Strength":   {},
						"WaitPolicy": {},
					},
					"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb": {
						"SnapshotRequest_Type": {},
					},
					"github.com/cockroachdb/cockroach/pkg/repstream/streampb": {
						"StreamID": {},
					},
					"github.com/cockroachdb/cockroach/pkg/roachpb": {
						"LeaseSequence":     {},
						"Method":            {},
						"NodeID":            {},
						"RangeGeneration":   {},
						"RangeID":           {},
						"ReplicaChangeType": {},
						"ReplicaID":         {},
						"ReplicaType":       {},
						"StoreID":           {},
						"TenantID":          {},
						"TransactionStatus": {},
					},
					"github.com/cockroachdb/cockroach/pkg/rpc": {
						"ConnectionClass": {},
					},
					"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb": {
						"JobID": {},
					},
					"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb": {
						"ConstraintValidity":           {},
						"DescriptorMutation_Direction": {},
						"DescriptorMutation_State":     {},
						"DescriptorState":              {},
						"DescriptorVersion":            {},
						"IndexDescriptorVersion":       {},
						"MutationID":                   {},
					},
					"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants": {
						"ConstraintType": {},
					},
					"github.com/cockroachdb/cockroach/pkg/sql/sem/semenumpb": {
						"ForeignKeyAction": {},
					},
					"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/scgraph": {
						"RuleName": {},
					},
					"github.com/cockroachdb/cockroach/pkg/sql/sem/catid": {
						"ColumnID":       {},
						"ConstraintID":   {},
						"DescID":         {},
						"FamilyID":       {},
						"IndexID":        {},
						"PGAttributeNum": {},
					},
					"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness": {
						"SessionID": {},
					},
					"github.com/cockroachdb/cockroach/pkg/storage/enginepb": {
						"MVCCStats":      {},
						"MVCCStatsDelta": {},
						"TxnEpoch":       {},
						"TxnSeq":         {},
					},
					"github.com/cockroachdb/cockroach/pkg/util/hlc": {
						"ClockTimestamp":  {},
						"LegacyTimestamp": {},
						"Timestamp":       {},
					},
					"github.com/cockroachdb/pebble/internal/humanize": {
						"FormattedString": {},
					},
					"github.com/cockroachdb/redact/interfaces": {
						"SafeString": {},
						"SafeInt":    {},
						"SafeUint":   {},
						"SafeFloat":  {},
						"SafeRune":   {},
					},
					"github.com/cockroachdb/redact/internal/redact": {
						"safeWrapper": {},
					},
				}
				ty := recv[0].Type
				reportFailure := func() {
					pass.Report(analysis.Diagnostic{
						Pos:     stmt.Recv.Opening,
						Message: fmt.Sprintf("unallowed redact-safe type %+v", ty),
					})
				}
				allowedTypes, ok := allowlist[pass.Pkg.Path()]
				if !ok {
					reportFailure()
					return
				}
				// Note the receiver can be either a single
				// identifier or a pointer receiver (Star). For
				// redactibility we don't particularly care
				// which is which, so they're all in the same
				// allow-list.
				var typeToCheck *ast.Ident
				switch receiver := ty.(type) {
				case *ast.Ident:
					typeToCheck = receiver
				case *ast.StarExpr:
					var okIdent bool
					typeToCheck, okIdent = receiver.X.(*ast.Ident)
					if !okIdent {
						reportFailure()
						return
					}
				default:
					reportFailure()
					return
				}
				_, ok = allowedTypes[typeToCheck.Name]
				if !ok {
					reportFailure()
					return
				}
			}
		case *ast.ExprStmt:
			if call, ok := stmt.X.(*ast.CallExpr); ok {
				// Check whether function expression is redact.RegisterSafeType.
				selector, ok := call.Fun.(*ast.SelectorExpr)
				if !ok || selector.Sel.Name != "RegisterSafeType" {
					return
				}
				ident, ok := selector.X.(*ast.Ident)
				if !ok {
					return
				}
				if ident.Name != "redact" {
					return
				}
				// The interior of the call will be of the form reflect.TypeOf(value).
				if len(call.Args) != 1 {
					pass.Report(analysis.Diagnostic{
						Pos:     call.Lparen,
						Message: "expected only one argument to RegisterSafeType",
					})
					return
				}
				reportFailure := func() {
					pass.Report(analysis.Diagnostic{
						Pos:     call.Lparen,
						Message: "expected argument to RegisterSafeType to be of the form reflect.TypeOf(value)",
					})
				}
				typeOfCall, ok := call.Args[0].(*ast.CallExpr)
				if !ok {
					reportFailure()
					return
				}
				typeOfFun, ok := typeOfCall.Fun.(*ast.SelectorExpr)
				if !ok || typeOfFun.Sel.Name != "TypeOf" {
					reportFailure()
					return
				}
				reflect, ok := typeOfFun.X.(*ast.Ident)
				if !ok || reflect.Name != "reflect" {
					reportFailure()
					return
				}
				if len(typeOfCall.Args) != 1 {
					reportFailure()
					return
				}
				// Now check the argument value.
				argExpr := typeOfCall.Args[0]
				reportFailure = func() {
					pass.Report(analysis.Diagnostic{
						Pos:     argExpr.Pos(),
						Message: fmt.Sprintf("unallowed redact-safe type %+v", argExpr),
					})
				}
				switch arg := argExpr.(type) {
				case *ast.BasicLit:
					if arg.Kind != token.INT {
						reportFailure()
						return
					}
				case *ast.CallExpr:
					switch argFunc := arg.Fun.(type) {
					case *ast.Ident:
						switch argFunc.Name {
						case "Channel",
							"complex64",
							"complex128",
							"int8",
							"int16",
							"int32",
							"int64",
							"float32",
							"float64",
							"uint8",
							"uint16",
							"uint32",
							"uint64":
						default:
							reportFailure()
							return
						}
					case *ast.SelectorExpr:
						pkg, ok := argFunc.X.(*ast.Ident)
						if !ok {
							reportFailure()
							return
						}
						ty := argFunc.Sel.Name
						if !((pkg.Name == "time" && ty == "Duration") ||
							(pkg.Name == "encodingtype" && ty == "T")) {
							reportFailure()
							return
						}
					}
				case *ast.CompositeLit:
					compositeType, ok := arg.Type.(*ast.SelectorExpr)
					if !ok || compositeType.Sel.Name != "Time" {
						reportFailure()
						return
					}
					pkg, ok := compositeType.X.(*ast.Ident)
					if !ok || pkg.Name != "time" {
						reportFailure()
						return
					}
				case *ast.Ident:
					if arg.Name != "true" {
						reportFailure()
						return
					}
				case *ast.SelectorExpr:
					pkg, ok := arg.X.(*ast.Ident)
					if !ok || pkg.Name != "os" || arg.Sel.Name != "Interrupt" {
						reportFailure()
						return
					}
				}
			}
		}
	})
	return nil, nil
}
