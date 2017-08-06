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

package base

import "bytes"

// This file contains shim Go functions that make it possible for the
// Go compiler (and linters) to check the templates *before* they are
// transformed into generated code.

// Type is a mock struct type.
type Type struct{ ref *node }

func macroGetExtraRefs(x extraStruct) []*node { return nil }

func macroGetName(ref *node) Type { return Type{} }

func macroSetName(ref *node, extra *extraStruct, x Type) {}

// FormatSExpr implements the SexprFormatter interface.
func (t Type) FormatSExpr(buf *bytes.Buffer) {}

// FormatSExprTypName is a stub for Sexpr formatters for primitive types.
func FormatSExprTypName(buf *bytes.Buffer, x Type) {}
