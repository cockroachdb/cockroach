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
//
// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file

package parser

import "bytes"

// SetVar represents a SET or RESET statement.
type SetVar struct {
	Name   VarName
	Values Exprs
}

// Format implements the NodeFormatter interface.
func (node *SetVar) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("SET ")
	if node.Name == nil {
		buf.WriteString("ROW (")
		FormatNode(buf, f, node.Values)
		buf.WriteString(")")
	} else {
		FormatNode(buf, f, node.Name)
		buf.WriteString(" = ")
		FormatNode(buf, f, node.Values)
	}
}

// SetClusterSetting represents a SET CLUSTER SETTING statement.
type SetClusterSetting struct {
	Name  VarName
	Value Expr
}

// Format implements the NodeFormatter interface.
func (node *SetClusterSetting) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("SET CLUSTER SETTING ")
	FormatNode(buf, f, node.Name)
	buf.WriteString(" = ")
	FormatNode(buf, f, node.Value)
}

// SetTransaction represents a SET TRANSACTION statement.
type SetTransaction struct {
	Modes TransactionModes
}

// Format implements the NodeFormatter interface.
func (node *SetTransaction) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("SET TRANSACTION")
	node.Modes.Format(buf, f)
}

// SetDefaultIsolation represents a SET SESSION CHARACTERISTICS AS TRANSACTION statement.
type SetDefaultIsolation struct {
	Isolation IsolationLevel
}

// Format implements the NodeFormatter interface.
func (node *SetDefaultIsolation) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL")
	if node.Isolation != UnspecifiedIsolation {
		buf.WriteByte(' ')
		buf.WriteString(node.Isolation.String())
	}
}
