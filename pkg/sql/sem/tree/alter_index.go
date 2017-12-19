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

package tree

import (
	"bytes"
)

// AlterIndex represents an ALTER INDEX statement.
type AlterIndex struct {
	IfExists bool
	Index    *TableNameWithIndex
	Cmds     AlterIndexCmds
}

var _ Statement = &AlterIndex{}

// Format implements the NodeFormatter interface.
func (node *AlterIndex) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("ALTER INDEX ")
	if node.IfExists {
		buf.WriteString("IF EXISTS ")
	}
	FormatNode(buf, f, node.Index)
	FormatNode(buf, f, node.Cmds)
}

// AlterIndexCmds represents a list of index alterations.
type AlterIndexCmds []AlterIndexCmd

// Format implements the NodeFormatter interface.
func (node AlterIndexCmds) Format(buf *bytes.Buffer, f FmtFlags) {
	for i, n := range node {
		if i > 0 {
			buf.WriteString(",")
		}
		FormatNode(buf, f, n)
	}
}

// AlterIndexCmd represents an index modification operation.
type AlterIndexCmd interface {
	NodeFormatter
	// Placeholder function to ensure that only desired types
	// (AlterIndex*) conform to the AlterIndexCmd interface.
	alterIndexCmd()
}

func (*AlterIndexPartitionBy) alterIndexCmd() {}

var _ AlterIndexCmd = &AlterIndexPartitionBy{}

// AlterIndexPartitionBy represents an ALTER INDEX PARTITION BY
// command.
type AlterIndexPartitionBy struct {
	*PartitionBy
}

// Format implements the NodeFormatter interface.
func (node *AlterIndexPartitionBy) Format(buf *bytes.Buffer, f FmtFlags) {
	FormatNode(buf, f, node.PartitionBy)
}
