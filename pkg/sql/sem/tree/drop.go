// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in licenses/BSD-vitess.txt.

// Portions of this file are additionally subject to the following
// license and copyright.
//
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

package tree

import "bytes"

// DropBehavior represents options for dropping schema elements.
type DropBehavior int

// DropBehavior values.
const (
	DropDefault DropBehavior = iota
	DropRestrict
	DropCascade
)

var dropBehaviorName = [...]string{
	DropDefault:  "",
	DropRestrict: "RESTRICT",
	DropCascade:  "CASCADE",
}

func (d DropBehavior) String() string {
	return dropBehaviorName[d]
}

// DropDatabase represents a DROP DATABASE statement.
type DropDatabase struct {
	Name         Name
	IfExists     bool
	DropBehavior DropBehavior
}

// Format implements the NodeFormatter interface.
func (node *DropDatabase) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("DROP DATABASE ")
	if node.IfExists {
		buf.WriteString("IF EXISTS ")
	}
	FormatNode(buf, f, node.Name)
	if node.DropBehavior != DropDefault {
		buf.WriteByte(' ')
		buf.WriteString(node.DropBehavior.String())
	}
}

// DropIndex represents a DROP INDEX statement.
type DropIndex struct {
	IndexList    TableNameWithIndexList
	IfExists     bool
	DropBehavior DropBehavior
}

// Format implements the NodeFormatter interface.
func (node *DropIndex) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("DROP INDEX ")
	if node.IfExists {
		buf.WriteString("IF EXISTS ")
	}
	FormatNode(buf, f, node.IndexList)
	if node.DropBehavior != DropDefault {
		buf.WriteByte(' ')
		buf.WriteString(node.DropBehavior.String())
	}
}

// DropTable represents a DROP TABLE statement.
type DropTable struct {
	Names        TableNameReferences
	IfExists     bool
	DropBehavior DropBehavior
}

// Format implements the NodeFormatter interface.
func (node *DropTable) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("DROP TABLE ")
	if node.IfExists {
		buf.WriteString("IF EXISTS ")
	}
	FormatNode(buf, f, node.Names)
	if node.DropBehavior != DropDefault {
		buf.WriteByte(' ')
		buf.WriteString(node.DropBehavior.String())
	}
}

// DropView represents a DROP VIEW statement.
type DropView struct {
	Names        TableNameReferences
	IfExists     bool
	DropBehavior DropBehavior
}

// Format implements the NodeFormatter interface.
func (node *DropView) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("DROP VIEW ")
	if node.IfExists {
		buf.WriteString("IF EXISTS ")
	}
	FormatNode(buf, f, node.Names)
	if node.DropBehavior != DropDefault {
		buf.WriteByte(' ')
		buf.WriteString(node.DropBehavior.String())
	}
}

// DropSequence represents a DROP SEQUENCE statement.
type DropSequence struct {
	Names        TableNameReferences
	IfExists     bool
	DropBehavior DropBehavior
}

// Format implements the NodeFormatter interface.
func (node *DropSequence) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("DROP SEQUENCE ")
	if node.IfExists {
		buf.WriteString("IF EXISTS ")
	}
	FormatNode(buf, f, node.Names)
	if node.DropBehavior != DropDefault {
		buf.WriteByte(' ')
		buf.WriteString(node.DropBehavior.String())
	}
}

// DropUser represents a DROP USER statement
type DropUser struct {
	Names    Exprs
	IfExists bool
}

// Format implements the NodeFormatter interface.
func (node *DropUser) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("DROP USER ")
	if node.IfExists {
		buf.WriteString("IF EXISTS ")
	}
	FormatNode(buf, f, node.Names)
}

// DropRole represents a DROP ROLE statement
type DropRole struct {
	Names    Exprs
	IfExists bool
}

// Format implements the NodeFormatter interface.
func (node *DropRole) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("DROP ROLE ")
	if node.IfExists {
		buf.WriteString("IF EXISTS ")
	}
	FormatNode(buf, f, node.Names)
}
