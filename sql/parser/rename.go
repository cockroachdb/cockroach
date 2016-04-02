// Copyright 2014 The Cockroach Authors.
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
//
// Author: Peter Mattis (peter@cockroachlabs.com)

// This code was derived from https://github.com/youtube/vitess.
//
// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file

package parser

import (
	"bytes"
	"fmt"
)

// RenameDatabase represents a RENAME DATABASE statement.
type RenameDatabase struct {
	Name    Name
	NewName Name
}

func (node *RenameDatabase) String() string {
	return fmt.Sprintf("ALTER DATABASE %s RENAME TO %s", node.Name, node.NewName)
}

// RenameTable represents a RENAME TABLE statement.
type RenameTable struct {
	Name     *QualifiedName
	NewName  *QualifiedName
	IfExists bool
}

func (node *RenameTable) String() string {
	var buf bytes.Buffer
	buf.WriteString("ALTER TABLE ")
	if node.IfExists {
		buf.WriteString("IF EXISTS ")
	}
	fmt.Fprintf(&buf, "%s RENAME TO %s", node.Name, node.NewName)
	return buf.String()
}

// RenameIndex represents a RENAME INDEX statement.
type RenameIndex struct {
	Index    *TableNameWithIndex
	NewName  Name
	IfExists bool
}

func (node *RenameIndex) String() string {
	var buf bytes.Buffer
	buf.WriteString("ALTER INDEX ")
	if node.IfExists {
		buf.WriteString("IF EXISTS ")
	}
	fmt.Fprintf(&buf, "%s RENAME TO %s", node.Index, node.NewName)
	return buf.String()
}

// RenameColumn represents a RENAME COLUMN statement.
type RenameColumn struct {
	Table   *QualifiedName
	Name    Name
	NewName Name
	// IfExists refers to the table, not the column.
	IfExists bool
}

func (node *RenameColumn) String() string {
	var buf bytes.Buffer
	buf.WriteString("ALTER TABLE ")
	if node.IfExists {
		buf.WriteString("IF EXISTS ")
	}
	fmt.Fprintf(&buf, "%s RENAME COLUMN %s TO %s", node.Table, node.Name, node.NewName)
	return buf.String()
}
