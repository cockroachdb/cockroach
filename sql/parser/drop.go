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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package parser

import (
	"bytes"
	"fmt"
)

func (*DropDatabase) statement() {}
func (*DropIndex) statement()    {}
func (*DropTable) statement()    {}
func (*DropView) statement()     {}

// DropDatabase represents a DROP DATABASE statement.
type DropDatabase struct {
	Name     string
	IfExists bool
}

func (node *DropDatabase) String() string {
	var buf bytes.Buffer
	buf.WriteString("DROP DATABASE ")
	if node.IfExists {
		buf.WriteString("IF EXISTS ")
	}
	buf.WriteString(node.Name)
	return buf.String()
}

// DropIndex represents a DROP INDEX statement.
type DropIndex struct {
	Name      string
	TableName string
}

func (node *DropIndex) String() string {
	return fmt.Sprintf("DROP INDEX %s ON %s", node.Name, node.TableName)
}

// DropTable represents a DROP TABLE statement.
type DropTable struct {
	Name     string
	IfExists bool
}

func (node *DropTable) String() string {
	var buf bytes.Buffer
	buf.WriteString("DROP TABLE ")
	if node.IfExists {
		buf.WriteString("IF EXISTS ")
	}
	buf.WriteString(node.Name)
	return buf.String()
}

// DropView represents a DROP TABLE statement.
type DropView struct {
	Name     string
	IfExists bool
}

func (node *DropView) String() string {
	var buf bytes.Buffer
	buf.WriteString("DROP VIEW ")
	if node.IfExists {
		buf.WriteString("IF EXISTS ")
	}
	buf.WriteString(node.Name)
	return buf.String()
}
