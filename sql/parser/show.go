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

func (*ShowColumns) statement()   {}
func (*ShowDatabases) statement() {}
func (*ShowIndex) statement()     {}
func (*ShowTables) statement()    {}

// ShowColumns represents a SHOW [FULL] COLUMNS statement.
type ShowColumns struct {
	Name *TableName
	Full bool
}

func (node *ShowColumns) String() string {
	var buf bytes.Buffer
	buf.WriteString("SHOW ")
	if node.Full {
		buf.WriteString("FULL ")
	}
	fmt.Fprintf(&buf, "COLUMNS FROM %s", node.Name)
	return buf.String()
}

// ShowDatabases represents a SHOW DATABASES statement.
type ShowDatabases struct {
}

func (node *ShowDatabases) String() string {
	return "SHOW DATABASES"
}

// ShowIndex represents a SHOW INDEX statement.
type ShowIndex struct {
	Name *TableName
}

func (node *ShowIndex) String() string {
	return fmt.Sprintf("SHOW INDEX FROM %s", node.Name)
}

// ShowTables represents a SHOW TABLES statement.
type ShowTables struct {
	Name string
}

func (node *ShowTables) String() string {
	var buf bytes.Buffer
	buf.WriteString("SHOW TABLES")
	if node.Name != "" {
		fmt.Fprintf(&buf, " FROM %s", node.Name)
	}
	return buf.String()
}
