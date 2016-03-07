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

// Show represents a SHOW statement.
type Show struct {
	Name string
}

func (node *Show) String() string {
	return fmt.Sprintf("SHOW %s", node.Name)
}

// ShowColumns represents a SHOW COLUMNS statement.
type ShowColumns struct {
	Table *QualifiedName
}

func (node *ShowColumns) String() string {
	var buf bytes.Buffer
	buf.WriteString("SHOW ")
	fmt.Fprintf(&buf, "COLUMNS FROM %s", node.Table)
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
	Table *QualifiedName
}

func (node *ShowIndex) String() string {
	return fmt.Sprintf("SHOW INDEXES FROM %s", node.Table)
}

// ShowTables represents a SHOW TABLES statement.
type ShowTables struct {
	Name *QualifiedName
}

func (node *ShowTables) String() string {
	var buf bytes.Buffer
	buf.WriteString("SHOW TABLES")
	if node.Name != nil {
		fmt.Fprintf(&buf, " FROM %s", node.Name)
	}
	return buf.String()
}

// ShowGrants represents a SHOW GRANTS statement.
// TargetList is defined in grant.go.
type ShowGrants struct {
	Targets  *TargetList
	Grantees NameList
}

func (node *ShowGrants) String() string {
	var buf bytes.Buffer
	buf.WriteString("SHOW GRANTS")
	if node.Targets != nil {
		fmt.Fprintf(&buf, " ON %s", *node.Targets)
	}
	if node.Grantees != nil {
		fmt.Fprintf(&buf, " FOR %s", node.Grantees)
	}
	return buf.String()
}
