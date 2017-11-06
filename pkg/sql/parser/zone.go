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

package parser

import (
	"bytes"
)

// ZoneSpecifier represents a reference to a configurable zone of the keyspace.
type ZoneSpecifier struct {
	// Only one of NamedZone, Database or TableOrIndex may be set.
	NamedZone    UnrestrictedName
	Database     Name
	TableOrIndex TableNameWithIndex

	// Partition is only respected when Table is set.
	Partition Name
}

// TargetsTable returns whether the zone specifier targets a table or a subzone
// within a table.
func (node ZoneSpecifier) TargetsTable() bool {
	return node.NamedZone == "" && node.Database == ""
}

// TargetsIndex returns whether the zone specifier targets an index.
func (node ZoneSpecifier) TargetsIndex() bool {
	return node.TargetsTable() && (node.TableOrIndex.Index != "" || node.TableOrIndex.SearchTable)
}

// Format implements the NodeFormatter interface.
func (node ZoneSpecifier) Format(buf *bytes.Buffer, f FmtFlags) {
	if node.NamedZone != "" {
		buf.WriteString("RANGE ")
		FormatNode(buf, f, &node.NamedZone)
	} else if node.Database != "" {
		buf.WriteString("DATABASE ")
		FormatNode(buf, f, &node.Database)
	} else {
		if node.TargetsIndex() {
			buf.WriteString("INDEX ")
		} else {
			buf.WriteString("TABLE ")
		}
		FormatNode(buf, f, &node.TableOrIndex)
	}
	if node.Partition != "" {
		buf.WriteString(" PARTITION ")
		FormatNode(buf, f, &node.Partition)
	}
}

func (node ZoneSpecifier) String() string { return AsString(node) }

// ShowZoneConfig represents an EXPERIMENTAL SHOW ZONE CONFIGURATION...
// statement.
type ShowZoneConfig struct {
	ZoneSpecifier
}

// Format implements the NodeFormatter interface.
func (node *ShowZoneConfig) Format(buf *bytes.Buffer, f FmtFlags) {
	if node.ZoneSpecifier == (ZoneSpecifier{}) {
		buf.WriteString("EXPERIMENTAL SHOW ZONE CONFIGURATIONS")
	} else {
		buf.WriteString("EXPERIMENTAL SHOW ZONE CONFIGURATION FOR ")
		FormatNode(buf, f, node.ZoneSpecifier)
	}
}

// SetZoneConfig represents an ALTER DATABASE/TABLE... EXPERIMENTAL CONFIGURE
// ZONE statement.
type SetZoneConfig struct {
	ZoneSpecifier
	YAMLConfig Expr
}

// Format implements the NodeFormatter interface.
func (node *SetZoneConfig) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("ALTER ")
	FormatNode(buf, f, node.ZoneSpecifier)
	buf.WriteString(" EXPERIMENTAL CONFIGURE ZONE ")
	FormatNode(buf, f, node.YAMLConfig)
}
