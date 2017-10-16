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
	// Only one of NamedZone, Database or Table may be set.
	NamedZone Name
	Database  Name
	Table     NormalizableTableName
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
		buf.WriteString("TABLE ")
		FormatNode(buf, f, &node.Table)
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
