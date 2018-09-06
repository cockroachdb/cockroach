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
func (node *ZoneSpecifier) Format(ctx *FmtCtx) {
	if node.Partition != "" {
		ctx.WriteString("PARTITION ")
		ctx.FormatNode(&node.Partition)
		ctx.WriteString(" OF ")
	}
	if node.NamedZone != "" {
		ctx.WriteString("RANGE ")
		ctx.FormatNode(&node.NamedZone)
	} else if node.Database != "" {
		ctx.WriteString("DATABASE ")
		ctx.FormatNode(&node.Database)
	} else {
		if node.TargetsIndex() {
			ctx.WriteString("INDEX ")
		} else {
			ctx.WriteString("TABLE ")
		}
		ctx.FormatNode(&node.TableOrIndex)
	}
}

func (node *ZoneSpecifier) String() string { return AsString(node) }

// ShowZoneConfig represents a SHOW ZONE CONFIGURATION
// statement.
type ShowZoneConfig struct {
	ZoneSpecifier
}

// Format implements the NodeFormatter interface.
func (node *ShowZoneConfig) Format(ctx *FmtCtx) {
	if node.ZoneSpecifier == (ZoneSpecifier{}) {
		ctx.WriteString("SHOW ZONE CONFIGURATIONS")
	} else {
		ctx.WriteString("SHOW ZONE CONFIGURATION FOR ")
		ctx.FormatNode(&node.ZoneSpecifier)
	}
}

// SetZoneConfig represents an ALTER DATABASE/TABLE... CONFIGURE ZONE
// statement.
type SetZoneConfig struct {
	ZoneSpecifier
	SetDefault bool
	YAMLConfig Expr
	Options    KVOptions
}

// Format implements the NodeFormatter interface.
func (node *SetZoneConfig) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER ")
	ctx.FormatNode(&node.ZoneSpecifier)
	ctx.WriteString(" CONFIGURE ZONE ")
	if node.SetDefault {
		ctx.WriteString("USING DEFAULT")
	} else if node.YAMLConfig != nil {
		if node.YAMLConfig == DNull {
			ctx.WriteString("DISCARD")
		} else {
			ctx.WriteString("= ")
			ctx.FormatNode(node.YAMLConfig)
		}
	} else {
		ctx.WriteString("USING ")
		ctx.FormatNode(&node.Options)
	}
}
