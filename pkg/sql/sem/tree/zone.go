// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

// ZoneSpecifier represents a reference to a configurable zone of the keyspace.
type ZoneSpecifier struct {
	// Only one of NamedZone, Database or TableOrIndex may be set.
	NamedZone UnrestrictedName
	Database  Name
	// TODO(radu): TableOrIndex abuses TableIndexName: it allows for the case when
	// an index is not specified, in which case TableOrIndex.Index is empty.
	TableOrIndex TableIndexName

	// Partition is only respected when Table is set.
	Partition Name
}

// TelemetryName returns a name fitting for telemetry purposes.
func (node ZoneSpecifier) TelemetryName() string {
	if node.NamedZone != "" {
		return "range"
	}
	if node.Database != "" {
		return "database"
	}
	str := ""
	if node.Partition != "" {
		str = "partition."
	}
	if node.TargetsIndex() {
		str += "index"
	} else {
		str += "table"
	}
	return str
}

// TargetsTable returns whether the zone specifier targets a table or a subzone
// within a table.
func (node ZoneSpecifier) TargetsTable() bool {
	return node.NamedZone == "" && node.Database == ""
}

// TargetsIndex returns whether the zone specifier targets an index.
func (node ZoneSpecifier) TargetsIndex() bool {
	return node.TargetsTable() && node.TableOrIndex.Index != ""
}

// TargetsPartition returns whether the zone specifier targets a partition.
func (node ZoneSpecifier) TargetsPartition() bool {
	return node.TargetsTable() && node.Partition != ""
}

// Format implements the NodeFormatter interface.
func (node *ZoneSpecifier) Format(ctx *FmtCtx) {
	if node.NamedZone != "" {
		ctx.WriteString("RANGE ")
		ctx.FormatNode(&node.NamedZone)
	} else if node.Database != "" {
		ctx.WriteString("DATABASE ")
		ctx.FormatNode(&node.Database)
	} else {
		if node.Partition != "" {
			ctx.WriteString("PARTITION ")
			ctx.FormatNode(&node.Partition)
			ctx.WriteString(" OF ")
		}
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
	// AllIndexes indicates that the zone configuration should be applied across
	// all of a tables indexes. (ALTER PARTITION ... OF INDEX <tablename>@*)
	AllIndexes bool
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
		kvOptions := node.Options
		comma := ""
		for _, kv := range kvOptions {
			ctx.WriteString(comma)
			comma = ", "
			ctx.FormatNode(&kv.Key)
			if kv.Value != nil {
				ctx.WriteString(` = `)
				ctx.FormatNode(kv.Value)
			} else {
				ctx.WriteString(` = COPY FROM PARENT`)
			}
		}
	}
}
