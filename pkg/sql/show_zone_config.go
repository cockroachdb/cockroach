// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"bytes"
	"context"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"gopkg.in/yaml.v2"
)

// These must match crdb_internal.zones.
var showZoneConfigColumns = sqlbase.ResultColumns{
	{Name: "zone_id", Typ: types.Int, Hidden: true},
	{Name: "target", Typ: types.String},
	{Name: "range_name", Typ: types.String, Hidden: true},
	{Name: "database_name", Typ: types.String, Hidden: true},
	{Name: "table_name", Typ: types.String, Hidden: true},
	{Name: "index_name", Typ: types.String, Hidden: true},
	{Name: "partition_name", Typ: types.String, Hidden: true},
	{Name: "config_yaml", Typ: types.String, Hidden: true},
	{Name: "config_sql", Typ: types.String},
	{Name: "config_protobuf", Typ: types.Bytes, Hidden: true},
}

// These must match showZoneConfigColumns.
const (
	zoneIDCol int = iota
	targetCol
	rangeNameCol
	databaseNameCol
	tableNameCol
	indexNameCol
	partitionNameCol
	configYAMLCol
	configSQLCol
	configProtobufCol
)

func (p *planner) ShowZoneConfig(ctx context.Context, n *tree.ShowZoneConfig) (planNode, error) {
	return &delayedNode{
		name:    n.String(),
		columns: showZoneConfigColumns,
		constructor: func(ctx context.Context, p *planner) (planNode, error) {
			v := p.newContainerValuesNode(showZoneConfigColumns, 0)

			if n.ZoneSpecifier == (tree.ZoneSpecifier{}) {
				// SHOW ALL ZONE CONFIGURATIONS case.
				rows, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.Query(
					ctx,
					"show-all-zone-configurations",
					p.txn,
					`SELECT * FROM crdb_internal.zones`,
				)
				if err != nil {
					return nil, err
				}
				for i := range rows {
					if _, err := v.rows.AddRow(ctx, rows[i]); err != nil {
						v.Close(ctx)
						return nil, err
					}
				}
			} else {
				row, err := getShowZoneConfigRow(ctx, p, n.ZoneSpecifier)
				if err != nil {
					v.Close(ctx)
					return nil, err
				}
				if _, err := v.rows.AddRow(ctx, row); err != nil {
					v.Close(ctx)
					return nil, err
				}
			}
			return v, nil
		},
	}, nil
}

func getShowZoneConfigRow(
	ctx context.Context, p *planner, zoneSpecifier tree.ZoneSpecifier,
) (tree.Datums, error) {
	tblDesc, err := p.resolveTableForZone(ctx, &zoneSpecifier)
	if err != nil {
		return nil, err
	}

	targetID, err := resolveZone(ctx, p.txn, &zoneSpecifier)
	if err != nil {
		return nil, err
	}

	index, partition, err := resolveSubzone(&zoneSpecifier, tblDesc)
	if err != nil {
		return nil, err
	}

	zoneID, zone, subzone, err := GetZoneConfigInTxn(ctx, p.txn,
		uint32(targetID), index, partition, false /* getInheritedDefault */)
	if err == errNoZoneConfigApplies {
		// TODO(benesch): This shouldn't be the caller's responsibility;
		// GetZoneConfigInTxn should just return the default zone config if no zone
		// config applies.
		zone = p.execCfg.DefaultZoneConfig
		zoneID = keys.RootNamespaceID
	} else if err != nil {
		return nil, err
	} else if subzone != nil {
		zone = &subzone.Config
	}

	// Determine the zone specifier for the zone config that actually applies
	// without performing another KV lookup.
	zs := ascendZoneSpecifier(zoneSpecifier, uint32(targetID), zoneID, subzone)

	// Ensure subzone configs don't infect the output of config_bytes.
	zone.Subzones = nil
	zone.SubzoneSpans = nil

	vals := make(tree.Datums, len(showZoneConfigColumns))
	if err := generateZoneConfigIntrospectionValues(
		vals, tree.NewDInt(tree.DInt(zoneID)), &zs, zone,
	); err != nil {
		return nil, err
	}
	return vals, nil
}

// generateZoneConfigIntrospectionValues creates a result row
// suitable for populating crdb_internal.zones or SHOW ZONE CONFIG.
// The values are populated into the `values` first argument.
// The caller is responsible for creating the DInt for the ID and
// provide it as 2nd argument. The function will compute
// the remaining values based on the zone specifier and configuration.
func generateZoneConfigIntrospectionValues(
	values tree.Datums, zoneID tree.Datum, zs *tree.ZoneSpecifier, zone *zonepb.ZoneConfig,
) error {
	// Populate the ID column.
	values[zoneIDCol] = zoneID

	// Populate the zone specifier columns.
	values[targetCol] = tree.DNull
	values[rangeNameCol] = tree.DNull
	values[databaseNameCol] = tree.DNull
	values[tableNameCol] = tree.DNull
	values[indexNameCol] = tree.DNull
	values[partitionNameCol] = tree.DNull
	if zs != nil {
		values[targetCol] = tree.NewDString(zs.String())
		if zs.NamedZone != "" {
			values[rangeNameCol] = tree.NewDString(string(zs.NamedZone))
		}
		if zs.Database != "" {
			values[databaseNameCol] = tree.NewDString(string(zs.Database))
		}
		if zs.TableOrIndex.Table.TableName != "" {
			values[databaseNameCol] = tree.NewDString(string(zs.TableOrIndex.Table.CatalogName))
			values[tableNameCol] = tree.NewDString(string(zs.TableOrIndex.Table.TableName))
		}
		if zs.TableOrIndex.Index != "" {
			values[indexNameCol] = tree.NewDString(string(zs.TableOrIndex.Index))
		}
		if zs.Partition != "" {
			values[partitionNameCol] = tree.NewDString(string(zs.Partition))
		}
	}

	// Populate the YAML column.
	yamlConfig, err := yaml.Marshal(zone)
	if err != nil {
		return err
	}
	values[configYAMLCol] = tree.NewDString(string(yamlConfig))

	// Populate the SQL column.
	if zs == nil {
		values[configSQLCol] = tree.DNull
	} else {
		constraints, err := yamlMarshalFlow(zonepb.ConstraintsList{
			Constraints: zone.Constraints,
			Inherited:   zone.InheritedConstraints})
		if err != nil {
			return err
		}
		constraints = strings.TrimSpace(constraints)
		prefs, err := yamlMarshalFlow(zone.LeasePreferences)
		if err != nil {
			return err
		}
		prefs = strings.TrimSpace(prefs)

		useComma := false
		f := tree.NewFmtCtx(tree.FmtParsable)
		f.WriteString("ALTER ")
		f.FormatNode(zs)
		f.WriteString(" CONFIGURE ZONE USING\n")
		if zone.RangeMinBytes != nil {
			f.Printf("\trange_min_bytes = %d", *zone.RangeMinBytes)
			useComma = true
		}
		if zone.RangeMaxBytes != nil {
			writeComma(f, useComma)
			f.Printf("\trange_max_bytes = %d", *zone.RangeMaxBytes)
			useComma = true
		}
		if zone.GC != nil {
			writeComma(f, useComma)
			f.Printf("\tgc.ttlseconds = %d", zone.GC.TTLSeconds)
			useComma = true
		}
		if zone.NumReplicas != nil {
			writeComma(f, useComma)
			f.Printf("\tnum_replicas = %d", *zone.NumReplicas)
			useComma = true
		}
		if !zone.InheritedConstraints {
			writeComma(f, useComma)
			f.Printf("\tconstraints = %s", lex.EscapeSQLString(constraints))
			useComma = true
		}
		if !zone.InheritedLeasePreferences {
			writeComma(f, useComma)
			f.Printf("\tlease_preferences = %s", lex.EscapeSQLString(prefs))
		}
		values[configSQLCol] = tree.NewDString(f.String())
	}

	// Populate the protobuf column.
	protoConfig, err := protoutil.Marshal(zone)
	if err != nil {
		return err
	}
	values[configProtobufCol] = tree.NewDBytes(tree.DBytes(protoConfig))

	return nil
}

// Writes a comma followed by a newline if useComma is true.
func writeComma(f *tree.FmtCtx, useComma bool) {
	if useComma {
		f.Printf(",\n")
	}
}

func yamlMarshalFlow(v interface{}) (string, error) {
	var buf bytes.Buffer
	e := yaml.NewEncoder(&buf)
	e.UseStyle(yaml.FlowStyle)
	if err := e.Encode(v); err != nil {
		return "", err
	}
	if err := e.Close(); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// ascendZoneSpecifier logically ascends the zone hierarchy for the zone
// specified by (zs, resolvedID) until the zone matching actualID is found, and
// returns that zone's specifier. Results are undefined if actualID is not in
// the hierarchy for (zs, resolvedID).
//
// Under the hood, this function encodes knowledge about the zone lookup
// hierarchy to avoid performing any KV lookups, and so must be kept in sync
// with GetZoneConfig.
//
// TODO(benesch): Teach GetZoneConfig to return the specifier of the zone it
// finds without impacting performance.
func ascendZoneSpecifier(
	zs tree.ZoneSpecifier, resolvedID, actualID uint32, actualSubzone *zonepb.Subzone,
) tree.ZoneSpecifier {
	if actualID == keys.RootNamespaceID {
		// We had to traverse to the top of the hierarchy, so we're showing the
		// default zone config.
		zs.NamedZone = zonepb.DefaultZoneName
		zs.Database = ""
		zs.TableOrIndex = tree.TableIndexName{}
		// Since the default zone has no partition, we can erase the
		// partition name field.
		zs.Partition = ""
	} else if resolvedID != actualID {
		// We traversed at least one level up, and we're not at the top of the
		// hierarchy, so we're showing the database zone config.
		zs.Database = zs.TableOrIndex.Table.CatalogName
		zs.TableOrIndex = tree.TableIndexName{}
		// Since databases don't have partition, we can erase the
		// partition name field.
		zs.Partition = ""
	} else if actualSubzone == nil {
		// We didn't find a subzone, so no index or partition zone config exists.
		zs.TableOrIndex.Index = ""
		zs.Partition = ""
	} else if actualSubzone.PartitionName == "" {
		// The resolved subzone did not name a partition, just an index.
		zs.Partition = ""
	}
	return zs
}
