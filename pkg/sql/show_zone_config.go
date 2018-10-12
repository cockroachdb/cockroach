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

package sql

import (
	"bytes"
	"context"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	yaml "gopkg.in/yaml.v2"
)

type showZoneConfigNode struct {
	optColumnsSlot
	zoneSpecifier tree.ZoneSpecifier

	run showZoneConfigRun
}

// These must match crdb_internal.zones.
var showZoneConfigNodeColumns = sqlbase.ResultColumns{
	{Name: "zone_id", Typ: types.Int, Hidden: true},
	{Name: "zone_name", Typ: types.String},
	{Name: "cli_specifier", Typ: types.String, Hidden: true},
	{Name: "config_yaml", Typ: types.String, Hidden: true},
	{Name: "config_sql", Typ: types.String},
	{Name: "config_protobuf", Typ: types.Bytes, Hidden: true},
}

// These must match showZoneConfigColumns.
const (
	zoneIDCol int = iota
	zoneNameCol
	cliSpecifierCol
	configYAMLCol
	configSQLCol
	configProtobufCol
)

func (p *planner) ShowZoneConfig(ctx context.Context, n *tree.ShowZoneConfig) (planNode, error) {
	if n.ZoneSpecifier == (tree.ZoneSpecifier{}) {
		planRenderNode, err := p.delegateQuery(ctx, "SHOW ZONE CONFIGURATIONS",
			`SELECT zone_id, cli_specifier AS zone_name, cli_specifier, config_sql
         FROM crdb_internal.zones
        WHERE cli_specifier IS NOT NULL`, nil, nil)
		if err != nil {
			return planRenderNode, err
		}

		// Using planMutableColumns to hide the cli_specifier column,
		// that needs to be supported for backwards compatibility with
		// the CLI.
		columns := planMutableColumns(planRenderNode)
		columns[zoneIDCol].Hidden = true
		columns[cliSpecifierCol].Hidden = true

		return planRenderNode, nil
	}
	return &showZoneConfigNode{
		zoneSpecifier: n.ZoneSpecifier,
	}, nil
}

// showZoneConfigRun contains the run-time state of showZoneConfigNode
// during local execution.
type showZoneConfigRun struct {
	values tree.Datums
	done   bool
}

func (n *showZoneConfigNode) startExec(params runParams) error {
	tblDesc, err := params.p.resolveTableForZone(params.ctx, &n.zoneSpecifier)
	if err != nil {
		return err
	}

	targetID, err := resolveZone(
		params.ctx, params.p.txn, &n.zoneSpecifier)
	if err != nil {
		return err
	}

	index, partition, err := resolveSubzone(
		params.ctx, params.p.txn, &n.zoneSpecifier, targetID, tblDesc)
	if err != nil {
		return err
	}

	zoneID, zone, subzone, err := GetZoneConfigInTxn(params.ctx, params.p.txn,
		uint32(targetID), index, partition, false /* getInheritedDefault */)
	if err == errNoZoneConfigApplies {
		// TODO(benesch): This shouldn't be the caller's responsibility;
		// GetZoneConfigInTxn should just return the default zone config if no zone
		// config applies.
		defZone := config.DefaultZoneConfig()
		zone = &defZone
		zoneID = keys.RootNamespaceID
	} else if err != nil {
		return err
	} else if subzone != nil {
		zone = &subzone.Config
	}

	// Determine the CLI specifier for the zone config that actually applies
	// without performing another KV lookup.
	zs := ascendZoneSpecifier(n.zoneSpecifier, uint32(targetID), zoneID, subzone)

	// Ensure subzone configs don't infect the output of config_bytes.
	zone.Subzones = nil
	zone.SubzoneSpans = nil

	n.run.values = make(tree.Datums, len(showZoneConfigNodeColumns))
	return generateZoneConfigIntrospectionValues(
		n.run.values, tree.NewDInt(tree.DInt(zoneID)), &zs, zone)
}

// generateZoneConfigIntrospectionValues creates a result row
// suitable for populating crdb_internal.zones or SHOW ZONE CONFIG.
// The values are populated into the `values` first argument.
// The caller is responsible for creating the DInt for the ID and
// provide it as 2nd argument. The function will compute
// the remaining values based on the zone specifier and configuration.
func generateZoneConfigIntrospectionValues(
	values tree.Datums, zoneID tree.Datum, zs *tree.ZoneSpecifier, zone *config.ZoneConfig,
) error {
	// Populate the ID column.
	values[zoneIDCol] = zoneID

	// Populate the specifier column.
	if zs == nil {
		values[zoneNameCol] = tree.DNull
	} else {
		values[zoneNameCol] = tree.NewDString(config.CLIZoneSpecifier(zs))
	}
	values[cliSpecifierCol] = values[zoneNameCol]

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
		constraints, err := yamlMarshalFlow(config.ConstraintsList{
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
		f := tree.NewFmtCtxWithBuf(tree.FmtParsable)
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
func writeComma(f *tree.FmtCtxWithBuf, useComma bool) {
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

func (n *showZoneConfigNode) Next(runParams) (bool, error) {
	if !n.run.done {
		n.run.done = true
		return true, nil
	}
	return false, nil
}

func (n *showZoneConfigNode) Values() tree.Datums {
	return n.run.values
}

func (*showZoneConfigNode) Close(context.Context) {}

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
	zs tree.ZoneSpecifier, resolvedID, actualID uint32, actualSubzone *config.Subzone,
) tree.ZoneSpecifier {
	if actualID == keys.RootNamespaceID {
		// We had to traverse to the top of the hierarchy, so we're showing the
		// default zone config.
		zs.NamedZone = config.DefaultZoneName
	} else if resolvedID != actualID {
		// We traversed at least one level up, and we're not at the top of the
		// hierarchy, so we're showing the database zone config.
		zs.Database = zs.TableOrIndex.Table.CatalogName
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
