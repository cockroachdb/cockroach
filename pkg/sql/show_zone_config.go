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

	yaml "gopkg.in/yaml.v2"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

type showZoneConfigNode struct {
	optColumnsSlot
	zoneSpecifier tree.ZoneSpecifier

	run showZoneConfigRun
}

func (p *planner) ShowZoneConfig(ctx context.Context, n *tree.ShowZoneConfig) (planNode, error) {
	if n.ZoneSpecifier == (tree.ZoneSpecifier{}) {
		return p.delegateQuery(ctx, "SHOW ZONE CONFIGURATIONS",
			`SELECT zone_id, cli_specifier, config_sql, config_protobuf
         FROM crdb_internal.zones
        WHERE cli_specifier IS NOT NULL`, nil, nil)
	}
	return &showZoneConfigNode{
		zoneSpecifier: n.ZoneSpecifier,
	}, nil
}

// These must match crdb_internal.zones.
var showZoneConfigNodeColumns = sqlbase.ResultColumns{
	{Name: "zone_id", Typ: types.Int},
	{Name: "cli_specifier", Typ: types.String},
	{Name: "config_yaml", Typ: types.String, Hidden: true},
	{Name: "config_sql", Typ: types.String},
	{Name: "config_protobuf", Typ: types.Bytes},
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
	values[0] = zoneID

	// Populate the specifier column.
	if zs == nil {
		values[1] = tree.DNull
	} else {
		values[1] = tree.NewDString(config.CLIZoneSpecifier(zs))
	}

	// Populate the YAML column.
	yamlConfig, err := yaml.Marshal(zone)
	if err != nil {
		return err
	}
	values[2] = tree.NewDString(string(yamlConfig))

	// Populate the SQL column.
	if zs == nil {
		values[3] = tree.DNull
	} else {
		constraints, err := yamlMarshalFlow(config.ConstraintsList(zone.Constraints))
		if err != nil {
			return err
		}
		constraints = strings.TrimSpace(constraints)
		prefs, err := yamlMarshalFlow(zone.LeasePreferences)
		if err != nil {
			return err
		}
		prefs = strings.TrimSpace(prefs)

		f := tree.NewFmtCtxWithBuf(tree.FmtParsable)
		f.WriteString("ALTER ")
		f.FormatNode(zs)
		f.WriteString(" CONFIGURE ZONE USING\n")
		f.Printf("\trange_min_bytes = %d,\n", zone.RangeMinBytes)
		f.Printf("\trange_max_bytes = %d,\n", zone.RangeMaxBytes)
		f.Printf("\tgc.ttlseconds = %d,\n", zone.GC.TTLSeconds)
		f.Printf("\tnum_replicas = %d,\n", zone.NumReplicas)
		f.Printf("\tconstraints = %s,\n", lex.EscapeSQLString(constraints))
		f.Printf("\tlease_preferences = %s", lex.EscapeSQLString(prefs))
		values[3] = tree.NewDString(f.String())
	}

	// Populate the protobuf column.
	protoConfig, err := protoutil.Marshal(zone)
	if err != nil {
		return err
	}
	values[4] = tree.NewDBytes(tree.DBytes(protoConfig))

	return nil
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
		zs.Database = zs.TableOrIndex.Table.TableName().CatalogName
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
