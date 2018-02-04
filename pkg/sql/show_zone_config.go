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
	"context"

	yaml "gopkg.in/yaml.v2"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/keys"
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
		return p.delegateQuery(ctx, "SHOW ZONE CONFIGURATIONS", "TABLE crdb_internal.zones", nil, nil)
	}
	return &showZoneConfigNode{
		zoneSpecifier: n.ZoneSpecifier,
	}, nil
}

// These should match crdb_internal.zones.
var showZoneConfigNodeColumns = sqlbase.ResultColumns{
	{
		Name: "id",
		Typ:  types.Int,
	},
	{
		Name: "cli_specifier",
		Typ:  types.String,
	},
	{
		Name: "config_yaml",
		Typ:  types.Bytes,
	},
	{
		Name: "config_proto",
		Typ:  types.Bytes,
	},
}

// showZoneConfigRun contains the run-time state of showZoneConfigNode
// during local execution.
type showZoneConfigRun struct {
	zoneID       uint32
	cliSpecifier string
	protoConfig  []byte
	yamlConfig   []byte
	done         bool
}

func (n *showZoneConfigNode) startExec(params runParams) error {
	var tblDesc *TableDescriptor
	var err error
	// We avoid the cache so that we can observe the zone configuration
	// without taking a lease, like other SHOW commands.  We also use
	// allowAdding=true so we can look at the configuration of a table
	// added in the same transaction.
	//
	// TODO(vivek): check if the cache can be used.
	params.p.runWithOptions(resolveFlags{allowAdding: true, skipCache: true}, func() {
		tblDesc, err = params.p.resolveTableForZone(params.ctx, &n.zoneSpecifier)
	})
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
		uint32(targetID), index, partition)
	if err == errNoZoneConfigApplies {
		// TODO(benesch): This shouldn't be the caller's responsibility;
		// GetZoneConfigInTxn should just return the default zone config if no zone
		// config applies.
		zone = config.DefaultZoneConfig()
		zoneID = keys.RootNamespaceID
	} else if err != nil {
		return err
	} else if subzone != nil {
		zone = subzone.Config
	}
	n.run.zoneID = zoneID

	// Determine the CLI specifier for the zone config that actually applies
	// without performing another KV lookup.
	zs := ascendZoneSpecifier(n.zoneSpecifier, uint32(targetID), zoneID, subzone)
	n.run.cliSpecifier = config.CLIZoneSpecifier(&zs)

	// Ensure subzone configs don't infect the output of config_bytes.
	zone.Subzones = nil
	zone.SubzoneSpans = nil
	n.run.protoConfig, err = protoutil.Marshal(&zone)
	if err != nil {
		return err
	}
	n.run.yamlConfig, err = yaml.Marshal(zone)
	return err
}

func (n *showZoneConfigNode) Next(runParams) (bool, error) {
	if !n.run.done {
		n.run.done = true
		return true, nil
	}
	return false, nil
}

func (n *showZoneConfigNode) Values() tree.Datums {
	return tree.Datums{
		tree.NewDInt(tree.DInt(n.run.zoneID)),
		tree.NewDString(n.run.cliSpecifier),
		tree.NewDBytes(tree.DBytes(n.run.yamlConfig)),
		tree.NewDBytes(tree.DBytes(n.run.protoConfig)),
	}
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
