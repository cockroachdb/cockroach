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
	"fmt"

	"golang.org/x/net/context"
	yaml "gopkg.in/yaml.v2"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/pkg/errors"
)

func zoneSpecifierNotFoundError(zs tree.ZoneSpecifier) error {
	if zs.NamedZone != "" {
		return pgerror.NewErrorf(
			pgerror.CodeInvalidCatalogNameError, "zone %q does not exist", zs.NamedZone)
	} else if zs.Database != "" {
		return sqlbase.NewUndefinedDatabaseError(string(zs.Database))
	} else {
		return sqlbase.NewUndefinedRelationError(&zs.TableOrIndex)
	}
}

func resolveZone(ctx context.Context, txn *client.Txn, zs *tree.ZoneSpecifier) (sqlbase.ID, error) {
	errMissingKey := errors.New("missing key")
	id, err := config.ResolveZoneSpecifier(zs,
		func(parentID uint32, name string) (uint32, error) {
			kv, err := txn.Get(ctx, sqlbase.MakeNameMetadataKey(sqlbase.ID(parentID), name))
			if err != nil {
				return 0, err
			}
			if kv.Value == nil {
				return 0, errMissingKey
			}
			id, err := kv.Value.GetInt()
			if err != nil {
				return 0, err
			}
			return uint32(id), nil
		},
	)
	if err != nil {
		if err == errMissingKey {
			return 0, zoneSpecifierNotFoundError(*zs)
		}
		return 0, err
	}
	return sqlbase.ID(id), nil
}

func resolveSubzone(
	ctx context.Context, txn *client.Txn, zs *tree.ZoneSpecifier, targetID sqlbase.ID,
) (*sqlbase.TableDescriptor, *sqlbase.IndexDescriptor, string, error) {
	if !zs.TargetsTable() {
		return nil, nil, "", nil
	}
	table, err := sqlbase.GetTableDescFromID(ctx, txn, targetID)
	if err != nil {
		return nil, nil, "", err
	}
	if indexName := string(zs.TableOrIndex.Index); indexName != "" {
		index, _, err := table.FindIndexByName(indexName)
		if err != nil {
			return nil, nil, "", err
		}
		return table, &index, "", nil
	} else if partitionName := string(zs.Partition); partitionName != "" {
		_, index, err := table.FindNonDropPartitionByName(partitionName)
		if err != nil {
			return nil, nil, "", err
		}
		zs.TableOrIndex.Index = tree.UnrestrictedName(index.Name)
		return table, index, partitionName, nil
	}
	return table, nil, "", nil
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
	zs tree.ZoneSpecifier, resolvedID, actualID uint32, actualSubzone *config.Subzone,
) (tree.ZoneSpecifier, error) {
	if actualID == keys.RootNamespaceID {
		// We had to traverse to the top of the hierarchy, so we're showing the
		// default zone config.
		zs.NamedZone = config.DefaultZoneName
	} else if resolvedID != actualID {
		// We traversed at least one level up, and we're not at the top of the
		// hierarchy, so we're showing the database zone config.
		tn, err := zs.TableOrIndex.Table.Normalize()
		if err != nil {
			return tree.ZoneSpecifier{}, err
		}
		zs.Database = tn.DatabaseName
	} else if actualSubzone == nil {
		// We didn't find a subzone, so no index or partition zone config exists.
		zs.TableOrIndex.Index = ""
		zs.Partition = ""
	} else if actualSubzone.PartitionName == "" {
		// The resolved subzone did not name a partition, just an index.
		zs.Partition = ""
	}
	return zs, nil
}

// getZoneConfigRaw looks up the zone config with the given ID. Unlike
// getZoneConfig, it does not attempt to ascend the zone config hierarchy. If no
// zone config exists for the given ID, it returns an empty zone config.
func getZoneConfigRaw(
	ctx context.Context, txn *client.Txn, id sqlbase.ID,
) (config.ZoneConfig, error) {
	kv, err := txn.Get(ctx, config.MakeZoneKey(uint32(id)))
	if err != nil {
		return config.ZoneConfig{}, err
	}
	if kv.Value == nil {
		return config.ZoneConfig{}, nil
	}
	var zone config.ZoneConfig
	if err := kv.ValueProto(&zone); err != nil {
		return config.ZoneConfig{}, err
	}
	return zone, nil
}

type setZoneConfigNode struct {
	zoneSpecifier tree.ZoneSpecifier
	yamlConfig    tree.TypedExpr

	run setZoneConfigRun
}

// setZoneConfigRun contains the run-time state of setZoneConfigNode during local execution.
type setZoneConfigRun struct {
	numAffected int
}

func (p *planner) SetZoneConfig(ctx context.Context, n *tree.SetZoneConfig) (planNode, error) {
	yamlConfig, err := p.analyzeExpr(
		ctx, n.YAMLConfig, nil, tree.IndexedVarHelper{}, types.String, false, "configure zone")
	if err != nil {
		return nil, err
	}
	return &setZoneConfigNode{
		zoneSpecifier: n.ZoneSpecifier,
		yamlConfig:    yamlConfig,
	}, nil
}

func (n *setZoneConfigNode) Start(params runParams) error {
	var yamlConfig *string
	datum, err := n.yamlConfig.Eval(params.evalCtx)
	if err != nil {
		return err
	}
	switch val := datum.(type) {
	case *tree.DString:
		yamlConfig = (*string)(val)
	case *tree.DBytes:
		yamlConfig = (*string)(val)
	default:
		if datum != tree.DNull {
			return fmt.Errorf("zone config must be of type string or bytes, not %T", val)
		}
	}

	if n.zoneSpecifier.TargetsIndex() {
		_, err := params.p.expandIndexName(params.ctx, &n.zoneSpecifier.TableOrIndex, true /* requireTable */)
		if err != nil {
			return err
		}
	}

	targetID, err := resolveZone(params.ctx, params.p.txn, &n.zoneSpecifier)
	if err != nil {
		return err
	}
	if targetID != keys.SystemDatabaseID && sqlbase.IsSystemConfigID(targetID) {
		return pgerror.NewErrorf(pgerror.CodeCheckViolationError,
			`cannot set zone configs for system config tables; `+
				`try setting your config on the entire "system" database instead`)
	} else if targetID == keys.RootNamespaceID && yamlConfig == nil {
		return pgerror.NewErrorf(pgerror.CodeCheckViolationError,
			"cannot remove default zone")
	}

	table, index, partition, err := resolveSubzone(params.ctx, params.p.txn,
		&n.zoneSpecifier, targetID)
	if err != nil {
		return err
	}

	_, zone, subzone, err := GetZoneConfigInTxn(params.ctx, params.p.txn,
		uint32(targetID), index, partition)
	if err == errNoZoneConfigApplies {
		// TODO(benesch): This shouldn't be the caller's responsibility;
		// GetZoneConfigInTxn should just return the default zone config if no zone
		// config applies.
		zone = config.DefaultZoneConfig()
	} else if err != nil {
		return err
	}

	if yamlConfig == nil {
		if index != nil {
			didDelete := zone.DeleteSubzone(uint32(index.ID), partition)
			if !didDelete {
				// If we didn't do any work, return early. We'd otherwise perform an
				// update that would make it look like one row was affected.
				return nil
			}
		} else {
			zone.DeleteTableConfig()
		}
	} else {
		newZone := zone
		if subzone != nil {
			newZone = subzone.Config
		}
		if err := yaml.UnmarshalStrict([]byte(*yamlConfig), &newZone); err != nil {
			return fmt.Errorf("could not parse zone config: %s", err)
		}
		if index == nil {
			zone = newZone
		} else {
			// If the zone config for targetID was a subzone placeholder, it'll have
			// been skipped over by GetZoneConfigInTxn. We need to load it regardless
			// to avoid blowing away other subzones.
			zone, err = getZoneConfigRaw(params.ctx, params.p.txn, targetID)
			if err != nil {
				return err
			}
			zone.SetSubzone(config.Subzone{
				IndexID:       uint32(index.ID),
				PartitionName: partition,
				Config:        newZone,
			})
		}
		if err := zone.Validate(); err != nil {
			return fmt.Errorf("could not validate zone config: %s", err)
		}
	}

	if len(zone.Subzones) > 0 {
		zone.SubzoneSpans, err = GenerateSubzoneSpans(table, zone.Subzones)
		if err != nil {
			return err
		}
	}

	internalExecutor := InternalExecutor{LeaseManager: params.p.LeaseMgr()}

	if zone.IsSubzonePlaceholder() && len(zone.Subzones) == 0 {
		n.run.numAffected, err = internalExecutor.ExecuteStatementInTransaction(
			params.ctx, "set zone", params.p.txn,
			"DELETE FROM system.zones WHERE id = $1", targetID)
		return err
	}

	buf, err := protoutil.Marshal(&zone)
	if err != nil {
		return fmt.Errorf("could not marshal zone config: %s", err)
	}
	n.run.numAffected, err = internalExecutor.ExecuteStatementInTransaction(
		params.ctx, "set zone", params.p.txn,
		"UPSERT INTO system.zones (id, config) VALUES ($1, $2)", targetID, buf)
	return err
}

func (n *setZoneConfigNode) Next(runParams) (bool, error) { return false, nil }
func (*setZoneConfigNode) Close(context.Context)          {}
func (n *setZoneConfigNode) Values() tree.Datums          { return nil }
func (n *setZoneConfigNode) FastPathResults() (int, bool) { return n.run.numAffected, true }

type showZoneConfigNode struct {
	optColumnsSlot
	zoneSpecifier tree.ZoneSpecifier

	run showZoneConfigRun
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

func (p *planner) ShowZoneConfig(ctx context.Context, n *tree.ShowZoneConfig) (planNode, error) {
	if n.ZoneSpecifier == (tree.ZoneSpecifier{}) {
		return p.delegateQuery(ctx, "SHOW ZONE CONFIGURATIONS", "TABLE crdb_internal.zones", nil, nil)
	}
	return &showZoneConfigNode{
		zoneSpecifier: n.ZoneSpecifier,
	}, nil
}

func (n *showZoneConfigNode) Start(params runParams) error {
	if n.zoneSpecifier.TargetsIndex() {
		_, err := params.p.expandIndexName(params.ctx, &n.zoneSpecifier.TableOrIndex, true /* requireTable */)
		if err != nil {
			return err
		}
	}

	targetID, err := resolveZone(params.ctx, params.p.txn, &n.zoneSpecifier)
	if err != nil {
		return err
	}

	_, index, partition, err := resolveSubzone(params.ctx, params.p.txn, &n.zoneSpecifier, targetID)
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
	zs, err := ascendZoneSpecifier(n.zoneSpecifier, uint32(targetID), zoneID, subzone)
	if err != nil {
		return err
	}
	n.run.cliSpecifier = config.CLIZoneSpecifier(zs)

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

func (n *showZoneConfigNode) Values() tree.Datums {
	return tree.Datums{
		tree.NewDInt(tree.DInt(n.run.zoneID)),
		tree.NewDString(n.run.cliSpecifier),
		tree.NewDBytes(tree.DBytes(n.run.yamlConfig)),
		tree.NewDBytes(tree.DBytes(n.run.protoConfig)),
	}
}

func (n *showZoneConfigNode) Next(runParams) (bool, error) {
	defer func() { n.run.done = true }()
	return !n.run.done, nil
}

func (*showZoneConfigNode) Close(context.Context) {}
