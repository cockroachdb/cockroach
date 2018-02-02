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
	"fmt"

	"github.com/pkg/errors"
	yaml "gopkg.in/yaml.v2"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

type setZoneConfigNode struct {
	zoneSpecifier tree.ZoneSpecifier
	yamlConfig    tree.TypedExpr

	run setZoneConfigRun
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

// setZoneConfigRun contains the run-time state of setZoneConfigNode during local execution.
type setZoneConfigRun struct {
	numAffected int
}

func (n *setZoneConfigNode) startExec(params runParams) error {
	var yamlConfig *string
	datum, err := n.yamlConfig.Eval(params.EvalContext())
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

	targetID, err := resolveZone(
		params.ctx, params.p.txn, &n.zoneSpecifier, params.SessionData().Database)
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

	n.run.numAffected, err = writeZoneConfig(params.ctx, params.p.txn,
		targetID, table, zone, params.extendedEvalCtx.ExecCfg)
	return err
}

func (n *setZoneConfigNode) Next(runParams) (bool, error) { return false, nil }
func (n *setZoneConfigNode) Values() tree.Datums          { return nil }
func (*setZoneConfigNode) Close(context.Context)          {}

func (n *setZoneConfigNode) FastPathResults() (int, bool) { return n.run.numAffected, true }

func writeZoneConfig(
	ctx context.Context,
	txn *client.Txn,
	targetID sqlbase.ID,
	table *sqlbase.TableDescriptor,
	zone config.ZoneConfig,
	execCfg *ExecutorConfig,
) (numAffected int, err error) {
	if len(zone.Subzones) > 0 {
		if !execCfg.Settings.Version.IsMinSupported(cluster.VersionPartitioning) {
			return 0, errors.New("cluster version does not support zone configs on indexes or partitions")
		}
		zone.SubzoneSpans, err = GenerateSubzoneSpans(table, zone.Subzones)
		if err != nil {
			return 0, err
		}
	}

	internalExecutor := InternalExecutor{ExecCfg: execCfg}

	if zone.IsSubzonePlaceholder() && len(zone.Subzones) == 0 {
		return internalExecutor.ExecuteStatementInTransaction(ctx, "set zone", txn,
			"DELETE FROM system.public.zones WHERE id = $1", targetID)
	}

	buf, err := protoutil.Marshal(&zone)
	if err != nil {
		return 0, fmt.Errorf("could not marshal zone config: %s", err)
	}
	return internalExecutor.ExecuteStatementInTransaction(ctx, "set zone", txn,
		"UPSERT INTO system.public.zones (id, config) VALUES ($1, $2)", targetID, buf)
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
