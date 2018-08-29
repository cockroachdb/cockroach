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
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
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

	table, err := params.p.resolveTableForZone(params.ctx, &n.zoneSpecifier)
	if err != nil {
		return err
	}

	targetID, err := resolveZone(
		params.ctx, params.p.txn, &n.zoneSpecifier)
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

	index, partition, err := resolveSubzone(params.ctx, params.p.txn,
		&n.zoneSpecifier, targetID, table)
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
		if err := validateZoneAttrsAndLocalities(
			params.ctx,
			params.extendedEvalCtx.StatusServer.Nodes,
			*yamlConfig,
		); err != nil {
			return err
		}
	}

	hasNewSubzones := yamlConfig != nil && index != nil
	n.run.numAffected, err = writeZoneConfig(params.ctx, params.p.txn,
		targetID, table, zone, params.extendedEvalCtx.ExecCfg, hasNewSubzones)
	if err != nil {
		return err
	}

	var eventLogType EventLogType
	info := struct {
		Target string
		Config string `json:",omitempty"`
		User   string
	}{
		Target: config.CLIZoneSpecifier(&n.zoneSpecifier),
		User:   params.SessionData().User,
	}
	if yamlConfig == nil {
		eventLogType = EventLogRemoveZoneConfig
	} else {
		eventLogType = EventLogSetZoneConfig
		info.Config = *yamlConfig
	}
	return MakeEventLogger(params.extendedEvalCtx.ExecCfg).InsertEventRecord(
		params.ctx,
		params.p.txn,
		eventLogType,
		int32(targetID),
		int32(params.extendedEvalCtx.NodeID),
		info,
	)
}

func (n *setZoneConfigNode) Next(runParams) (bool, error) { return false, nil }
func (n *setZoneConfigNode) Values() tree.Datums          { return nil }
func (*setZoneConfigNode) Close(context.Context)          {}

func (n *setZoneConfigNode) FastPathResults() (int, bool) { return n.run.numAffected, true }

type nodeGetter func(context.Context, *serverpb.NodesRequest) (*serverpb.NodesResponse, error)

// validateZoneAttrsAndLocalities ensures that all constraints/lease preferences
// specified in the new zone config snippet are actually valid, meaning that
// they match at least one node. This protects against user typos causing
// zone configs that silently don't work as intended.
//
// Note that this really only catches typos in required constraints -- we don't
// want to reject prohibited constraints whose attributes/localities don't
// match any of the current nodes because it's a reasonable use case to add
// prohibited constraints for a new set of nodes before adding the new nodes to
// the cluster. If you had to first add one of the nodes before creating the
// constraints, data could be replicated there that shouldn't be.
func validateZoneAttrsAndLocalities(
	ctx context.Context, getNodes nodeGetter, yamlConfig string,
) error {
	// The caller should have already parsed the yaml once into a pre-populated
	// zone config, so this shouldn't ever fail, but we want to re-parse it so
	// that we only verify newly-set fields, not existing ones.
	var zone config.ZoneConfig
	if err := yaml.UnmarshalStrict([]byte(yamlConfig), &zone); err != nil {
		return fmt.Errorf("could not parse zone config: %s", err)
	}

	if len(zone.Constraints) == 0 && len(zone.LeasePreferences) == 0 {
		return nil
	}

	// Given that we have something to validate, do the work to retrieve the
	// set of attributes and localities present on at least one node.
	nodes, err := getNodes(ctx, &serverpb.NodesRequest{})
	if err != nil {
		return err
	}

	// Accumulate a unique list of constraints to validate.
	toValidate := make([]config.Constraint, 0)
	addToValidate := func(c config.Constraint) {
		var alreadyInList bool
		for _, val := range toValidate {
			if c == val {
				alreadyInList = true
				break
			}
		}
		if !alreadyInList {
			toValidate = append(toValidate, c)
		}
	}
	for _, constraints := range zone.Constraints {
		for _, constraint := range constraints.Constraints {
			addToValidate(constraint)
		}
	}
	for _, leasePreferences := range zone.LeasePreferences {
		for _, constraint := range leasePreferences.Constraints {
			addToValidate(constraint)
		}
	}

	// Check that each constraint matches some store somewhere in the cluster.
	for _, constraint := range toValidate {
		var found bool
	node:
		for _, node := range nodes.Nodes {
			for _, store := range node.StoreStatuses {
				// We could alternatively use config.storeHasConstraint here to catch
				// typos in prohibited constraints as well, but as noted in the
				// function-level comment that could break very reasonable use cases
				// for prohibited constraints.
				if config.StoreMatchesConstraint(store.Desc, constraint) {
					found = true
					break node
				}
			}
		}
		if !found {
			return fmt.Errorf(
				"constraint %q matches no existing nodes within the cluster - did you enter it correctly?",
				constraint)
		}
	}

	return nil
}

func writeZoneConfig(
	ctx context.Context,
	txn *client.Txn,
	targetID sqlbase.ID,
	table *sqlbase.TableDescriptor,
	zone config.ZoneConfig,
	execCfg *ExecutorConfig,
	hasNewSubzones bool,
) (numAffected int, err error) {
	if len(zone.Subzones) > 0 {
		st := execCfg.Settings
		if !st.Version.IsMinSupported(cluster.VersionPartitioning) {
			return 0, errors.New("cluster version does not support zone configs on indexes or partitions")
		}
		zone.SubzoneSpans, err = GenerateSubzoneSpans(
			st, execCfg.ClusterID(), table, zone.Subzones, hasNewSubzones)
		if err != nil {
			return 0, err
		}
	}
	if len(zone.Constraints) > 1 || (len(zone.Constraints) == 1 && zone.Constraints[0].NumReplicas != 0) {
		st := execCfg.Settings
		if !st.Version.IsMinSupported(cluster.VersionPerReplicaZoneConstraints) {
			return 0, errors.New(
				"cluster version does not support zone configs with per-replica constraints")
		}
	}
	if len(zone.LeasePreferences) > 0 {
		st := execCfg.Settings
		if !st.Version.IsMinSupported(cluster.VersionLeasePreferences) {
			return 0, errors.New(
				"cluster version does not support zone configs with lease placement preferences")
		}
	}

	if zone.IsSubzonePlaceholder() && len(zone.Subzones) == 0 {
		return execCfg.InternalExecutor.Exec(ctx, "delete-zone", txn,
			"DELETE FROM system.zones WHERE id = $1", targetID)
	}

	buf, err := protoutil.Marshal(&zone)
	if err != nil {
		return 0, fmt.Errorf("could not marshal zone config: %s", err)
	}
	return execCfg.InternalExecutor.Exec(ctx, "update-zone", txn,
		"UPSERT INTO system.zones (id, config) VALUES ($1, $2)", targetID, buf)
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

func removeIndexZoneConfigs(
	ctx context.Context,
	txn *client.Txn,
	execCfg *ExecutorConfig,
	tableID sqlbase.ID,
	indexDescs []sqlbase.IndexDescriptor,
) error {
	tableDesc, err := sqlbase.GetTableDescFromID(ctx, txn, tableID)
	if err != nil {
		return err
	}

	zone, err := getZoneConfigRaw(ctx, txn, tableID)
	if err != nil {
		return err
	}

	for _, indexDesc := range indexDescs {
		zone.DeleteIndexSubzones(uint32(indexDesc.ID))
	}

	hasNewSubzones := false
	_, err = writeZoneConfig(ctx, txn, tableID, tableDesc, zone, execCfg, hasNewSubzones)
	if sqlbase.IsCCLRequiredError(err) {
		return sqlbase.NewCCLRequiredError(fmt.Errorf("schema change requires a CCL binary "+
			"because table %q has at least one remaining index or partition with a zone config",
			tableDesc.Name))
	}
	return err
}
