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
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
	yaml "gopkg.in/yaml.v2"
)

type optionValue struct {
	inheritValue  bool
	explicitValue tree.TypedExpr
}

type setZoneConfigNode struct {
	zoneSpecifier tree.ZoneSpecifier
	yamlConfig    tree.TypedExpr
	options       map[tree.Name]optionValue
	setDefault    bool

	run setZoneConfigRun
}

// supportedZoneConfigOptions indicates how to translate SQL variable
// assignments in ALTER CONFIGURE ZONE to assignments to the member
// fields of config.ZoneConfig.
var supportedZoneConfigOptions = map[tree.Name]struct {
	requiredType *types.T
	setter       func(*config.ZoneConfig, tree.Datum)
}{
	"range_min_bytes": {types.Int, func(c *config.ZoneConfig, d tree.Datum) { c.RangeMinBytes = proto.Int64(int64(tree.MustBeDInt(d))) }},
	"range_max_bytes": {types.Int, func(c *config.ZoneConfig, d tree.Datum) { c.RangeMaxBytes = proto.Int64(int64(tree.MustBeDInt(d))) }},
	"num_replicas":    {types.Int, func(c *config.ZoneConfig, d tree.Datum) { c.NumReplicas = proto.Int32(int32(tree.MustBeDInt(d))) }},
	"gc.ttlseconds": {types.Int, func(c *config.ZoneConfig, d tree.Datum) {
		c.GC = &config.GCPolicy{TTLSeconds: int32(tree.MustBeDInt(d))}
	}},
	"constraints": {types.String, func(c *config.ZoneConfig, d tree.Datum) {
		constraintsList := config.ConstraintsList{
			Constraints: c.Constraints,
			Inherited:   c.InheritedConstraints,
		}
		loadYAML(&constraintsList, string(tree.MustBeDString(d)))
		c.Constraints = constraintsList.Constraints
		c.InheritedConstraints = false
	}},
	"lease_preferences": {types.String, func(c *config.ZoneConfig, d tree.Datum) {
		loadYAML(&c.LeasePreferences, string(tree.MustBeDString(d)))
		c.InheritedLeasePreferences = false
	}},
}

// zoneOptionKeys contains the keys from suportedZoneConfigOptions in
// deterministic order. Needed to make the event log output
// deterministic.
var zoneOptionKeys = func() []string {
	l := make([]string, 0, len(supportedZoneConfigOptions))
	for k := range supportedZoneConfigOptions {
		l = append(l, string(k))
	}
	sort.Strings(l)
	return l
}()

func loadYAML(dst interface{}, yamlString string) {
	if err := yaml.UnmarshalStrict([]byte(yamlString), dst); err != nil {
		panic(err)
	}
}

func (p *planner) SetZoneConfig(ctx context.Context, n *tree.SetZoneConfig) (planNode, error) {
	var yamlConfig tree.TypedExpr

	if n.YAMLConfig != nil {
		// We have a CONFIGURE ZONE = <expr> assignment.
		// This can be either a literal NULL (deletion), or a string containing YAML.
		// We also support byte arrays for backward compatibility with
		// previous versions of CockroachDB.

		var err error
		yamlConfig, err = p.analyzeExpr(
			ctx, n.YAMLConfig, nil, tree.IndexedVarHelper{}, types.String, false /*requireType*/, "configure zone")
		if err != nil {
			return nil, err
		}

		switch typ := yamlConfig.ResolvedType(); typ.Family() {
		case types.UnknownFamily:
			// Unknown occurs if the user entered a literal NULL. That's OK and will mean deletion.
		case types.StringFamily:
		case types.BytesFamily:
		default:
			return nil, pgerror.Newf(pgcode.InvalidParameterValue,
				"zone config must be of type string or bytes, not %s", typ)
		}
	}

	var options map[tree.Name]optionValue
	if n.Options != nil {
		// We have a CONFIGURE ZONE USING ... assignment.
		// Here we are constrained by the supported ZoneConfig fields,
		// as described by supportedZoneConfigOptions above.

		options = make(map[tree.Name]optionValue)
		for _, opt := range n.Options {
			if _, alreadyExists := options[opt.Key]; alreadyExists {
				return nil, pgerror.Newf(pgcode.InvalidParameterValue,
					"duplicate zone config parameter: %q", tree.ErrString(&opt.Key))
			}
			req, ok := supportedZoneConfigOptions[opt.Key]
			if !ok {
				return nil, pgerror.Newf(pgcode.InvalidParameterValue,
					"unsupported zone config parameter: %q", tree.ErrString(&opt.Key))
			}
			if opt.Value == nil {
				options[opt.Key] = optionValue{inheritValue: true, explicitValue: nil}
				continue
			}
			valExpr, err := p.analyzeExpr(
				ctx, opt.Value, nil, tree.IndexedVarHelper{}, req.requiredType, true /*requireType*/, string(opt.Key))
			if err != nil {
				return nil, err
			}
			options[opt.Key] = optionValue{inheritValue: false, explicitValue: valExpr}
		}
	}

	return &setZoneConfigNode{
		zoneSpecifier: n.ZoneSpecifier,
		yamlConfig:    yamlConfig,
		options:       options,
		setDefault:    n.SetDefault,
	}, nil
}

// setZoneConfigRun contains the run-time state of setZoneConfigNode during local execution.
type setZoneConfigRun struct {
	numAffected int
}

func (n *setZoneConfigNode) startExec(params runParams) error {
	var yamlConfig string
	var setters []func(c *config.ZoneConfig)
	deleteZone := false
	subzonePlaceholder := false

	// Evaluate the configuration input.
	if n.yamlConfig != nil {
		// From a YAML string.
		datum, err := n.yamlConfig.Eval(params.EvalContext())
		if err != nil {
			return err
		}
		switch val := datum.(type) {
		case *tree.DString:
			yamlConfig = string(*val)
		case *tree.DBytes:
			yamlConfig = string(*val)
		default:
			deleteZone = true
		}
		// Trim spaces, to detect empty zonfigurations.
		// We'll add back the missing newline below.
		yamlConfig = strings.TrimSpace(yamlConfig)
	}
	var optionStr strings.Builder
	var copyFromParentList []tree.Name
	if n.options != nil {
		// Set from var = value attributes.
		//
		// We iterate over zoneOptionKeys instead of iterating over
		// n.options directly so that the optionStr string constructed for
		// the event log remains deterministic.
		for i := range zoneOptionKeys {
			name := (*tree.Name)(&zoneOptionKeys[i])
			val, ok := n.options[*name]
			if !ok {
				continue
			}
			// We don't add the setters for the fields that will copy values
			// from the parents. These fields will be set by taking what
			// value would apply to the zone and setting that value explicitly.
			// Instead we add the fields to a list that we use at a later time
			// to copy values over.
			inheritVal, expr := val.inheritValue, val.explicitValue
			if inheritVal {
				copyFromParentList = append(copyFromParentList, *name)
				if optionStr.Len() > 0 {
					optionStr.WriteString(", ")
				}
				fmt.Fprintf(&optionStr, "%s = COPY FROM PARENT", name)
				continue
			}
			datum, err := expr.Eval(params.EvalContext())
			if err != nil {
				return err
			}
			if datum == tree.DNull {
				return pgerror.Newf(pgcode.InvalidParameterValue,
					"unsupported NULL value for %q", tree.ErrString(name))
			}
			setter := supportedZoneConfigOptions[*name].setter
			setters = append(setters, func(c *config.ZoneConfig) { setter(c, datum) })
			if optionStr.Len() > 0 {
				optionStr.WriteString(", ")
			}
			fmt.Fprintf(&optionStr, "%s = %s", name, datum)

		}
	}

	// If the specifier is for a table, partition or index, this will
	// resolve the table descriptor. If the specifier is for a database
	// or range, this is a no-op and a nil pointer is returned as
	// descriptor.
	table, err := params.p.resolveTableForZone(params.ctx, &n.zoneSpecifier)
	if err != nil {
		return err
	}

	// resolveZone determines the ID of the target object of the zone
	// specifier. This ought to succeed regardless of whether there is
	// already a zone config for the target object.
	targetID, err := resolveZone(
		params.ctx, params.p.txn, &n.zoneSpecifier)
	if err != nil {
		return err
	}
	if targetID != keys.SystemDatabaseID && sqlbase.IsSystemConfigID(targetID) {
		return pgerror.Newf(pgcode.CheckViolation,
			`cannot set zone configs for system config tables; `+
				`try setting your config on the entire "system" database instead`)
	} else if targetID == keys.RootNamespaceID && deleteZone {
		return pgerror.Newf(pgcode.CheckViolation,
			"cannot remove default zone")
	}

	// resolveSubzone determines the sub-parts of the zone
	// specifier. This ought to succeed regardless of whether there is
	// already a zone config.
	index, partition, err := resolveSubzone(&n.zoneSpecifier, table)
	if err != nil {
		return err
	}

	// Retrieve the partial zone configuration
	partialZone, err := getZoneConfigRaw(params.ctx, params.p.txn, targetID)
	if err != nil {
		return err
	}

	// No zone was found. Possibly a SubzonePlaceholder depending on the index.
	if partialZone == nil {
		partialZone = config.NewZoneConfig()
		if index != nil {
			subzonePlaceholder = true
		}
	}

	var partialSubzone *config.Subzone
	if index != nil {
		partialSubzone = partialZone.GetSubzone(uint32(index.ID), partition)
		if partialSubzone == nil {
			partialSubzone = &config.Subzone{Config: *config.NewZoneConfig()}
		}
	}

	// Retrieve the zone configuration.
	//
	// If the statement was USING DEFAULT, we want to ignore the zone
	// config that exists on targetID and instead skip to the inherited
	// default (whichever applies -- a database if targetID is a table,
	// default if targetID is a database, etc.). For this, we use the last
	// parameter getInheritedDefault to GetZoneConfigInTxn().
	// These zones are only used for validations. The merged zone is will
	// not be written.
	_, completeZone, completeSubzone, err := GetZoneConfigInTxn(params.ctx, params.p.txn,
		uint32(targetID), index, partition, n.setDefault)

	if err == errNoZoneConfigApplies {
		// No zone config yet.
		//
		// GetZoneConfigInTxn will fail with errNoZoneConfigApplies when
		// the target ID is not a database object, i.e. one of the system
		// ranges (liveness, meta, etc.), and did not have a zone config
		// already.
		completeZone = protoutil.Clone(params.extendedEvalCtx.ExecCfg.DefaultZoneConfig).(*config.ZoneConfig)
	} else if err != nil {
		return err
	}

	// Copy the fields set by the INHERIT field command.
	partialZone.CopyFromZone(*completeZone, copyFromParentList)

	if deleteZone {
		if index != nil {
			didDelete := completeZone.DeleteSubzone(uint32(index.ID), partition)
			_ = partialZone.DeleteSubzone(uint32(index.ID), partition)
			if !didDelete {
				// If we didn't do any work, return early. We'd otherwise perform an
				// update that would make it look like one row was affected.
				return nil
			}
		} else {
			completeZone.DeleteTableConfig()
			partialZone.DeleteTableConfig()
		}
	} else {
		// Validate the user input.
		if len(yamlConfig) == 0 || yamlConfig[len(yamlConfig)-1] != '\n' {
			// YAML values must always end with a newline character. If there is none,
			// for UX convenience add one.
			yamlConfig += "\n"
		}

		// Determine where to load the configuration.
		newZone := *completeZone
		if completeSubzone != nil {
			newZone = completeSubzone.Config
		}

		// Determine where to load the partial configuration.
		// finalZone is where the new changes are unmarshalled onto.
		// It must be a fresh ZoneConfig if a new subzone is being created.
		// If an existing subzone is being modified, finalZone is overridden.
		finalZone := *partialZone
		if partialSubzone != nil {
			finalZone = partialSubzone.Config
		}

		// ALTER RANGE default USING DEFAULT sets the default to the in
		// memory default value.
		if n.setDefault && keys.RootNamespaceID == uint32(targetID) {
			finalZone = *protoutil.Clone(params.extendedEvalCtx.ExecCfg.DefaultZoneConfig).(*config.ZoneConfig)
		} else if n.setDefault {
			finalZone = *config.NewZoneConfig()
		}
		// Load settings from YAML. If there was no YAML (e.g. because the
		// query specified CONFIGURE ZONE USING), the YAML string will be
		// empty, in which case the unmarshaling will be a no-op. This is
		// innocuous.
		if err := yaml.UnmarshalStrict([]byte(yamlConfig), &newZone); err != nil {
			return pgerror.Newf(pgcode.CheckViolation,
				"could not parse zone config: %v", err)
		}

		// Load settings from YAML into the partial zone as well.
		if err := yaml.UnmarshalStrict([]byte(yamlConfig), &finalZone); err != nil {
			return pgerror.Newf(pgcode.CheckViolation,
				"could not parse zone config: %v", err)
		}

		// Load settings from var = val assignments. If there were no such
		// settings, (e.g. because the query specified CONFIGURE ZONE = or
		// USING DEFAULT), the setter slice will be empty and this will be
		// a no-op. This is innocuous.
		for _, setter := range setters {
			// A setter may fail with an error-via-panic. Catch those.
			if err := func() (err error) {
				defer func() {
					if p := recover(); p != nil {
						if errP, ok := p.(error); ok {
							// Catch and return the error.
							err = errP
						} else {
							// Nothing we know about, let it continue as a panic.
							panic(p)
						}
					}
				}()

				setter(&newZone)
				setter(&finalZone)
				return nil
			}(); err != nil {
				return err
			}
		}

		// Validate that there are no conflicts in the zone setup.
		if err := validateNoRepeatKeysInZone(&newZone); err != nil {
			return err
		}

		// Validate that the result makes sense.
		if err := validateZoneAttrsAndLocalities(
			params.ctx,
			params.extendedEvalCtx.StatusServer.Nodes,
			&newZone,
		); err != nil {
			return err
		}

		// Are we operating on an index?
		if index == nil {
			// No: the final zone config is the one we just processed.
			completeZone = &newZone
			partialZone = &finalZone
		} else {
			// If the zone config for targetID was a subzone placeholder, it'll have
			// been skipped over by GetZoneConfigInTxn. We need to load it regardless
			// to avoid blowing away other subzones.

			// TODO(ridwanmsharif): How is this supposed to change? getZoneConfigRaw
			// gives no guarantees about completeness. Some work might need to happen
			// here to complete the missing fields. The reason is because we don't know
			// here if a zone is a placeholder or not. Can we do a GetConfigInTxn here?
			// And if it is a placeholder, we use getZoneConfigRaw to create one.
			completeZone, err = getZoneConfigRaw(params.ctx, params.p.txn, targetID)
			if err != nil {
				return err
			} else if completeZone == nil {
				completeZone = config.NewZoneConfig()
			}
			completeZone.SetSubzone(config.Subzone{
				IndexID:       uint32(index.ID),
				PartitionName: partition,
				Config:        newZone,
			})

			// The partial zone might just be empty. If so,
			// replace it with a SubzonePlaceholder.
			if subzonePlaceholder {
				partialZone.DeleteTableConfig()
			}

			partialZone.SetSubzone(config.Subzone{
				IndexID:       uint32(index.ID),
				PartitionName: partition,
				Config:        finalZone,
			})
		}

		// Finally revalidate everything. Validate only the completeZone config.
		if err := completeZone.Validate(); err != nil {
			return pgerror.Newf(pgcode.CheckViolation,
				"could not validate zone config: %v", err)
		}
	}

	// Write the partial zone configuration.
	hasNewSubzones := !deleteZone && index != nil
	execConfig := params.extendedEvalCtx.ExecCfg
	zoneToWrite := partialZone

	// Finally check for the extra protection partial zone configs would
	// require from changes made to parent zones. The extra protections are:
	//
	// RangeMinBytes and RangeMaxBytes must be set together
	// LeasePreferences cannot be set unless Constraints are explicitly set
	// Per-replica constraints cannot be set unless num_replicas is explicitly set
	if err := zoneToWrite.ValidateTandemFields(); err != nil {
		err = errors.Wrap(err, "could not validate zone config")
		err = pgerror.WithCandidateCode(err, pgcode.InvalidParameterValue)
		err = errors.WithHint(err,
			"try ALTER ... CONFIGURE ZONE USING <field_name> = COPY FROM PARENT [, ...] so populate the field")
		return err
	}
	n.run.numAffected, err = writeZoneConfig(params.ctx, params.p.txn,
		targetID, table, zoneToWrite, execConfig, hasNewSubzones)
	if err != nil {
		return err
	}

	// Record that the change has occurred for auditing.
	var eventLogType EventLogType
	info := struct {
		Target  string
		Config  string `json:",omitempty"`
		Options string `json:",omitempty"`
		User    string
	}{
		Target:  config.CLIZoneSpecifier(&n.zoneSpecifier),
		Config:  strings.TrimSpace(yamlConfig),
		Options: optionStr.String(),
		User:    params.SessionData().User,
	}
	if deleteZone {
		eventLogType = EventLogRemoveZoneConfig
	} else {
		eventLogType = EventLogSetZoneConfig
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

// Check that there are not duplicated values for a particular
// constraint. For example, constraints [+region=us-east1,+region=us-east2]
// will be rejected. Additionally, invalid constraints such as
// [+region=us-east1, -region=us-east1] will also be rejected.
func validateNoRepeatKeysInZone(zone *config.ZoneConfig) error {
	for _, constraints := range zone.Constraints {
		// Because we expect to have a small number of constraints, a nested
		// loop is probably better than allocating a map.
		for i, curr := range constraints.Constraints {
			for _, other := range constraints.Constraints[i+1:] {
				if curr.Type == config.Constraint_REQUIRED {
					// Verify that there is not another +k=v pair where k=curr.Key, and
					// that -k=v does not exist.
					if other.Type == config.Constraint_REQUIRED && other.Key == curr.Key ||
						other.Type == config.Constraint_PROHIBITED && other.Key == curr.Key && other.Value == curr.Value {
						return pgerror.Newf(pgcode.CheckViolation,
							"incompatible zone constraints: %q and %q", curr, other)
					}
				} else if curr.Type == config.Constraint_PROHIBITED {
					// If we have a -k=v pair, verify that there are not any
					// +k=v pairs in the constraints.
					if other.Type == config.Constraint_REQUIRED && other.Key == curr.Key && other.Value == curr.Value {
						return pgerror.Newf(pgcode.CheckViolation,
							"incompatible zone constraints: %q and %q", curr, other)
					}
				}
			}
		}
	}
	return nil
}

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
	ctx context.Context, getNodes nodeGetter, zone *config.ZoneConfig,
) error {
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
			return pgerror.Newf(pgcode.CheckViolation,
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
	zone *config.ZoneConfig,
	execCfg *ExecutorConfig,
	hasNewSubzones bool,
) (numAffected int, err error) {
	if len(zone.Subzones) > 0 {
		st := execCfg.Settings
		zone.SubzoneSpans, err = GenerateSubzoneSpans(
			st, execCfg.ClusterID(), table, zone.Subzones, hasNewSubzones)
		if err != nil {
			return 0, err
		}
	} else {
		// To keep the Subzone and SubzoneSpan arrays consistent
		zone.SubzoneSpans = nil
	}

	if zone.IsSubzonePlaceholder() && len(zone.Subzones) == 0 {
		return execCfg.InternalExecutor.Exec(ctx, "delete-zone", txn,
			"DELETE FROM system.zones WHERE id = $1", targetID)
	}

	buf, err := protoutil.Marshal(zone)
	if err != nil {
		return 0, pgerror.Newf(pgcode.CheckViolation,
			"could not marshal zone config: %v", err)
	}
	return execCfg.InternalExecutor.Exec(ctx, "update-zone", txn,
		"UPSERT INTO system.zones (id, config) VALUES ($1, $2)", targetID, buf)
}

// getZoneConfigRaw looks up the zone config with the given ID. Unlike
// getZoneConfig, it does not attempt to ascend the zone config hierarchy. If no
// zone config exists for the given ID, it returns nil.
func getZoneConfigRaw(
	ctx context.Context, txn *client.Txn, id sqlbase.ID,
) (*config.ZoneConfig, error) {
	kv, err := txn.Get(ctx, config.MakeZoneKey(uint32(id)))
	if err != nil {
		return nil, err
	}
	if kv.Value == nil {
		return nil, nil
	}
	var zone config.ZoneConfig
	if err := kv.ValueProto(&zone); err != nil {
		return nil, err
	}
	return &zone, nil
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
	} else if zone == nil {
		zone = config.NewZoneConfig()
	}

	for _, indexDesc := range indexDescs {
		zone.DeleteIndexSubzones(uint32(indexDesc.ID))
	}

	// Ignore CCL required error to allow schema change to progress.
	_, err = writeZoneConfig(ctx, txn, tableID, tableDesc, zone, execCfg, false /* hasNewSubzones */)
	if err != nil && !sqlbase.IsCCLRequiredError(err) {
		return err
	}
	return nil
}
