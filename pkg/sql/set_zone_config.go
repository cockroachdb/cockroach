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

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/zone"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
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
	allIndexes    bool
	yamlConfig    tree.TypedExpr
	options       map[tree.Name]optionValue
	setDefault    bool

	run setZoneConfigRun
}

// supportedZoneConfigOptions indicates how to translate SQL variable
// assignments in ALTER CONFIGURE ZONE to assignments to the member
// fields of zonepb.ZoneConfig.
var supportedZoneConfigOptions = map[tree.Name]struct {
	requiredType *types.T
	setter       func(*zonepb.ZoneConfig, tree.Datum)
	checkAllowed func(context.Context, *ExecutorConfig, tree.Datum) error // optional
}{
	"range_min_bytes": {
		requiredType: types.Int,
		setter:       func(c *zonepb.ZoneConfig, d tree.Datum) { c.RangeMinBytes = proto.Int64(int64(tree.MustBeDInt(d))) },
	},
	"range_max_bytes": {
		requiredType: types.Int,
		setter:       func(c *zonepb.ZoneConfig, d tree.Datum) { c.RangeMaxBytes = proto.Int64(int64(tree.MustBeDInt(d))) },
	},
	"global_reads": {
		requiredType: types.Bool,
		setter:       func(c *zonepb.ZoneConfig, d tree.Datum) { c.GlobalReads = proto.Bool(bool(tree.MustBeDBool(d))) },
		checkAllowed: func(ctx context.Context, execCfg *ExecutorConfig, d tree.Datum) error {
			if !tree.MustBeDBool(d) {
				// Always allow the value to be unset.
				return nil
			}
			return base.CheckEnterpriseEnabled(
				execCfg.Settings,
				execCfg.NodeInfo.LogicalClusterID(),
				"global_reads",
			)
		},
	},
	"num_replicas": {
		requiredType: types.Int,
		setter:       func(c *zonepb.ZoneConfig, d tree.Datum) { c.NumReplicas = proto.Int32(int32(tree.MustBeDInt(d))) },
	},
	"num_voters": {
		requiredType: types.Int,
		setter:       func(c *zonepb.ZoneConfig, d tree.Datum) { c.NumVoters = proto.Int32(int32(tree.MustBeDInt(d))) },
	},
	"gc.ttlseconds": {
		requiredType: types.Int,
		setter: func(c *zonepb.ZoneConfig, d tree.Datum) {
			c.GC = &zonepb.GCPolicy{TTLSeconds: int32(tree.MustBeDInt(d))}
		},
	},
	"constraints": {
		requiredType: types.String,
		setter: func(c *zonepb.ZoneConfig, d tree.Datum) {
			constraintsList := zonepb.ConstraintsList{
				Constraints: c.Constraints,
				Inherited:   c.InheritedConstraints,
			}
			loadYAML(&constraintsList, string(tree.MustBeDString(d)))
			c.Constraints = constraintsList.Constraints
			c.InheritedConstraints = false
		},
	},
	"voter_constraints": {
		requiredType: types.String,
		setter: func(c *zonepb.ZoneConfig, d tree.Datum) {
			voterConstraintsList := zonepb.ConstraintsList{
				Constraints: c.VoterConstraints,
				Inherited:   c.InheritedVoterConstraints(),
			}
			loadYAML(&voterConstraintsList, string(tree.MustBeDString(d)))
			c.VoterConstraints = voterConstraintsList.Constraints
			c.NullVoterConstraintsIsEmpty = true
		},
	},
	"lease_preferences": {
		requiredType: types.String,
		setter: func(c *zonepb.ZoneConfig, d tree.Datum) {
			loadYAML(&c.LeasePreferences, string(tree.MustBeDString(d)))
			c.InheritedLeasePreferences = false
		},
	},
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

func (p *planner) getUpdatedZoneConfigYamlConfig(
	ctx context.Context, n tree.Expr,
) (tree.TypedExpr, error) {
	var yamlConfig tree.TypedExpr

	if n != nil {
		// We have a CONFIGURE ZONE = <expr> assignment.
		// This can be either a literal NULL (deletion), or a string containing YAML.
		// We also support byte arrays for backward compatibility with
		// previous versions of CockroachDB.

		var err error
		yamlConfig, err = p.analyzeExpr(
			ctx, n, nil, tree.IndexedVarHelper{}, types.String, false /*requireType*/, "configure zone")
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
	return yamlConfig, nil
}

func (p *planner) getUpdatedZoneConfigOptions(
	ctx context.Context, n tree.KVOptions, telemetryName string,
) (map[tree.Name]optionValue, error) {

	var options map[tree.Name]optionValue
	if n != nil {
		// We have a CONFIGURE ZONE USING ... assignment.
		// Here we are constrained by the supported ZoneConfig fields,
		// as described by supportedZoneConfigOptions above.

		options = make(map[tree.Name]optionValue)
		for _, opt := range n {
			if _, alreadyExists := options[opt.Key]; alreadyExists {
				return nil, pgerror.Newf(pgcode.InvalidParameterValue,
					"duplicate zone config parameter: %q", tree.ErrString(&opt.Key))
			}
			req, ok := supportedZoneConfigOptions[opt.Key]
			if !ok {
				return nil, pgerror.Newf(pgcode.InvalidParameterValue,
					"unsupported zone config parameter: %q", tree.ErrString(&opt.Key))
			}
			telemetry.Inc(
				sqltelemetry.SchemaSetZoneConfigCounter(
					telemetryName,
					string(opt.Key),
				),
			)
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
	return options, nil
}

func (p *planner) SetZoneConfig(ctx context.Context, n *tree.SetZoneConfig) (planNode, error) {

	execCfg := p.ExecCfg()
	if err := checkSchemaChangeEnabled(
		ctx,
		execCfg,
		"CONFIGURE ZONE",
	); err != nil {
		return nil, err
	}

	if err := execCfg.RequireSystemTenantOrClusterSetting(SecondaryTenantZoneConfigsEnabled); err != nil {
		// Return an unimplemented error here instead of referencing the cluster
		// setting here as zone configurations for secondary tenants are intended to
		// be hidden.
		return nil, errorutil.UnsupportedWithMultiTenancy(MultitenancyZoneCfgIssueNo)
	}

	if err := checkPrivilegeForSetZoneConfig(ctx, p, n.ZoneSpecifier); err != nil {
		return nil, err
	}

	if err := p.CheckZoneConfigChangePermittedForMultiRegion(
		ctx,
		n.ZoneSpecifier,
		n.Options,
	); err != nil {
		return nil, err
	}

	yamlConfig, err := p.getUpdatedZoneConfigYamlConfig(ctx, n.YAMLConfig)
	if err != nil {
		return nil, err
	}

	options, err := p.getUpdatedZoneConfigOptions(ctx, n.Options, n.ZoneSpecifier.TelemetryName())
	if err != nil {
		return nil, err
	}

	return &setZoneConfigNode{
		zoneSpecifier: n.ZoneSpecifier,
		allIndexes:    n.AllIndexes,
		yamlConfig:    yamlConfig,
		options:       options,
		setDefault:    n.SetDefault,
	}, nil
}

func checkPrivilegeForSetZoneConfig(ctx context.Context, p *planner, zs tree.ZoneSpecifier) error {
	// For system ranges, the system database, or system tables, the user must be
	// an admin. Otherwise we require CREATE privileges on the database or table
	// in question.
	if zs.NamedZone != "" {
		return p.RequireAdminRole(ctx, "alter system ranges")
	}
	if zs.Database != "" {
		if zs.Database == "system" {
			return p.RequireAdminRole(ctx, "alter the system database")
		}
		dbDesc, err := p.Descriptors().ByNameWithLeased(p.txn).Get().Database(ctx, string(zs.Database))
		if err != nil {
			return err
		}
		dbCreatePrivilegeErr := p.CheckPrivilege(ctx, dbDesc, privilege.CREATE)
		dbZoneConfigPrivilegeErr := p.CheckPrivilege(ctx, dbDesc, privilege.ZONECONFIG)

		// Can set ZoneConfig if user has either CREATE privilege or ZONECONFIG privilege at the Database level
		if dbZoneConfigPrivilegeErr == nil || dbCreatePrivilegeErr == nil {
			return nil
		}

		return pgerror.Newf(pgcode.InsufficientPrivilege,
			"user %s does not have %s or %s privilege on %s %s",
			p.SessionData().User(), privilege.ZONECONFIG, privilege.CREATE, dbDesc.DescriptorType(), dbDesc.GetName())
	}
	tableDesc, err := p.resolveTableForZone(ctx, &zs)
	if err != nil {
		if zs.TargetsIndex() && zs.TableOrIndex.Table.ObjectName == "" {
			err = errors.WithHint(err, "try specifying the index as <tablename>@<indexname>")
		}
		return err
	}
	if tableDesc.GetParentID() == keys.SystemDatabaseID {
		return p.RequireAdminRole(ctx, "alter system tables")
	}

	// Can set ZoneConfig if user has either CREATE privilege or ZONECONFIG privilege at the Table level
	tableCreatePrivilegeErr := p.CheckPrivilege(ctx, tableDesc, privilege.CREATE)
	tableZoneConfigPrivilegeErr := p.CheckPrivilege(ctx, tableDesc, privilege.ZONECONFIG)

	if tableCreatePrivilegeErr == nil || tableZoneConfigPrivilegeErr == nil {
		return nil
	}

	return pgerror.Newf(pgcode.InsufficientPrivilege,
		"user %s does not have %s or %s privilege on %s %s",
		p.SessionData().User(), privilege.ZONECONFIG, privilege.CREATE, tableDesc.DescriptorType(), tableDesc.GetName())
}

// setZoneConfigRun contains the run-time state of setZoneConfigNode during local execution.
type setZoneConfigRun struct {
	numAffected int
}

// ReadingOwnWrites implements the planNodeReadingOwnWrites interface.
// This is because CONFIGURE ZONE performs multiple KV operations on descriptors
// and expects to see its own writes.
func (n *setZoneConfigNode) ReadingOwnWrites() {}

func evaluateYAMLConfig(expr tree.TypedExpr, params runParams) (string, bool, error) {
	var yamlConfig string
	deleteZone := false
	if expr != nil {
		datum, err := eval.Expr(params.ctx, params.EvalContext(), expr)
		if err != nil {
			return "", false, err
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
	return yamlConfig, deleteZone, nil
}

func evaluateZoneOptions(
	options map[tree.Name]optionValue, params runParams,
) (
	optionsStr []string,
	copyFromParentList []tree.Name,
	setters []func(c *zonepb.ZoneConfig),
	err error,
) {
	if options != nil {
		// Set from var = value attributes.
		//
		// We iterate over zoneOptionKeys instead of iterating over
		// n.options directly so that the optionStr string constructed for
		// the event log remains deterministic.
		for i := range zoneOptionKeys {
			name := (*tree.Name)(&zoneOptionKeys[i])
			val, ok := options[*name]
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
				optionsStr = append(optionsStr, fmt.Sprintf("%s = COPY FROM PARENT", name))
				continue
			}
			datum, err := eval.Expr(params.ctx, params.EvalContext(), expr)
			if err != nil {
				return nil, nil, nil, err
			}
			if datum == tree.DNull {
				return nil, nil, nil, pgerror.Newf(pgcode.InvalidParameterValue,
					"unsupported NULL value for %q", tree.ErrString(name))
			}
			opt := supportedZoneConfigOptions[*name]
			if opt.checkAllowed != nil {
				if err := opt.checkAllowed(params.ctx, params.ExecCfg(), datum); err != nil {
					return nil, nil, nil, err
				}
			}
			setter := opt.setter
			setters = append(setters, func(c *zonepb.ZoneConfig) { setter(c, datum) })
			optionsStr = append(optionsStr, fmt.Sprintf("%s = %s", name, datum))
		}
	}
	return optionsStr, copyFromParentList, setters, nil
}

func (n *setZoneConfigNode) startExec(params runParams) error {
	yamlConfig, deleteZone, err := evaluateYAMLConfig(n.yamlConfig, params)
	if err != nil {
		return err
	}

	optionsStr, copyFromParentList, setters, err := evaluateZoneOptions(n.options, params)
	if err != nil {
		return err
	}

	telemetry.Inc(
		sqltelemetry.SchemaChangeAlterCounterWithExtra(n.zoneSpecifier.TelemetryName(), "configure_zone"),
	)

	// If the specifier is for a table, partition or index, this will
	// resolve the table descriptor. If the specifier is for a database
	// or range, this is a no-op and a nil pointer is returned as
	// descriptor.
	table, err := params.p.resolveTableForZone(params.ctx, &n.zoneSpecifier)
	if err != nil {
		return err
	}

	// If the table descriptor is resolved but is not a
	// physical table then return an error.
	if table != nil && !table.IsPhysicalTable() {
		return pgerror.Newf(pgcode.WrongObjectType, "cannot set a zone configuration on non-physical object %s", table.GetName())
	}

	if n.zoneSpecifier.TargetsPartition() && len(n.zoneSpecifier.TableOrIndex.Index) == 0 && !n.allIndexes {
		// Backward compatibility for ALTER PARTITION ... OF TABLE. Determine which
		// index has the specified partition.
		partitionName := string(n.zoneSpecifier.Partition)

		var indexes []catalog.Index
		for _, idx := range table.NonDropIndexes() {
			if idx.GetPartitioning().FindPartitionByName(partitionName) != nil {
				indexes = append(indexes, idx)
			}
		}

		switch len(indexes) {
		case 0:
			return fmt.Errorf("partition %q does not exist on table %q", partitionName, table.GetName())
		case 1:
			n.zoneSpecifier.TableOrIndex.Index = tree.UnrestrictedName(indexes[0].GetName())
		case 2:
			// Temporary indexes create during backfill should always share the same
			// zone configs as the corresponding new index.
			if catalog.IsCorrespondingTemporaryIndex(indexes[1], indexes[0]) {
				n.zoneSpecifier.TableOrIndex.Index = tree.UnrestrictedName(indexes[0].GetName())
				break
			}
			fallthrough
		default:
			err := fmt.Errorf(
				"partition %q exists on multiple indexes of table %q", partitionName, table.GetName())
			err = pgerror.WithCandidateCode(err, pgcode.InvalidParameterValue)
			err = errors.WithHint(err, "try ALTER PARTITION ... OF INDEX ...")
			return err
		}
	}

	// If this is an ALTER ALL PARTITIONS statement, we need to find all indexes
	// with the specified partition name and apply the zone configuration to all
	// of them.
	var specifiers []tree.ZoneSpecifier
	if n.zoneSpecifier.TargetsPartition() && n.allIndexes {
		sqltelemetry.IncrementPartitioningCounter(sqltelemetry.AlterAllPartitions)
		for _, idx := range table.NonDropIndexes() {
			if idx.GetPartitioning().FindPartitionByName(string(n.zoneSpecifier.Partition)) != nil {
				zs := n.zoneSpecifier
				zs.TableOrIndex.Index = tree.UnrestrictedName(idx.GetName())
				specifiers = append(specifiers, zs)
			}
		}
	} else {
		specifiers = append(specifiers, n.zoneSpecifier)
	}

	applyZoneConfig := func(zs tree.ZoneSpecifier) error {
		subzonePlaceholder := false
		// resolveZone determines the ID of the target object of the zone
		// specifier. This ought to succeed regardless of whether there is
		// already a zone config for the target object.
		targetID, err := resolveZone(params.ctx, params.p.txn, params.p.Descriptors(), &zs, params.ExecCfg().Settings.Version)
		if err != nil {
			return err
		}
		// NamespaceTableID is not in the system gossip range, but users should not
		// be allowed to set zone configs on it.
		if targetID != keys.SystemDatabaseID && descpb.IsSystemConfigID(targetID) || targetID == keys.NamespaceTableID {
			return pgerror.Newf(pgcode.CheckViolation,
				`cannot set zone configs for system config tables; `+
					`try setting your config on the entire "system" database instead`)
		} else if targetID == keys.RootNamespaceID && deleteZone {
			return pgerror.Newf(pgcode.CheckViolation,
				"cannot remove default zone")
		}

		// Secondary tenants are not allowed to set zone configurations on any named
		// zones other than RANGE DEFAULT.
		if !params.p.execCfg.Codec.ForSystemTenant() {
			zoneName, found := zonepb.NamedZonesByID[uint32(targetID)]
			if found && zoneName != zonepb.DefaultZoneName {
				return pgerror.Newf(
					pgcode.CheckViolation,
					"non-system tenants cannot configure zone for %s range",
					zoneName,
				)
			}
		}

		// resolveSubzone determines the sub-parts of the zone
		// specifier. This ought to succeed regardless of whether there is
		// already a zone config.
		index, partition, err := resolveSubzone(&zs, table)
		if err != nil {
			return err
		}

		var tempIndex catalog.Index
		if index != nil {
			tempIndex = catalog.FindCorrespondingTemporaryIndexByID(table, index.GetID())
		}

		// Retrieve the partial zone configuration
		partialZoneWithRaw, err := params.p.Descriptors().GetZoneConfig(params.ctx, params.p.Txn(), targetID)
		if err != nil {
			return err
		}

		// No zone was found. Possibly a SubzonePlaceholder depending on the index.
		if partialZoneWithRaw == nil {
			partialZoneWithRaw = zone.NewZoneConfigWithRawBytes(zonepb.NewZoneConfig(), nil)
			if index != nil {
				subzonePlaceholder = true
			}
		}
		partialZone := partialZoneWithRaw.ZoneConfigProto()

		var partialSubzone *zonepb.Subzone
		if index != nil {
			partialSubzone = partialZone.GetSubzoneExact(uint32(index.GetID()), partition)
			if partialSubzone == nil {
				partialSubzone = &zonepb.Subzone{Config: *zonepb.NewZoneConfig()}
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
		_, completeZone, completeSubzone, err := GetZoneConfigInTxn(
			params.ctx, params.p.txn, params.p.Descriptors(), targetID, index, partition, n.setDefault,
		)

		if errors.Is(err, errNoZoneConfigApplies) {
			// No zone config yet.
			//
			// GetZoneConfigInTxn will fail with errNoZoneConfigApplies when
			// the target ID is not a database object, i.e. one of the system
			// ranges (liveness, meta, etc.), and did not have a zone config
			// already.
			completeZone = protoutil.Clone(
				params.extendedEvalCtx.ExecCfg.DefaultZoneConfig).(*zonepb.ZoneConfig)
		} else if err != nil {
			return err
		}

		// We need to inherit zone configuration information from the correct zone,
		// not completeZone.
		{
			zcHelper := descs.AsZoneConfigHydrationHelper(params.p.Descriptors())
			if index == nil {
				// If we are operating on a zone, get all fields that the zone would
				// inherit from its parent. We do this by using an empty zoneConfig
				// and completing at the level of the current zone.
				zoneInheritedFields := zonepb.ZoneConfig{}
				if err := completeZoneConfig(
					params.ctx, &zoneInheritedFields, params.p.Txn(), zcHelper, targetID,
				); err != nil {
					return err
				}
				partialZone.CopyFromZone(zoneInheritedFields, copyFromParentList)
			} else {
				// If we are operating on a subZone, we need to inherit all remaining
				// unset fields in its parent zone, which is partialZone.
				zoneInheritedFields := *partialZone
				if err := completeZoneConfig(
					params.ctx, &zoneInheritedFields, params.p.Txn(), zcHelper, targetID,
				); err != nil {
					return err
				}
				// In the case we have just an index, we should copy from the inherited
				// zone's fields (whether that was the table or database).
				if partition == "" {
					partialSubzone.Config.CopyFromZone(zoneInheritedFields, copyFromParentList)
				} else {
					// In the case of updating a partition, we need try inheriting fields
					// from the subzone's index, and inherit the remainder from the zone.
					subzoneInheritedFields := zonepb.ZoneConfig{}
					if indexSubzone := completeZone.GetSubzone(uint32(index.GetID()), ""); indexSubzone != nil {
						subzoneInheritedFields.InheritFromParent(&indexSubzone.Config)
					}
					subzoneInheritedFields.InheritFromParent(&zoneInheritedFields)
					// After inheriting fields, copy the requested ones into the
					// partialSubzone.Config.
					partialSubzone.Config.CopyFromZone(subzoneInheritedFields, copyFromParentList)
				}
			}
		}

		if deleteZone {
			if index != nil {
				didDelete := completeZone.DeleteSubzone(uint32(index.GetID()), partition)
				_ = partialZone.DeleteSubzone(uint32(index.GetID()), partition)
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
				finalZone = *protoutil.Clone(
					params.extendedEvalCtx.ExecCfg.DefaultZoneConfig).(*zonepb.ZoneConfig)
			} else if n.setDefault {
				finalZone = *zonepb.NewZoneConfig()
			}
			// Load settings from YAML. If there was no YAML (e.g. because the
			// query specified CONFIGURE ZONE USING), the YAML string will be
			// empty, in which case the unmarshaling will be a no-op. This is
			// innocuous.
			if err := yaml.UnmarshalStrict([]byte(yamlConfig), &newZone); err != nil {
				return pgerror.Wrap(err, pgcode.CheckViolation, "could not parse zone config")
			}

			// Load settings from YAML into the partial zone as well.
			if err := yaml.UnmarshalStrict([]byte(yamlConfig), &finalZone); err != nil {
				return pgerror.Wrap(err, pgcode.CheckViolation, "could not parse zone config")
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

			if err := validateZoneAttrsAndLocalities(params.ctx, params.p.ExecCfg(), &newZone); err != nil {
				return err
			}

			// Are we operating on an index?
			if index == nil {
				// No: the final zone config is the one we just processed.
				completeZone = &newZone
				partialZone = &finalZone
				// Since we are writing to a zone that is not a subzone, we need to
				// make sure that the zone config is not considered a placeholder
				// anymore. If the settings applied to this zone don't touch the
				// NumReplicas field, set it to nil so that the zone isn't considered a
				// placeholder anymore.
				if partialZone.IsSubzonePlaceholder() {
					partialZone.NumReplicas = nil
				}
			} else {
				// If the zone config for targetID was a subzone placeholder, it'll have
				// been skipped over by GetZoneConfigInTxn. We need to load it regardless
				// to avoid blowing away other subzones.

				// TODO(ridwanmsharif): How is this supposed to change? getZoneConfigRaw
				// gives no guarantees about completeness. Some work might need to happen
				// here to complete the missing fields. The reason is because we don't know
				// here if a zone is a placeholder or not. Can we do a GetConfigInTxn here?
				// And if it is a placeholder, we use getZoneConfigRaw to create one.
				completeZoneWithRaw, err := params.p.Descriptors().GetZoneConfig(params.ctx, params.p.Txn(), targetID)
				if err != nil {
					return err
				}

				if completeZoneWithRaw == nil {
					completeZone = zonepb.NewZoneConfig()
				} else {
					completeZone = completeZoneWithRaw.ZoneConfigProto()
				}
				completeZone.SetSubzone(zonepb.Subzone{
					IndexID:       uint32(index.GetID()),
					PartitionName: partition,
					Config:        newZone,
				})

				// The partial zone might just be empty. If so,
				// replace it with a SubzonePlaceholder.
				if subzonePlaceholder {
					partialZone.DeleteTableConfig()
				}

				partialZone.SetSubzone(zonepb.Subzone{
					IndexID:       uint32(index.GetID()),
					PartitionName: partition,
					Config:        finalZone,
				})

				// Also set the same zone configs for any corresponding temporary indexes.
				if tempIndex != nil {
					completeZone.SetSubzone(zonepb.Subzone{
						IndexID:       uint32(tempIndex.GetID()),
						PartitionName: partition,
						Config:        newZone,
					})

					partialZone.SetSubzone(zonepb.Subzone{
						IndexID:       uint32(tempIndex.GetID()),
						PartitionName: partition,
						Config:        finalZone,
					})
				}
			}

			// Finally revalidate everything. Validate only the completeZone config.
			if err := completeZone.Validate(); err != nil {
				return pgerror.Wrap(err, pgcode.CheckViolation, "could not validate zone config")
			}

			// Finally check for the extra protection partial zone configs would
			// require from changes made to parent zones. The extra protections are:
			//
			// RangeMinBytes and RangeMaxBytes must be set together
			// LeasePreferences cannot be set unless Constraints/VoterConstraints are
			// explicitly set
			// Per-replica constraints cannot be set unless num_replicas is explicitly
			// set
			// Per-voter constraints cannot be set unless num_voters is explicitly set
			if err := finalZone.ValidateTandemFields(); err != nil {
				err = errors.Wrap(err, "could not validate zone config")
				err = pgerror.WithCandidateCode(err, pgcode.InvalidParameterValue)
				err = errors.WithHint(err,
					"try ALTER ... CONFIGURE ZONE USING <field_name> = COPY FROM PARENT [, ...] to populate the field")
				return err
			}
		}

		// Write the partial zone configuration.
		hasNewSubzones := !deleteZone && index != nil
		execConfig := params.extendedEvalCtx.ExecCfg
		zoneToWrite := partialZone
		// TODO(ajwerner): This is extremely fragile because we accept a nil table
		// all the way down here.
		n.run.numAffected, err = writeZoneConfig(
			params.ctx,
			params.p.txn,
			targetID,
			table,
			zoneToWrite,
			partialZoneWithRaw.GetRawBytesInStorage(),
			execConfig,
			params.p.Descriptors(),
			hasNewSubzones,
			params.extendedEvalCtx.Tracing.KVTracingEnabled(),
		)
		if err != nil {
			return err
		}

		// Record that the change has occurred for auditing.
		eventDetails := eventpb.CommonZoneConfigDetails{
			Target:  tree.AsStringWithFQNames(&zs, params.Ann()),
			Config:  strings.TrimSpace(yamlConfig),
			Options: optionsStr,
		}
		var info logpb.EventPayload
		if deleteZone {
			info = &eventpb.RemoveZoneConfig{CommonZoneConfigDetails: eventDetails}
		} else {
			info = &eventpb.SetZoneConfig{CommonZoneConfigDetails: eventDetails}
		}
		return params.p.logEvent(params.ctx, targetID, info)
	}
	for _, zs := range specifiers {
		// Note(solon): Currently the zone configurations are applied serially for
		// each specifier. This could certainly be made more efficient. For
		// instance, we should only need to write to the system.zones table once
		// rather than once for every specifier. However, the number of specifiers
		// is expected to be low--it's bounded by the number of indexes on the
		// table--so I'm holding off on adding that complexity unless we find it's
		// necessary.
		if err := applyZoneConfig(zs); err != nil {
			return err
		}
	}
	return nil
}

func (n *setZoneConfigNode) Next(runParams) (bool, error) { return false, nil }
func (n *setZoneConfigNode) Values() tree.Datums          { return nil }
func (*setZoneConfigNode) Close(context.Context)          {}

func (n *setZoneConfigNode) FastPathResults() (int, bool) { return n.run.numAffected, true }

type nodeGetter func(context.Context, *serverpb.NodesRequest) (*serverpb.NodesResponse, error)
type regionsGetter func(context.Context, *serverpb.RegionsRequest) (*serverpb.RegionsResponse, error)

// Check that there are not duplicated values for a particular
// constraint. For example, constraints [+region=us-east1,+region=us-east2]
// will be rejected. Additionally, invalid constraints such as
// [+region=us-east1, -region=us-east1] will also be rejected.
func validateNoRepeatKeysInZone(zone *zonepb.ZoneConfig) error {
	for _, leasePreference := range zone.LeasePreferences {
		if err := validateNoRepeatKeysInConstraints(leasePreference.Constraints); err != nil {
			return err
		}
	}
	if err := validateNoRepeatKeysInConjunction(zone.Constraints); err != nil {
		return err
	}
	return validateNoRepeatKeysInConjunction(zone.VoterConstraints)
}

func validateNoRepeatKeysInConjunction(conjunctions []zonepb.ConstraintsConjunction) error {
	for _, constraints := range conjunctions {
		if err := validateNoRepeatKeysInConstraints(constraints.Constraints); err != nil {
			return err
		}
	}
	return nil
}

func validateNoRepeatKeysInConstraints(constraints []zonepb.Constraint) error {
	// Because we expect to have a small number of constraints, a nested
	// loop is probably better than allocating a map.
	for i, curr := range constraints {
		for _, other := range constraints[i+1:] {
			// We don't want to enter the other validation logic if both of the constraints
			// are attributes, due to the keys being the same for attributes.
			if curr.Key == "" && other.Key == "" {
				if curr.Value == other.Value {
					return pgerror.Newf(pgcode.CheckViolation,
						"incompatible zone constraints: %q and %q", curr, other)
				}
			} else {
				if curr.Type == zonepb.Constraint_REQUIRED {
					if other.Type == zonepb.Constraint_REQUIRED && other.Key == curr.Key ||
						other.Type == zonepb.Constraint_PROHIBITED && other.Key == curr.Key && other.Value == curr.Value {
						return pgerror.Newf(pgcode.CheckViolation,
							"incompatible zone constraints: %q and %q", curr, other)
					}
				} else if curr.Type == zonepb.Constraint_PROHIBITED {
					// If we have a -k=v pair, verify that there are not any
					// +k=v pairs in the constraints.
					if other.Type == zonepb.Constraint_REQUIRED && other.Key == curr.Key && other.Value == curr.Value {
						return pgerror.Newf(pgcode.CheckViolation,
							"incompatible zone constraints: %q and %q", curr, other)
					}
				}
			}
		}
	}
	return nil
}

// accumulateUniqueConstraints returns a list of unique constraints in the
// given zone config proto.
func accumulateUniqueConstraints(zone *zonepb.ZoneConfig) []zonepb.Constraint {
	constraints := make([]zonepb.Constraint, 0)
	addToValidate := func(c zonepb.Constraint) {
		var alreadyInList bool
		for _, val := range constraints {
			if c == val {
				alreadyInList = true
				break
			}
		}
		if !alreadyInList {
			constraints = append(constraints, c)
		}
	}
	for _, constraints := range zone.Constraints {
		for _, constraint := range constraints.Constraints {
			addToValidate(constraint)
		}
	}
	for _, constraints := range zone.VoterConstraints {
		for _, constraint := range constraints.Constraints {
			addToValidate(constraint)
		}
	}
	for _, leasePreferences := range zone.LeasePreferences {
		for _, constraint := range leasePreferences.Constraints {
			addToValidate(constraint)
		}
	}
	return constraints
}

// validateZoneAttrsAndLocalities ensures that all constraints/lease preferences
// specified in the new zone config snippet are actually valid, meaning that
// they match at least one node. This protects against user typos causing
// zone configs that silently don't work as intended.
//
// validateZoneAttrsAndLocalities is tenant aware in its validation. Secondary
// tenants don't have access to the NodeStatusServer, and as such, aren't
// allowed to set non-locality attributes in their constraints.
func validateZoneAttrsAndLocalities(
	ctx context.Context, execCfg *ExecutorConfig, zone *zonepb.ZoneConfig,
) error {
	// Avoid RPCs to the Node/Region server if we don't have anything to validate.
	if len(zone.Constraints) == 0 && len(zone.VoterConstraints) == 0 && len(zone.LeasePreferences) == 0 {
		return nil
	}
	if execCfg.Codec.ForSystemTenant() {
		ss, err := execCfg.NodesStatusServer.OptionalNodesStatusServer(MultitenancyZoneCfgIssueNo)
		if err != nil {
			return err
		}
		return validateZoneAttrsAndLocalitiesForSystemTenant(ctx, ss.ListNodesInternal, zone)
	}
	return validateZoneLocalitiesForSecondaryTenants(ctx, execCfg.TenantStatusServer.Regions, zone)
}

// validateZoneAttrsAndLocalitiesForSystemTenant performs all the constraint/
// lease preferences validation for the system tenant. The system tenant is
// allowed to reference both locality and non-locality attributes as it has
// access to node information via the NodeStatusServer.
//
// For the system tenant, this only catches typos in required constraints. This
// is by design. We don't want to reject prohibited constraints whose
// attributes/localities don't match any of the current nodes because it's a
// reasonable use case to add prohibited constraints for a new set of nodes
// before adding the new nodes to the cluster. If you had to first add one of
// the nodes before creating the constraints, data could be replicated there
// that shouldn't be.
func validateZoneAttrsAndLocalitiesForSystemTenant(
	ctx context.Context, getNodes nodeGetter, zone *zonepb.ZoneConfig,
) error {
	nodes, err := getNodes(ctx, &serverpb.NodesRequest{})
	if err != nil {
		return err
	}

	toValidate := accumulateUniqueConstraints(zone)

	// Check that each constraint matches some store somewhere in the cluster.
	for _, constraint := range toValidate {
		// We skip validation for negative constraints. See the function-level comment.
		if constraint.Type == zonepb.Constraint_PROHIBITED {
			continue
		}
		var found bool
	node:
		for _, node := range nodes.Nodes {
			for _, store := range node.StoreStatuses {
				// We could alternatively use zonepb.StoreMatchesConstraint here to
				// catch typos in prohibited constraints as well, but as noted in the
				// function-level comment that could break very reasonable use cases for
				// prohibited constraints.
				if zonepb.StoreSatisfiesConstraint(store.Desc, constraint) {
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

// validateZoneLocalitiesForSecondaryTenants performs all the constraint/lease
// preferences validation for secondary tenants. Secondary tenants are only
// allowed to reference locality attributes as they only have access to region
// information via the serverpb.TenantStatusServer. Even then, they're only
// allowed to reference the "region" and "zone" tiers.
//
// Unlike the system tenant, we also validate prohibited constraints. This is
// because secondary tenant must operate in the narrow view exposed via the
// serverpb.TenantStatusServer and are not allowed to configure arbitrary
// constraints (required or otherwise).
func validateZoneLocalitiesForSecondaryTenants(
	ctx context.Context, getRegions regionsGetter, zone *zonepb.ZoneConfig,
) error {
	toValidate := accumulateUniqueConstraints(zone)
	resp, err := getRegions(ctx, &serverpb.RegionsRequest{})
	if err != nil {
		return err
	}
	regions := make(map[string]struct{})
	zones := make(map[string]struct{})
	for regionName, regionMeta := range resp.Regions {
		regions[regionName] = struct{}{}
		for _, zone := range regionMeta.Zones {
			zones[zone] = struct{}{}
		}
	}

	for _, constraint := range toValidate {
		switch constraint.Key {
		case "zone":
			_, found := zones[constraint.Value]
			if !found {
				return pgerror.Newf(
					pgcode.CheckViolation,
					"zone %q not found",
					constraint.Value,
				)
			}
		case "region":
			_, found := regions[constraint.Value]
			if !found {
				return pgerror.Newf(
					pgcode.CheckViolation,
					"region %q not found",
					constraint.Value,
				)
			}
		default:
			return errors.WithHint(pgerror.Newf(
				pgcode.CheckViolation,
				"invalid constraint attribute: %q",
				constraint.Key,
			),
				`only "zone" and "region" are allowed`,
			)
		}
	}
	return nil
}

// MultitenancyZoneCfgIssueNo points to the multitenancy zone config issue number.
const MultitenancyZoneCfgIssueNo = 49854

type zoneConfigUpdate struct {
	id         descpb.ID
	zoneConfig catalog.ZoneConfig
}

func prepareZoneConfigWrites(
	ctx context.Context,
	execCfg *ExecutorConfig,
	targetID descpb.ID,
	table catalog.TableDescriptor,
	z *zonepb.ZoneConfig,
	expectedExistingRawBytes []byte,
	hasNewSubzones bool,
) (_ *zoneConfigUpdate, err error) {
	if len(z.Subzones) > 0 {
		st := execCfg.Settings
		z.SubzoneSpans, err = GenerateSubzoneSpans(
			st, execCfg.NodeInfo.LogicalClusterID(), execCfg.Codec, table, z.Subzones, hasNewSubzones)
		if err != nil {
			return nil, err
		}
	} else {
		// To keep the Subzone and SubzoneSpan arrays consistent
		z.SubzoneSpans = nil
	}
	if z.IsSubzonePlaceholder() && len(z.Subzones) == 0 {
		return &zoneConfigUpdate{id: targetID}, nil
	}
	return &zoneConfigUpdate{id: targetID, zoneConfig: zone.NewZoneConfigWithRawBytes(z, expectedExistingRawBytes)}, nil
}

func writeZoneConfig(
	ctx context.Context,
	txn *kv.Txn,
	targetID descpb.ID,
	table catalog.TableDescriptor,
	zone *zonepb.ZoneConfig,
	expectedExistingRawBytes []byte,
	execCfg *ExecutorConfig,
	descriptors *descs.Collection,
	hasNewSubzones bool,
	kvTrace bool,
) (numAffected int, err error) {
	update, err := prepareZoneConfigWrites(ctx, execCfg, targetID, table, zone, expectedExistingRawBytes, hasNewSubzones)
	if err != nil {
		return 0, err
	}
	return writeZoneConfigUpdate(ctx, txn, kvTrace, descriptors, update)
}

func writeZoneConfigUpdate(
	ctx context.Context,
	txn *kv.Txn,
	kvTrace bool,
	descriptors *descs.Collection,
	update *zoneConfigUpdate,
) (numAffected int, err error) {
	b := txn.NewBatch()
	if update.zoneConfig == nil {
		err = descriptors.DeleteZoneConfigInBatch(ctx, kvTrace, b, update.id)
	} else {
		numAffected = 1
		err = descriptors.WriteZoneConfigToBatch(ctx, kvTrace, b, update.id, update.zoneConfig)
	}
	if err != nil {
		return 0, err
	}

	if err := txn.Run(ctx, b); err != nil {
		return 0, err
	}
	r := b.Results[0]
	if r.Err != nil {
		panic("run succeeded even through the result has an error")
	}
	// We don't really care how many keys are affected since this function always
	// write one single zone config.
	if len(r.Keys) > 0 {
		numAffected = 1
	}
	return numAffected, err
}

// RemoveIndexZoneConfigs removes the zone configurations for some
// indexes being dropped. It is a no-op if there is no zone
// configuration, there's no index zone configs to be dropped,
// or it is run on behalf of a tenant.
//
// It operates entirely on the current goroutine and is thus able to
// reuse an existing client.Txn safely.
func RemoveIndexZoneConfigs(
	ctx context.Context,
	txn isql.Txn,
	execCfg *ExecutorConfig,
	kvTrace bool,
	descriptors *descs.Collection,
	tableDesc catalog.TableDescriptor,
	indexIDs []uint32,
) error {
	zoneWithRaw, err := descriptors.GetZoneConfig(ctx, txn.KV(), tableDesc.GetID())
	if err != nil {
		return err
	}
	// If there are no zone configs, there's nothing to remove.
	if zoneWithRaw == nil {
		return nil
	}

	zone := zoneWithRaw.ZoneConfigProto()
	// Look through all of the subzones and determine if we need to remove any
	// of them. We only want to rewrite the zone config below if there's actual
	// work to be done here.
	zcRewriteNecessary := false
	for _, indexID := range indexIDs {
		for _, s := range zone.Subzones {
			if s.IndexID == indexID {
				// We've found an subzone that matches the given indexID. Delete all of
				// this index's subzones and move on to the next index.
				zone.DeleteIndexSubzones(indexID)
				zcRewriteNecessary = true
				break
			}
		}
	}

	if zcRewriteNecessary {
		// Ignore CCL required error to allow schema change to progress.
		_, err = writeZoneConfig(
			ctx, txn.KV(), tableDesc.GetID(), tableDesc, zone,
			zoneWithRaw.GetRawBytesInStorage(), execCfg, descriptors,
			false /* hasNewSubzones */, kvTrace,
		)
		if err != nil && !sqlerrors.IsCCLRequiredError(err) {
			return err
		}
	}
	return nil
}
