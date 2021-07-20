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

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"gopkg.in/yaml.v2"
)

// These must match crdb_internal.zones.
var showZoneConfigColumns = colinfo.ResultColumns{
	{Name: "zone_id", Typ: types.Int, Hidden: true},
	{Name: "subzone_id", Typ: types.Int, Hidden: true},
	{Name: "target", Typ: types.String},
	{Name: "range_name", Typ: types.String, Hidden: true},
	{Name: "database_name", Typ: types.String, Hidden: true},
	{Name: "schema_name", Typ: types.String, Hidden: true},
	{Name: "table_name", Typ: types.String, Hidden: true},
	{Name: "index_name", Typ: types.String, Hidden: true},
	{Name: "partition_name", Typ: types.String, Hidden: true},
	{Name: "raw_config_yaml", Typ: types.String, Hidden: true},
	{Name: "raw_config_sql", Typ: types.String},
	{Name: "raw_config_protobuf", Typ: types.Bytes, Hidden: true},
	{Name: "full_config_yaml", Typ: types.String, Hidden: true},
	{Name: "full_config_sql", Typ: types.String, Hidden: true},
}

// These must match showZoneConfigColumns.
const (
	zoneIDCol int = iota
	subZoneIDCol
	targetCol
	rangeNameCol
	databaseNameCol
	schemaNameCol
	tableNameCol
	indexNameCol
	partitionNameCol
	rawConfigYAMLCol
	rawConfigSQLCol
	rawConfigProtobufCol
	fullConfigYamlCol
	fullConfigSQLCol
)

func (p *planner) ShowZoneConfig(ctx context.Context, n *tree.ShowZoneConfig) (planNode, error) {
	if !p.ExecCfg().Codec.ForSystemTenant() {
		return nil, errorutil.UnsupportedWithMultiTenancy(MultitenancyZoneCfgIssueNo)
	}

	return &delayedNode{
		name:    n.String(),
		columns: showZoneConfigColumns,
		constructor: func(ctx context.Context, p *planner) (planNode, error) {
			v := p.newContainerValuesNode(showZoneConfigColumns, 0)

			// This signifies SHOW ALL.
			// However, SHOW ALL should be handled by the delegate.
			if n.ZoneSpecifier == (tree.ZoneSpecifier{}) {
				return nil, errors.AssertionFailedf("zone must be specified")
			}

			row, err := getShowZoneConfigRow(ctx, p, n.ZoneSpecifier)
			if err != nil {
				v.Close(ctx)
				return nil, err
			}
			if _, err := v.rows.AddRow(ctx, row); err != nil {
				v.Close(ctx)
				return nil, err
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

	if zoneSpecifier.TableOrIndex.Table.ObjectName != "" {
		if err = p.CheckAnyPrivilege(ctx, tblDesc); err != nil {
			return nil, err
		}
	} else if zoneSpecifier.Database != "" {
		database, err := p.Descriptors().GetImmutableDatabaseByName(
			ctx,
			p.txn,
			string(zoneSpecifier.Database),
			tree.DatabaseLookupFlags{Required: true},
		)
		if err != nil {
			return nil, err
		}
		if err = p.CheckAnyPrivilege(ctx, database); err != nil {
			return nil, err
		}
	}

	targetID, err := resolveZone(ctx, p.txn, &zoneSpecifier)
	if err != nil {
		return nil, err
	}

	index, partition, err := resolveSubzone(&zoneSpecifier, tblDesc)
	if err != nil {
		return nil, err
	}

	subZoneIdx := uint32(0)
	zoneID, zone, subzone, err := GetZoneConfigInTxn(ctx, p.txn,
		config.SystemTenantObjectID(targetID), index, partition, false /* getInheritedDefault */)
	if errors.Is(err, errNoZoneConfigApplies) {
		// TODO(benesch): This shouldn't be the caller's responsibility;
		// GetZoneConfigInTxn should just return the default zone config if no zone
		// config applies.
		zone = p.execCfg.DefaultZoneConfig
		zoneID = keys.RootNamespaceID
	} else if err != nil {
		return nil, err
	} else if subzone != nil {
		for i := range zone.Subzones {
			subZoneIdx++
			if subzone == &zone.Subzones[i] {
				break
			}
		}
		zone = &subzone.Config
	}

	// Determine the zone specifier for the zone config that actually applies
	// without performing another KV lookup.
	zs := ascendZoneSpecifier(zoneSpecifier, config.SystemTenantObjectID(targetID), zoneID, subzone)

	// Ensure subzone configs don't infect the output of config_bytes.
	zone.Subzones = nil
	zone.SubzoneSpans = nil

	vals := make(tree.Datums, len(showZoneConfigColumns))
	if err := generateZoneConfigIntrospectionValues(
		vals, tree.NewDInt(tree.DInt(zoneID)), tree.NewDInt(tree.DInt(subZoneIdx)), &zs, zone, nil,
	); err != nil {
		return nil, err
	}
	return vals, nil
}

// zoneConfigToSQL pretty prints a zone configuration as a SQL string.
func zoneConfigToSQL(zs *tree.ZoneSpecifier, zone *zonepb.ZoneConfig) (string, error) {
	constraints, err := yamlMarshalFlow(zonepb.ConstraintsList{
		Constraints: zone.Constraints,
		Inherited:   zone.InheritedConstraints})
	if err != nil {
		return "", err
	}
	constraints = strings.TrimSpace(constraints)
	voterConstraints, err := yamlMarshalFlow(zonepb.ConstraintsList{
		Constraints: zone.VoterConstraints,
		Inherited:   zone.InheritedVoterConstraints(),
	})
	if err != nil {
		return "", err
	}
	voterConstraints = strings.TrimSpace(voterConstraints)
	prefs, err := yamlMarshalFlow(zone.LeasePreferences)
	if err != nil {
		return "", err
	}
	prefs = strings.TrimSpace(prefs)

	useComma := false
	maybeWriteComma := func(f *tree.FmtCtx) {
		if useComma {
			f.Printf(",\n")
		}
		useComma = true
	}

	f := tree.NewFmtCtx(tree.FmtParsable)
	f.WriteString("ALTER ")
	f.FormatNode(zs)
	f.WriteString(" CONFIGURE ZONE USING\n")
	if zone.RangeMinBytes != nil {
		maybeWriteComma(f)
		f.Printf("\trange_min_bytes = %d", *zone.RangeMinBytes)
	}
	if zone.RangeMaxBytes != nil {
		maybeWriteComma(f)
		f.Printf("\trange_max_bytes = %d", *zone.RangeMaxBytes)
	}
	if zone.GC != nil {
		maybeWriteComma(f)
		f.Printf("\tgc.ttlseconds = %d", zone.GC.TTLSeconds)
	}
	if zone.GlobalReads != nil {
		maybeWriteComma(f)
		f.Printf("\tglobal_reads = %t", *zone.GlobalReads)
	}
	if zone.NumReplicas != nil {
		maybeWriteComma(f)
		f.Printf("\tnum_replicas = %d", *zone.NumReplicas)
	}
	if zone.NumVoters != nil {
		maybeWriteComma(f)
		f.Printf("\tnum_voters = %d", *zone.NumVoters)
	}
	if !zone.InheritedConstraints {
		maybeWriteComma(f)
		f.Printf("\tconstraints = %s", lex.EscapeSQLString(constraints))
	}
	if !zone.InheritedVoterConstraints() && zone.NumVoters != nil && *zone.NumVoters > 0 {
		maybeWriteComma(f)
		f.Printf("\tvoter_constraints = %s", lex.EscapeSQLString(voterConstraints))
	}
	if !zone.InheritedLeasePreferences {
		maybeWriteComma(f)
		f.Printf("\tlease_preferences = %s", lex.EscapeSQLString(prefs))
	}
	return f.String(), nil
}

// generateZoneConfigIntrospectionValues creates a result row
// suitable for populating crdb_internal.zones or SHOW ZONE CONFIG.
// The values are populated into the `values` first argument.
// The caller is responsible for creating the DInt for the ID and
// provide it as 2nd argument. The function will compute
// the remaining values based on the zone specifier and configuration.
// The fullZoneConfig argument is a zone config populated with all
// inherited zone configuration information. If this argument is nil,
// then the zone argument is used to populate the full_config_sql and
// full_config_yaml columns.
func generateZoneConfigIntrospectionValues(
	values tree.Datums,
	zoneID tree.Datum,
	subZoneID tree.Datum,
	zs *tree.ZoneSpecifier,
	zone *zonepb.ZoneConfig,
	fullZoneConfig *zonepb.ZoneConfig,
) error {
	// Populate the ID column.
	values[zoneIDCol] = zoneID
	values[subZoneIDCol] = subZoneID

	// Populate the zone specifier columns.
	values[targetCol] = tree.DNull
	values[rangeNameCol] = tree.DNull
	values[databaseNameCol] = tree.DNull
	values[schemaNameCol] = tree.DNull
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
		if zs.TableOrIndex.Table.ObjectName != "" {
			values[databaseNameCol] = tree.NewDString(string(zs.TableOrIndex.Table.CatalogName))
			values[schemaNameCol] = tree.NewDString(string(zs.TableOrIndex.Table.SchemaName))
			values[tableNameCol] = tree.NewDString(string(zs.TableOrIndex.Table.ObjectName))
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
	values[rawConfigYAMLCol] = tree.NewDString(string(yamlConfig))

	// Populate the SQL column.
	if zs == nil {
		values[rawConfigSQLCol] = tree.DNull
	} else {
		sqlStr, err := zoneConfigToSQL(zs, zone)
		if err != nil {
			return err
		}
		values[rawConfigSQLCol] = tree.NewDString(sqlStr)
	}

	// Populate the protobuf column.
	protoConfig, err := protoutil.Marshal(zone)
	if err != nil {
		return err
	}
	values[rawConfigProtobufCol] = tree.NewDBytes(tree.DBytes(protoConfig))

	// Populate the full_config_yaml and full_config_sql columns.
	inheritedConfig := fullZoneConfig
	if inheritedConfig == nil {
		inheritedConfig = zone
	}

	yamlConfig, err = yaml.Marshal(inheritedConfig)
	if err != nil {
		return err
	}
	values[fullConfigYamlCol] = tree.NewDString(string(yamlConfig))

	if zs == nil {
		values[fullConfigSQLCol] = tree.DNull
	} else {
		sqlStr, err := zoneConfigToSQL(zs, inheritedConfig)
		if err != nil {
			return err
		}
		values[fullConfigSQLCol] = tree.NewDString(sqlStr)
	}
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
	zs tree.ZoneSpecifier,
	resolvedID, actualID config.SystemTenantObjectID,
	actualSubzone *zonepb.Subzone,
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
