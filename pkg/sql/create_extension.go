// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
)

type createExtensionNode struct {
	zeroInputPlanNode
	CreateExtension tree.CreateExtension
}

func (p *planner) CreateExtension(ctx context.Context, n *tree.CreateExtension) (planNode, error) {
	return &createExtensionNode{
		CreateExtension: *n,
	}, nil
}

func (n *createExtensionNode) unimplementedExtensionError(issue int) error {
	name := n.CreateExtension.Name
	return unimplemented.NewWithIssueDetailf(
		issue,
		"CREATE EXTENSION "+string(name),
		"extension %q is not yet supported",
		name,
	)
}

func (n *createExtensionNode) startExec(params runParams) error {
	switch n.CreateExtension.Name {
	case "postgis",
		"pg_trgm",
		"fuzzystrmatch",
		"pgcrypto",
		"uuid-ossp",
		"vector":
		telemetry.Inc(sqltelemetry.CreateExtensionCounter(string(n.CreateExtension.Name)))
		return nil
	case "postgis_raster",
		"postgis_topology",
		"postgis_sfcgal",
		"address_standardizer",
		"address_standardizer_data_us",
		"postgis_tiger_geocoder":
		// PostGIS specific extensions.
		return n.unimplementedExtensionError(54514)
	case "btree_gin":
		return n.unimplementedExtensionError(51992)
	case "btree_gist":
		return n.unimplementedExtensionError(51993)
	case "citext":
		return n.unimplementedExtensionError(41276)
	case "postgres_fdw":
		return n.unimplementedExtensionError(20249)
	case "adminpack",
		"amcheck",
		"auth_delay",
		"auto_explain",
		"bloom",
		"cube",
		"dblink",
		"dict_int",
		"dict_xsyn",
		"earthdistance",
		"file_fdw",
		"hstore",
		"intagg",
		"intarray",
		"isn",
		"lo",
		"ltree",
		"pageinspect",
		"passwordcheck",
		"pg_buffercache",
		"pg_freespacemap",
		"pg_prewarm",
		"pgrowlocks",
		"pg_stat_statements",
		"pgstattuple",
		"pg_visibility",
		"seg",
		"sepgsql",
		"spi",
		"sslinfo",
		"tablefunc",
		"tcn",
		"test_decoding",
		"tsm_system_rows",
		"tsm_system_time",
		"unaccent",
		"xml2":
		return n.unimplementedExtensionError(54516)
	}
	return pgerror.Newf(pgcode.UndefinedParameter, "unknown extension: %s", n.CreateExtension.Name)
}

func (n *createExtensionNode) Next(params runParams) (bool, error) { return false, nil }
func (n *createExtensionNode) Values() tree.Datums                 { return tree.Datums{} }
func (n *createExtensionNode) Close(ctx context.Context)           {}
