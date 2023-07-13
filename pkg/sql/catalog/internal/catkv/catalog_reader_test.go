// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package catkv_test

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/internal/catkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/datadriven"
	"github.com/kylelemons/godebug/diff"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

// TestDataDriven exercises the methods of a catkv.CatalogReader in a
// data-driven fashion. The methods are run on both an uncached and a cached
// implementation and the results are marshalled to yaml-like strings and
// compared.
func TestDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	datadriven.Walk(t, datapathutils.TestDataPath(t), func(t *testing.T, path string) {
		s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
			DefaultTestTenant: base.TODOTestTenantDisabled,
			Knobs: base.TestingKnobs{
				SQLExecutor: &sql.ExecutorTestingKnobs{
					UseTransactionalDescIDGenerator: true,
				},
			},
		})
		defer s.Stopper().Stop(ctx)
		tdb := sqlutils.MakeSQLRunner(sqlDB)
		execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
		v := execCfg.Settings.Version.ActiveVersion(ctx)
		sdc := catkv.NewSystemDatabaseCache(execCfg.Codec, execCfg.Settings)
		ccr := catkv.NewCatalogReader(execCfg.Codec, v, sdc, nil /* maybeMonitor */)
		ucr := catkv.NewUncachedCatalogReader(execCfg.Codec)

		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) (ret string) {
			h := testHelper{
				t:       t,
				d:       d,
				execCfg: &execCfg,
				ucr:     ucr,
				ccr:     ccr,
			}

			switch d.Cmd {
			case "setup":
				sqlutils.VerifyStatementPrettyRoundtrip(t, d.Input)
				stmts, err := parser.Parse(d.Input)
				require.NoError(t, err)
				for _, stmt := range stmts {
					tdb.Exec(t, stmt.SQL)
				}
				return ""

			case "reset":
				ucr.Reset(ctx)
				ccr.Reset(ctx)
				return ""

			case "is_id_in_cache":
				var id int
				d.ScanArgs(t, "id", &id)
				return fmt.Sprintf("%v", ccr.IsIDInCache(descpb.ID(id)))

			case "is_name_in_cache":
				var name string
				var dbID, scID int
				d.ScanArgs(t, "name_key", &dbID, &scID, &name)
				ni := descpb.NameInfo{
					ParentID:       descpb.ID(dbID),
					ParentSchemaID: descpb.ID(scID),
					Name:           name,
				}
				return fmt.Sprintf("%v", ccr.IsNameInCache(&ni))

			case "is_desc_id_known_to_not_exist":
				var id, maybeParentID int
				d.ScanArgs(t, "id", &id)
				if d.HasArg("maybe_parent_id") {
					d.ScanArgs(t, "maybe_parent_id", &maybeParentID)
				}
				return fmt.Sprintf("%v", ccr.IsDescIDKnownToNotExist(descpb.ID(id), descpb.ID(maybeParentID)))

			case "scan_all":
				q := func(ctx context.Context, txn *kv.Txn, cr catkv.CatalogReader) (nstree.Catalog, error) {
					return cr.ScanAll(ctx, txn)
				}
				return h.doCatalogQuery(ctx, q)

			case "scan_all_comments":
				q := func(ctx context.Context, txn *kv.Txn, cr catkv.CatalogReader) (nstree.Catalog, error) {
					return cr.ScanAllComments(ctx, txn)
				}
				return h.doCatalogQuery(ctx, q)

			case "scan_namespace_for_databases":
				q := func(ctx context.Context, txn *kv.Txn, cr catkv.CatalogReader) (nstree.Catalog, error) {
					return cr.ScanNamespaceForDatabases(ctx, txn)
				}
				return h.doCatalogQuery(ctx, q)

			case "scan_namespace_for_database_schemas":
				db := h.argDesc(ctx, "db_id", catalog.Database).(catalog.DatabaseDescriptor)
				q := func(ctx context.Context, txn *kv.Txn, cr catkv.CatalogReader) (nstree.Catalog, error) {
					return cr.ScanNamespaceForDatabaseSchemas(ctx, txn, db)
				}
				return h.doCatalogQuery(ctx, q)

			case "scan_namespace_for_database_schemas_and_objects":
				db := h.argDesc(ctx, "db_id", catalog.Database).(catalog.DatabaseDescriptor)
				q := func(ctx context.Context, txn *kv.Txn, cr catkv.CatalogReader) (nstree.Catalog, error) {
					return cr.ScanNamespaceForDatabaseSchemasAndObjects(ctx, txn, db)
				}
				return h.doCatalogQuery(ctx, q)

			case "scan_namespace_for_schema_objects":
				db := h.argDesc(ctx, "db_id", catalog.Database).(catalog.DatabaseDescriptor)
				sc := h.argDesc(ctx, "sc_id", catalog.Schema).(catalog.SchemaDescriptor)
				q := func(ctx context.Context, txn *kv.Txn, cr catkv.CatalogReader) (nstree.Catalog, error) {
					return cr.ScanNamespaceForSchemaObjects(ctx, txn, db, sc)
				}
				return h.doCatalogQuery(ctx, q)

			case "get_by_ids":
				var ids []descpb.ID
				for _, pair := range d.CmdArgs {
					if len(pair.Vals) != 1 || pair.Key != "id" {
						t.Fatalf("%s: bad id arguments", d.Pos)
					}
					idInt, err := strconv.ParseInt(pair.Vals[0], 10, 64)
					require.NoErrorf(t, err, "%s: bad id arguments", d.Pos)
					ids = append(ids, descpb.ID(idInt))
				}
				q := func(ctx context.Context, txn *kv.Txn, cr catkv.CatalogReader) (nstree.Catalog, error) {
					const isDescriptorRequired = false
					return cr.GetByIDs(ctx, txn, ids, isDescriptorRequired, catalog.Any)
				}
				return h.doCatalogQuery(ctx, q)

			case "get_by_names":
				var nis []descpb.NameInfo
				for _, pair := range d.CmdArgs {
					if len(pair.Vals) != 3 || pair.Key != "name_key" {
						t.Fatalf("%s: bad name_key arguments", d.Pos)
					}
					dbID, err := strconv.ParseInt(pair.Vals[0], 10, 64)
					require.NoErrorf(t, err, "%s: bad name_key arguments", d.Pos)
					scID, err := strconv.ParseInt(pair.Vals[1], 10, 64)
					require.NoErrorf(t, err, "%s: bad name_key arguments", d.Pos)
					nis = append(nis, descpb.NameInfo{
						ParentID:       descpb.ID(dbID),
						ParentSchemaID: descpb.ID(scID),
						Name:           pair.Vals[2],
					})
				}
				q := func(ctx context.Context, txn *kv.Txn, cr catkv.CatalogReader) (nstree.Catalog, error) {
					return cr.GetByNames(ctx, txn, nis)
				}
				return h.doCatalogQuery(ctx, q)
			}
			return fmt.Sprintf("%s: unknown command: %s", d.Pos, d.Cmd)
		})
	})
}

type testHelper struct {
	t        *testing.T
	d        *datadriven.TestData
	execCfg  *sql.ExecutorConfig
	ucr, ccr catkv.CatalogReader
}

func (h testHelper) argDesc(
	ctx context.Context, idArgName string, expectedType catalog.DescriptorType,
) catalog.Descriptor {
	var idInt int
	h.d.ScanArgs(h.t, idArgName, &idInt)
	require.NotZerof(h.t, idInt,
		"%s: descriptor ID argument %q should be non-zero", h.d.Pos, idArgName)
	id := descpb.ID(idInt)
	var c nstree.Catalog
	err := h.execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
		const isDescriptorRequired = true
		c, err = h.ucr.GetByIDs(ctx, txn, []descpb.ID{id}, isDescriptorRequired, expectedType)
		return err
	})
	require.NoErrorf(h.t, err,
		"%s: error reading %s descriptor %d", h.d.Pos, expectedType, id)
	desc := c.LookupDescriptor(id)
	require.NotNilf(h.t, desc,
		"%s: nil %s descriptor %d", h.d.Pos, expectedType, id)
	require.Equal(h.t, expectedType, desc.DescriptorType(),
		"%s: unexpected type for descriptor %d", h.d.Pos, id)
	return desc
}

type queryFunc func(ctx context.Context, txn *kv.Txn, cr catkv.CatalogReader) (nstree.Catalog, error)

func (h testHelper) wrappedQuery(
	ctx context.Context, label string, cr catkv.CatalogReader, unwrapped queryFunc,
) (c nstree.Catalog, rs tracingpb.RecordedSpan) {
	err := h.execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
		tracer := h.execCfg.AmbientCtx.Tracer
		vCtx, vSpan := tracer.StartSpanCtx(
			ctx, catkv.TestingSpanOperationName, tracing.WithRecording(tracingpb.RecordingVerbose),
		)
		c, err = unwrapped(vCtx, txn, cr)
		rec := vSpan.FinishAndGetRecording(tracingpb.RecordingVerbose)
		var found bool
		rs, found = rec.FindSpan(catkv.TestingSpanOperationName)
		require.True(h.t, found)
		return err
	})
	require.NoErrorf(h.t, err, "%s: error running query with %s CatalogReader", h.d.Pos, label)
	return c, rs
}

func (h testHelper) doCatalogQuery(ctx context.Context, fn queryFunc) string {
	u := h.marshalResult(h.wrappedQuery(ctx, "uncached", h.ucr, fn))
	c := h.marshalResult(h.wrappedQuery(ctx, "cached", h.ccr, fn))
	d := diff.Diff(u, c)
	if len(d) == 0 {
		return u
	}
	lines := strings.Split(d, "\n")
	var shortDiffLines []string
	var showFullDiff bool
	for _, line := range lines {
		if !strings.HasPrefix(line, "+") && !strings.HasPrefix(line, "-") {
			continue
		}
		if strings.HasPrefix(line, "+trace:") || strings.HasPrefix(line, "-trace:") {
			continue
		}
		if strings.HasPrefix(line, "+-") {
			shortDiffLines = append(shortDiffLines, "+"+strings.TrimPrefix(line, "+-"))
		} else if strings.HasPrefix(line, "--") {
			shortDiffLines = append(shortDiffLines, "-"+strings.TrimPrefix(line, "--"))
		} else {
			showFullDiff = true
			break
		}
	}
	if !showFullDiff {
		d = strings.TrimSpace(strings.Join(shortDiffLines, "\n"))
	}
	return fmt.Sprintf("%scached:\n%s", u, d)
}

func (h testHelper) marshalResult(c nstree.Catalog, rs tracingpb.RecordedSpan) string {
	m := map[string]interface{}{
		"catalog": h.catalogToYaml(c),
		"trace":   h.traceToYaml(rs),
	}
	bytes, err := yaml.Marshal(m)
	require.NoError(h.t, err)
	return string(bytes)
}

func (h testHelper) catalogToYaml(c nstree.Catalog) interface{} {
	type joined struct {
		ns       *descpb.NameInfo
		d        catalog.Descriptor
		zc       catalog.ZoneConfig
		comments []struct {
			key   catalogkeys.CommentKey
			value string
		}
	}
	m := make(map[descpb.ID]joined)
	_ = c.ForEachNamespaceEntry(func(ns nstree.NamespaceEntry) error {
		j := m[ns.GetID()]
		j.ns = &descpb.NameInfo{
			ParentID:       ns.GetParentID(),
			ParentSchemaID: ns.GetParentSchemaID(),
			Name:           ns.GetName(),
		}
		m[ns.GetID()] = j
		return nil
	})
	_ = c.ForEachDescriptor(func(d catalog.Descriptor) error {
		mut := d.NewBuilder().BuildCreatedMutable()
		mut.ResetModificationTime()
		j := m[d.GetID()]
		j.d = mut
		m[d.GetID()] = j
		return nil
	})
	_ = c.ForEachZoneConfig(func(id descpb.ID, zc catalog.ZoneConfig) error {
		j := m[id]
		j.zc = zc
		m[id] = j
		return nil
	})
	_ = c.ForEachComment(func(key catalogkeys.CommentKey, cmt string) error {
		id := descpb.ID(key.ObjectID)
		j := m[id]
		j.comments = append(j.comments, struct {
			key   catalogkeys.CommentKey
			value string
		}{key: key, value: cmt})
		m[id] = j
		return nil
	})
	var ids catalog.DescriptorIDSet
	for id := range m {
		ids.Add(id)
	}
	y := make(map[string]interface{})
	for _, id := range ids.Ordered() {
		j := m[id]
		jm := make(map[string]interface{})
		if j.d != nil {
			jm["descriptor"] = j.d.DescriptorType()
		}
		if j.ns != nil {
			jm["namespace"] = fmt.Sprintf("(%d, %d, %q)",
				j.ns.ParentID, j.ns.ParentSchemaID, j.ns.Name)
		}
		if j.zc != nil {
			ttl := "default"
			if gc := j.zc.ZoneConfigProto().GC; gc != nil {
				ttl = fmt.Sprintf("%d", gc.TTLSeconds)
			}
			jm["zone"] = "gc.ttlseconds=" + ttl
		}
		cm := make(map[string]interface{})
		for _, pair := range j.comments {
			k := strings.ToLower(strings.TrimSuffix(pair.key.CommentType.String(), "CommentType"))
			if pair.key.SubID > 0 {
				k += fmt.Sprintf("_%d", pair.key.SubID)
			}
			cm[k] = pair.value
		}
		if len(cm) > 0 {
			jm["comments"] = cm
		}
		y[fmt.Sprintf("%03d", id)] = jm
	}
	return y
}

func (h testHelper) traceToYaml(rs tracingpb.RecordedSpan) interface{} {
	re, err := regexp.Compile(`^\S*\s+`)
	require.NoError(h.t, err)

	var l []interface{}
	for i := range rs.Logs {
		msgWithFilePrefix := rs.Logs[i].Message.StripMarkers()
		stripped := re.ReplaceAllString(msgWithFilePrefix, "")
		l = append(l, stripped)
	}
	return l
}
