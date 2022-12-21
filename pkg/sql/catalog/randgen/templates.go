// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package randgen

import (
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

type tbTemplates struct {
	templates []tbTemplate
	cur       int
}

type tbTemplate struct {
	namePat         string
	desc            descpb.TableDescriptor
	baseColumnNames []string
}

func (t *tbTemplates) pickTemplate() *tbTemplate {
	tmpl := &t.templates[t.cur]
	t.cur = (t.cur + 1) % len(t.templates)
	return tmpl
}

func (g *testSchemaGenerator) loadTemplates(ctx context.Context) {
	if !g.createTables {
		// No template needed.
		return
	}
	if len(g.cfg.TableTemplates) == 0 {
		// No template specified: we use a simple predefined table template.
		g.templates = append(g.templates, defaultTemplate())
		return
	}

	// The user has specified some table patterns. Look them up.
	objIDs := make(map[descpb.ID]struct{})
outer:
	for _, pat := range g.cfg.TableTemplates {
		tbPat, err := parser.ParseTablePattern(pat)
		if err != nil {
			panic(genError{errors.Wrapf(err, "parsing template name %q", pat)})
		}
		ids, err := g.expandTableGlob(ctx, tbPat)
		if err != nil {
			panic(genError{errors.Wrapf(err, "expanding template name %q", pat)})
		}
		for _, id := range ids {
			const maxTemplates = 100
			if len(objIDs) > maxTemplates {
				// Let's not let the template list get out of hand.
				break outer
			}
			objIDs[id] = struct{}{}
		}
	}
	sobjIDs := make([]descpb.ID, 0, len(objIDs))
	for id := range objIDs {
		sobjIDs = append(sobjIDs, id)
	}
	sort.Slice(sobjIDs, func(i, j int) bool { return sobjIDs[i] < sobjIDs[j] })
	descs, err := g.coll.GetImmutableDescriptorsByID(ctx, g.txn,
		tree.CommonLookupFlags{Required: true},
		sobjIDs...)
	if err != nil {
		panic(genError{errors.Wrap(err, "retrieving template descriptors")})
	}
	for _, desc := range descs {
		tb, ok := desc.(catalog.TableDescriptor)
		if !ok {
			// We don't support templating anything else than tables for now.
			continue
		}
		origDesc := tb.TableDesc()
		// Instead of trying to use the original descriptor directly as template,
		// which would require us to "clean it up" to extricate it from any links
		// to other descriptors, we build a fresh new descriptor instead and take
		// over the "interesting" properties from the original descriptor.
		// For now, that's just the list of non-hidden columns.
		t := tbTemplate{
			namePat: origDesc.Name,
			desc:    startDescriptor(),
		}
		for _, origColDef := range origDesc.Columns {
			if origColDef.Hidden || origColDef.Inaccessible {
				continue
			}
			colID := t.desc.NextColumnID
			t.desc.NextColumnID++

			typ := origColDef.Type
			if typ.UserDefined() {
				typ = types.String
			}

			newColDef := descpb.ColumnDescriptor{
				ID:   colID,
				Type: typ,
				Name: origColDef.Name,
			}
			t.baseColumnNames = append(t.baseColumnNames, newColDef.Name)
			t.desc.Columns = append(t.desc.Columns, newColDef)
			t.desc.Families[0].ColumnIDs = append(
				t.desc.Families[0].ColumnIDs, colID)
			t.desc.Families[0].ColumnNames = append(
				t.desc.Families[0].ColumnNames, newColDef.Name)
		}
		g.templates = append(g.templates, t)
	}

	if len(g.templates) == 0 {
		panic(genError{pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
			"template name expansion did not find any tables")})
	}
}

var uniqueRowIDString = "unique_rowid()"

func defaultTemplate() tbTemplate {
	t := tbTemplate{
		namePat:         "test",
		baseColumnNames: []string{"name", "address"},
		desc:            startDescriptor(),
	}
	for _, colName := range t.baseColumnNames {
		colID := t.desc.NextColumnID
		t.desc.NextColumnID++
		t.desc.Columns = append(t.desc.Columns,
			descpb.ColumnDescriptor{ID: colID, Name: colName, Type: types.String})
		t.desc.Families[0].ColumnIDs = append(
			t.desc.Families[0].ColumnIDs, colID)
		t.desc.Families[0].ColumnNames = append(
			t.desc.Families[0].ColumnNames, colName)
	}
	return t
}

func startDescriptor() descpb.TableDescriptor {
	return descpb.TableDescriptor{
		Version: 1,
		State:   descpb.DescriptorState_PUBLIC,
		Columns: []descpb.ColumnDescriptor{
			{ID: 1, Name: "rowid", Type: types.Int, DefaultExpr: &uniqueRowIDString, Nullable: false, Hidden: true},
		},
		Families: []descpb.ColumnFamilyDescriptor{
			{
				ID:              0,
				Name:            "primary",
				ColumnNames:     []string{"rowid"},
				ColumnIDs:       []descpb.ColumnID{1},
				DefaultColumnID: 1,
			},
		},
		PrimaryIndex: descpb.IndexDescriptor{
			ID:                  1,
			Name:                "pk",
			KeyColumnIDs:        []descpb.ColumnID{1},
			KeyColumnNames:      []string{"rowid"},
			KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
			EncodingType:        catenumpb.PrimaryIndexEncoding,
			Version:             descpb.LatestIndexDescriptorVersion,
			ConstraintID:        1,
		},
		NextColumnID:     2,
		NextConstraintID: 2,
		NextIndexID:      2,
		NextFamilyID:     1,
		NextMutationID:   1,
		FormatVersion:    descpb.InterleavedFormatVersion,
	}
}
