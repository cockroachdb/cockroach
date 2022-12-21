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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/randident"
	"github.com/cockroachdb/errors"
)

func (g *testSchemaGenerator) genMultipleTables(
	ctx context.Context, db catalog.DatabaseDescriptor, sc catalog.SchemaDescriptor,
) {
	if g.numTablesPerSchema <= 0 {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(genError); ok {
				err := errors.Wrapf(e.err,
					"generating %d tables with name pattern %q under schema %s.%s",
					g.numTablesPerSchema, g.tbNamePat,
					tree.NameString(db.GetName()), tree.NameString(sc.GetName()))
				panic(genError{err})
			}
			panic(r)
		}
	}()

	// Compute the shared table privileges just once.
	privs := catprivilege.CreatePrivilegesFromDefaultPrivileges(
		db.GetDefaultPrivilegeDescriptor(),
		sc.GetDefaultPrivilegeDescriptor(),
		db.GetID(),
		g.user,
		privilege.Tables,
	)

	// Compute the names ahead of time; this also takes care of
	// avoiding duplicates.
	conflictNames := g.objNamesInSchema(ctx, db, sc)
	selectTemplates, selectNames := g.selectNamesAndTemplates(ctx, conflictNames, g.numTablesPerSchema)

	// Actually generate the tables.
	firstID := g.makeIDs(ctx, g.numTablesPerSchema)
	for i := 0; i < g.numTablesPerSchema; i++ {
		tmpl := selectTemplates[i]
		tbName := selectNames[i]
		id := firstID + catid.DescID(i)
		g.genOneTable(ctx, db, sc, privs, id, tmpl, tbName)
	}
}

// selectNamesAndTemplates selects table templates and assigns names to them.
//
// If the top level table pattern is "_", it computes as follows:
// given the templates a,b,c,
// and we are requested to produce 10 tables,
// then select (a, "a1"), (b, "b1"), (c "c1"), (a, "a2"), (b, "b2") ... (a, "a4")
//
// Otherwise, non-undescore "tb", it computes as follows:
// (a, "tb1"), (b, "tb2"), (c, "tb3"), (a, "tb4"), ... (a, "tb10")
//
// Additionally, the templates are shuffled so we get a different
// subset of the templates every time (useful in particular when num
// is smaller than the number of templates).
func (g *testSchemaGenerator) selectNamesAndTemplates(
	ctx context.Context, conflictNames map[string]struct{}, num int,
) (selectTmpl []*tbTemplate, selectNames []string) {
	// Let's consider a different template order every time.
	g.rand.Shuffle(len(g.templates), func(i, j int) {
		g.templates[i], g.templates[j] = g.templates[j], g.templates[i]
	})

	// How many names are we generating per template?
	namesPerTemplate := num / len(g.templates)
	if namesPerTemplate*len(g.templates) < num {
		namesPerTemplate++
	}

	// Names/templates, per generator.
	startTmpl := make([]*tbTemplate, 0, num)
	startNames := make([][]string, 0, num)
	if g.tbNamePat == "_" {
		// Rotate through templates and their names.
		for i := range g.templates {
			tbNamePat := g.templates[i].namePat
			cfg := g.cfg.NameGen
			if namesPerTemplate == 1 {
				cfg.Number = false
				if !cfg.HasVariability() {
					cfg.Number = true
				}
			}
			ng := randident.NewNameGenerator(&cfg, g.rand, tbNamePat)
			tbNames, err := ng.GenerateMultiple(ctx, namesPerTemplate, conflictNames)
			if err != nil {
				panic(genError{err})
			}
			startNames = append(startNames, tbNames)
			startTmpl = append(startTmpl, &g.templates[i])
		}
		// Now select the name,template pairs.
		for nameOffset := 0; len(selectNames) < num; nameOffset++ {
			for i := range startTmpl {
				if len(selectNames) >= num {
					break
				}
				selectTmpl = append(selectTmpl, startTmpl[i])
				selectNames = append(selectNames, startNames[i][nameOffset])
			}
		}
	} else {
		// Rotate through templates, but use a single sequence of names.
		ng := randident.NewNameGenerator(&g.cfg.NameGen, g.rand, g.tbNamePat)
		tbNames, err := ng.GenerateMultiple(ctx, num, conflictNames)
		if err != nil {
			panic(genError{err})
		}
		selectNames = tbNames
		for len(selectTmpl) < num {
			for i := range g.templates {
				if len(selectTmpl) >= num {
					break
				}
				selectTmpl = append(selectTmpl, &g.templates[i])
			}
		}
	}
	return selectTmpl, selectNames
}

func (g *testSchemaGenerator) genOneTable(
	ctx context.Context,
	parentDb catalog.DatabaseDescriptor,
	parentSc catalog.SchemaDescriptor,
	privs *catpb.PrivilegeDescriptor,
	id catid.DescID,
	tmpl *tbTemplate,
	tbName string,
) {
	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(genError); ok {
				err := errors.Wrapf(e.err,
					"generating table %s.%s.%s",
					tree.NameString(parentDb.GetName()),
					tree.NameString(parentSc.GetName()),
					tree.NameString(tbName))
				panic(genError{err})
			}
			panic(r)
		}
	}()

	tmpl.desc.ID = id
	tmpl.desc.Name = tbName
	tmpl.desc.ParentID = parentDb.GetID()
	tmpl.desc.UnexposedParentSchemaID = parentSc.GetID()
	tmpl.desc.Privileges = privs
	tmpl.desc.Temporary = parentSc.SchemaKind() == catalog.SchemaTemporary
	if g.cfg.RandomizeColumns {
		nameGenCfg := g.cfg.NameGen
		nameGenCfg.Number = false
		for i, cPat := range tmpl.baseColumnNames {
			ng := randident.NewNameGenerator(&nameGenCfg, g.rand, cPat)
			colName := ng.GenerateOne(0)
			tmpl.desc.Columns[i+1].Name = colName
			tmpl.desc.Families[0].ColumnNames[i+1] = colName
		}
	}

	tb := tabledesc.NewBuilder(&tmpl.desc).BuildCreatedMutableTable()

	g.cfg.GeneratedCounts.Tables++
	g.newDesc(ctx, tb)
}
