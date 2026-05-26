// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	if g.gencfg.numTablesPerSchema <= 0 {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(genError); ok {
				e.err = errors.Wrapf(e.err,
					"generating %d tables with name pattern %q under schema %s.%s",
					g.gencfg.numTablesPerSchema, g.gencfg.tbNamePat,
					tree.NameString(db.GetName()), tree.NameString(sc.GetName()))
				panic(e)
			}
			panic(r)
		}
	}()

	// Compute the shared table privileges just once.
	privs, err := catprivilege.CreatePrivilegesFromDefaultPrivileges(
		db.GetDefaultPrivilegeDescriptor(),
		sc.GetDefaultPrivilegeDescriptor(),
		db.GetID(),
		g.cfg.user,
		privilege.Tables,
	)
	if err != nil {
		panic(genError{err: err})
	}

	// Compute the names ahead of time; this also takes care of
	// avoiding duplicates.
	conflictNames := g.objNamesInSchema(ctx, db, sc)
	selectTemplates, selectNames := g.selectNamesAndTemplates(ctx, conflictNames, g.gencfg.numTablesPerSchema)

	// Actually generate the tables.
	firstID := g.makeIDs(ctx, g.gencfg.numTablesPerSchema)
	for i := 0; i < g.gencfg.numTablesPerSchema; i++ {
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
	g.rand.Shuffle(len(g.models.tb), func(i, j int) {
		g.models.tb[i], g.models.tb[j] = g.models.tb[j], g.models.tb[i]
	})

	// How many names are we generating per template?
	namesPerTemplate := num / len(g.models.tb)
	if namesPerTemplate*len(g.models.tb) < num {
		namesPerTemplate++
	}

	// Names/templates, per generator.
	startTmpl := make([]*tbTemplate, 0, num)
	startNames := make([][]string, 0, num)
	if g.gencfg.tbNamePat == "_" {
		// Rotate through templates and their names.
		for i := range g.models.tb {
			tbNamePat := g.models.tb[i].namePat
			cfg := g.cfg.NameGen
			if namesPerTemplate == 1 {
				cfg.Suffix = false
				if !cfg.HasVariability() {
					cfg.Suffix = true
				}
			}
			ng := randident.NewNameGenerator(&cfg, g.rand, tbNamePat)
			tbNames, err := ng.GenerateMultiple(ctx, namesPerTemplate, conflictNames)
			if err != nil {
				panic(genError{err})
			}
			startNames = append(startNames, tbNames)
			startTmpl = append(startTmpl, &g.models.tb[i])
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
		ng := randident.NewNameGenerator(&g.cfg.NameGen, g.rand, g.gencfg.tbNamePat)
		tbNames, err := ng.GenerateMultiple(ctx, num, conflictNames)
		if err != nil {
			panic(genError{err})
		}
		selectNames = tbNames
		for len(selectTmpl) < num {
			for i := range g.models.tb {
				if len(selectTmpl) >= num {
					break
				}
				selectTmpl = append(selectTmpl, &g.models.tb[i])
			}
		}
	}
	return selectTmpl, selectNames
}

func (g *testSchemaGenerator) genOneTable(
	ctx context.Context,
	db catalog.DatabaseDescriptor,
	sc catalog.SchemaDescriptor,
	privs *catpb.PrivilegeDescriptor,
	id catid.DescID,
	tmpl *tbTemplate,
	tbName string,
) {
	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(genError); ok {
				e.err = errors.Wrapf(e.err,
					"generating table %s.%s.%s",
					tree.NameString(db.GetName()),
					tree.NameString(sc.GetName()),
					tree.NameString(tbName))
				panic(e)
			}
			panic(r)
		}
	}()

	tmpl.desc.ID = id
	tmpl.desc.Name = tbName
	tmpl.desc.PrimaryIndex.Name = tabledesc.PrimaryKeyIndexName(tbName)
	tmpl.desc.ParentID = db.GetID()
	tmpl.desc.UnexposedParentSchemaID = sc.GetID()
	tmpl.desc.Privileges = privs
	tmpl.desc.Temporary = sc.SchemaKind() == catalog.SchemaTemporary
	if g.cfg.RandomizeColumns {
		nameGenCfg := g.cfg.NameGen
		nameGenCfg.Suffix = false
		for i, cPat := range tmpl.baseColumnNames {
			ng := randident.NewNameGenerator(&nameGenCfg, g.rand, cPat)
			colName := ng.GenerateOne("0")
			tmpl.desc.Columns[i+1].Name = colName
			tmpl.desc.Families[0].ColumnNames[i+1] = colName
			for j := range tmpl.desc.PrimaryIndex.KeyColumnNames {
				if tmpl.desc.PrimaryIndex.KeyColumnIDs[j] == tmpl.desc.Columns[i+1].ID {
					tmpl.desc.PrimaryIndex.KeyColumnNames[j] = colName
				}
			}
			for j := range tmpl.desc.PrimaryIndex.StoreColumnNames {
				if tmpl.desc.PrimaryIndex.StoreColumnIDs[j] == tmpl.desc.Columns[i+1].ID {
					tmpl.desc.PrimaryIndex.StoreColumnNames[j] = colName
				}
			}
		}
		ng := randident.NewNameGenerator(&nameGenCfg, g.rand, "primary")
		idxName := ng.GenerateOne("0")
		tmpl.desc.PrimaryIndex.Name = idxName
	}

	// Note: we do not call RunPostDeserializationChanges here. This is
	// intentional: it incurs a large hit in performance. Also the call
	// is not needed, because we 100% control the structure of the
	// template and this is validated by tests.
	tb := tabledesc.NewBuilder(&tmpl.desc).BuildCreatedMutableTable()

	g.cfg.GeneratedCounts.Tables++
	g.newDesc(ctx, tb)
}
