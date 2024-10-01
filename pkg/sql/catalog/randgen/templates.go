// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package randgen

import (
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

type tbTemplate struct {
	// namePat is the pattern to use to generate a table name.
	namePat string
	// desc is the descriptor template.
	desc descpb.TableDescriptor
	// baseColumnNames is the original names of the columns prior to
	// name randomization.
	baseColumnNames []string
}

func (g *testSchemaGenerator) loadTemplates(ctx context.Context) {
	if !g.gencfg.createTables {
		// No template needed.
		return
	}
	if len(g.cfg.TableTemplates) == 0 {
		// No template specified: we use a simple predefined table template.
		g.models.tb = append(g.models.tb, defaultTemplate())
		return
	}

	// The user has specified some table patterns. Look them up.
	objIDs := make(map[descpb.ID]struct{})
outer:
	for _, pat := range g.cfg.TableTemplates {
		// The user may specify a template either as an optionally
		// qualified table name, or a "table name pattern": a name whose
		// last component is a '*'.
		tbPat, err := parser.ParseTablePattern(pat)
		if err != nil {
			panic(genError{errors.Wrapf(err, "parsing template name %q", pat)})
		}

		// The pattern -> table IDs expansion is provided by the caller.
		// We share this code with the GRANT statement.
		_, ids, err := g.ext.cat.ExpandTableGlob(ctx, tbPat)
		if err != nil {
			panic(genError{errors.Wrapf(err, "expanding template name %q", pat)})
		}

		// De-dup the objects: if a user specifies e.g.
		// ["foo.*","foo.bar"] we should use foo.bar only once.
		for _, id := range ids {
			const maxTemplates = 100
			if len(objIDs) > maxTemplates {
				// Let's not let the template list get out of hand.
				break outer
			}
			objIDs[id] = struct{}{}
		}
	}

	// We want name generation and template reuse to be deterministic as
	// a function of the random seed, so we must remove non-determinism
	// coming from the go map order.
	sobjIDs := make([]descpb.ID, 0, len(objIDs))
	for id := range objIDs {
		sobjIDs = append(sobjIDs, id)
	}
	sort.Slice(sobjIDs, func(i, j int) bool { return sobjIDs[i] < sobjIDs[j] })

	// Look up the descriptors from the IDs.
	descs, err := g.ext.coll.ByIDWithoutLeased(g.ext.txn).WithoutNonPublic().Get().Descs(ctx, sobjIDs)
	if err != nil {
		panic(genError{errors.Wrap(err, "retrieving template descriptors")})
	}

	// Extract the templates.
	for _, desc := range descs {
		// Can this user even see this table?
		if ok, err := g.ext.cat.HasAnyPrivilege(ctx, desc); err != nil {
			panic(genError{err})
		} else if !ok {
			if len(descs) == 1 {
				// The pattern was specific to just one table, so let's be
				// helpful to the user about why it can't be used.
				panic(genError{
					pgerror.Newf(
						pgcode.InsufficientPrivilege,
						"user has no privileges on %s",
						desc.GetName(),
					),
				})
			} else {
				// The expansion resulted in multiple objects; we simply
				// ignore objects the user can't use.
				continue
			}
		}

		tb, ok := desc.(catalog.TableDescriptor)
		if !ok {
			// We don't support templating anything else than tables for now.
			continue
		}

		origDesc := tb.TableDesc()

		// Instead of trying to use the original descriptor directly as
		// template, which would require us to "clean it up" to extricate
		// it from any links to other descriptors and also call the
		// (expensive!) RunPostDeserializationChanges() method on the desc
		// builder, we build a fresh new descriptor instead and take over
		// the "interesting" properties from the original descriptor. For
		// now, that's just the list of non-hidden columns.
		t := tbTemplate{
			namePat: origDesc.Name,
			desc:    startDescriptor(),
		}

		for _, origColDef := range origDesc.Columns {
			// We don't take over hidden/inaccessible/virtual columns.
			if origColDef.Hidden || origColDef.Inaccessible || origColDef.Virtual {
				continue
			}
			colID := t.desc.NextColumnID
			t.desc.NextColumnID++

			typ := origColDef.Type
			// We can't depend on user-defined types because our generation
			// code does not handle inter-descriptor dependencies yet.
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

			// Add to the primary index as either a key or store column.
			for i, name := range origDesc.PrimaryIndex.KeyColumnNames {
				if name == origColDef.Name {
					t.desc.PrimaryIndex.KeyColumnIDs = append(t.desc.PrimaryIndex.KeyColumnIDs, colID)
					t.desc.PrimaryIndex.KeyColumnNames = append(t.desc.PrimaryIndex.KeyColumnNames, name)
					t.desc.PrimaryIndex.KeyColumnDirections = append(t.desc.PrimaryIndex.KeyColumnDirections, origDesc.PrimaryIndex.KeyColumnDirections[i])
					break
				}
			}
			for _, name := range origDesc.PrimaryIndex.StoreColumnNames {
				if name == origColDef.Name {
					t.desc.PrimaryIndex.StoreColumnIDs = append(t.desc.PrimaryIndex.StoreColumnIDs, colID)
					t.desc.PrimaryIndex.StoreColumnNames = append(t.desc.PrimaryIndex.StoreColumnNames, name)
					break
				}
			}
		}
		g.models.tb = append(g.models.tb, t)
	}

	if len(g.models.tb) == 0 {
		panic(genError{pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
			"template name expansion did not find any usable tables")})
	}
}

var uniqueRowIDString = "unique_rowid()"

// defaultTemplate provides a simple test template that is used when
// the caller does not specify any template.
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
		if colID == 0 {
			t.desc.PrimaryIndex.KeyColumnIDs = []descpb.ColumnID{colID}
			t.desc.PrimaryIndex.KeyColumnNames = []string{colName}
			t.desc.PrimaryIndex.KeyColumnDirections = []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC}
		} else {
			t.desc.PrimaryIndex.StoreColumnIDs = append(t.desc.PrimaryIndex.StoreColumnIDs, colID)
			t.desc.PrimaryIndex.StoreColumnNames = append(t.desc.PrimaryIndex.StoreColumnNames, colName)
		}
	}
	return t
}

// startDescriptor is used as a base table descriptor when building
// new templates.
func startDescriptor() descpb.TableDescriptor {
	return descpb.TableDescriptor{
		Version: 1,
		State:   descpb.DescriptorState_PUBLIC,
		Privileges: catpb.NewCustomSuperuserPrivilegeDescriptor(
			privilege.List{privilege.ALL}, username.RootUserName(),
		),
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
