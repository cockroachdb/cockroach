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
	"math"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/errors"
)

// NewTestSchemaGenerator instantiates a TestSchemaGenerator
func NewTestSchemaGenerator(
	cfg TestSchemaGeneratorConfig,
	txn *kv.Txn,
	cat Catalog,
	descCollection *descs.Collection,
	callingUser username.SQLUsername,
	currentDatabase string,
	currentSearchPath sessiondata.SearchPath,
	kvTrace bool,
	idGen eval.DescIDGenerator,
	rand *rand.Rand,
) TestSchemaGenerator {
	dbPrivs := catpb.NewBaseDatabasePrivilegeDescriptor(callingUser)
	dbDefaultPrivs := catprivilege.MakeDefaultPrivilegeDescriptor(catpb.DefaultPrivilegeDescriptor_DATABASE)
	publicSchemaPrivs := catpb.NewPublicSchemaPrivilegeDescriptor()
	publicSchemaPrivs.SetOwner(callingUser)
	return &testSchemaGenerator{
		cfg:               cfg,
		txn:               txn,
		cat:               cat,
		user:              callingUser,
		kvTrace:           kvTrace,
		coll:              descCollection,
		currentDatabase:   currentDatabase,
		currentSearchPath: currentSearchPath,
		idGen:             idGen,
		rand:              rand,

		dbTemplate: descpb.DatabaseDescriptor{
			Version:           1,
			Privileges:        dbPrivs,
			DefaultPrivileges: dbDefaultPrivs,
			Schemas: map[string]descpb.DatabaseDescriptor_SchemaInfo{
				tree.PublicSchema: {ID: 0 /* will be replaced later */},
			},
		},
		publicScTemplate: descpb.SchemaDescriptor{
			Version:    1,
			Name:       tree.PublicSchema,
			Privileges: publicSchemaPrivs,
		},
		scTemplate: descpb.SchemaDescriptor{
			Version: 1,
		},
	}
	// Note: the table templates are loaded via loadTemplates() below.
}

func (g *testSchemaGenerator) Generate(
	ctx context.Context,
) (finalCfg TestSchemaGeneratorConfig, err error) {
	err = func() (resErr error) {
		// The logic below uses panic-based error propagation.
		defer func() {
			if r := recover(); r != nil {
				if e, ok := r.(genError); ok {
					resErr = e.err
					return
				}
				panic(r)
			}
		}()

		// What names should we use for the various target objects?
		tn, err := parser.ParseQualifiedTableName(g.cfg.Names)
		if err != nil {
			return errors.Wrap(err, "invalid name pattern")
		}

		// What are we creating?
		expDbs, expScs, expTbs := g.prepareCounts(tn)

		if expDbs+expScs+expTbs > 0 && !g.cfg.NameGen.HasVariability() {
			// Without name variability, we cannot guarantee that unique
			// names will be generated, and we risk looping on name
			// generation forever.
			// Note that we need variability even to generate just 1 object,
			// because an object with the same name might already exist in
			// the namespace.
			return pgerror.New(pgcode.Syntax,
				"name generation needs variability to generate objects")
		}

		// Because we allocate desc IDs in advance of writing the batches,
		// and ID allocation is non-reversible, we reserve allocations of big
		// chunks of the ID space to admin users.
		const maxNewDescsPerNonAdmin = 10000
		if expDbs+expScs+expTbs > maxNewDescsPerNonAdmin && !g.isAdmin(ctx) {
			return pgerror.Newf(pgcode.InsufficientPrivilege,
				"only admin users can generate more than %d descriptors at a time", maxNewDescsPerNonAdmin)
		}

		// Where are we creating the objects?
		g.lookupBaseNamespaces(ctx, tn)

		// Which templates are we using?
		g.loadTemplates(ctx)

		g.reset()
		g.maybeGenDatabases(ctx)
		g.flush(ctx)

		if expDbs != g.cfg.GeneratedCounts.Databases ||
			expScs != g.cfg.GeneratedCounts.Schemas ||
			expTbs != g.cfg.GeneratedCounts.Tables {
			return errors.AssertionFailedf("generated count mistmatch: got (%d,%d,%d), expected (%d,%d,%d)",
				g.cfg.GeneratedCounts.Databases,
				g.cfg.GeneratedCounts.Schemas,
				g.cfg.GeneratedCounts.Tables,
				expDbs, expScs, expTbs,
			)
		}

		return nil
	}()
	return g.cfg, err
}

type genError struct {
	err error
}

// Catalog interfaces with the schema resolver and privilege checker.
type Catalog interface {
	HasAdminRole(context.Context) (bool, error)
	CheckAnyPrivilege(context.Context, privilege.Object) error
	CanCreateDatabase(context.Context) error
	CheckPrivilegeForUser(context.Context, privilege.Object, privilege.Kind, username.SQLUsername) error
	ExpandTableGlob(context.Context, tree.TablePattern) (tree.TableNames, descpb.IDs, error)
}

// testSchemaGenerator encapsulates the state of the generator.
type testSchemaGenerator struct {
	cfg TestSchemaGeneratorConfig

	txn *kv.Txn

	// Catalog interface.
	cat Catalog

	// Session parameters.
	user              username.SQLUsername
	currentDatabase   string
	currentSearchPath sessiondata.SearchPath
	kvTrace           bool

	// Descriptor collection and ID generator.
	coll  *descs.Collection
	idGen eval.DescIDGenerator

	// The random number generator we're using.
	rand *rand.Rand

	// Which databases to generate or use.
	createDatabases bool
	numDatabases    int
	dbNamePat       string
	baseDatabase    *dbdesc.Mutable

	// Which schemas to generate or use.
	createSchemas            bool
	numSchemasPerDatabase    int
	scNamePat                string
	useGeneratedPublicSchema bool
	baseSchema               catalog.SchemaDescriptor

	// Which tables to generate.
	createTables       bool
	numTablesPerSchema int
	tbNamePat          string

	// Template for the various descriptors being created. We define
	// them here and then write to them in-place in each generation
	// function so as to avoid heap allocations.
	dbTemplate       descpb.DatabaseDescriptor
	publicScTemplate descpb.SchemaDescriptor
	scTemplate       descpb.SchemaDescriptor

	templates []tbTemplate

	// b is the current batch.
	b *kv.Batch
	// newDescs are the recently added descriptors that need a new
	// namespace entry.
	newDescs []catalog.MutableDescriptor
	// queuedDescs are the recently updated descriptors that need to be
	// written to KV.
	queuedDescs []catalog.MutableDescriptor
}

func (g *testSchemaGenerator) prepareCounts(
	namePattern *tree.TableName,
) (expectedNewDbs, expectedNewSchemas, expectedNewTables int) {
	counts := g.cfg.Counts
	for _, sz := range counts {
		if sz < 0 || sz > math.MaxInt {
			panic(genError{errors.WithHintf(pgerror.Newf(pgcode.Syntax, "invalid count"),
				"Each count must be between 0 and %d.", math.MaxInt)})
		}
	}

	g.createDatabases = false
	g.createSchemas = false
	g.useGeneratedPublicSchema = false
	if len(counts) > 3 {
		panic(genError{errors.WithHint(pgerror.Newf(pgcode.Syntax, "too many counts"),
			"Must specify between 1 and 3 counts.")})
	}
	numNewDatabases := 0
	numNewSchemas := 0
	if len(counts) == 3 {
		if !namePattern.ExplicitCatalog {
			if namePattern.ExplicitSchema {
				namePattern.ExplicitCatalog = true
				namePattern.CatalogName = namePattern.SchemaName
				namePattern.SchemaName = tree.PublicSchemaName
				g.useGeneratedPublicSchema = true
			} else {
				panic(genError{pgerror.Newf(pgcode.Syntax, "missing database name pattern")})
			}
		}
		g.dbNamePat = string(namePattern.CatalogName)
		g.createDatabases = true
		g.numDatabases = counts[0]
		counts = counts[1:]
		numNewDatabases = g.numDatabases
		numNewSchemas = g.numDatabases
	}
	if len(counts) == 2 {
		if !namePattern.ExplicitSchema {
			panic(genError{pgerror.Newf(pgcode.Syntax, "missing schema name pattern")})
		}
		if counts[0] > 0 || !g.useGeneratedPublicSchema {
			g.createSchemas = true
			g.scNamePat = string(namePattern.SchemaName)
			g.numSchemasPerDatabase = counts[0]
			if g.createDatabases {
				numNewSchemas += numNewDatabases * g.numSchemasPerDatabase
			} else {
				numNewSchemas = g.numSchemasPerDatabase
			}
		}
		counts = counts[1:]
	}
	g.createTables = counts[0] > 0
	g.numTablesPerSchema = counts[0]
	g.tbNamePat = string(namePattern.ObjectName)
	numNewTables := g.numTablesPerSchema
	if g.createSchemas {
		numNewTables = numNewTables * g.numSchemasPerDatabase
	}
	if g.createDatabases {
		numNewTables = numNewTables * g.numDatabases
	}
	return numNewDatabases, numNewSchemas, numNewTables
}

func (g *testSchemaGenerator) lookupBaseNamespaces(
	ctx context.Context, namePattern *tree.TableName,
) {
	if g.createDatabases {
		// No base db nor schema - we're creating our own.
		return
	}

	if !namePattern.ExplicitCatalog {
		g.baseDatabase = g.lookupDatabase(ctx, g.currentDatabase)
	} else {
		g.baseDatabase = g.lookupDatabase(ctx, string(namePattern.CatalogName))
	}

	if g.createSchemas || g.useGeneratedPublicSchema {
		// No base schema - we're creating our own.
		return
	}

	db := g.baseDatabase
	if !namePattern.ExplicitSchema {
		// Search for a schema. The good target schema is not necessarily
		// the first one in the search path. We need to iterate, like
		// resolver.ResolveTarget does.
		iter := g.currentSearchPath.IterWithoutImplicitPGSchemas()
		for scName, ok := iter.Next(); ok; scName, ok = iter.Next() {
			g.baseSchema = g.lookupSchema(ctx, db, scName, false /* required */)
			if g.baseSchema != nil {
				break
			}
		}
		if g.baseSchema == nil {
			panic(genError{errors.New("no valid target schema found")})
		}
	} else {
		g.baseSchema = g.lookupSchema(ctx, db, string(namePattern.SchemaName), true /* required */)
	}
}
