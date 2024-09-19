// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package randgen

import (
	"context"
	"math/rand"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
)

// NewTestSchemaGenerator instantiates a TestSchemaGenerator
func NewTestSchemaGenerator(
	cfg TestSchemaGeneratorConfig,
	st *cluster.Settings,
	txn *kv.Txn,
	cat Catalog,
	descCollection *descs.Collection,
	callingUser username.SQLUsername,
	currentDatabase string,
	currentSearchPath sessiondata.SearchPath,
	kvTrace bool,
	idGen eval.DescIDGenerator,
	mon *mon.BytesMonitor,
	rand *rand.Rand,
) TestSchemaGenerator {
	dbPrivs := catpb.NewBaseDatabasePrivilegeDescriptor(callingUser)
	dbDefaultPrivs := catprivilege.MakeDefaultPrivilegeDescriptor(catpb.DefaultPrivilegeDescriptor_DATABASE)
	publicSchemaPrivs := catpb.NewPublicSchemaPrivilegeDescriptor(callingUser, true /*includeCreatePriv*/)
	publicSchemaPrivs.SetOwner(callingUser)
	g := &testSchemaGenerator{
		rand: rand,
	}

	g.ext.txn = txn
	g.ext.st = st
	g.ext.cat = cat
	g.ext.coll = descCollection
	g.ext.idGen = idGen
	g.ext.mon = mon

	g.cfg.TestSchemaGeneratorConfig = cfg
	g.cfg.user = callingUser
	g.cfg.kvTrace = kvTrace
	g.cfg.currentDatabase = currentDatabase
	g.cfg.currentSearchPath = currentSearchPath

	g.models.db = descpb.DatabaseDescriptor{
		Version:           1,
		Privileges:        dbPrivs,
		DefaultPrivileges: dbDefaultPrivs,
		Schemas: map[string]descpb.DatabaseDescriptor_SchemaInfo{
			catconstants.PublicSchemaName: {ID: 0 /* will be replaced later */},
		},
	}
	g.models.publicSc = descpb.SchemaDescriptor{
		Version:    1,
		Name:       catconstants.PublicSchemaName,
		Privileges: publicSchemaPrivs,
	}
	g.models.sc = descpb.SchemaDescriptor{
		Version: 1,
	}
	// Note: the table templates are loaded via loadTemplates() below.
	return g
}

const genEnabledSettingName = "sql.schema.test_object_generator.enabled"

// genEnabled controls access to the functionality. It may feel a
// little weird to have a cluster setting since seemingly an admin
// user could set this to whatever they want. But this is not so; with
// a setting, it's possible to add an override in the system tenant to
// force disable the functionality from an unprivileged secondary
// tenant.
var genEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	genEnabledSettingName,
	"enable the generate_test_objects function",
	true,
)

const genEnabledForNonAdminsSettingName = "sql.schema.test_object_generator.non_admin.enabled"

var genEnabledForNonAdmins = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	genEnabledForNonAdminsSettingName,
	"allow non-admin users to use the generate_test_objects function",
	false,
)

func (g *testSchemaGenerator) Generate(
	ctx context.Context,
) (finalCfg TestSchemaGeneratorConfig, err error) {
	if !genEnabled.Get(&g.ext.st.SV) {
		return finalCfg, errors.WithHint(
			pgerror.New(pgcode.ObjectNotInPrerequisiteState,
				"generation disabled by configuration"),
			"See the cluster setting "+genEnabledSettingName+".")
	}

	// Cache whether the user is an admin. We'll reuse this in
	// multiple places.
	isAdmin, err := g.ext.cat.HasAdminRole(ctx)
	if err != nil {
		return finalCfg, err
	}

	if !isAdmin && !genEnabledForNonAdmins.Get(&g.ext.st.SV) {
		return finalCfg, errors.WithHint(
			pgerror.New(pgcode.InsufficientPrivilege,
				"must have admin role to generate objects"),
			"See the cluster setting "+genEnabledForNonAdminsSettingName+".")
	}

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
			return errors.WithHint(pgerror.New(pgcode.Syntax,
				"name generation needs variability to generate objects"),
				"Add either noise or numbering to the name generation options.")
		}

		// Because we allocate desc IDs in advance of writing the batches,
		// and ID allocation is non-reversible, we reserve allocations of big
		// chunks of the ID space to admin users.
		const maxNewDescsPerNonAdmin = 10000
		if expDbs+expScs+expTbs > maxNewDescsPerNonAdmin && !isAdmin {
			return pgerror.Newf(pgcode.InsufficientPrivilege,
				"only admin users can generate more than %d descriptors at a time", maxNewDescsPerNonAdmin)
		}

		// Check that we have enough memory upfront.
		acc := g.ext.mon.MakeBoundAccount()
		defer acc.Close(ctx)
		g.checkMemoryUsage(ctx, &acc, expDbs, expScs, expTbs)

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
	return g.cfg.TestSchemaGeneratorConfig, err
}

type genError struct {
	err error
}

// Catalog interfaces with the schema resolver and privilege checker.
type Catalog interface {
	HasAdminRole(context.Context) (bool, error)
	CheckAnyPrivilege(context.Context, privilege.Object) error
	HasAnyPrivilege(context.Context, privilege.Object) (bool, error)
	CanCreateDatabase(context.Context) error
	CheckPrivilegeForUser(context.Context, privilege.Object, privilege.Kind, username.SQLUsername) error
	ExpandTableGlob(context.Context, tree.TablePattern) (tree.TableNames, descpb.IDs, error)
}

// testSchemaGenerator encapsulates the state of the generator.
type testSchemaGenerator struct {
	// cfg is the configuration provided by the client of this package.
	cfg struct {
		TestSchemaGeneratorConfig

		// Session parameters.
		user              username.SQLUsername
		currentDatabase   string
		currentSearchPath sessiondata.SearchPath
		kvTrace           bool
		isAdmin           bool
	}

	// ext is the interface to the stateful parts of the
	// rest of the system.
	ext struct {
		txn *kv.Txn
		st  *cluster.Settings
		mon *mon.BytesMonitor

		// Catalog interface.
		cat Catalog

		// Descriptor collection and ID generator.
		coll  *descs.Collection
		idGen eval.DescIDGenerator
	}

	// gencfg is the derived configuration.
	// It remains immutable through the generation.
	gencfg struct {
		// Which databases to generate or use.
		createDatabases bool
		numDatabases    int
		dbNamePat       string

		// Which schemas to generate or use.
		createSchemas            bool
		numSchemasPerDatabase    int
		scNamePat                string
		useGeneratedPublicSchema bool

		// Which tables to generate.
		createTables       bool
		numTablesPerSchema int
		tbNamePat          string
	}

	// The random number generator we're using.
	rand *rand.Rand

	target struct {
		// db is the database onto which new schemas/tables get created.
		db *dbdesc.Mutable
		// sc is the schema onto which new tables get created.
		sc catalog.SchemaDescriptor
	}

	models struct {
		// Template for the various descriptors being created. We define
		// them here and then write to them in-place in each generation
		// function so as to avoid heap allocations.
		db       descpb.DatabaseDescriptor
		publicSc descpb.SchemaDescriptor
		sc       descpb.SchemaDescriptor
		tb       []tbTemplate
	}

	output struct {
		// b is the current batch.
		b *kv.Batch
		// newDescs are the recently added descriptors that need a new
		// namespace entry.
		newDescs []catalog.MutableDescriptor
		// queuedDescs are the recently updated descriptors that need to be
		// written to KV.
		queuedDescs []catalog.MutableDescriptor
	}
}

func (g *testSchemaGenerator) prepareCounts(
	namePattern *tree.TableName,
) (expectedNewDbs, expectedNewSchemas, expectedNewTables int) {
	counts := g.cfg.Counts
	const reasonableMaxCount = 10000000
	for _, sz := range counts {
		// We cap the individual count to a maximum number for two purposes.
		// One is that abnormally large values are likely indicative
		// of a user mistake; in which case we want an informative error message
		// instead of something related to memory usage.
		// The second is that we want to avoid an overflow in the computation
		// of the byte monitor pre-allocation in checkMemoryUsage().
		if sz < 0 || sz > reasonableMaxCount {
			panic(genError{errors.WithHintf(pgerror.Newf(pgcode.Syntax, "invalid count"),
				"Each count must be between 0 and %d.", reasonableMaxCount)})
		}
	}

	g.gencfg.createDatabases = false
	g.gencfg.createSchemas = false
	g.gencfg.useGeneratedPublicSchema = false
	if len(counts) < 1 || len(counts) > 3 {
		panic(genError{errors.WithHint(pgerror.Newf(pgcode.Syntax, "invalid count"),
			"Must specify between 1 and 3 counts.")})
	}
	numNewDatabases := 0
	numNewSchemas := 0
	if len(counts) == 3 {
		if !namePattern.ExplicitCatalog {
			if namePattern.ExplicitSchema {
				namePattern.ExplicitCatalog = true
				namePattern.CatalogName = namePattern.SchemaName
				namePattern.SchemaName = catconstants.PublicSchemaName
				g.gencfg.useGeneratedPublicSchema = true
			} else {
				panic(genError{pgerror.Newf(pgcode.Syntax, "missing database name pattern")})
			}
		}
		g.gencfg.dbNamePat = string(namePattern.CatalogName)
		g.gencfg.createDatabases = true
		g.gencfg.numDatabases = counts[0]
		counts = counts[1:]
		numNewDatabases = g.gencfg.numDatabases
		numNewSchemas = g.gencfg.numDatabases
	}
	if len(counts) == 2 {
		if !namePattern.ExplicitSchema {
			panic(genError{pgerror.Newf(pgcode.Syntax, "missing schema name pattern")})
		}
		if counts[0] > 0 || !g.gencfg.useGeneratedPublicSchema {
			g.gencfg.createSchemas = true
			g.gencfg.scNamePat = string(namePattern.SchemaName)
			g.gencfg.numSchemasPerDatabase = counts[0]
			if g.gencfg.createDatabases {
				numNewSchemas += numNewDatabases * g.gencfg.numSchemasPerDatabase
			} else {
				numNewSchemas = g.gencfg.numSchemasPerDatabase
			}
		}
		counts = counts[1:]
	}
	g.gencfg.createTables = counts[0] > 0
	g.gencfg.numTablesPerSchema = counts[0]
	g.gencfg.tbNamePat = string(namePattern.ObjectName)
	numNewTables := g.gencfg.numTablesPerSchema
	if g.gencfg.createSchemas {
		numNewTables = numNewTables * g.gencfg.numSchemasPerDatabase
	}
	if g.gencfg.createDatabases {
		numNewTables = numNewTables * g.gencfg.numDatabases
	}
	if numNewDatabases+numNewSchemas+numNewTables > reasonableMaxCount {
		panic(genError{errors.WithHintf(
			pgerror.Newf(pgcode.Syntax, "too many objects generated"),
			"Can generate max %d at a time.", reasonableMaxCount)})
	}
	return numNewDatabases, numNewSchemas, numNewTables
}

func (g *testSchemaGenerator) checkMemoryUsage(
	ctx context.Context, acc *mon.BoundAccount, expDbs, expScs, expTbs int,
) {
	// Below we count the descriptors two times, one for the go struct
	// stored in the catalog and one for the protobuf encoding in the
	// KV batch.
	// We also count the length of the name patterns 4 times, because
	// we estimate that the noise generation can add up to 4 bytes
	// per byte in the name on average.
	if err := acc.Grow(ctx,
		// Descriptors.
		int64(expDbs)*(int64(2*unsafe.Sizeof(g.models.db))+
			int64(2*unsafe.Sizeof(g.models.publicSc)))+
			// Names and IDs.
			int64(g.gencfg.numDatabases)*(int64(4*len(g.gencfg.dbNamePat))+
				int64(unsafe.Sizeof(descpb.ID(0))))); err != nil {
		panic(genError{err})
	}
	if err := acc.Grow(ctx,
		// Descriptors.
		int64(expScs)*int64(2*unsafe.Sizeof(g.models.sc))+
			// Names and IDs.
			int64(g.gencfg.numSchemasPerDatabase)*(int64(4*len(g.gencfg.scNamePat))+
				int64(unsafe.Sizeof(descpb.ID(0))))); err != nil {
		panic(genError{err})
	}
	if err := acc.Grow(ctx,
		// Descriptors.
		int64(expTbs)*int64(2*unsafe.Sizeof(g.models.tb))+
			// Names and IDs.
			int64(g.gencfg.numTablesPerSchema)*(int64(4*len(g.gencfg.tbNamePat))+
				int64(unsafe.Sizeof(descpb.ID(0))))); err != nil {
		panic(genError{err})
	}
}

func (g *testSchemaGenerator) lookupBaseNamespaces(
	ctx context.Context, namePattern *tree.TableName,
) {
	if g.gencfg.createDatabases {
		// No base db nor schema - we're creating our own.
		return
	}

	if !namePattern.ExplicitCatalog {
		g.target.db = g.lookupDatabase(ctx, g.cfg.currentDatabase)
	} else {
		g.target.db = g.lookupDatabase(ctx, string(namePattern.CatalogName))
	}

	if g.gencfg.createSchemas || g.gencfg.useGeneratedPublicSchema {
		// No base schema - we're creating our own.
		return
	}

	db := g.target.db
	if !namePattern.ExplicitSchema {
		// Search for a schema. The good target schema is not necessarily
		// the first one in the search path. We need to iterate, like
		// resolver.ResolveTarget does.
		iter := g.cfg.currentSearchPath.IterWithoutImplicitPGSchemas()
		for scName, ok := iter.Next(); ok; scName, ok = iter.Next() {
			g.target.sc = g.lookupSchema(ctx, db, scName, false /* required */)
			if g.target.sc != nil {
				break
			}
		}
		if g.target.sc == nil {
			panic(genError{errors.New("no valid target schema found")})
		}
	} else {
		g.target.sc = g.lookupSchema(ctx, db, string(namePattern.SchemaName), true /* required */)
	}
}
