// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vtable

import (
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
)

// PGCatalogAm describes the schema of the pg_catalog.pg_am table.
// The catalog pg_am stores information about relation access methods.
// It's important to note that this table changed drastically between Postgres
// versions 9.5 and 9.6. We currently support both versions of this table.
// See: https://www.postgresql.org/docs/9.5/static/catalog-pg-am.html and
// https://www.postgresql.org/docs/9.6/static/catalog-pg-am.html.
const PGCatalogAm = `
CREATE TABLE pg_catalog.pg_am (
	oid OID,
	amname NAME,
	amstrategies INT2,
	amsupport INT2,
	amcanorder BOOL,
	amcanorderbyop BOOL,
	amcanbackward BOOL,
	amcanunique BOOL,
	amcanmulticol BOOL,
	amoptionalkey BOOL,
	amsearcharray BOOL,
	amsearchnulls BOOL,
	amstorage BOOL,
	amclusterable BOOL,
	ampredlocks BOOL,
	amkeytype OID,
	aminsert OID,
	ambeginscan OID,
	amgettuple OID,
	amgetbitmap OID,
	amrescan OID,
	amendscan OID,
	ammarkpos OID,
	amrestrpos OID,
	ambuild OID,
	ambuildempty OID,
	ambulkdelete OID,
	amvacuumcleanup OID,
	amcanreturn OID,
	amcostestimate OID,
	amoptions OID,
	amhandler OID,
	amtype "char"
)`

// PGCatalogAttrDef describes the schema of the pg_catalog.pg_attrdef table.
// https://www.postgresql.org/docs/9.5/catalog-pg-attrdef.html,
const PGCatalogAttrDef = `
CREATE TABLE pg_catalog.pg_attrdef (
	oid OID,
	adrelid OID NOT NULL,
	adnum INT2,
	adbin STRING,
	adsrc STRING,
  INDEX(adrelid)
)`

// PGCatalogAttribute describes the schema of the pg_catalog.pg_attribute table.
// https://www.postgresql.org/docs/12/catalog-pg-attribute.html,
const PGCatalogAttribute = `
CREATE TABLE pg_catalog.pg_attribute (
	attrelid OID NOT NULL,
	attname NAME,
	atttypid OID,
	attstattarget INT4,
	attlen INT2,
	attnum INT2,
	attndims INT4,
	attcacheoff INT4,
	atttypmod INT4,
	attbyval BOOL,
	attstorage "char",
	attalign "char",
	attnotnull BOOL,
	atthasdef BOOL,
	attidentity "char", 
	attgenerated "char",
	attisdropped BOOL,
	attislocal BOOL,
	attinhcount INT4,
	attcollation OID,
	attacl STRING[],
	attoptions STRING[],
	attfdwoptions STRING[],
	atthasmissing BOOL,
	attmissingval STRING[],
	attishidden BOOL, -- CRDB only field to indicate if a column is NOT VISIBLE.
  INDEX(attrelid)
)`

// PGCatalogCast describes the schema of the pg_catalog.pg_cast table.
// https://www.postgresql.org/docs/9.6/catalog-pg-cast.html,
const PGCatalogCast = `
CREATE TABLE pg_catalog.pg_cast (
	oid OID,
	castsource OID,
	casttarget OID,
	castfunc OID,
	castcontext "char",
	castmethod "char"
)`

// PGCatalogAuthID describes the schema of the pg_catalog.pg_authid table.
// https://www.postgresql.org/docs/9.5/catalog-pg-authid.html,
const PGCatalogAuthID = `
CREATE TABLE pg_catalog.pg_authid (
  oid OID,
  rolname NAME,
  rolsuper BOOL,
  rolinherit BOOL,
  rolcreaterole BOOL,
  rolcreatedb BOOL,
  rolcanlogin BOOL,
  rolreplication BOOL,
  rolbypassrls BOOL,
  rolconnlimit INT4,
  rolpassword TEXT, 
  rolvaliduntil TIMESTAMPTZ
)`

// PGCatalogAuthMembers describes the schema of the pg_catalog.pg_auth_members
// table.
// https://www.postgresql.org/docs/9.5/catalog-pg-auth-members.html,
const PGCatalogAuthMembers = `
CREATE TABLE pg_catalog.pg_auth_members (
	roleid OID,
	member OID,
	grantor OID,
	admin_option BOOL
)`

// PGCatalogAvailableExtensions describes the schema of the
// pg_catalog.pg_available_extensions table.
// https://www.postgresql.org/docs/9.6/view-pg-available-extensions.html,
const PGCatalogAvailableExtensions = `
CREATE TABLE pg_catalog.pg_available_extensions (
	name NAME,
	default_version TEXT,
	installed_version TEXT,
	comment TEXT
)`

// PGCatalogClass describes the schema of the pg_catalog.pg_class table.
// https://www.postgresql.org/docs/9.5/catalog-pg-class.html,
const PGCatalogClass = `
CREATE TABLE pg_catalog.pg_class (
	oid OID NOT NULL,
	relname NAME NOT NULL,
	relnamespace OID,
	reltype OID,
	reloftype OID,
	relowner OID,
	relam OID,
	relfilenode OID,
	reltablespace OID,
	relpages INT4,
	reltuples FLOAT4,
	relallvisible INT4,
	reltoastrelid OID,
	relhasindex BOOL,
	relisshared BOOL,
	relpersistence "char",
	relistemp BOOL,
	relkind "char",
	relnatts INT2,
	relchecks INT2,
	relhasoids BOOL,
	relhaspkey BOOL,
	relhasrules BOOL,
	relhastriggers BOOL,
	relhassubclass BOOL,
	relfrozenxid INT,
	relacl STRING[],
	reloptions STRING[],
	relforcerowsecurity BOOL,
	relispartition BOOL,
	relispopulated BOOL,
	relreplident "char",
	relrewrite OID,
	relrowsecurity BOOL,
	relpartbound STRING,
	relminmxid INT,
  INDEX (oid)
)`

// PGCatalogCollation describes the schema of the pg_catalog.pg_collation table.
// https://www.postgresql.org/docs/9.5/catalog-pg-collation.html,
const PGCatalogCollation = `
CREATE TABLE pg_catalog.pg_collation (
  oid OID,
  collname STRING,
  collnamespace OID,
  collowner OID,
  collencoding INT4,
  collcollate STRING,
  collctype STRING,
  collprovider "char",
  collversion STRING,
  collisdeterministic BOOL
)`

// PGCatalogConstraint describes the schema of the pg_catalog.pg_constraint
// table.
// https://www.postgresql.org/docs/9.5/catalog-pg-constraint.html,
const PGCatalogConstraint = `
CREATE TABLE pg_catalog.pg_constraint (
	oid OID,
	conname NAME,
	connamespace OID,
	contype "char",
	condeferrable BOOL,
	condeferred BOOL,
	convalidated BOOL,
	conrelid OID NOT NULL,
	contypid OID,
	conindid OID,
	confrelid OID,
	confupdtype "char",
	confdeltype "char",
	confmatchtype "char",
	conislocal BOOL,
	coninhcount INT4,
	connoinherit BOOL,
	conkey INT2[],
	confkey INT2[],
	conpfeqop OID[],
	conppeqop OID[],
	conffeqop OID[],
	conexclop OID[],
	conbin STRING,
	consrc STRING,
	-- condef is a CockroachDB extension that provides a SHOW CREATE CONSTRAINT
	-- style string, for use by pg_get_constraintdef().
	condef STRING,
	conparentid OID,
  INDEX (conrelid)
)`

// PGCatalogConversion describes the schema of the pg_catalog.pg_conversion
// table.
// https://www.postgresql.org/docs/9.6/catalog-pg-conversion.html,
const PGCatalogConversion = `
CREATE TABLE pg_catalog.pg_conversion (
	oid OID,
	conname NAME,
	connamespace OID,
	conowner OID,
	conforencoding INT4,
	contoencoding INT4,
	conproc OID,
	condefault BOOL
)`

// PGCatalogDatabase describes the schema of the pg_catalog.pg_database table.
// https://www.postgresql.org/docs/9.5/catalog-pg-database.html,
const PGCatalogDatabase = `
CREATE TABLE pg_catalog.pg_database (
	oid OID,
	datname Name,
	datdba OID,
	encoding INT4,
	datcollate STRING,
	datctype STRING,
	datistemplate BOOL,
	datallowconn BOOL,
	datconnlimit INT4,
	datlastsysoid OID,
	datfrozenxid INT,
	datminmxid INT,
	dattablespace OID,
	datacl STRING[]
)`

// PGCatalogDefaultACL describes the schema of the pg_catalog.pg_default_acl
// table.
// https://www.postgresql.org/docs/9.6/catalog-pg-default-acl.html,
const PGCatalogDefaultACL = `
CREATE TABLE pg_catalog.pg_default_acl (
	oid OID,
	defaclrole OID,
	defaclnamespace OID,
	defaclobjtype "char",
	defaclacl STRING[]
)`

// PGCatalogDepend describes the schema of the pg_catalog.pg_depend table.
// https://www.postgresql.org/docs/9.5/catalog-pg-depend.html,
const PGCatalogDepend = `
CREATE TABLE pg_catalog.pg_depend (
  classid OID,
  objid OID,
  objsubid INT4,
  refclassid OID,
  refobjid OID,
  refobjsubid INT4,
  deptype "char"
)`

// PGCatalogDescription describes the schema of the pg_catalog.pg_description
// table.
// https://www.postgresql.org/docs/9.5/catalog-pg-description.html,
var PGCatalogDescription = `
CREATE VIEW pg_catalog.pg_description AS SELECT
  objoid, classoid, objsubid, description
FROM crdb_internal.kv_catalog_comments
WHERE classoid != ` + strconv.Itoa(catconstants.PgCatalogDatabaseTableID) + `
UNION ALL
	SELECT
	oid AS objoid,
	` + strconv.Itoa(catconstants.PgCatalogProcTableID) + `:::oid AS classoid,
	0:::INT4 AS objsubid,
	description AS description
	FROM crdb_internal.kv_builtin_function_comments
`

// PGCatalogSharedDescription describes the schema of the
// pg_catalog.pg_shdescription table.
// https://www.postgresql.org/docs/9.5/catalog-pg-shdescription.html,
var PGCatalogSharedDescription = `
CREATE VIEW pg_catalog.pg_shdescription AS
SELECT objoid, classoid, description
FROM "".crdb_internal.kv_catalog_comments
WHERE classoid = ` + strconv.Itoa(catconstants.PgCatalogDatabaseTableID) + `:::oid`

// PGCatalogEnum describes the schema of the pg_catalog.pg_enum table.
// https://www.postgresql.org/docs/9.5/catalog-pg-enum.html,
const PGCatalogEnum = `
CREATE TABLE pg_catalog.pg_enum (
  oid OID,
  enumtypid OID,
  enumsortorder FLOAT4,
  enumlabel STRING
)`

// PGCatalogEventTrigger describes the schema of the pg_catalog.pg_event_trigger
// table.
// https://www.postgresql.org/docs/9.6/catalog-pg-event-trigger.html,
const PGCatalogEventTrigger = `
CREATE TABLE pg_catalog.pg_event_trigger (
	evtname NAME,
	evtevent NAME,
	evtowner OID,
	evtfoid OID,
	evtenabled "char",
	evttags TEXT[],
	oid OID
)`

// PGCatalogExtension describes the schema of the pg_catalog.pg_extension table.
// https://www.postgresql.org/docs/9.5/catalog-pg-extension.html,
const PGCatalogExtension = `
CREATE TABLE pg_catalog.pg_extension (
  oid OID,
  extname NAME,
  extowner OID,
  extnamespace OID,
  extrelocatable BOOL,
  extversion STRING,
  extconfig STRING,
  extcondition STRING
)`

// PGCatalogForeignDataWrapper describes the schema of the
// pg_catalog.pg_foreign_data_wrapper table.
// https://www.postgresql.org/docs/9.5/catalog-pg-foreign-data-wrapper.html,
const PGCatalogForeignDataWrapper = `
CREATE TABLE pg_catalog.pg_foreign_data_wrapper (
  oid OID,
  fdwname NAME,
  fdwowner OID,
  fdwhandler OID,
  fdwvalidator OID,
  fdwacl STRING[],
  fdwoptions STRING[]
)`

// PGCatalogForeignServer describes the schema of the
// pg_catalog.pg_foreign_server table.
// https://www.postgresql.org/docs/9.5/catalog-pg-foreign-server.html,
const PGCatalogForeignServer = `
CREATE TABLE pg_catalog.pg_foreign_server (
  oid OID,
  srvname NAME,
  srvowner OID,
  srvfdw OID,
  srvtype STRING,
  srvversion STRING,
  srvacl STRING[],
  srvoptions STRING[]
)`

// PGCatalogForeignTable describes the schema of the pg_catalog.pg_foreign_table
// table.
// https://www.postgresql.org/docs/9.5/catalog-pg-foreign-table.html,
const PGCatalogForeignTable = `
CREATE TABLE pg_catalog.pg_foreign_table (
  ftrelid OID,
  ftserver OID,
  ftoptions STRING[]
)`

// PGCatalogIndex describes the schema of the pg_catalog.pg_index table.
// https://www.postgresql.org/docs/9.5/catalog-pg-index.html,
const PGCatalogIndex = `
CREATE TABLE pg_catalog.pg_index (
    indexrelid OID,
    indrelid OID,
    indnatts INT2,
    indisunique BOOL,
    indnullsnotdistinct BOOL,
    indisprimary BOOL,
    indisexclusion BOOL,
    indimmediate BOOL,
    indisclustered BOOL,
    indisvalid BOOL,
    indcheckxmin BOOL,
    indisready BOOL,
    indislive BOOL,
    indisreplident BOOL,
    indkey INT2VECTOR,
    indcollation OIDVECTOR,
    indclass OIDVECTOR,
    indoption INT2VECTOR,
    indexprs STRING,
    indpred STRING,
	indnkeyatts INT2
)`

// PGCatalogIndexes describes the schema of the pg_catalog.pg_indexes table.
// https://www.postgresql.org/docs/9.5/view-pg-indexes.html,
// Note: crdb_oid is an extension of the schema to much more easily map
// index OIDs to the corresponding index definition.
const PGCatalogIndexes = `
CREATE TABLE pg_catalog.pg_indexes (
	crdb_oid OID,
	schemaname NAME,
	tablename NAME,
	indexname NAME,
	tablespace NAME,
	indexdef STRING
)`

// PGCatalogInherits describes the schema of the pg_catalog.pg_inherits table.
// https://www.postgresql.org/docs/9.5/catalog-pg-inherits.html,
const PGCatalogInherits = `
CREATE TABLE pg_catalog.pg_inherits (
	inhrelid OID,
	inhparent OID,
	inhseqno INT4
)`

// PGCatalogLanguage describes the schema of the pg_catalog.pg_language table.
// https://www.postgresql.org/docs/9.5/catalog-pg-language.html,
const PGCatalogLanguage = `
CREATE TABLE pg_catalog.pg_language (
	oid OID,
	lanname NAME,
	lanowner OID,
	lanispl BOOL,
	lanpltrusted BOOL,
	lanplcallfoid OID,
	laninline OID,
	lanvalidator OID,
	lanacl STRING[]
)`

// PGCatalogLocks describes the schema of the pg_catalog.pg_locks table.
// https://www.postgresql.org/docs/9.6/view-pg-locks.html,
const PGCatalogLocks = `
CREATE TABLE pg_catalog.pg_locks (
  locktype TEXT,
  database OID,
  relation OID,
  page INT4,
  tuple SMALLINT,
  virtualxid TEXT,
  transactionid INT,
  classid OID,
  objid OID,
  objsubid SMALLINT,
  virtualtransaction TEXT,
  pid INT4,
  mode TEXT,
  granted BOOLEAN,
  fastpath BOOLEAN
)`

// PGCatalogMatViews describes the schema of the pg_catalog.pg_matviews table.
// https://www.postgresql.org/docs/9.6/view-pg-matviews.html,
const PGCatalogMatViews = `
CREATE TABLE pg_catalog.pg_matviews (
  schemaname NAME,
  matviewname NAME,
  matviewowner NAME,
  tablespace NAME,
  hasindexes BOOL,
  ispopulated BOOL,
  definition TEXT
)`

// PGCatalogNamespace describes the schema of the pg_catalog.pg_namespace table.
// https://www.postgresql.org/docs/9.5/catalog-pg-namespace.html,
const PGCatalogNamespace = `
CREATE TABLE pg_catalog.pg_namespace (
	oid OID,
	nspname NAME NOT NULL,
	nspowner OID,
	nspacl STRING[],
	INDEX (oid)
)`

// PGCatalogOpclass describes the schema of the pg_catalog.pg_opclass table.
// https://www.postgresql.org/docs/12/catalog-pg-opclass.html
const PGCatalogOpclass = `
CREATE TABLE pg_catalog.pg_opclass (
	oid OID,
	opcmethod OID,
	opcname NAME,
	opcnamespace OID,
	opcowner OID,
	opcfamily OID,
	opcintype OID,
	opcdefault BOOL,
	opckeytype OID
)`

// PGCatalogOperator describes the schema of the pg_catalog.pg_operator table.
// https://www.postgresql.org/docs/9.5/catalog-pg-operator.html,
const PGCatalogOperator = `
CREATE TABLE pg_catalog.pg_operator (
	oid OID,
	oprname NAME,
	oprnamespace OID,
	oprowner OID,
	oprkind "char",
	oprcanmerge BOOL,
	oprcanhash BOOL,
	oprleft OID,
	oprright OID,
	oprresult OID,
	oprcom OID,
	oprnegate OID,
	oprcode OID,
	oprrest OID,
	oprjoin OID
)`

// PGCatalogPreparedXacts describes the schema of the
// pg_catalog.pg_prepared_xacts table.
// https://www.postgresql.org/docs/9.6/view-pg-prepared-xacts.html,
const PGCatalogPreparedXacts = `
CREATE TABLE pg_catalog.pg_prepared_xacts (
  transaction INTEGER,
  gid TEXT,
  prepared TIMESTAMP WITH TIME ZONE,
  owner NAME,
  database NAME
)`

// PGCatalogPreparedStatements describes the schema of the
// pg_catalog.pg_prepared_statements table.
// The statement field differs in that it uses the parsed version
// of the PREPARE statement.
// The parameter_types field differs from Postgres as the type names in
// CockroachDB are slightly different.
// https://www.postgresql.org/docs/9.6/view-pg-prepared-statements.html,
const PGCatalogPreparedStatements = `
CREATE TABLE pg_catalog.pg_prepared_statements (
	name TEXT,
	statement TEXT,
	prepare_time TIMESTAMPTZ,
	parameter_types REGTYPE[],
	from_sql boolean
)`

// PGCatalogProc describes the schema of the pg_catalog.pg_proc table.
// https://www.postgresql.org/docs/16/catalog-pg-proc.html,
const PGCatalogProc = `
CREATE TABLE pg_catalog.pg_proc (
	oid OID,
	proname NAME,
	pronamespace OID,
	proowner OID,
	prolang OID,
	procost FLOAT4,
	prorows FLOAT4,
	provariadic OID,
	prosupport REGPROC,
	prokind "char",
	prosecdef BOOL,
	proleakproof BOOL,
	proisstrict BOOL,
	proretset BOOL,
	provolatile "char",
	proparallel "char",
	pronargs INT2,
	pronargdefaults INT2,
	prorettype OID,
	proargtypes OIDVECTOR,
	proallargtypes OID[],
	proargmodes "char"[],
	proargnames STRING[],
	proargdefaults STRING,
	protrftypes OID[],
	prosrc STRING,
	probin STRING,
	prosqlbody STRING,
	proconfig STRING[],
	proacl STRING[],
	INDEX(oid)
)`

// PGCatalogRange describes the schema of the pg_catalog.pg_range table.
// https://www.postgresql.org/docs/9.5/catalog-pg-range.html,
const PGCatalogRange = `
CREATE TABLE pg_catalog.pg_range (
	rngtypid OID,
	rngsubtype OID,
	rngcollation OID,
	rngsubopc OID,
	rngcanonical OID,
	rngsubdiff OID
)`

// PGCatalogRewrite describes the schema of the pg_catalog.pg_rewrite table.
// https://www.postgresql.org/docs/9.5/catalog-pg-rewrite.html,
const PGCatalogRewrite = `
CREATE TABLE pg_catalog.pg_rewrite (
	oid OID,
	rulename NAME,
	ev_class OID,
	ev_type "char",
	ev_enabled "char",
	is_instead BOOL,
	ev_qual TEXT,
	ev_action TEXT
)`

// PGCatalogRoles describes the schema of the pg_catalog.pg_roles table.
// https://www.postgresql.org/docs/9.5/view-pg-roles.html,
const PGCatalogRoles = `
CREATE TABLE pg_catalog.pg_roles (
	oid OID,
	rolname NAME,
	rolsuper BOOL,
	rolinherit BOOL,
	rolcreaterole BOOL,
	rolcreatedb BOOL,
	rolcatupdate BOOL,
	rolcanlogin BOOL,
	rolreplication BOOL,
	rolconnlimit INT4,
	rolpassword STRING,
	rolvaliduntil TIMESTAMPTZ,
	rolbypassrls BOOL,
	rolconfig STRING[]
)`

// PGCatalogSecLabels describes the schema of the pg_catalog.pg_seclabels table.
// https://www.postgresql.org/docs/9.6/view-pg-seclabels.html,
const PGCatalogSecLabels = `
CREATE TABLE pg_catalog.pg_seclabels (
	objoid OID,
  classoid OID,
  objsubid INT4,
  objtype TEXT,
	objnamespace OID,
	objname TEXT,
	provider TEXT,
	label TEXT
)`

// PGCatalogSequence describes the schema of the pg_catalog.pg_sequence table.
// https://www.postgresql.org/docs/9.5/catalog-pg-sequence.html,
const PGCatalogSequence = `
CREATE TABLE pg_catalog.pg_sequence (
	seqrelid OID,
	seqtypid OID,
	seqstart INT8,
	seqincrement INT8,
	seqmax INT8,
	seqmin INT8,
	seqcache INT8,
	seqcycle BOOL
)`

// PGCatalogSettings describes the schema of the pg_catalog.pg_settings table.
// https://www.postgresql.org/docs/9.5/catalog-pg-settings.html,
const PGCatalogSettings = `
CREATE TABLE pg_catalog.pg_settings (
    name STRING,
    setting STRING,
    unit STRING,
    category STRING,
    short_desc STRING,
    extra_desc STRING,
    context STRING,
    vartype STRING,
    source STRING,
    min_val STRING,
    max_val STRING,
    enumvals STRING,
    boot_val STRING,
    reset_val STRING,
    sourcefile STRING,
    sourceline INT4,
    pending_restart BOOL
)`

// PGCatalogShdepend describes the schema of the pg_catalog. table.
// https://www.postgresql.org/docs/9.6/catalog-pg-shdepend.html,
const PGCatalogShdepend = `
CREATE TABLE pg_catalog.pg_shdepend (
	dbid OID,
	classid OID,
	objid OID,
  objsubid INT4,
	refclassid OID,
	refobjid OID,
	deptype "char"
)`

// PGCatalogTables describes the schema of the pg_catalog.pg_tables table.
// https://www.postgresql.org/docs/9.5/view-pg-tables.html,
const PGCatalogTables = `
CREATE TABLE pg_catalog.pg_tables (
	schemaname NAME,
	tablename NAME,
	tableowner NAME,
	tablespace NAME,
	hasindexes BOOL,
	hasrules BOOL,
	hastriggers BOOL,
	rowsecurity BOOL
)`

// PGCatalogTablespace describes the schema of the pg_catalog.pg_tablespace
// table.
// https://www.postgresql.org/docs/9.5/catalog-pg-tablespace.html,
const PGCatalogTablespace = `
CREATE TABLE pg_catalog.pg_tablespace (
	oid OID,
	spcname NAME,
	spcowner OID,
	spclocation TEXT,
	spcacl TEXT[],
	spcoptions TEXT[]
)`

// PGCatalogTrigger describes the schema of the pg_catalog.pg_trigger table.
// https://www.postgresql.org/docs/9.5/catalog-pg-trigger.html,
const PGCatalogTrigger = `
CREATE TABLE pg_catalog.pg_trigger (
	oid OID,
	tgrelid OID,
	tgname NAME,
	tgfoid OID,
	tgtype INT2,
	tgenabled "char",
	tgisinternal BOOL,
	tgconstrrelid OID,
	tgconstrindid OID,
	tgconstraint OID,
	tgdeferrable BOOL,
	tginitdeferred BOOL,
	tgnargs INT2,
	tgattr INT2VECTOR,
	tgargs BYTEA,
	tgqual TEXT,
	tgoldtable NAME,
	tgnewtable NAME,
	tgparentid OID
)`

// PGCatalogType describes the schema of the pg_catalog.pg_type table.
// https://www.postgresql.org/docs/9.5/catalog-pg-type.html,
const PGCatalogType = `
CREATE TABLE pg_catalog.pg_type (
	oid OID NOT NULL,
	typname NAME NOT NULL,
	typnamespace OID,
	typowner OID,
	typlen INT2,
	typbyval BOOL,
	typtype "char",
	typcategory "char",
	typispreferred BOOL,
	typisdefined BOOL,
	typdelim "char",
	typrelid OID,
	typelem OID,
	typarray OID,
	typinput REGPROC,
	typoutput REGPROC,
	typreceive REGPROC,
	typsend REGPROC,
	typmodin REGPROC,
	typmodout REGPROC,
	typanalyze REGPROC,
	typalign "char",
	typstorage "char",
	typnotnull BOOL,
	typbasetype OID,
	typtypmod INT4,
	typndims INT4,
	typcollation OID,
	typdefaultbin STRING,
	typdefault STRING,
	typacl STRING[],
  INDEX(oid)
)`

// PGCatalogUser describes the schema of the pg_catalog.pg_user table.
// https://www.postgresql.org/docs/9.5/view-pg-user.html,
const PGCatalogUser = `
CREATE TABLE pg_catalog.pg_user (
	usename NAME,
	usesysid OID,
	usecreatedb BOOL,
	usesuper BOOL,
	userepl  BOOL,
	usebypassrls BOOL,
	passwd TEXT,
	valuntil TIMESTAMPTZ,
	useconfig TEXT[]
)`

// PGCatalogUserMapping describes the schema of the pg_catalog.pg_user_mapping
// table.
// https://www.postgresql.org/docs/9.5/catalog-pg-user-mapping.html,
const PGCatalogUserMapping = `
CREATE TABLE pg_catalog.pg_user_mapping (
	oid OID,
	umuser OID,
	umserver OID,
	umoptions TEXT[]
)`

// PGCatalogStatActivity describes the schema of the pg_catalog.pg_stat_activity
// table.
// https://www.postgresql.org/docs/9.6/monitoring-stats.html#PG-STAT-ACTIVITY-VIEW,
const PGCatalogStatActivity = `
CREATE TABLE pg_catalog.pg_stat_activity (
	datid OID,
	datname NAME,
	pid INTEGER,
	usesysid OID,
	usename NAME,
	application_name TEXT,
	client_addr INET,
	client_hostname TEXT,
	client_port INTEGER,
	backend_start TIMESTAMPTZ,
	xact_start TIMESTAMPTZ,
	query_start TIMESTAMPTZ,
	state_change TIMESTAMPTZ,
	wait_event_type TEXT,
	wait_event TEXT,
	state TEXT,
	backend_xid INTEGER,
	backend_xmin INTEGER,
	query TEXT,
	backend_type STRING,
	leader_pid INT4
)`

// PGCatalogSecurityLabel describes the schema of the pg_catalog.pg_seclabel
// table.
// https://www.postgresql.org/docs/9.5/catalog-pg-seclabel.html,
const PGCatalogSecurityLabel = `
CREATE TABLE pg_catalog.pg_seclabel (
	objoid OID,
	classoid OID,
	objsubid INTEGER,
	provider TEXT,
	label TEXT
)`

// PGCatalogSharedSecurityLabel describes the schema of the
// pg_catalog.pg_shseclabel table.
// https://www.postgresql.org/docs/9.5/catalog-pg-shseclabel.html,
const PGCatalogSharedSecurityLabel = `
CREATE TABLE pg_catalog.pg_shseclabel (
	objoid OID,
	classoid OID,
	provider TEXT,
	label TEXT
)`

// PGCatalogViews describes the schema of the pg_catalog.pg_views table.
// https://www.postgresql.org/docs/9.5/view-pg-views.html,
const PGCatalogViews = `
CREATE TABLE pg_catalog.pg_views (
	schemaname NAME,
	viewname NAME,
	viewowner NAME,
	definition STRING
)`

// PGCatalogAggregate describes the schema of the pg_catalog.pg_aggregate table.
// https://www.postgresql.org/docs/9.6/catalog-pg-aggregate.html,
const PGCatalogAggregate = `
CREATE TABLE pg_catalog.pg_aggregate (
	aggfnoid REGPROC,
	aggkind  "char",
	aggnumdirectargs INT2,
	aggtransfn REGPROC,
	aggfinalfn REGPROC,
	aggcombinefn REGPROC,
	aggserialfn REGPROC,
	aggdeserialfn REGPROC,
	aggmtransfn REGPROC,
	aggminvtransfn REGPROC,
	aggmfinalfn REGPROC,
	aggfinalextra BOOL,
	aggmfinalextra BOOL,
	aggsortop OID,
	aggtranstype OID,
	aggtransspace INT4,
	aggmtranstype OID,
	aggmtransspace INT4,
	agginitval TEXT,
	aggminitval TEXT,
	aggfinalmodify "char",
	aggmfinalmodify "char"
)`

// PgCatalogDbRoleSetting contains the default values that have been configured
// for session variables, for each role and database combination. This table
// contains the same data no matter which database the current session is using.
const PgCatalogDbRoleSetting = `
CREATE TABLE pg_catalog.pg_db_role_setting (
	setconfig STRING[],
	setdatabase OID,
	setrole OID
)`

// PgCatalogShadow is the implementation of pg_catalog.pg_shadow
// see https://www.postgresql.org/docs/13/view-pg-shadow.html
const PgCatalogShadow = `
CREATE TABLE pg_catalog.pg_shadow (
	usename NAME,
	usesysid OID,
	usecreatedb BOOL,
	usesuper BOOL,
  userepl BOOL,
  usebypassrls BOOL,
	passwd STRING,
  valuntil TIMESTAMPTZ,
	useconfig STRING[]
)`

// PgCatalogStatisticExt describes the schema of pg_catalog.pg_statistic_ext
// https://www.postgresql.org/docs/13/catalog-pg-statistic-ext.html
const PgCatalogStatisticExt = `
CREATE TABLE pg_catalog.pg_statistic_ext (
	oid OID,
	stxrelid OID,
  stxname NAME,
  stxnamespace OID,
	stxowner OID,
	stxstattarget INT4,
	stxkeys INT2VECTOR,
	stxkind "char"[]
)`

// PgCatalogSequences is an empty table in the pg_catalog that is not implemented yet
const PgCatalogSequences = `
CREATE TABLE pg_catalog.pg_sequences (
	schemaname NAME,
	sequencename NAME,
	sequenceowner NAME,
	data_type REGTYPE,
	start_value INT,
	min_value INT,
	max_value INT,
	increment_by INT,
	cycle BOOL,
	cache_size INT,
	last_value INT
)`

// PgCatalogStatDatabaseConflicts is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatDatabaseConflicts = `
CREATE TABLE pg_catalog.pg_stat_database_conflicts (
	datid OID,
	datname NAME,
	confl_tablespace INT,
	confl_lock INT,
	confl_snapshot INT,
	confl_bufferpin INT,
	confl_deadlock INT
)`

// PgCatalogReplicationOrigin is an empty table in the pg_catalog that is not implemented yet
const PgCatalogReplicationOrigin = `
CREATE TABLE pg_catalog.pg_replication_origin (
	roident OID,
	roname STRING
)`

// PgCatalogStatistic is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatistic = `
CREATE TABLE pg_catalog.pg_statistic (
	starelid OID,
	staattnum INT2,
	stainherit BOOL,
	stanullfrac FLOAT4,
	stawidth INT4,
	stadistinct FLOAT4,
	stakind1 INT2,
	stakind2 INT2,
	stakind3 INT2,
	stakind4 INT2,
	stakind5 INT2,
	staop1 OID,
	staop2 OID,
	staop3 OID,
	staop4 OID,
	staop5 OID,
	stacoll1 OID,
	stacoll2 OID,
	stacoll3 OID,
	stacoll4 OID,
	stacoll5 OID,
	stanumbers1 FLOAT4[],
	stanumbers2 FLOAT4[],
	stanumbers3 FLOAT4[],
	stanumbers4 FLOAT4[],
	stanumbers5 FLOAT4[],
	stavalues1 STRING[],
	stavalues2 STRING[],
	stavalues3 STRING[],
	stavalues4 STRING[],
	stavalues5 STRING[]
)`

// PgCatalogStatXactSysTables is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatXactSysTables = `
CREATE TABLE pg_catalog.pg_stat_xact_sys_tables (
	relid OID,
	schemaname NAME,
	relname NAME,
	seq_scan INT,
	seq_tup_read INT,
	idx_scan INT,
	idx_tup_fetch INT,
	n_tup_ins INT,
	n_tup_upd INT,
	n_tup_del INT,
	n_tup_hot_upd INT
)`

// PgCatalogAmop is an empty table in the pg_catalog that is not implemented yet
const PgCatalogAmop = `
CREATE TABLE pg_catalog.pg_amop (
	oid OID,
	amopfamily OID,
	amoplefttype OID,
	amoprighttype OID,
	amopstrategy INT2,
	amoppurpose "char",
	amopopr OID,
	amopmethod OID,
	amopsortfamily OID
)`

// PgCatalogStatProgressVacuum is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatProgressVacuum = `
CREATE TABLE pg_catalog.pg_stat_progress_vacuum (
	pid INT4,
	datid OID,
	datname NAME,
	relid OID,
	phase STRING,
	heap_blks_total INT,
	heap_blks_scanned INT,
	heap_blks_vacuumed INT,
	index_vacuum_count INT,
	max_dead_tuples INT,
	num_dead_tuples INT
)`

// PgCatalogStatSysIndexes is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatSysIndexes = `
CREATE TABLE pg_catalog.pg_stat_sys_indexes (
	relid OID,
	indexrelid OID,
	schemaname NAME,
	relname NAME,
	indexrelname NAME,
	idx_scan INT,
	idx_tup_read INT,
	idx_tup_fetch INT
)`

// PgCatalogStatioAllTables is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatioAllTables = `
CREATE TABLE pg_catalog.pg_statio_all_tables (
	relid OID,
	schemaname NAME,
	relname NAME,
	heap_blks_read INT,
	heap_blks_hit INT,
	idx_blks_read INT,
	idx_blks_hit INT,
	toast_blks_read INT,
	toast_blks_hit INT,
	tidx_blks_read INT,
	tidx_blks_hit INT
)`

// PgCatalogTsTemplate is an empty table in the pg_catalog that is not implemented yet
const PgCatalogTsTemplate = `
CREATE TABLE pg_catalog.pg_ts_template (
	oid OID,
	tmplname NAME,
	tmplnamespace OID,
	tmplinit REGPROC,
	tmpllexize REGPROC
)`

// PgCatalogPublicationRel is an empty table in the pg_catalog that is not implemented yet
const PgCatalogPublicationRel = `
CREATE TABLE pg_catalog.pg_publication_rel (
	oid OID,
	prpubid OID,
	prrelid OID
)`

// PgCatalogAvailableExtensionVersions is an empty table in the pg_catalog that is not implemented yet
const PgCatalogAvailableExtensionVersions = `
CREATE TABLE pg_catalog.pg_available_extension_versions (
	name NAME,
	version STRING,
	installed BOOL,
	superuser BOOL,
	trusted BOOL,
	relocatable BOOL,
	schema NAME,
	requires NAME[],
	comment STRING
)`

// PgCatalogStatReplication is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatReplication = `
CREATE TABLE pg_catalog.pg_stat_replication (
	pid INT4,
	usesysid OID,
	usename NAME,
	application_name STRING,
	client_addr INET,
	client_hostname STRING,
	client_port INT4,
	backend_start TIMESTAMPTZ,
	backend_xmin INT,
	state STRING,
	sent_lsn STRING,
	write_lsn STRING,
	flush_lsn STRING,
	replay_lsn STRING,
	write_lag INTERVAL,
	flush_lag INTERVAL,
	replay_lag INTERVAL,
	sync_priority INT4,
	sync_state STRING,
	reply_time TIMESTAMPTZ
)`

// PgCatalogOpfamily is an empty table in the pg_catalog that is not implemented yet
const PgCatalogOpfamily = `
CREATE TABLE pg_catalog.pg_opfamily (
	oid OID,
	opfmethod OID,
	opfname NAME,
	opfnamespace OID,
	opfowner OID
)`

// PgCatalogStatioAllSequences is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatioAllSequences = `
CREATE TABLE pg_catalog.pg_statio_all_sequences (
	relid OID,
	schemaname NAME,
	relname NAME,
	blks_read INT,
	blks_hit INT
)`

// PgCatalogInitPrivs is an empty table in the pg_catalog that is not implemented yet
const PgCatalogInitPrivs = `
CREATE TABLE pg_catalog.pg_init_privs (
	objoid OID,
	classoid OID,
	objsubid INT4,
	privtype "char",
	initprivs STRING[]
)`

// PgCatalogStatProgressCreateIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatProgressCreateIndex = `
CREATE TABLE pg_catalog.pg_stat_progress_create_index (
	pid INT4,
	datid OID,
	datname NAME,
	relid OID,
	index_relid OID,
	command STRING,
	phase STRING,
	lockers_total INT,
	lockers_done INT,
	current_locker_pid INT,
	blocks_total INT,
	blocks_done INT,
	tuples_total INT,
	tuples_done INT,
	partitions_total INT,
	partitions_done INT
)`

// PgCatalogUserMappings is an empty table in the pg_catalog that is not implemented yet
const PgCatalogUserMappings = `
CREATE TABLE pg_catalog.pg_user_mappings (
	umid OID,
	srvid OID,
	srvname NAME,
	umuser OID,
	usename NAME,
	umoptions STRING[]
)`

// PgCatalogStatGssapi is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatGssapi = `
CREATE TABLE pg_catalog.pg_stat_gssapi (
	pid INT4,
	gss_authenticated BOOL,
	principal STRING,
	encrypted BOOL
)`

// PgCatalogPolicies is an empty table in the pg_catalog that is not implemented yet
const PgCatalogPolicies = `
CREATE TABLE pg_catalog.pg_policies (
	schemaname NAME,
	tablename NAME,
	policyname NAME,
	permissive STRING,
	roles NAME[],
	cmd STRING,
	qual STRING,
	with_check STRING
)`

// PgCatalogStatsExt is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatsExt = `
CREATE TABLE pg_catalog.pg_stats_ext (
	schemaname NAME,
	tablename NAME,
	statistics_schemaname NAME,
	statistics_name NAME,
	statistics_owner NAME,
	attnames NAME[],
	kinds "char"[],
	n_distinct BYTES,
	dependencies BYTES,
	most_common_vals STRING[],
	most_common_val_nulls BOOL[],
	most_common_freqs FLOAT[],
	most_common_base_freqs FLOAT[]
)`

// PgCatalogTimezoneAbbrevs is an empty table in the pg_catalog that is not implemented yet
const PgCatalogTimezoneAbbrevs = `
CREATE TABLE pg_catalog.pg_timezone_abbrevs (
	abbrev STRING,
	utc_offset INTERVAL,
	is_dst BOOL
)`

// PgCatalogStatSysTables is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatSysTables = `
CREATE TABLE pg_catalog.pg_stat_sys_tables (
	relid OID,
	schemaname NAME,
	relname NAME,
	seq_scan INT,
	seq_tup_read INT,
	idx_scan INT,
	idx_tup_fetch INT,
	n_tup_ins INT,
	n_tup_upd INT,
	n_tup_del INT,
	n_tup_hot_upd INT,
	n_live_tup INT,
	n_dead_tup INT,
	n_mod_since_analyze INT,
	n_ins_since_vacuum INT,
	last_vacuum TIMESTAMPTZ,
	last_autovacuum TIMESTAMPTZ,
	last_analyze TIMESTAMPTZ,
	last_autoanalyze TIMESTAMPTZ,
	vacuum_count INT,
	autovacuum_count INT,
	analyze_count INT,
	autoanalyze_count INT
)`

// PgCatalogStatioSysSequences is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatioSysSequences = `
CREATE TABLE pg_catalog.pg_statio_sys_sequences (
	relid OID,
	schemaname NAME,
	relname NAME,
	blks_read INT,
	blks_hit INT
)`

// PgCatalogStatDatabase is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatDatabase = `
CREATE TABLE pg_catalog.pg_stat_database (
	datid OID,
	datname NAME,
	numbackends INT4,
	xact_commit INT,
	xact_rollback INT,
	blks_read INT,
	blks_hit INT,
	tup_returned INT,
	tup_fetched INT,
	tup_inserted INT,
	tup_updated INT,
	tup_deleted INT,
	conflicts INT,
	temp_files INT,
	temp_bytes INT,
	deadlocks INT,
	checksum_failures INT,
	checksum_last_failure TIMESTAMPTZ,
	blk_read_time FLOAT,
	blk_write_time FLOAT,
	stats_reset TIMESTAMPTZ
)`

// PgCatalogStatioUserIndexes is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatioUserIndexes = `
CREATE TABLE pg_catalog.pg_statio_user_indexes (
	relid OID,
	indexrelid OID,
	schemaname NAME,
	relname NAME,
	indexrelname NAME,
	idx_blks_read INT,
	idx_blks_hit INT
)`

// PgCatalogStatSsl is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatSsl = `
CREATE TABLE pg_catalog.pg_stat_ssl (
	pid INT4,
	ssl BOOL,
	version STRING,
	cipher STRING,
	bits INT4,
	compression BOOL,
	client_dn STRING,
	client_serial DECIMAL,
	issuer_dn STRING
)`

// PgCatalogStatioAllIndexes is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatioAllIndexes = `
CREATE TABLE pg_catalog.pg_statio_all_indexes (
	relid OID,
	indexrelid OID,
	schemaname NAME,
	relname NAME,
	indexrelname NAME,
	idx_blks_read INT,
	idx_blks_hit INT
)`

// PgCatalogTsConfig is an empty table in the pg_catalog that is not implemented yet
const PgCatalogTsConfig = `
CREATE TABLE pg_catalog.pg_ts_config (
	oid OID,
	cfgname NAME,
	cfgnamespace OID,
	cfgowner OID,
	cfgparser OID
)`

// PgCatalogStats is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStats = `
CREATE TABLE pg_catalog.pg_stats (
	schemaname NAME,
	tablename NAME,
	attname NAME,
	inherited BOOL,
	null_frac FLOAT4,
	avg_width INT4,
	n_distinct FLOAT4,
	most_common_vals STRING[],
	most_common_freqs FLOAT4[],
	histogram_bounds STRING[],
	correlation FLOAT4,
	most_common_elems STRING[],
	most_common_elem_freqs FLOAT4[],
	elem_count_histogram FLOAT4[]
)`

// PgCatalogStatAllTables is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatAllTables = `
CREATE TABLE pg_catalog.pg_stat_all_tables (
	relid OID,
	schemaname NAME,
	relname NAME,
	seq_scan INT,
	seq_tup_read INT,
	idx_scan INT,
	idx_tup_fetch INT,
	n_tup_ins INT,
	n_tup_upd INT,
	n_tup_del INT,
	n_tup_hot_upd INT,
	n_live_tup INT,
	n_dead_tup INT,
	n_mod_since_analyze INT,
	n_ins_since_vacuum INT,
	last_vacuum TIMESTAMPTZ,
	last_autovacuum TIMESTAMPTZ,
	last_analyze TIMESTAMPTZ,
	last_autoanalyze TIMESTAMPTZ,
	vacuum_count INT,
	autovacuum_count INT,
	analyze_count INT,
	autoanalyze_count INT
)`

// PgCatalogStatioSysTables is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatioSysTables = `
CREATE TABLE pg_catalog.pg_statio_sys_tables (
	relid OID,
	schemaname NAME,
	relname NAME,
	heap_blks_read INT,
	heap_blks_hit INT,
	idx_blks_read INT,
	idx_blks_hit INT,
	toast_blks_read INT,
	toast_blks_hit INT,
	tidx_blks_read INT,
	tidx_blks_hit INT
)`

// PgCatalogStatXactUserFunctions is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatXactUserFunctions = `
CREATE TABLE pg_catalog.pg_stat_xact_user_functions (
	funcid OID,
	schemaname NAME,
	funcname NAME,
	calls INT,
	total_time FLOAT,
	self_time FLOAT
)`

// PgCatalogStatUserFunctions is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatUserFunctions = `
CREATE TABLE pg_catalog.pg_stat_user_functions (
	funcid OID,
	schemaname NAME,
	funcname NAME,
	calls INT,
	total_time FLOAT,
	self_time FLOAT
)`

// PgCatalogStatProgressBasebackup is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatProgressBasebackup = `
CREATE TABLE pg_catalog.pg_stat_progress_basebackup (
	pid INT4,
	phase STRING,
	backup_total INT,
	backup_streamed INT,
	tablespaces_total INT,
	tablespaces_streamed INT
)`

// PgCatalogPolicy is an empty table in the pg_catalog that is not implemented yet
const PgCatalogPolicy = `
CREATE TABLE pg_catalog.pg_policy (
	oid OID,
	polname NAME,
	polrelid OID,
	polcmd "char",
	polpermissive BOOL,
	polroles OID[],
	polqual STRING,
	polwithcheck STRING
)`

// PgCatalogStatArchiver is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatArchiver = `
CREATE TABLE pg_catalog.pg_stat_archiver (
	archived_count INT,
	last_archived_wal STRING,
	last_archived_time TIMESTAMPTZ,
	failed_count INT,
	last_failed_wal STRING,
	last_failed_time TIMESTAMPTZ,
	stats_reset TIMESTAMPTZ
)`

// PgCatalogStatXactAllTables is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatXactAllTables = `
CREATE TABLE pg_catalog.pg_stat_xact_all_tables (
	relid OID,
	schemaname NAME,
	relname NAME,
	seq_scan INT,
	seq_tup_read INT,
	idx_scan INT,
	idx_tup_fetch INT,
	n_tup_ins INT,
	n_tup_upd INT,
	n_tup_del INT,
	n_tup_hot_upd INT
)`

// PgCatalogHbaFileRules is an empty table in the pg_catalog that is not implemented yet
const PgCatalogHbaFileRules = `
CREATE TABLE pg_catalog.pg_hba_file_rules (
	line_number INT4,
	type STRING,
	database STRING[],
	user_name STRING[],
	address STRING,
	netmask STRING,
	auth_method STRING,
	options STRING[],
	error STRING
)`

// PgCatalogPublication is an empty table in the pg_catalog that is not implemented yet
const PgCatalogPublication = `
CREATE TABLE pg_catalog.pg_publication (
	oid OID,
	pubname NAME,
	pubowner OID,
	puballtables BOOL,
	pubinsert BOOL,
	pubupdate BOOL,
	pubdelete BOOL,
	pubtruncate BOOL,
	pubviaroot BOOL
)`

// PgCatalogAmproc is an empty table in the pg_catalog that is not implemented yet
const PgCatalogAmproc = `
CREATE TABLE pg_catalog.pg_amproc (
	oid OID,
	amprocfamily OID,
	amproclefttype OID,
	amprocrighttype OID,
	amprocnum INT2,
	amproc REGPROC
)`

// PgCatalogStatProgressAnalyze is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatProgressAnalyze = `
CREATE TABLE pg_catalog.pg_stat_progress_analyze (
	pid INT4,
	datid OID,
	datname NAME,
	relid OID,
	phase STRING,
	sample_blks_total INT,
	sample_blks_scanned INT,
	ext_stats_total INT,
	ext_stats_computed INT,
	child_tables_total INT,
	child_tables_done INT,
	current_child_table_relid OID
)`

// PgCatalogStatSlru is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatSlru = `
CREATE TABLE pg_catalog.pg_stat_slru (
	name STRING,
	blks_zeroed INT,
	blks_hit INT,
	blks_read INT,
	blks_written INT,
	blks_exists INT,
	flushes INT,
	truncates INT,
	stats_reset TIMESTAMPTZ
)`

// PgCatalogFileSettings is an empty table in the pg_catalog that is not implemented yet
const PgCatalogFileSettings = `
CREATE TABLE pg_catalog.pg_file_settings (
	sourcefile STRING,
	sourceline INT4,
	seqno INT4,
	name STRING,
	setting STRING,
	applied BOOL,
	error STRING
)`

// PgCatalogCursors is an empty table in the pg_catalog that is not implemented yet
const PgCatalogCursors = `
CREATE TABLE pg_catalog.pg_cursors (
	name STRING,
	statement STRING,
	is_holdable BOOL,
	is_binary BOOL,
	is_scrollable BOOL,
	creation_time TIMESTAMPTZ
)`

// PgCatalogRules is an empty table in the pg_catalog that is not implemented yet
const PgCatalogRules = `
CREATE TABLE pg_catalog.pg_rules (
	schemaname NAME,
	tablename NAME,
	rulename NAME,
	definition STRING
)`

// PgCatalogStatioUserSequences is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatioUserSequences = `
CREATE TABLE pg_catalog.pg_statio_user_sequences (
	relid OID,
	schemaname NAME,
	relname NAME,
	blks_read INT,
	blks_hit INT
)`

// PgCatalogStatUserIndexes is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatUserIndexes = `
CREATE TABLE pg_catalog.pg_stat_user_indexes (
	relid OID,
	indexrelid OID,
	schemaname NAME,
	relname NAME,
	indexrelname NAME,
	idx_scan INT,
	idx_tup_read INT,
	idx_tup_fetch INT
)`

// PgCatalogStatXactUserTables is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatXactUserTables = `
CREATE TABLE pg_catalog.pg_stat_xact_user_tables (
	relid OID,
	schemaname NAME,
	relname NAME,
	seq_scan INT,
	seq_tup_read INT,
	idx_scan INT,
	idx_tup_fetch INT,
	n_tup_ins INT,
	n_tup_upd INT,
	n_tup_del INT,
	n_tup_hot_upd INT
)`

// PgCatalogPublicationTables is an empty table in the pg_catalog that is not implemented yet
const PgCatalogPublicationTables = `
CREATE TABLE pg_catalog.pg_publication_tables (
	pubname NAME,
	schemaname NAME,
	tablename NAME
)`

// PgCatalogStatProgressCluster is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatProgressCluster = `
CREATE TABLE pg_catalog.pg_stat_progress_cluster (
	pid INT4,
	datid OID,
	datname NAME,
	relid OID,
	command STRING,
	phase STRING,
	cluster_index_relid OID,
	heap_tuples_scanned INT,
	heap_tuples_written INT,
	heap_blks_total INT,
	heap_blks_scanned INT,
	index_rebuild_count INT
)`

// PgCatalogGroup is an empty table in the pg_catalog that is not implemented yet
const PgCatalogGroup = `
CREATE TABLE pg_catalog.pg_group (
	groname NAME,
	grosysid OID,
	grolist OID[]
)`

// PgCatalogStatAllIndexes is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatAllIndexes = `
CREATE TABLE pg_catalog.pg_stat_all_indexes (
	relid OID,
	indexrelid OID,
	schemaname NAME,
	relname NAME,
	indexrelname NAME,
	idx_scan INT,
	idx_tup_read INT,
	idx_tup_fetch INT
)`

// PgCatalogTsConfigMap is an empty table in the pg_catalog that is not implemented yet
const PgCatalogTsConfigMap = `
CREATE TABLE pg_catalog.pg_ts_config_map (
	mapcfg OID,
	maptokentype INT4,
	mapseqno INT4,
	mapdict OID
)`

// PgCatalogStatBgwriter is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatBgwriter = `
CREATE TABLE pg_catalog.pg_stat_bgwriter (
	checkpoints_timed INT,
	checkpoints_req INT,
	checkpoint_write_time FLOAT,
	checkpoint_sync_time FLOAT,
	buffers_checkpoint INT,
	buffers_clean INT,
	maxwritten_clean INT,
	buffers_backend INT,
	buffers_backend_fsync INT,
	buffers_alloc INT,
	stats_reset TIMESTAMPTZ
)`

// PgCatalogTransform is an empty table in the pg_catalog that is not implemented yet
const PgCatalogTransform = `
CREATE TABLE pg_catalog.pg_transform (
	oid OID,
	trftype OID,
	trflang OID,
	trffromsql REGPROC,
	trftosql REGPROC
)`

// PgCatalogTsParser is an empty table in the pg_catalog that is not implemented yet
const PgCatalogTsParser = `
CREATE TABLE pg_catalog.pg_ts_parser (
	oid OID,
	prsname NAME,
	prsnamespace OID,
	prsstart REGPROC,
	prstoken REGPROC,
	prsend REGPROC,
	prsheadline REGPROC,
	prslextype REGPROC
)`

// PgCatalogStatisticExtData is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatisticExtData = `
CREATE TABLE pg_catalog.pg_statistic_ext_data (
	stxoid OID,
	stxdndistinct BYTES,
	stxddependencies BYTES,
	stxdmcv BYTES
)`

// PgCatalogLargeobjectMetadata is an empty table in the pg_catalog that is not implemented yet
const PgCatalogLargeobjectMetadata = `
CREATE TABLE pg_catalog.pg_largeobject_metadata (
	oid OID,
	lomowner OID,
	lomacl STRING[]
)`

// PgCatalogReplicationSlots is an empty table in the pg_catalog that is not implemented yet
const PgCatalogReplicationSlots = `
CREATE TABLE pg_catalog.pg_replication_slots (
	slot_name NAME,
	plugin NAME,
	slot_type STRING,
	datoid OID,
	database NAME,
	temporary BOOL,
	active BOOL,
	active_pid INT4,
	xmin INT,
	catalog_xmin INT,
	restart_lsn STRING,
	confirmed_flush_lsn STRING,
	wal_status STRING,
	safe_wal_size INT
)`

// PgCatalogSubscriptionRel is an empty table in the pg_catalog that is not implemented yet
const PgCatalogSubscriptionRel = `
CREATE TABLE pg_catalog.pg_subscription_rel (
	srsubid OID,
	srrelid OID,
	srsubstate "char",
	srsublsn STRING
)`

// PgCatalogStatioUserTables is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatioUserTables = `
CREATE TABLE pg_catalog.pg_statio_user_tables (
	relid OID,
	schemaname NAME,
	relname NAME,
	heap_blks_read INT,
	heap_blks_hit INT,
	idx_blks_read INT,
	idx_blks_hit INT,
	toast_blks_read INT,
	toast_blks_hit INT,
	tidx_blks_read INT,
	tidx_blks_hit INT
)`

// PgCatalogTimezoneNames describes the schema of pg_catalog.pg_timezone_names.
const PgCatalogTimezoneNames = `
CREATE TABLE pg_catalog.pg_timezone_names (
	name STRING,
	abbrev STRING,
	utc_offset INTERVAL,
	is_dst BOOL,
	INDEX (name)
)`

// PgCatalogPartitionedTable is an empty table in the pg_catalog that is not implemented yet
const PgCatalogPartitionedTable = `
CREATE TABLE pg_catalog.pg_partitioned_table (
	partrelid OID,
	partstrat "char",
	partnatts INT2,
	partdefid OID,
	partattrs INT2VECTOR,
	partclass OIDVECTOR,
	partcollation OIDVECTOR,
	partexprs STRING
)`

// PgCatalogStatioSysIndexes is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatioSysIndexes = `
CREATE TABLE pg_catalog.pg_statio_sys_indexes (
	relid OID,
	indexrelid OID,
	schemaname NAME,
	relname NAME,
	indexrelname NAME,
	idx_blks_read INT,
	idx_blks_hit INT
)`

// PgCatalogConfig is an empty table in the pg_catalog that is not implemented yet
const PgCatalogConfig = `
CREATE TABLE pg_catalog.pg_config (
	name STRING,
	setting STRING
)`

// PgCatalogStatUserTables is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatUserTables = `
CREATE TABLE pg_catalog.pg_stat_user_tables (
	relid OID,
	schemaname NAME,
	relname NAME,
	seq_scan INT,
	seq_tup_read INT,
	idx_scan INT,
	idx_tup_fetch INT,
	n_tup_ins INT,
	n_tup_upd INT,
	n_tup_del INT,
	n_tup_hot_upd INT,
	n_live_tup INT,
	n_dead_tup INT,
	n_mod_since_analyze INT,
	n_ins_since_vacuum INT,
	last_vacuum TIMESTAMPTZ,
	last_autovacuum TIMESTAMPTZ,
	last_analyze TIMESTAMPTZ,
	last_autoanalyze TIMESTAMPTZ,
	vacuum_count INT,
	autovacuum_count INT,
	analyze_count INT,
	autoanalyze_count INT
)`

// PgCatalogSubscription is an empty table in the pg_catalog that is not implemented yet
const PgCatalogSubscription = `
CREATE TABLE pg_catalog.pg_subscription (
	oid OID,
	subdbid OID,
	subname NAME,
	subowner OID,
	subenabled BOOL,
	subconninfo STRING,
	subslotname NAME,
	subsynccommit STRING,
	subpublications STRING[]
)`

// PgCatalogTsDict is an empty table in the pg_catalog that is not implemented yet
const PgCatalogTsDict = `
CREATE TABLE pg_catalog.pg_ts_dict (
	oid OID,
	dictname NAME,
	dictnamespace OID,
	dictowner OID,
	dicttemplate OID,
	dictinitoption STRING
)`

// PgCatalogLargeobject is an empty table in the pg_catalog that is not implemented yet
const PgCatalogLargeobject = `
CREATE TABLE pg_catalog.pg_largeobject (
	loid OID,
	pageno INT4,
	data BYTES
)`

// PgCatalogReplicationOriginStatus is an empty table in the pg_catalog that is not implemented yet
const PgCatalogReplicationOriginStatus = `
CREATE TABLE pg_catalog.pg_replication_origin_status (
	local_id OID,
	external_id STRING,
	remote_lsn STRING,
	local_lsn STRING
)`

// PgCatalogShmemAllocations is an empty table in the pg_catalog that is not implemented yet
const PgCatalogShmemAllocations = `
CREATE TABLE pg_catalog.pg_shmem_allocations (
	name STRING,
	off INT,
	size INT,
	allocated_size INT
)`

// PgCatalogStatWalReceiver is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatWalReceiver = `
CREATE TABLE pg_catalog.pg_stat_wal_receiver (
	pid INT4,
	status STRING,
	receive_start_lsn STRING,
	receive_start_tli INT4,
	written_lsn STRING,
	flushed_lsn STRING,
	received_tli INT4,
	last_msg_send_time TIMESTAMPTZ,
	last_msg_receipt_time TIMESTAMPTZ,
	latest_end_lsn STRING,
	latest_end_time TIMESTAMPTZ,
	slot_name STRING,
	sender_host STRING,
	sender_port INT4,
	conninfo STRING
)`

// PgCatalogStatSubscription is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatSubscription = `
CREATE TABLE pg_catalog.pg_stat_subscription (
	subid OID,
	subname NAME,
	pid INT4,
	relid OID,
	received_lsn STRING,
	last_msg_send_time TIMESTAMPTZ,
	last_msg_receipt_time TIMESTAMPTZ,
	latest_end_lsn STRING,
	latest_end_time TIMESTAMPTZ
)`
