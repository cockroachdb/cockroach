// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package vtable

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
const PGCatalogDescription = `
CREATE TABLE pg_catalog.pg_description (
	objoid OID,
	classoid OID,
	objsubid INT4,
	description STRING
)`

// PGCatalogSharedDescription describes the schema of the
// pg_catalog.pg_shdescription table.
// https://www.postgresql.org/docs/9.5/catalog-pg-shdescription.html,
const PGCatalogSharedDescription = `
CREATE TABLE pg_catalog.pg_shdescription (
	objoid OID,
	classoid OID,
	description STRING
)`

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
	nspacl STRING[]
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
// https://www.postgresql.org/docs/9.5/catalog-pg-proc.html,
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
	protransform STRING,
	proisagg BOOL,
	proiswindow BOOL,
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
	proconfig STRING[],
	proacl STRING[],
	prokind "char",
	prosupport REGPROC
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

// PGCatalogShdepend describes the schema of the pg_catalog.pg_shdepend table.
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
	valuntil TIMESTAMP,
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

// PgCatalogLargeobject is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogLargeobject = `
CREATE TABLE pg_catalog.pg_largeobject (
	data BYTES,
	loid OID,
	pageno INT4
)`

// PgCatalogConfig is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogConfig = `
CREATE TABLE pg_catalog.pg_config (
	name STRING,
	setting STRING
)`

// PgCatalogAvailableExtensionVersions is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogAvailableExtensionVersions = `
CREATE TABLE pg_catalog.pg_available_extension_versions (
	trusted BOOL,
	comment STRING,
	relocatable BOOL,
	requires NAME[],
	schema NAME,
	installed BOOL,
	name NAME,
	superuser BOOL,
	version STRING
)`

// PgCatalogPublicationRel is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogPublicationRel = `
CREATE TABLE pg_catalog.pg_publication_rel (
	oid OID,
	prpubid OID,
	prrelid OID
)`

// PgCatalogOpfamily is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogOpfamily = `
CREATE TABLE pg_catalog.pg_opfamily (
	opfname NAME,
	opfnamespace OID,
	opfowner OID,
	oid OID,
	opfmethod OID
)`

// PgCatalogShmemAllocations is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogShmemAllocations = `
CREATE TABLE pg_catalog.pg_shmem_allocations (
	name STRING,
	off INT,
	size INT,
	allocated_size INT
)`

// PgCatalogDbRoleSetting is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogDbRoleSetting = `
CREATE TABLE pg_catalog.pg_db_role_setting (
	setconfig STRING[],
	setdatabase OID,
	setrole OID
)`

// PgCatalogTimezoneNames is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogTimezoneNames = `
CREATE TABLE pg_catalog.pg_timezone_names (
	abbrev STRING,
	is_dst BOOL,
	name STRING,
	utc_offset INTERVAL
)`

// PgCatalogPublicationTables is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogPublicationTables = `
CREATE TABLE pg_catalog.pg_publication_tables (
	pubname NAME,
	schemaname NAME,
	tablename NAME
)`

// PgCatalogUserMappings is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogUserMappings = `
CREATE TABLE pg_catalog.pg_user_mappings (
	srvname NAME,
	umid OID,
	umoptions STRING[],
	umuser OID,
	usename NAME,
	srvid OID
)`

// PgCatalogTsTemplate is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogTsTemplate = `
CREATE TABLE pg_catalog.pg_ts_template (
	oid OID,
	tmplinit REGPROC,
	tmpllexize REGPROC,
	tmplname NAME,
	tmplnamespace OID
)`

// PgCatalogRules is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogRules = `
CREATE TABLE pg_catalog.pg_rules (
	definition STRING,
	rulename NAME,
	schemaname NAME,
	tablename NAME
)`

// PgCatalogShadow is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogShadow = `
CREATE TABLE pg_catalog.pg_shadow (
	useconfig STRING[],
	usecreatedb BOOL,
	userepl BOOL,
	usesuper BOOL,
	usesysid OID,
	valuntil TIMESTAMPTZ,
	passwd STRING,
	usename NAME,
	usebypassrls BOOL
)`

// PgCatalogPublication is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogPublication = `
CREATE TABLE pg_catalog.pg_publication (
	pubupdate BOOL,
	oid OID,
	puballtables BOOL,
	pubdelete BOOL,
	pubinsert BOOL,
	pubname NAME,
	pubowner OID,
	pubtruncate BOOL,
	pubviaroot BOOL
)`

// PgCatalogGroup is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogGroup = `
CREATE TABLE pg_catalog.pg_group (
	grolist OID[],
	groname NAME,
	grosysid OID
)`

// PgCatalogCursors is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogCursors = `
CREATE TABLE pg_catalog.pg_cursors (
	is_scrollable BOOL,
	name STRING,
	statement STRING,
	creation_time TIMESTAMPTZ,
	is_binary BOOL,
	is_holdable BOOL
)`

// PgCatalogTsParser is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogTsParser = `
CREATE TABLE pg_catalog.pg_ts_parser (
	prslextype REGPROC,
	prsname NAME,
	prsnamespace OID,
	prsstart REGPROC,
	prstoken REGPROC,
	oid OID,
	prsend REGPROC,
	prsheadline REGPROC
)`

// PgCatalogSubscription is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogSubscription = `
CREATE TABLE pg_catalog.pg_subscription (
	subname NAME,
	subpublications STRING[],
	subslotname NAME,
	subsynccommit STRING,
	oid OID,
	subconninfo STRING,
	subdbid OID,
	subenabled BOOL,
	subowner OID
)`

// PgCatalogAmproc is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogAmproc = `
CREATE TABLE pg_catalog.pg_amproc (
	amproc REGPROC,
	amprocfamily OID,
	amproclefttype OID,
	amprocnum INT2,
	amprocrighttype OID,
	oid OID
)`

// PgCatalogTsDict is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogTsDict = `
CREATE TABLE pg_catalog.pg_ts_dict (
	dictinitoption STRING,
	dictname NAME,
	dictnamespace OID,
	dictowner OID,
	dicttemplate OID,
	oid OID
)`

// PgCatalogTimezoneAbbrevs is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogTimezoneAbbrevs = `
CREATE TABLE pg_catalog.pg_timezone_abbrevs (
	abbrev STRING,
	is_dst BOOL,
	utc_offset INTERVAL
)`

// PgCatalogTransform is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogTransform = `
CREATE TABLE pg_catalog.pg_transform (
	trffromsql REGPROC,
	trflang OID,
	trftosql REGPROC,
	trftype OID,
	oid OID
)`

// PgCatalogTsConfigMap is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogTsConfigMap = `
CREATE TABLE pg_catalog.pg_ts_config_map (
	mapcfg OID,
	mapdict OID,
	mapseqno INT4,
	maptokentype INT4
)`

// PgCatalogFileSettings is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogFileSettings = `
CREATE TABLE pg_catalog.pg_file_settings (
	error STRING,
	name STRING,
	seqno INT4,
	setting STRING,
	sourcefile STRING,
	sourceline INT4,
	applied BOOL
)`

// PgCatalogPolicies is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogPolicies = `
CREATE TABLE pg_catalog.pg_policies (
	with_check STRING,
	cmd STRING,
	permissive STRING,
	policyname NAME,
	qual STRING,
	roles NAME[],
	schemaname NAME,
	tablename NAME
)`

// PgCatalogTsConfig is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogTsConfig = `
CREATE TABLE pg_catalog.pg_ts_config (
	cfgname NAME,
	cfgnamespace OID,
	cfgowner OID,
	cfgparser OID,
	oid OID
)`

// PgCatalogHbaFileRules is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogHbaFileRules = `
CREATE TABLE pg_catalog.pg_hba_file_rules (
	address STRING,
	database STRING[],
	line_number INT4,
	netmask STRING,
	type STRING,
	user_name STRING[],
	auth_method STRING,
	error STRING,
	options STRING[]
)`

// PgCatalogStatisticExt is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogStatisticExt = `
CREATE TABLE pg_catalog.pg_statistic_ext (
	stxrelid OID,
	stxstattarget INT4,
	oid OID,
	stxkeys INT2VECTOR,
	stxkind "char"[],
	stxname NAME,
	stxnamespace OID,
	stxowner OID
)`

// PgCatalogReplicationOrigin is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogReplicationOrigin = `
CREATE TABLE pg_catalog.pg_replication_origin (
	roident OID,
	roname STRING
)`

// PgCatalogAmop is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogAmop = `
CREATE TABLE pg_catalog.pg_amop (
	amoplefttype OID,
	amopmethod OID,
	amopopr OID,
	amoppurpose "char",
	amoprighttype OID,
	oid OID,
	amopfamily OID,
	amopsortfamily OID,
	amopstrategy INT2
)`

//PgCatalogLargeobjectMetadata is an empty table in the pg_catalog that is not implemented yet
const PgCatalogLargeobjectMetadata = `
CREATE TABLE pg_catalog.pg_largeobject_metadata (
	oid OID,
	lomacl STRING[],
	lomowner OID
)`

//PgCatalogPartitionedTable is an empty table in the pg_catalog that is not implemented yet
const PgCatalogPartitionedTable = `
CREATE TABLE pg_catalog.pg_partitioned_table (
	partrelid OID,
	partstrat "char",
	partattrs INT2VECTOR,
	partclass OIDVECTOR,
	partcollation OIDVECTOR,
	partdefid OID,
	partexprs STRING,
	partnatts INT2
)`

//PgCatalogReplicationOriginStatus is an empty table in the pg_catalog that is not implemented yet
const PgCatalogReplicationOriginStatus = `
CREATE TABLE pg_catalog.pg_replication_origin_status (
	local_lsn STRING,
	remote_lsn STRING,
	external_id STRING,
	local_id OID
)`

//PgCatalogReplicationSlots is an empty table in the pg_catalog that is not implemented yet
const PgCatalogReplicationSlots = `
CREATE TABLE pg_catalog.pg_replication_slots (
	safe_wal_size INT,
	wal_status STRING,
	plugin NAME,
	restart_lsn STRING,
	xmin INT,
	confirmed_flush_lsn STRING,
	database NAME,
	datoid OID,
	active BOOL,
	catalog_xmin INT,
	slot_name NAME,
	active_pid INT4,
	slot_type STRING,
	temporary BOOL
)`

//PgCatalogInitPrivs is an empty table in the pg_catalog that is not implemented yet
const PgCatalogInitPrivs = `
CREATE TABLE pg_catalog.pg_init_privs (
	classoid OID,
	initprivs STRING[],
	objoid OID,
	objsubid INT4,
	privtype "char"
)`

//PgCatalogPolicy is an empty table in the pg_catalog that is not implemented yet
const PgCatalogPolicy = `
CREATE TABLE pg_catalog.pg_policy (
	polrelid OID,
	polroles OID[],
	polwithcheck STRING,
	oid OID,
	polcmd "char",
	polname NAME,
	polpermissive BOOL,
	polqual STRING
)`

//PgCatalogSequences is an empty table in the pg_catalog that is not implemented yet
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

//PgCatalogSubscriptionRel is an empty table in the pg_catalog that is not implemented yet
const PgCatalogSubscriptionRel = `
CREATE TABLE pg_catalog.pg_subscription_rel (
	srrelid OID,
	srsubid OID,
	srsublsn STRING,
	srsubstate "char"
)`
