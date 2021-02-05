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
	relrowsecurity BOOL,
	relforcerowsecurity BOOL,
	relispartition BOOL,
	relispopulated BOOL,
	relreplident "char",
	relrewrite OID,
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
	contype STRING,
	condeferrable BOOL,
	condeferred BOOL,
	convalidated BOOL,
	conrelid OID NOT NULL,
	contypid OID,
	conindid OID,
	confrelid OID,
	confupdtype STRING,
	confdeltype STRING,
	confmatchtype STRING,
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
	oprkind TEXT,
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
	proargmodes STRING[],
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
	ev_type TEXT,
	ev_enabled TEXT,
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
	tgenabled TEXT,
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
	aggmfinalmodify "char",
	aggfinalmodify "char"
)`

//PgCatalogReplicationOriginRonameIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogReplicationOriginRonameIndex = `
CREATE TABLE pg_catalog.pg_replication_origin_roname_index (
	roname STRING
)`

//PgCatalogConfig is an empty table in the pg_catalog that is not implemented yet
const PgCatalogConfig = `
CREATE TABLE pg_catalog.pg_config (
	setting STRING,
	name STRING
)`

//PgCatalogLargeobjectLoidPnIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogLargeobjectLoidPnIndex = `
CREATE TABLE pg_catalog.pg_largeobject_loid_pn_index (
	loid OID,
	pageno INT4
)`

//PgCatalogTsConfig is an empty table in the pg_catalog that is not implemented yet
const PgCatalogTsConfig = `
CREATE TABLE pg_catalog.pg_ts_config (
	cfgname NAME,
	cfgnamespace OID,
	cfgowner OID,
	cfgparser OID,
	oid OID
)`

//PgCatalogOperatorOidIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogOperatorOidIndex = `
CREATE TABLE pg_catalog.pg_operator_oid_index (
	oid OID
)`

//PgCatalogConversionOidIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogConversionOidIndex = `
CREATE TABLE pg_catalog.pg_conversion_oid_index (
	oid OID
)`

//PgCatalogDependDependerIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogDependDependerIndex = `
CREATE TABLE pg_catalog.pg_depend_depender_index (
	objsubid INT4,
	classid OID,
	objid OID
)`

//PgCatalogStatUserFunctions is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatUserFunctions = `
CREATE TABLE pg_catalog.pg_stat_user_functions (
	calls INT,
	funcid OID,
	funcname NAME,
	schemaname NAME,
	self_time FLOAT,
	total_time FLOAT
)`

//PgCatalogIndexIndrelidIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogIndexIndrelidIndex = `
CREATE TABLE pg_catalog.pg_index_indrelid_index (
	indrelid OID
)`

//PgCatalogGroup is an empty table in the pg_catalog that is not implemented yet
const PgCatalogGroup = `
CREATE TABLE pg_catalog.pg_group (
	grolist OID[],
	groname NAME,
	grosysid OID
)`

//PgCatalogDbRoleSettingDatabaseidRolIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogDbRoleSettingDatabaseidRolIndex = `
CREATE TABLE pg_catalog.pg_db_role_setting_databaseid_rol_index (
	setdatabase OID,
	setrole OID
)`

//PgCatalogStatioUserTables is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatioUserTables = `
CREATE TABLE pg_catalog.pg_statio_user_tables (
	toast_blks_read INT,
	idx_blks_hit INT,
	idx_blks_read INT,
	relname NAME,
	tidx_blks_read INT,
	toast_blks_hit INT,
	heap_blks_hit INT,
	heap_blks_read INT,
	relid OID,
	schemaname NAME,
	tidx_blks_hit INT
)`

//PgCatalogPartitionedTablePartrelidIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogPartitionedTablePartrelidIndex = `
CREATE TABLE pg_catalog.pg_partitioned_table_partrelid_index (
	partrelid OID
)`

//PgCatalogPublicationOidIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogPublicationOidIndex = `
CREATE TABLE pg_catalog.pg_publication_oid_index (
	oid OID
)`

//PgCatalogPublication is an empty table in the pg_catalog that is not implemented yet
const PgCatalogPublication = `
CREATE TABLE pg_catalog.pg_publication (
	pubowner OID,
	pubupdate BOOL,
	pubviaroot BOOL,
	pubname NAME,
	pubtruncate BOOL,
	oid OID,
	puballtables BOOL,
	pubdelete BOOL,
	pubinsert BOOL
)`

//PgCatalogPublicationTables is an empty table in the pg_catalog that is not implemented yet
const PgCatalogPublicationTables = `
CREATE TABLE pg_catalog.pg_publication_tables (
	pubname NAME,
	schemaname NAME,
	tablename NAME
)`

//PgCatalogStatXactUserTables is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatXactUserTables = `
CREATE TABLE pg_catalog.pg_stat_xact_user_tables (
	n_tup_hot_upd INT,
	n_tup_ins INT,
	n_tup_upd INT,
	relname NAME,
	seq_scan INT,
	seq_tup_read INT,
	idx_scan INT,
	idx_tup_fetch INT,
	n_tup_del INT,
	relid OID,
	schemaname NAME
)`

//PgCatalogTsConfigMapIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogTsConfigMapIndex = `
CREATE TABLE pg_catalog.pg_ts_config_map_index (
	mapcfg OID,
	mapseqno INT4,
	maptokentype INT4
)`

//PgCatalogReplicationOriginRoiidentIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogReplicationOriginRoiidentIndex = `
CREATE TABLE pg_catalog.pg_replication_origin_roiident_index (
	roident OID
)`

//PgCatalogStatProgressVacuum is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatProgressVacuum = `
CREATE TABLE pg_catalog.pg_stat_progress_vacuum (
	heap_blks_vacuumed INT,
	index_vacuum_count INT,
	phase STRING,
	pid INT4,
	relid OID,
	datid OID,
	datname NAME,
	heap_blks_scanned INT,
	heap_blks_total INT,
	max_dead_tuples INT,
	num_dead_tuples INT
)`

//PgCatalogFileSettings is an empty table in the pg_catalog that is not implemented yet
const PgCatalogFileSettings = `
CREATE TABLE pg_catalog.pg_file_settings (
	sourceline INT4,
	applied BOOL,
	error STRING,
	name STRING,
	seqno INT4,
	setting STRING,
	sourcefile STRING
)`

//PgCatalogStatioAllIndexes is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatioAllIndexes = `
CREATE TABLE pg_catalog.pg_statio_all_indexes (
	schemaname NAME,
	idx_blks_hit INT,
	idx_blks_read INT,
	indexrelid OID,
	indexrelname NAME,
	relid OID,
	relname NAME
)`

//PgCatalogShseclabelObjectIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogShseclabelObjectIndex = `
CREATE TABLE pg_catalog.pg_shseclabel_object_index (
	objoid OID,
	provider STRING,
	classoid OID
)`

//PgCatalogEnumOidIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogEnumOidIndex = `
CREATE TABLE pg_catalog.pg_enum_oid_index (
	oid OID
)`

//PgCatalogStatisticRelidAttInhIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatisticRelidAttInhIndex = `
CREATE TABLE pg_catalog.pg_statistic_relid_att_inh_index (
	staattnum INT2,
	stainherit BOOL,
	starelid OID
)`

//PgCatalogLargeobjectMetadataOidIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogLargeobjectMetadataOidIndex = `
CREATE TABLE pg_catalog.pg_largeobject_metadata_oid_index (
	oid OID
)`

//PgCatalogStatArchiver is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatArchiver = `
CREATE TABLE pg_catalog.pg_stat_archiver (
	archived_count INT,
	failed_count INT,
	last_archived_time TIMESTAMPTZ,
	last_archived_wal STRING,
	last_failed_time TIMESTAMPTZ,
	last_failed_wal STRING,
	stats_reset TIMESTAMPTZ
)`

//PgCatalogStatioSysSequences is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatioSysSequences = `
CREATE TABLE pg_catalog.pg_statio_sys_sequences (
	relname NAME,
	schemaname NAME,
	blks_hit INT,
	blks_read INT,
	relid OID
)`

//PgCatalogCollationOidIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogCollationOidIndex = `
CREATE TABLE pg_catalog.pg_collation_oid_index (
	oid OID
)`

//PgCatalogSubscription is an empty table in the pg_catalog that is not implemented yet
const PgCatalogSubscription = `
CREATE TABLE pg_catalog.pg_subscription (
	subconninfo STRING,
	subdbid OID,
	subname NAME,
	subowner OID,
	subpublications STRING[],
	subslotname NAME,
	oid OID,
	subenabled BOOL,
	subsynccommit STRING
)`

//PgCatalogNamespaceOidIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogNamespaceOidIndex = `
CREATE TABLE pg_catalog.pg_namespace_oid_index (
	oid OID
)`

//PgCatalogAmop is an empty table in the pg_catalog that is not implemented yet
const PgCatalogAmop = `
CREATE TABLE pg_catalog.pg_amop (
	amopsortfamily OID,
	amopstrategy INT2,
	amopfamily OID,
	amoplefttype OID,
	amopmethod OID,
	amopopr OID,
	amoppurpose "char",
	amoprighttype OID,
	oid OID
)`

//PgCatalogStatDatabaseConflicts is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatDatabaseConflicts = `
CREATE TABLE pg_catalog.pg_stat_database_conflicts (
	confl_bufferpin INT,
	confl_deadlock INT,
	confl_lock INT,
	confl_snapshot INT,
	confl_tablespace INT,
	datid OID,
	datname NAME
)`

//PgCatalogTablespaceOidIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogTablespaceOidIndex = `
CREATE TABLE pg_catalog.pg_tablespace_oid_index (
	oid OID
)`

//PgCatalogClassOidIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogClassOidIndex = `
CREATE TABLE pg_catalog.pg_class_oid_index (
	oid OID
)`

//PgCatalogRangeRngtypidIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogRangeRngtypidIndex = `
CREATE TABLE pg_catalog.pg_range_rngtypid_index (
	rngtypid OID
)`

//PgCatalogDescriptionOCOIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogDescriptionOCOIndex = `
CREATE TABLE pg_catalog.pg_description_o_c_o_index (
	classoid OID,
	objoid OID,
	objsubid INT4
)`

//PgCatalogOpclassOidIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogOpclassOidIndex = `
CREATE TABLE pg_catalog.pg_opclass_oid_index (
	oid OID
)`

//PgCatalogSequenceSeqrelidIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogSequenceSeqrelidIndex = `
CREATE TABLE pg_catalog.pg_sequence_seqrelid_index (
	seqrelid OID
)`

//PgCatalogTriggerOidIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogTriggerOidIndex = `
CREATE TABLE pg_catalog.pg_trigger_oid_index (
	oid OID
)`

//PgCatalogTimezoneAbbrevs is an empty table in the pg_catalog that is not implemented yet
const PgCatalogTimezoneAbbrevs = `
CREATE TABLE pg_catalog.pg_timezone_abbrevs (
	utc_offset INTERVAL,
	abbrev STRING,
	is_dst BOOL
)`

//PgCatalogTsParserOidIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogTsParserOidIndex = `
CREATE TABLE pg_catalog.pg_ts_parser_oid_index (
	oid OID
)`

//PgCatalogTransform is an empty table in the pg_catalog that is not implemented yet
const PgCatalogTransform = `
CREATE TABLE pg_catalog.pg_transform (
	trftosql REGPROC,
	trftype OID,
	oid OID,
	trffromsql REGPROC,
	trflang OID
)`

//PgCatalogExtensionOidIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogExtensionOidIndex = `
CREATE TABLE pg_catalog.pg_extension_oid_index (
	oid OID
)`

//PgCatalogStatioUserSequences is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatioUserSequences = `
CREATE TABLE pg_catalog.pg_statio_user_sequences (
	relid OID,
	relname NAME,
	schemaname NAME,
	blks_hit INT,
	blks_read INT
)`

//PgCatalogShdependReferenceIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogShdependReferenceIndex = `
CREATE TABLE pg_catalog.pg_shdepend_reference_index (
	refobjid OID,
	refclassid OID
)`

//PgCatalogForeignDataWrapperOidIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogForeignDataWrapperOidIndex = `
CREATE TABLE pg_catalog.pg_foreign_data_wrapper_oid_index (
	oid OID
)`

//PgCatalogTsConfigOidIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogTsConfigOidIndex = `
CREATE TABLE pg_catalog.pg_ts_config_oid_index (
	oid OID
)`

//PgCatalogTsDictOidIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogTsDictOidIndex = `
CREATE TABLE pg_catalog.pg_ts_dict_oid_index (
	oid OID
)`

//PgCatalogInitPrivsOCOIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogInitPrivsOCOIndex = `
CREATE TABLE pg_catalog.pg_init_privs_o_c_o_index (
	objoid OID,
	objsubid INT4,
	classoid OID
)`

//PgCatalogUserMappings is an empty table in the pg_catalog that is not implemented yet
const PgCatalogUserMappings = `
CREATE TABLE pg_catalog.pg_user_mappings (
	srvname NAME,
	umid OID,
	umoptions STRING[],
	umuser OID,
	usename NAME,
	srvid OID
)`

//PgCatalogDefaultACLRoleNspObjIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogDefaultACLRoleNspObjIndex = `
CREATE TABLE pg_catalog.pg_default_acl_role_nsp_obj_index (
	defaclobjtype "char",
	defaclrole OID,
	defaclnamespace OID
)`

//PgCatalogStatSlru is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatSlru = `
CREATE TABLE pg_catalog.pg_stat_slru (
	flushes INT,
	name STRING,
	blks_exists INT,
	blks_hit INT,
	blks_read INT,
	blks_written INT,
	blks_zeroed INT,
	stats_reset TIMESTAMPTZ,
	truncates INT
)`

//PgCatalogConstraintContypidIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogConstraintContypidIndex = `
CREATE TABLE pg_catalog.pg_constraint_contypid_index (
	contypid OID
)`

//PgCatalogStatProgressCreateIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatProgressCreateIndex = `
CREATE TABLE pg_catalog.pg_stat_progress_create_index (
	blocks_total INT,
	command STRING,
	lockers_done INT,
	partitions_done INT,
	phase STRING,
	tuples_total INT,
	blocks_done INT,
	pid INT4,
	current_locker_pid INT,
	partitions_total INT,
	datname NAME,
	index_relid OID,
	lockers_total INT,
	relid OID,
	tuples_done INT,
	datid OID
)`

//PgCatalogTransformTypeLangIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogTransformTypeLangIndex = `
CREATE TABLE pg_catalog.pg_transform_type_lang_index (
	trflang OID,
	trftype OID
)`

//PgCatalogAuthidOidIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogAuthidOidIndex = `
CREATE TABLE pg_catalog.pg_authid_oid_index (
	oid OID
)`

//PgCatalogShmemAllocations is an empty table in the pg_catalog that is not implemented yet
const PgCatalogShmemAllocations = `
CREATE TABLE pg_catalog.pg_shmem_allocations (
	allocated_size INT,
	name STRING,
	off INT,
	size INT
)`

//PgCatalogStatioSysTables is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatioSysTables = `
CREATE TABLE pg_catalog.pg_statio_sys_tables (
	schemaname NAME,
	tidx_blks_hit INT,
	tidx_blks_read INT,
	toast_blks_read INT,
	idx_blks_hit INT,
	relid OID,
	idx_blks_read INT,
	relname NAME,
	toast_blks_hit INT,
	heap_blks_hit INT,
	heap_blks_read INT
)`

//PgCatalogStatioAllSequences is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatioAllSequences = `
CREATE TABLE pg_catalog.pg_statio_all_sequences (
	schemaname NAME,
	blks_hit INT,
	blks_read INT,
	relid OID,
	relname NAME
)`

//PgCatalogPolicyOidIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogPolicyOidIndex = `
CREATE TABLE pg_catalog.pg_policy_oid_index (
	oid OID
)`

//PgCatalogShdependDependerIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogShdependDependerIndex = `
CREATE TABLE pg_catalog.pg_shdepend_depender_index (
	classid OID,
	dbid OID,
	objid OID,
	objsubid INT4
)`

//PgCatalogAttributeRelidAttnumIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogAttributeRelidAttnumIndex = `
CREATE TABLE pg_catalog.pg_attribute_relid_attnum_index (
	attnum INT2,
	attrelid OID
)`

//PgCatalogEventTriggerOidIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogEventTriggerOidIndex = `
CREATE TABLE pg_catalog.pg_event_trigger_oid_index (
	oid OID
)`

//PgCatalogAmproc is an empty table in the pg_catalog that is not implemented yet
const PgCatalogAmproc = `
CREATE TABLE pg_catalog.pg_amproc (
	oid OID,
	amproc REGPROC,
	amprocfamily OID,
	amproclefttype OID,
	amprocnum INT2,
	amprocrighttype OID
)`

//PgCatalogCastOidIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogCastOidIndex = `
CREATE TABLE pg_catalog.pg_cast_oid_index (
	oid OID
)`

//PgCatalogConstraintConparentidIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogConstraintConparentidIndex = `
CREATE TABLE pg_catalog.pg_constraint_conparentid_index (
	conparentid OID
)`

//PgCatalogStatioSysIndexes is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatioSysIndexes = `
CREATE TABLE pg_catalog.pg_statio_sys_indexes (
	idx_blks_read INT,
	indexrelid OID,
	indexrelname NAME,
	relid OID,
	relname NAME,
	schemaname NAME,
	idx_blks_hit INT
)`

//PgCatalogConversionDefaultIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogConversionDefaultIndex = `
CREATE TABLE pg_catalog.pg_conversion_default_index (
	conforencoding INT4,
	connamespace OID,
	contoencoding INT4,
	oid OID
)`

//PgCatalogStatisticExt is an empty table in the pg_catalog that is not implemented yet
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

//PgCatalogShadow is an empty table in the pg_catalog that is not implemented yet
const PgCatalogShadow = `
CREATE TABLE pg_catalog.pg_shadow (
	usecreatedb BOOL,
	usesysid OID,
	passwd STRING,
	usebypassrls BOOL,
	useconfig STRING[],
	valuntil TIMESTAMPTZ,
	usename NAME,
	userepl BOOL,
	usesuper BOOL
)`

//PgCatalogTriggerTgconstraintIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogTriggerTgconstraintIndex = `
CREATE TABLE pg_catalog.pg_trigger_tgconstraint_index (
	tgconstraint OID
)`

//PgCatalogTsTemplate is an empty table in the pg_catalog that is not implemented yet
const PgCatalogTsTemplate = `
CREATE TABLE pg_catalog.pg_ts_template (
	tmplnamespace OID,
	oid OID,
	tmplinit REGPROC,
	tmpllexize REGPROC,
	tmplname NAME
)`

//PgCatalogCastSourceTargetIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogCastSourceTargetIndex = `
CREATE TABLE pg_catalog.pg_cast_source_target_index (
	castsource OID,
	casttarget OID
)`

//PgCatalogTypeOidIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogTypeOidIndex = `
CREATE TABLE pg_catalog.pg_type_oid_index (
	oid OID
)`

//PgCatalogAmprocFamProcIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogAmprocFamProcIndex = `
CREATE TABLE pg_catalog.pg_amproc_fam_proc_index (
	amproclefttype OID,
	amprocnum INT2,
	amprocrighttype OID,
	amprocfamily OID
)`

//PgCatalogAggregateFnoidIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogAggregateFnoidIndex = `
CREATE TABLE pg_catalog.pg_aggregate_fnoid_index (
	aggfnoid REGPROC
)`

//PgCatalogStatProgressBasebackup is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatProgressBasebackup = `
CREATE TABLE pg_catalog.pg_stat_progress_basebackup (
	backup_total INT,
	phase STRING,
	pid INT4,
	tablespaces_streamed INT,
	tablespaces_total INT,
	backup_streamed INT
)`

//PgCatalogDefaultACLOidIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogDefaultACLOidIndex = `
CREATE TABLE pg_catalog.pg_default_acl_oid_index (
	oid OID
)`

//PgCatalogForeignServerOidIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogForeignServerOidIndex = `
CREATE TABLE pg_catalog.pg_foreign_server_oid_index (
	oid OID
)`

//PgCatalogStatisticExtDataStxoidIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatisticExtDataStxoidIndex = `
CREATE TABLE pg_catalog.pg_statistic_ext_data_stxoid_index (
	stxoid OID
)`

//PgCatalogTransformOidIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogTransformOidIndex = `
CREATE TABLE pg_catalog.pg_transform_oid_index (
	oid OID
)`

//PgCatalogLanguageOidIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogLanguageOidIndex = `
CREATE TABLE pg_catalog.pg_language_oid_index (
	oid OID
)`

//PgCatalogDbRoleSetting is an empty table in the pg_catalog that is not implemented yet
const PgCatalogDbRoleSetting = `
CREATE TABLE pg_catalog.pg_db_role_setting (
	setconfig STRING[],
	setdatabase OID,
	setrole OID
)`

//PgCatalogAmopOprFamIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogAmopOprFamIndex = `
CREATE TABLE pg_catalog.pg_amop_opr_fam_index (
	amopfamily OID,
	amopopr OID,
	amoppurpose "char"
)`

//PgCatalogUserMappingOidIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogUserMappingOidIndex = `
CREATE TABLE pg_catalog.pg_user_mapping_oid_index (
	oid OID
)`

//PgCatalogHbaFileRules is an empty table in the pg_catalog that is not implemented yet
const PgCatalogHbaFileRules = `
CREATE TABLE pg_catalog.pg_hba_file_rules (
	address STRING,
	auth_method STRING,
	database STRING[],
	line_number INT4,
	type STRING,
	error STRING,
	netmask STRING,
	options STRING[],
	user_name STRING[]
)`

//PgCatalogAmOidIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogAmOidIndex = `
CREATE TABLE pg_catalog.pg_am_oid_index (
	oid OID
)`

//PgCatalogStatioAllTables is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatioAllTables = `
CREATE TABLE pg_catalog.pg_statio_all_tables (
	tidx_blks_hit INT,
	tidx_blks_read INT,
	heap_blks_hit INT,
	idx_blks_hit INT,
	idx_blks_read INT,
	relid OID,
	relname NAME,
	heap_blks_read INT,
	schemaname NAME,
	toast_blks_hit INT,
	toast_blks_read INT
)`

//PgCatalogStatioUserIndexes is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatioUserIndexes = `
CREATE TABLE pg_catalog.pg_statio_user_indexes (
	relname NAME,
	schemaname NAME,
	idx_blks_hit INT,
	idx_blks_read INT,
	indexrelid OID,
	indexrelname NAME,
	relid OID
)`

//PgCatalogStatProgressAnalyze is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatProgressAnalyze = `
CREATE TABLE pg_catalog.pg_stat_progress_analyze (
	relid OID,
	sample_blks_scanned INT,
	sample_blks_total INT,
	current_child_table_relid OID,
	datid OID,
	ext_stats_computed INT,
	ext_stats_total INT,
	pid INT4,
	child_tables_done INT,
	child_tables_total INT,
	datname NAME,
	phase STRING
)`

//PgCatalogReplicationOrigin is an empty table in the pg_catalog that is not implemented yet
const PgCatalogReplicationOrigin = `
CREATE TABLE pg_catalog.pg_replication_origin (
	roident OID,
	roname STRING
)`

//PgCatalogDependReferenceIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogDependReferenceIndex = `
CREATE TABLE pg_catalog.pg_depend_reference_index (
	refobjid OID,
	refobjsubid INT4,
	refclassid OID
)`

//PgCatalogStatBgwriter is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatBgwriter = `
CREATE TABLE pg_catalog.pg_stat_bgwriter (
	checkpoints_req INT,
	buffers_alloc INT,
	buffers_backend_fsync INT,
	buffers_checkpoint INT,
	buffers_clean INT,
	checkpoint_sync_time FLOAT,
	buffers_backend INT,
	checkpoint_write_time FLOAT,
	checkpoints_timed INT,
	maxwritten_clean INT,
	stats_reset TIMESTAMPTZ
)`

//PgCatalogAttrdefAdrelidAdnumIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogAttrdefAdrelidAdnumIndex = `
CREATE TABLE pg_catalog.pg_attrdef_adrelid_adnum_index (
	adnum INT2,
	adrelid OID
)`

//PgCatalogStatSysIndexes is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatSysIndexes = `
CREATE TABLE pg_catalog.pg_stat_sys_indexes (
	indexrelid OID,
	indexrelname NAME,
	relid OID,
	relname NAME,
	schemaname NAME,
	idx_scan INT,
	idx_tup_fetch INT,
	idx_tup_read INT
)`

//PgCatalogStatisticExtOidIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatisticExtOidIndex = `
CREATE TABLE pg_catalog.pg_statistic_ext_oid_index (
	oid OID
)`

//PgCatalogForeignTableRelidIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogForeignTableRelidIndex = `
CREATE TABLE pg_catalog.pg_foreign_table_relid_index (
	ftrelid OID
)`

//PgCatalogUserMappingUserServerIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogUserMappingUserServerIndex = `
CREATE TABLE pg_catalog.pg_user_mapping_user_server_index (
	umserver OID,
	umuser OID
)`

//PgCatalogSeclabelObjectIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogSeclabelObjectIndex = `
CREATE TABLE pg_catalog.pg_seclabel_object_index (
	classoid OID,
	objoid OID,
	objsubid INT4,
	provider STRING
)`

//PgCatalogSubscriptionRelSrrelidSrsubidIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogSubscriptionRelSrrelidSrsubidIndex = `
CREATE TABLE pg_catalog.pg_subscription_rel_srrelid_srsubid_index (
	srrelid OID,
	srsubid OID
)`

//PgCatalogPublicationRelPrrelidPrpubidIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogPublicationRelPrrelidPrpubidIndex = `
CREATE TABLE pg_catalog.pg_publication_rel_prrelid_prpubid_index (
	prpubid OID,
	prrelid OID
)`

//PgCatalogInheritsParentIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogInheritsParentIndex = `
CREATE TABLE pg_catalog.pg_inherits_parent_index (
	inhparent OID
)`

//PgCatalogTsDict is an empty table in the pg_catalog that is not implemented yet
const PgCatalogTsDict = `
CREATE TABLE pg_catalog.pg_ts_dict (
	dictinitoption STRING,
	dictname NAME,
	dictnamespace OID,
	dictowner OID,
	dicttemplate OID,
	oid OID
)`

//PgCatalogStatUserTables is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatUserTables = `
CREATE TABLE pg_catalog.pg_stat_user_tables (
	relname NAME,
	schemaname NAME,
	seq_tup_read INT,
	vacuum_count INT,
	last_autovacuum TIMESTAMPTZ,
	relid OID,
	n_ins_since_vacuum INT,
	n_mod_since_analyze INT,
	n_tup_hot_upd INT,
	analyze_count INT,
	idx_tup_fetch INT,
	n_tup_del INT,
	n_tup_upd INT,
	idx_scan INT,
	last_vacuum TIMESTAMPTZ,
	last_analyze TIMESTAMPTZ,
	last_autoanalyze TIMESTAMPTZ,
	n_dead_tup INT,
	n_live_tup INT,
	n_tup_ins INT,
	seq_scan INT,
	autoanalyze_count INT,
	autovacuum_count INT
)`

//PgCatalogStatProgressCluster is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatProgressCluster = `
CREATE TABLE pg_catalog.pg_stat_progress_cluster (
	command STRING,
	datname NAME,
	heap_blks_total INT,
	heap_tuples_scanned INT,
	heap_tuples_written INT,
	pid INT4,
	cluster_index_relid OID,
	datid OID,
	heap_blks_scanned INT,
	index_rebuild_count INT,
	phase STRING,
	relid OID
)`

//PgCatalogStatXactAllTables is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatXactAllTables = `
CREATE TABLE pg_catalog.pg_stat_xact_all_tables (
	schemaname NAME,
	seq_scan INT,
	seq_tup_read INT,
	idx_tup_fetch INT,
	n_tup_hot_upd INT,
	n_tup_ins INT,
	relid OID,
	idx_scan INT,
	n_tup_del INT,
	n_tup_upd INT,
	relname NAME
)`

//PgCatalogStatDatabase is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatDatabase = `
CREATE TABLE pg_catalog.pg_stat_database (
	datname NAME,
	stats_reset TIMESTAMPTZ,
	tup_fetched INT,
	blk_read_time FLOAT,
	checksum_last_failure TIMESTAMPTZ,
	checksum_failures INT,
	conflicts INT,
	temp_files INT,
	xact_commit INT,
	blk_write_time FLOAT,
	blks_read INT,
	numbackends INT4,
	temp_bytes INT,
	blks_hit INT,
	datid OID,
	tup_inserted INT,
	tup_returned INT,
	tup_updated INT,
	xact_rollback INT,
	deadlocks INT,
	tup_deleted INT
)`

//PgCatalogShdescriptionOCIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogShdescriptionOCIndex = `
CREATE TABLE pg_catalog.pg_shdescription_o_c_index (
	classoid OID,
	objoid OID
)`

//PgCatalogPublicationRelOidIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogPublicationRelOidIndex = `
CREATE TABLE pg_catalog.pg_publication_rel_oid_index (
	oid OID
)`

//PgCatalogLargeobject is an empty table in the pg_catalog that is not implemented yet
const PgCatalogLargeobject = `
CREATE TABLE pg_catalog.pg_largeobject (
	data BYTES,
	loid OID,
	pageno INT4
)`

//PgCatalogPublicationRel is an empty table in the pg_catalog that is not implemented yet
const PgCatalogPublicationRel = `
CREATE TABLE pg_catalog.pg_publication_rel (
	prrelid OID,
	oid OID,
	prpubid OID
)`

//PgCatalogRewriteOidIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogRewriteOidIndex = `
CREATE TABLE pg_catalog.pg_rewrite_oid_index (
	oid OID
)`

//PgCatalogStatAllIndexes is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatAllIndexes = `
CREATE TABLE pg_catalog.pg_stat_all_indexes (
	relname NAME,
	schemaname NAME,
	idx_scan INT,
	idx_tup_fetch INT,
	idx_tup_read INT,
	indexrelid OID,
	indexrelname NAME,
	relid OID
)`

//PgCatalogAmopFamStratIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogAmopFamStratIndex = `
CREATE TABLE pg_catalog.pg_amop_fam_strat_index (
	amopfamily OID,
	amoplefttype OID,
	amoprighttype OID,
	amopstrategy INT2
)`

//PgCatalogProcOidIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogProcOidIndex = `
CREATE TABLE pg_catalog.pg_proc_oid_index (
	oid OID
)`

//PgCatalogDatabaseOidIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogDatabaseOidIndex = `
CREATE TABLE pg_catalog.pg_database_oid_index (
	oid OID
)`

//PgCatalogSequences is an empty table in the pg_catalog that is not implemented yet
const PgCatalogSequences = `
CREATE TABLE pg_catalog.pg_sequences (
	cycle BOOL,
	last_value INT,
	start_value INT,
	cache_size INT,
	data_type REGTYPE,
	increment_by INT,
	max_value INT,
	min_value INT,
	schemaname NAME,
	sequencename NAME,
	sequenceowner NAME
)`

//PgCatalogSubscriptionOidIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogSubscriptionOidIndex = `
CREATE TABLE pg_catalog.pg_subscription_oid_index (
	oid OID
)`

//PgCatalogEnumTypidSortorderIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogEnumTypidSortorderIndex = `
CREATE TABLE pg_catalog.pg_enum_typid_sortorder_index (
	enumsortorder FLOAT4,
	enumtypid OID
)`

//PgCatalogTsParser is an empty table in the pg_catalog that is not implemented yet
const PgCatalogTsParser = `
CREATE TABLE pg_catalog.pg_ts_parser (
	prstoken REGPROC,
	oid OID,
	prsend REGPROC,
	prsheadline REGPROC,
	prslextype REGPROC,
	prsname NAME,
	prsnamespace OID,
	prsstart REGPROC
)`

//PgCatalogTsConfigMap is an empty table in the pg_catalog that is not implemented yet
const PgCatalogTsConfigMap = `
CREATE TABLE pg_catalog.pg_ts_config_map (
	maptokentype INT4,
	mapcfg OID,
	mapdict OID,
	mapseqno INT4
)`

//PgCatalogAttrdefOidIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogAttrdefOidIndex = `
CREATE TABLE pg_catalog.pg_attrdef_oid_index (
	oid OID
)`

//PgCatalogTimezoneNames is an empty table in the pg_catalog that is not implemented yet
const PgCatalogTimezoneNames = `
CREATE TABLE pg_catalog.pg_timezone_names (
	abbrev STRING,
	is_dst BOOL,
	name STRING,
	utc_offset INTERVAL
)`

//PgCatalogTsTemplateOidIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogTsTemplateOidIndex = `
CREATE TABLE pg_catalog.pg_ts_template_oid_index (
	oid OID
)`

//PgCatalogStatisticExtRelidIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatisticExtRelidIndex = `
CREATE TABLE pg_catalog.pg_statistic_ext_relid_index (
	stxrelid OID
)`

//PgCatalogIndexIndexrelidIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogIndexIndexrelidIndex = `
CREATE TABLE pg_catalog.pg_index_indexrelid_index (
	indexrelid OID
)`

//PgCatalogAmopOidIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogAmopOidIndex = `
CREATE TABLE pg_catalog.pg_amop_oid_index (
	oid OID
)`

//PgCatalogAmprocOidIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogAmprocOidIndex = `
CREATE TABLE pg_catalog.pg_amproc_oid_index (
	oid OID
)`

//PgCatalogRules is an empty table in the pg_catalog that is not implemented yet
const PgCatalogRules = `
CREATE TABLE pg_catalog.pg_rules (
	definition STRING,
	rulename NAME,
	schemaname NAME,
	tablename NAME
)`

//PgCatalogOpfamily is an empty table in the pg_catalog that is not implemented yet
const PgCatalogOpfamily = `
CREATE TABLE pg_catalog.pg_opfamily (
	opfname NAME,
	opfnamespace OID,
	opfowner OID,
	oid OID,
	opfmethod OID
)`

//PgCatalogStatXactSysTables is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatXactSysTables = `
CREATE TABLE pg_catalog.pg_stat_xact_sys_tables (
	schemaname NAME,
	seq_scan INT,
	seq_tup_read INT,
	idx_scan INT,
	idx_tup_fetch INT,
	n_tup_del INT,
	n_tup_upd INT,
	relid OID,
	n_tup_hot_upd INT,
	n_tup_ins INT,
	relname NAME
)`

//PgCatalogPolicies is an empty table in the pg_catalog that is not implemented yet
const PgCatalogPolicies = `
CREATE TABLE pg_catalog.pg_policies (
	roles NAME[],
	schemaname NAME,
	tablename NAME,
	with_check STRING,
	cmd STRING,
	permissive STRING,
	policyname NAME,
	qual STRING
)`

//PgCatalogConstraintOidIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogConstraintOidIndex = `
CREATE TABLE pg_catalog.pg_constraint_oid_index (
	oid OID
)`

//PgCatalogStatSysTables is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatSysTables = `
CREATE TABLE pg_catalog.pg_stat_sys_tables (
	relid OID,
	vacuum_count INT,
	idx_scan INT,
	n_live_tup INT,
	n_tup_hot_upd INT,
	last_autovacuum TIMESTAMPTZ,
	last_vacuum TIMESTAMPTZ,
	n_dead_tup INT,
	n_ins_since_vacuum INT,
	n_tup_ins INT,
	autoanalyze_count INT,
	idx_tup_fetch INT,
	last_analyze TIMESTAMPTZ,
	schemaname NAME,
	seq_tup_read INT,
	analyze_count INT,
	autovacuum_count INT,
	relname NAME,
	n_tup_upd INT,
	seq_scan INT,
	last_autoanalyze TIMESTAMPTZ,
	n_mod_since_analyze INT,
	n_tup_del INT
)`

//PgCatalogStatXactUserFunctions is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatXactUserFunctions = `
CREATE TABLE pg_catalog.pg_stat_xact_user_functions (
	calls INT,
	funcid OID,
	funcname NAME,
	schemaname NAME,
	self_time FLOAT,
	total_time FLOAT
)`

//PgCatalogAvailableExtensionVersions is an empty table in the pg_catalog that is not implemented yet
const PgCatalogAvailableExtensionVersions = `
CREATE TABLE pg_catalog.pg_available_extension_versions (
	schema NAME,
	trusted BOOL,
	comment STRING,
	installed BOOL,
	requires NAME[],
	version STRING,
	name NAME,
	relocatable BOOL,
	superuser BOOL
)`

//PgCatalogStatAllTables is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatAllTables = `
CREATE TABLE pg_catalog.pg_stat_all_tables (
	schemaname NAME,
	n_live_tup INT,
	n_tup_del INT,
	n_tup_hot_upd INT,
	seq_scan INT,
	idx_scan INT,
	idx_tup_fetch INT,
	last_analyze TIMESTAMPTZ,
	n_dead_tup INT,
	n_ins_since_vacuum INT,
	n_mod_since_analyze INT,
	n_tup_upd INT,
	relid OID,
	analyze_count INT,
	autoanalyze_count INT,
	last_autovacuum TIMESTAMPTZ,
	relname NAME,
	seq_tup_read INT,
	vacuum_count INT,
	n_tup_ins INT,
	autovacuum_count INT,
	last_autoanalyze TIMESTAMPTZ,
	last_vacuum TIMESTAMPTZ
)`

//PgCatalogAuthMembersRoleMemberIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogAuthMembersRoleMemberIndex = `
CREATE TABLE pg_catalog.pg_auth_members_role_member_index (
	roleid OID,
	member OID
)`

//PgCatalogAuthMembersMemberRoleIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogAuthMembersMemberRoleIndex = `
CREATE TABLE pg_catalog.pg_auth_members_member_role_index (
	member OID,
	roleid OID
)`

//PgCatalogInheritsRelidSeqnoIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogInheritsRelidSeqnoIndex = `
CREATE TABLE pg_catalog.pg_inherits_relid_seqno_index (
	inhrelid OID,
	inhseqno INT4
)`

//PgCatalogOpfamilyOidIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogOpfamilyOidIndex = `
CREATE TABLE pg_catalog.pg_opfamily_oid_index (
	oid OID
)`

//PgCatalogClassTblspcRelfilenodeIndex is an empty table in the pg_catalog that is not implemented yet
const PgCatalogClassTblspcRelfilenodeIndex = `
CREATE TABLE pg_catalog.pg_class_tblspc_relfilenode_index (
	relfilenode OID,
	reltablespace OID
)`

//PgCatalogCursors is an empty table in the pg_catalog that is not implemented yet
const PgCatalogCursors = `
CREATE TABLE pg_catalog.pg_cursors (
	name STRING,
	statement STRING,
	creation_time TIMESTAMPTZ,
	is_binary BOOL,
	is_holdable BOOL,
	is_scrollable BOOL
)`

//PgCatalogStatGssapi is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatGssapi = `
CREATE TABLE pg_catalog.pg_stat_gssapi (
	encrypted BOOL,
	gss_authenticated BOOL,
	pid INT4,
	principal STRING
)`

//PgCatalogStatSsl is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatSsl = `
CREATE TABLE pg_catalog.pg_stat_ssl (
	version STRING,
	cipher STRING,
	client_dn STRING,
	client_serial DECIMAL,
	issuer_dn STRING,
	pid INT4,
	ssl BOOL,
	bits INT4,
	compression BOOL
)`

//PgCatalogStatUserIndexes is an empty table in the pg_catalog that is not implemented yet
const PgCatalogStatUserIndexes = `
CREATE TABLE pg_catalog.pg_stat_user_indexes (
	idx_scan INT,
	idx_tup_fetch INT,
	idx_tup_read INT,
	indexrelid OID,
	indexrelname NAME,
	relid OID,
	relname NAME,
	schemaname NAME
)`
