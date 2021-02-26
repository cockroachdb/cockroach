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
	aggfinalmodify "char",
	aggmfinalmodify "char"
)`

// PgCatalogStatioUserTables is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogStatioUserTables = `
CREATE TABLE pg_catalog.pg_statio_user_tables (
	idx_blks_read INT,
	relid OID,
	tidx_blks_read INT,
	toast_blks_read INT,
	heap_blks_hit INT,
	heap_blks_read INT,
	idx_blks_hit INT,
	relname NAME,
	schemaname NAME,
	tidx_blks_hit INT,
	toast_blks_hit INT
)`

// PgCatalogStatioSysTables is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogStatioSysTables = `
CREATE TABLE pg_catalog.pg_statio_sys_tables (
	idx_blks_hit INT,
	relid OID,
	relname NAME,
	tidx_blks_hit INT,
	tidx_blks_read INT,
	toast_blks_read INT,
	heap_blks_read INT,
	idx_blks_read INT,
	schemaname NAME,
	toast_blks_hit INT,
	heap_blks_hit INT
)`

// PgCatalogAmopOprFamIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogAmopOprFamIndex = `
CREATE TABLE pg_catalog.pg_amop_opr_fam_index (
	amoppurpose "char",
	amopfamily OID,
	amopopr OID
)`

// PgCatalogDatabaseOidIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogDatabaseOidIndex = `
CREATE TABLE pg_catalog.pg_database_oid_index (
	oid OID
)`

// PgCatalogExtensionOidIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogExtensionOidIndex = `
CREATE TABLE pg_catalog.pg_extension_oid_index (
	oid OID
)`

// PgCatalogIndexIndexrelidIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogIndexIndexrelidIndex = `
CREATE TABLE pg_catalog.pg_index_indexrelid_index (
	indexrelid OID
)`

// PgCatalogLargeobject is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogLargeobject = `
CREATE TABLE pg_catalog.pg_largeobject (
	data BYTES,
	loid OID,
	pageno INT4
)`

// PgCatalogTriggerOidIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogTriggerOidIndex = `
CREATE TABLE pg_catalog.pg_trigger_oid_index (
	oid OID
)`

// PgCatalogTsConfigMapIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogTsConfigMapIndex = `
CREATE TABLE pg_catalog.pg_ts_config_map_index (
	mapcfg OID,
	mapseqno INT4,
	maptokentype INT4
)`

// PgCatalogAmopOidIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogAmopOidIndex = `
CREATE TABLE pg_catalog.pg_amop_oid_index (
	oid OID
)`

// PgCatalogNamespaceOidIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogNamespaceOidIndex = `
CREATE TABLE pg_catalog.pg_namespace_oid_index (
	oid OID
)`

// PgCatalogConfig is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogConfig = `
CREATE TABLE pg_catalog.pg_config (
	name STRING,
	setting STRING
)`

// PgCatalogPublicationOidIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogPublicationOidIndex = `
CREATE TABLE pg_catalog.pg_publication_oid_index (
	oid OID
)`

// PgCatalogAttributeRelidAttnumIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogAttributeRelidAttnumIndex = `
CREATE TABLE pg_catalog.pg_attribute_relid_attnum_index (
	attnum INT2,
	attrelid OID
)`

// PgCatalogStatioAllTables is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogStatioAllTables = `
CREATE TABLE pg_catalog.pg_statio_all_tables (
	schemaname NAME,
	tidx_blks_hit INT,
	tidx_blks_read INT,
	toast_blks_read INT,
	heap_blks_read INT,
	idx_blks_read INT,
	relid OID,
	relname NAME,
	heap_blks_hit INT,
	idx_blks_hit INT,
	toast_blks_hit INT
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

// PgCatalogInheritsRelidSeqnoIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogInheritsRelidSeqnoIndex = `
CREATE TABLE pg_catalog.pg_inherits_relid_seqno_index (
	inhrelid OID,
	inhseqno INT4
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

// PgCatalogPolicyOidIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogPolicyOidIndex = `
CREATE TABLE pg_catalog.pg_policy_oid_index (
	oid OID
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

// PgCatalogPublicationRelPrrelidPrpubidIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogPublicationRelPrrelidPrpubidIndex = `
CREATE TABLE pg_catalog.pg_publication_rel_prrelid_prpubid_index (
	prpubid OID,
	prrelid OID
)`

// PgCatalogStatUserTables is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogStatUserTables = `
CREATE TABLE pg_catalog.pg_stat_user_tables (
	last_analyze TIMESTAMPTZ,
	n_dead_tup INT,
	n_ins_since_vacuum INT,
	n_live_tup INT,
	n_tup_ins INT,
	seq_tup_read INT,
	autovacuum_count INT,
	last_autovacuum TIMESTAMPTZ,
	last_vacuum TIMESTAMPTZ,
	n_mod_since_analyze INT,
	n_tup_del INT,
	n_tup_hot_upd INT,
	relid OID,
	last_autoanalyze TIMESTAMPTZ,
	n_tup_upd INT,
	relname NAME,
	analyze_count INT,
	autoanalyze_count INT,
	idx_scan INT,
	idx_tup_fetch INT,
	schemaname NAME,
	seq_scan INT,
	vacuum_count INT
)`

// PgCatalogAmprocFamProcIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogAmprocFamProcIndex = `
CREATE TABLE pg_catalog.pg_amproc_fam_proc_index (
	amprocfamily OID,
	amproclefttype OID,
	amprocnum INT2,
	amprocrighttype OID
)`

// PgCatalogUserMappingOidIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogUserMappingOidIndex = `
CREATE TABLE pg_catalog.pg_user_mapping_oid_index (
	oid OID
)`

// PgCatalogPartitionedTablePartrelidIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogPartitionedTablePartrelidIndex = `
CREATE TABLE pg_catalog.pg_partitioned_table_partrelid_index (
	partrelid OID
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

// PgCatalogStatioSysSequences is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogStatioSysSequences = `
CREATE TABLE pg_catalog.pg_statio_sys_sequences (
	relname NAME,
	schemaname NAME,
	blks_hit INT,
	blks_read INT,
	relid OID
)`

// PgCatalogAuthidOidIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogAuthidOidIndex = `
CREATE TABLE pg_catalog.pg_authid_oid_index (
	oid OID
)`

// PgCatalogIndexIndrelidIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogIndexIndrelidIndex = `
CREATE TABLE pg_catalog.pg_index_indrelid_index (
	indrelid OID
)`

// PgCatalogStatProgressCreateIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogStatProgressCreateIndex = `
CREATE TABLE pg_catalog.pg_stat_progress_create_index (
	blocks_total INT,
	partitions_total INT,
	relid OID,
	tuples_done INT,
	current_locker_pid INT,
	datid OID,
	index_relid OID,
	partitions_done INT,
	blocks_done INT,
	datname NAME,
	lockers_total INT,
	phase STRING,
	command STRING,
	lockers_done INT,
	pid INT4,
	tuples_total INT
)`

// PgCatalogPublicationTables is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogPublicationTables = `
CREATE TABLE pg_catalog.pg_publication_tables (
	pubname NAME,
	schemaname NAME,
	tablename NAME
)`

// PgCatalogTriggerTgconstraintIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogTriggerTgconstraintIndex = `
CREATE TABLE pg_catalog.pg_trigger_tgconstraint_index (
	tgconstraint OID
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

// PgCatalogSubscriptionRelSrrelidSrsubidIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogSubscriptionRelSrrelidSrsubidIndex = `
CREATE TABLE pg_catalog.pg_subscription_rel_srrelid_srsubid_index (
	srrelid OID,
	srsubid OID
)`

// PgCatalogStatSysTables is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogStatSysTables = `
CREATE TABLE pg_catalog.pg_stat_sys_tables (
	relid OID,
	relname NAME,
	vacuum_count INT,
	n_ins_since_vacuum INT,
	n_tup_ins INT,
	schemaname NAME,
	seq_tup_read INT,
	autovacuum_count INT,
	idx_scan INT,
	last_vacuum TIMESTAMPTZ,
	n_dead_tup INT,
	n_mod_since_analyze INT,
	n_tup_del INT,
	n_tup_upd INT,
	seq_scan INT,
	autoanalyze_count INT,
	idx_tup_fetch INT,
	last_autoanalyze TIMESTAMPTZ,
	n_live_tup INT,
	analyze_count INT,
	last_analyze TIMESTAMPTZ,
	last_autovacuum TIMESTAMPTZ,
	n_tup_hot_upd INT
)`

// PgCatalogShseclabelObjectIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogShseclabelObjectIndex = `
CREATE TABLE pg_catalog.pg_shseclabel_object_index (
	classoid OID,
	objoid OID,
	provider STRING
)`

// PgCatalogConstraintOidIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogConstraintOidIndex = `
CREATE TABLE pg_catalog.pg_constraint_oid_index (
	oid OID
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

// PgCatalogEventTriggerOidIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogEventTriggerOidIndex = `
CREATE TABLE pg_catalog.pg_event_trigger_oid_index (
	oid OID
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

// PgCatalogStatGssapi is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogStatGssapi = `
CREATE TABLE pg_catalog.pg_stat_gssapi (
	encrypted BOOL,
	gss_authenticated BOOL,
	pid INT4,
	principal STRING
)`

// PgCatalogTsParserOidIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogTsParserOidIndex = `
CREATE TABLE pg_catalog.pg_ts_parser_oid_index (
	oid OID
)`

// PgCatalogReplicationOriginRonameIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogReplicationOriginRonameIndex = `
CREATE TABLE pg_catalog.pg_replication_origin_roname_index (
	roname STRING
)`

// PgCatalogDbRoleSettingDatabaseidRolIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogDbRoleSettingDatabaseidRolIndex = `
CREATE TABLE pg_catalog.pg_db_role_setting_databaseid_rol_index (
	setdatabase OID,
	setrole OID
)`

// PgCatalogLargeobjectLoidPnIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogLargeobjectLoidPnIndex = `
CREATE TABLE pg_catalog.pg_largeobject_loid_pn_index (
	loid OID,
	pageno INT4
)`

// PgCatalogTablespaceOidIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogTablespaceOidIndex = `
CREATE TABLE pg_catalog.pg_tablespace_oid_index (
	oid OID
)`

// PgCatalogCollationOidIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogCollationOidIndex = `
CREATE TABLE pg_catalog.pg_collation_oid_index (
	oid OID
)`

// PgCatalogAggregateFnoidIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogAggregateFnoidIndex = `
CREATE TABLE pg_catalog.pg_aggregate_fnoid_index (
	aggfnoid REGPROC
)`

// PgCatalogTsTemplateOidIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogTsTemplateOidIndex = `
CREATE TABLE pg_catalog.pg_ts_template_oid_index (
	oid OID
)`

// PgCatalogReplicationOriginRoiidentIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogReplicationOriginRoiidentIndex = `
CREATE TABLE pg_catalog.pg_replication_origin_roiident_index (
	roident OID
)`

// PgCatalogAttrdefOidIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogAttrdefOidIndex = `
CREATE TABLE pg_catalog.pg_attrdef_oid_index (
	oid OID
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

// PgCatalogClassOidIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogClassOidIndex = `
CREATE TABLE pg_catalog.pg_class_oid_index (
	oid OID
)`

// PgCatalogStatisticExtDataStxoidIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogStatisticExtDataStxoidIndex = `
CREATE TABLE pg_catalog.pg_statistic_ext_data_stxoid_index (
	stxoid OID
)`

// PgCatalogTsDictOidIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogTsDictOidIndex = `
CREATE TABLE pg_catalog.pg_ts_dict_oid_index (
	oid OID
)`

// PgCatalogCastOidIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogCastOidIndex = `
CREATE TABLE pg_catalog.pg_cast_oid_index (
	oid OID
)`

// PgCatalogGroup is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogGroup = `
CREATE TABLE pg_catalog.pg_group (
	grolist OID[],
	groname NAME,
	grosysid OID
)`

// PgCatalogDependDependerIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogDependDependerIndex = `
CREATE TABLE pg_catalog.pg_depend_depender_index (
	classid OID,
	objid OID,
	objsubid INT4
)`

// PgCatalogStatProgressBasebackup is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogStatProgressBasebackup = `
CREATE TABLE pg_catalog.pg_stat_progress_basebackup (
	backup_streamed INT,
	backup_total INT,
	phase STRING,
	pid INT4,
	tablespaces_streamed INT,
	tablespaces_total INT
)`

// PgCatalogDescriptionOCOIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogDescriptionOCOIndex = `
CREATE TABLE pg_catalog.pg_description_o_c_o_index (
	classoid OID,
	objoid OID,
	objsubid INT4
)`

// PgCatalogStatAllIndexes is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogStatAllIndexes = `
CREATE TABLE pg_catalog.pg_stat_all_indexes (
	schemaname NAME,
	idx_scan INT,
	idx_tup_fetch INT,
	idx_tup_read INT,
	indexrelid OID,
	indexrelname NAME,
	relid OID,
	relname NAME
)`

// PgCatalogStatBgwriter is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogStatBgwriter = `
CREATE TABLE pg_catalog.pg_stat_bgwriter (
	buffers_backend_fsync INT,
	buffers_clean INT,
	checkpoint_sync_time FLOAT,
	checkpoints_req INT,
	checkpoints_timed INT,
	maxwritten_clean INT,
	buffers_alloc INT,
	buffers_backend INT,
	buffers_checkpoint INT,
	checkpoint_write_time FLOAT,
	stats_reset TIMESTAMPTZ
)`

// PgCatalogInitPrivsOCOIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogInitPrivsOCOIndex = `
CREATE TABLE pg_catalog.pg_init_privs_o_c_o_index (
	classoid OID,
	objoid OID,
	objsubid INT4
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

// PgCatalogConstraintContypidIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogConstraintContypidIndex = `
CREATE TABLE pg_catalog.pg_constraint_contypid_index (
	contypid OID
)`

// PgCatalogStatProgressCluster is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogStatProgressCluster = `
CREATE TABLE pg_catalog.pg_stat_progress_cluster (
	heap_blks_scanned INT,
	heap_tuples_scanned INT,
	index_rebuild_count INT,
	phase STRING,
	relid OID,
	cluster_index_relid OID,
	command STRING,
	datname NAME,
	pid INT4,
	datid OID,
	heap_blks_total INT,
	heap_tuples_written INT
)`

// PgCatalogConversionDefaultIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogConversionDefaultIndex = `
CREATE TABLE pg_catalog.pg_conversion_default_index (
	oid OID,
	conforencoding INT4,
	connamespace OID,
	contoencoding INT4
)`

// PgCatalogStatSsl is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogStatSsl = `
CREATE TABLE pg_catalog.pg_stat_ssl (
	pid INT4,
	ssl BOOL,
	client_dn STRING,
	client_serial DECIMAL,
	compression BOOL,
	issuer_dn STRING,
	version STRING,
	bits INT4,
	cipher STRING
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

// PgCatalogStatisticExtRelidIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogStatisticExtRelidIndex = `
CREATE TABLE pg_catalog.pg_statistic_ext_relid_index (
	stxrelid OID
)`

// PgCatalogAttrdefAdrelidAdnumIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogAttrdefAdrelidAdnumIndex = `
CREATE TABLE pg_catalog.pg_attrdef_adrelid_adnum_index (
	adnum INT2,
	adrelid OID
)`

// PgCatalogTypeOidIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogTypeOidIndex = `
CREATE TABLE pg_catalog.pg_type_oid_index (
	oid OID
)`

// PgCatalogStatXactUserTables is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogStatXactUserTables = `
CREATE TABLE pg_catalog.pg_stat_xact_user_tables (
	n_tup_hot_upd INT,
	n_tup_ins INT,
	relid OID,
	seq_scan INT,
	n_tup_del INT,
	idx_tup_fetch INT,
	n_tup_upd INT,
	relname NAME,
	schemaname NAME,
	seq_tup_read INT,
	idx_scan INT
)`

// PgCatalogAuthMembersMemberRoleIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogAuthMembersMemberRoleIndex = `
CREATE TABLE pg_catalog.pg_auth_members_member_role_index (
	member OID,
	roleid OID
)`

// PgCatalogStatAllTables is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogStatAllTables = `
CREATE TABLE pg_catalog.pg_stat_all_tables (
	n_tup_ins INT,
	schemaname NAME,
	analyze_count INT,
	autovacuum_count INT,
	last_autovacuum TIMESTAMPTZ,
	seq_tup_read INT,
	vacuum_count INT,
	last_vacuum TIMESTAMPTZ,
	n_ins_since_vacuum INT,
	n_tup_del INT,
	n_live_tup INT,
	relid OID,
	seq_scan INT,
	autoanalyze_count INT,
	idx_scan INT,
	idx_tup_fetch INT,
	n_mod_since_analyze INT,
	n_tup_hot_upd INT,
	n_tup_upd INT,
	relname NAME,
	last_analyze TIMESTAMPTZ,
	last_autoanalyze TIMESTAMPTZ,
	n_dead_tup INT
)`

// PgCatalogEnumOidIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogEnumOidIndex = `
CREATE TABLE pg_catalog.pg_enum_oid_index (
	oid OID
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

// PgCatalogEnumTypidSortorderIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogEnumTypidSortorderIndex = `
CREATE TABLE pg_catalog.pg_enum_typid_sortorder_index (
	enumsortorder FLOAT4,
	enumtypid OID
)`

// PgCatalogProcOidIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogProcOidIndex = `
CREATE TABLE pg_catalog.pg_proc_oid_index (
	oid OID
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

// PgCatalogRewriteOidIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogRewriteOidIndex = `
CREATE TABLE pg_catalog.pg_rewrite_oid_index (
	oid OID
)`

// PgCatalogTsConfigOidIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogTsConfigOidIndex = `
CREATE TABLE pg_catalog.pg_ts_config_oid_index (
	oid OID
)`

// PgCatalogStatUserFunctions is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogStatUserFunctions = `
CREATE TABLE pg_catalog.pg_stat_user_functions (
	funcname NAME,
	schemaname NAME,
	self_time FLOAT,
	total_time FLOAT,
	calls INT,
	funcid OID
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

// PgCatalogStatXactUserFunctions is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogStatXactUserFunctions = `
CREATE TABLE pg_catalog.pg_stat_xact_user_functions (
	total_time FLOAT,
	calls INT,
	funcid OID,
	funcname NAME,
	schemaname NAME,
	self_time FLOAT
)`

// PgCatalogSeclabelObjectIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogSeclabelObjectIndex = `
CREATE TABLE pg_catalog.pg_seclabel_object_index (
	objoid OID,
	objsubid INT4,
	provider STRING,
	classoid OID
)`

// PgCatalogTransformTypeLangIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogTransformTypeLangIndex = `
CREATE TABLE pg_catalog.pg_transform_type_lang_index (
	trflang OID,
	trftype OID
)`

// PgCatalogStatioUserIndexes is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogStatioUserIndexes = `
CREATE TABLE pg_catalog.pg_statio_user_indexes (
	idx_blks_read INT,
	indexrelid OID,
	indexrelname NAME,
	relid OID,
	relname NAME,
	schemaname NAME,
	idx_blks_hit INT
)`

// PgCatalogTimezoneAbbrevs is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogTimezoneAbbrevs = `
CREATE TABLE pg_catalog.pg_timezone_abbrevs (
	abbrev STRING,
	is_dst BOOL,
	utc_offset INTERVAL
)`

// PgCatalogAmopFamStratIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogAmopFamStratIndex = `
CREATE TABLE pg_catalog.pg_amop_fam_strat_index (
	amoplefttype OID,
	amoprighttype OID,
	amopstrategy INT2,
	amopfamily OID
)`

// PgCatalogForeignDataWrapperOidIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogForeignDataWrapperOidIndex = `
CREATE TABLE pg_catalog.pg_foreign_data_wrapper_oid_index (
	oid OID
)`

// PgCatalogShdescriptionOCIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogShdescriptionOCIndex = `
CREATE TABLE pg_catalog.pg_shdescription_o_c_index (
	classoid OID,
	objoid OID
)`

// PgCatalogPublicationRelOidIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogPublicationRelOidIndex = `
CREATE TABLE pg_catalog.pg_publication_rel_oid_index (
	oid OID
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

// PgCatalogOpclassOidIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogOpclassOidIndex = `
CREATE TABLE pg_catalog.pg_opclass_oid_index (
	oid OID
)`

// PgCatalogInheritsParentIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogInheritsParentIndex = `
CREATE TABLE pg_catalog.pg_inherits_parent_index (
	inhparent OID
)`

// PgCatalogStatSlru is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogStatSlru = `
CREATE TABLE pg_catalog.pg_stat_slru (
	blks_exists INT,
	blks_hit INT,
	blks_written INT,
	flushes INT,
	name STRING,
	truncates INT,
	blks_read INT,
	blks_zeroed INT,
	stats_reset TIMESTAMPTZ
)`

// PgCatalogStatProgressAnalyze is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogStatProgressAnalyze = `
CREATE TABLE pg_catalog.pg_stat_progress_analyze (
	sample_blks_scanned INT,
	datname NAME,
	ext_stats_computed INT,
	relid OID,
	datid OID,
	ext_stats_total INT,
	phase STRING,
	pid INT4,
	sample_blks_total INT,
	child_tables_done INT,
	child_tables_total INT,
	current_child_table_relid OID
)`

// PgCatalogShdependReferenceIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogShdependReferenceIndex = `
CREATE TABLE pg_catalog.pg_shdepend_reference_index (
	refclassid OID,
	refobjid OID
)`

// PgCatalogAuthMembersRoleMemberIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogAuthMembersRoleMemberIndex = `
CREATE TABLE pg_catalog.pg_auth_members_role_member_index (
	member OID,
	roleid OID
)`

// PgCatalogOperatorOidIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogOperatorOidIndex = `
CREATE TABLE pg_catalog.pg_operator_oid_index (
	oid OID
)`

// PgCatalogStatioAllIndexes is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogStatioAllIndexes = `
CREATE TABLE pg_catalog.pg_statio_all_indexes (
	idx_blks_hit INT,
	idx_blks_read INT,
	indexrelid OID,
	indexrelname NAME,
	relid OID,
	relname NAME,
	schemaname NAME
)`

// PgCatalogForeignServerOidIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogForeignServerOidIndex = `
CREATE TABLE pg_catalog.pg_foreign_server_oid_index (
	oid OID
)`

// PgCatalogDefaultACLRoleNspObjIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogDefaultACLRoleNspObjIndex = `
CREATE TABLE pg_catalog.pg_default_acl_role_nsp_obj_index (
	defaclnamespace OID,
	defaclobjtype "char",
	defaclrole OID
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

// PgCatalogStatDatabaseConflicts is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogStatDatabaseConflicts = `
CREATE TABLE pg_catalog.pg_stat_database_conflicts (
	confl_lock INT,
	confl_snapshot INT,
	confl_tablespace INT,
	datid OID,
	datname NAME,
	confl_bufferpin INT,
	confl_deadlock INT
)`

// PgCatalogRangeRngtypidIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogRangeRngtypidIndex = `
CREATE TABLE pg_catalog.pg_range_rngtypid_index (
	rngtypid OID
)`

// PgCatalogTransformOidIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogTransformOidIndex = `
CREATE TABLE pg_catalog.pg_transform_oid_index (
	oid OID
)`

// PgCatalogStatioUserSequences is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogStatioUserSequences = `
CREATE TABLE pg_catalog.pg_statio_user_sequences (
	relname NAME,
	schemaname NAME,
	blks_hit INT,
	blks_read INT,
	relid OID
)`

// PgCatalogUserMappingUserServerIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogUserMappingUserServerIndex = `
CREATE TABLE pg_catalog.pg_user_mapping_user_server_index (
	umserver OID,
	umuser OID
)`

// PgCatalogSubscriptionOidIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogSubscriptionOidIndex = `
CREATE TABLE pg_catalog.pg_subscription_oid_index (
	oid OID
)`

// PgCatalogStatisticExtOidIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogStatisticExtOidIndex = `
CREATE TABLE pg_catalog.pg_statistic_ext_oid_index (
	oid OID
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

// PgCatalogSequenceSeqrelidIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogSequenceSeqrelidIndex = `
CREATE TABLE pg_catalog.pg_sequence_seqrelid_index (
	seqrelid OID
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

// PgCatalogLanguageOidIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogLanguageOidIndex = `
CREATE TABLE pg_catalog.pg_language_oid_index (
	oid OID
)`

// PgCatalogClassTblspcRelfilenodeIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogClassTblspcRelfilenodeIndex = `
CREATE TABLE pg_catalog.pg_class_tblspc_relfilenode_index (
	relfilenode OID,
	reltablespace OID
)`

// PgCatalogDependReferenceIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogDependReferenceIndex = `
CREATE TABLE pg_catalog.pg_depend_reference_index (
	refclassid OID,
	refobjid OID,
	refobjsubid INT4
)`

// PgCatalogStatisticRelidAttInhIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogStatisticRelidAttInhIndex = `
CREATE TABLE pg_catalog.pg_statistic_relid_att_inh_index (
	staattnum INT2,
	stainherit BOOL,
	starelid OID
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

// PgCatalogConversionOidIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogConversionOidIndex = `
CREATE TABLE pg_catalog.pg_conversion_oid_index (
	oid OID
)`

// PgCatalogStatXactAllTables is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogStatXactAllTables = `
CREATE TABLE pg_catalog.pg_stat_xact_all_tables (
	n_tup_del INT,
	n_tup_hot_upd INT,
	n_tup_ins INT,
	schemaname NAME,
	seq_scan INT,
	seq_tup_read INT,
	idx_scan INT,
	idx_tup_fetch INT,
	n_tup_upd INT,
	relid OID,
	relname NAME
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

// PgCatalogAmOidIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogAmOidIndex = `
CREATE TABLE pg_catalog.pg_am_oid_index (
	oid OID
)`

// PgCatalogConstraintConparentidIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogConstraintConparentidIndex = `
CREATE TABLE pg_catalog.pg_constraint_conparentid_index (
	conparentid OID
)`

// PgCatalogStatXactSysTables is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogStatXactSysTables = `
CREATE TABLE pg_catalog.pg_stat_xact_sys_tables (
	n_tup_del INT,
	n_tup_hot_upd INT,
	n_tup_upd INT,
	relname NAME,
	schemaname NAME,
	seq_scan INT,
	idx_scan INT,
	idx_tup_fetch INT,
	seq_tup_read INT,
	n_tup_ins INT,
	relid OID
)`

// PgCatalogStatDatabase is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogStatDatabase = `
CREATE TABLE pg_catalog.pg_stat_database (
	blks_read INT,
	datid OID,
	deadlocks INT,
	temp_files INT,
	tup_updated INT,
	conflicts INT,
	temp_bytes INT,
	tup_returned INT,
	xact_rollback INT,
	blk_read_time FLOAT,
	blks_hit INT,
	checksum_failures INT,
	tup_deleted INT,
	xact_commit INT,
	blk_write_time FLOAT,
	checksum_last_failure TIMESTAMPTZ,
	datname NAME,
	numbackends INT4,
	stats_reset TIMESTAMPTZ,
	tup_fetched INT,
	tup_inserted INT
)`

// PgCatalogStatSysIndexes is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogStatSysIndexes = `
CREATE TABLE pg_catalog.pg_stat_sys_indexes (
	idx_tup_fetch INT,
	idx_tup_read INT,
	indexrelid OID,
	indexrelname NAME,
	relid OID,
	relname NAME,
	schemaname NAME,
	idx_scan INT
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

// PgCatalogCastSourceTargetIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogCastSourceTargetIndex = `
CREATE TABLE pg_catalog.pg_cast_source_target_index (
	castsource OID,
	casttarget OID
)`

// PgCatalogReplicationOrigin is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogReplicationOrigin = `
CREATE TABLE pg_catalog.pg_replication_origin (
	roident OID,
	roname STRING
)`

// PgCatalogOpfamilyOidIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogOpfamilyOidIndex = `
CREATE TABLE pg_catalog.pg_opfamily_oid_index (
	oid OID
)`

// PgCatalogStatUserIndexes is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogStatUserIndexes = `
CREATE TABLE pg_catalog.pg_stat_user_indexes (
	relname NAME,
	schemaname NAME,
	idx_scan INT,
	idx_tup_fetch INT,
	idx_tup_read INT,
	indexrelid OID,
	indexrelname NAME,
	relid OID
)`

// PgCatalogAmprocOidIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogAmprocOidIndex = `
CREATE TABLE pg_catalog.pg_amproc_oid_index (
	oid OID
)`

// PgCatalogDefaultACLOidIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogDefaultACLOidIndex = `
CREATE TABLE pg_catalog.pg_default_acl_oid_index (
	oid OID
)`

// PgCatalogForeignTableRelidIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogForeignTableRelidIndex = `
CREATE TABLE pg_catalog.pg_foreign_table_relid_index (
	ftrelid OID
)`

// PgCatalogStatProgressVacuum is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogStatProgressVacuum = `
CREATE TABLE pg_catalog.pg_stat_progress_vacuum (
	index_vacuum_count INT,
	num_dead_tuples INT,
	relid OID,
	datid OID,
	datname NAME,
	heap_blks_vacuumed INT,
	max_dead_tuples INT,
	phase STRING,
	pid INT4,
	heap_blks_scanned INT,
	heap_blks_total INT
)`

// PgCatalogLargeobjectMetadataOidIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogLargeobjectMetadataOidIndex = `
CREATE TABLE pg_catalog.pg_largeobject_metadata_oid_index (
	oid OID
)`

// PgCatalogStatioAllSequences is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogStatioAllSequences = `
CREATE TABLE pg_catalog.pg_statio_all_sequences (
	relid OID,
	relname NAME,
	schemaname NAME,
	blks_hit INT,
	blks_read INT
)`

// PgCatalogStatArchiver is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogStatArchiver = `
CREATE TABLE pg_catalog.pg_stat_archiver (
	last_archived_time TIMESTAMPTZ,
	last_archived_wal STRING,
	last_failed_time TIMESTAMPTZ,
	last_failed_wal STRING,
	stats_reset TIMESTAMPTZ,
	archived_count INT,
	failed_count INT
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

// PgCatalogShdependDependerIndex is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogShdependDependerIndex = `
CREATE TABLE pg_catalog.pg_shdepend_depender_index (
	objsubid INT4,
	classid OID,
	dbid OID,
	objid OID
)`

// PgCatalogStatioSysIndexes is an empty table created by pg_catalog_test
// and is currently unimplemented.
const PgCatalogStatioSysIndexes = `
CREATE TABLE pg_catalog.pg_statio_sys_indexes (
	indexrelname NAME,
	relid OID,
	relname NAME,
	schemaname NAME,
	idx_blks_hit INT,
	idx_blks_read INT,
	indexrelid OID
)`
