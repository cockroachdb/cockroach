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
	amtype CHAR
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
	attstorage CHAR,
	attalign CHAR,
	attnotnull BOOL,
	atthasdef BOOL,
	attidentity CHAR, 
	attgenerated CHAR,
	attisdropped BOOL,
	attislocal BOOL,
	attinhcount INT4,
	attcollation OID,
	attacl STRING[],
	attoptions STRING[],
	attfdwoptions STRING[],
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
	castcontext CHAR,
	castmethod CHAR
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
	relpersistence CHAR,
	relistemp BOOL,
	relkind CHAR,
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
  collctype STRING
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
	defaclobjtype CHAR,
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
  deptype CHAR
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
	evtenabled CHAR,
	evttags TEXT[]
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
    indpred STRING
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
	provolatile CHAR,
	proparallel CHAR,
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
	proacl STRING[]
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
	deptype CHAR
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
	tgnewtable NAME
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
	typtype CHAR,
	typcategory CHAR,
	typispreferred BOOL,
	typisdefined BOOL,
	typdelim CHAR,
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
	typalign CHAR,
	typstorage CHAR,
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
	query TEXT
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
	aggkind  CHAR,
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
	aggminitval TEXT
)`
