// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package importccl

import (
	"context"
	"fmt"
	"io/ioutil"
	"math"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/gcjob"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/storage/cloudimpl"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

const (
	csvDelimiter    = "delimiter"
	csvComment      = "comment"
	csvNullIf       = "nullif"
	csvSkip         = "skip"
	csvRowLimit     = "row_limit"
	csvStrictQuotes = "strict_quotes"

	mysqlOutfileRowSep   = "rows_terminated_by"
	mysqlOutfileFieldSep = "fields_terminated_by"
	mysqlOutfileEnclose  = "fields_enclosed_by"
	mysqlOutfileEscape   = "fields_escaped_by"

	importOptionSSTSize          = "sstsize"
	importOptionDecompress       = "decompress"
	importOptionOversample       = "oversample"
	importOptionSkipFKs          = "skip_foreign_keys"
	importOptionDisableGlobMatch = "disable_glob_matching"
	importOptionSaveRejected     = "experimental_save_rejected"

	pgCopyDelimiter = "delimiter"
	pgCopyNull      = "nullif"

	optMaxRowSize = "max_row_size"

	// Turn on strict validation when importing avro records.
	avroStrict = "strict_validation"
	// Default input format is assumed to be OCF (object container file).
	// This default can be changed by specified either of these options.
	avroBinRecords  = "data_as_binary_records"
	avroJSONRecords = "data_as_json_records"
	// Record separator; default "\n"
	avroRecordsSeparatedBy = "records_terminated_by"
	// If we are importing avro records (binary or JSON), we must specify schema
	// as either an inline JSON schema, or an external schema URI.
	avroSchema    = "schema"
	avroSchemaURI = "schema_uri"

	// RunningStatusImportBundleParseSchema indicates to the user that a bundle format
	// schema is being parsed
	runningStatusImportBundleParseSchema jobs.RunningStatus = "parsing schema on Import Bundle"
)

var importOptionExpectValues = map[string]sql.KVStringOptValidate{
	csvDelimiter:    sql.KVStringOptRequireValue,
	csvComment:      sql.KVStringOptRequireValue,
	csvNullIf:       sql.KVStringOptRequireValue,
	csvSkip:         sql.KVStringOptRequireValue,
	csvRowLimit:     sql.KVStringOptRequireValue,
	csvStrictQuotes: sql.KVStringOptRequireNoValue,

	mysqlOutfileRowSep:   sql.KVStringOptRequireValue,
	mysqlOutfileFieldSep: sql.KVStringOptRequireValue,
	mysqlOutfileEnclose:  sql.KVStringOptRequireValue,
	mysqlOutfileEscape:   sql.KVStringOptRequireValue,

	importOptionSSTSize:      sql.KVStringOptRequireValue,
	importOptionDecompress:   sql.KVStringOptRequireValue,
	importOptionOversample:   sql.KVStringOptRequireValue,
	importOptionSaveRejected: sql.KVStringOptRequireNoValue,

	importOptionSkipFKs:          sql.KVStringOptRequireNoValue,
	importOptionDisableGlobMatch: sql.KVStringOptRequireNoValue,

	optMaxRowSize: sql.KVStringOptRequireValue,

	avroStrict:             sql.KVStringOptRequireNoValue,
	avroSchema:             sql.KVStringOptRequireValue,
	avroSchemaURI:          sql.KVStringOptRequireValue,
	avroRecordsSeparatedBy: sql.KVStringOptRequireValue,
	avroBinRecords:         sql.KVStringOptRequireNoValue,
	avroJSONRecords:        sql.KVStringOptRequireNoValue,
}

func makeStringSet(opts ...string) map[string]struct{} {
	res := make(map[string]struct{}, len(opts))
	for _, opt := range opts {
		res[opt] = struct{}{}
	}
	return res
}

// Options common to all formats.
var allowedCommonOptions = makeStringSet(
	importOptionSSTSize, importOptionDecompress, importOptionOversample,
	importOptionSaveRejected, importOptionDisableGlobMatch)

// Format specific allowed options.
var avroAllowedOptions = makeStringSet(
	avroStrict, avroBinRecords, avroJSONRecords,
	avroRecordsSeparatedBy, avroSchema, avroSchemaURI, optMaxRowSize, csvRowLimit,
)
var csvAllowedOptions = makeStringSet(
	csvDelimiter, csvComment, csvNullIf, csvSkip, csvStrictQuotes, csvRowLimit,
)
var mysqlOutAllowedOptions = makeStringSet(
	mysqlOutfileRowSep, mysqlOutfileFieldSep, mysqlOutfileEnclose,
	mysqlOutfileEscape, csvNullIf, csvSkip, csvRowLimit,
)
var mysqlDumpAllowedOptions = makeStringSet(importOptionSkipFKs)
var pgCopyAllowedOptions = makeStringSet(pgCopyDelimiter, pgCopyNull, optMaxRowSize)
var pgDumpAllowedOptions = makeStringSet(optMaxRowSize, importOptionSkipFKs)

// DROP is required because the target table needs to be take offline during
// IMPORT INTO.
var importIntoRequiredPrivileges = []privilege.Kind{privilege.INSERT, privilege.DROP}

// File formats supported for IMPORT INTO
var allowedIntoFormats = map[string]struct{}{
	"CSV":       {},
	"AVRO":      {},
	"DELIMITED": {},
	"PGCOPY":    {},
}

func validateFormatOptions(
	format string, specified map[string]string, formatAllowed map[string]struct{},
) error {
	for opt := range specified {
		if _, ok := formatAllowed[opt]; !ok {
			if _, ok = allowedCommonOptions[opt]; !ok {
				return errors.Errorf(
					"invalid option %q specified for %s import format", opt, format)
			}
		}
	}
	return nil
}

func importJobDescription(
	p sql.PlanHookState,
	orig *tree.Import,
	defs tree.TableDefs,
	files []string,
	opts map[string]string,
) (string, error) {
	stmt := *orig
	stmt.CreateFile = nil
	stmt.CreateDefs = defs
	stmt.Files = nil
	for _, file := range files {
		clean, err := cloudimpl.SanitizeExternalStorageURI(file, nil /* extraParams */)
		if err != nil {
			return "", err
		}
		stmt.Files = append(stmt.Files, tree.NewDString(clean))
	}
	stmt.Options = nil
	for k, v := range opts {
		opt := tree.KVOption{Key: tree.Name(k)}
		val := importOptionExpectValues[k] == sql.KVStringOptRequireValue
		val = val || (importOptionExpectValues[k] == sql.KVStringOptAny && len(v) > 0)
		if val {
			opt.Value = tree.NewDString(v)
		}
		stmt.Options = append(stmt.Options, opt)
	}
	sort.Slice(stmt.Options, func(i, j int) bool { return stmt.Options[i].Key < stmt.Options[j].Key })
	ann := p.ExtendedEvalContext().Annotations
	return tree.AsStringWithFQNames(&stmt, ann), nil
}

func ensureRequiredPrivileges(
	ctx context.Context,
	requiredPrivileges []privilege.Kind,
	p sql.PlanHookState,
	desc *tabledesc.Mutable,
) error {
	for _, priv := range requiredPrivileges {
		err := p.CheckPrivilege(ctx, desc, priv)
		if err != nil {
			return err
		}
	}

	return nil
}

// addToFileFormatTelemetry records the different stages of IMPORT on a per file
// format basis.
//
// The current states being counted are:
// attempted: Counted at the very beginning of the IMPORT.
// started: Counted just before the IMPORT job is started.
// failed: Counted when the IMPORT job is failed or canceled.
// succeeded: Counted when the IMPORT job completes successfully.
func addToFileFormatTelemetry(fileFormat, state string) {
	telemetry.Count(fmt.Sprintf("%s.%s.%s", "import", strings.ToLower(fileFormat), state))
}

// importPlanHook implements sql.PlanHookFn.
func importPlanHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, colinfo.ResultColumns, []sql.PlanNode, bool, error) {
	importStmt, ok := stmt.(*tree.Import)
	if !ok {
		return nil, nil, nil, false, nil
	}

	addToFileFormatTelemetry(importStmt.FileFormat, "attempted")

	if !p.ExecCfg().Settings.Version.IsActive(ctx, clusterversion.VersionPartitionedBackup) {
		return nil, nil, nil, false, errors.Errorf("IMPORT requires a cluster fully upgraded to version >= 19.2")
	}

	filesFn, err := p.TypeAsStringArray(ctx, importStmt.Files, "IMPORT")
	if err != nil {
		return nil, nil, nil, false, err
	}

	var createFileFn func() (string, error)
	if !importStmt.Bundle && !importStmt.Into && importStmt.CreateDefs == nil {
		createFileFn, err = p.TypeAsString(ctx, importStmt.CreateFile, "IMPORT")
		if err != nil {
			return nil, nil, nil, false, err
		}
	}

	optsFn, err := p.TypeAsStringOpts(ctx, importStmt.Options, importOptionExpectValues)
	if err != nil {
		return nil, nil, nil, false, err
	}

	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		// TODO(dan): Move this span into sql.
		ctx, span := tracing.ChildSpan(ctx, importStmt.StatementTag())
		defer span.Finish()

		walltime := p.ExecCfg().Clock.Now().WallTime

		if !p.ExtendedEvalContext().TxnImplicit {
			return errors.Errorf("IMPORT cannot be used inside a transaction")
		}

		opts, err := optsFn()
		if err != nil {
			return err
		}

		filenamePatterns, err := filesFn()
		if err != nil {
			return err
		}

		// Certain ExternalStorage URIs require super-user access. Check all the
		// URIs passed to the IMPORT command.
		for _, file := range filenamePatterns {
			hasExplicitAuth, uriScheme, err := cloudimpl.AccessIsWithExplicitAuth(file)
			if err != nil {
				return err
			}
			if !hasExplicitAuth {
				err := p.RequireAdminRole(ctx,
					fmt.Sprintf("IMPORT from the specified %s URI", uriScheme))
				if err != nil {
					return err
				}
			}
		}

		var files []string
		if _, ok := opts[importOptionDisableGlobMatch]; ok {
			files = filenamePatterns
		} else {
			for _, file := range filenamePatterns {
				if cloudimpl.URINeedsGlobExpansion(file) {
					s, err := p.ExecCfg().DistSQLSrv.ExternalStorageFromURI(ctx, file, p.User())
					if err != nil {
						return err
					}
					expandedFiles, err := s.ListFiles(ctx, "")
					if err != nil {
						return err
					}
					if len(expandedFiles) < 1 {
						return errors.Errorf(`no files matched uri provided: '%s'`, file)
					}
					files = append(files, expandedFiles...)
				} else {
					files = append(files, file)
				}
			}
		}

		// Typically the SQL grammar means it is only possible to specifying exactly
		// one pgdump/mysqldump URI, but glob-expansion could have changed that.
		if importStmt.Bundle && len(files) != 1 {
			return pgerror.New(pgcode.FeatureNotSupported, "SQL dump files must be imported individually")
		}

		table := importStmt.Table
		var parentID, parentSchemaID descpb.ID
		if table != nil {
			// TODO: As part of work for #34240, we should be operating on
			//  UnresolvedObjectNames here, rather than TableNames.
			// We have a target table, so it might specify a DB in its name.
			un := table.ToUnresolvedObjectName()
			found, prefix, resPrefixI, err := tree.ResolveTarget(ctx,
				un, p, p.SessionData().Database, p.SessionData().SearchPath)
			if err != nil {
				return pgerror.Wrap(err, pgcode.UndefinedTable,
					"resolving target import name")
			}
			table.ObjectNamePrefix = prefix
			if !found {
				// Check if database exists right now. It might not after the import is done,
				// but it's better to fail fast than wait until restore.
				return pgerror.Newf(pgcode.UndefinedObject,
					"database does not exist: %q", table)
			}
			resPrefix := resPrefixI.(*catalog.ResolvedObjectPrefix)
			dbDesc := resPrefix.Database
			schema := resPrefix.Schema
			// If this is a non-INTO import that will thus be making a new table, we
			// need the CREATE priv in the target DB.
			if !importStmt.Into {
				if err := p.CheckPrivilege(ctx, dbDesc, privilege.CREATE); err != nil {
					return err
				}
			}
			parentID = dbDesc.GetID()
			switch schema.Kind {
			case catalog.SchemaVirtual:
				return pgerror.Newf(pgcode.InvalidSchemaName,
					"cannot import into schema %q", table.SchemaName)
			case catalog.SchemaUserDefined, catalog.SchemaPublic, catalog.SchemaTemporary:
				parentSchemaID = schema.ID
			}
		} else {
			// No target table means we're importing whatever we find into the session
			// database, so it must exist.
			dbDesc, err := p.ResolveUncachedDatabaseByName(ctx, p.SessionData().Database, true /*required*/)
			if err != nil {
				return pgerror.Wrap(err, pgcode.UndefinedObject,
					"could not resolve current database")
			}
			// If this is a non-INTO import that will thus be making a new table, we
			// need the CREATE priv in the target DB.
			if !importStmt.Into {
				if err := p.CheckPrivilege(ctx, dbDesc, privilege.CREATE); err != nil {
					return err
				}
			}
			parentID = dbDesc.GetID()
			parentSchemaID = keys.PublicSchemaID
		}

		format := roachpb.IOFileFormat{}
		switch importStmt.FileFormat {
		case "CSV":
			if err = validateFormatOptions(importStmt.FileFormat, opts, csvAllowedOptions); err != nil {
				return err
			}
			format.Format = roachpb.IOFileFormat_CSV
			// Set the default CSV separator for the cases when it is not overwritten.
			format.Csv.Comma = ','
			if override, ok := opts[csvDelimiter]; ok {
				comma, err := util.GetSingleRune(override)
				if err != nil {
					return pgerror.Wrap(err, pgcode.Syntax, "invalid comma value")
				}
				format.Csv.Comma = comma
			}

			if override, ok := opts[csvComment]; ok {
				comment, err := util.GetSingleRune(override)
				if err != nil {
					return pgerror.Wrap(err, pgcode.Syntax, "invalid comment value")
				}
				format.Csv.Comment = comment
			}

			if override, ok := opts[csvNullIf]; ok {
				format.Csv.NullEncoding = &override
			}

			if override, ok := opts[csvSkip]; ok {
				skip, err := strconv.Atoi(override)
				if err != nil {
					return pgerror.Wrapf(err, pgcode.Syntax, "invalid %s value", csvSkip)
				}
				if skip < 0 {
					return pgerror.Newf(pgcode.Syntax, "%s must be >= 0", csvSkip)
				}
				format.Csv.Skip = uint32(skip)
			}
			if _, ok := opts[csvStrictQuotes]; ok {
				format.Csv.StrictQuotes = true
			}
			if _, ok := opts[importOptionSaveRejected]; ok {
				format.SaveRejected = true
			}
			if override, ok := opts[csvRowLimit]; ok {
				rowLimit, err := strconv.Atoi(override)
				if err != nil {
					return pgerror.Wrapf(err, pgcode.Syntax, "invalid numeric %s value", csvRowLimit)
				}
				if rowLimit <= 0 {
					return pgerror.Newf(pgcode.Syntax, "%s must be > 0", csvRowLimit)
				}
				format.Csv.RowLimit = int64(rowLimit)
			}
		case "DELIMITED":
			if err = validateFormatOptions(importStmt.FileFormat, opts, mysqlOutAllowedOptions); err != nil {
				return err
			}
			format.Format = roachpb.IOFileFormat_MysqlOutfile
			format.MysqlOut = roachpb.MySQLOutfileOptions{
				RowSeparator:   '\n',
				FieldSeparator: '\t',
			}
			if override, ok := opts[mysqlOutfileRowSep]; ok {
				c, err := util.GetSingleRune(override)
				if err != nil {
					return pgerror.Wrapf(err, pgcode.Syntax,
						"invalid %q value", mysqlOutfileRowSep)
				}
				format.MysqlOut.RowSeparator = c
			}

			if override, ok := opts[mysqlOutfileFieldSep]; ok {
				c, err := util.GetSingleRune(override)
				if err != nil {
					return pgerror.Wrapf(err, pgcode.Syntax, "invalid %q value", mysqlOutfileFieldSep)
				}
				format.MysqlOut.FieldSeparator = c
			}

			if override, ok := opts[mysqlOutfileEnclose]; ok {
				c, err := util.GetSingleRune(override)
				if err != nil {
					return pgerror.Wrapf(err, pgcode.Syntax, "invalid %q value", mysqlOutfileRowSep)
				}
				format.MysqlOut.Enclose = roachpb.MySQLOutfileOptions_Always
				format.MysqlOut.Encloser = c
			}

			if override, ok := opts[mysqlOutfileEscape]; ok {
				c, err := util.GetSingleRune(override)
				if err != nil {
					return pgerror.Wrapf(err, pgcode.Syntax, "invalid %q value", mysqlOutfileRowSep)
				}
				format.MysqlOut.HasEscape = true
				format.MysqlOut.Escape = c
			}
			if override, ok := opts[csvSkip]; ok {
				skip, err := strconv.Atoi(override)
				if err != nil {
					return pgerror.Wrapf(err, pgcode.Syntax, "invalid %s value", csvSkip)
				}
				if skip < 0 {
					return pgerror.Newf(pgcode.Syntax, "%s must be >= 0", csvSkip)
				}
				format.MysqlOut.Skip = uint32(skip)
			}
			if override, ok := opts[csvNullIf]; ok {
				format.MysqlOut.NullEncoding = &override
			}
			if _, ok := opts[importOptionSaveRejected]; ok {
				format.SaveRejected = true
			}
			if override, ok := opts[csvRowLimit]; ok {
				rowLimit, err := strconv.Atoi(override)
				if err != nil {
					return pgerror.Wrapf(err, pgcode.Syntax, "invalid numeric %s value", csvRowLimit)
				}
				if rowLimit <= 0 {
					return pgerror.Newf(pgcode.Syntax, "%s must be > 0", csvRowLimit)
				}
				format.MysqlOut.RowLimit = int64(rowLimit)
			}
		case "MYSQLDUMP":
			if err = validateFormatOptions(importStmt.FileFormat, opts, mysqlDumpAllowedOptions); err != nil {
				return err
			}
			format.Format = roachpb.IOFileFormat_Mysqldump
		case "PGCOPY":
			if err = validateFormatOptions(importStmt.FileFormat, opts, pgCopyAllowedOptions); err != nil {
				return err
			}
			format.Format = roachpb.IOFileFormat_PgCopy
			format.PgCopy = roachpb.PgCopyOptions{
				Delimiter: '\t',
				Null:      `\N`,
			}
			if override, ok := opts[pgCopyDelimiter]; ok {
				c, err := util.GetSingleRune(override)
				if err != nil {
					return pgerror.Wrapf(err, pgcode.Syntax, "invalid %q value", pgCopyDelimiter)
				}
				format.PgCopy.Delimiter = c
			}
			if override, ok := opts[pgCopyNull]; ok {
				format.PgCopy.Null = override
			}
			maxRowSize := int32(defaultScanBuffer)
			if override, ok := opts[optMaxRowSize]; ok {
				sz, err := humanizeutil.ParseBytes(override)
				if err != nil {
					return err
				}
				if sz < 1 || sz > math.MaxInt32 {
					return errors.Errorf("%d out of range: %d", maxRowSize, sz)
				}
				maxRowSize = int32(sz)
			}
			format.PgCopy.MaxRowSize = maxRowSize
		case "PGDUMP":
			if err = validateFormatOptions(importStmt.FileFormat, opts, pgDumpAllowedOptions); err != nil {
				return err
			}
			format.Format = roachpb.IOFileFormat_PgDump
			maxRowSize := int32(defaultScanBuffer)
			if override, ok := opts[optMaxRowSize]; ok {
				sz, err := humanizeutil.ParseBytes(override)
				if err != nil {
					return err
				}
				if sz < 1 || sz > math.MaxInt32 {
					return errors.Errorf("%d out of range: %d", maxRowSize, sz)
				}
				maxRowSize = int32(sz)
			}
			format.PgDump.MaxRowSize = maxRowSize
		case "AVRO":
			if err = validateFormatOptions(importStmt.FileFormat, opts, avroAllowedOptions); err != nil {
				return err
			}
			err := parseAvroOptions(ctx, opts, p, &format)
			if err != nil {
				return err
			}
		default:
			return unimplemented.Newf("import.format", "unsupported import format: %q", importStmt.FileFormat)
		}

		// sstSize, if 0, will be set to an appropriate default by the specific
		// implementation (local or distributed) since each has different optimal
		// settings.
		var sstSize int64
		if override, ok := opts[importOptionSSTSize]; ok {
			sz, err := humanizeutil.ParseBytes(override)
			if err != nil {
				return err
			}
			sstSize = sz
		}
		var oversample int64
		if override, ok := opts[importOptionOversample]; ok {
			os, err := strconv.ParseInt(override, 10, 64)
			if err != nil {
				return err
			}
			oversample = os
		}

		var skipFKs bool
		if _, ok := opts[importOptionSkipFKs]; ok {
			skipFKs = true
		}

		if override, ok := opts[importOptionDecompress]; ok {
			found := false
			for name, value := range roachpb.IOFileFormat_Compression_value {
				if strings.EqualFold(name, override) {
					format.Compression = roachpb.IOFileFormat_Compression(value)
					found = true
					break
				}
			}
			if !found {
				return unimplemented.Newf("import.compression", "unsupported compression value: %q", override)
			}
		}

		var tableDetails []jobspb.ImportDetails_Table
		var tableDescs []*tabledesc.Mutable // parallel with tableDetails
		jobDesc, err := importJobDescription(p, importStmt, nil, filenamePatterns, opts)
		if err != nil {
			return err
		}

		if importStmt.Into {
			// TODO(dt): this is a prototype for incremental import but there are many
			// TODOs remaining before it is ready to graduate to prime-time. Some of
			// them are captured in specific TODOs below, but some of the big, scary
			// things to do are:
			// - review planner vs txn use very carefully. We should try to get to a
			//   single txn used to plan the job and create it. Using the planner's
			//   txn today is very wrong since it will not commit until after the job
			//   has run, so starting a job based on reads it returned is very wrong.
			// - audit every place that we resolve/lease/read table descs to be sure
			//   that the IMPORTING state is handled correctly. SQL lease acquisition
			//   is probably the easy one here since it has single read path -- the
			//   things that read directly like the queues or background jobs are the
			//   ones we'll need to really carefully look though.
			// - Look at if/how cleanup/rollback works. Reconsider the cpu from the
			//   desc version (perhaps we should be re-reading instead?).
			// - Write _a lot_ of tests.
			if _, ok := allowedIntoFormats[importStmt.FileFormat]; !ok {
				return errors.Newf(
					"%s file format is currently unsupported by IMPORT INTO",
					importStmt.FileFormat)
			}
			found, err := p.ResolveMutableTableDescriptor(ctx, table, true, tree.ResolveRequireTableDesc)
			if err != nil {
				return err
			}

			err = ensureRequiredPrivileges(ctx, importIntoRequiredPrivileges, p, found)
			if err != nil {
				return err
			}

			// IMPORT INTO does not currently support interleaved tables.
			if found.IsInterleaved() {
				// TODO(miretskiy): Handle import into when tables are interleaved.
				return pgerror.New(pgcode.FeatureNotSupported, "Cannot use IMPORT INTO with interleaved tables")
			}

			// Validate target columns.
			var intoCols []string
			var isTargetCol = make(map[string]bool)
			for _, name := range importStmt.IntoCols {
				active, err := found.FindActiveColumnsByNames(tree.NameList{name})
				if err != nil {
					return errors.Wrap(err, "verifying target columns")
				}

				isTargetCol[active[0].Name] = true
				intoCols = append(intoCols, active[0].Name)
			}

			// Ensure that non-target columns that don't have default
			// expressions are nullable.
			if len(isTargetCol) != 0 {
				for _, col := range found.VisibleColumns() {
					if !(isTargetCol[col.Name] || col.Nullable || col.HasDefault() || col.IsComputed()) {
						return errors.Newf(
							"all non-target columns in IMPORT INTO must be nullable "+
								"or have default expressions, or have computed expressions"+
								" but violated by column %q",
							col.Name,
						)
					}
					if isTargetCol[col.Name] && col.IsComputed() {
						return schemaexpr.CannotWriteToComputedColError(col.Name)
					}
				}
			}
			tableDescs = []*tabledesc.Mutable{found}
			tableDetails = []jobspb.ImportDetails_Table{{Desc: &found.TableDescriptor, IsNew: false, TargetCols: intoCols}}
		} else {
			seqVals := make(map[descpb.ID]int64)

			if importStmt.Bundle {
				// If we target a single table, populate details with one entry of tableName.
				if table != nil {
					tableDetails = make([]jobspb.ImportDetails_Table, 1)
					tableDetails[0] = jobspb.ImportDetails_Table{
						Name:  table.ObjectName.String(),
						IsNew: true,
					}
				}
			} else {
				if table == nil {
					return errors.Errorf("non-bundle format %q should always have a table name", importStmt.FileFormat)
				}
				var create *tree.CreateTable
				if importStmt.CreateDefs != nil {
					create = &tree.CreateTable{
						Table: *importStmt.Table,
						Defs:  importStmt.CreateDefs,
					}
				} else {
					filename, err := createFileFn()
					if err != nil {
						return err
					}
					create, err = readCreateTableFromStore(ctx, filename,
						p.ExecCfg().DistSQLSrv.ExternalStorageFromURI, p.User())
					if err != nil {
						return err
					}

					if table.ObjectName != create.Table.ObjectName {
						return errors.Errorf(
							"importing table %s, but file specifies a schema for table %s",
							table.ObjectName, create.Table.ObjectName,
						)
					}
				}
				tbl, err := MakeSimpleTableDescriptor(
					ctx, p.SemaCtx(), p.ExecCfg().Settings, create, parentID, parentSchemaID, defaultCSVTableID, NoFKs, walltime)
				if err != nil {
					return err
				}
				descStr, err := importJobDescription(p, importStmt, create.Defs, filenamePatterns, opts)
				if err != nil {
					return err
				}
				jobDesc = descStr

				tableDescs = []*tabledesc.Mutable{tbl}
				for _, tbl := range tableDescs {
					// For reasons relating to #37691, we disallow user defined types in
					// the standard IMPORT case.
					for _, col := range tbl.Columns {
						if col.Type.UserDefined() {
							return errors.Newf("IMPORT cannot be used with user defined types; use IMPORT INTO instead")
						}
					}
				}

				tableDetails = make([]jobspb.ImportDetails_Table, len(tableDescs))
				for i := range tableDescs {
					tableDetails[i] = jobspb.ImportDetails_Table{
						Desc:   tableDescs[i].TableDesc(),
						SeqVal: seqVals[tableDescs[i].ID],
						IsNew:  true,
					}
				}
			}

			// Due to how we generate and rewrite descriptor ID's for import, we run
			// into problems when using user defined schemas.
			if parentSchemaID != keys.PublicSchemaID {
				err := errors.New("cannot use IMPORT with a user defined schema")
				hint := errors.WithHint(err, "create the table with CREATE TABLE and use IMPORT INTO instead")
				return hint
			}
		}

		telemetry.CountBucketed("import.files", int64(len(files)))

		// Record telemetry for userfile being used as the import target.
		for _, file := range files {
			uri, err := url.Parse(file)
			// This should never be true as we have parsed these file names in an
			// earlier step of import.
			if err != nil {
				log.Warningf(ctx, "failed to collect file specific import telemetry for %s", uri)
				continue
			}

			if uri.Scheme == "userfile" {
				telemetry.Count("import.storage.userfile")
				break
			}
		}

		// Here we create the job and protected timestamp records in a side
		// transaction and then kick off the job. This is awful. Rather we should be
		// disallowing this statement in an explicit transaction and then we should
		// create the job in the user's transaction here and then in a post-commit
		// hook we should kick of the StartableJob which we attached to the
		// connExecutor somehow.

		importDetails := jobspb.ImportDetails{
			URIs:              files,
			Format:            format,
			ParentID:          parentID,
			Tables:            tableDetails,
			SSTSize:           sstSize,
			Oversample:        oversample,
			SkipFKs:           skipFKs,
			ParseBundleSchema: importStmt.Bundle,
		}

		// Prepare the protected timestamp record.
		var spansToProtect []roachpb.Span
		codec := p.(sql.PlanHookState).ExecCfg().Codec
		for i := range tableDetails {
			if td := &tableDetails[i]; !td.IsNew {
				spansToProtect = append(spansToProtect, tableDescs[i].TableSpan(codec))
			}
		}
		if len(spansToProtect) > 0 {
			protectedtsID := uuid.MakeV4()
			importDetails.ProtectedTimestampRecord = &protectedtsID
		}
		jr := jobs.Record{
			Description: jobDesc,
			Username:    p.User(),
			Details:     importDetails,
			Progress:    jobspb.ImportProgress{},
		}

		var sj *jobs.StartableJob
		if err := p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
			sj, err = p.ExecCfg().JobRegistry.CreateStartableJobWithTxn(ctx, jr, txn, resultsCh)
			if err != nil {
				return err
			}

			if len(spansToProtect) > 0 {
				// NB: We protect the timestamp preceding the import statement timestamp
				// because that's the timestamp to which we want to revert.
				tsToProtect := hlc.Timestamp{WallTime: walltime}.Prev()
				rec := jobsprotectedts.MakeRecord(*importDetails.ProtectedTimestampRecord,
					*sj.ID(), tsToProtect, spansToProtect)
				return p.ExecCfg().ProtectedTimestampProvider.Protect(ctx, txn, rec)
			}
			return nil
		}); err != nil {
			if sj != nil {
				if cleanupErr := sj.CleanupOnRollback(ctx); cleanupErr != nil {
					log.Warningf(ctx, "failed to cleanup StartableJob: %v", cleanupErr)
				}
			}
			return err
		}

		err = sj.Run(ctx)
		if err != nil {
			return err
		}
		addToFileFormatTelemetry(format.Format.String(), "started")

		return nil
	}
	return fn, utilccl.BulkJobExecutionResultHeader, nil, false, nil
}

func parseAvroOptions(
	ctx context.Context, opts map[string]string, p sql.PlanHookState, format *roachpb.IOFileFormat,
) error {
	format.Format = roachpb.IOFileFormat_Avro
	// Default input format is OCF.
	format.Avro.Format = roachpb.AvroOptions_OCF
	_, format.Avro.StrictMode = opts[avroStrict]

	_, haveBinRecs := opts[avroBinRecords]
	_, haveJSONRecs := opts[avroJSONRecords]

	if haveBinRecs && haveJSONRecs {
		return errors.Errorf("only one of the %s or %s options can be set", avroBinRecords, avroJSONRecords)
	}

	if override, ok := opts[csvRowLimit]; ok {
		rowLimit, err := strconv.Atoi(override)
		if err != nil {
			return pgerror.Wrapf(err, pgcode.Syntax, "invalid numeric %s value", csvRowLimit)
		}
		if rowLimit <= 0 {
			return pgerror.Newf(pgcode.Syntax, "%s must be > 0", csvRowLimit)
		}
		format.Avro.RowLimit = int64(rowLimit)
	}

	if haveBinRecs || haveJSONRecs {
		// Input is a "records" format.
		if haveBinRecs {
			format.Avro.Format = roachpb.AvroOptions_BIN_RECORDS
		} else {
			format.Avro.Format = roachpb.AvroOptions_JSON_RECORDS
		}

		// Set record separator.
		format.Avro.RecordSeparator = '\n'
		if override, ok := opts[avroRecordsSeparatedBy]; ok {
			c, err := util.GetSingleRune(override)
			if err != nil {
				return pgerror.Wrapf(err, pgcode.Syntax,
					"invalid %q value", avroRecordsSeparatedBy)
			}
			format.Avro.RecordSeparator = c
		}

		// See if inline schema is specified.
		format.Avro.SchemaJSON = opts[avroSchema]

		if len(format.Avro.SchemaJSON) == 0 {
			// Inline schema not set; We must have external schema.
			uri, ok := opts[avroSchemaURI]
			if !ok {
				return errors.Errorf(
					"either %s or %s option must be set when importing avro record files", avroSchema, avroSchemaURI)
			}

			store, err := p.ExecCfg().DistSQLSrv.ExternalStorageFromURI(ctx, uri, p.User())
			if err != nil {
				return err
			}
			defer store.Close()

			raw, err := store.ReadFile(ctx, "")
			if err != nil {
				return err
			}
			defer raw.Close()
			schemaBytes, err := ioutil.ReadAll(raw)
			if err != nil {
				return err
			}
			format.Avro.SchemaJSON = string(schemaBytes)
		}

		if override, ok := opts[optMaxRowSize]; ok {
			sz, err := humanizeutil.ParseBytes(override)
			if err != nil {
				return err
			}
			if sz < 1 || sz > math.MaxInt32 {
				return errors.Errorf("%s out of range: %d", override, sz)
			}
			format.Avro.MaxRecordSize = int32(sz)
		}
	}
	return nil
}

type importResumer struct {
	job      *jobs.Job
	settings *cluster.Settings
	res      backupccl.RowCount

	testingKnobs struct {
		afterImport               func(summary backupccl.RowCount) error
		alwaysFlushJobProgress    bool
		ignoreProtectedTimestamps bool
	}
}

// Prepares descriptors for newly created tables being imported into.
func prepareNewTableDescsForIngestion(
	ctx context.Context,
	txn *kv.Txn,
	descsCol *descs.Collection,
	p sql.JobExecContext,
	importTables []jobspb.ImportDetails_Table,
	parentID descpb.ID,
) ([]*descpb.TableDescriptor, error) {
	newMutableTableDescriptors := make([]*tabledesc.Mutable, len(importTables))
	for i := range importTables {
		newMutableTableDescriptors[i] = tabledesc.NewCreatedMutable(*importTables[i].Desc)
	}

	// Verification steps have passed, generate a new table ID if we're
	// restoring. We do this last because we want to avoid calling
	// GenerateUniqueDescID if there's any kind of error above.
	// Reserving a table ID now means we can avoid the rekey work during restore.
	tableRewrites := make(backupccl.DescRewriteMap)
	seqVals := make(map[descpb.ID]int64, len(importTables))
	for _, tableDesc := range importTables {
		id, err := catalogkv.GenerateUniqueDescID(ctx, p.ExecCfg().DB, p.ExecCfg().Codec)
		if err != nil {
			return nil, err
		}
		tableRewrites[tableDesc.Desc.ID] = &jobspb.RestoreDetails_DescriptorRewrite{
			ID:       id,
			ParentID: parentID,
		}
		seqVals[id] = tableDesc.SeqVal
	}
	// TODO(ajwerner): Remove this in 21.1.
	canResetModTime := p.ExecCfg().Settings.Version.IsActive(
		ctx, clusterversion.VersionLeasedDatabaseDescriptors)
	if err := backupccl.RewriteTableDescs(
		newMutableTableDescriptors, tableRewrites, "", canResetModTime,
	); err != nil {
		return nil, err
	}

	// After all of the ID's have been remapped, ensure that there aren't any name
	// collisions with any importing tables.
	for i := range newMutableTableDescriptors {
		tbl := newMutableTableDescriptors[i]
		if err := backupccl.CheckObjectExists(
			ctx,
			txn,
			p.ExecCfg().Codec,
			tbl.GetParentID(),
			tbl.GetParentSchemaID(),
			tbl.GetName(),
		); err != nil {
			return nil, err
		}
	}

	// tableDescs contains the same slice as newMutableTableDescriptors but
	// as tabledesc.TableDescriptor.
	tableDescs := make([]catalog.TableDescriptor, len(newMutableTableDescriptors))
	for i := range tableDescs {
		newMutableTableDescriptors[i].State = descpb.DescriptorState_OFFLINE
		newMutableTableDescriptors[i].OfflineReason = "importing"
		tableDescs[i] = newMutableTableDescriptors[i]
	}

	var seqValKVs []roachpb.KeyValue
	for _, desc := range newMutableTableDescriptors {
		if v, ok := seqVals[desc.GetID()]; ok && v != 0 {
			key, val, err := sql.MakeSequenceKeyVal(p.ExecCfg().Codec, desc, v, false)
			if err != nil {
				return nil, err
			}
			kv := roachpb.KeyValue{Key: key}
			kv.Value.SetInt(val)
			seqValKVs = append(seqValKVs, kv)
		}
	}

	// Write the new TableDescriptors and flip the namespace entries over to
	// them. After this call, any queries on a table will be served by the newly
	// imported data.
	if err := backupccl.WriteDescriptors(ctx, txn, p.User(), descsCol,
		nil /* databases */, nil, /* schemas */
		tableDescs, nil, tree.RequestedDescriptors,
		p.ExecCfg().Settings, seqValKVs); err != nil {
		return nil, errors.Wrapf(err, "creating importTables")
	}

	newPreparedTableDescs := make([]*descpb.TableDescriptor, len(newMutableTableDescriptors))
	for i := range newMutableTableDescriptors {
		newPreparedTableDescs[i] = newMutableTableDescriptors[i].TableDesc()
	}

	return newPreparedTableDescs, nil
}

// Prepares descriptors for existing tables being imported into.
func prepareExistingTableDescForIngestion(
	ctx context.Context, txn *kv.Txn, descsCol *descs.Collection, desc *descpb.TableDescriptor,
) (*descpb.TableDescriptor, error) {
	if len(desc.Mutations) > 0 {
		return nil, errors.Errorf("cannot IMPORT INTO a table with schema changes in progress -- try again later (pending mutation %s)", desc.Mutations[0].String())
	}

	// Note that desc is just used to verify that the version matches.
	importing, err := descsCol.GetMutableTableVersionByID(ctx, desc.ID, txn)
	if err != nil {
		return nil, err
	}
	// Ensure that the version of the table has not been modified since this
	// job was created.
	if got, exp := importing.Version, desc.Version; got != exp {
		return nil, errors.Errorf("another operation is currently operating on the table")
	}

	// Take the table offline for import.
	// TODO(dt): audit everywhere we get table descs (leases or otherwise) to
	// ensure that filtering by state handles IMPORTING correctly.
	importing.State = descpb.DescriptorState_OFFLINE
	importing.OfflineReason = "importing"

	// TODO(dt): de-validate all the FKs.
	if err := descsCol.WriteDesc(
		ctx, false /* kvTrace */, importing, txn,
	); err != nil {
		return nil, err
	}

	return importing.TableDesc(), nil
}

// prepareTableDescsForIngestion prepares table descriptors for the ingestion
// step of import. The descriptors are in an IMPORTING state (offline) on
// successful completion of this method.
func (r *importResumer) prepareTableDescsForIngestion(
	ctx context.Context, p sql.JobExecContext, details jobspb.ImportDetails,
) error {
	err := descs.Txn(ctx, p.ExecCfg().Settings, p.ExecCfg().LeaseManager,
		p.ExecCfg().InternalExecutor, p.ExecCfg().DB, func(
			ctx context.Context, txn *kv.Txn, descsCol *descs.Collection,
		) error {

			importDetails := details
			importDetails.Tables = make([]jobspb.ImportDetails_Table, len(details.Tables))

			newTablenameToIdx := make(map[string]int, len(importDetails.Tables))
			var hasExistingTables bool
			var err error
			var newTableDescs []jobspb.ImportDetails_Table
			var desc *descpb.TableDescriptor
			for i, table := range details.Tables {
				if !table.IsNew {
					desc, err = prepareExistingTableDescForIngestion(ctx, txn, descsCol, table.Desc)
					if err != nil {
						return err
					}
					importDetails.Tables[i] = jobspb.ImportDetails_Table{Desc: desc, Name: table.Name,
						SeqVal:     table.SeqVal,
						IsNew:      table.IsNew,
						TargetCols: table.TargetCols}

					hasExistingTables = true
				} else {
					newTablenameToIdx[table.Desc.Name] = i
					// Make a deep copy of the table descriptor so that rewrites do not
					// partially clobber the descriptor stored in details.
					newTableDescs = append(newTableDescs,
						*protoutil.Clone(&table).(*jobspb.ImportDetails_Table))
				}
			}

			// Prepare the table descriptors for newly created tables being imported
			// into.
			//
			// TODO(adityamaru): This is still unnecessarily complicated. If we can get
			// the new table desc preparation to work on a per desc basis, rather than
			// requiring all the newly created descriptors, then this can look like the
			// call to prepareExistingTableDescForIngestion. Currently, FK references
			// misbehave when I tried to write the desc one at a time.
			if len(newTableDescs) != 0 {
				res, err := prepareNewTableDescsForIngestion(
					ctx, txn, descsCol, p, newTableDescs, importDetails.ParentID)
				if err != nil {
					return err
				}

				for _, desc := range res {
					i := newTablenameToIdx[desc.Name]
					table := details.Tables[i]
					importDetails.Tables[i] = jobspb.ImportDetails_Table{Desc: desc,
						Name:       table.Name,
						SeqVal:     table.SeqVal,
						IsNew:      table.IsNew,
						TargetCols: table.TargetCols}
				}
			}

			importDetails.PrepareComplete = true

			// If we do not have pending schema changes on existing descriptors we can
			// choose our Walltime (to IMPORT from) immediately. Otherwise, we have to
			// wait for all nodes to see the same descriptor version before doing so.
			if !hasExistingTables {
				importDetails.Walltime = p.ExecCfg().Clock.Now().WallTime
			} else {
				importDetails.Walltime = 0
			}

			// Update the job once all descs have been prepared for ingestion.
			err = r.job.WithTxn(txn).SetDetails(ctx, importDetails)

			return err
		})
	return err
}

// parseAndCreateBundleTableDescs parses and creates the table
// descriptors for bundle formats.
func parseAndCreateBundleTableDescs(
	ctx context.Context,
	p sql.JobExecContext,
	details jobspb.ImportDetails,
	seqVals map[descpb.ID]int64,
	skipFKs bool,
	parentID descpb.ID,
	files []string,
	format roachpb.IOFileFormat,
	walltime int64,
	owner security.SQLUsername,
) ([]*tabledesc.Mutable, error) {

	var tableDescs []*tabledesc.Mutable
	var tableName string

	// A single table entry in the import job details when importing a bundle format
	// indicates that we are performing a single table import.
	// This info is populated during the planning phase.
	if len(details.Tables) > 0 {
		tableName = details.Tables[0].Name
	}

	store, err := p.ExecCfg().DistSQLSrv.ExternalStorageFromURI(ctx, files[0], p.User())
	if err != nil {
		return tableDescs, err
	}
	defer store.Close()

	raw, err := store.ReadFile(ctx, "")
	if err != nil {
		return tableDescs, err
	}
	defer raw.Close()
	reader, err := decompressingReader(raw, files[0], format.Compression)
	if err != nil {
		return tableDescs, err
	}
	defer reader.Close()

	fks := fkHandler{skip: skipFKs, allowed: true, resolver: make(fkResolver)}
	switch format.Format {
	case roachpb.IOFileFormat_Mysqldump:
		evalCtx := &p.ExtendedEvalContext().EvalContext
		tableDescs, err = readMysqlCreateTable(ctx, reader, evalCtx, p, defaultCSVTableID, parentID, tableName, fks, seqVals, owner)
	case roachpb.IOFileFormat_PgDump:
		evalCtx := &p.ExtendedEvalContext().EvalContext
		tableDescs, err = readPostgresCreateTable(ctx, reader, evalCtx, p, tableName, parentID, walltime, fks, int(format.PgDump.MaxRowSize), owner)
	default:
		return tableDescs, errors.Errorf("non-bundle format %q does not support reading schemas", format.Format.String())
	}

	if err != nil {
		return tableDescs, err
	}

	if tableDescs == nil && len(details.Tables) > 0 {
		return tableDescs, errors.Errorf("table definition not found for %q", tableName)
	}

	return tableDescs, err
}

func (r *importResumer) parseBundleSchemaIfNeeded(ctx context.Context, phs interface{}) error {
	p := phs.(sql.JobExecContext)
	seqVals := make(map[descpb.ID]int64)
	details := r.job.Details().(jobspb.ImportDetails)
	skipFKs := details.SkipFKs
	parentID := details.ParentID
	files := details.URIs
	format := details.Format

	owner := r.job.Payload().UsernameProto.Decode()

	if details.ParseBundleSchema {
		if err := r.job.RunningStatus(ctx, func(_ context.Context, _ jobspb.Details) (jobs.RunningStatus, error) {
			return runningStatusImportBundleParseSchema, nil
		}); err != nil {
			return errors.Wrapf(err, "failed to update running status of job %d", errors.Safe(*r.job.ID()))
		}

		var tableDescs []*tabledesc.Mutable
		var err error
		walltime := p.ExecCfg().Clock.Now().WallTime

		if tableDescs, err = parseAndCreateBundleTableDescs(
			ctx, p, details, seqVals, skipFKs, parentID, files, format, walltime, owner); err != nil {
			return err
		}

		tableDetails := make([]jobspb.ImportDetails_Table, len(tableDescs))
		for i := range tableDescs {
			tableDetails[i] = jobspb.ImportDetails_Table{
				Desc:   tableDescs[i].TableDesc(),
				SeqVal: seqVals[tableDescs[i].ID],
				IsNew:  true,
			}
		}
		details.Tables = tableDetails

		for _, tbl := range tableDescs {
			// For reasons relating to #37691, we disallow user defined types in
			// the standard IMPORT case.
			for _, col := range tbl.Columns {
				if col.Type.UserDefined() {
					return errors.Newf("IMPORT cannot be used with user defined types; use IMPORT INTO instead")
				}
			}
		}
		// Prevent job from redoing schema parsing and table desc creation
		// on subsequent resumptions.
		details.ParseBundleSchema = false
		if err := r.job.WithTxn(nil).SetDetails(ctx, details); err != nil {
			return err
		}
	}
	return nil
}

// Resume is part of the jobs.Resumer interface.
func (r *importResumer) Resume(
	ctx context.Context, execCtx interface{}, resultsCh chan<- tree.Datums,
) error {
	p := execCtx.(sql.JobExecContext)
	if err := r.parseBundleSchemaIfNeeded(ctx, p); err != nil {
		return err
	}

	details := r.job.Details().(jobspb.ImportDetails)
	files := details.URIs
	format := details.Format
	ptsID := details.ProtectedTimestampRecord
	if ptsID != nil && !r.testingKnobs.ignoreProtectedTimestamps {
		if err := p.ExecCfg().ProtectedTimestampProvider.Verify(ctx, *ptsID); err != nil {
			if errors.Is(err, protectedts.ErrNotExists) {
				// No reason to return an error which might cause problems if it doesn't
				// seem to exist.
				log.Warningf(ctx, "failed to release protected which seems not to exist: %v", err)
			} else {
				return err
			}
		}
	}

	tables := make(map[string]*execinfrapb.ReadImportDataSpec_ImportTable, len(details.Tables))
	if details.Tables != nil {
		// Skip prepare stage on job resumption, if it has already been completed.
		if !details.PrepareComplete {
			if err := r.prepareTableDescsForIngestion(ctx, p, details); err != nil {
				return err
			}

			// Re-initialize details after prepare step.
			details = r.job.Details().(jobspb.ImportDetails)
		}

		for _, i := range details.Tables {
			if i.Name != "" {
				tables[i.Name] = &execinfrapb.ReadImportDataSpec_ImportTable{Desc: i.Desc, TargetCols: i.TargetCols}
			} else if i.Desc != nil {
				tables[i.Desc.Name] = &execinfrapb.ReadImportDataSpec_ImportTable{Desc: i.Desc, TargetCols: i.TargetCols}
			} else {
				return errors.Errorf("invalid table specification")
			}
		}
	}

	// In the case of importing into existing tables we must wait for all nodes
	// to see the same version of the updated table descriptor, after which we
	// shall chose a ts to import from.
	if details.Walltime == 0 {
		// TODO(dt): update job status to mention waiting for tables to go offline.
		for _, i := range details.Tables {
			if !i.IsNew {
				if _, err := p.ExecCfg().LeaseManager.WaitForOneVersion(ctx, i.Desc.ID, retry.Options{}); err != nil {
					return err
				}
			}
		}

		// Now that we know all the tables are offline, pick a walltime at which we
		// will write.
		details.Walltime = p.ExecCfg().Clock.Now().WallTime

		// Check if the tables being imported into are starting empty, in which
		// case we can cheaply clear-range instead of revert-range to cleanup.
		for i := range details.Tables {
			if !details.Tables[i].IsNew {
				tblSpan := tabledesc.NewImmutable(*details.Tables[i].Desc).TableSpan(keys.TODOSQLCodec)
				res, err := p.ExecCfg().DB.Scan(ctx, tblSpan.Key, tblSpan.EndKey, 1 /* maxRows */)
				if err != nil {
					return errors.Wrap(err, "checking if existing table is empty")
				}
				details.Tables[i].WasEmpty = len(res) == 0
			}
		}

		if err := r.job.WithTxn(nil).SetDetails(ctx, details); err != nil {
			return err
		}
	}

	res, err := sql.DistIngest(ctx, p, r.job, tables, files, format, details.Walltime, r.testingKnobs.alwaysFlushJobProgress)
	if err != nil {
		return err
	}
	pkIDs := make(map[uint64]struct{}, len(details.Tables))
	for _, t := range details.Tables {
		pkIDs[roachpb.BulkOpSummaryID(uint64(t.Desc.ID), uint64(t.Desc.PrimaryIndex.ID))] = struct{}{}
	}
	r.res.DataSize = res.DataSize
	for id, count := range res.EntryCounts {
		if _, ok := pkIDs[id]; ok {
			r.res.Rows += count
		} else {
			r.res.IndexEntries += count
		}
	}
	if r.testingKnobs.afterImport != nil {
		if err := r.testingKnobs.afterImport(r.res); err != nil {
			return err
		}
	}

	if err := r.publishTables(ctx, p.ExecCfg()); err != nil {
		return err
	}
	// TODO(ajwerner): Should this actually return the error? At this point we've
	// successfully finished the import but failed to drop the protected
	// timestamp. The reconciliation loop ought to pick it up.
	if ptsID != nil && !r.testingKnobs.ignoreProtectedTimestamps {
		if err := p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			return r.releaseProtectedTimestamp(ctx, txn, p.ExecCfg().ProtectedTimestampProvider)
		}); err != nil {
			log.Errorf(ctx, "failed to release protected timestamp: %v", err)
		}
	}

	addToFileFormatTelemetry(details.Format.Format.String(), "succeeded")
	telemetry.CountBucketed("import.rows", r.res.Rows)
	const mb = 1 << 20
	sizeMb := r.res.DataSize / mb
	telemetry.CountBucketed("import.size-mb", sizeMb)

	sec := int64(timeutil.Since(timeutil.FromUnixMicros(r.job.Payload().StartedMicros)).Seconds())
	var mbps int64
	if sec > 0 {
		mbps = mb / sec
	}
	telemetry.CountBucketed("import.duration-sec.succeeded", sec)
	telemetry.CountBucketed("import.speed-mbps", mbps)
	// Tiny imports may skew throughput numbers due to overhead.
	if sizeMb > 10 {
		telemetry.CountBucketed("import.speed-mbps.over10mb", mbps)
	}

	resultsCh <- tree.Datums{
		tree.NewDInt(tree.DInt(*r.job.ID())),
		tree.NewDString(string(jobs.StatusSucceeded)),
		tree.NewDFloat(tree.DFloat(1.0)),
		tree.NewDInt(tree.DInt(r.res.Rows)),
		tree.NewDInt(tree.DInt(r.res.IndexEntries)),
		tree.NewDInt(tree.DInt(r.res.DataSize)),
	}

	return nil
}

// publishTables updates the status of imported tables from OFFLINE to PUBLIC.
func (r *importResumer) publishTables(ctx context.Context, execCfg *sql.ExecutorConfig) error {
	details := r.job.Details().(jobspb.ImportDetails)
	// Tables should only be published once.
	if details.TablesPublished {
		return nil
	}
	log.Event(ctx, "making tables live")

	lm, ie, db := execCfg.LeaseManager, execCfg.InternalExecutor, execCfg.DB
	err := descs.Txn(ctx, execCfg.Settings, lm, ie, db, func(
		ctx context.Context, txn *kv.Txn, descsCol *descs.Collection,
	) error {
		b := txn.NewBatch()
		for _, tbl := range details.Tables {
			newTableDesc, err := descsCol.GetMutableTableVersionByID(ctx, tbl.Desc.ID, txn)
			if err != nil {
				return err
			}
			newTableDesc.State = descpb.DescriptorState_PUBLIC
			newTableDesc.OfflineReason = ""

			if !tbl.IsNew {
				// NB: This is not using AllNonDropIndexes or directly mutating the
				// constraints returned by the other usual helpers because we need to
				// replace the `OutboundFKs` and `Checks` slices of newTableDesc with copies
				// that we can mutate. We need to do that because newTableDesc is a shallow
				// copy of tbl.Desc that we'll be asserting is the current version when we
				// CPut below.
				//
				// Set FK constraints to unvalidated before publishing the table imported
				// into.
				newTableDesc.OutboundFKs = make([]descpb.ForeignKeyConstraint, len(newTableDesc.OutboundFKs))
				copy(newTableDesc.OutboundFKs, tbl.Desc.OutboundFKs)
				for i := range newTableDesc.OutboundFKs {
					newTableDesc.OutboundFKs[i].Validity = descpb.ConstraintValidity_Unvalidated
				}

				// Set CHECK constraints to unvalidated before publishing the table imported into.
				for _, c := range newTableDesc.AllActiveAndInactiveChecks() {
					c.Validity = descpb.ConstraintValidity_Unvalidated
				}
			}

			// TODO(dt): re-validate any FKs?
			if err := descsCol.WriteDescToBatch(
				ctx, false /* kvTrace */, newTableDesc, b,
			); err != nil {
				return errors.Wrapf(err, "publishing table %d", newTableDesc.ID)
			}
		}
		if err := txn.Run(ctx, b); err != nil {
			return errors.Wrap(err, "publishing tables")
		}

		// Update job record to mark tables published state as complete.
		details.TablesPublished = true
		err := r.job.WithTxn(txn).SetDetails(ctx, details)
		if err != nil {
			return errors.Wrap(err, "updating job details after publishing tables")
		}
		return nil
	})

	if err != nil {
		return err
	}

	// Initiate a run of CREATE STATISTICS. We don't know the actual number of
	// rows affected per table, so we use a large number because we want to make
	// sure that stats always get created/refreshed here.
	for i := range details.Tables {
		execCfg.StatsRefresher.NotifyMutation(details.Tables[i].Desc.ID, math.MaxInt32 /* rowsAffected */)
	}

	return nil
}

// OnFailOrCancel is part of the jobs.Resumer interface. Removes data that has
// been committed from a import that has failed or been canceled. It does this
// by adding the table descriptors in DROP state, which causes the schema change
// stuff to delete the keys in the background.
func (r *importResumer) OnFailOrCancel(ctx context.Context, execCtx interface{}) error {
	details := r.job.Details().(jobspb.ImportDetails)
	addToFileFormatTelemetry(details.Format.Format.String(), "failed")
	cfg := execCtx.(sql.JobExecContext).ExecCfg()
	lm, ie, db := cfg.LeaseManager, cfg.InternalExecutor, cfg.DB
	return descs.Txn(ctx, cfg.Settings, lm, ie, db, func(
		ctx context.Context, txn *kv.Txn, descsCol *descs.Collection,
	) error {
		if err := r.dropTables(ctx, txn, descsCol, cfg); err != nil {
			return err
		}
		return r.releaseProtectedTimestamp(ctx, txn, cfg.ProtectedTimestampProvider)
	})
}

func (r *importResumer) releaseProtectedTimestamp(
	ctx context.Context, txn *kv.Txn, pts protectedts.Storage,
) error {
	details := r.job.Details().(jobspb.ImportDetails)
	ptsID := details.ProtectedTimestampRecord
	// If the job doesn't have a protected timestamp then there's nothing to do.
	if ptsID == nil {
		return nil
	}
	err := pts.Release(ctx, txn, *ptsID)
	if errors.Is(err, protectedts.ErrNotExists) {
		// No reason to return an error which might cause problems if it doesn't
		// seem to exist.
		log.Warningf(ctx, "failed to release protected which seems not to exist: %v", err)
		err = nil
	}
	return err
}

// dropTables implements the OnFailOrCancel logic.
func (r *importResumer) dropTables(
	ctx context.Context, txn *kv.Txn, descsCol *descs.Collection, execCfg *sql.ExecutorConfig,
) error {
	details := r.job.Details().(jobspb.ImportDetails)

	// If the prepare step of the import job was not completed then the
	// descriptors do not need to be rolled back as the txn updating them never
	// completed.
	if !details.PrepareComplete {
		return nil
	}

	var revert []*tabledesc.Immutable
	var empty []*tabledesc.Immutable
	for _, tbl := range details.Tables {
		if !tbl.IsNew {
			desc, err := descsCol.GetMutableTableVersionByID(ctx, tbl.Desc.ID, txn)
			if err != nil {
				return err
			}
			imm := desc.ImmutableCopy().(*tabledesc.Immutable)
			if tbl.WasEmpty {
				empty = append(empty, imm)
			} else {
				revert = append(revert, imm)
			}
		}
	}

	// NB: if a revert fails it will abort the rest of this failure txn, which is
	// also what brings tables back online. We _could_ change the error handling
	// or just move the revert into Resume()'s error return path, however it isn't
	// clear that just bringing a table back online with partially imported data
	// that may or may not be partially reverted is actually a good idea. It seems
	// better to do the revert here so that the table comes back if and only if,
	// it was rolled back to its pre-IMPORT state, and instead provide a manual
	// admin knob (e.g. ALTER TABLE REVERT TO SYSTEM TIME) if anything goes wrong.
	if len(revert) > 0 {
		// Sanity check Walltime so it doesn't become a TRUNCATE if there's a bug.
		if details.Walltime == 0 {
			return errors.Errorf("invalid pre-IMPORT time to rollback")
		}
		ts := hlc.Timestamp{WallTime: details.Walltime}.Prev()
		if err := sql.RevertTables(ctx, txn.DB(), execCfg, revert, ts, sql.RevertTableDefaultBatchSize); err != nil {
			return errors.Wrap(err, "rolling back partially completed IMPORT")
		}
	}

	for i := range empty {
		if err := gcjob.ClearTableData(ctx, execCfg.DB, execCfg.DistSender, execCfg.Codec, empty[i]); err != nil {
			return errors.Wrapf(err, "clearing data for table %d", empty[i].ID)
		}
	}

	b := txn.NewBatch()
	dropTime := int64(1)
	tablesToGC := make([]descpb.ID, 0, len(details.Tables))
	for _, tbl := range details.Tables {
		newTableDesc, err := descsCol.GetMutableTableVersionByID(ctx, tbl.Desc.ID, txn)
		if err != nil {
			return err
		}
		if tbl.IsNew {
			newTableDesc.State = descpb.DescriptorState_DROP
			// If the DropTime if set, a table uses RangeClear for fast data removal. This
			// operation starts at DropTime + the GC TTL. If we used now() here, it would
			// not clean up data until the TTL from the time of the error. Instead, use 1
			// (that is, 1ns past the epoch) to allow this to be cleaned up as soon as
			// possible. This is safe since the table data was never visible to users,
			// and so we don't need to preserve MVCC semantics.
			newTableDesc.DropTime = dropTime
			catalogkv.WriteObjectNamespaceEntryRemovalToBatch(
				ctx,
				b,
				execCfg.Codec,
				newTableDesc.ParentID,
				newTableDesc.GetParentSchemaID(),
				newTableDesc.Name,
				false, /* kvTrace */
			)
			tablesToGC = append(tablesToGC, newTableDesc.ID)
		} else {
			// IMPORT did not create this table, so we should not drop it.
			newTableDesc.State = descpb.DescriptorState_PUBLIC
		}
		if err := descsCol.WriteDescToBatch(
			ctx, false /* kvTrace */, newTableDesc, b,
		); err != nil {
			return err
		}
	}

	// Queue a GC job.
	gcDetails := jobspb.SchemaChangeGCDetails{}
	for _, tableID := range tablesToGC {
		gcDetails.Tables = append(gcDetails.Tables, jobspb.SchemaChangeGCDetails_DroppedID{
			ID:       tableID,
			DropTime: dropTime,
		})
	}
	gcJobRecord := jobs.Record{
		Description:   fmt.Sprintf("GC for %s", r.job.Payload().Description),
		Username:      r.job.Payload().UsernameProto.Decode(),
		DescriptorIDs: tablesToGC,
		Details:       gcDetails,
		Progress:      jobspb.SchemaChangeGCProgress{},
		NonCancelable: true,
	}
	if _, err := execCfg.JobRegistry.CreateJobWithTxn(ctx, gcJobRecord, txn); err != nil {
		return err
	}

	return errors.Wrap(txn.Run(ctx, b), "rolling back tables")
}

var _ jobs.Resumer = &importResumer{}

func init() {
	sql.AddPlanHook(importPlanHook)
	jobs.RegisterConstructor(
		jobspb.TypeImport,
		func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
			return &importResumer{
				job:      job,
				settings: settings,
			}
		},
	)
}
