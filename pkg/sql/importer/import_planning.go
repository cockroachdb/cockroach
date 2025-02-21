// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package importer

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/docs"
	"github.com/cockroachdb/cockroach/pkg/featureflag"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/exprutil"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

const (
	csvDelimiter        = "delimiter"
	csvComment          = "comment"
	csvNullIf           = "nullif"
	csvSkip             = "skip"
	csvRowLimit         = "row_limit"
	csvStrictQuotes     = "strict_quotes"
	csvAllowQuotedNulls = "allow_quoted_null"

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
	importOptionDetached         = "detached"

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

	pgDumpIgnoreAllUnsupported     = "ignore_unsupported_statements"
	pgDumpIgnoreShuntFileDest      = "log_ignored_statements"
	pgDumpUnsupportedSchemaStmtLog = "unsupported_schema_stmts"
	pgDumpUnsupportedDataStmtLog   = "unsupported_data_stmts"

	// statusImportBundleParseSchema indicates to the user that a bundle format
	// schema is being parsed
	statusImportBundleParseSchema jobs.StatusMessage = "parsing schema on Import Bundle"
)

var importOptionExpectValues = map[string]exprutil.KVStringOptValidate{
	csvDelimiter:        exprutil.KVStringOptRequireValue,
	csvComment:          exprutil.KVStringOptRequireValue,
	csvNullIf:           exprutil.KVStringOptRequireValue,
	csvSkip:             exprutil.KVStringOptRequireValue,
	csvRowLimit:         exprutil.KVStringOptRequireValue,
	csvStrictQuotes:     exprutil.KVStringOptRequireNoValue,
	csvAllowQuotedNulls: exprutil.KVStringOptRequireNoValue,

	mysqlOutfileRowSep:   exprutil.KVStringOptRequireValue,
	mysqlOutfileFieldSep: exprutil.KVStringOptRequireValue,
	mysqlOutfileEnclose:  exprutil.KVStringOptRequireValue,
	mysqlOutfileEscape:   exprutil.KVStringOptRequireValue,

	importOptionSSTSize:      exprutil.KVStringOptRequireValue,
	importOptionDecompress:   exprutil.KVStringOptRequireValue,
	importOptionOversample:   exprutil.KVStringOptRequireValue,
	importOptionSaveRejected: exprutil.KVStringOptRequireNoValue,

	importOptionSkipFKs:          exprutil.KVStringOptRequireNoValue,
	importOptionDisableGlobMatch: exprutil.KVStringOptRequireNoValue,
	importOptionDetached:         exprutil.KVStringOptRequireNoValue,

	optMaxRowSize: exprutil.KVStringOptRequireValue,

	avroStrict:             exprutil.KVStringOptRequireNoValue,
	avroSchema:             exprutil.KVStringOptRequireValue,
	avroSchemaURI:          exprutil.KVStringOptRequireValue,
	avroRecordsSeparatedBy: exprutil.KVStringOptRequireValue,
	avroBinRecords:         exprutil.KVStringOptRequireNoValue,
	avroJSONRecords:        exprutil.KVStringOptRequireNoValue,

	pgDumpIgnoreAllUnsupported: exprutil.KVStringOptRequireNoValue,
	pgDumpIgnoreShuntFileDest:  exprutil.KVStringOptRequireValue,
}

var pgDumpMaxLoggedStmts = 1024

func testingSetMaxLogIgnoredImportStatements(maxLogSize int) (cleanup func()) {
	prevLogSize := pgDumpMaxLoggedStmts
	pgDumpMaxLoggedStmts = maxLogSize
	return func() {
		pgDumpMaxLoggedStmts = prevLogSize
	}
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
	importOptionSaveRejected, importOptionDisableGlobMatch, importOptionDetached)

// Format specific allowed options.
var avroAllowedOptions = makeStringSet(
	avroStrict, avroBinRecords, avroJSONRecords,
	avroRecordsSeparatedBy, avroSchema, avroSchemaURI, optMaxRowSize, csvRowLimit,
)

var csvAllowedOptions = makeStringSet(
	csvDelimiter, csvComment, csvNullIf, csvSkip, csvStrictQuotes, csvRowLimit, csvAllowQuotedNulls,
)

var mysqlOutAllowedOptions = makeStringSet(
	mysqlOutfileRowSep, mysqlOutfileFieldSep, mysqlOutfileEnclose,
	mysqlOutfileEscape, csvNullIf, csvSkip, csvRowLimit,
)

var (
	mysqlDumpAllowedOptions = makeStringSet(importOptionSkipFKs, csvRowLimit)
	pgCopyAllowedOptions    = makeStringSet(pgCopyDelimiter, pgCopyNull, optMaxRowSize)
	pgDumpAllowedOptions    = makeStringSet(optMaxRowSize, importOptionSkipFKs, csvRowLimit,
		pgDumpIgnoreAllUnsupported, pgDumpIgnoreShuntFileDest)
)

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

// featureImportEnabled is used to enable and disable the IMPORT feature.
var featureImportEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"feature.import.enabled",
	"set to true to enable imports, false to disable; default is true",
	featureflag.FeatureFlagEnabledDefault,
	settings.WithPublic)

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
	ctx context.Context,
	p sql.PlanHookState,
	orig *tree.Import,
	files []string,
	opts map[string]string,
) (string, error) {
	stmt := *orig
	stmt.Files = nil
	for _, file := range files {
		clean, err := cloud.SanitizeExternalStorageURI(file, nil /* extraParams */)
		if err != nil {
			return "", err
		}
		logSanitizedImportDestination(ctx, clean)
		stmt.Files = append(stmt.Files, tree.NewDString(clean))
	}
	stmt.Options = nil
	for k, v := range opts {
		opt := tree.KVOption{Key: tree.Name(k)}
		val := importOptionExpectValues[k] == exprutil.KVStringOptRequireValue
		val = val || (importOptionExpectValues[k] == exprutil.KVStringOptAny && len(v) > 0)
		if val {
			opt.Value = tree.NewDString(v)
		}
		stmt.Options = append(stmt.Options, opt)
	}
	sort.Slice(stmt.Options, func(i, j int) bool { return stmt.Options[i].Key < stmt.Options[j].Key })
	ann := p.ExtendedEvalContext().Annotations
	return tree.AsStringWithFlags(
		&stmt, tree.FmtAlwaysQualifyNames|tree.FmtShowFullURIs, tree.FmtAnnotations(ann),
	), nil
}

func logSanitizedImportDestination(ctx context.Context, destination string) {
	log.Ops.Infof(ctx, "import planning to connect to destination %v", redact.Safe(destination))
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

// resolveUDTsUsedByImportInto resolves all the user defined types that are
// referenced by the table being imported into.
func resolveUDTsUsedByImportInto(
	ctx context.Context, p sql.PlanHookState, table *tabledesc.Mutable,
) ([]catalog.TypeDescriptor, error) {
	typeDescs := make([]catalog.TypeDescriptor, 0)
	var dbDesc catalog.DatabaseDescriptor
	err := sql.DescsTxn(ctx, p.ExecCfg(), func(
		ctx context.Context, txn isql.Txn, descriptors *descs.Collection,
	) (err error) {
		dbDesc, err = descriptors.ByIDWithoutLeased(txn.KV()).WithoutNonPublic().Get().Database(ctx, table.GetParentID())
		if err != nil {
			return err
		}
		typeIDs, _, err := table.GetAllReferencedTypeIDs(dbDesc,
			func(id descpb.ID) (catalog.TypeDescriptor, error) {
				immutDesc, err := descriptors.ByIDWithoutLeased(txn.KV()).WithoutNonPublic().Get().Type(ctx, id)
				if err != nil {
					return nil, err
				}
				return immutDesc, nil
			})
		if err != nil {
			return errors.Wrap(err, "resolving type descriptors")
		}

		for _, typeID := range typeIDs {
			immutDesc, err := descriptors.ByIDWithoutLeased(txn.KV()).WithoutNonPublic().Get().Type(ctx, typeID)
			if err != nil {
				return err
			}
			typeDescs = append(typeDescs, immutDesc)
		}
		return err
	})
	return typeDescs, err
}

func importTypeCheck(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (matched bool, header colinfo.ResultColumns, _ error) {
	importStmt, ok := stmt.(*tree.Import)
	if !ok {
		return false, nil, nil
	}
	if err := exprutil.TypeCheck(
		ctx, "IMPORT", p.SemaCtx(),
		exprutil.KVOptions{
			KVOptions: importStmt.Options, Validation: importOptionExpectValues,
		},
		exprutil.StringArrays{
			importStmt.Files,
		},
	); err != nil {
		return false, nil, err
	}
	header = jobs.BulkJobExecutionResultHeader
	if importStmt.Options.HasKey(importOptionDetached) {
		header = jobs.DetachedJobExecutionResultHeader
	}
	return true, header, nil
}

// importPlanHook implements sql.PlanHookFn.
func importPlanHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, colinfo.ResultColumns, bool, error) {
	importStmt, ok := stmt.(*tree.Import)
	if !ok {
		return nil, nil, false, nil
	}

	if !importStmt.Bundle && !importStmt.Into {
		p.BufferClientNotice(ctx, pgnotice.Newf("IMPORT TABLE has been deprecated in 21.2, and will be removed in a future version."+
			" Instead, use CREATE TABLE with the desired schema, and IMPORT INTO the newly created table."))
	}
	switch f := strings.ToUpper(importStmt.FileFormat); f {
	case "PGDUMP", "MYSQLDUMP":
		p.BufferClientNotice(ctx, pgnotice.Newf(
			"IMPORT %s has been deprecated in 23.1, and will be removed in a future version. See %s for alternatives.",
			redact.SafeString(f),
			redact.SafeString(docs.URL("migration-overview")),
		))
	}

	addToFileFormatTelemetry(importStmt.FileFormat, "attempted")

	if err := featureflag.CheckEnabled(
		ctx,
		p.ExecCfg(),
		featureImportEnabled,
		"IMPORT",
	); err != nil {
		return nil, nil, false, err
	}

	exprEval := p.ExprEvaluator("IMPORT")
	opts, err := exprEval.KVOptions(
		ctx, importStmt.Options, importOptionExpectValues,
	)
	if err != nil {
		return nil, nil, false, err
	}

	var isDetached bool
	if _, ok := opts[importOptionDetached]; ok {
		isDetached = true
	}

	filenamePatterns, err := exprEval.StringArray(ctx, importStmt.Files)
	if err != nil {
		return nil, nil, false, err
	}

	// Certain ExternalStorage URIs require super-user access. Check all the
	// URIs passed to the IMPORT command.
	for _, file := range filenamePatterns {
		_, err := cloud.ExternalStorageConfFromURI(file, p.User())
		if err != nil {
			// If it is a workload URI, it won't parse as a storage config, but it
			// also doesn't have any auth concerns so just continue.
			if _, workloadErr := parseWorkloadConfig(file); workloadErr == nil {
				continue
			}
			return nil, nil, false, err
		}
		if err := sql.CheckDestinationPrivileges(ctx, p, []string{file}); err != nil {
			return nil, nil, false, err
		}
	}

	fn := func(ctx context.Context, resultsCh chan<- tree.Datums) error {
		// TODO(dan): Move this span into sql.
		ctx, span := tracing.ChildSpan(ctx, importStmt.StatementTag())
		defer span.Finish()

		if !(p.ExtendedEvalContext().TxnIsSingleStmt || isDetached) {
			return errors.Errorf("IMPORT cannot be used inside a multi-statement transaction without DETACHED option")
		}

		var files []string
		if _, ok := opts[importOptionDisableGlobMatch]; ok {
			files = filenamePatterns
		} else {
			for _, file := range filenamePatterns {
				uri, err := url.Parse(file)
				if err != nil {
					return err
				}
				if strings.Contains(uri.Scheme, "workload") || strings.HasPrefix(uri.Scheme, "http") {
					files = append(files, file)
					continue
				}
				prefix := cloud.GetPrefixBeforeWildcard(uri.Path)
				if len(prefix) < len(uri.Path) {
					pattern := uri.Path[len(prefix):]
					uri.Path = prefix
					s, err := p.ExecCfg().DistSQLSrv.ExternalStorageFromURI(ctx, uri.String(), p.User())
					if err != nil {
						return err
					}
					var expandedFiles []string
					if err := s.List(ctx, "", "", func(s string) error {
						ok, err := path.Match(pattern, s)
						if ok {
							uri.Path = prefix + s
							expandedFiles = append(expandedFiles, uri.String())
						}
						return err
					}); err != nil {
						return err
					}
					if len(expandedFiles) < 1 {
						return errors.Errorf(`no files matched %q in prefix %q in uri provided: %q`, pattern, prefix, file)
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
		var db catalog.DatabaseDescriptor
		var sc catalog.SchemaDescriptor
		if table != nil {
			// TODO: As part of work for #34240, we should be operating on
			//  UnresolvedObjectNames here, rather than TableNames.
			// We have a target table, so it might specify a DB in its name.
			un := table.ToUnresolvedObjectName()
			found, prefix, resPrefix, err := resolver.ResolveTarget(ctx,
				un, p, p.SessionData().Database, p.SessionData().SearchPath)
			if err != nil {
				return pgerror.Wrap(err, pgcode.UndefinedTable,
					"resolving target import name")
			}
			if !found {
				// Check if database exists right now. It might not after the import is done,
				// but it's better to fail fast than wait until restore.
				return pgerror.Newf(pgcode.UndefinedObject,
					"database does not exist: %q", table)
			}
			table.ObjectNamePrefix = prefix
			db = resPrefix.Database
			sc = resPrefix.Schema
			// If this is a non-INTO import that will thus be making a new table, we
			// need the CREATE priv in the target DB.
			if !importStmt.Into {
				if err := p.CheckPrivilege(ctx, db, privilege.CREATE); err != nil {
					return err
				}
			}

			switch sc.SchemaKind() {
			case catalog.SchemaVirtual:
				return pgerror.Newf(pgcode.InvalidSchemaName,
					"cannot import into schema %q", table.SchemaName)
			}
		} else {
			// No target table means we're importing whatever we find into the session
			// database, so it must exist.
			db, err = p.MustGetCurrentSessionDatabase(ctx)
			if err != nil {
				return pgerror.Wrap(err, pgcode.UndefinedObject,
					"could not resolve current database")
			}
			// If this is a non-INTO import that will thus be making a new table, we
			// need the CREATE priv in the target DB.
			if !importStmt.Into {
				if err := p.CheckPrivilege(ctx, db, privilege.CREATE); err != nil {
					return err
				}
			}
			sc = schemadesc.GetPublicSchema()
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

			if _, ok := opts[csvAllowQuotedNulls]; ok {
				format.Csv.AllowQuotedNull = true
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
			if override, ok := opts[csvRowLimit]; ok {
				rowLimit, err := strconv.Atoi(override)
				if err != nil {
					return pgerror.Wrapf(err, pgcode.Syntax, "invalid numeric %s value", csvRowLimit)
				}
				if rowLimit <= 0 {
					return pgerror.Newf(pgcode.Syntax, "%s must be > 0", csvRowLimit)
				}
				format.MysqlDump.RowLimit = int64(rowLimit)
			}
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
			if _, ok := opts[pgDumpIgnoreAllUnsupported]; ok {
				format.PgDump.IgnoreUnsupported = true
			}

			if dest, ok := opts[pgDumpIgnoreShuntFileDest]; ok {
				if !format.PgDump.IgnoreUnsupported {
					return errors.New("cannot log unsupported PGDUMP stmts without `ignore_unsupported_statements` option")
				}
				format.PgDump.IgnoreUnsupportedLog = dest
			}

			if override, ok := opts[csvRowLimit]; ok {
				rowLimit, err := strconv.Atoi(override)
				if err != nil {
					return pgerror.Wrapf(err, pgcode.Syntax, "invalid numeric %s value", csvRowLimit)
				}
				if rowLimit <= 0 {
					return pgerror.Newf(pgcode.Syntax, "%s must be > 0", csvRowLimit)
				}
				format.PgDump.RowLimit = int64(rowLimit)
			}
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
		var typeDetails []jobspb.ImportDetails_Type
		jobDesc, err := importJobDescription(ctx, p, importStmt, filenamePatterns, opts)
		if err != nil {
			return err
		}

		if importStmt.Into {
			if _, ok := allowedIntoFormats[importStmt.FileFormat]; !ok {
				return errors.Newf(
					"%s file format is currently unsupported by IMPORT INTO",
					importStmt.FileFormat)
			}
			_, found, err := p.ResolveMutableTableDescriptor(ctx, table, true, tree.ResolveRequireTableDesc)
			if err != nil {
				return err
			}

			err = ensureRequiredPrivileges(ctx, importIntoRequiredPrivileges, p, found)
			if err != nil {
				return err
			}

			if len(found.LDRJobIDs) > 0 {
				return errors.Newf("cannot run an import on table %s which is apart of a Logical Data Replication stream", table)
			}

			// Import into an RLS table is blocked, unless this is the admin. It is
			// allowed for admins since they are exempt from RLS policies and have
			// unrestricted read/write access.
			if found.IsRowLevelSecurityEnabled() {
				admin, err := p.HasAdminRole(ctx)
				if err != nil {
					return err
				} else if !admin {
					return pgerror.New(pgcode.FeatureNotSupported,
						"IMPORT INTO not supported with row-level security for non-admin users")
				}
			}

			// Validate target columns.
			var intoCols []string
			isTargetCol := make(map[string]bool)
			for _, name := range importStmt.IntoCols {
				active, err := catalog.MustFindPublicColumnsByNameList(found, tree.NameList{name})
				if err != nil {
					return errors.Wrap(err, "verifying target columns")
				}

				isTargetCol[active[0].GetName()] = true
				intoCols = append(intoCols, active[0].GetName())
			}

			// Ensure that non-target columns that don't have default
			// expressions are nullable.
			if len(isTargetCol) != 0 {
				for _, col := range found.VisibleColumns() {
					if !(isTargetCol[col.GetName()] || col.IsNullable() || col.HasDefault() || col.IsComputed()) {
						return errors.Newf(
							"all non-target columns in IMPORT INTO must be nullable "+
								"or have default expressions, or have computed expressions"+
								" but violated by column %q",
							col.GetName(),
						)
					}
					if isTargetCol[col.GetName()] && col.IsComputed() {
						return schemaexpr.CannotWriteToComputedColError(col.GetName())
					}
				}
			}

			{
				// Resolve the UDTs used by the table being imported into.
				typeDescs, err := resolveUDTsUsedByImportInto(ctx, p, found)
				if err != nil {
					return errors.Wrap(err, "resolving UDTs used by table being imported into")
				}
				if len(typeDescs) > 0 {
					typeDetails = make([]jobspb.ImportDetails_Type, 0, len(typeDescs))
				}
				for _, typeDesc := range typeDescs {
					typeDetails = append(typeDetails, jobspb.ImportDetails_Type{Desc: typeDesc.TypeDesc()})
				}
			}

			tableDetails = []jobspb.ImportDetails_Table{{Desc: &found.TableDescriptor, IsNew: false, TargetCols: intoCols}}
		} else if importStmt.Bundle {
			// If we target a single table, populate details with one entry of tableName.
			if table != nil {
				tableDetails = make([]jobspb.ImportDetails_Table, 1)
				tableName := table.ObjectName.String()
				// PGDUMP supports importing tables from non-public schemas, thus we
				// must prepend the target table name with the target schema name.
				if format.Format == roachpb.IOFileFormat_PgDump {
					if table.Schema() == "" {
						return errors.Newf("expected schema for target table %s to be resolved",
							tableName)
					}
					tableName = fmt.Sprintf("%s.%s", table.SchemaName.String(),
						table.ObjectName.String())
				}
				tableDetails[0] = jobspb.ImportDetails_Table{
					Name:  tableName,
					IsNew: true,
				}
			}

			// Due to how we generate and rewrite descriptor ID's for import, we run
			// into problems when using user defined schemas.
			publicSchemaID := db.GetSchemaID(catconstants.PublicSchemaName)
			if sc.GetID() != publicSchemaID && sc.GetID() != keys.PublicSchemaID {
				err := errors.New("cannot use IMPORT with a user defined schema")
				hint := errors.WithHint(err, "create the table with CREATE TABLE and use IMPORT INTO instead")
				return hint
			}
		}

		// Store the primary region of the database being imported into. This is
		// used during job execution to evaluate certain default expressions and
		// computed columns such as `gateway_region`.
		var databasePrimaryRegion catpb.RegionName
		if db.IsMultiRegion() {
			if err := sql.DescsTxn(ctx, p.ExecCfg(), func(
				ctx context.Context, txn isql.Txn, descsCol *descs.Collection,
			) error {
				regionConfig, err := sql.SynthesizeRegionConfig(ctx, txn.KV(), db.GetID(), descsCol)
				if err != nil {
					return err
				}
				databasePrimaryRegion = regionConfig.PrimaryRegion()
				return nil
			}); err != nil {
				return errors.Wrap(err, "failed to resolve region config for multi region database")
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
		if importStmt.Into {
			telemetry.Count("import.into")
		}

		// Here we create the job in a side transaction and then kick off the job.
		// This is awful. Rather we should be disallowing this statement in an
		// explicit transaction and then we should create the job in the user's
		// transaction here and then in a post-commit hook we should kick of the
		// StartableJob which we attached to the connExecutor somehow.

		importDetails := jobspb.ImportDetails{
			URIs:                  files,
			Format:                format,
			ParentID:              db.GetID(),
			Tables:                tableDetails,
			Types:                 typeDetails,
			SSTSize:               sstSize,
			Oversample:            oversample,
			SkipFKs:               skipFKs,
			ParseBundleSchema:     importStmt.Bundle,
			DefaultIntSize:        p.SessionData().DefaultIntSize,
			DatabasePrimaryRegion: databasePrimaryRegion,
		}

		jr := jobs.Record{
			Description: jobDesc,
			Username:    p.User(),
			Details:     importDetails,
			Progress:    jobspb.ImportProgress{},
		}

		if isDetached {
			// When running inside an explicit transaction, we simply create the job
			// record. We do not wait for the job to finish.
			jobID := p.ExecCfg().JobRegistry.MakeJobID()
			_, err := p.ExecCfg().JobRegistry.CreateAdoptableJobWithTxn(
				ctx, jr, jobID, p.InternalSQLTxn())
			if err != nil {
				return err
			}

			addToFileFormatTelemetry(format.Format.String(), "started")
			resultsCh <- tree.Datums{tree.NewDInt(tree.DInt(jobID))}
			return nil
		}

		// We create the job record in the planner's transaction to ensure that
		// the job record creation happens transactionally.
		plannerTxn := p.InternalSQLTxn()

		// Construct the job and commit the transaction. Perform this work in a
		// closure to ensure that the job is cleaned up if an error occurs.
		var sj *jobs.StartableJob
		if err := func() (err error) {
			defer func() {
				if err == nil || sj == nil {
					return
				}
				if cleanupErr := sj.CleanupOnRollback(ctx); cleanupErr != nil {
					log.Errorf(ctx, "failed to cleanup job: %v", cleanupErr)
				}
			}()
			jobID := p.ExecCfg().JobRegistry.MakeJobID()
			if err := p.ExecCfg().JobRegistry.CreateStartableJobWithTxn(ctx, &sj, jobID, plannerTxn, jr); err != nil {
				return err
			}

			// We commit the transaction here so that the job can be started. This
			// is safe because we're in an implicit transaction. If we were in an
			// explicit transaction the job would have to be run with the detached
			// option and would have been handled above.
			return plannerTxn.KV().Commit(ctx)
		}(); err != nil {
			return err
		}

		// Release all descriptor leases here. We need to do this because we're
		// about to kick off a job which is going to potentially write descriptors.
		// Note that we committed the underlying transaction in the above closure
		// -- so we're not using any leases anymore, but we might be holding some
		// because some sql queries might have been executed by this transaction
		// (indeed some certainly were when we created the job we're going to run).
		//
		// This is all a bit of a hack to deal with the fact that we want to
		// return results as part of this statement and the usual machinery for
		// releasing leases assumes that that does not happen during statement
		// execution.
		p.InternalSQLTxn().Descriptors().ReleaseAll(ctx)
		if err := sj.Start(ctx); err != nil {
			return err
		}
		addToFileFormatTelemetry(format.Format.String(), "started")
		if err := sj.AwaitCompletion(ctx); err != nil {
			return err
		}
		return sj.ReportExecutionResults(ctx, resultsCh)
	}

	if isDetached {
		return fn, jobs.DetachedJobExecutionResultHeader, false, nil
	}
	return fn, jobs.BulkJobExecutionResultHeader, false, nil
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

			raw, _, err := store.ReadFile(ctx, "", cloud.ReadOptions{NoFileSize: true})
			if err != nil {
				return err
			}
			defer raw.Close(ctx)
			schemaBytes, err := ioctx.ReadAll(ctx, raw)
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

type loggerKind int

const (
	schemaParsing loggerKind = iota
	dataIngestion
)

// unsupportedStmtLogger is responsible for handling unsupported PGDUMP SQL
// statements seen during the import.
type unsupportedStmtLogger struct {
	ctx   context.Context
	user  username.SQLUsername
	jobID int64

	// Values are initialized based on the options specified in the IMPORT PGDUMP
	// stmt.
	ignoreUnsupported        bool
	ignoreUnsupportedLogDest string
	externalStorage          cloud.ExternalStorageFactory

	// logBuffer holds the string to be flushed to the ignoreUnsupportedLogDest.
	logBuffer       *bytes.Buffer
	numIgnoredStmts int

	// Incremented every time the logger flushes. It is used as the suffix of the
	// log file written to external storage.
	flushCount int

	loggerType loggerKind
}

func makeUnsupportedStmtLogger(
	ctx context.Context,
	user username.SQLUsername,
	jobID int64,
	ignoreUnsupported bool,
	unsupportedLogDest string,
	loggerType loggerKind,
	externalStorage cloud.ExternalStorageFactory,
) *unsupportedStmtLogger {
	return &unsupportedStmtLogger{
		ctx:                      ctx,
		user:                     user,
		jobID:                    jobID,
		ignoreUnsupported:        ignoreUnsupported,
		ignoreUnsupportedLogDest: unsupportedLogDest,
		loggerType:               loggerType,
		logBuffer:                new(bytes.Buffer),
		externalStorage:          externalStorage,
	}
}

func (u *unsupportedStmtLogger) log(logLine string, isParseError bool) error {
	// We have already logged parse errors during the schema ingestion phase, so
	// skip them to avoid duplicate entries.
	skipLoggingParseErr := isParseError && u.loggerType == dataIngestion
	if u.ignoreUnsupportedLogDest == "" || skipLoggingParseErr {
		return nil
	}

	// Flush to a file if we have hit the max size of our buffer.
	if u.numIgnoredStmts >= pgDumpMaxLoggedStmts {
		err := u.flush()
		if err != nil {
			return err
		}
	}

	if isParseError {
		logLine = fmt.Sprintf("%s: could not be parsed\n", logLine)
	} else {
		logLine = fmt.Sprintf("%s: unsupported by IMPORT\n", logLine)
	}
	u.logBuffer.Write([]byte(logLine))
	u.numIgnoredStmts++
	return nil
}

func (u *unsupportedStmtLogger) flush() error {
	if u.ignoreUnsupportedLogDest == "" {
		return nil
	}

	conf, err := cloud.ExternalStorageConfFromURI(u.ignoreUnsupportedLogDest, u.user)
	if err != nil {
		return errors.Wrap(err, "failed to log unsupported stmts during IMPORT PGDUMP")
	}
	var s cloud.ExternalStorage
	if s, err = u.externalStorage(u.ctx, conf); err != nil {
		return errors.New("failed to log unsupported stmts during IMPORT PGDUMP")
	}
	defer s.Close()

	logFileName := fmt.Sprintf("import%d", u.jobID)
	if u.loggerType == dataIngestion {
		logFileName = path.Join(logFileName, pgDumpUnsupportedDataStmtLog, fmt.Sprintf("%d.log", u.flushCount))
	} else {
		logFileName = path.Join(logFileName, pgDumpUnsupportedSchemaStmtLog, fmt.Sprintf("%d.log", u.flushCount))
	}
	err = cloud.WriteFile(u.ctx, s, logFileName, bytes.NewReader(u.logBuffer.Bytes()))
	if err != nil {
		return errors.Wrap(err, "failed to log unsupported stmts to log during IMPORT PGDUMP")
	}
	u.flushCount++
	u.numIgnoredStmts = 0
	u.logBuffer.Truncate(0)
	return nil
}

func init() {
	sql.AddPlanHook("import", importPlanHook, importTypeCheck)
}
