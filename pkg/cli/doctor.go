// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"bufio"
	"context"
	"database/sql/driver"
	"encoding/base64"
	hx "encoding/hex"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	apd "github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/cli/clierror"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlclient"
	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/doctor"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
	"github.com/spf13/cobra"
)

var debugDoctorCmd = &cobra.Command{
	Use:   "doctor [command]",
	Short: "run a cockroach doctor tool command",
	Long: `
Run the doctor tool to recreate or examine cockroach system table contents. 
System tables are queried either from a live cluster or from an unzipped 
debug.zip.
`,
}

var doctorExamineCmd = &cobra.Command{
	Use:   "examine [cluster|zipdir]",
	Short: "examine system tables for inconsistencies",
	Long: `
Run the doctor tool to examine the system table contents and perform validation
checks. System tables are queried either from a live cluster or from an unzipped 
debug.zip.
`,
}

var doctorRecreateCmd = &cobra.Command{
	Use:   "recreate [cluster|zipdir]",
	Short: "prints SQL that tries to recreate system table state",
	Long: `
Run the doctor tool to examine system tables and generate SQL statements that,
when run on an empty cluster, recreate that state as closely as possible. System
tables are queried either from a live cluster or from an unzipped debug.zip.
`,
}

func makeZipDirCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "zipdir <debug_zip_dir>",
		Short: "run doctor tool on data from an unzipped debug.zip",
		Long: `
Run the doctor tool on system data from an unzipped debug.zip. This command
requires the path of the unzipped 'debug' directory as its argument.
`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			descs, ns, jobs, err := fromZipDir(args[0])
			if err != nil {
				return err
			}
			return runDoctor(cmd.Parent().Name(), descs, ns, jobs, os.Stdout)
		},
	}
}

func makeClusterCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "cluster --url=<cluster connection string>",
		Short: "run doctor tool on live cockroach cluster",
		Long: `
Run the doctor tool system data from a live cluster specified by --url.
`,
		Args: cobra.NoArgs,
		RunE: MaybeDecorateGRPCError(
			func(cmd *cobra.Command, args []string) (resErr error) {
				sqlConn, err := makeSQLClient("cockroach doctor", useSystemDb)
				if err != nil {
					return errors.Wrap(err, "could not establish connection to cluster")
				}
				defer func() { resErr = errors.CombineErrors(resErr, sqlConn.Close()) }()
				descs, ns, jobs, err := fromCluster(sqlConn, cliCtx.cmdTimeout)
				if err != nil {
					return err
				}
				return runDoctor(cmd.Parent().Name(), descs, ns, jobs, os.Stdout)
			}),
	}
}

func deprecateCommand(cmd *cobra.Command) *cobra.Command {
	cmd.Hidden = true
	cmd.Deprecated = fmt.Sprintf("use 'doctor examine %s' instead.", cmd.Name())
	return cmd
}

var doctorExamineClusterCmd = makeClusterCommand()
var doctorExamineZipDirCmd = makeZipDirCommand()
var doctorExamineFallbackClusterCmd = deprecateCommand(makeClusterCommand())
var doctorExamineFallbackZipDirCmd = deprecateCommand(makeZipDirCommand())
var doctorRecreateClusterCmd = makeClusterCommand()
var doctorRecreateZipDirCmd = makeZipDirCommand()

func runDoctor(
	commandName string,
	descTable doctor.DescriptorTable,
	namespaceTable doctor.NamespaceTable,
	jobsTable doctor.JobsTable,
	out io.Writer,
) (err error) {
	switch commandName {
	case "recreate":
		err = doctor.DumpSQL(out, descTable, namespaceTable)
	case "doctor":
		fallthrough // Default to "examine".
	case "examine":
		var valid bool
		valid, err = doctor.Examine(
			context.Background(), descTable, namespaceTable, jobsTable, debugCtx.verbose, out)
		if err == nil {
			if !valid {
				return clierror.NewError(errors.New("validation failed"),
					exit.DoctorValidationFailed())
			}
			fmt.Fprintln(out, "No problems found!")
		}
	default:
		log.Fatalf(context.Background(), "Unexpected doctor command %q.", commandName)
	}
	if err == nil {
		return nil
	}
	return clierror.NewError(
		errors.Wrapf(err, "doctor command %q failed", commandName),
		// Note: we are using "unspecified" here because the error
		// return does not distinguish errors like connection errors
		// etc, from errors during extraction.
		exit.UnspecifiedError())
}

// fromCluster collects system table data from a live cluster.
func fromCluster(
	sqlConn clisqlclient.Conn, timeout time.Duration,
) (
	descTable doctor.DescriptorTable,
	namespaceTable doctor.NamespaceTable,
	jobsTable doctor.JobsTable,
	retErr error,
) {
	maybePrint := func(stmt string) string {
		if debugCtx.verbose {
			fmt.Println("querying " + stmt)
		}
		return stmt
	}
	if timeout != 0 {
		stmt := fmt.Sprintf(`SET statement_timeout = '%s'`, timeout)
		if err := sqlConn.Exec(maybePrint(stmt), nil); err != nil {
			return nil, nil, nil, err
		}
	}
	stmt := `
SELECT id, descriptor, crdb_internal_mvcc_timestamp AS mod_time_logical
FROM system.descriptor ORDER BY id`
	checkColumnExistsStmt := "SELECT crdb_internal_mvcc_timestamp"
	_, err := sqlConn.Query(maybePrint(checkColumnExistsStmt), nil)
	// On versions before 20.2, the system.descriptor won't have the builtin
	// crdb_internal_mvcc_timestamp. If we can't find it, use NULL instead.
	if pqErr := (*pq.Error)(nil); errors.As(err, &pqErr) {
		if pgcode.MakeCode(string(pqErr.Code)) == pgcode.UndefinedColumn {
			stmt = `
SELECT id, descriptor, NULL AS mod_time_logical
FROM system.descriptor ORDER BY id`
		}
	}
	descTable = make([]doctor.DescriptorTableRow, 0)

	if err := selectRowsMap(sqlConn, maybePrint(stmt), make([]driver.Value, 3), func(vals []driver.Value) error {
		var row doctor.DescriptorTableRow
		if id, ok := vals[0].(int64); ok {
			row.ID = id
		} else {
			return errors.Errorf("unexpected value: %T of %v", vals[0], vals[0])
		}
		if descBytes, ok := vals[1].([]byte); ok {
			row.DescBytes = descBytes
		} else {
			return errors.Errorf("unexpected value: %T of %v", vals[1], vals[1])
		}
		if vals[2] == nil {
			row.ModTime = hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
		} else if mt, ok := vals[2].([]byte); ok {
			decimal, _, err := apd.NewFromString(string(mt))
			if err != nil {
				return err
			}
			ts, err := tree.DecimalToHLC(decimal)
			if err != nil {
				return err
			}
			row.ModTime = ts
		} else {
			return errors.Errorf("unexpected value: %T of %v", vals[2], vals[2])
		}
		descTable = append(descTable, row)
		return nil
	}); err != nil {
		return nil, nil, nil, err
	}

	stmt = `SELECT "parentID", "parentSchemaID", name, id FROM system.namespace`

	checkColumnExistsStmt = `SELECT "parentSchemaID" FROM system.namespace LIMIT 0`
	_, err = sqlConn.Query(maybePrint(checkColumnExistsStmt), nil)
	// On versions before 20.1, table system.namespace does not have this column.
	// In that case the ParentSchemaID for tables is 29 and for databases is 0.
	if pqErr := (*pq.Error)(nil); errors.As(err, &pqErr) {
		if pgcode.MakeCode(string(pqErr.Code)) == pgcode.UndefinedColumn {
			stmt = `
SELECT "parentID", CASE WHEN "parentID" = 0 THEN 0 ELSE 29 END AS "parentSchemaID", name, id
FROM system.namespace`
		}
	}

	namespaceTable = make([]doctor.NamespaceTableRow, 0)
	if err := selectRowsMap(sqlConn, maybePrint(stmt), make([]driver.Value, 4), func(vals []driver.Value) error {
		var row doctor.NamespaceTableRow
		if parentID, ok := vals[0].(int64); ok {
			row.ParentID = descpb.ID(parentID)
		} else {
			return errors.Errorf("unexpected value: %T of %v", vals[0], vals[0])
		}
		if schemaID, ok := vals[1].(int64); ok {
			row.ParentSchemaID = descpb.ID(schemaID)
		} else {
			return errors.Errorf("unexpected value: %T of %v", vals[0], vals[0])
		}
		if name, ok := vals[2].(string); ok {
			row.Name = name
		} else {
			return errors.Errorf("unexpected value: %T of %v", vals[1], vals[1])
		}
		if id, ok := vals[3].(int64); ok {
			row.ID = id
		} else {
			return errors.Errorf("unexpected value: %T of %v", vals[0], vals[0])
		}
		namespaceTable = append(namespaceTable, row)
		return nil
	}); err != nil {
		return nil, nil, nil, err
	}

	stmt = `SELECT id, status, payload, progress FROM system.jobs`
	jobsTable = make(doctor.JobsTable, 0)

	if err := selectRowsMap(sqlConn, maybePrint(stmt), make([]driver.Value, 4), func(vals []driver.Value) error {
		md := jobs.JobMetadata{}
		md.ID = jobspb.JobID(vals[0].(int64))
		md.Status = jobs.Status(vals[1].(string))
		md.Payload = &jobspb.Payload{}
		if err := protoutil.Unmarshal(vals[2].([]byte), md.Payload); err != nil {
			return err
		}
		md.Progress = &jobspb.Progress{}
		// Progress is a nullable column, so have to check for nil here.
		progressBytes, ok := vals[3].([]byte)
		if !ok {
			return errors.Errorf("unexpected NULL progress on job row: %v", md)
		}
		if err := protoutil.Unmarshal(progressBytes, md.Progress); err != nil {
			return err
		}
		jobsTable = append(jobsTable, md)
		return nil
	}); err != nil {
		return nil, nil, nil, err
	}

	return descTable, namespaceTable, jobsTable, nil
}

// fromZipDir collects system table data from a decompressed debug zip dir.
func fromZipDir(
	zipDirPath string,
) (
	descTable doctor.DescriptorTable,
	namespaceTable doctor.NamespaceTable,
	jobsTable doctor.JobsTable,
	retErr error,
) {
	// To make parsing user functions code happy.
	_ = builtins.AllBuiltinNames

	maybePrint := func(fileName string) string {
		path := path.Join(zipDirPath, fileName)
		if debugCtx.verbose {
			fmt.Println("reading " + path)
		}
		return path
	}

	descFile, err := os.Open(maybePrint("system.descriptor.txt"))
	if err != nil {
		return nil, nil, nil, err
	}
	defer descFile.Close()
	descTable = make(doctor.DescriptorTable, 0)

	if err := tableMap(descFile, func(cols colInfo, fields []string) error {
		idCol := cols.get("id")
		i, err := strconv.Atoi(fields[idCol])
		if err != nil {
			return errors.Errorf("failed to parse descriptor id %s: %v", fields[idCol], err)
		}

		var descBytes []byte
		if hexDescCol := cols.get("hex_descriptor"); hexDescCol >= 0 {
			if hexDescCol == len(cols)-1 {
				// TODO(knz): This assignment is for compatibility with
				// previous-version zip files, which were capturing the
				// descriptor data using the "escape" byte encoding, thereby
				// introducing spaces and breaking the alignment between
				// "fields" and "cols".
				// Until we drop compatibility with those previous versions,
				// we need to realign the column index with the field,
				// with knowledge that the hex column was the last one.
				// This logic can be dropped at some later release.
				hexDescCol = len(fields) - 1
			}
			descBytes, err = hx.DecodeString(fields[hexDescCol])
			if err != nil {
				return errors.Errorf("failed to decode hex descriptor %d: %v", i, err)
			}
		} else {
			// No hex_descriptor column: descriptor is encoded as base64.
			// TODO(knz): Remove the other branch when only base64 columns are supported.
			descCol := cols.get("descriptor")
			descBytes, err = base64.StdEncoding.DecodeString(fields[descCol])
			if err != nil {
				return errors.Errorf("failed to decode base64 descriptor %d: %v", i, err)
			}
		}

		ts := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
		descTable = append(descTable, doctor.DescriptorTableRow{ID: int64(i), DescBytes: descBytes, ModTime: ts})
		return nil
	}); err != nil {
		return nil, nil, nil, err
	}

	// Handle old debug zips where the namespace table dump is from namespace2.
	namespaceFileName := "system.namespace2.txt"
	if _, err := os.Stat(namespaceFileName); err != nil {
		namespaceFileName = "system.namespace.txt"
	}

	namespaceFile, err := os.Open(maybePrint(namespaceFileName))
	if err != nil {
		return nil, nil, nil, err
	}
	defer namespaceFile.Close()

	namespaceTable = make(doctor.NamespaceTable, 0)
	if err := tableMap(namespaceFile, func(cols colInfo, fields []string) error {
		parentIDCol := cols.get("parentID")
		scIDCol := cols.get("parentSchemaID")
		idCol := cols.get("id")
		parID, err := strconv.Atoi(fields[parentIDCol])
		if err != nil {
			return errors.Errorf("failed to parse parent id %s: %v", fields[parentIDCol], err)
		}
		parSchemaID, err := strconv.Atoi(fields[scIDCol])
		if err != nil {
			return errors.Errorf("failed to parse parent schema id %s: %v", fields[scIDCol], err)
		}
		id, err := strconv.Atoi(fields[idCol])
		if err != nil {
			if fields[idCol] == "NULL" {
				id = int(descpb.InvalidID)
			} else {
				return errors.Errorf("failed to parse id %s: %v", fields[idCol], err)
			}
		}

		nameCol := cols.get("name")
		namespaceTable = append(namespaceTable, doctor.NamespaceTableRow{
			NameInfo: descpb.NameInfo{
				ParentID: descpb.ID(parID), ParentSchemaID: descpb.ID(parSchemaID), Name: fields[nameCol],
			},
			ID: int64(id),
		})
		return nil
	}); err != nil {
		return nil, nil, nil, err
	}

	jobsFile, err := os.Open(maybePrint("system.jobs.txt"))
	if err != nil {
		return nil, nil, nil, err
	}
	defer jobsFile.Close()
	jobsTable = make(doctor.JobsTable, 0)

	if err := tableMap(jobsFile, func(cols colInfo, fields []string) error {
		idCol := cols.get("id")
		statusCol := cols.get("status")
		md := jobs.JobMetadata{}
		md.Status = jobs.Status(fields[statusCol])

		id, err := strconv.Atoi(fields[idCol])
		if err != nil {
			return errors.Errorf("failed to parse job id %s: %v", fields[idCol], err)
		}
		md.ID = jobspb.JobID(id)

		var payloadBytes []byte
		if hexPayloadCol := cols.get("hex_payload"); hexPayloadCol >= 0 {
			if hexPayloadCol == len(cols)-2 {
				// TODO(knz): This assignment is for compatibility with
				// previous-version zip files, which were capturing the
				// descriptor data using the "escape" byte encoding, thereby
				// introducing spaces and breaking the alignment between
				// "fields" and "cols".
				// Until we drop compatibility with those previous versions,
				// we need to realign the column index with the field,
				// with knowledge that the hex column was the next-to-last one.
				// This logic can be dropped at some later release.
				hexPayloadCol = len(fields) - 2
			}
			payloadBytes, err = hx.DecodeString(fields[hexPayloadCol])
			if err != nil {
				return errors.Errorf("job %d: failed to decode hex payload: %v", id, err)
			}
		} else {
			// No hex_payload column: payload is encoded as base64.
			// TODO(knz): Remove the other branch when only base64 columns are supported.
			payloadCol := cols.get("payload")
			payloadBytes, err = base64.StdEncoding.DecodeString(fields[payloadCol])
			if err != nil {
				return errors.Errorf("job %d: failed to decode base64 payload: %v", id, err)
			}
		}

		md.Payload = &jobspb.Payload{}
		if err := protoutil.Unmarshal(payloadBytes, md.Payload); err != nil {
			return errors.Wrap(err, "failed unmarshalling job payload")
		}

		var progressBytes []byte
		if hexProgressCol := cols.get("hex_progress"); hexProgressCol >= 0 {
			if hexProgressCol == len(cols)-1 {
				// TODO(knz): See comment above.
				// This logic can be dropped at some later release.
				hexProgressCol = len(fields) - 1
			}
			progressBytes, err = hx.DecodeString(fields[hexProgressCol])
			if err != nil {
				return errors.Errorf("job %d: failed to decode hex progress: %v", id, err)
			}
		} else {
			// No hex_progress column: progress is encoded as base64.
			// TODO(knz): Remove the other branch when only base64 columns are supported.
			progressCol := cols.get("progress")
			progressBytes, err = base64.StdEncoding.DecodeString(fields[progressCol])
			if err != nil {
				return errors.Errorf("job %d: failed to decode base64 payload: %v", id, err)
			}
		}

		md.Progress = &jobspb.Progress{}
		if err := protoutil.Unmarshal(progressBytes, md.Progress); err != nil {
			return errors.Wrap(err, "failed unmarshalling job progress")
		}

		jobsTable = append(jobsTable, md)
		return nil
	}); err != nil {
		return nil, nil, nil, err
	}

	return descTable, namespaceTable, jobsTable, nil
}

type colInfo map[string]int

func (c colInfo) get(colName string) int {
	res, ok := c[colName]
	if !ok {
		return -1 // cause a panic: column does not exist.
	}
	return res
}

func toColInfo(colRow string) colInfo {
	colNames := strings.Fields(colRow)
	res := make(colInfo, len(colNames))
	for i, colName := range colNames {
		if _, ok := res[colName]; !ok {
			// Only remember the first mention of this column's name.  This
			// way, if the column is repeated, uses of the name will select
			// the first occurrence.
			res[colName] = i
		}
	}
	return res
}

// tableMap applies `fn` to all rows in `in`.
func tableMap(in io.Reader, fn func(colInfo, []string) error) error {
	firstLine := true
	sc := bufio.NewScanner(in)
	// Read lines up to 50 MB in size.
	sc.Buffer(make([]byte, 64*1024), 50*1024*1024)
	var cols colInfo
	for sc.Scan() {
		if firstLine {
			cols = toColInfo(sc.Text())
			firstLine = false
			continue
		}
		if err := fn(cols, strings.Fields(sc.Text())); err != nil {
			return err
		}
	}
	return sc.Err()
}

// selectRowsMap applies `fn` to all rows returned from a select statement.
func selectRowsMap(
	conn clisqlclient.Conn, stmt string, vals []driver.Value, fn func([]driver.Value) error,
) error {
	rows, err := conn.Query(stmt, nil)
	if err != nil {
		return errors.Wrapf(err, "query '%s'", stmt)
	}
	for {
		var err error
		if err = rows.Next(vals); err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if err := fn(vals); err != nil {
			return err
		}
	}
	return nil
}
