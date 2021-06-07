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
	hx "encoding/hex"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/apd/v2"
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
			func(cmd *cobra.Command, args []string) error {
				sqlConn, err := makeSQLClient("cockroach doctor", useSystemDb)
				if err != nil {
					return errors.Wrap(err, "could not establish connection to cluster")
				}
				defer sqlConn.Close()
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
				return &cliError{
					exitCode: exit.DoctorValidationFailed(),
					cause:    errors.New("validation failed"),
				}
			}
			fmt.Fprintln(out, "No problems found!")
		}
	default:
		log.Fatalf(context.Background(), "Unexpected doctor command %q.", commandName)
	}
	if err == nil {
		return nil
	}
	return &cliError{
		// Note: we are using "unspecified" here because the error
		// return does not distinguish errors like connection errors
		// etc, from errors during extraction.
		exitCode: exit.UnspecifiedError(),
		cause:    errors.Wrapf(err, "doctor command %q failed", commandName),
	}
}

// fromCluster collects system table data from a live cluster.
func fromCluster(
	sqlConn *sqlConn, timeout time.Duration,
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

	if err := tableMap(descFile, func(row string) error {
		fields := strings.Fields(row)
		last := len(fields) - 1
		i, err := strconv.Atoi(fields[0])
		if err != nil {
			return errors.Errorf("failed to parse descriptor id %s: %v", fields[0], err)
		}

		descBytes, err := hx.DecodeString(fields[last])
		if err != nil {
			return errors.Errorf("failed to decode hex descriptor %d: %v", i, err)
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
	if err := tableMap(namespaceFile, func(row string) error {
		fields := strings.Fields(row)
		parID, err := strconv.Atoi(fields[0])
		if err != nil {
			return errors.Errorf("failed to parse parent id %s: %v", fields[0], err)
		}
		parSchemaID, err := strconv.Atoi(fields[1])
		if err != nil {
			return errors.Errorf("failed to parse parent schema id %s: %v", fields[1], err)
		}
		id, err := strconv.Atoi(fields[3])
		if err != nil {
			if fields[3] == "NULL" {
				id = int(descpb.InvalidID)
			} else {
				return errors.Errorf("failed to parse id %s: %v", fields[3], err)
			}
		}

		namespaceTable = append(namespaceTable, doctor.NamespaceTableRow{
			NameInfo: descpb.NameInfo{
				ParentID: descpb.ID(parID), ParentSchemaID: descpb.ID(parSchemaID), Name: fields[2],
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

	if err := tableMap(jobsFile, func(row string) error {
		fields := strings.Fields(row)
		md := jobs.JobMetadata{}
		md.Status = jobs.Status(fields[1])

		id, err := strconv.Atoi(fields[0])
		if err != nil {
			return errors.Errorf("failed to parse job id %s: %v", fields[0], err)
		}
		md.ID = jobspb.JobID(id)

		last := len(fields) - 1
		payloadBytes, err := hx.DecodeString(fields[last-1])
		if err != nil {
			return errors.Errorf("job %d: failed to decode hex payload: %v", id, err)
		}
		md.Payload = &jobspb.Payload{}
		if err := protoutil.Unmarshal(payloadBytes, md.Payload); err != nil {
			return errors.Wrap(err, "failed unmarshalling job payload")
		}
		progressBytes, err := hx.DecodeString(fields[last])
		if err != nil {
			return errors.Errorf("job %d: failed to decode hex progress: %v", id, err)
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

// tableMap applies `fn` to all rows in `in`.
func tableMap(in io.Reader, fn func(string) error) error {
	firstLine := true
	sc := bufio.NewScanner(in)
	// Read lines up to 50 MB in size.
	sc.Buffer(make([]byte, 64*1024), 50*1024*1024)
	for sc.Scan() {
		if firstLine {
			firstLine = false
			continue
		}
		if err := fn(sc.Text()); err != nil {
			return err
		}
	}
	return sc.Err()
}

// selectRowsMap applies `fn` to all rows returned from a select statement.
func selectRowsMap(
	conn *sqlConn, stmt string, vals []driver.Value, fn func([]driver.Value) error,
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
