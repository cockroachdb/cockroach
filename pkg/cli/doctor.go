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
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/doctor"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
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
Runs various consistency checks over cockroach internal system tables read from
a live cluster or a unzipped debug zip.
`,
}

var debugDoctorCmds = []*cobra.Command{
	doctorZipDirCmd,
	doctorClusterCmd,
}

var doctorZipDirCmd = &cobra.Command{
	Use:   "zipdir <debug_zip_dir>",
	Short: "run doctor tool on data from a directory unzipped from debug.zip",
	Long: `
Run doctor tool on system data from directory	created by unzipping debug.zip.
`,
	Args: cobra.ExactArgs(1),
	RunE: runZipDirDoctor,
}

var doctorClusterCmd = &cobra.Command{
	Use:   "cluster --url=<cluster connection string>",
	Short: "run doctor tool on live cockroach cluster",
	Long: `
Run doctor tool reading system data from a live cluster specified by --url.
`,
	Args: cobra.NoArgs,
	RunE: MaybeDecorateGRPCError(
		func(cmd *cobra.Command, args []string) error {
			sqlConn, err := makeSQLClient("cockroach doctor", useSystemDb)
			if err != nil {
				return errors.Wrap(err, "could not establish connection to cluster")
			}
			defer sqlConn.Close()
			return runClusterDoctor(cmd, args, sqlConn, os.Stdout, cliCtx.cmdTimeout)
		}),
}

func wrapExamine(
	descTable doctor.DescriptorTable,
	namespaceTable doctor.NamespaceTable,
	jobsTable doctor.JobsTable,
	out io.Writer,
) error {
	// TODO(spaskob): add --verbose flag.
	valid, err := doctor.Examine(
		context.Background(), descTable, namespaceTable, jobsTable, false, out)
	if err != nil {
		return &cliError{exitCode: 2, cause: errors.Wrap(err, "examine failed")}
	}
	if !valid {
		return &cliError{exitCode: 1, cause: errors.New("validation failed")}
	}
	fmt.Println("No problems found!")
	return nil
}

// runClusterDoctor runs the doctors tool reading data from a live cluster.
func runClusterDoctor(
	_ *cobra.Command, _ []string, sqlConn *sqlConn, out io.Writer, timeout time.Duration,
) (retErr error) {
	if timeout != 0 {
		if err := sqlConn.Exec(fmt.Sprintf(`SET statement_timeout = '%s'`, timeout), nil); err != nil {
			return err
		}
	}
	stmt := `
SELECT id, descriptor, crdb_internal_mvcc_timestamp AS mod_time_logical
FROM system.descriptor ORDER BY id`
	checkColumnExistsStmt := "SELECT crdb_internal_mvcc_timestamp"
	_, err := sqlConn.Query(checkColumnExistsStmt, nil)
	// On versions before 20.2, the system.descriptor won't have the builtin
	// crdb_internal_mvcc_timestamp. If we can't find it, use NULL instead.
	if pqErr := (*pq.Error)(nil); errors.As(err, &pqErr) {
		if pgcode.MakeCode(string(pqErr.Code)) == pgcode.UndefinedColumn {
			stmt = `
SELECT id, descriptor, NULL AS mod_time_logical
FROM system.descriptor ORDER BY id`
		}
	}
	descTable := make([]doctor.DescriptorTableRow, 0)

	if err := selectRowsMap(sqlConn, stmt, make([]driver.Value, 3), func(vals []driver.Value) error {
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
		return err
	}

	stmt = `SELECT "parentID", "parentSchemaID", name, id FROM system.namespace`

	checkColumnExistsStmt = `SELECT "parentSchemaID" FROM system.namespace LIMIT 0`
	_, err = sqlConn.Query(checkColumnExistsStmt, nil)
	// On versions before 20.1, table system.namespace does not have this column.
	// In that case the ParentSchemaID for tables is 29 and for databases is 0.
	if pqErr := (*pq.Error)(nil); errors.As(err, &pqErr) {
		if pgcode.MakeCode(string(pqErr.Code)) == pgcode.UndefinedColumn {
			stmt = `
SELECT "parentID", CASE WHEN "parentID" = 0 THEN 0 ELSE 29 END AS "parentSchemaID", name, id
FROM system.namespace`
		}
	}

	namespaceTable := make([]doctor.NamespaceTableRow, 0)
	if err := selectRowsMap(sqlConn, stmt, make([]driver.Value, 4), func(vals []driver.Value) error {
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
		return err
	}

	stmt = `SELECT id, status, payload, progress FROM system.jobs`
	jobsTable := make(doctor.JobsTable, 0)

	if err := selectRowsMap(sqlConn, stmt, make([]driver.Value, 4), func(vals []driver.Value) error {
		md := jobs.JobMetadata{}
		md.ID = vals[0].(int64)
		md.Status = jobs.Status(vals[1].(string))
		md.Payload = &jobspb.Payload{}
		if err := protoutil.Unmarshal(vals[2].([]byte), md.Payload); err != nil {
			return err
		}
		md.Progress = &jobspb.Progress{}
		if err := protoutil.Unmarshal(vals[3].([]byte), md.Progress); err != nil {
			return err
		}
		if md.Status == jobs.StatusRunning {
			jobsTable = append(jobsTable, md)
		}
		return nil
	}); err != nil {
		return err
	}

	return wrapExamine(descTable, namespaceTable, jobsTable, out)
}

// runZipDirDoctor runs the doctors tool reading data from a debug zip dir.
func runZipDirDoctor(cmd *cobra.Command, args []string) (retErr error) {
	// To make parsing user functions code happy.
	_ = builtins.AllBuiltinNames

	descFile, err := os.Open(path.Join(args[0], "system.descriptor.txt"))
	if err != nil {
		return err
	}
	defer descFile.Close()
	descTable := make(doctor.DescriptorTable, 0)

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
		return err
	}

	namespaceFile, err := os.Open(path.Join(args[0], "system.namespace2.txt"))
	if err != nil {
		return err
	}
	defer namespaceFile.Close()

	namespaceTable := make(doctor.NamespaceTable, 0)
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
		return err
	}

	jobsFile, err := os.Open(path.Join(args[0], "system.jobs.txt"))
	if err != nil {
		return err
	}
	defer jobsFile.Close()
	jobsTable := make(doctor.JobsTable, 0)

	if err := tableMap(jobsFile, func(row string) error {
		fields := strings.Fields(row)
		md := jobs.JobMetadata{}
		md.Status = jobs.Status(fields[1])
		if md.Status != jobs.StatusRunning {
			return nil
		}

		id, err := strconv.Atoi(fields[0])
		if err != nil {
			return errors.Errorf("failed to parse job id %s: %v", fields[0], err)
		}
		md.ID = int64(id)

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
		return err
	}

	return wrapExamine(descTable, namespaceTable, jobsTable, os.Stdout)
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
