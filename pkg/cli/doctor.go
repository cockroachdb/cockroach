// Copyright 2017 The Cockroach Authors.
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
	"database/sql/driver"
	hx "encoding/hex"
	"io"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/sql/doctor"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

var debugDoctorCmd = &cobra.Command{
	Use:   "doctor [command]",
	Short: "run a cockroach doctor tool command",
	Long: `
Allows to doctor a live cluster, debug zip or a backup, or run other consistency
checking tools.
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
Run doctor tool on system data from directory	created by unzipping debug.zip
`,
	Args: cobra.ExactArgs(1),
	RunE: RunZipDirDoctor,
}

var doctorClusterCmd = &cobra.Command{
	Use:   "cluster --url=<cluster connection string>",
	Short: "run doctor tool on live cockroach cluster",
	Long: `
Run doctor tool reading system data from a live cluster specified by --url
`,
	Args: cobra.NoArgs,
	RunE: MaybeDecorateGRPCError(RunClusterDoctor),
}

// RunClusterDoctor runs the doctors tool reading data from a live cluster.
func RunClusterDoctor(cmd *cobra.Command, args []string) (retErr error) {
	sqlConn, err := makeSQLClient("cockroach doctor", useSystemDb)
	if err != nil {
		return err
	}
	defer sqlConn.Close()

	rows, err := sqlConn.Query(`
SELECT id, descriptor, mod_time_logical
FROM system.descriptor LEFT OUTER JOIN crdb_internal.tables ON (table_id = id)
ORDER BY id`,
		nil,
	)
	if err != nil {
		return err
	}

	descTable := make([]doctor.DescriptorTableRow, 0)
	vals := make([]driver.Value, 3)
	for {
		if err := rows.Next(vals); err == io.EOF {
			break
		}
		var row doctor.DescriptorTableRow
		if id, ok := vals[0].(int64); ok {
			row.ID = id
		} else {
			return errors.Errorf("unexpected value: %T", id)
		}
		if descBytes, ok := vals[1].([]byte); ok {
			row.DescBytes = descBytes
		} else {
			return errors.Errorf("unexpected value: %T", descBytes)
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
			return errors.Errorf("unexpected value: %T of %+v", mt, vals[2])
		}
		descTable = append(descTable, row)
	}

	doctor.Examine(descTable, false)
	return nil
}

// RunZipDirDoctor runs the doctors tool reading data from a debug zip dir.
func RunZipDirDoctor(cmd *cobra.Command, args []string) (retErr error) {
	// To make parsing user functions happy.
	_ = builtins.AllBuiltinNames

	file, err := os.Open(path.Join(args[0], "system.descriptor.txt"))
	if err != nil {
		return err
	}
	defer file.Close()

	descTable := make([]doctor.DescriptorTableRow, 0)
	sc := bufio.NewScanner(file)
	firstLine := true
	for sc.Scan() {
		if firstLine {
			firstLine = false
			continue
		}
		fields := strings.Fields(sc.Text())
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
	}

	// TODO(spaskob): add --verbose flag.
	doctor.Examine(descTable, false)
	return nil
}
