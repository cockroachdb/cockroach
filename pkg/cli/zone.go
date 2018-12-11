// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package cli

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/spf13/cobra"
)

type runQueryRawFn func(q string, parameters ...driver.Value) ([]string, [][]string, error)

var verZones = version.MustParse("v1.2.0-alpha.20171026")

// runQueryRawMaybeExperimental tries to run the query without the
// experimental keyword, and if that fails with a syntax error, with
// it. The placement of the experimental keyword must be marked with %[1]s in
// the string. This is intended for backward-compatibility.
// TODO(knz): Remove this post-2.2.
func runQueryRawMaybeExperimental(conn *sqlConn, txnFn func(runQuery runQueryRawFn) error) error {
	withExecute := ""
	eqSign := "="
	runQueryFn := func(q string, parameters ...driver.Value) ([]string, [][]string, error) {
		return runQueryRaw(conn, makeQuery(fmt.Sprintf(q, withExecute, eqSign), parameters...))
	}
	queryFn := func(_ *sqlConn) error { return txnFn(runQueryFn) }

	err := conn.ExecTxn(queryFn)
	if err != nil && strings.Contains(err.Error(), "syntax error") {
		withExecute = "EXPERIMENTAL"
		eqSign = ""
		err = conn.ExecTxn(queryFn)
	}
	return err
}

// A getZoneCmd command displays a zone config.
var getZoneCmd = &cobra.Command{
	Use:   "get [options] <database[.table]>",
	Short: "fetches and displays the zone config",
	Long: `
Fetches and displays the zone configuration for the specified database or
table.
`,
	Args:       cobra.ExactArgs(1),
	RunE:       MaybeDecorateGRPCError(runGetZone),
	Deprecated: "use SHOW ZONE CONFIGURATION FOR ... in a SQL client instead.",
}

// runGetZone retrieves the zone config for a given object id,
// and if present, outputs its YAML representation.
func runGetZone(cmd *cobra.Command, args []string) error {
	zs, err := config.ParseCLIZoneSpecifier(args[0])
	if err != nil {
		return err
	}

	conn, err := getPasswordAndMakeSQLClient("cockroach zone")
	if err != nil {
		return err
	}
	defer conn.Close()

	// NOTE: We too aggressively broke backwards compatibility in this command.
	// Future changes should maintain compatibility with the last two released
	// versions of CockroachDB.
	if err := conn.requireServerVersion(verZones); err != nil {
		return err
	}

	var vals [][]string
	if err := runQueryRawMaybeExperimental(conn,
		func(runQuery runQueryRawFn) (err error) {
			vals, err = getCLISpecifierAndZoneConf(&zs, runQuery)
			return
		}); err != nil {
		return err
	}

	return printCLISpecifierAndZoneConf(&zs, vals)
}

func getCLISpecifierAndZoneConf(
	zs *tree.ZoneSpecifier, runQuery runQueryRawFn,
) ([][]string, error) {
	_, vals, err := runQuery(fmt.Sprintf(
		`SELECT cli_specifier, config_yaml FROM [%%[1]s SHOW ZONE CONFIGURATION FOR %s]`,
		zs))
	return vals, err
}

func printCLISpecifierAndZoneConf(zs *tree.ZoneSpecifier, vals [][]string) error {
	if len(vals) == 0 {
		return fmt.Errorf("no zone configuration found for %s",
			config.CLIZoneSpecifier(zs))
	}
	cliSpecifier := vals[0][0]
	configYAML := vals[0][1]
	fmt.Println(cliSpecifier)
	fmt.Print(configYAML)
	return nil
}

// A lsZonesCmd command displays a list of zone configs.
var lsZonesCmd = &cobra.Command{
	Use:   "ls [options]",
	Short: "list all zone configs",
	Long: `
List zone configs.
`,
	Args:       cobra.NoArgs,
	RunE:       MaybeDecorateGRPCError(runLsZones),
	Deprecated: "use SHOW ZONE CONFIGURATIONS in a SQL client instead.",
}

func runLsZones(cmd *cobra.Command, args []string) error {
	conn, err := getPasswordAndMakeSQLClient("cockroach zone")
	if err != nil {
		return err
	}
	defer conn.Close()

	// NOTE: We too aggressively broke backwards compatibility in this command.
	// Future changes should maintain compatibility with the last two released
	// versions of CockroachDB.
	if err := conn.requireServerVersion(verZones); err != nil {
		return err
	}

	var vals [][]string
	if err := runQueryRawMaybeExperimental(conn,
		func(runQuery runQueryRawFn) (err error) {
			_, vals, err = runQuery(
				`SELECT cli_specifier AS zone
           FROM [%[1]s SHOW ZONE CONFIGURATIONS]
          WHERE cli_specifier IS NOT NULL
       ORDER BY cli_specifier`)
			return
		}); err != nil {
		return err
	}
	dumpQueryResultsFlat(vals)
	return nil
}

func dumpQueryResultsFlat(rows [][]string) {
	for _, row := range rows {
		comma := ""
		for _, col := range row {
			fmt.Print(comma)
			fmt.Print(col)
			comma = " "
		}
		fmt.Println()
	}
}

// A rmZoneCmd command removes a zone config.
var rmZoneCmd = &cobra.Command{
	Use:   "rm [options] <database[.table]>",
	Short: "remove a zone config",
	Long: `
Remove an existing zone config for the specified database or table.
`,
	Args:       cobra.ExactArgs(1),
	RunE:       MaybeDecorateGRPCError(runRmZone),
	Deprecated: "use ALTER ... CONFIGURE ZONE DISCARD in a SQL client instead.",
}

func runRmZone(cmd *cobra.Command, args []string) error {
	conn, err := getPasswordAndMakeSQLClient("cockroach zone")
	if err != nil {
		return err
	}
	defer conn.Close()

	// NOTE: We too aggressively broke backwards compatibility in this command.
	// Future changes should maintain compatibility with the last two released
	// versions of CockroachDB.
	if err := conn.requireServerVersion(verZones); err != nil {
		return err
	}

	zs, err := config.ParseCLIZoneSpecifier(args[0])
	if err != nil {
		return err
	}

	return runQueryRawMaybeExperimental(conn,
		func(runQuery runQueryRawFn) (err error) {
			_, _, err = runQuery(fmt.Sprintf(
				`ALTER %s %%[1]s CONFIGURE ZONE %%[2]s NULL`, &zs))
			return
		})
}

// A setZoneCmd command creates a new or updates an existing zone config.
var setZoneCmd = &cobra.Command{
	Use:   "set [options] <database[.table]> -f file.yaml",
	Short: "create or update zone config for object ID",
	Long: `
Create or update the zone config for the specified database or table to the
specified zone-config from the given file ("-" for stdin).

The zone config format has the following YAML schema:

  num_replicas: <num>
  constraints: [comma-separated attribute list]
  range_min_bytes: <size-in-bytes>
  range_max_bytes: <size-in-bytes>
  gc:
    ttlseconds: <time-in-seconds>

For example, to set the zone config for the system database, run:
$ cockroach zone set system -f - << EOF
num_replicas: 3
constraints: [ssd, -mem]
EOF

Note that the specified zone config is merged with the existing zone config for
the database or table.
`,
	Args:       cobra.ExactArgs(1),
	RunE:       MaybeDecorateGRPCError(runSetZone),
	Deprecated: "use ALTER ... CONFIGURE ZONE in a SQL client instead.",
}

func readZoneConfig() (conf []byte, err error) {
	if zoneCtx.zoneDisableReplication {
		if zoneCtx.zoneConfig != "" {
			return nil, fmt.Errorf("cannot specify --disable-replication and -f at the same time")
		}
		conf = []byte("num_replicas: 1")
	} else {
		switch zoneCtx.zoneConfig {
		case "":
			err = fmt.Errorf("no filename specified with -f")
		case "-":
			conf, err = ioutil.ReadAll(os.Stdin)
		default:
			conf, err = ioutil.ReadFile(zoneCtx.zoneConfig)
		}
	}
	return conf, err
}

// runSetZone parses the yaml input file, converts it to proto, and inserts it
// in the system.zones table.
func runSetZone(cmd *cobra.Command, args []string) error {
	conn, err := getPasswordAndMakeSQLClient("cockroach zone")
	if err != nil {
		return err
	}
	defer conn.Close()

	// NOTE: We too aggressively broke backwards compatibility in this command.
	// Future changes should maintain compatibility with the last two released
	// versions of CockroachDB.
	if err := conn.requireServerVersion(verZones); err != nil {
		return err
	}

	zs, err := config.ParseCLIZoneSpecifier(args[0])
	if err != nil {
		return err
	}

	configYAML, err := readZoneConfig()
	if err != nil {
		return err
	}

	var vals [][]string
	if err := runQueryRawMaybeExperimental(conn,
		func(runQuery runQueryRawFn) (err error) {
			if _, _, err := runQuery(fmt.Sprintf(
				`ALTER %s %%[1]s CONFIGURE ZONE %%[2]s %s`,
				&zs, lex.EscapeSQLString(string(configYAML)))); err != nil {
				return err
			}
			vals, err = getCLISpecifierAndZoneConf(&zs, runQuery)
			return
		}); err != nil {
		return err
	}

	if len(vals) == 0 {
		return errors.New("zone configuration disappeared during rm")
	}
	fmt.Println(vals[0][1])
	return nil
}

var zoneCmds = []*cobra.Command{
	getZoneCmd,
	lsZonesCmd,
	rmZoneCmd,
	setZoneCmd,
}

var zoneCmd = &cobra.Command{
	Use:        "zone",
	Short:      "get, set, list and remove zones",
	RunE:       usageAndErr,
	Deprecated: "use SHOW ZONE and CONFIGURE ZONE commands in a SQL client instead.",
}

func init() {
	zoneCmd.AddCommand(zoneCmds...)
}
