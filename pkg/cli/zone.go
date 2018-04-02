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
	"fmt"
	"io"
	"io/ioutil"
	"os"

	"github.com/spf13/cobra"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/sql/lex"
)

func queryZoneSpecifiers(conn *sqlConn) ([]string, error) {
	rows, err := makeQuery(
		`SELECT cli_specifier FROM [EXPERIMENTAL SHOW ZONE CONFIGURATIONS] ORDER BY cli_specifier`,
	)(conn)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	vals := make([]driver.Value, len(rows.Columns()))
	specifiers := []string{}

	for {
		if err := rows.Next(vals); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		if vals[0] == nil {
			// Zone configs for deleted tables and partitions are left around until
			// their table descriptors are deleted, which happens after the
			// configured GC TTL duration. Such zones have no cli_specifier and
			// shouldn't be displayed.
			continue
		}

		s, ok := vals[0].(string)
		if !ok {
			return nil, fmt.Errorf("unexpected value: %T", vals[0])
		}
		specifiers = append(specifiers, s)
	}
	return specifiers, nil
}

// A getZoneCmd command displays a zone config.
var getZoneCmd = &cobra.Command{
	Use:   "get [options] <database[.table]>",
	Short: "fetches and displays the zone config",
	Long: `
Fetches and displays the zone configuration for the specified database or
table.
`,
	Args: cobra.ExactArgs(1),
	RunE: MaybeDecorateGRPCError(runGetZone),
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
	if err := conn.requireServerVersion(">=v1.2-alpha.20171026"); err != nil {
		return err
	}

	vals, err := conn.QueryRow(fmt.Sprintf(
		`SELECT cli_specifier, config_yaml FROM [EXPERIMENTAL SHOW ZONE CONFIGURATION FOR %s]`, &zs), nil)
	if err != nil {
		return err
	}

	cliSpecifier, ok := vals[0].(string)
	if !ok {
		return fmt.Errorf("unexpected result type: %T", vals[0])
	}

	configYAML, ok := vals[1].([]byte)
	if !ok {
		return fmt.Errorf("unexpected result type: %T", vals[0])
	}

	fmt.Println(cliSpecifier)
	fmt.Printf("%s", configYAML)
	return nil
}

// A lsZonesCmd command displays a list of zone configs.
var lsZonesCmd = &cobra.Command{
	Use:   "ls [options]",
	Short: "list all zone configs",
	Long: `
List zone configs.
`,
	Args: cobra.NoArgs,
	RunE: MaybeDecorateGRPCError(runLsZones),
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
	if err := conn.requireServerVersion(">=v1.2-alpha.20171026"); err != nil {
		return err
	}

	specifiers, err := queryZoneSpecifiers(conn)
	if err != nil {
		return err
	}
	if len(specifiers) == 0 {
		fmt.Printf("No zones found\n")
		return nil
	}
	for _, s := range specifiers {
		fmt.Println(s)
	}
	return nil
}

// A rmZoneCmd command removes a zone config.
var rmZoneCmd = &cobra.Command{
	Use:   "rm [options] <database[.table]>",
	Short: "remove a zone config",
	Long: `
Remove an existing zone config for the specified database or table.
`,
	Args: cobra.ExactArgs(1),
	RunE: MaybeDecorateGRPCError(runRmZone),
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
	if err := conn.requireServerVersion(">=v1.2-alpha.20171026"); err != nil {
		return err
	}

	zs, err := config.ParseCLIZoneSpecifier(args[0])
	if err != nil {
		return err
	}

	return runQueryAndFormatResults(conn, os.Stdout,
		makeQuery(fmt.Sprintf(`ALTER %s EXPERIMENTAL CONFIGURE ZONE NULL`, &zs)))
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
	Args: cobra.ExactArgs(1),
	RunE: MaybeDecorateGRPCError(runSetZone),
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
	if err := conn.requireServerVersion(">=v1.2-alpha.20171026"); err != nil {
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

	err = conn.ExecTxn(func(conn *sqlConn) error {
		if err := conn.Exec(fmt.Sprintf(`ALTER %s EXPERIMENTAL CONFIGURE ZONE %s`, &zs,
			lex.EscapeSQLString(string(configYAML))), nil); err != nil {
			return err
		}
		vals, err := conn.QueryRow(fmt.Sprintf(
			`SELECT config_yaml FROM [EXPERIMENTAL SHOW ZONE CONFIGURATION FOR %s]`, &zs), nil)
		if err != nil {
			return err
		}
		var ok bool
		configYAML, ok = vals[0].([]byte)
		if !ok {
			return fmt.Errorf("unexpected result type: %T", vals[0])
		}
		return nil
	})
	if err != nil {
		return err
	}

	fmt.Printf("%s", configYAML)
	return nil
}

var zoneCmds = []*cobra.Command{
	getZoneCmd,
	lsZonesCmd,
	rmZoneCmd,
	setZoneCmd,
}

var zoneCmd = &cobra.Command{
	Use:   "zone",
	Short: "get, set, list and remove zones",
	RunE: func(cmd *cobra.Command, args []string) error {
		return cmd.Usage()
	},
}

func init() {
	zoneCmd.AddCommand(zoneCmds...)
}
