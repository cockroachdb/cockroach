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
	"gopkg.in/yaml.v2"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

func unmarshalProto(val driver.Value, msg protoutil.Message) error {
	raw, ok := val.([]byte)
	if !ok {
		return fmt.Errorf("unexpected value: %T", val)
	}
	return protoutil.Unmarshal(raw, msg)
}

func queryZoneSpecifiers(conn *sqlConn) ([]string, error) {
	rows, err := makeQuery(`SELECT cli_specifier FROM crdb_internal.zones ORDER BY 1`)(conn)
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

		s, ok := vals[0].(string)
		if !ok {
			return nil, fmt.Errorf("unexpected value: %T", vals[0])
		}
		specifiers = append(specifiers, s)
	}
	return specifiers, nil
}

type zoneRow struct {
	cliSpecifier string
	configYAML   []byte
	configProto  []byte
}

func queryZone(conn *sqlConn, id uint32) (zoneRow, error) {
	rows, err := makeQuery(
		`SELECT cli_specifier, config_yaml, config_proto FROM crdb_internal.zones WHERE id = $1`,
		id)(conn)
	if err != nil {
		return zoneRow{}, err
	}
	defer func() { _ = rows.Close() }()

	if len(rows.Columns()) != 3 {
		return zoneRow{}, fmt.Errorf("unexpected result columns: %d", len(rows.Columns()))
	}

	vals := make([]driver.Value, len(rows.Columns()))
	if err := rows.Next(vals); err != nil {
		if err == io.EOF {
			return zoneRow{}, nil
		}
		return zoneRow{}, err
	}

	zr := zoneRow{}
	var ok bool
	zr.cliSpecifier, ok = vals[0].(string)
	if !ok {
		return zoneRow{}, fmt.Errorf("unexpected value: %T", vals[0])
	}
	zr.configYAML, ok = vals[1].([]byte)
	if !ok {
		return zoneRow{}, fmt.Errorf("unexpected value: %T", vals[1])
	}
	zr.configProto, ok = vals[2].([]byte)
	if !ok {
		return zoneRow{}, fmt.Errorf("unexpected value: %T", vals[2])
	}

	return zr, nil
}

func queryZonePath(conn *sqlConn, path []uint32) (zoneRow, error) {
	for i := len(path) - 1; i >= 0; i-- {
		zr, err := queryZone(conn, path[i])
		if err != nil || zr.cliSpecifier != "" {
			return zr, err
		}
	}
	return zoneRow{}, nil
}

func queryNamespace(conn *sqlConn) func(parentID uint32, name string) (uint32, error) {
	return func(parentID uint32, name string) (uint32, error) {
		rows, err := makeQuery(
			`SELECT id FROM system.namespace WHERE "parentID" = $1 AND name = $2`,
			parentID, name)(conn)
		if err != nil {
			return 0, err
		}
		defer func() { _ = rows.Close() }()

		if err != nil {
			return 0, fmt.Errorf("%s not found: %v", name, err)
		}
		if len(rows.Columns()) != 1 {
			return 0, fmt.Errorf("unexpected result columns: %d", len(rows.Columns()))
		}
		vals := make([]driver.Value, 1)
		if err := rows.Next(vals); err != nil {
			return 0, err
		}
		switch t := vals[0].(type) {
		case int64:
			return uint32(t), nil
		default:
			return 0, fmt.Errorf("unexpected result type: %T", vals[0])
		}
	}
}

// A getZoneCmd command displays a zone config.
var getZoneCmd = &cobra.Command{
	Use:   "get [options] <database[.table]>",
	Short: "fetches and displays the zone config",
	Long: `
Fetches and displays the zone configuration for the specified database or
table.
`,
	RunE: MaybeDecorateGRPCError(runGetZone),
}

// runGetZone retrieves the zone config for a given object id,
// and if present, outputs its YAML representation.
func runGetZone(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return usageAndError(cmd)
	}

	zs, err := config.ParseCLIZoneSpecifier(args[0])
	if err != nil {
		return err
	}

	conn, err := getPasswordAndMakeSQLClient()
	if err != nil {
		return err
	}
	defer conn.Close()

	path, err := zs.ResolvePath(queryNamespace(conn))
	if err != nil {
		if err == io.EOF {
			fmt.Printf("%s not found\n", args[0])
			return nil
		}
		return err
	}

	zr, err := queryZonePath(conn, path)
	if err != nil {
		return err
	}
	fmt.Println(zr.cliSpecifier)
	fmt.Print(string(zr.configYAML))
	return nil
}

// A lsZonesCmd command displays a list of zone configs.
var lsZonesCmd = &cobra.Command{
	Use:   "ls [options]",
	Short: "list all zone configs",
	Long: `
List zone configs.
`,
	RunE: MaybeDecorateGRPCError(runLsZones),
}

func runLsZones(cmd *cobra.Command, args []string) error {
	if len(args) > 0 {
		return usageAndError(cmd)
	}
	conn, err := getPasswordAndMakeSQLClient()
	if err != nil {
		return err
	}
	defer conn.Close()

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
	RunE: MaybeDecorateGRPCError(runRmZone),
}

func runRmZone(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return usageAndError(cmd)
	}

	zs, err := config.ParseCLIZoneSpecifier(args[0])
	if err != nil {
		return err
	}

	conn, err := getPasswordAndMakeSQLClient()
	if err != nil {
		return err
	}
	defer conn.Close()

	return conn.ExecTxn(func(conn *sqlConn) error {
		path, err := zs.ResolvePath(queryNamespace(conn))
		if err != nil {
			if err == io.EOF {
				fmt.Printf("%s not found\n", args[0])
				return nil
			}
			return err
		}
		id := path[len(path)-1]
		if id == keys.RootNamespaceID {
			return fmt.Errorf("unable to remove special zone %s", args[0])
		}

		return runQueryAndFormatResults(conn, os.Stdout,
			makeQuery(`DELETE FROM system.zones WHERE id=$1`, id))
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
	if len(args) != 1 {
		return usageAndError(cmd)
	}

	conn, err := getPasswordAndMakeSQLClient()
	if err != nil {
		return err
	}
	defer conn.Close()

	zs, err := config.ParseCLIZoneSpecifier(args[0])
	if err != nil {
		return err
	}

	return conn.ExecTxn(func(conn *sqlConn) error {
		path, err := zs.ResolvePath(queryNamespace(conn))
		if err != nil {
			if err == io.EOF {
				fmt.Printf("%s not found\n", args[0])
				return nil
			}
			return err
		}

		if len(path) > 2 && path[1] == keys.SystemDatabaseID {
			return fmt.Errorf("setting zone configs for individual system tables is not supported; " +
				"try setting your config on the entire \"system\" database instead")
		}

		zr, err := queryZonePath(conn, path)
		if err != nil {
			return err
		}
		zone := config.ZoneConfig{}
		if err := protoutil.Unmarshal([]byte(zr.configProto), &zone); err != nil {
			return err
		}
		// Convert it to proto and marshal it again to put into the table. This is a
		// bit more tedious than taking protos directly, but yaml is a more widely
		// understood format.
		// Read zoneConfig file to conf.

		conf, err := readZoneConfig()
		if err != nil {
			return fmt.Errorf("error reading zone config: %s", err)
		}
		if err := yaml.Unmarshal(conf, &zone); err != nil {
			return fmt.Errorf("unable to parse zoneConfig file: %s", err)
		}

		if err := zone.Validate(); err != nil {
			return err
		}

		buf, err := protoutil.Marshal(&zone)
		if err != nil {
			return fmt.Errorf("unable to parse zone config file %q: %s", args[1], err)
		}

		id := path[len(path)-1]
		_, _, err = runQuery(conn, makeQuery(
			`UPSERT INTO system.zones (id, config) VALUES ($1, $2)`,
			id, buf), false)
		if err != nil {
			return err
		}

		res, err := yaml.Marshal(zone)
		if err != nil {
			return err
		}
		fmt.Print(string(res))
		return nil
	})
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
