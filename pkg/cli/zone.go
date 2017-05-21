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
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)
// Author: Bram Gruneir (bram+code@cockroachlabs.com)

package cli

import (
	"database/sql/driver"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strings"

	"github.com/gogo/protobuf/proto"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

const (
	defaultZoneName    = ".default"
	metaZoneName       = ".meta"
	systemZoneName     = ".system"
	timeseriesZoneName = ".timeseries"
)

var specialZonesByID = map[sqlbase.ID]string{
	keys.RootNamespaceID:    defaultZoneName,
	keys.MetaRangesID:       metaZoneName,
	keys.SystemRangesID:     systemZoneName,
	keys.TimeseriesRangesID: timeseriesZoneName,
}

func unmarshalProto(val driver.Value, msg proto.Message) error {
	raw, ok := val.([]byte)
	if !ok {
		return fmt.Errorf("unexpected value: %T", val)
	}
	return proto.Unmarshal(raw, msg)
}

func queryZones(conn *sqlConn) (map[sqlbase.ID]config.ZoneConfig, error) {
	rows, err := makeQuery(`SELECT * FROM system.zones`)(conn)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	vals := make([]driver.Value, len(rows.Columns()))
	zones := make(map[sqlbase.ID]config.ZoneConfig)

	for {
		if err := rows.Next(vals); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		id, ok := vals[0].(int64)
		if !ok {
			return nil, fmt.Errorf("unexpected value: %T", vals[0])
		}
		zone := config.ZoneConfig{}
		if err := unmarshalProto(vals[1], &zone); err != nil {
			return nil, err
		}
		zones[sqlbase.ID(id)] = zone
	}
	return zones, nil
}

func queryZone(conn *sqlConn, id sqlbase.ID) (config.ZoneConfig, bool, error) {
	rows, err := makeQuery(`SELECT config FROM system.zones WHERE id = $1`, id)(conn)
	if err != nil {
		return config.ZoneConfig{}, false, err
	}
	defer func() { _ = rows.Close() }()

	if len(rows.Columns()) != 1 {
		return config.ZoneConfig{}, false, fmt.Errorf("unexpected result columns: %d", len(rows.Columns()))
	}

	vals := make([]driver.Value, 1)
	if err := rows.Next(vals); err != nil {
		if err == io.EOF {
			return config.ZoneConfig{}, false, nil
		}
		return config.ZoneConfig{}, false, err
	}

	var zone config.ZoneConfig
	return zone, true, unmarshalProto(vals[0], &zone)
}

func queryZonePath(conn *sqlConn, path []sqlbase.ID) (sqlbase.ID, config.ZoneConfig, error) {
	for i := len(path) - 1; i >= 0; i-- {
		zone, found, err := queryZone(conn, path[i])
		if err != nil || found {
			return path[i], zone, err
		}
	}
	return 0, config.ZoneConfig{}, nil
}

func queryDescriptors(conn *sqlConn) (map[sqlbase.ID]*sqlbase.Descriptor, error) {
	rows, err := makeQuery(`SELECT descriptor FROM system.descriptor`)(conn)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	vals := make([]driver.Value, len(rows.Columns()))
	descs := map[sqlbase.ID]*sqlbase.Descriptor{}

	for {
		if err := rows.Next(vals); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		desc := &sqlbase.Descriptor{}
		if err := unmarshalProto(vals[0], desc); err != nil {
			return nil, err
		}
		descs[desc.GetID()] = desc
	}

	return descs, nil
}

func queryNamespace(conn *sqlConn, parentID sqlbase.ID, name string) (sqlbase.ID, error) {
	rows, err := makeQuery(
		`SELECT id FROM system.namespace WHERE parentID = $1 AND name = $2`,
		parentID, parser.Name(name).Normalize())(conn)
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
		return sqlbase.ID(t), nil
	default:
		return 0, fmt.Errorf("unexpected result type: %T", vals[0])
	}
}

func queryDescriptorIDPath(conn *sqlConn, names []string) ([]sqlbase.ID, error) {
	path := []sqlbase.ID{keys.RootNamespaceID}
	switch strings.Join(names, ".") {
	case defaultZoneName:
		return path, nil
	case metaZoneName:
		return append(path, keys.MetaRangesID), nil
	case systemZoneName:
		return append(path, keys.SystemRangesID), nil
	case timeseriesZoneName:
		return append(path, keys.TimeseriesRangesID), nil
	}
	for _, name := range names {
		id, err := queryNamespace(conn, path[len(path)-1], name)
		if err != nil {
			return nil, err
		}
		path = append(path, id)
	}
	return path, nil
}

func parseZoneName(s string) ([]string, error) {
	switch t := strings.ToLower(s); s {
	case defaultZoneName, metaZoneName, timeseriesZoneName, systemZoneName:
		return []string{t}, nil
	}

	// TODO(knz): we are passing a name that might not be escaped correctly.
	// See #8389.
	tn, err := parser.ParseTableName(s)
	if err != nil {
		return nil, fmt.Errorf("malformed name: %s", s)
	}
	// This is a bit of a hack: "." is not a valid database name.
	// We use this to detect when a database name was not specified, in
	// which case we interpret the table name as a database name below.
	if err := tn.QualifyWithDatabase("."); err != nil {
		return nil, err
	}
	var names []string
	if n := tn.Database(); n != "." {
		names = append(names, n)
	}
	names = append(names, tn.Table())
	return names, nil
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

	names, err := parseZoneName(args[0])
	if err != nil {
		return err
	}

	conn, err := getPasswordAndMakeSQLClient()
	if err != nil {
		return err
	}
	defer conn.Close()

	path, err := queryDescriptorIDPath(conn, names)
	if err != nil {
		if err == io.EOF {
			fmt.Printf("%s not found\n", args[0])
			return nil
		}
		return err
	}

	id, zone, err := queryZonePath(conn, path)
	if err != nil {
		return err
	}

	if zoneName, ok := specialZonesByID[id]; ok {
		fmt.Println(zoneName)
	} else {
		for i := range path {
			if path[i] == id {
				fmt.Println(strings.Join(names[:i], "."))
				break
			}
		}
	}

	res, err := yaml.Marshal(zone)
	if err != nil {
		return err
	}
	fmt.Print(string(res))
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

	zones, err := queryZones(conn)
	if err != nil {
		return err
	}
	if len(zones) == 0 {
		fmt.Printf("No zones found\n")
		return nil
	}

	// TODO(pmattis): This is inefficient. Instead of querying for all of the
	// descriptors in the system, we could query for only those identified by
	// zones. We'd also need to do a second query to retrieve all of the database
	// descriptors referred to by table descriptors.
	descs, err := queryDescriptors(conn)
	if err != nil {
		return err
	}

	// Loop over the zones and determine the name for each based on the name of
	// the corresponding descriptor.
	var output []string
	for id := range zones {
		if id == 0 {
			// We handle the default zone below.
			continue
		}
		desc, ok := descs[id]
		if !ok {
			continue
		}
		var name string
		if tableDesc := desc.GetTable(); tableDesc != nil {
			dbDesc, ok := descs[tableDesc.ParentID]
			if !ok {
				continue
			}
			name = parser.Name(dbDesc.GetName()).String() + "."
		}
		name += parser.Name(desc.GetName()).String()
		output = append(output, name)
	}

	for id, zoneName := range specialZonesByID {
		if _, ok := zones[id]; ok {
			output = append(output, zoneName)
		}
	}

	// Ensure the system zones are always printed first.
	sort.Strings(output)
	for _, o := range output {
		fmt.Println(o)
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

	names, err := parseZoneName(args[0])
	if err != nil {
		return err
	}

	conn, err := getPasswordAndMakeSQLClient()
	if err != nil {
		return err
	}
	defer conn.Close()

	return conn.ExecTxn(func(conn *sqlConn) error {
		path, err := queryDescriptorIDPath(conn, names)
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

		if err := runQueryAndFormatResults(conn, os.Stdout,
			makeQuery(`DELETE FROM system.zones WHERE id=$1`, id), cliCtx.tableDisplayFormat); err != nil {
			return err
		}

		return nil
	})
}

// A setZoneCmd command creates a new or updates an existing zone config.
var setZoneCmd = &cobra.Command{
	Use:   "set [options] <database[.table]> <zone-config>",
	Short: "create or update zone config for object ID",
	Long: `
Create or update the zone config for the specified database or table to the
specified zone-config.

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
	if zoneDisableReplication {
		if zoneConfig != "" {
			return nil, fmt.Errorf("cannot specify --disable-replication and -f at the same time")
		}
		conf = []byte("num_replicas: 1")
	} else {
		switch zoneConfig {
		case "":
			err = fmt.Errorf("no filename specified with -f")
		case "-":
			conf, err = ioutil.ReadAll(os.Stdin)
		default:
			conf, err = ioutil.ReadFile(zoneConfig)
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

	names, err := parseZoneName(args[0])
	if err != nil {
		return err
	}

	return conn.ExecTxn(func(conn *sqlConn) error {
		path, err := queryDescriptorIDPath(conn, names)
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

		_, zone, err := queryZonePath(conn, path)
		if err != nil {
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
		_, _, _, err = runQuery(conn, makeQuery(
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
