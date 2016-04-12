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
	"os"
	"sort"
	"strings"

	"github.com/gogo/protobuf/proto"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v1"

	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util/protoutil"
)

func unmarshalProto(val driver.Value, msg proto.Message) error {
	raw, ok := val.([]byte)
	if !ok {
		return fmt.Errorf("unexpected value: %T", val)
	}
	return proto.Unmarshal(raw, msg)
}

func queryZones(conn *sqlConn) (map[sql.ID]config.ZoneConfig, error) {
	rows, err := makeQuery(`SELECT * FROM system.zones`)(conn)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	vals := make([]driver.Value, len(rows.Columns()))
	zones := make(map[sql.ID]config.ZoneConfig)

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
		zones[sql.ID(id)] = zone
	}
	return zones, nil
}

func queryZone(conn *sqlConn, id sql.ID) (*config.ZoneConfig, error) {
	rows, err := makeQuery(`SELECT config FROM system.zones WHERE id = $1`, id)(conn)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	if len(rows.Columns()) != 1 {
		return nil, fmt.Errorf("unexpected result columns: %d", len(rows.Columns()))
	}

	vals := make([]driver.Value, 1)
	if err := rows.Next(vals); err != nil {
		if err == io.EOF {
			return nil, nil
		}
		return nil, err
	}

	zone := &config.ZoneConfig{}
	if err := unmarshalProto(vals[0], zone); err != nil {
		return nil, err
	}
	return zone, nil
}

func queryZonePath(conn *sqlConn, path []sql.ID) (sql.ID, *config.ZoneConfig, error) {
	for i := len(path) - 1; i >= 0; i-- {
		zone, err := queryZone(conn, path[i])
		if err != nil || zone != nil {
			return path[i], zone, err
		}
	}
	return 0, nil, nil
}

func queryDescriptors(conn *sqlConn) (map[sql.ID]*sql.Descriptor, error) {
	rows, err := makeQuery(`SELECT descriptor FROM system.descriptor`)(conn)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	vals := make([]driver.Value, len(rows.Columns()))
	descs := map[sql.ID]*sql.Descriptor{}

	for {
		if err := rows.Next(vals); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		desc := &sql.Descriptor{}
		if err := unmarshalProto(vals[0], desc); err != nil {
			return nil, err
		}
		descs[desc.GetID()] = desc
	}

	return descs, nil
}

func queryNamespace(conn *sqlConn, parentID sql.ID, name string) (sql.ID, error) {
	rows, err := makeQuery(
		`SELECT id FROM system.namespace WHERE parentID = $1 AND name = $2`,
		parentID, sql.NormalizeName(name))(conn)
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
		return sql.ID(t), nil
	default:
		return 0, fmt.Errorf("unexpected result type: %T", vals[0])
	}
}

func queryDescriptorIDPath(conn *sqlConn, names []string) ([]sql.ID, error) {
	path := []sql.ID{keys.RootNamespaceID}
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
	if strings.ToLower(s) == ".default" {
		return nil, nil
	}
	expr, err := parser.ParseExprTraditional(s)
	if err != nil {
		return nil, fmt.Errorf("malformed name: %s", s)
	}
	qname, ok := expr.(*parser.QualifiedName)
	if !ok {
		return nil, fmt.Errorf("malformed name: %s", s)
	}
	// This is a bit of a hack: "." is not a valid database name which we use as
	// a placeholder in order to normalize the qualified name.
	if err := qname.NormalizeTableName("."); err != nil {
		return nil, err
	}
	var names []string
	if n := qname.Database(); n != "." {
		names = append(names, n)
	}
	names = append(names, qname.Table())
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
	SilenceUsage: true,
	RunE:         runGetZone,
}

// runGetZone retrieves the zone config for a given object id,
// and if present, outputs its YAML representation.
func runGetZone(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		mustUsage(cmd)
		return nil
	}

	names, err := parseZoneName(args[0])
	if err != nil {
		return err
	}

	conn, err := makeSQLClient()
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

	if id == 0 {
		fmt.Println(".default")
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
	SilenceUsage: true,
	RunE:         runLsZones,
}

func runLsZones(cmd *cobra.Command, args []string) error {
	if len(args) > 0 {
		mustUsage(cmd)
		return nil
	}
	conn, err := makeSQLClient()
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

	sort.Strings(output)
	// Ensure the default zone is always printed first.
	if _, ok := zones[0]; ok {
		fmt.Println(".default")
	}
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
	SilenceUsage: true,
	RunE:         runRmZone,
}

func runRmZone(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		mustUsage(cmd)
		return nil
	}

	names, err := parseZoneName(args[0])
	if err != nil {
		return err
	}

	conn, err := makeSQLClient()
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := conn.Exec(`BEGIN`, nil); err != nil {
		return err
	}

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
		return fmt.Errorf("unable to remove %s", args[0])
	}

	if err := runPrettyQuery(conn, os.Stdout,
		makeQuery(`DELETE FROM system.zones WHERE id=$1`, id)); err != nil {
		return err
	}
	return conn.Exec(`COMMIT`, nil)
}

// A setZoneCmd command creates a new or updates an existing zone config.
var setZoneCmd = &cobra.Command{
	Use:   "set [options] <database[.table]> <zone-config>",
	Short: "create or update zone config for object ID",
	Long: `
Create or update the zone config for the specified database or table to the
specified zone-config.

The zone config format has the following YAML schema:

  replicas:
    - attrs: [comma-separated attribute list]
    - attrs:  ...
  range_min_bytes: <size-in-bytes>
  range_max_bytes: <size-in-bytes>
  gc:
    ttlseconds: <time-in-seconds>

For example, to set the zone config for the system database, run:
cockroach zone set system "replicas:
- attrs: [us-east-1a, ssd]
- attrs: [us-east-1b, ssd]
- attrs: [us-west-1b, ssd]"

Note that the specified zone config is merged with the existing zone config for
the database or table.
`,
	SilenceUsage: true,
	RunE:         runSetZone,
}

// runSetZone parses the yaml input file, converts it to proto, and inserts it
// in the system.zones table.
func runSetZone(cmd *cobra.Command, args []string) error {
	if len(args) != 2 {
		mustUsage(cmd)
		return nil
	}

	conn, err := makeSQLClient()
	if err != nil {
		return err
	}
	defer conn.Close()

	names, err := parseZoneName(args[0])
	if err != nil {
		return err
	}

	if err := conn.Exec(`BEGIN`, nil); err != nil {
		return err
	}

	path, err := queryDescriptorIDPath(conn, names)
	if err != nil {
		if err == io.EOF {
			fmt.Printf("%s not found\n", args[0])
			return nil
		}
		return err
	}

	zoneID, zone, err := queryZonePath(conn, path)
	if err != nil {
		return err
	}

	// Convert it to proto and marshal it again to put into the table. This is a
	// bit more tedious than taking protos directly, but yaml is a more widely
	// understood format.
	origReplicaAttrs := zone.ReplicaAttrs
	zone.ReplicaAttrs = nil
	if err := yaml.Unmarshal([]byte(args[1]), zone); err != nil {
		return fmt.Errorf("unable to parse zone config file %q: %s", args[1], err)
	}
	if zone.ReplicaAttrs == nil {
		zone.ReplicaAttrs = origReplicaAttrs
	}

	if err := zone.Validate(); err != nil {
		return err
	}

	buf, err := protoutil.Marshal(zone)
	if err != nil {
		return fmt.Errorf("unable to parse zone config file %q: %s", args[1], err)
	}

	id := path[len(path)-1]
	if id == zoneID {
		err = runPrettyQuery(conn, os.Stdout,
			makeQuery(`UPDATE system.zones SET config = $2 WHERE id = $1`, id, buf))
	} else {
		err = runPrettyQuery(conn, os.Stdout,
			makeQuery(`INSERT INTO system.zones VALUES ($1, $2)`, id, buf))
	}
	if err != nil {
		return err
	}
	if err := conn.Exec(`COMMIT`, nil); err != nil {
		return err
	}

	res, err := yaml.Marshal(zone)
	if err != nil {
		return err
	}
	fmt.Print(string(res))
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
	Run: func(cmd *cobra.Command, args []string) {
		mustUsage(cmd)
	},
}

func init() {
	zoneCmd.AddCommand(zoneCmds...)
}
