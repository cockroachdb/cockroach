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
	"os"
	"strconv"

	"github.com/gogo/protobuf/proto"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v1"

	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/util/log"
)

// zoneProtoToYAMLString takes a marshalled proto and returns
// its yaml representation.
func zoneProtoToYAMLString(val []byte) (string, error) {
	var zone config.ZoneConfig
	if err := proto.Unmarshal(val, &zone); err != nil {
		return "", err
	}
	ret, err := yaml.Marshal(zone)
	if err != nil {
		return "", err
	}
	return string(ret), nil
}

// formatZone is a callback used to format the raw zone config
// protobuf in a sql.Rows column for pretty printing.
func formatZone(val driver.Value) string {
	if raw, ok := val.([]byte); ok {
		if ret, err := zoneProtoToYAMLString(raw); err == nil {
			return ret
		}
	}
	// Fallback to raw string in case of problems.
	return fmt.Sprintf("%#v", val)
}

// A getZoneCmd command displays the zone config for the specified ID.
var getZoneCmd = &cobra.Command{
	Use:   "get [options] <object-ID>",
	Short: "fetches and displays the zone config",
	Long: `
Fetches and displays the zone configuration for <object-id>.
`,
	SilenceUsage: true,
	RunE:         panicGuard(runGetZone),
}

// runGetZone retrieves the zone config for a given object id,
// and if present, outputs its YAML representation.
// TODO(marc): accept db/table names rather than IDs.
func runGetZone(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		mustUsage(cmd)
		return
	}
	id, err := strconv.Atoi(args[0])
	if err != nil {
		log.Errorf("could not parse object ID %s", args[0])
		return
	}

	conn := makeSQLClient()
	defer conn.Close()
	_, rows, err := runQueryWithFormat(conn, fmtMap{"config": formatZone},
		`SELECT * FROM system.zones WHERE id=$1`, id)
	if err != nil {
		log.Error(err)
		return
	}

	if len(rows) == 0 {
		log.Errorf("Object %d: no zone config found", id)
		return
	}
	fmt.Println(rows[0][1])
}

// A lsZonesCmd command displays a list of zone configs by object ID.
var lsZonesCmd = &cobra.Command{
	Use:   "ls [options]",
	Short: "list all zone configs by object ID",
	Long: `
List zone configs.
`,
	SilenceUsage: true,
	RunE:         panicGuard(runLsZones),
}

// TODO(marc): return db/table names rather than IDs.
func runLsZones(cmd *cobra.Command, args []string) {
	if len(args) > 0 {
		mustUsage(cmd)
		return
	}
	conn := makeSQLClient()
	defer conn.Close()
	_, rows, err := runQueryWithFormat(conn, fmtMap{"config": formatZone}, `SELECT * FROM system.zones`)
	if err != nil {
		log.Error(err)
		return
	}

	if len(rows) == 0 {
		log.Error("No zone configs found")
		return
	}
	for _, r := range rows {
		fmt.Printf("Object %s:\n%s\n", r[0], r[1])
	}
}

// A rmZoneCmd command removes a zone config by ID.
var rmZoneCmd = &cobra.Command{
	Use:   "rm [options] <object-id>",
	Short: "remove a zone config by object ID",
	Long: `
Remove an existing zone config by object ID. No action is taken if no
zone configuration exists for the specified object ID.
`,
	SilenceUsage: true,
	RunE:         panicGuard(runRmZone),
}

// TODO(marc): accept db/table names rather than IDs.
func runRmZone(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		mustUsage(cmd)
		return
	}
	id, err := strconv.Atoi(args[0])
	if err != nil {
		log.Errorf("could not parse object ID %s", args[0])
		return
	}

	conn := makeSQLClient()
	defer conn.Close()
	err = runPrettyQuery(conn, os.Stdout,
		`DELETE FROM system.zones WHERE id=$1`, id)
	if err != nil {
		log.Error(err)
		return
	}
}

// A setZoneCmd command creates a new or updates an existing zone config.
var setZoneCmd = &cobra.Command{
	Use:   "set [options] <object-id> <zone-config>",
	Short: "create or update zone config for object ID",
	Long: `
Create or update a zone config for the specified object ID (first argument: <object-id>)
to the specified zone-config (second argument: <zone-config>).

The zone config format has the following YAML schema:

  replicas:
    - attrs: [comma-separated attribute list]
    - attrs:  ...
  range_min_bytes: <size-in-bytes>
  range_max_bytes: <size-in-bytes>

For example, to set the zone config for object 100, run:
cockroach zone set 100 "replicas:
- attrs: [us-east-1a, ssd]
- attrs: [us-east-1b, ssd]
- attrs: [us-west-1b, ssd]
range_min_bytes: 8388608
range_max_bytes: 67108864"
`,
	SilenceUsage: true,
	RunE:         panicGuard(runSetZone),
}

// runSetZone parses the yaml input file, converts it to proto,
// and inserts it in the system.zones table.
// TODO(marc): accept db/table names rather than IDs.
func runSetZone(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		mustUsage(cmd)
		return
	}
	id, err := strconv.Atoi(args[0])
	if err != nil {
		log.Errorf("could not parse object ID %s", args[0])
		return
	}

	// Convert it to proto and marshal it again to put into the table.
	// This is a bit more tedious than taking protos directly,
	// but yaml is a more widely understood format.
	var pbZoneConfig config.ZoneConfig
	if err := yaml.Unmarshal([]byte(args[1]), &pbZoneConfig); err != nil {
		log.Errorf("unable to parse zone config file %q: %s", args[1], err)
		return
	}

	if err := pbZoneConfig.Validate(); err != nil {
		log.Error(err)
		return
	}

	buf, err := proto.Marshal(&pbZoneConfig)
	if err != nil {
		log.Errorf("unable to parse zone config file %q: %s", args[1], err)
		return
	}

	conn := makeSQLClient()
	defer conn.Close()
	// TODO(marc): switch to UPSERT.
	err = runPrettyQuery(conn, os.Stdout,
		`INSERT INTO system.zones VALUES ($1, $2)`, id, buf)
	if err != nil {
		log.Error(err)
		return
	}
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
