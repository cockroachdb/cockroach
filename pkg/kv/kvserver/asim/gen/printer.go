// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gen

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
)

// compareSettingsToDefault compares the given settings against DefaultSimulationSettings
// and returns a string listing the fields that have changed from their default values.
// Returns an empty string if no changes are found.
func compareSettingsToDefault(settings config.SimulationSettings) string {
	defaultSettings := config.DefaultSimulationSettings()
	var changes []string

	// Use reflection to compare all fields
	settingsVal := reflect.ValueOf(settings)
	defaultVal := reflect.ValueOf(*defaultSettings)
	settingsType := reflect.TypeOf(settings)

	for i := 0; i < settingsVal.NumField(); i++ {
		field := settingsType.Field(i)
		settingsFieldVal := settingsVal.Field(i)
		defaultFieldVal := defaultVal.Field(i)

		// Skip cluster setting ST and seed. The simulation seed is derived from
		// rand.New(rand.NewSource(42)).Int63() by default, while the default
		// simulation setting itself uses 42.
		if field.Name == "ST" || field.Name == "Seed" {
			continue
		}

		if !reflect.DeepEqual(settingsFieldVal.Interface(), defaultFieldVal.Interface()) {
			changes = append(changes, fmt.Sprintf("\t%s: %v (default: %v)",
				field.Name,
				settingsFieldVal.Interface(),
				defaultFieldVal.Interface()))
		}
	}

	if len(changes) == 0 {
		return ""
	}

	return strings.Join(changes, "\n")
}

// generateClusterVisualization generates a visualization of the cluster state.
// Example: mma_one_voter_skewed_cpu_skewed_write_setup.txt.
//
// Cluster Set Up
// n1(AU_EAST,AU_EAST_1,0vcpu): {s1}
// n2(AU_EAST,AU_EAST_1,0vcpu): {s2}
// Key Space
// [1,10000): 100(rf=1), 25MiB, [{s1*}:100]
// [10001,20000): 100(rf=1), 25MiB, [{s2*}:100]
// Event
// set LBRebalancingMode to 4
// Workload Set Up
// [1,10000): read-only high-cpu [500.00cpu-us/op, 1B/op, 1000ops/s]
// [10001,20000): write-only large-block [0.00cpu-us/write(raft), 1000B/op, 5000ops/s]Settings
// Changed settings:
// ReplicateQueueEnabled: false (default: true)
// LeaseQueueEnabled: false (default: true)
// SplitQueueEnabled: false (default: true)
func generateClusterVisualization(
	buf *strings.Builder,
	s state.State,
	loadGen LoadGen,
	eventGen EventGen,
	rangeStateStr string,
	settings config.SimulationSettings,
) {
	if buf == nil {
		return
	}
	_, _ = fmt.Fprintf(buf, "Cluster Set Up\n")
	_, _ = fmt.Fprintf(buf, "%v", s.NodesString())
	_, _ = fmt.Fprintf(buf, "Key Space\n%s", rangeStateStr)
	_, _ = fmt.Fprintf(buf, "Event\n%s", eventGen.String())
	_, _ = fmt.Fprintf(buf, "Workload Set Up\n%s", loadGen.String())

	// Only print settings section if there are changes from defaults.
	if settingsChanges := compareSettingsToDefault(settings); settingsChanges != "" {
		_, _ = fmt.Fprintf(buf, "Changed Settings\n%s\n", settingsChanges)
	}
}
