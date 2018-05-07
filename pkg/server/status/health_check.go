// Copyright 2018 The Cockroach Authors.
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

package status

import "context"

// A HealthChecker inspects the node metrics and optionally a NodeStatus for
// anomalous conditions that the operator should be alerted to.
type HealthChecker struct {
	// TODO(tschottdorf): introduce a state for counter-based metrics so that
	// they can be tracked as well (without state, counters that trigger once
	// will trigger until the metrics are reset -- not ok).
}

// CheckHealth performs a (cheap) health check.
func (h *HealthChecker) CheckHealth(ctx context.Context, nodeStatus NodeStatus) HealthCheckResult {
	// Gauges that trigger alerts when nonzero.
	metricGauges := map[string]struct{}{
		"ranges.unavailable":     {},
		"ranges.underreplicated": {},
	}

	var alerts []HealthAlert

	for gauge := range metricGauges {
		if value := nodeStatus.Metrics[gauge]; value != 0 {
			alerts = append(alerts, HealthAlert{
				Category:    HealthAlert_METRICS,
				Description: gauge,
				Value:       value,
			})
		}
		for _, storeStatus := range nodeStatus.StoreStatuses {
			if value := storeStatus.Metrics[gauge]; value != 0 {
				alerts = append(alerts, HealthAlert{
					StoreID:     storeStatus.Desc.StoreID,
					Category:    HealthAlert_METRICS,
					Description: gauge,
					Value:       value,
				})
			}
		}
	}

	return HealthCheckResult{Alerts: alerts}
}
