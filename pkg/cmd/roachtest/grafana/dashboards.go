// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package grafana

import _ "embed"

//go:embed configs/changefeed-roachtest-grafana-dashboard.json
var ChangefeedRoachtestGrafanaDashboardJSON string

//go:embed configs/snapshot-admission-control-grafana.json
var SnapshotAdmissionControlGrafanaJSON string

//go:embed configs/multi-tenant-fairness-grafana.json
var MultiTenantFairnessGrafanaJSON string

//go:embed configs/backup-admission-control-grafana.json
var BackupAdmissionControlGrafanaJSON string

//go:embed configs/changefeed-admission-control-grafana.json
var ChangefeedAdmissionControlGrafana string
