// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
