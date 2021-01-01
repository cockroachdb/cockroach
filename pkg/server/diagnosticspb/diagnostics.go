// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package diagnosticspb

import (
	"net/url"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// updatesURL is the URL used to check for new versions. Can be nil if an empty
// URL is set.
var updatesURL *url.URL

const defaultUpdatesURL = `https://register.cockroachdb.com/api/clusters/updates`

// reportingURL is the URL used to report diagnostics/telemetry. Can be nil if
// an empty URL is set.
var reportingURL *url.URL

const defaultReportingURL = `https://register.cockroachdb.com/api/clusters/report`

func init() {
	var err error
	updatesURL, err = url.Parse(
		envutil.EnvOrDefaultString("COCKROACH_UPDATE_CHECK_URL", defaultUpdatesURL),
	)
	if err != nil {
		panic(err)
	}
	reportingURL, err = url.Parse(
		envutil.EnvOrDefaultString("COCKROACH_USAGE_REPORT_URL", defaultReportingURL),
	)
	if err != nil {
		panic(err)
	}
}

// TestingKnobs groups testing knobs for diagnostics.
type TestingKnobs struct {
	// OverrideUpdatesURL if set, overrides the URL used to check for new
	// versions. It is a pointer to pointer to allow overriding to the nil URL.
	OverrideUpdatesURL **url.URL

	// OverrideReportingURL if set, overrides the URL used to report diagnostics.
	// It is a pointer to pointer to allow overriding to the nil URL.
	OverrideReportingURL **url.URL
}

// ClusterInfo contains cluster information that will become part of URLs.
type ClusterInfo struct {
	ClusterID  uuid.UUID
	TenantID   roachpb.TenantID
	IsInsecure bool
	IsInternal bool
}

// BuildUpdatesURL creates a URL to check for version updates.
// If an empty updates URL is set (via empty environment variable), returns nil.
func BuildUpdatesURL(clusterInfo *ClusterInfo, nodeInfo *NodeInfo, knobs *TestingKnobs) *url.URL {
	url := updatesURL
	if knobs != nil && knobs.OverrideUpdatesURL != nil {
		url = *knobs.OverrideUpdatesURL
	}
	return addInfoToURL(url, clusterInfo, nodeInfo, &SQLInstanceInfo{})
}

// BuildReportingURL creates a URL to report diagnostics. If this is a CRDB
// node, then nodeInfo is filled (and nodeInfo.NodeID is non-zero). Otherwise,
// this is a SQL-only tenant and sqlInfo is filled.
//
// If an empty updates URL is set (via empty environment variable), returns nil.
func BuildReportingURL(
	clusterInfo *ClusterInfo, nodeInfo *NodeInfo, sqlInfo *SQLInstanceInfo, knobs *TestingKnobs,
) *url.URL {
	url := reportingURL
	if knobs != nil && knobs.OverrideReportingURL != nil {
		url = *knobs.OverrideReportingURL
	}
	return addInfoToURL(url, clusterInfo, nodeInfo, sqlInfo)
}

func addInfoToURL(
	url *url.URL, clusterInfo *ClusterInfo, nodeInfo *NodeInfo, sqlInfo *SQLInstanceInfo,
) *url.URL {
	if url == nil {
		return nil
	}
	result := *url
	q := result.Query()

	// If this is a SQL-only instance, then NodeID will be zero.
	var b build.Info
	if nodeInfo.NodeID != 0 {
		b = nodeInfo.Build
		q.Set("nodeid", strconv.Itoa(int(nodeInfo.NodeID)))
		q.Set("uptime", strconv.Itoa(int(nodeInfo.Uptime)))
		q.Set("licensetype", nodeInfo.LicenseType)
	} else {
		b = sqlInfo.Build
		q.Set("sqlid", strconv.Itoa(int(sqlInfo.SQLInstanceID)))
		q.Set("uptime", strconv.Itoa(int(sqlInfo.Uptime)))
		q.Set("licensetype", sqlInfo.LicenseType)
	}

	q.Set("version", b.Tag)
	q.Set("platform", b.Platform)
	q.Set("uuid", clusterInfo.ClusterID.String())
	q.Set("tenantid", clusterInfo.TenantID.String())
	q.Set("insecure", strconv.FormatBool(clusterInfo.IsInsecure))
	q.Set("internal", strconv.FormatBool(clusterInfo.IsInternal))
	q.Set("buildchannel", b.Channel)
	q.Set("envchannel", b.EnvChannel)
	result.RawQuery = q.Encode()
	return &result
}
