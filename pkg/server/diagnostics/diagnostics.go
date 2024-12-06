// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package diagnostics

import (
	"context"
	"math/rand"
	"net/url"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl/licenseccl"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/diagnostics/diagnosticspb"
	"github.com/cockroachdb/cockroach/pkg/util/cloudinfo"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/system"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/host"
	"github.com/shirou/gopsutil/v3/load"
	"github.com/shirou/gopsutil/v3/mem"
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

	// TimeSource if set will be used to set timestamp for last
	// successful telemetry ping.
	TimeSource timeutil.TimeSource
}

// ClusterInfo contains cluster information that will become part of URLs.
type ClusterInfo struct {
	StorageClusterID uuid.UUID
	LogicalClusterID uuid.UUID
	TenantID         roachpb.TenantID
	IsInsecure       bool
	IsInternal       bool
	License          *licenseccl.License
}

// addInfoToURL sets query parameters on the URL used to report diagnostics. If
// this is a CRDB node, then nodeInfo is filled (and nodeInfo.NodeID is
// non-zero). Otherwise, this is a SQL-only tenant and sqlInfo is filled.
func addInfoToURL(
	url *url.URL,
	clusterInfo *ClusterInfo,
	env *diagnosticspb.Environment,
	nodeID roachpb.NodeID,
	sqlInfo *diagnosticspb.SQLInstanceInfo,
) *url.URL {
	if url == nil {
		return nil
	}
	result := *url
	q := result.Query()

	// Don't set nodeid if this is a SQL-only instance.
	if nodeID != 0 {
		q.Set("nodeid", strconv.Itoa(int(nodeID)))
	}

	environment := ""
	licenseExpiry := ""
	organizationID := ""
	licenseID := ""
	if clusterInfo.License != nil {
		license := clusterInfo.License
		environment = license.Environment.String()
		if license.OrganizationId != nil {
			organizationUUID, err := uuid.FromBytes(license.OrganizationId)
			if err != nil {
				log.Infof(context.Background(), "unexpected error parsing organization id from license %s", err)
			}
			organizationID = organizationUUID.String()
		}
		if license.LicenseId != nil {
			licenseExpiry = strconv.Itoa(int(license.ValidUntilUnixSec))
			licenseUUID, err := uuid.FromBytes(license.LicenseId)
			if err != nil {
				log.Infof(context.Background(), "unexpected error parsing organization id from license %s", err)
			}
			licenseID = licenseUUID.String()
		}
	}

	b := env.Build
	q.Set("sqlid", strconv.Itoa(int(sqlInfo.SQLInstanceID)))
	q.Set("uptime", strconv.Itoa(int(sqlInfo.Uptime)))
	q.Set("version", b.Tag)
	q.Set("platform", b.Platform)
	q.Set("uuid", clusterInfo.StorageClusterID.String())
	q.Set("logical_uuid", clusterInfo.LogicalClusterID.String())
	q.Set("tenantid", clusterInfo.TenantID.String())
	q.Set("insecure", strconv.FormatBool(clusterInfo.IsInsecure))
	q.Set("internal", strconv.FormatBool(clusterInfo.IsInternal))
	q.Set("buildchannel", b.Channel)
	q.Set("envchannel", b.EnvChannel)
	// license type must come from the environment because it uses the base package.
	q.Set("licensetype", env.LicenseType)
	q.Set("organization_id", organizationID)
	q.Set("license_id", licenseID)
	q.Set("license_expiry_seconds", licenseExpiry)
	q.Set("environment", environment)
	result.RawQuery = q.Encode()
	return &result
}

// randomly shift `d` to be up to `jitterSeconds` shorter or longer.
func addJitter(d time.Duration) time.Duration {
	const jitterSeconds = 120
	j := time.Duration(rand.Intn(jitterSeconds*2)-jitterSeconds) * time.Second
	return d + j
}

var populateMutex syncutil.Mutex

// populateHardwareInfo populates OS, CPU, memory, etc. information about the
// environment in which CRDB is running.
func populateHardwareInfo(ctx context.Context, e *diagnosticspb.Environment) {
	// The shirou/gopsutil/host library is not multi-thread safe. As one
	// example, it lazily initializes a global map the first time the
	// Virtualization function is called, but takes no lock while doing so.
	// Work around this limitation by taking our own lock.
	populateMutex.Lock()
	defer populateMutex.Unlock()

	if platform, family, version, err := host.PlatformInformation(); err == nil {
		e.Os.Family = family
		e.Os.Platform = platform
		e.Os.Version = version
	}

	if virt, role, err := host.Virtualization(); err == nil && role == "guest" {
		e.Hardware.Virtualization = virt
	}

	if m, err := mem.VirtualMemory(); err == nil {
		e.Hardware.Mem.Available = m.Available
		e.Hardware.Mem.Total = m.Total
	}

	e.Hardware.Cpu.Numcpu = int32(system.NumCPU())
	if cpus, err := cpu.InfoWithContext(ctx); err == nil && len(cpus) > 0 {
		e.Hardware.Cpu.Sockets = int32(len(cpus))
		c := cpus[0]
		e.Hardware.Cpu.Cores = c.Cores
		e.Hardware.Cpu.Model = c.ModelName
		e.Hardware.Cpu.Mhz = float32(c.Mhz)
		e.Hardware.Cpu.Features = c.Flags
	}

	if l, err := load.AvgWithContext(ctx); err == nil {
		e.Hardware.Loadavg15 = float32(l.Load15)
	}

	e.Hardware.Provider, e.Hardware.InstanceClass = cloudinfo.GetInstanceClass(ctx)
	e.Topology.Provider, e.Topology.Region = cloudinfo.GetInstanceRegion(ctx)
}
