// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/mixedversion"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
)

//go:embed db-console/admin_endpoints.json
var adminEndpointsJSON string

//go:embed db-console/api_v2_endpoints.json
var apiV2EndpointsJSON string

//go:embed db-console/status_endpoints.json
var statusEndpointsJSON string

type HTTPMethod string

const (
	GET  HTTPMethod = "GET"
	POST HTTPMethod = "POST"
)

// endpoint represents a DB console endpoint to test.
type endpoint struct {
	url            string
	method         HTTPMethod
	verifyResponse *func(resp *http.Response) error
}

// endpointJSON represents the common structure for all endpoint JSON files
type endpointJSON struct {
	Endpoints []struct {
		URL  string `json:"url"`
		Verb string `json:"verb"`
		Skip bool   `json:"skip,omitempty"`
	} `json:"endpoints"`
}

// These will be used in the test when placeholders need to be filled.
var tableID, databaseID, fingerprintID uint64

// Placeholders in the DB console endpoints.
const (
	nodeIDPlaceholder                = "{node_id}"
	databasePlaceholder              = "{database}"
	databaseNamePlaceholder          = "{database_name}"
	tablePlaceholder                 = "{table}"
	tableNamePlaceholder             = "{table_name}"
	jobIDPlaceholder                 = "{job_id}"
	rangeIDPlaceholder               = "{range_id}"
	snapshotIDPlaceholder            = "{snapshot_id}"
	databaseIDPlaceholder            = "{database_id}"
	tableIDPlaceholder               = "{table_id}"
	filePlaceholder                  = "{file}"
	fingerprintIDPlaceholder         = "{fingerprint_id}"
	statementDiagnosticIDPlaceholder = "{statement_diagnostics_id}"
)

// The DB console endpoints roachtest is a smoke test for a bunch of endpoints on
// the status server, admin server and api v2 server. Custom verification can be
// implemented per endpoint, but by default as long as we get a 200 response,
// the test passes.

// TODO(alyshan): This will be registered as a nightly test. After some time baking
// in master we can move this to the acceptance tests suite, replacing the old
// acceptance/status-server test.
func registerDBConsoleEndpoints(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:             "db-console/endpoints",
		Owner:            registry.OwnerObservability,
		Cluster:          r.MakeClusterSpec(3),
		CompatibleClouds: registry.AllClouds,
		Suites:           registry.Suites(registry.Nightly),
		Randomized:       true,
		Run:              runDBConsole,
		Timeout:          1 * time.Hour,
	})
}

// TODO(alyshan): Mixed version still needs some auditing & investigation.
// Some endpoints fail.
func registerDBConsoleEndpointsMixedVersion(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:             "db-console/mixed-version-endpoints",
		Owner:            registry.OwnerObservability,
		Cluster:          r.MakeClusterSpec(5, spec.WorkloadNode()),
		CompatibleClouds: registry.AllClouds,
		Suites:           registry.Suites(registry.MixedVersion, registry.Nightly),
		Randomized:       true,
		Run:              runDBConsoleMixedVersion,
		Timeout:          1 * time.Hour,
	})
}

func runDBConsoleMixedVersion(ctx context.Context, t test.Test, c cluster.Cluster) {
	mvt := mixedversion.NewTest(ctx, t, t.L(), c,
		c.CRDBNodes(),
		// TODO(alyshan): Determine minimmum supported version.
		// There were some incompatibilities across versions
		// ex. admin server range log endpoint.
		mixedversion.MinimumSupportedVersion("v23.2.0"),
	)

	mvt.InMixedVersion("test db console endpoints", func(ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper) error {
		if err := initializeSchemaAndIDs(ctx, c, t.L()); err != nil {
			t.Fatal(err)
		}
		return testEndpoints(ctx, c, l, getEndpoints(t), true)
	})

	mvt.Run()
}

func runDBConsole(ctx context.Context, t test.Test, c cluster.Cluster) {
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings())

	if err := initializeSchemaAndIDs(ctx, c, t.L()); err != nil {
		t.Fatal(err)
	}

	if err := testEndpoints(ctx, c, t.L(), getEndpoints(t), false); err != nil {
		t.Fatal(err)
	}
}

func testEndpoints(
	ctx context.Context,
	c cluster.Cluster,
	l *logger.Logger,
	endpoints []endpoint,
	multiVersionTest bool,
) error {
	// Get the NodeIDs and DB console URLs for each node.
	idMap := make(map[int]roachpb.NodeID)
	urlMap := make(map[int]string)
	adminUIAddrs, err := c.ExternalAdminUIAddr(ctx, l, c.CRDBNodes())
	if err != nil {
		return err
	}
	client := roachtestutil.DefaultHTTPClient(c, l, roachtestutil.HTTPTimeout(15*time.Second), roachtestutil.WithHeaders(map[string]string{"X-Cockroach-API-Session": "cookie"}))
	for i, addr := range adminUIAddrs {
		var details serverpb.DetailsResponse
		url := `https://` + addr + `/_status/details/local`
		if err := retry.ForDuration(10*time.Second, func() error {
			return client.GetJSON(ctx, url, &details)
		}); err != nil {
			return err
		}
		idMap[i+1] = details.NodeID
		urlMap[i+1] = `https://` + addr
	}

	// Test each endpoint.
	for _, ep := range endpoints {
		// For endpoints with node ID placeholders, test with "local", own node ID, and another node ID.
		if ep.hasNodeID() {
			for nodeID := 1; nodeID <= len(idMap); nodeID++ {
				baseURL := urlMap[nodeID]
				// Test with "local".
				if err := testEndpoint(ctx, client, baseURL, ep, "local", ep.getVerifyResponse(), l, multiVersionTest); err != nil {
					return errors.Wrapf(err, "failed testing endpoint %s with local", ep.url)
				}
				// Test with own node ID.
				if err := testEndpoint(ctx, client, baseURL, ep, idMap[nodeID].String(), ep.getVerifyResponse(), l, multiVersionTest); err != nil {
					return errors.Wrapf(err, "failed testing endpoint %s with own node ID", ep.url)
				}
				// Test with another node ID.
				otherNodeID := (nodeID % len(idMap)) + 1
				if err := testEndpoint(ctx, client, baseURL, ep, idMap[otherNodeID].String(), ep.getVerifyResponse(), l, multiVersionTest); err != nil {
					return errors.Wrapf(err, "failed testing endpoint %s with other node ID", ep.url)
				}
			}
		} else {
			// For endpoints without node IDs, test on each node.
			for nodeID := 1; nodeID <= len(idMap); nodeID++ {
				if err := testEndpoint(ctx, client, urlMap[nodeID], ep, "", ep.getVerifyResponse(), l, multiVersionTest); err != nil {
					return errors.Wrapf(err, "failed testing endpoint %s", ep.url)
				}
			}
		}
	}

	return nil
}

func testEndpoint(
	ctx context.Context,
	client *roachtestutil.RoachtestHTTPClient,
	baseURL string,
	ep endpoint,
	nodeID string,
	verifyResponse func(*http.Response) error,
	l *logger.Logger,
	multiVersionTest bool,
) error {
	fullURL := baseURL + ep.url
	var err error
	fullURL, err = fillPlaceholders(fullURL, nodeID)
	if err != nil {
		return err
	}

	l.Printf("testing endpoint: %s", fullURL)
	var resp *http.Response
	switch ep.method {
	case GET:
		resp, err = client.Get(ctx, fullURL)
	default:
		return errors.Newf("unsupported HTTP method: %s", ep.method)
	}

	if err != nil {
		return errors.Wrapf(err, "failed to %s %s", ep.method, fullURL)
	}
	defer resp.Body.Close()

	// Note(alyshan): In multiversion tests, some endpoints may be unavailable
	// as we may be acting as a secondary tenant (401, 501), or the database is on a version
	// that does not support the endpoint (409).
	if multiVersionTest {
		// Since nodes get restarted in multiversion tests there is a race where certain nodes have
		// not registered certain routes yet. Retry a few times to handle transient 404s.
		if resp.StatusCode == http.StatusNotFound {
			r := retry.StartWithCtx(ctx, retry.Options{
				MaxRetries: 10,
			})
			for r.Next() {
				resp, err = client.Get(ctx, fullURL)
				if err != nil {
					return errors.Wrapf(err, "failed to %s %s", ep.method, fullURL)
				}
				if resp.StatusCode != http.StatusNotFound {
					break
				}
			}
		}

		// TODO(alyshan): Consider explicitly noting which endpoints fall into this category.
		if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusNotImplemented ||
			resp.StatusCode == http.StatusConflict {
			l.Printf("%v returned %v in multiversion testing mode", fullURL, resp.StatusCode)
			return nil
		}
	}

	err = verifyResponse(resp)
	if err != nil {
		return errors.Wrap(err, "failed to verify response")
	}

	return nil
}

func initializeSchemaAndIDs(ctx context.Context, c cluster.Cluster, l *logger.Logger) error {
	// Initialize some schema objects.
	initTpcc := fmt.Sprint("./cockroach workload init tpcc --drop {pgurl:1}")
	c.Run(ctx, option.WithNodes([]int{1}), initTpcc)

	// Get SQL connection to query for a tableID, databaseID and fingerprintID.
	conn, err := c.ConnE(ctx, l, 1)
	if err != nil {
		return err
	}
	defer conn.Close()

	err = conn.QueryRowContext(ctx, "select id, \"parentID\" from system.namespace where name='warehouse'").Scan(&tableID, &databaseID)
	if err != nil {
		return err
	}
	l.Printf("Found tableID: %d, databaseID: %d", tableID, databaseID)

	var fingerprintHexString string
	err = conn.QueryRowContext(ctx, "select encode(fingerprint_id, 'hex') from crdb_internal.statement_statistics limit 1").Scan(&fingerprintHexString)
	if err != nil {
		return err
	}
	fingerprintID, err = strconv.ParseUint(fingerprintHexString, 16, 64)
	if err != nil {
		return err
	}
	l.Printf("Found fingerprintID hex %s, cast to int: %d", fingerprintHexString, fingerprintID)

	return nil
}

func getEndpoints(t test.Test) []endpoint {
	var endpoints []endpoint

	endpoints = append(endpoints, parseEndpointsJSON(t, adminEndpointsJSON)...)
	endpoints = append(endpoints, parseEndpointsJSON(t, apiV2EndpointsJSON)...)
	endpoints = append(endpoints, parseEndpointsJSON(t, statusEndpointsJSON)...)

	return endpoints
}

func parseEndpointsJSON(t test.Test, jsonStr string) []endpoint {
	var endpoints endpointJSON
	if err := json.Unmarshal([]byte(jsonStr), &endpoints); err != nil {
		t.Fatal(errors.Wrap(err, "failed to parse endpoints JSON"))
	}

	var result []endpoint
	for _, ep := range endpoints.Endpoints {
		if ep.Skip {
			t.L().Printf("skipping endpoint: %v", ep.URL)
			continue
		}
		if ep.Verb == "POST" {
			t.L().Printf("skipping POST endpoint: %v", ep.URL)
			continue
		}
		result = append(result, endpoint{
			url:    ep.URL,
			method: HTTPMethod(ep.Verb),
		})
	}
	return result
}

func (e endpoint) hasNodeID() bool {
	return strings.Contains(e.url, nodeIDPlaceholder)
}

// getVerifyResponse returns the verifyResponse function to use for this endpoint.
// If verifyResponse is not specified, returns defaultVerifyResponse.
func (e endpoint) getVerifyResponse() func(resp *http.Response) error {
	if e.verifyResponse == nil {
		return defaultVerifyResponse
	}
	return *e.verifyResponse
}

// defaultVerifyResponse is the default implementation that just checks for 200 status code.
func defaultVerifyResponse(resp *http.Response) error {
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return errors.Newf("unexpected status code: %d, body: %s", resp.StatusCode, body)
	}
	return nil
}

func fillPlaceholders(url string, nodeID string) (string, error) {
	// Fill node_id.
	if strings.Contains(url, nodeIDPlaceholder) {
		url = strings.Replace(url, nodeIDPlaceholder, nodeID, 1)
	}

	// For database name use tpcc, for table name use warehouse.
	if strings.Contains(url, databasePlaceholder) {
		url = strings.Replace(url, databasePlaceholder, "tpcc", 1)
	}
	if strings.Contains(url, databaseNamePlaceholder) {
		url = strings.Replace(url, databaseNamePlaceholder, "tpcc", 1)
	}
	if strings.Contains(url, tablePlaceholder) {
		url = strings.Replace(url, tablePlaceholder, "warehouse", 1)
	}
	if strings.Contains(url, tableNamePlaceholder) {
		url = strings.Replace(url, tableNamePlaceholder, "warehouse", 1)
	}

	// 103 is the sql activity job id.
	if strings.Contains(url, jobIDPlaceholder) {
		url = strings.Replace(url, jobIDPlaceholder, "103", 1)
	}

	// database_id, table_id and fingerprint_id.
	if strings.Contains(url, databaseIDPlaceholder) {
		url = strings.Replace(url, databaseIDPlaceholder, strconv.FormatUint(databaseID, 10), 1)
	}
	if strings.Contains(url, tableIDPlaceholder) {
		url = strings.Replace(url, tableIDPlaceholder, strconv.FormatUint(tableID, 10), 1)
	}
	if strings.Contains(url, fingerprintIDPlaceholder) {
		url = strings.Replace(url, fingerprintIDPlaceholder, strconv.FormatUint(fingerprintID, 10), 1)
	}

	// 1 is the minimum range id.
	if strings.Contains(url, rangeIDPlaceholder) {
		url = strings.Replace(url, rangeIDPlaceholder, "1", 1)
	}

	// Unimplemented.
	for _, placeholder := range []string{snapshotIDPlaceholder, filePlaceholder, statementDiagnosticIDPlaceholder} {
		if strings.Contains(url, placeholder) {
			return "", errors.Newf("unimplemented placeholder %v", placeholder)
		}
	}

	return url, nil
}
