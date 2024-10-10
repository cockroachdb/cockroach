// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/require"
)

const (
	regionUsEast    = "us-east1-b"
	regionUsCentral = "us-central1-b"
	regionUsWest    = "us-west1-b"
	regionEuWest    = "europe-west2-b"

	ldapTestUserPassEnvToken = "LDAP_TEST_USER_PASSWORD"
	ldapTestUserNameEnvToken = "LDAP_TEST_USER_NAME"
)

func runConnectionLatencyTest(
	ctx context.Context, t test.Test, c cluster.Cluster, numNodes int, numZones int, password bool,
) {
	// The connection latency workload is not available in the cockroach binary,
	// so we must use the deprecated workload.
	err := c.PutE(ctx, t.L(), t.DeprecatedWorkload(), "./workload")
	require.NoError(t, err)

	settings := install.MakeClusterSettings()
	// Don't start a backup schedule as this roachtest reports roachperf results.
	err = c.StartE(ctx, t.L(), option.NewStartOpts(option.NoBackupSchedule), settings)
	require.NoError(t, err)

	urlTemplate := func(host string) string {
		if password {
			return fmt.Sprintf("postgres://%s:%s@%s:{pgport:1}?sslmode=require&sslrootcert=%s/ca.crt", install.DefaultUser, install.DefaultPassword, host, install.CockroachNodeCertsDir)
		}

		return fmt.Sprintf("postgres://%[1]s@%[2]s:{pgport:1}?sslcert=%[3]s/client.%[1]s.crt&sslkey=%[3]s/client.%[1]s.key&sslrootcert=%[3]s/ca.crt&sslmode=require", install.DefaultUser, host, install.CockroachNodeCertsDir)
	}

	// Only create the user once.
	if password {
		err = c.RunE(ctx, option.WithNodes(c.Node(1)), fmt.Sprintf("./workload init connectionlatency --secure '%s'", urlTemplate("localhost")))
		require.NoError(t, err)
	} else {
		err = c.RunE(ctx, option.WithNodes(c.Node(1)), fmt.Sprintf("./workload init connectionlatency --secure '%s'", urlTemplate("localhost")))
		require.NoError(t, err)
	}

	runWorkload := func(roachNodes, loadNode option.NodeListOption, locality string) {
		var urlString string
		var urls []string
		externalIps, err := c.ExternalIP(ctx, t.L(), roachNodes)
		require.NoError(t, err)

		for _, u := range externalIps {
			url := urlTemplate(u)
			urls = append(urls, fmt.Sprintf("'%s'", url))
		}
		urlString = strings.Join(urls, " ")

		t.L().Printf("running workload in %q against urls:\n%s", locality, strings.Join(urls, "\n"))

		workloadCmd := fmt.Sprintf(
			`./workload run connectionlatency %s --secure --duration 30s --histograms=%s/stats.json --locality %s`,
			urlString,
			t.PerfArtifactsDir(),
			locality,
		)
		err = c.RunE(ctx, option.WithNodes(loadNode), workloadCmd)
		require.NoError(t, err)
	}

	if numZones > 1 {
		numLoadNodes := numZones
		loadGroups := makeLoadGroups(c, numZones, numNodes, numLoadNodes)
		cockroachUsEast := loadGroups[0].loadNodes
		cockroachUsWest := loadGroups[1].loadNodes
		cockroachEuWest := loadGroups[2].loadNodes

		runWorkload(loadGroups[0].roachNodes, cockroachUsEast, regionUsEast)
		runWorkload(loadGroups[1].roachNodes, cockroachUsWest, regionUsWest)
		runWorkload(loadGroups[2].roachNodes, cockroachEuWest, regionEuWest)
	} else {
		// Run only on the load node.
		runWorkload(c.Range(1, numNodes), c.Node(numNodes+1), regionUsCentral)
	}
}

func registerConnectionLatencyTest(r registry.Registry) {
	// Single region test.
	numNodes := 3
	r.Add(registry.TestSpec{
		Name:      fmt.Sprintf("connection_latency/nodes=%d/certs", numNodes),
		Owner:     registry.OwnerSQLFoundations,
		Benchmark: true,
		// Add one more node for load node.
		Cluster:                    r.MakeClusterSpec(numNodes+1, spec.WorkloadNode(), spec.GCEZones(regionUsCentral)),
		CompatibleClouds:           registry.OnlyGCE,
		Suites:                     registry.Suites(registry.Nightly),
		RequiresDeprecatedWorkload: true, // uses connectionlatency
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runConnectionLatencyTest(ctx, t, c, numNodes, 1, false /*password*/)
		},
	})

	geoZones := []string{regionUsEast, regionUsWest, regionEuWest}
	geoZonesStr := strings.Join(geoZones, ",")
	numMultiRegionNodes := 9
	numZones := len(geoZones)
	loadNodes := numZones

	r.Add(registry.TestSpec{
		Name:                       fmt.Sprintf("connection_latency/nodes=%d/multiregion/certs", numMultiRegionNodes),
		Owner:                      registry.OwnerSQLFoundations,
		Benchmark:                  true,
		Cluster:                    r.MakeClusterSpec(numMultiRegionNodes+loadNodes, spec.Geo(), spec.GCEZones(geoZonesStr)),
		CompatibleClouds:           registry.OnlyGCE,
		Suites:                     registry.Suites(registry.Nightly),
		RequiresDeprecatedWorkload: true, // uses connectionlatency
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runConnectionLatencyTest(ctx, t, c, numMultiRegionNodes, numZones, false /*password*/)
		},
	})

	r.Add(registry.TestSpec{
		Name:                       fmt.Sprintf("connection_latency/nodes=%d/multiregion/password", numMultiRegionNodes),
		Owner:                      registry.OwnerSQLFoundations,
		Benchmark:                  true,
		Cluster:                    r.MakeClusterSpec(numMultiRegionNodes+loadNodes, spec.Geo(), spec.GCEZones(geoZonesStr)),
		CompatibleClouds:           registry.OnlyGCE,
		Suites:                     registry.Suites(registry.Nightly),
		RequiresDeprecatedWorkload: true, // uses connectionlatency
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runConnectionLatencyTest(ctx, t, c, numMultiRegionNodes, numZones, true /*password*/)
		},
	})
}

func registerLDAPConnectionLatencyTest(r registry.Registry) {

	// Single region, 3 node test for LDAP connection latency
	numNodes := 3
	r.Add(registry.TestSpec{
		Name:      "ldap_connection_latency",
		Owner:     registry.OwnerProductSecurity,
		Benchmark: true,
		// currently env. var is only set for Azure nightly runs
		CompatibleClouds: registry.OnlyAzure,
		Suites:           registry.Suites(registry.Nightly),
		Cluster: r.MakeClusterSpec(numNodes+1,
			spec.WorkloadNode(), spec.GCEZones(regionUsCentral)),
		RequiresLicense:            true,
		RequiresDeprecatedWorkload: true,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runLDAPConnectionLatencyTest(ctx, t, c, numNodes)
		},
	})
}

func runLDAPConnectionLatencyTest(
	ctx context.Context, t test.Test, c cluster.Cluster, numNodes int,
) {

	ldapTestUserName, ok := os.LookupEnv(ldapTestUserNameEnvToken)
	if !ok {
		t.Fatalf("environment variable %s not set", ldapTestUserNameEnvToken)
	}

	ldapTestUserPassword, ok := os.LookupEnv(ldapTestUserPassEnvToken)
	if !ok {
		t.Fatalf("environment variable %s not set", ldapTestUserPassEnvToken)
	}

	err := c.PutE(ctx, t.L(), t.DeprecatedWorkload(), "./workload")
	require.NoError(t, err)

	settings := install.MakeClusterSettings()
	setClusterSettingsForLDAP(t, &settings, ldapTestUserPassword)

	// Don't start a backup schedule as this roachtest reports roachperf results.
	err = c.StartE(ctx, t.L(), option.NewStartOpts(option.NoBackupSchedule),
		settings)
	require.NoError(t, err)

	prepareSQLUserForLDAP(ctx, t, c, ldapTestUserName)

	// Includes the workload node for cert update
	updateNodeCACrtForAllNodes(ctx, t, c, numNodes+1)

	urlTemplate := func(host string) string {
		return fmt.Sprintf("postgresql://%s:%s@%s:{pgport:1}",
			ldapTestUserName, ldapTestUserPassword, host)
	}

	runWorkload := func(roachNodes, loadNode option.NodeListOption, locality string) {
		var urlString string
		var urls []string
		externalIps, err := c.ExternalIP(ctx, t.L(), roachNodes)
		require.NoError(t, err)

		for _, u := range externalIps {
			url := urlTemplate(u)
			urls = append(urls, fmt.Sprintf("'%s'", url))
		}
		urlString = strings.Join(urls, " ")

		t.L().Printf("running workload in %q against urls:\n%s", locality, strings.Join(urls, "\n"))

		workloadCmd := fmt.Sprintf(
			`./workload run connectionlatency %s --secure --duration 30s --histograms=%s/stats.json --locality %s`,
			urlString,
			t.PerfArtifactsDir(),
			locality,
		)
		err = c.RunE(ctx, option.WithNodes(loadNode), workloadCmd)
		require.NoError(t, err)
	}

	//Run only on the load node.
	runWorkload(c.Range(1, numNodes), c.Node(numNodes+1), regionUsCentral)
}

// setClusterSettingsForLDAP sets the HBA conf and the custom CA
// required for LDAP connection enablement.
func setClusterSettingsForLDAP(t test.Test, settings *install.ClusterSettings, ldapUserPass string) {
	// HBA conf particularly sets the roachprod user for password authentication
	// as the first rule to enable roachtest specific functionality intact.
	hbaConf := "server.host_based_authentication.configuration"
	// update the password for the ldap user from the
	// env variable fetched earlier.
	(*settings).ClusterSettings[hbaConf] = fmt.Sprintf(getTestDataFileContent(
		t, "./pkg/cmd/roachtest/testdata/ldap_authentication_hba_conf"), ldapUserPass)
	customCA := "server.ldap_authentication.domain.custom_ca"
	(*settings).ClusterSettings[customCA] = getTestDataFileContent(
		t, "./pkg/cmd/roachtest/testdata/ldap_authentication_domain_custom_ca")
}

// getTestDataFileContent reads data from the specified filepath
func getTestDataFileContent(t test.Test, filePath string) string {
	byteValue, err := os.ReadFile(filePath)
	require.NoError(t, err)
	return string(byteValue)
}

// prepareUserForLDAP creates a SQL user and grants admin privilege
// to the user. The `ldapUserName` must be same as the username
// on the LDAP server.
func prepareSQLUserForLDAP(
	ctx context.Context, t test.Test, c cluster.Cluster, ldapUserName string,
) {
	// Connect to the node to create the required SQL users
	// which will be authenticated using LDAP.
	pgURL, err := c.ExternalPGUrl(ctx, t.L(),
		c.Node(1), roachprod.PGURLOptions{})
	require.NoError(t, err)
	require.NotEmpty(t, pgURL)
	conn, err := pgx.Connect(ctx, pgURL[0])
	require.NoError(t, err)

	row1, err := conn.Query(ctx,
		fmt.Sprintf("CREATE ROLE %s LOGIN", ldapUserName))
	require.NoError(t, err)
	// The row must be closed for the connection to be used again
	row1.Close()

	// connectionlatency workload checks the presence of the database named
	// `connectionlatency`, if not present it creates it using the logged-in user
	// Hence for ease of use, giving admin privilege to the `testuser`.
	row2, err := conn.Query(ctx,
		fmt.Sprintf("GRANT admin to %s", ldapUserName))
	require.NoError(t, err)
	// The row must be closed for the connection to be used again
	row2.Close()
}

// updateNodeCACrtForAllNodes appends the LDAP server associated
// ca certificate onto the node's ca.crt file.
func updateNodeCACrtForAllNodes(
	ctx context.Context, t test.Test, c cluster.Cluster, nodeCount int,
) {

	for i := 1; i <= nodeCount; i++ {
		// read the current node's ca.crt
		out, err := c.RunWithDetailsSingleNode(ctx, t.L(), option.WithNodes(c.Node(i)),
			fmt.Sprintf("cd %s && cat ca.crt", install.CockroachNodeCertsDir))
		require.NoError(t, err)

		// Append the ca.crt associated with the LDAP server on the node
		content := out.Stdout + getTestDataFileContent(
			t, "./pkg/cmd/roachtest/testdata/ldap_authentication_ca_crt")

		err = c.PutString(ctx, content,
			fmt.Sprintf("%s/ca.crt", install.CockroachNodeCertsDir), 0755, c.Node(i))
		require.NoError(t, err)
	}
}
