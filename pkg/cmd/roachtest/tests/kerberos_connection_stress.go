// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

const (
	kerberosTestUserName       = "tester"
	kerberosUserPassword       = "psql"
	stressTestRunTimeInMinutes = 10
)

func registerKerberosConnectionStressTest(r registry.Registry) {
	// Single region test.
	numNodes := 1
	r.Add(registry.TestSpec{
		Name:      "kerberos_connection_stress_test",
		Owner:     registry.OwnerProductSecurity,
		Benchmark: true,
		Cluster:   r.MakeClusterSpec(numNodes, spec.GCEZones(regionUsCentral)),
		// Cannot be run locally as it is dependent on Linux.
		CompatibleClouds: registry.OnlyGCE,
		Suites:           registry.Suites(registry.Nightly),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runKerberosConnectionStressTest(ctx, t, c, numNodes, 1)
		},
	})
}

func runKerberosConnectionStressTest(
	ctx context.Context, t test.Test, c cluster.Cluster, numNodes int, numZones int,
) {
	settings := install.MakeClusterSettings()
	// Set the environment variables which must be visible to the cockroach process
	// and not set in individual nodes.
	settings.Env = append(settings.Env, "KRB5_KTNAME=/home/ubuntu/crdb.keytab")
	settings.Env = append(settings.Env, "MALLOC_CONF=prof:true")
	settings.Env = append(settings.Env, "COCKROACH_MEMPROF_INTERVAL=10s")
	setClusterSettingsForKerberos(t, &settings)

	// Don't start a backup schedule as this roachtest reports roachperf results.
	err := c.StartE(ctx, t.L(),
		option.NewStartOpts(option.NoBackupSchedule), settings)
	require.NoError(t, err)

	// The user must be named the same as registered in the kerberos service.
	prepareSQLUserForKerberos(ctx, t, c)

	nodeArr := make([]int, numNodes)
	for i := 1; i <= numNodes; i++ {
		nodeArr[i-1] = i
	}
	nodeListOptions := c.Nodes(nodeArr...)

	putKerberosConfigOnAllNodes(ctx, t, c, nodeListOptions)
	installKerberosRelatedPackages(ctx, t, c, nodeListOptions)
	setupKerberosServiceAndUsers(ctx, t, c, nodeListOptions, numNodes)

	cmd := fmt.Sprintf("echo %s | kinit %s && ./cockroach sql --url "+
		"\"postgresql://%s:%s@localhost:{pgport:1}?sslmode=require\" -e 'SELECT 1;'",
		kerberosUserPassword, kerberosTestUserName,
		kerberosTestUserName, kerberosUserPassword)
	// Check once if the connection works.
	out, err := c.RunWithDetailsSingleNode(ctx, t.L(),
		option.WithNodes(c.Node(1)), cmd)
	require.NoError(t, err)
	require.Contains(t, out.Stdout, "1")
	t.L().Printf("out: %s.", out.Stdout)

	// Start the service and let it run for a while
	// to check if there is a memory leak.
	startKerberosConnSpamExecutable(ctx, t, c, nodeListOptions)
	time.Sleep(stressTestRunTimeInMinutes * time.Minute)
	// Check the memory retained by the kerberos functions.
	checkForKerberosMemoryConsumption(ctx, t, c, nodeListOptions)
	// Stop the service
	stopRunningExecutable(ctx, t, c, nodeListOptions)
}

// prepareSQLUserForKerberos creates a SQL user
// the username must be the same as the username
// created in kerberos service.
func prepareSQLUserForKerberos(ctx context.Context, t test.Test, c cluster.Cluster) {
	// Connect to the node to create the required SQL users
	// which will be authenticated using Kerberos.
	pgURL, err := c.ExternalPGUrl(ctx, t.L(),
		c.Node(1), roachprod.PGURLOptions{})
	require.NoError(t, err)
	require.NotEmpty(t, pgURL)
	conn, err := pgx.Connect(ctx, pgURL[0])
	require.NoError(t, err)

	row1, err := conn.Query(ctx,
		fmt.Sprintf("CREATE USER %s;", kerberosTestUserName))
	require.NoError(t, err)
	// The row must be closed for the connection to be used again
	row1.Close()
}

// setClusterSettingsForKerberos sets the HBA conf
// required for Kerberos connection enablement.
func setClusterSettingsForKerberos(t test.Test, settings *install.ClusterSettings) {
	// Settings related to memory profiling
	(*settings).ClusterSettings["server.mem_profile.max_profiles"] = "1000"
	(*settings).ClusterSettings["server.mem_profile.total_dump_size_limit"] = "1GiB"

	// HBA conf particularly sets the roachprod user for password authentication
	// as the first rule to enable roachtest specific functionality intact.
	hbaConf := "server.host_based_authentication.configuration"
	// update the password for the ldap user from the
	// env variable fetched earlier.
	(*settings).ClusterSettings[hbaConf] = getTestDataFileContent(
		t, "./pkg/cmd/roachtest/testdata/kerberos_authentication_hba_conf")
}

// getTestDataFileContent reads data from the specified filepath
func getTestDataFileContent(t test.Test, filePath string) string {
	byteValue, err := os.ReadFile(filePath)
	require.NoError(t, err)
	return string(byteValue)
}

// putKerberosConfigOnAllNodes moves the kerberos config
// to the cluster for the kerberos service to consume.
func putKerberosConfigOnAllNodes(
	ctx context.Context, t test.Test, c cluster.Cluster, nodeListOptions option.NodeListOption,
) {
	err := c.PutE(ctx, t.L(),
		"./pkg/cmd/roachtest/testdata/krb5.conf", "krb5.conf",
		nodeListOptions)
	require.NoError(t, err)

	krb5Cmd := "sudo mv krb5.conf /etc/krb5.conf"
	err = c.RunE(ctx, option.WithNodes(nodeListOptions), krb5Cmd)
	require.NoError(t, err)
}

// installKerberosRelatedPackages installs the kerberos and
// memory profiling specific packages.
func installKerberosRelatedPackages(
	ctx context.Context, t test.Test, c cluster.Cluster, nodeListOptions option.NodeListOption,
) {
	cmd := "sudo apt install -yyq krb5-kdc krb5-admin-server ghostscript " +
		"graphviz postgresql-client libjemalloc-dev"
	err := c.RunE(ctx, option.WithNodes(nodeListOptions), cmd)
	require.NoError(t, err)
}

func setupKerberosServiceAndUsers(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	nodeListOptions option.NodeListOption,
	numNodes int,
) {
	// Create master password for the database and stash
	// so that Key Distribution Center can use the password without prompting.
	cmd := "sudo kdb5_util create -s -P kpass"
	err := c.RunE(ctx, option.WithNodes(nodeListOptions), cmd)
	require.NoError(t, err)

	// Create a new kerberos identity(user) and sets it's password
	cmd = fmt.Sprintf("sudo kadmin.local -q \"addprinc -pw %s tester@MY.EX\"",
		kerberosUserPassword)
	err = c.RunE(ctx, option.WithNodes(nodeListOptions), cmd)
	require.NoError(t, err)

	// Create a new principal(service principal) which is
	// generally of the form `service/hostname@REALM`
	// It sets a random ley as the password for the created principal.
	cmd = "sudo kadmin.local -q \"addprinc -randkey postgres/localhost@MY.EX\""
	err = c.RunE(ctx, option.WithNodes(nodeListOptions), cmd)
	require.NoError(t, err)

	// Create a corresponding encrypted key for the principal created above
	// and make it readable by the CRDB process under ubuntu.
	// The CRDB process is made aware of the file path via
	// env. variable named `KRB5_KTNAME`.
	cmd = "sudo kadmin.local -q \"ktadd -k " +
		"/home/ubuntu/crdb.keytab postgres/localhost@MY.EX\""
	err = c.RunE(ctx, option.WithNodes(nodeListOptions), cmd)
	require.NoError(t, err)
	cmd = "sudo chown ubuntu crdb.keytab"
	err = c.RunE(ctx, option.WithNodes(nodeListOptions), cmd)
	require.NoError(t, err)

	// Run the KDC(Key Distribution Service) in the background as a daemon.
	cmd = "sudo systemd-run --unit krb5kdc -- krb5kdc -n"
	err = c.RunE(ctx, option.WithNodes(nodeListOptions), cmd)
	require.NoError(t, err)

	// Check the status of the service.
	cmd = "systemctl status krb5kdc.service"
	for i := 1; i <= numNodes; i++ {
		_, err = c.RunWithDetailsSingleNode(ctx, t.L(),
			option.WithNodes(c.Node(i)), "systemctl status krb5kdc.service")
		require.NoError(t, err)
	}
}

func startKerberosConnSpamExecutable(
	ctx context.Context, t test.Test, c cluster.Cluster, nodeListOptions option.NodeListOption,
) {
	// Move the connection creating script to the cluster
	err := c.PutE(ctx, t.L(),
		"./pkg/cmd/roachtest/testdata/kerberos_connspam.sh", "connspam.sh",
		nodeListOptions)
	require.NoError(t, err)

	// Change permissions to make the file executable.
	krb5Cmd := "chmod +x connspam.sh"
	err = c.RunE(ctx, option.WithNodes(nodeListOptions), krb5Cmd)
	require.NoError(t, err)

	// Run as a service
	cmd := fmt.Sprintf("sudo systemd-run --unit spam1 ./connspam.sh \"%s\" \"%s\"",
		kerberosUserPassword, kerberosTestUserName)
	err = c.RunE(ctx, option.WithNodes(nodeListOptions), cmd)
	require.NoError(t, err)

	// Ensure that the service is in active running state.
	cmd = "systemctl status spam1"
	out, err := c.RunWithDetailsSingleNode(ctx, t.L(),
		option.WithNodes(c.Node(1)), cmd)
	require.NoError(t, err)
	require.Contains(t, out.Stdout, "active (running)")
}

// checkForKerberosMemoryConsumption reads the most recent file named
// of the form logs/heap_profiler/jeprof.*
// Then it parses the output and checks for the retained memory by
// functions having the name containing 'krb5'.
func checkForKerberosMemoryConsumption(
	ctx context.Context, t test.Test, c cluster.Cluster, nodeListOptions option.NodeListOption,
) {
	cmd := "jeprof --text cockroach $(ls logs/heap_profiler/jeprof.* | tail -n 1)"
	out, err := c.RunWithDetailsSingleNode(ctx, t.L(),
		option.WithNodes(c.Node(1)), cmd)
	require.NoError(t, err)

	parseOutputEntriesForMemoryRetained(t, out.Stdout)
}

// Function to parse the jeprof output and extract lines containing 'krb5'
// Compares that the output should not exceed 1% of the allocated memory.
func parseOutputEntriesForMemoryRetained(t test.Test, data string) {
	reader := strings.NewReader(data)
	scanner := bufio.NewScanner(reader)

	for scanner.Scan() {
		// The line are of the form
		// MemoryAllocated PercentAllocated CumulativePercent MemoryRetained
		// PercentRetained FunctionName
		//         1.5             1.1%             80.0%            1.5
		//     1.1%   krb5_gss_accept_sec_context
		line := scanner.Text()
		if strings.Contains(line, "krb5") {
			// Split the line by spaces and process it
			fields := strings.Fields(line)

			// Parse field[4] (PercentRetained) into a float using strconv
			percentRetained, err := parsePercentage(fields[4])
			require.NoError(t, err)
			// Cap PercentRetained at 1.0
			if percentRetained >= 1.0 {
				t.Fatalf("Detected a memory leak in kerberos "+
					"related function: %s.", fields[5])
			}
		}
	}

	err := scanner.Err()
	require.NoError(t, err)
}

// Helper function to parse a percentage string (e.g., "3.3%") to float
func parsePercentage(percentageStr string) (float64, error) {
	// Remove the '%' sign if it exists
	cleanedStr := strings.TrimSuffix(percentageStr, "%")
	return strconv.ParseFloat(cleanedStr, 64)
}

// stopRunningExecutable stops the running kerberos service
// and the connection creation script running as a service.
func stopRunningExecutable(
	ctx context.Context, t test.Test, c cluster.Cluster, nodeListOptions option.NodeListOption,
) {
	cmd := "sudo systemctl stop spam1"
	err := c.RunE(ctx, option.WithNodes(nodeListOptions), cmd)
	require.NoError(t, err)

	cmd = "sudo systemctl stop krb5kdc.service"
	err = c.RunE(ctx, option.WithNodes(nodeListOptions), cmd)
	require.NoError(t, err)
}
