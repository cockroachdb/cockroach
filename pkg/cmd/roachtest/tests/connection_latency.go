// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

const (
	regionUsEast    = "us-east1-b"
	regionUsCentral = "us-central1-b"
	regionUsWest    = "us-west1-b"
	regionEuWest    = "europe-west2-b"

	ldapTestUserName     = "jdoe"
	ldapTestUserPassword = "password"
	ldapAdminPassword    = "password"
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

	err = c.RunE(ctx, option.WithNodes(c.Node(1)), fmt.Sprintf("./workload init connectionlatency --secure '%s'", urlTemplate("localhost")))
	require.NoError(t, err)

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

		labels := map[string]string{
			"duration": "30000",
			"locality": locality,
		}

		workloadCmd := fmt.Sprintf(
			`./workload run connectionlatency %s --secure --duration 30s %s --locality %s`,
			urlString,
			roachtestutil.GetWorkloadHistogramArgs(t, c, labels),
			locality,
		)
		err = c.RunE(ctx, option.WithNodes(loadNode), workloadCmd)
		require.NoError(t, err)
	}

	if numZones > 1 {
		numLoadNodes := numZones
		loadGroups := roachtestutil.MakeLoadGroups(c, numZones, numNodes, numLoadNodes)
		cockroachUsEast := loadGroups[0].LoadNodes
		cockroachUsWest := loadGroups[1].LoadNodes
		cockroachEuWest := loadGroups[2].LoadNodes

		runWorkload(loadGroups[0].RoachNodes, cockroachUsEast, regionUsEast)
		runWorkload(loadGroups[1].RoachNodes, cockroachUsWest, regionUsWest)
		runWorkload(loadGroups[2].RoachNodes, cockroachEuWest, regionEuWest)
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

	// Single region, 1 node test for LDAP connection latency
	numNodes := 1
	r.Add(registry.TestSpec{
		Name:                       "ldap_connection_latency",
		Owner:                      registry.OwnerProductSecurity,
		Benchmark:                  true,
		Cluster:                    r.MakeClusterSpec(numNodes, spec.GCEZones(regionUsCentral)),
		RequiresDeprecatedWorkload: true,
		// Cannot be run locally as it is dependent on Linux.
		CompatibleClouds: registry.OnlyGCE,
		Suites:           registry.Suites(registry.Nightly),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runLDAPConnectionLatencyTest(ctx, t, c, numNodes)
		},
	})
}

// runLDAPConnectionLatencyTest creates a local openLDAP server
// and measures the connection latency.
// NOTE: The tests accesses the testdata. Since this is an implicit dependency
// of the test, the test will fail if the roachtest binary is executed
// outside the project directory.
func runLDAPConnectionLatencyTest(
	ctx context.Context, t test.Test, c cluster.Cluster, numNodes int,
) {

	// put the workload binary onto each node
	err := c.PutE(ctx, t.L(), t.DeprecatedWorkload(), "./workload")
	require.NoError(t, err)

	// Initialize default cluster settings for the cluster
	settings := install.MakeClusterSettings()

	// Don't start a backup schedule as this roachtest reports roachperf results.
	err = c.StartE(ctx, t.L(),
		option.NewStartOpts(option.NoBackupSchedule), settings)
	require.NoError(t, err)

	// Prepare the user which will later be used for ldap bind.
	prepareSQLUserForLDAP(ctx, t, c, ldapTestUserName)

	nodeArr := make([]int, numNodes)
	for i := 1; i <= numNodes; i++ {
		nodeArr[i-1] = i
	}
	nodeListOptions := c.Nodes(nodeArr...)

	// Get the hostname, to be used for openLDAP configuration setup.
	hostName := getLDAPHostName(ctx, t, c)

	// These functions are responsible for setting up openLDAP on the cluster.
	installOpenLDAPOnHost(ctx, t, c, nodeListOptions)
	createCertificates(ctx, t, c, nodeListOptions, hostName)
	updateOpenLDAPConfig(ctx, t, c, nodeListOptions)

	// Import the user and groups to make LDAP directory structure,
	// this creates 3 users and adds them as members of the group.
	importUserAndGroupsToLDAPServer(ctx, t, c, nodeListOptions)

	// Setup CRDB specific configs for LDAP authentication.
	// Update the relevant cluster settings for configuring and debugging.
	setupCRDBForLDAPAuth(ctx, t, c, hostName)

	urlTemplate := func(host string) string {
		return fmt.Sprintf("postgresql://%s:%s@localhost:{pgport:1}",
			ldapTestUserName, ldapTestUserPassword)
	}

	// Create the SQL authenticating URL and test the connection.
	testAuthCmd := fmt.Sprintf(
		"./cockroach sql --url \"%s\" --certs-dir=certs", urlTemplate("localhost"))
	err = c.RunE(ctx, option.WithNodes(nodeListOptions), testAuthCmd)
	require.NoError(t, err)

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
	runWorkload(c.Range(1, numNodes), c.Node(1), regionUsCentral)
}

// getLDAPHostName retrieves the hostname of the cluster node
// the result is returned by trimming the trailing '\n'.
func getLDAPHostName(ctx context.Context, t test.Test, c cluster.Cluster) string {
	cmd := "hostname -f"
	out, err := c.RunWithDetailsSingleNode(ctx, t.L(),
		option.WithNodes(c.Node(1)), cmd)
	require.NoError(t, err)

	return strings.ReplaceAll(out.Stdout, "\n", "")
}

// Install the packages to set up openLDAP on ubuntu.
func installOpenLDAPOnHost(
	ctx context.Context, t test.Test, c cluster.Cluster, nodeListOptions option.NodeListOption,
) {
	// debconf-set-selections must be updated to preset the
	// prompts for installing `slapd` silently.
	configCmd := `cat <<EOF | sudo debconf-set-selections
slapd slapd/no_configuration boolean false
slapd slapd/domain string example.com
slapd shared/organization string Example Organization
slapd slapd/backend select HDB
slapd slapd/purge_database boolean true
slapd slapd/move_old_database boolean true
slapd slapd/allow_ldap_v2 boolean false
slapd slapd/password1 password password
slapd slapd/password2 password password
slapd slapd/internal/generated_adminpw string
slapd slapd/internal/adminpw string
EOF`
	err := c.RunE(ctx, option.WithNodes(nodeListOptions), configCmd)
	require.NoError(t, err)

	// run the command to install the relevant packages in non-interactibve mode.
	installCmd := "sudo DEBIAN_FRONTEND=noninteractive apt install -yyq slapd ldap-utils"
	err = c.RunE(ctx, option.WithNodes(nodeListOptions), installCmd)
	require.NoError(t, err)

	// The slapd installation has to be reconfigured as the previous command
	// even after specifically unsetting `slapd/internal/generated_adminpw`
	// and `slapd/internal/adminpw` was generating a randomized passsword
	// for the admin user. Hence, this is a hack to not let that happen
	// and set the admin password as `password`.
	// Same `debconf-set-selections` is used without the last line
	// `slapd slapd/internal/adminpw string` which makes the reconfiguration work
	configCmd = `cat <<EOF | sudo debconf-set-selections
slapd slapd/no_configuration boolean false
slapd slapd/domain string example.com
slapd shared/organization string Example Organization
slapd slapd/backend select HDB
slapd slapd/purge_database boolean true
slapd slapd/move_old_database boolean true
slapd slapd/allow_ldap_v2 boolean false
slapd slapd/password1 password password
slapd slapd/password2 password password
slapd slapd/internal/generated_adminpw string
EOF`
	err = c.RunE(ctx, option.WithNodes(nodeListOptions), configCmd)
	require.NoError(t, err)

	// Reconfigure the slapd, by taking the above inputs.
	reconfigureCmd := "sudo DEBIAN_FRONTEND=noninteractive dpkg-reconfigure slapd"
	err = c.RunE(ctx, option.WithNodes(nodeListOptions), reconfigureCmd)
	require.NoError(t, err)

	// Verify that the installation was successful by
	// authenticating the admin user.
	testSlapdCmd := fmt.Sprintf("ldapwhoami -H ldap://localhost -D \"cn=admin,dc=example,dc=com\" -w %s", ldapAdminPassword)
	err = c.RunE(ctx, option.WithNodes(nodeListOptions), testSlapdCmd)
	require.NoError(t, err)
}

// Create the certificates to run the openLDAP with TLS
func createCertificates(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	nodeListOptions option.NodeListOption,
	hostName string,
) {

	// Create directories to keep the openLDAP specific certificates on the node.
	createFolderCmd := "mkdir certificates my-safe-directory"
	err := c.RunE(ctx, option.WithNodes(nodeListOptions), createFolderCmd)
	require.NoError(t, err)

	// Create a custom CA by using the cockroach commands.
	createCACertCmd := "./cockroach cert create-ca --certs-dir=certificates --ca-key=my-safe-directory/ca.key"
	err = c.RunE(ctx, option.WithNodes(nodeListOptions), createCACertCmd)
	require.NoError(t, err)

	// Create a server certificate using the above created CA cert.
	createServerCertCmd := fmt.Sprintf("./cockroach cert create-node %s --certs-dir=certificates --ca-key=/home/ubuntu/my-safe-directory/ca.key", hostName)
	err = c.RunE(ctx, option.WithNodes(nodeListOptions), createServerCertCmd)
	require.NoError(t, err)

	// Copy the certificates to the default directory for LDAP configuration.
	copyCertCmd := fmt.Sprintf("sudo cp %[1]s/ca.crt %[1]s/node.crt %[1]s/node.key /etc/ssl/certs/", "/home/ubuntu/certificates")
	err = c.RunE(ctx, option.WithNodes(nodeListOptions), copyCertCmd)
	require.NoError(t, err)

	// Give openldap permission to access the certificates in the
	// `/etc/ssl/certs` directory.
	accessPermissionCmd := "sudo chown openldap:openldap /etc/ssl/certs/ca.crt /etc/ssl/certs/node.key /etc/ssl/certs/node.crt"
	err = c.RunE(ctx, option.WithNodes(nodeListOptions), accessPermissionCmd)
	require.NoError(t, err)
}

// Update all the openLDAP configurations and test correctness.
func updateOpenLDAPConfig(
	ctx context.Context, t test.Test, c cluster.Cluster, nodeListOptions option.NodeListOption,
) {

	// Create a ldif file to be used to modify the LDAP config
	// containing all the certificate information for TLS.
	// each of the certificates have action as `replace`
	// which would update if already present else add if the
	// certificates were already present.
	updateCertCmd := `cat <<EOF | sudo tee /etc/ldap/tls.ldif
dn: cn=config
changetype: modify
replace: olcTLSCACertificateFile
olcTLSCACertificateFile: /etc/ssl/certs/ca.crt
-
replace: olcTLSCertificateKeyFile
olcTLSCertificateKeyFile: /etc/ssl/certs/node.key
-
replace: olcTLSCertificateFile
olcTLSCertificateFile: /etc/ssl/certs/node.crt
EOF`
	err := c.RunE(ctx, option.WithNodes(nodeListOptions), updateCertCmd)
	require.NoError(t, err)

	// Use the above created file to modify the ldap config.
	ldapModifyCmd := "sudo ldapmodify -Y EXTERNAL -H ldapi:/// -f /etc/ldap/tls.ldif"
	err = c.RunE(ctx, option.WithNodes(nodeListOptions), ldapModifyCmd)
	require.NoError(t, err)

	// Configure the Uncomplicated Firewall (UFW) to allow incoming
	// connections on port 636
	enableFirewallCmd := "sudo ufw allow 636"
	err = c.RunE(ctx, option.WithNodes(nodeListOptions), enableFirewallCmd)
	require.NoError(t, err)

	// Update the file /etc/default/slapd, to contain
	// SLAPD_SERVICES="ldap:/// ldaps:/// ldapi:///"
	// this enables connection over LDAP with TLS.
	updateSlapdConfigCmd := "sudo sed -i 's/^SLAPD_SERVICES=.*/" +
		"SLAPD_SERVICES=\"ldap:\\/\\/\\/ ldapi:\\/\\/\\/ " +
		"ldaps:\\/\\/\\/\"/' /etc/default/slapd"
	err = c.RunE(ctx, option.WithNodes(nodeListOptions), updateSlapdConfigCmd)
	require.NoError(t, err)

	// Copy the CA certificate to `/usr/local/share/ca-certificates/`
	// to add it as a trusted CA for the host node.
	trustedCACertCmd := "sudo cp /etc/ssl/certs/ca.crt /usr/local/share/ca-certificates/"
	err = c.RunE(ctx, option.WithNodes(nodeListOptions), trustedCACertCmd)
	require.NoError(t, err)

	// Add the custom CA created before as a trusted CA cert for the host node.
	updateCACertCmd := "sudo update-ca-certificates"
	err = c.RunE(ctx, option.WithNodes(nodeListOptions), updateCACertCmd)
	require.NoError(t, err)

	// Restart the slapd service running in the background
	// to apply all the configurations changed above.
	restartSlapdCmd := "sudo systemctl restart slapd"
	err = c.RunE(ctx, option.WithNodes(nodeListOptions), restartSlapdCmd)
	require.NoError(t, err)

	// Test the ldap connection over TLS
	cmd := "sudo ldapsearch -H ldaps://localhost -x -b \"\" -s base -LLL -d 1"
	_, err = c.RunWithDetailsSingleNode(ctx, t.L(),
		option.WithNodes(c.Node(1)), cmd)
	require.NoError(t, err)
}

// Create the directory structure for openLDAP and test LDAP bind
// using the `ldapwhoami` command.
func importUserAndGroupsToLDAPServer(
	ctx context.Context, t test.Test, c cluster.Cluster, nodeListOptions option.NodeListOption,
) {
	// Copy the base structure file onto the node
	content := getTestDataFileContent(
		t, "./pkg/cmd/roachtest/testdata/ldap_base_structure.ldif")
	err := c.PutString(ctx, content, "base_structure.ldif", 0755)
	require.NoError(t, err)

	// Apply the base structure onto the running LDAP service.
	baseStructureAddCmd := fmt.Sprintf("ldapadd -x -D \"cn=admin,dc=example,dc=com\" -w %s -f base_structure.ldif", ldapAdminPassword)
	err = c.RunE(ctx, option.WithNodes(nodeListOptions), baseStructureAddCmd)
	require.NoError(t, err)

	// Copy the user and groups file onto the node.
	content = getTestDataFileContent(
		t, "./pkg/cmd/roachtest/testdata/ldap_user_group.ldif")
	err = c.PutString(ctx, content, "ldap_user_group.ldif", 0755)
	require.NoError(t, err)

	// Apply the user and group creation onto the running LDAP service.
	structureAddCmd := fmt.Sprintf("ldapadd -x -D \"cn=admin,dc=example,dc=com\" -w %s -f ldap_user_group.ldif", ldapAdminPassword)
	err = c.RunE(ctx, option.WithNodes(nodeListOptions), structureAddCmd)
	require.NoError(t, err)

	testImportCmd := "ldapsearch -x -LLL -b \"dc=example,dc=com\" \"(objectClass=*)\""
	err = c.RunE(ctx, option.WithNodes(nodeListOptions), testImportCmd)
	require.NoError(t, err)

	testBindCmd := fmt.Sprintf("ldapwhoami -x -H ldaps://localhost -D \"cn=John Doe,ou=Users,dc=example,dc=com\" -w %s", ldapAdminPassword)
	err = c.RunE(ctx, option.WithNodes(nodeListOptions), testBindCmd)
	require.NoError(t, err)
}

// Setup CRDB specific configs for LDAP authentication.
// Update the relevant cluster settings for configuring and debugging.
func setupCRDBForLDAPAuth(ctx context.Context, t test.Test, c cluster.Cluster, hostName string) {
	// Connect to the node using the root user to update the
	// cluster settings for LDAP.
	conn := createPgxConnWithExternalURL(ctx, t, c)

	// read and set the HBA conf for LDAP bind of the user `jdoe`.
	// this also retains the login of roachprod user using
	// password authentication as that is required for roachtest.
	hba := fmt.Sprintf(getTestDataFileContent(
		t, "./pkg/cmd/roachtest/testdata/ldap_authentication_hba_conf"),
		hostName, ldapTestUserPassword)
	hbaQuery := createClusterSettingQuery(
		"server.host_based_authentication.configuration", fmt.Sprintf("'%s'", hba))
	runSQLQueryForConnection(ctx, t, conn, hbaQuery)

	// Get the ca.crt used to setup TLS on the openLDAP server.
	getCACertCmd := "cd certificates && cat ca.crt"
	out, err := c.RunWithDetailsSingleNode(ctx, t.L(),
		option.WithNodes(c.Node(1)), getCACertCmd)
	require.NoError(t, err)
	trimmed := strings.TrimSuffix(out.Stdout, "\n")

	caCertQuery := createClusterSettingQuery(
		"server.ldap_authentication.domain.custom_ca", fmt.Sprintf("'%s'", trimmed))
	runSQLQueryForConnection(ctx, t, conn, caCertQuery)

	// This is required for better debugging in-case of failures in authentication.
	debuggingQuery := createClusterSettingQuery(
		"server.auth_log.sql_sessions.enabled", "true")
	runSQLQueryForConnection(ctx, t, conn, debuggingQuery)
}

// runSQLQueryForConnection uses the provided psql connection
// to execute the given query.
func runSQLQueryForConnection(ctx context.Context, t test.Test, conn *pgx.Conn, query string) {
	row, err := conn.Query(ctx, query)
	require.NoError(t, err)
	// The row must be closed for the connection to be used again
	row.Close()
}

// createClusterSettingQuery returns the template for updating
// cluster settings.
func createClusterSettingQuery(key, value string) string {
	return fmt.Sprintf("SET cluster setting %s=%s", key, value)
}

// prepareUserForLDAP creates a SQL user and grants admin privilege
// to the user. The `ldapUserName` must be same as the username
// on the LDAP server.
func prepareSQLUserForLDAP(
	ctx context.Context, t test.Test, c cluster.Cluster, ldapUserName string,
) {
	// Connect to the node to create the required SQL users
	// which will be authenticated using LDAP.
	conn := createPgxConnWithExternalURL(ctx, t, c)

	runSQLQueryForConnection(ctx, t, conn,
		fmt.Sprintf("CREATE ROLE %s LOGIN", ldapUserName))

	// connectionlatency workload checks the presence of the database named
	// `connectionlatency`, if not present it creates it using the logged-in user
	// Hence for ease of use, giving admin privilege to the user.
	runSQLQueryForConnection(ctx, t, conn,
		fmt.Sprintf("GRANT admin to %s", ldapUserName))
}

// createPgxConnWithExternalURL returns a psql connection
// using the cluster's external url.
func createPgxConnWithExternalURL(ctx context.Context, t test.Test, c cluster.Cluster) *pgx.Conn {
	pgURL, err := c.ExternalPGUrl(ctx, t.L(),
		c.Node(1), roachprod.PGURLOptions{})
	require.NoError(t, err)
	require.NotEmpty(t, pgURL)
	conn, err := pgx.Connect(ctx, pgURL[0])
	require.NoError(t, err)
	return conn
}
