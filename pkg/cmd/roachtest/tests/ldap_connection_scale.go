// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

const (
	ldapUser1FullName     = "First1 Last1"
	ldapScaleUserCount    = 2500
	maxParallelGoRoutines = 100
)

func registerLDAPConnectionScaleTest(r registry.Registry) {
	// Single region test.
	numNodes := 1
	r.Add(registry.TestSpec{
		Name:      "ldap_connection_scale",
		Owner:     registry.OwnerProductSecurity,
		Benchmark: true,
		Cluster:   r.MakeClusterSpec(numNodes, spec.GCEZones(regionUsCentral)),
		// currently env. var is only set for GCE nightly runs
		CompatibleClouds: registry.OnlyGCE,
		Suites:           registry.Suites(registry.Nightly),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			// dummy test
			runLDAPConnectionScaleTest(ctx, t, c, numNodes)
		},
	})
}

// runLDAPConnectionScaleTest creates a local openLDAP server
// and creates multiple parallel connection to test scale.
// NOTE: The tests accesses the testdata. Since this is an implicit dependency
// of the test, the test will fail if the roachtest binary is executed
// outside the project directory.
func runLDAPConnectionScaleTest(ctx context.Context, t test.Test, c cluster.Cluster, numNodes int) {

	// Initialize default cluster settings for the cluster
	settings := install.MakeClusterSettings()

	// Don't start a backup schedule as this roachtest reports roachperf results.
	err := c.StartE(ctx, t.L(),
		option.NewStartOpts(option.NoBackupSchedule), settings)
	require.NoError(t, err)

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

	// Prepare user and group LDIF file, which is imported
	// into the LDAP server.
	userGroupFileContent, userUidList := prepareUserAndGroups()
	importUserAndGroupsForLDAPScale(ctx, t, c, nodeListOptions, userGroupFileContent)

	// Create the users with login role which will later
	// used for authentication.
	prepareSQLUserForLDAPScale(ctx, t, c)

	// Setup CRDB specific configs for LDAP authentication.
	// Update the relevant cluster settings for configuring and debugging.
	setupCRDBFOrLDAPScale(ctx, t, c, hostName)

	// Test that `ldapScaleUserCount` number of connections
	// can be created in parallel.
	testParallelConnections(ctx, t, c, userUidList, numNodes)
}

// prepareUserAndGroups prepares user and group LDIF file content.
// The function creates user ldif structure for `ldapScaleUserCount`
// number of users. Each user is then added as a member of the group.
// It returns the content of the ldif file as string and the
// list of uid's of the users.
func prepareUserAndGroups() (string, []string) {
	// Define constants for user attributes
	const (
		baseDN    = "ou=Users,dc=example,dc=com"
		gidNumber = 5000
		// SSHA encoded password for `password`
		password   = "{SSHA}UweAl2O1Zh95nijbT+SaQB5FuaHi7xnE"
		loginShell = "/bin/bash"
	)

	// Preserve the list of all uid's of the user created
	// to be used later for authentication.
	userUidList := make([]string, 0)

	// Create one group and make all users a part of the same group.
	groupString := fmt.Sprintf("# Group: Developers\n" +
		"dn: cn=Developers,ou=Groups,dc=example,dc=com\n" +
		"objectClass: top\n" +
		"objectClass: groupOfUniqueNames\n" +
		"cn: Developers\n" +
		"description: Group for software development team members\n")

	userString := ""
	for i := 1; i <= ldapScaleUserCount; i++ {
		uid := fmt.Sprintf("user%d", i)
		userUidList = append(userUidList, uid)
		firstName := fmt.Sprintf("First%d", i)
		lastName := fmt.Sprintf("Last%d", i)
		fullName := fmt.Sprintf("%s %s", firstName, lastName)
		mail := fmt.Sprintf("%s@example.com", uid)
		homeDirectory := fmt.Sprintf("/home/%s", uid)
		uidNumber := 1000 + i

		// Constructs the user ldif structure using the above formatted inputs.
		userString = userString + fmt.Sprintf("# User %d: %s\n"+
			"dn: cn=%s,%s\n"+
			"objectClass: top\n"+
			"objectClass: inetOrgPerson\n"+
			"objectClass: posixAccount\n"+
			"objectClass: shadowAccount\n"+
			"uid: %s\n"+
			"cn: %s\n"+
			"sn: %s\n"+
			"givenName: %s\n"+
			"displayName: %s\n"+
			"mail: %s\n"+
			"uidNumber: %d\n"+
			"gidNumber: %d\n"+
			"homeDirectory: %s\n"+
			"loginShell: %s\n"+
			"userPassword: %s\n\n",
			i, fullName, fullName, baseDN, uid, fullName, lastName, firstName,
			fullName, mail, uidNumber, gidNumber, homeDirectory, loginShell, password)

		// Add the user's DN to the membership property of the group.
		groupMemberString := fmt.Sprintf("uniqueMember: cn=%s,%s\n", fullName, baseDN)
		groupString = groupString + groupMemberString
	}

	return userString + groupString, userUidList
}

// Create the directory structure for openLDAP by importing the created file
// and test LDAP bind using the `ldapwhoami` command.
func importUserAndGroupsForLDAPScale(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	nodeListOptions option.NodeListOption,
	userGroupFileContent string,
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
	err = c.PutString(ctx, userGroupFileContent, "ldap_user_group.ldif", 0755)
	require.NoError(t, err)

	// Apply the user and group creation onto the running LDAP service.
	structureAddCmd := fmt.Sprintf("ldapadd -x -D \"cn=admin,dc=example,dc=com\" -w %s -f ldap_user_group.ldif", ldapAdminPassword)
	err = c.RunE(ctx, option.WithNodes(nodeListOptions), structureAddCmd)
	require.NoError(t, err)

	testBindCmd := fmt.Sprintf("ldapwhoami -x -H ldaps://localhost -D \"cn=%s,ou=Users,dc=example,dc=com\" -w %s", ldapUser1FullName, ldapAdminPassword)
	err = c.RunE(ctx, option.WithNodes(nodeListOptions), testBindCmd)
	require.NoError(t, err)
}

// prepareSQLUserForLDAPScale runs a cockroach command to
// log in to the sql shell using the root user credentials.
// Then create role with the name in format user<num>
// using CRDB sql syntax of for loop.
func prepareSQLUserForLDAPScale(ctx context.Context, t test.Test, c cluster.Cluster) {
	query := fmt.Sprintf("./cockroach sql --certs-dir=%s "+
		"--host=localhost:{pgport:1} -e '\\| for ((i=%d;i<=%d;++i)); "+
		"do echo \"CREATE ROLE user$i LOGIN;\"; done'",
		install.CockroachNodeCertsDir, 1, ldapScaleUserCount)
	err := c.RunE(ctx, option.WithNodes(c.Node(1)), query)
	require.NoError(t, err)
	t.L().Printf("Created all login roles in crdb.")
}

// Setup CRDB specific configs for LDAP authentication.
// Update the relevant cluster settings for configuring and debugging.
func setupCRDBFOrLDAPScale(ctx context.Context, t test.Test, c cluster.Cluster, hostName string) {
	// Connect to the node using the root user to update the
	// cluster settings for LDAP.
	conn := createPgxConnWithExternalURL(ctx, t, c)

	// Set the HBA conf for LDAP bind of all the users.
	// This also retains the login of roachprod user using
	// password authentication as that is required for roachtest.
	userHBALine := getHBAForUser(ldapUser1FullName, hostName)
	hbaString := "host    all           roachprod           0.0.0.0/0          password\n"
	hbaString = hbaString + userHBALine
	hbaString = hbaString + "host    all           root           0.0.0.0/0          password\n"

	hbaQuery := createClusterSettingQuery(
		"server.host_based_authentication.configuration", fmt.Sprintf("'%s'", hbaString))
	runSQLQueryForConnection(ctx, t, conn, hbaQuery)

	// Get the ca.crt used to set up TLS on the openLDAP server.
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

// getHBAForUser creates HBA conf. for the user and the respective host.
func getHBAForUser(userFullName, hostname string) string {
	return fmt.Sprintf("host    all           all            all           "+
		"      ldap ldapserver=%s ldapport=636 "+
		"\"ldapbasedn=OU=Users,DC=example,DC=com\" "+
		"\"ldapbinddn=CN=%s,OU=Users,DC=example,DC=com\" ldapbindpasswd=%s "+
		"ldapsearchattribute=uid \"ldapsearchfilter=(mail=*)\"\n",
		hostname, userFullName, ldapTestUserPassword)
}

// getPostgresConnectionURL gets the connection url
// of the users created for ldap authentication having
// password as the constant `ldapTestUserPassword`.
func getPostgresConnectionURL(host string, userUid string, port int) string {
	return fmt.Sprintf("postgresql://%s:%s@%s:%d",
		userUid, ldapTestUserPassword, host, port)
}

func testParallelConnections(
	ctx context.Context, t test.Test, c cluster.Cluster, userUidList []string, numNodes int,
) {
	// The connection to postgres client is not done directly via roachprod commands
	// hence fetching the port and replacing it manually in the connection URL.
	// Roachprod command replaces the {pgport:<nodeNum>} with the port on
	// which the SQL service runs, due to the usage of pgx.Conn() this has been done via code.
	ports, err := c.SQLPorts(ctx, t.L(), c.CRDBNodes(), "" /* tenant */, 0 /* sqlInstance */)
	require.NoError(t, err)
	require.NotEmpty(t, ports)

	externalIps, err := c.ExternalIP(ctx, t.L(), c.Range(1, numNodes))
	require.NoError(t, err)
	require.NotEmpty(t, externalIps)

	// waitGroup tracking that `ldapScaleUserCount` number
	// of connections have been created.
	var wg sync.WaitGroup
	wg.Add(ldapScaleUserCount)

	// This ensures that not more than `maxParallelGoRoutines`
	// go routines have been spawned at a time to be
	// under the memory limit of the node created.
	semaphore := make(chan struct{}, maxParallelGoRoutines)
	// save the list of connection created to be destroyed later ensuring
	// that `ldapScaleUserCount` number of connections are alive in parallel.
	connectionList := make([]*pgx.Conn, ldapScaleUserCount)

	// Function to create the connection and signal to the workGroup
	// that connection creation and validation is complete.
	createConnectionFunc := func(index int) {
		defer wg.Done()

		// Get the connection url for the designated user at `index`.
		currUrl := getPostgresConnectionURL(
			externalIps[0], userUidList[index], ports[0])
		conn, err := pgx.Connect(ctx, currUrl)
		require.NoError(t, err)

		// preserve the connection List
		connectionList[index] = conn

		// validate that the connection is indeed functional
		_, err = conn.Exec(ctx, "SELECT 1")
		require.NoError(t, err)
	}

	// Create `ldapScaleUserCount` number of connections
	// in parallel ensuring not more than `maxParallelGoRoutines`
	// goroutines are running at once.
	for i := 0; i < ldapScaleUserCount; i++ {
		t.L().Printf("Creating connection: %d.", i+1)
		semaphore <- struct{}{}

		go func(index int) {
			defer func() { <-semaphore }()
			createConnectionFunc(index)
		}(i)
	}

	// Ensure all connections have been created.
	wg.Wait()
	for i := 0; i < maxParallelGoRoutines; i++ {
		// Ensure all go Routines have exited
		semaphore <- struct{}{}
	}
	t.L().Printf("All Connections created.")

	// Close all the created connections post test validation.
	for index, conn := range connectionList {
		if conn != nil {
			err := conn.Close(ctx)
			require.NoError(t, err)
		} else {
			t.L().Printf("Connection %d is nil", index)
		}
	}
	t.L().Printf("All Connections closed.")
}
