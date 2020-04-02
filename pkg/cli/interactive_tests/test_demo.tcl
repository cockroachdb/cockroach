#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_test "Check that demo insecure says hello properly"
spawn $argv demo --insecure
# Be polite.
eexpect "Welcome"
# Warn the user that they won't get persistence.
eexpect "your changes to data stored in the demo session will not be saved!"
# Inform the necessary URL.
eexpect "Web UI: http:"
# Ensure same messages as cockroach sql.
eexpect "Server version"
eexpect "Cluster ID"
eexpect "brief introduction"
# Ensure user is root.
eexpect root@
# Ensure db is movr.
eexpect "movr>"
end_test

start_test "Check that demo secure says hello properly"
spawn $argv demo
# Be polite.
eexpect "Welcome"
# Warn the user that they won't get persistence.
eexpect "your changes to data stored in the demo session will not be saved!"
# Inform the necessary URL.
eexpect "Web UI: http:"
# Ensure that user login information is present.
eexpect "The user \"root\" with password \"admin\" has been created."
# Ensure same messages as cockroach sql.
eexpect "Server version"
eexpect "Cluster ID"
eexpect "brief introduction"
# Ensure user is root.
eexpect root@
# Ensure db is movr.
eexpect "movr>"
end_test

# Test that demo displays connection URLs for nodes in the cluster.
start_test "Check that node URLs are displayed"
spawn $argv demo --insecure
# Check that we see our message.
eexpect "Connect to the cluster on a SQL shell at"

# Start the test again with a multi node cluster.
spawn $argv demo --insecure --nodes 3

# Check that we get a message for each node.
eexpect "Connect to different nodes in the cluster on a SQL shell at"
eexpect "Node 1"
eexpect "Node 2"
eexpect "Node 3"

spawn $argv demo --insecure=false
eexpect "Connect to the cluster on a SQL shell at"
# Expect that security related tags are part of the connection URL.
eexpect "sslcert="
eexpect "sslkey="
eexpect "sslrootcert="

end_test

