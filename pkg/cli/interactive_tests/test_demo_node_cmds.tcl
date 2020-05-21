#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_test "Check \\demo commands work as expected"
# Start a demo with 5 nodes.
spawn $argv demo movr --nodes=5

# Ensure db is movr.
eexpect "movr>"

# Wrong number of args
send "\\demo node\r"
eexpect "\\demo expects 2 parameters"

# Cannot shutdown node 1
send "\\demo shutdown 1\r"
eexpect "cannot shutdown node 1"

# Cannot operate on a node which does not exist.
send "\\demo shutdown 8\r"
eexpect "node 8 does not exist"
send "\\demo restart 8\r"
eexpect "node 8 does not exist"
send "\\demo decommission 8\r"
eexpect "node 8 does not exist"
send "\\demo recommission 8\r"
eexpect "node 8 does not exist"

# Cannot restart a node that is not shut down.
send "\\demo restart 2\r"
eexpect "node 2 is already running"

# Shut down a separate node.
send "\\demo shutdown 3\r"
eexpect "node 3 has been shutdown"

send "select node_id, draining, decommissioning from crdb_internal.gossip_liveness ORDER BY node_id;\r"
eexpect "1 |  false   |      false"
eexpect "2 |  false   |      false"
eexpect "3 |   true   |      false"
eexpect "4 |  false   |      false"
eexpect "5 |  false   |      false"

# Cannot shut it down again.
send "\\demo shutdown 3\r"
eexpect "node 3 is already shut down"

# Expect queries to still work with just one node down.
send "SELECT count(*) FROM movr.rides;\r"
eexpect "500"
eexpect "movr>"

# Now restart the node.
send "\\demo restart 3\r"
eexpect "node 3 has been restarted"

send "select node_id, draining, decommissioning from crdb_internal.gossip_liveness ORDER BY node_id;\r"
eexpect "1 |  false   |      false"
eexpect "2 |  false   |      false"
eexpect "3 |  false   |      false"
eexpect "4 |  false   |      false"
eexpect "5 |  false   |      false"

# Try commissioning commands
send "\\demo decommission 4\r"
eexpect "node 4 has been decommissioned"

send "select node_id, draining, decommissioning from crdb_internal.gossip_liveness ORDER BY node_id;\r"
eexpect "1 |  false   |      false"
eexpect "2 |  false   |      false"
eexpect "3 |  false   |      false"
eexpect "4 |  false   |      true"
eexpect "5 |  false   |      false"

send "\\demo recommission 4\r"
eexpect "node 4 has been recommissioned"

send "select node_id, draining, decommissioning from crdb_internal.gossip_liveness ORDER BY node_id;\r"
eexpect "1 |  false   |      false"
eexpect "2 |  false   |      false"
eexpect "3 |  false   |      false"
eexpect "4 |  false   |      false"
eexpect "5 |  false   |      false"

interrupt
eexpect eof
end_test
