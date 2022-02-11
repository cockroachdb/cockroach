#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_test "Check \\demo commands work as expected"
# Start a demo with 5 nodes. Set multitenant=false due to unsupported
# gossip commands below.
spawn $argv demo movr --nodes=5 --multitenant=false

# Ensure db is movr.
eexpect "movr>"

# Wrong number of args
send "\\demo node\r"
eexpect "invalid syntax: \\\\demo node. Try \\\\? for help."
eexpect "movr>"

# Cannot shutdown node 1
send "\\demo shutdown 1\r"
eexpect "cannot shutdown node 1"
eexpect "movr>"

# Cannot operate on a node which does not exist.
send "\\demo shutdown 8\r"
eexpect "node 8 does not exist"
send "\\demo restart 8\r"
eexpect "node 8 does not exist"
send "\\demo decommission 8\r"
eexpect "node 8 does not exist"
send "\\demo recommission 8\r"
eexpect "node 8 does not exist"
eexpect "movr>"

# Cannot restart a node that is not shut down.
send "\\demo restart 2\r"
eexpect "node 2 is already running"
eexpect "movr>"

# Shut down a separate node.
send "\\demo shutdown 3\r"
eexpect "node 3 has been shutdown"
eexpect "movr>"

send "select node_id, draining, decommissioning, membership from crdb_internal.gossip_liveness ORDER BY node_id;\r"
eexpect "1 |  false   |      false      | active"
eexpect "2 |  false   |      false      | active"
eexpect "3 |   true   |      false      | active"
eexpect "4 |  false   |      false      | active"
eexpect "5 |  false   |      false      | active"
eexpect "movr>"

# Cannot shut it down again.
send "\\demo shutdown 3\r"
eexpect "node 3 is already shut down"
eexpect "movr>"

# Expect queries to still work with just one node down.
send "SELECT count(*) FROM movr.rides;\r"
eexpect "500"
eexpect "movr>"

# Now restart the node.
send "\\demo restart 3\r"
eexpect "node 3 has been restarted"
eexpect "movr>"

send "select node_id, draining, decommissioning, membership from crdb_internal.gossip_liveness ORDER BY node_id;\r"
eexpect "1 |  false   |      false      | active"
eexpect "2 |  false   |      false      | active"
eexpect "3 |  false   |      false      | active"
eexpect "4 |  false   |      false      | active"
eexpect "5 |  false   |      false      | active"
eexpect "movr>"

# Try commissioning commands
send "\\demo decommission 4\r"
eexpect "node 4 has been decommissioned"
eexpect "movr>"

send "select node_id, draining, decommissioning, membership from crdb_internal.gossip_liveness ORDER BY node_id;\r"
eexpect "1 |  false   |      false      | active"
eexpect "2 |  false   |      false      | active"
eexpect "3 |  false   |      false      | active"
eexpect "4 |  false   |      true       | decommissioned"
eexpect "5 |  false   |      false      | active"
eexpect "movr>"

send "\\demo recommission 4\r"
eexpect "can only recommission a decommissioning node"
eexpect "movr>"

send "\\demo add blah\r"
eexpect "internal server error: tier must be in the form \"key=value\" not \"blah\""
eexpect "movr>"

send "\\demo add region=ca-central,zone=a\r"
eexpect "node 6 has been added with locality \"region=ca-central,zone=a\""
eexpect "movr>"

send "show regions from cluster;\r"
eexpect "ca-central | \{a\}"
eexpect "us-east1   | \{b,c,d\}"
eexpect "us-west1   | \{b\}"
eexpect "movr>"

# We use kv_node_status here because gossip_liveness is timing dependant.
# Node 4's status entry should have been removed by now.
send "select node_id, locality from crdb_internal.kv_node_status;\r"
eexpect "1 | region=us-east1,az=b"
eexpect "2 | region=us-east1,az=c"
eexpect "3 | region=us-east1,az=d"
eexpect "5 | region=us-west1,az=b"
eexpect "6 | region=ca-central,zone=a"
eexpect "movr>"

# Shut down the newly created node.
send "\\demo shutdown 6\r"
eexpect "node 6 has been shutdown"
eexpect "movr>"

# By now the node should have stabilized in gossip which allows us to query the more detailed information there.
send "select node_id, draining, decommissioning, membership from crdb_internal.gossip_liveness ORDER BY node_id;\r"
eexpect "1 |  false   |      false      | active"
eexpect "2 |  false   |      false      | active"
eexpect "3 |  false   |      false      | active"
eexpect "4 |  false   |      true       | decommissioned"
eexpect "5 |  false   |      false      | active"
eexpect "6 |   true   |      false      | active"
eexpect "movr>"

send_eof
eexpect eof
end_test
