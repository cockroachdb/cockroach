#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_test "Check \\demo commands work as expected"
# Start a demo with 5 nodes. Set multitenant=false due to unsupported
# gossip commands below.
spawn $argv demo --empty --no-line-editor --nodes=5 --multitenant=false --log-dir=logs

eexpect "defaultdb>"

# Wrong number of args
send "\\demo node\r"
eexpect "invalid syntax: \\\\demo node"
eexpect "Try \\\\? for help."
eexpect "defaultdb>"

# Cannot shutdown node 1
send "\\demo shutdown 1\r"
eexpect "cannot shutdown node 1"
eexpect "defaultdb>"

# Cannot operate on a node which does not exist.
send "\\demo shutdown 8\r"
eexpect "node 8 does not exist"
eexpect "defaultdb>"
send "\\demo restart 8\r"
eexpect "node 8 does not exist"
eexpect "defaultdb>"
send "\\demo decommission 8\r"
eexpect "node 8 does not exist"
eexpect "defaultdb>"

# Cannot restart a node that is not shut down.
send "\\demo restart 2\r"
eexpect "node 2 is already running"
eexpect "defaultdb>"

# Shut down a separate node.
send "\\demo shutdown 3\r"
eexpect "node 3 has been shutdown"
eexpect "defaultdb>"

send "select node_id, draining, membership from crdb_internal.kv_node_liveness ORDER BY node_id;\r"
eexpect "1 |    f     | active"
eexpect "2 |    f     | active"
eexpect "3 |    t     | active"
eexpect "4 |    f     | active"
eexpect "5 |    f     | active"
eexpect "defaultdb>"

# Cannot shut it down again.
send "\\demo shutdown 3\r"
eexpect "node 3 is already shut down"
eexpect "defaultdb>"

# Expect queries to still work with just one node down.
send "SELECT count(*) FROM system.users;\r"
eexpect "1"
eexpect "defaultdb>"

# Now restart the node, and wait for liveness to update.
# See: https://github.com/cockroachdb/cockroach/issues/122134#issuecomment-2050667479.
send "\\demo restart 3\r"
eexpect "node 3 has been restarted"
eexpect "defaultdb>"

set timeout 1
set stmt "select node_id, draining, membership from crdb_internal.kv_node_liveness ORDER BY node_id;\r"
send $stmt
expect {
    "3 |    f     | active" {
        puts "\rfound n3 active and no longer draining"
    }
    timeout {
        puts "\rdid not see n3 active and no longer draining - retrying"
        sleep 2
        send $stmt
        exp_continue
    }
}
# Reset timeout back to 45 to match common.tcl.
set timeout 45

send "select node_id, draining, membership from crdb_internal.kv_node_liveness ORDER BY node_id;\r"
eexpect "1 |    f     | active"
eexpect "2 |    f     | active"
eexpect "3 |    f     | active"
eexpect "4 |    f     | active"
eexpect "5 |    f     | active"
eexpect "defaultdb>"

# Try decommissioning commands
send "\\demo decommission 4\r"
eexpect "node is draining"
eexpect "node 4 has been decommissioned"
eexpect "defaultdb>"

send "select node_id, draining, membership from crdb_internal.kv_node_liveness ORDER BY node_id;\r"
eexpect "1 |    f     | active"
eexpect "2 |    f     | active"
eexpect "3 |    f     | active"
eexpect "4 |    t     | decommissioned"
eexpect "5 |    f     | active"
eexpect "defaultdb>"

send "\\demo add blah\r"
eexpect "internal server error: tier must be in the form \"key=value\" not \"blah\""
eexpect "defaultdb>"

send "\\demo add region=ca-central,zone=a\r"
eexpect "node 6 has been added with locality \"region=ca-central,zone=a\""
eexpect "defaultdb>"

send "show regions from cluster;\r"
eexpect "ca-central | \{a\}"
eexpect "us-east1   | \{b,c,d\}"
eexpect "us-west1   | \{b\}"
eexpect "defaultdb>"

# We use kv_node_status here because gossip_liveness is timing dependant.
# Node 4's status entry should have been removed by now.
send "select node_id, locality from crdb_internal.kv_node_status;\r"
eexpect "1 | region=us-east1,az=b"
eexpect "2 | region=us-east1,az=c"
eexpect "3 | region=us-east1,az=d"
eexpect "5 | region=us-west1,az=b"
eexpect "6 | region=ca-central,zone=a"
eexpect "defaultdb>"

# Shut down the newly created node.
send "\\demo shutdown 6\r"
set timeout 120
eexpect "node 6 has been shutdown"
set timeout 30
eexpect "defaultdb>"

# NB: use kv_node_liveness to avoid flakes due to gossip delays.
# See https://github.com/cockroachdb/cockroach/issues/76391
send "select node_id, draining, membership from crdb_internal.kv_node_liveness ORDER BY node_id;\r"
eexpect "1 |    f     | active"
eexpect "2 |    f     | active"
eexpect "3 |    f     | active"
eexpect "4 |    t     | decommissioned"
eexpect "5 |    f     | active"
eexpect "6 |    t     | active"
eexpect "defaultdb>"

send "\\demo restart 4\r"
eexpect "node 4 is permanently decommissioned"
eexpect "defaultdb>"
send "\\demo shutdown 4\r"
eexpect "node 4 is permanently decommissioned"
eexpect "defaultdb>"
send "\\demo decommission 4\r"
eexpect "node 4 is permanently decommissioned"
eexpect "defaultdb>"

send "\\q\r"
eexpect eof
end_test
