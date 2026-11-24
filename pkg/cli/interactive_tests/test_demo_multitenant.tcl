#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

set ::env(COCKROACH_DEMO_PASSWORD) "hunter2hunter2hunter2hunter2"

spawn $argv demo --no-line-editor --empty --nodes 5 --multitenant --log-dir=logs

start_test "Check that the demo cluster starts properly"
eexpect "Welcome"
eexpect "You are connected to tenant \"demoapp\""
eexpect "(sql)"
eexpect "127.0.0.1:26257/defaultdb"
eexpect "defaultdb>"
end_test

start_test "Check that the demo ls command lists the tenants"
send "\\demo ls\r"
eexpect "Application tenant"
eexpect "127.0.0.1:26257/defaultdb"
eexpect "rpc"
eexpect "127.0.0.1:26357"

eexpect "System tenant"
eexpect "127.0.0.1:26257/defaultdb"
eexpect "rpc"
eexpect "127.0.0.1:26360"

eexpect "defaultdb>"
end_test

# Ideally, we'd also check that you can connect to each of the tenants
# with \connect. But, it's a little annoying to try to parse the randomly
# generated URL strings, and we also have unit tests exercising that
# functionality, so for now this is just a smoke test that ensures that
# the mt demo can at least start up.


start_test "Check the default gateway region"
send "SELECT gateway_region();\n"
eexpect "us-east1"
eexpect "defaultdb>"
end_test

start_test "Check that the demo add command adds a node with no error in ls"
send "\\demo add region=us-west1\r"
eexpect "node 6 has been added"
eexpect "defaultdb>"
send "\\demo ls\r"
eexpect "node 1:"
eexpect "node 2:"
eexpect "node 3:"
eexpect "node 4:"
eexpect "node 5:"
eexpect "node 6:"
eexpect "Application tenant:"
eexpect "System tenant:"
eexpect "defaultdb>"
end_test

start_test "Check that a node can be restarted"

# Switch to system tenant to check the liveness range.
send "\\connect - - - - tenant=system\r"
eexpect "the cluster ID has changed!"
eexpect "defaultdb>"

# Wait for the liveness range to have the default 5 voters. If its replication
# factor is too low, shutting down the node below can cause it to lose quorum
# and stall the shutdown command (example: #147867).
set timeout 2
set stmt "select range_id, array_length(voting_replicas,1) from crdb_internal.ranges where range_id=2;\r"
send $stmt
expect {
    "2 |            5" {
        puts "\rliveness range has 5 voters"
    }
    timeout {
        puts "\rliveness range does not yet have 5 voters"
        sleep 2
        send $stmt
        exp_continue
    }
}
# Reset timeout back to 45 to match common.tcl.
set timeout 45
eexpect "defaultdb>"

# Switch back to application tenant for the rest of the test.
send "\\connect - - - - tenant=demoapp\r"
eexpect "the cluster ID has changed!"
eexpect "defaultdb>"

send "\\demo shutdown 6\r"
eexpect "node 6 has been shutdown"
eexpect "defaultdb>"
send "\\demo restart 6\r"
eexpect "node 6 has been restarted"
eexpect "defaultdb>"
send "\\demo ls\r"
eexpect "node 1:"
eexpect "node 2:"
eexpect "node 3:"
eexpect "node 4:"
eexpect "node 5:"
eexpect "node 6:"
eexpect "Application tenant:"
eexpect "System tenant:"
eexpect "defaultdb>"
end_test

send_eof
eexpect eof
end_test
