#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_test "Check demo shutdown and restart works as expected"
# Start a demo with no nodes.
spawn $argv demo movr --nodes=7

# Ensure db is movr.
eexpect "movr>"

# Cannot shutdown node 1
send "\\demo_node shutdown 1\n"
eexpect "cannot shutdown node 1"

# Cannot shutdown or restart a node which does not exist.
send "\\demo_node shutdown 8\n"
eexpect "node 8 does not exist"
send "\\demo_node restart 8\n"
eexpect "node 8 does not exist"

# Cannot restart a node that is not shut down.
send "\\demo_node restart 2\n"
eexpect "node 2 is already running"

# Shut down a separate node.
send "\\demo_node shutdown 3\n"
eexpect "node 3 has been shutdown"

# Cannot shut it down again.
send "\\demo_node shutdown 3\n"
eexpect "node 3 is already shut down"

# Expect queries to still work with just one node down.
send "SELECT count(*) FROM movr.rides;\n"
eexpect "500"
eexpect "movr>"

# Now restart the node.
send "\\demo_node restart 3\n"
eexpect "node 3 has been restarted"

interrupt
eexpect eof
end_test
