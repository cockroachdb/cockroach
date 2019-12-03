#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_test "Check \\demo_node commands work as expected"
# Start a demo with 5 nodes.
spawn $argv demo movr --nodes=5

# Ensure db is movr.
eexpect "movr>"

# Wrong number of args
send "\\demo_node\r"
eexpect "Usage:"

# Cannot shutdown node 1
send "\\demo_node shutdown 1\r"
eexpect "cannot shutdown node 1"

# Cannot operate on a node which does not exist.
send "\\demo_node shutdown 8\r"
eexpect "node 8 does not exist"
send "\\demo_node restart 8\r"
eexpect "node 8 does not exist"
send "\\demo_node decommission 8\r"
eexpect "node 8 does not exist"
send "\\demo_node recommission 8\r"
eexpect "node 8 does not exist"

# Cannot restart a node that is not shut down.
send "\\demo_node restart 2\r"
eexpect "node 2 is already running"

# Shut down a separate node.
send "\\demo_node shutdown 3\r"
eexpect "node 3 has been shut down"

# The shutdown request may not be processed immediately
# so we need to wait a bit before we see the expected output.
set cmdDone 0
set timeout 1
for {set i 0} {$i < 10} {incr i} {
  send "select node_id, draining, decommissioning from crdb_internal.gossip_liveness ORDER BY node_id;\r"
  expect {
	  "3 |   true   |      false" {
		  set cmdDone 1
		  break
	  }
	  timeout {}
  }
}
if {!$cmdDone} {
  report "Node did not shut down"
  exit 1
}
set timeout $stdTimeout


# Cannot shut it down again.
send "\\demo_node shutdown 3\r"
eexpect "node 3 is already shut down"

# Expect queries to still work with just one node down.
send "SELECT count(*) FROM movr.rides;\r"
eexpect "500"
eexpect "movr>"

# Now restart the node.
send "\\demo_node restart 3\r"
eexpect "node 3 has been restarted"

set cmdDone 0
set timeout 1
for {set i 0} {$i < 10} {incr i} {
  send "select node_id, draining, decommissioning from crdb_internal.gossip_liveness ORDER BY node_id;\r"
  expect {
	  "3 |  false   |      false" {
		  set cmdDone 1
		  break
	  }
	  timeout {}
  }
}
if {!$cmdDone} {
  report "Node did not restart"
  exit 1
}
set timeout $stdTimeout

# Try commissioning commands
send "\\demo_node decommission 4\r"
eexpect "node 4 has been decommissioned"

set cmdDone 0
set timeout 1
for {set i 0} {$i < 10} {incr i} {
  send "select node_id, draining, decommissioning from crdb_internal.gossip_liveness ORDER BY node_id;\r"
  expect {
	  "4 |  false   |      true" {
		  set cmdDone 1
		  break
	  }
	  timeout {}
  }
}
if {!$cmdDone} {
  report "Node did not restart"
  exit 1
}
set timeout $stdTimeout

send "\\demo_node recommission 4\r"
eexpect "node 4 has been recommissioned"

set cmdDone 0
set timeout 1
for {set i 0} {$i < 10} {incr i} {
  send "select node_id, draining, decommissioning from crdb_internal.gossip_liveness ORDER BY node_id;\r"
  expect {
	  "4 |  false   |      false" {
		  set cmdDone 1
		  break
	  }
	  timeout {}
  }
}
if {!$cmdDone} {
  report "Node did not restart"
  exit 1
}
set timeout $stdTimeout

interrupt
eexpect eof
end_test
