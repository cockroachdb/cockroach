#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_test "Check cockroach demo --with-load runs the movr workload"

# Start demo with movr and the movr workload.
spawn $argv demo movr --with-load

eexpect "movr>"

# Try a few times, but expect that we eventually see the workload
# queries show up as the highest count queries in the system.

for {set i 0} {$i < 10} {incr i} {
  send "select key from crdb_internal.node_statement_statistics order by count desc limit 1;\r"
  # Sleep a little bit to let the workload catch up.
  sleep 1
}

eexpect "SELECT city, id FROM vehicles WHERE city = \$1"

interrupt
end_test
