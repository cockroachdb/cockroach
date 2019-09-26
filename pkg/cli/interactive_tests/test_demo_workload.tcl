#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_test "Check cockroach demo --with-load runs the movr workload"

# Disable trying to acquire the demo license. This test does not
# need enterprise features, and sometimes if the licensing server
# is unavailable, the error message from failing to receive the
# testing license pollutes the test output.
set env(COCKROACH_SKIP_ENABLING_DIAGNOSTIC_REPORTING) "true"

# Start demo with movr and the movr workload.
spawn $argv demo movr --with-load

eexpect "movr>"

# Try a few times, but expect that we eventually see the workload
# queries show up as the highest count queries in the system.

set workloadRunning 0

for {set i 0} {$i < 10} {incr i} {
  set timeout 1
  send "select key from crdb_internal.node_statement_statistics order by count desc limit 1;\r"
  expect {
    "SELECT city, id FROM vehicles WHERE city = \$1" {
      set workloadRunning 1
      break
    }
    timeout {}
  }
}

if {!$workloadRunning} {
  report "Workload is not running"
  exit 1
}

interrupt
eexpect eof
end_test
