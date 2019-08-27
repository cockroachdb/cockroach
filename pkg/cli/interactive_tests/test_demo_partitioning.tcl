#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_test "Expect an error if geo-partitioning is requested and a license cannot be acquired"

# set the proper environment variable
set env(COCKROACH_SKIP_ENABLING_DIAGNOSTIC_REPORTING) "true"
spawn $argv demo --geo-partitioned-replicas
# expect a failure
eexpect "Error: license acquisition was unsuccessful. Enterprise features are needed to partition data"
# clean up after the test
interrupt
eexpect eof
end_test
