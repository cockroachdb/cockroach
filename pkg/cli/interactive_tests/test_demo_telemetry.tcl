#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_test "Check cockroach demo telemetry and license check can be disabled"

# set the proper environment variable
set env(COCKROACH_SKIP_ENABLING_DIAGNOSTIC_REPORTING) "true"
spawn $argv demo

# Expect an informational message.
eexpect "Telemetry and automatic license acquisition disabled by configuration."

# wait for the CLI to start up
eexpect "movr>"
# send a request for an enterprise feature
send "alter table vehicles partition by list (city) (partition p1 values in ('nyc'));\n"
# expect that it failed, as no license was requested.
eexpect "use of partitions requires an enterprise license"
# clean up after the test
interrupt
eexpect eof

end_test
