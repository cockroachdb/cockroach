#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_test "Check cockroach demo telemetry can be disabled"

# set the proper environment variable
set env(COCKROACH_SKIP_ENABLING_DIAGNOSTIC_REPORTING) "true"
spawn $argv demo --no-line-editor

# Expect an informational message.
eexpect "Telemetry disabled by configuration."

# wait for the CLI to start up
eexpect "movr>"
send_eof
eexpect eof

end_test
