#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_test "Demo core changefeed using format=csv"
spawn $argv demo --no-line-editor --format=csv --log-dir=logs

# We should start in a populated database.
eexpect "movr>"

# initial_scan=only prevents the changefeed from hanging waiting for more changes. 
send "CREATE CHANGEFEED FOR users WITH initial_scan='only';\r"

# header for the results of a successful changefeed
eexpect "table,key,value"

# Statement execution time after the initial scan completes
eexpect "Time:"

eexpect "movr>"
send_eof
eexpect eof

end_test

start_test "Demo with rangefeeds disabled as they are in real life"
spawn $argv demo --no-line-editor --format=csv --auto-enable-rangefeeds=false --log-dir=logs

# We should start in a populated database.
eexpect "movr>"

# changefeed should exist since rangefeed not enabled.
send "CREATE CHANGEFEED FOR users;\r"

# changefeed should fail fast with an informative error.
eexpect "ERROR: rangefeeds require the kv.rangefeed.enabled setting."

eexpect "movr>"
send_eof
eexpect eof

end_test
