#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_server $argv

# Try connect and check the session variables match.

spawn $argv sql --no-line-editor --url "postgresql://root@localhost:26257?options=-cintervalstyle%3Diso_8601"
eexpect root@
send "SHOW intervalstyle;\r"
eexpect "iso_8601"
eexpect root@
send_eof
eexpect eof

spawn $argv sql --no-line-editor --url "postgresql://root@localhost:26257?options=-cdatestyle%3Dymd"
eexpect root@
send "SHOW datestyle;\r"
eexpect "ISO, YMD"
eexpect root@
send_eof
eexpect eof

stop_server $argv
