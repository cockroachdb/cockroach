#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_server $argv

# Setup.
spawn $argv sql
eexpect root@
send "SET CLUSTER SETTING sql.defaults.intervalstyle.enabled = true;\r"
eexpect "SET CLUSTER SETTING"
eexpect root@
send "SET CLUSTER SETTING sql.defaults.datestyle.enabled = true;\r"
eexpect "SET CLUSTER SETTING"
eexpect root@
send_eof
eexpect eof


# Try connect and check the session variables match.

spawn $argv sql --url "postgresql://root@localhost:26257?options=-cintervalstyle%3Diso_8601"
eexpect root@
send "SHOW intervalstyle;\r"
eexpect "iso_8601"
eexpect root@
send_eof
eexpect eof

# TODO(#72065): uncomment
#spawn $argv sql --url "postgresql://root@localhost:26257?options=-cdatestyle%3Dymd"
#eexpect root@
#send "SHOW datestyle;\r"
#eexpect "ISO, YMD"
#eexpect root@
#interrupt
#eexpect eof

stop_server $argv
