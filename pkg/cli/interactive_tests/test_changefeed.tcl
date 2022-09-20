#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_server $argv

spawn /bin/bash
send "PS1=':''/# '\r"
eexpect ":/# "

start_test "Check that changefeed flushes readable output to the terminal."
send "$argv sql --no-line-editor\r"
eexpect root@
send "create table target(i int primary key);insert into target values (0);\r"
eexpect "CREATE"
eexpect "INSERT"
eexpect root@
send "create changefeed for target with diff;\r"
eexpect "ERROR: rangefeeds require the kv.rangefeed.enabled setting"
eexpect root@
send "SET CLUSTER SETTING kv.rangefeed.enabled=true;\r"
eexpect "SET"
eexpect root@
send "create changefeed for target with diff;\r"
eexpect "target"
eexpect "before"
interrupt
eexpect root@
end_test

start_test "Check that display changes for the changefeed are reset afterwards."
send "select 'A'::bytea;\r"
eexpect "\\x41"
eexpect root@
end_test

start_test "Check that non-default formats are honored."
send "\\set display_format=csv;\r"
eexpect root@
send "create changefeed for target;\r"
eexpect "table,key,value"
eexpect "target,"
interrupt
end_test

stop_server $argv
