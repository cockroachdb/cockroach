#! /usr/bin/env expect -f
#
source [file join [file dirname $argv0] common.tcl]

start_server $argv

spawn /bin/bash
send "PS1=':''/# '\r"
eexpect ":/# "

start_test "Test that quit with a very short timeout still proceeds with hard shutdown"

send "$argv quit --insecure --drain-wait=1ns\r"
eexpect "the server.shutdown.{drain,query,connection,lease_transfer}_wait cluster settings require a value of at least"
eexpect "ok"
eexpect ":/# "

end_test

stop_server $argv
