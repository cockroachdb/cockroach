#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

spawn /bin/bash
send "PS1=':''/# '\r"
eexpect ":/# "

start_test "Check that --max-dist-temp-storage works."
send "$argv start --insecure --store=path=mystore --max-disk-temp-storage=10GiB\r"
eexpect "node starting"
interrupt
eexpect ":/# "

start_test "Check that --max-dist-temp-storage can be expressed as a percentage."
send "$argv start --insecure --store=path=mystore --max-disk-temp-storage=10%\r"
eexpect "node starting"
interrupt
eexpect ":/# "

start_test "Check that --max-dist-temp-storage percentage works when the store is in-memory."
send "$argv start --insecure --store=type=mem,size=1GB --max-disk-temp-storage=10%\r"
eexpect "node starting"
interrupt
eexpect ":/# "

send "exit 0\r"
eexpect eof

