#! /usr/bin/env expect -f
#
source [file join [file dirname $argv0] common.tcl]

spawn /bin/bash
send "PS1=':''/# '\r"
eexpect ":/# "

start_test "Check that on node startup a temporary subdirectory is created under --temp-dir."
send "mkdir -p mystore/temp\r"
send "$argv start --insecure --store=mystore --temp-dir=mystore/temp\r"
eexpect "node starting"
eexpect "temp dir:*mystore/temp/cockroach-temp"
send "ls mystore/temp/cockroach-temp*; echo $?\r"
expect "0"
interrupt
eexpect ":/# "
end_test

start_test "Check that on node startup a temporary subdirectory is created under --temp-dir even if store is in-memory."
send "mkdir -p temp\r"
send "$argv start --insecure --store=type=mem,size=1GB --temp-dir=temp\r"
eexpect "node starting"
eexpect "temp dir:*temp/cockroach-temp"
send "ls temp/cockroach-temp*; echo $?\r"
expect "0"
interrupt
eexpect ":/# "
end_test

start_test "Check that temporary directories in record file are cleaned up during cli startup."
send "mkdir -p mystore/temp mystore/temp1 mystore/temp2\r"
send "touch mystore/temp1/foo.txt\r"
send "$argv start --insecure --temp-dir=mystore/temp\r"
eexpect "node starting"
eexpect "temp dir:*mystore/temp/cockroach-temp"
send "test -d mystore/temp1; echo $?\r"
expect "1"
send "test -d mystore/temp2; echo $?\r"
expect "1"
interrupt
eexpect ":/# "
end_test

start_test "Check that if --temp-dir is unspecified, a temporary directory is created under --store"
send "$argv start --insecure --store=mystore\r"
eexpect "node starting"
eexpect "temp dir:*mystore/cockroach-temp"
send "ls mystore/cockroach-temp*; echo $?\r"
expect "0"
interrupt
eexpect ":/# "
end_test
