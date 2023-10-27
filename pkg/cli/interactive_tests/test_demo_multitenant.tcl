#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

spawn $argv demo --no-line-editor --empty --nodes 3 --multitenant --log-dir=logs

start_test "Check that the demo cluster starts properly"
eexpect "Welcome"
eexpect "You are connected to tenant \"demoapp\""
eexpect "(sql)"
eexpect "127.0.0.1:26257/defaultdb"
eexpect "defaultdb>"
end_test

start_test "Check that the demo ls command lists the tenants"
send "\\demo ls\r"
eexpect "Application tenant"
eexpect "127.0.0.1:26257/defaultdb"
eexpect "rpc"
eexpect "127.0.0.1:26357"

eexpect "System tenant"
eexpect "127.0.0.1:26257/defaultdb"
eexpect "rpc"
eexpect "127.0.0.1:26360"

eexpect "defaultdb>"
end_test

# Ideally, we'd also check that you can connect to each of the tenants
# with \connect. But, it's a little annoying to try to parse the randomly
# generated URL strings, and we also have unit tests exercising that
# functionality, so for now this is just a smoke test that ensures that
# the mt demo can at least start up.


start_test "Check the default gateway region"
send "SELECT gateway_region();\n"
eexpect "us-east1"
eexpect "defaultdb>"
end_test

start_test "Check that the demo add command adds a node with no error in ls"
send "\\demo add region=us-east-1\r"
eexpect "node 4 has been added"
eexpect "defaultdb>"
send "\\demo ls\r"
eexpect "node 1:"
eexpect "node 2:"
eexpect "node 3:"
eexpect "node 4:"
eexpect "Application tenant:"
eexpect "System tenant:"
eexpect "defaultdb>"
end_test

start_test "Check that a node can be restarted"
send "\\demo shutdown 4\r"
eexpect "node 4 has been shutdown"
eexpect "defaultdb>"
send "\\demo restart 4\r"
eexpect "node 4 has been restarted"
eexpect "defaultdb>"
send "\\demo ls\r"
eexpect "node 1:"
eexpect "node 2:"
eexpect "node 3:"
eexpect "node 4:"
eexpect "Application tenant:"
eexpect "System tenant:"
eexpect "defaultdb>"
end_test

send_eof
eexpect eof
end_test
