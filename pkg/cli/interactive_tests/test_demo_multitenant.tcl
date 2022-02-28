#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]
start_test "Check --multitenant flag runs as expected"

# Start a demo with --multitenant set
spawn $argv demo --empty --nodes 3 --multitenant

eexpect "system tenant"
eexpect "tenant 1"
eexpect "tenant 2"
eexpect "tenant 3"

# Ideally, we'd also check that you can connect to each of the tenants
# with \connect. But, it's a little annoying to try to parse the randomly
# generated URL strings, and we also have unit tests exercising that
# functionality, so for now this is just a smoke test that ensures that
# the mt demo can at least start up.

# Ensure db is defaultdb.
eexpect "defaultdb>"

# Ensure the gateway_region is set.
send "SELECT gateway_region();\n"
eexpect "us-east1"
eexpect "defaultdb>"

send_eof
eexpect eof
end_test
