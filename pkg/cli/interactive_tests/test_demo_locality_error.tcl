#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_test "Expect error with incorrect demo locality settings"

# Test failure with less localities than expected.
spawn $argv demo --nodes 3 --demo-locality=region=us-east1:region=us-east2
# wait for the CLI to start up
eexpect "ERROR: number of localities specified must equal number of nodes"
eexpect eof

# Test failure with more localities than expected.
spawn $argv demo --nodes 3 --demo-locality=region=us-east1:region=us-east2:region=us-east3:region=us-east4
# wait for the CLI to start up
eexpect "ERROR: number of localities specified must equal number of nodes"
eexpect eof

end_test
