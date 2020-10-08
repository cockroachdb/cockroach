#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_test "Check demo alerts when there is a memory warning."
spawn $argv demo --max-sql-memory=10% --nodes=8

eexpect "WARNING: HIGH MEMORY USAGE"

end_test
