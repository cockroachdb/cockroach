#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_test "Check demo alerts when there is a memory warning."
spawn $argv demo --no-line-editor --no-example-database --max-sql-memory=10% --nodes=8

eexpect "WARNING: HIGH MEMORY USAGE"

eexpect "defaultdb>"
interrupt

end_test
