#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_test "Check demo alerts when there is a memory warning."
spawn $argv demo --no-line-editor --no-example-database --max-sql-memory=10% --nodes=8 --log-dir=logs

eexpect "WARNING: HIGH MEMORY USAGE"

# try to exit early via Ctrl+C.
interrupt
# If the demo command started the prompt before we got a chance to send Ctrl+C above,
# it's not going to accept Ctrl+C any more. Instead, send a quit command.
send "\r\\q\r"
eexpect eof

end_test
