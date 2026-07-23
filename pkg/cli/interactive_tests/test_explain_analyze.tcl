#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_server $argv

start_test "Ensure that EXPLAIN ANALYZE works as expected in the sql shell"

# Spawn a sql shell.
spawn $argv sql --no-line-editor
set client_spawn_id $spawn_id
eexpect root@

# Check for a regression where the CLI would get confused when the statement
# had a different number of result columns.
send "EXPLAIN ANALYZE SELECT 1,2;\r"
eexpect "info"
eexpect "planning time"
eexpect "actual row count"

send_eof
eexpect eof

end_test

stop_server $argv
