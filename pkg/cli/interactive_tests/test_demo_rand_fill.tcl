#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_test "Test that \\rand_fill_table works as expected"

spawn $argv demo

eexpect "movr>"

# Wrong number of args.
send "\\rand_fill_table t\n"
eexpect "Usage:"

# Wrong argument types.
send "\\rand_fill_table t a\n"
eexpect "cannot convert a to string"

# Test on a table that doesn't exist.
send "\\rand_fill_table t 10\n"
eexpect "pq: relation \"t\" does not exist"

eexpect "movr>"

# Create a table.
send "CREATE TABLE t (x INT, y TIMESTAMPTZ, z STRING, w UUID);\n"
eexpect "movr>"

# Test that the command doesn't work within a transaction.
send "BEGIN;\n"
eexpect ">"

send "\\rand_fill_table t 100\n"
eexpect "\\rand_fill_table cannot be run within a transaction"

send "\n"
eexpect "OPEN>"

send "\\rand_fill_table t 100\n"
eexpect "\\rand_fill_table cannot be run within a transaction"

send "SELECT * FROM t2;\n"
eexpect "ERROR>"

send "\\rand_fill_table t 100\n"
eexpect "\\rand_fill_table cannot be run within a transaction"

send "ABORT;\n"
eexpect "movr>"

# Run the command.
send "\\rand_fill_table t 100\n"
eexpect "Successfully loaded 100 random rows into t"

eexpect "movr>"

send "SELECT count(*) FROM t;\n"
eexpect "count"
eexpect "100"
eexpect "(1 row)"

eexpect "movr>"

interrupt
eexpect eof

end_test
