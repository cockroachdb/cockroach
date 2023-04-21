#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

set certs_dir "/certs"
set ::env(COCKROACH_INSECURE) "false"
set ::env(COCKROACH_HOST) "localhost"

proc start_secure_server {argv certs_dir extra} {
    report "BEGIN START SECURE SERVER"
    system "$argv start-single-node --host=localhost --socket-dir=. --certs-dir=$certs_dir --pid-file=server_pid -s=path=logs/db --background $extra >>expect-cmd.log 2>&1;
            $argv sql --certs-dir=$certs_dir -e 'select 1'"
    report "END START SECURE SERVER"
}

start_secure_server $argv $certs_dir ""

spawn $argv sql --certs-dir=$certs_dir --no-line-editor
eexpect root@

start_test "Test initialization"
send "create database t; set database = t;\r"
eexpect root@
eexpect "/t>"
send "create user foo with password 'abc';\r"
eexpect "CREATE ROLE"
eexpect root@
eexpect "/t>"
end_test

start_test "Check that the client-side connect cmd prints the current conn details"
send "\\c\r"
eexpect "Connection string:"
eexpect "You are connected to database \"t\" as user \"root\""
eexpect root@
eexpect "/t>"
end_test

start_test "Check that the client-side connect cmd can change databases"
send "\\c postgres\r"
eexpect "using new connection URL"
eexpect root@
eexpect "/postgres>"
end_test

start_test "Check that the client-side connect cmd can change users using a password"

send "\\c - foo\r"
eexpect "using new connection URL"
eexpect "Connecting to server"
eexpect "as user \"foo\""
eexpect "Enter password:"
send "foo\r"
eexpect "password authentication failed"
eexpect foo@
eexpect "?>"

send "\\c -\r"
eexpect "Enter password:"
send "abc\r"
eexpect foo@
eexpect "/postgres>"
end_test

start_test "Check that the client-side connect cmd can change databases"
send "\\c system\r"
eexpect "using new connection URL"
eexpect "Connecting to server"
eexpect "as user \"foo\""
eexpect "Enter password:"
send "abc\r"
eexpect foo@
eexpect "/system>"
end_test

start_test "Check that the user can recover from an invalid database"
send "\\c invaliddb\r"
eexpect "Enter password:"
send "abc\r"
eexpect "error retrieving the database name"
eexpect foo@
eexpect "?>"

send "\\c system\r"
eexpect "Enter password:"
send "abc\r"
eexpect foo@
eexpect "/system>"
end_test

start_test "Check that the client-side connect cmd can change hosts"
send "\\c - - localhost\r"
eexpect "using new connection URL"
eexpect "Connecting to server"
eexpect "as user \"foo\""
eexpect "Enter password:"
send "abc\r"
eexpect foo@
eexpect "/system>"
end_test

start_test "Check that the client-side connect cmd can change ports"
send "\\c - - - 26257\r"
eexpect "using new connection URL"
eexpect "Connecting to server"
eexpect "as user \"foo\""
eexpect "Enter password:"
send "abc\r"
eexpect foo@
eexpect "/system>"
end_test

start_test "Check that the client-side connect cmd can change users with automatic client cert detection"
send "\\c - root - - autocerts\r"
eexpect "using new connection URL"
eexpect root@
eexpect "/system>"
end_test

start_test "Check that the auto-cert feature properly fails if certs were not found"
send "\\c - unknownuser - - autocerts\r"
eexpect "unable to find TLS client cert and key"
eexpect root@
eexpect "/system>"
end_test

start_test "Check that the client-side connect cmd can detect syntax errors"
send "\\c - - - - abc\r"
eexpect "unknown syntax"
eexpect root@
eexpect "/system>"
end_test

start_test "Check that the client-side connect cmd recognizes invalid URLs"
send "\\c postgres://root@localhost:26257/defaultdb?sslmode=invalid&sslcert=$certs_dir%2Fclient.root.crt&sslkey=$certs_dir%2Fclient.root.key&sslrootcert=$certs_dir%2Fca.crt\r"
eexpect "unrecognized sslmode parameter"
eexpect root@
eexpect "/system>"
end_test

start_test "Check that the client-side connect cmd can change users with certs using a URL"
# first test that it can recover from an invalid database
send "\\c postgres://root@localhost:26257/invaliddb?sslmode=require&sslcert=$certs_dir%2Fclient.root.crt&sslkey=$certs_dir%2Fclient.root.key&sslrootcert=$certs_dir%2Fca.crt\r"
eexpect "using new connection URL"
eexpect "error retrieving the database name: ERROR: database \"invaliddb\" does not exist"
eexpect root@
eexpect "?>"

send "\\c postgres://root@localhost:26257/defaultdb?sslmode=require&sslcert=$certs_dir%2Fclient.root.crt&sslkey=$certs_dir%2Fclient.root.key&sslrootcert=$certs_dir%2Fca.crt\r"
eexpect "using new connection URL"
eexpect root@
eexpect "/defaultdb>"
end_test

send "\\q\r"
eexpect eof

start_test "Check that default certs dir is respected"

set ::env(HOME) "."
system "mkdir -p ./.cockroach-certs"
system "cp $certs_dir/* ./.cockroach-certs/"

spawn $argv sql --no-line-editor
eexpect root@
eexpect "/defaultdb>"
send "\\c\r"
eexpect "Connection string:"
eexpect "sslrootcert=.cockroach-certs"
eexpect "You are connected to database \"defaultdb\" as user \"root\""
eexpect root@

end_test

send "\\q\r"
eexpect eof

start_test "Check that extra URL params are preserved when changing database"

spawn $argv sql --no-line-editor --certs-dir=$certs_dir --url=postgres://root@localhost:26257/defaultdb?options=--search_path%3Dcustom_path&statement_timeout=1234
eexpect root@
eexpect "/defaultdb>"
send "SHOW search_path;\r"
eexpect "custom_path"
send "SHOW statement_timeout;\r"
eexpect "1234"
eexpect root@
eexpect "/defaultdb>"
send "\\c postgres\r"
eexpect "using new connection URL"
eexpect root@
eexpect "/postgres>"
send "SHOW search_path;\r"
eexpect "custom_path"
send "SHOW statement_timeout;\r"
eexpect "1234"

end_test

send "\\q\r"
eexpect eof

start_test "Check that the client-side connect cmd prints the current conn details with password redacted"

spawn $argv sql --no-line-editor --certs-dir=$certs_dir --url=postgres://foo:abc@localhost:26257/defaultdb
eexpect foo@
send "\\c\r"
eexpect "Connection string: postgresql://foo:~~~~~~@"
eexpect "You are connected to database \"defaultdb\" as user \"foo\""
eexpect foo@
eexpect "/defaultdb>"
end_test

send "\\q\r"
eexpect eof

stop_server $argv

# Some more tests with the insecure mode.
set ::env(COCKROACH_INSECURE) "true"
start_server $argv

spawn $argv sql --no-line-editor
eexpect root@
eexpect "defaultdb>"

start_test "Check that the connect cmd can switch dbs in insecure mode"
send "\\c system\r"
eexpect root@
eexpect "system>"
end_test

start_test "Check that the connect cmd can switch users in insecure mode"
send "\\c - foo\r"
eexpect foo@
eexpect "system>"
end_test

stop_server $argv

system "rm -rf logs/db"
start_server $argv

start_test "Check that the connect cmd does not generate an error when the cluster ID changes"
send "\\c - root\r"
eexpect "warning: the cluster ID has changed"
eexpect "Previous ID"
eexpect "New Cluster ID"
eexpect "system>"

send "\\q\r"
expect {
    "ERROR" {
	report "reconnect generated an unexpected error"
	exit 1
    }
    eof { }
}

end_test


stop_server $argv
