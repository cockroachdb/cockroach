#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

set certs_dir "/certs"
set ::env(COCKROACH_INSECURE) "false"
set ::env(COCKROACH_HOST) "localhost"

spawn /bin/bash
send "PS1=':''/# '\r"

set prompt ":/# "
eexpect $prompt

start_test "Check that --insecure reports that the server is really insecure"
send "$argv start-single-node --insecure\r"
eexpect "WARNING: RUNNING IN INSECURE MODE"
eexpect "node starting"
interrupt
eexpect $prompt
end_test


proc start_secure_server {argv certs_dir} {
    report "BEGIN START SECURE SERVER"
    system "$argv start-single-node --certs-dir=$certs_dir --pid-file=server_pid -s=path=logs/db --background >>expect-cmd.log 2>&1;
            $argv sql --certs-dir=$certs_dir -e 'select 1'"
    report "END START SECURE SERVER"
}

proc stop_secure_server {argv certs_dir} {
    report "BEGIN STOP SECURE SERVER"
    system "$argv quit --certs-dir=$certs_dir"
    report "END STOP SECURE SERVER"
}

start_secure_server $argv $certs_dir

start_test "Check 'node ls' works with certificates."
send "$argv node ls --certs-dir=$certs_dir\r"
eexpect "id"
eexpect "1"
eexpect "1 row"
eexpect $prompt
end_test


start_test "Can create users without passwords."
send "$argv sql -e 'create user testuser' --certs-dir=$certs_dir\r"
eexpect $prompt
end_test

start_test "Passwords are not requested when a certificate for the user exists"
send "$argv sql --user=testuser --certs-dir=$certs_dir\r"
eexpect "testuser@"
send "\\q\r"
eexpect $prompt
end_test

start_test "Check that root cannot use password."
# Run as root but with a non-existent certs directory.
send "$argv sql --url='postgresql://root@localhost:26257?sslmode=verify-full&sslrootcert=$certs_dir/ca.crt'\r"
eexpect "ERROR: connections with user root must use a client certificate"
eexpect "Failed running \"sql\""
end_test

start_test "Check that CREATE USER WITH PASSWORD can be used from transactions."
# Create a user from a transaction.
send "$argv sql --certs-dir=$certs_dir\r"
eexpect "root@"
send "BEGIN TRANSACTION;\r"
eexpect " ->"
send "CREATE USER eisen WITH PASSWORD 'hunter2';\r"
eexpect " ->"
send "COMMIT TRANSACTION;\r"
eexpect "root@"
send "\\q\r"
# Log in with the correct password.
eexpect $prompt
send "$argv sql --certs-dir=$certs_dir --user=eisen\r"
eexpect "Enter password:"
send "hunter2\r"
eexpect "eisen@"
send "\\q\r"
# Try to log in with an incorrect password.
eexpect $prompt
send "$argv sql --certs-dir=$certs_dir --user=eisen\r"
eexpect "Enter password:"
send "*****\r"
eexpect "ERROR: password authentication failed for user eisen"
eexpect "Failed running \"sql\""
# Check that history is scrubbed.
send "$argv sql --certs-dir=$certs_dir\r"
eexpect "root@"
interrupt
end_test

# Terminate with Ctrl+C.
interrupt

eexpect $prompt

send "exit 0\r"
eexpect eof

stop_secure_server $argv $certs_dir
