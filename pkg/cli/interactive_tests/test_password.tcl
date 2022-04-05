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

proc stop_secure_server {argv certs_dir} {
    report "BEGIN STOP SECURE SERVER"
    system "$argv quit --certs-dir=$certs_dir"
    report "END STOP SECURE SERVER"
}

start_secure_server $argv $certs_dir ""

spawn /bin/bash
send "PS1=':''/# '\r"
set prompt ":/# "
eexpect $prompt

send "$argv sql --certs-dir=$certs_dir\r"
eexpect root@

start_test "Test setting password"
send "drop user if exists myuser;\r"
eexpect "DROP ROLE"
eexpect root@
eexpect "/defaultdb>"
# NB: the user cannot change their own password unless
# they have the createlogin and createrole options.
send "create user myuser with createrole createlogin;\r"
eexpect "CREATE ROLE"
eexpect root@
eexpect "/defaultdb>"
send "\\password myuser\r"
eexpect "Enter password: "
send "123\r"
eexpect "Enter it again: "
send "123\r"
eexpect "ALTER ROLE"
eexpect root@

# check SQL injection
send "\\password a;b\r"
eexpect "Enter password: "
send "123\r"
eexpect "Enter it again: "
send "123\r"
eexpect "ERROR: role/user a;b does not exist"
eexpect root@

send "\\password myuser\r"
eexpect "Enter password: "
send "123\r"
eexpect "Enter it again: "
send "124\r"
eexpect "passwords didn't match"
eexpect root@
eexpect "/defaultdb>"

send "\\q\r"
eexpect $prompt
end_test

start_test "Test connect to crdb with password"

send "$argv sql --certs-dir=$certs_dir --user=myuser\r"
eexpect "Enter password:"
send "123\r"
eexpect myuser@
end_test

start_test "Test change own password"
send "\\password\r"
eexpect "Enter password: "
send "124\r"
eexpect "Enter it again: "
send "124\r"
eexpect "ALTER ROLE"
eexpect myuser@
end_test

send "\\q\r"
eexpect $prompt

start_test "Test connect to crdb with new own password"
send "$argv sql --certs-dir=$certs_dir --user=myuser\r"
eexpect "Enter password:"
send "124\r"
eexpect myuser@
end_test

send "\\q\r"
eexpect $prompt

start_test "Log in with wrong password"
send "$argv sql --certs-dir=$certs_dir --user=myuser\r"
eexpect "Enter password:"
send "125\r"
eexpect "password authentication failed"
eexpect $prompt
end_test

send "exit 0\r"
eexpect eof


stop_secure_server $argv $certs_dir
