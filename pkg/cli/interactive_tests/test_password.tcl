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

spawn $argv sql --certs-dir=$certs_dir
eexpect root@

start_test "Test setting password"
send "drop user if exists myuser;\r"
eexpect "DROP ROLE"
eexpect root@
eexpect "/defaultdb>"
send "create user myuser;\r"
eexpect "CREATE ROLE"
eexpect root@
eexpect "/defaultdb>"
send "\\password myuser\r"
eexpect "Enter password: "
send "123\r"
eexpect "Enter it again: Enter password: "
send "123\r"
eexpect "ALTER ROLE"

send "\\password myuser\r"
eexpect "Enter password: "
send "123\r"
eexpect "Enter it again: Enter password: "
send "124\r"
eexpect "passwords didn't match"
eexpect root@
eexpect "/defaultdb>"

send "\\q\r"
eexpect eof
end_test

start_test "Test connect to crdb with password"
spawn $argv sql --certs-dir=certs --user=myuser
eexpect Enter password:
send "123"
eexpect myuser@
send "\\q\r"
eexpect eof

spawn $argv sql --certs-dir=certs --user=myuser
eexpect Enter password:
send "124"
eexpect "password authentication failed"
send "\\q\r"
eexpect eof
end_test

stop_secure_server $argv $certs_dir
