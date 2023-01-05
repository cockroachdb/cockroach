#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

set certs_dir "/certs"
set home "/home/roach"
set ::env(COCKROACH_INSECURE) "false"
set ::env(COCKROACH_HOST) "localhost"

proc start_secure_server {argv certs_dir extra} {
    report "BEGIN START SECURE SERVER"
    system "$argv start-single-node --host=localhost --socket-dir=. --certs-dir=$certs_dir --pid-file=server_pid -s=path=logs/db --background $extra >>expect-cmd.log 2>&1;
            $argv sql --certs-dir=$certs_dir -e 'select 1'"
    report "END START SECURE SERVER"
}

start_secure_server $argv $certs_dir ""

spawn /bin/bash
send "PS1=':''/# '\r"
set prompt ":/# "
eexpect $prompt

send "$argv sql --no-line-editor --certs-dir=$certs_dir\r"
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
eexpect "ERROR: role/user \"a;b\" does not exist"
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

send "$argv sql --no-line-editor --certs-dir=$certs_dir --user=myuser\r"
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
send "$argv sql --no-line-editor --certs-dir=$certs_dir --user=myuser\r"
eexpect "Enter password:"
send "124\r"
eexpect myuser@
end_test

send "\\q\r"
eexpect $prompt

start_test "Log in with wrong password"
send "$argv sql --no-line-editor --certs-dir=$certs_dir --user=myuser\r"
eexpect "Enter password:"
send "125\r"
eexpect "password authentication failed"
eexpect $prompt
end_test

start_test "Log in using pgpass file"
system "echo 'localhost:*:*:myuser:124' > $home/.pgpass"
send "$argv sql --no-line-editor --certs-dir=$certs_dir --user=myuser\r"
eexpect myuser@
eexpect "defaultdb>"
send "\\q\r"
eexpect $prompt
system "rm $home/.pgpass"
end_test

start_test "Log in using custom pgpass file"
system "echo 'localhost:*:*:myuser:125' > $home/.pgpass"
system "echo 'localhost:*:*:myuser:124' > $home/my_pgpass"
send "export PGPASSFILE=$home/my_pgpass\r"
send "$argv sql --no-line-editor --certs-dir=$certs_dir --user=myuser\r"
eexpect myuser@
eexpect "defaultdb>"
send "\\q\r"
eexpect $prompt
system "rm $home/.pgpass"
system "rm $home/my_pgpass"
send "unset PGPASSFILE\r"
end_test

start_test "Log in using pgservicefile and custom pgpass"
send "export PGDATABASE=postgres\r"
system "echo 'localhost:*:*:myuser:124' > $home/my_pgpass"
system "echo '
# servicefile should override environment variables
\[myservice\]
host=localhost
port=26257
dbname=defaultdb
user=myuser
passfile=$home/my_pgpass
' > $home/.pg_service.conf"
send "$argv sql --no-line-editor --url='postgres://myuser@localhost?service=myservice&sslrootcert=$certs_dir/ca.crt'\r"
eexpect myuser@
eexpect "defaultdb>"
send "\\q\r"
send "unset PGDATABASE\r"
system "rm $home/.pg_service.conf"
system "rm $home/my_pgpass"
eexpect $prompt
end_test

start_test "Log in using custom pgservicefile with default root cert"
system "mkdir $home/.postgresql/ && cp $certs_dir/ca.crt $home/.postgresql/root.crt"
system "echo '
\[myservice\]
host=localhost
port=26257
dbname=postgres
user=myuser
password=124
' > $home/my_pg_service.conf"
send "$argv sql --url='postgres://myuser@localhost?service=myservice&servicefile=$home/my_pg_service.conf'\r"
eexpect myuser@
eexpect "postgres>"
send "\\q\r"
system "rm $home/my_pg_service.conf"
eexpect $prompt
end_test

send "exit 0\r"
eexpect eof


stop_server $argv
