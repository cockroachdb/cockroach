#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

set certs_dir "./certs"
set ::env(COCKROACH_INSECURE) "false"
set ::env(COCKROACH_HOST) "localhost"

proc start_secure_server {argv certs_dir extra} {
    report "BEGIN START SECURE SERVER"
    system "$argv start-single-node --host=localhost --certs-dir=$certs_dir --pid-file=server_pid -s=path=logs/db --background $extra >>expect-cmd.log 2>&1;
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

start_test "Test initialization"
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
eexpect "Passwords didn't match"
eexpect root@
eexpect "/defaultdb>"

send "drop user if exists userhpw;\r"
eexpect "DROP ROLE"
eexpect root@
eexpect "/defaultdb>"
send "create user userhpw;\r"
eexpect "CREATE ROLE"
eexpect root@
eexpect "/defaultdb>"
send "\\password userhpw\r"
eexpect "Enter password: "
send "CRDB-BCRYPT\$2a\$10\$vcmoIBvgeHjgScVHWRMWI.Z3v03WMixAw2bBS6qZihljSUuwi88Yq\r"
eexpect "Enter it again: Enter password: "
send "CRDB-BCRYPT\$2a\$10\$vcmoIBvgeHjgScVHWRMWI.Z3v03WMixAw2bBS6qZihljSUuwi88Yq\r"
eexpect "ALTER ROLE"

send "drop user if exists user1;\r"
eexpect "DROP ROLE"
eexpect root@
eexpect "/defaultdb>"
send "create user user1;\r"
eexpect "CREATE ROLE"
eexpect root@
eexpect "/defaultdb>"
send "\\password user1\r"
eexpect "Enter password: "
send "SCRAM-SHA-256\$119680:jR6OSCOeSgM/KXEJgL+kow==\$bFJu5id4fRjCZtewy84JEPK/99JXzlJ02xb+QNvZseY=:gTTWFm0PlOcbotZcoiW17Asin2G/RFf1UQSCtEBNisc=\r"
eexpect "Enter it again: Enter password: "
send "SCRAM-SHA-256\$119680:jR6OSCOeSgM/KXEJgL+kow==\$bFJu5id4fRjCZtewy84JEPK/99JXzlJ02xb+QNvZseY=:gTTWFm0PlOcbotZcoiW17Asin2G/RFf1UQSCtEBNisc=\r"
eexpect "ALTER ROLE"

send "\\q\r"
eexpect eof

spawn $argv sql --url "postgresql://myuser:123@localhost:26257/defaultdb?sslmode=verify-full&sslrootcert=certs%2Fca.crt"
eexpect myuser@
send "\\q\r"
eexpect eof

spawn $argv sql --url "postgresql://myuser:1233@localhost:26257/defaultdb?sslmode=verify-full&sslrootcert=certs%2Fca.crt"
eexpect "password authentication failed"
send "\\q\r"
eexpect eof

spawn $argv sql --url "postgresql://userhpw:demo37559@localhost:26257/defaultdb?sslmode=verify-full&sslrootcert=certs%2Fca.crt"
eexpect userhpw@
send "\\q\r"
eexpect eof

spawn $argv sql --url "postgresql://userhpw:1233@localhost:26257/defaultdb?sslmode=verify-full&sslrootcert=certs%2Fca.crt"
eexpect "password authentication failed"
send "\\q\r"
eexpect eof

spawn $argv sql --url "postgresql://user1:123@localhost:26257/defaultdb?sslmode=verify-full&sslrootcert=certs%2Fca.crt"
eexpect user1@
send "\\q\r"
eexpect eof

spawn $argv sql --url "postgresql://user1:1234@localhost:26257/defaultdb?sslmode=verify-full&sslrootcert=certs%2Fca.crt"
eexpect "password authentication failed"
send "\\q\r"
eexpect eof

stop_secure_server $argv $certs_dir
