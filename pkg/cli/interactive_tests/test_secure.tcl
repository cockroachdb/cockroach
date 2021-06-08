#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

set python "python2.7"
set certs_dir "/certs"
set ::env(COCKROACH_INSECURE) "false"
set ::env(COCKROACH_HOST) "localhost"

spawn /bin/bash
send "PS1=':''/# '\r"

set prompt ":/# "
eexpect $prompt

start_test "Check that --insecure reports that the server is really insecure"
send "$argv start-single-node --host=localhost --insecure\r"
eexpect "WARNING: ALL SECURITY CONTROLS HAVE BEEN DISABLED"
eexpect "node starting"
interrupt
eexpect $prompt
end_test


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

start_test "Check that CREATE USER WITH PASSWORD can be used from transactions."
# Create a user from a transaction.
send "$argv sql --certs-dir=$certs_dir\r"
eexpect "root@"
send "BEGIN TRANSACTION;\r"
eexpect "root@"
send "CREATE USER eisen WITH PASSWORD 'hunter2';\r"
eexpect "root@"
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

# Terminate the shell with Ctrl+C.
interrupt
eexpect $prompt

start_test "Check that an auth cookie cannot be created for a user that does not exist."
send "$argv auth-session login nonexistent --certs-dir=$certs_dir\r"
eexpect "user \"nonexistent\" does not exist"
eexpect $prompt
end_test

set mywd [pwd]

start_test "Check that socket-based login works."

send "$argv sql --url 'postgres://eisen@?host=$mywd&port=26257'\r"
eexpect "Enter password:"
send "hunter2\r"
eexpect "eisen@"
interrupt
eexpect $prompt

send "$argv sql --url 'postgres://eisen:hunter2@?host=$mywd&port=26257'\r"
eexpect "eisen@"
interrupt
eexpect $prompt

end_test

start_test "Check that the auth cookie creation works and reports useful output."
send "$argv auth-session login eisen --certs-dir=$certs_dir\r"
eexpect "authentication cookie"
eexpect "session="
eexpect "HttpOnly"
eexpect "Example uses:"
eexpect "curl"
eexpect "wget"
eexpect $prompt
end_test

start_test "Check that the auth cookie can be emitted standalone."
send "$argv auth-session login eisen --certs-dir=$certs_dir --only-cookie >cookie.txt\r"
eexpect $prompt
# we'll also need a root cookie for another test below.
send "$argv auth-session login root --certs-dir=$certs_dir --only-cookie >cookie_root.txt\r"
eexpect $prompt
system "grep HttpOnly cookie.txt"
system "grep HttpOnly cookie_root.txt"
end_test

start_test "Check that the session is visible in the output of list."
send "$argv auth-session list --certs-dir=$certs_dir\r"
eexpect username
eexpect eisen
eexpect eisen
eexpect root
eexpect "3 rows"
eexpect $prompt
end_test

set pyfile [file join [file dirname $argv0] test_auth_cookie.py]

start_test "Check that the auth cookie works."
send "$python $pyfile cookie.txt 'https://localhost:8080/_admin/v1/settings'\r"
eexpect "cluster.organization"
eexpect $prompt
end_test


start_test "Check that the cookie can be revoked."
send "$argv auth-session logout eisen --certs-dir=$certs_dir\r"
eexpect username
eexpect eisen
eexpect eisen
eexpect "2 rows"
eexpect $prompt

send "$python $pyfile cookie.txt 'https://localhost:8080/_admin/v1/settings'\r"
eexpect "HTTP Error 401"
eexpect $prompt
end_test

start_test "Check that a root cookie works."
send "$python $pyfile cookie_root.txt 'https://localhost:8080/_admin/v1/settings'\r"
eexpect "cluster.organization"
eexpect $prompt
end_test

# Now test the cookies with non-TLS http.
stop_secure_server $argv $certs_dir

start_secure_server $argv $certs_dir --unencrypted-localhost-http

start_test "Check that a root cookie works with non-TLS."
send "$python $pyfile cookie_root.txt 'http://localhost:8080/_admin/v1/settings'\r"
eexpect "cluster.organization"
eexpect $prompt
end_test

send "exit 0\r"
eexpect eof

stop_secure_server $argv $certs_dir
