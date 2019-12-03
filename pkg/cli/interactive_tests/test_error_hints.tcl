#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

set fakeserv [file join [file dirname $argv0] netcat.py]

# We'll override security defaults below.
set certs_dir "/certs"
set ::env(COCKROACH_INSECURE) "false"

spawn /bin/bash
set shell_spawn_id $spawn_id

send "PS1=':''/# '\r"
eexpect ":/# "

# Check what happens when attempting to connect and the server does not exist.

start_test "Connecting a RPC client to a non-started server"
send "$argv quit\r"
eexpect "ERROR: cannot load certificates.\r\nCheck your certificate settings"
eexpect "or use --insecure"
eexpect ":/# "

send "$argv quit --certs-dir=$certs_dir\r"
eexpect "ERROR: cannot dial server.\r\nIs the server running?"
eexpect "connection refused"
eexpect ":/# "
end_test

start_test "Connecting a SQL client to a non-started server"
send "$argv sql -e 'select 1'\r"
eexpect "ERROR: cannot load certificates.\r\nCheck your certificate settings"
eexpect "or use --insecure"
eexpect ":/# "

send "$argv sql -e 'select 1' --certs-dir=$certs_dir\r"
eexpect "ERROR: cannot dial server.\r\nIs the server running?"
eexpect "connection refused"
eexpect ":/# "
end_test

# Check what happens when attempting to connect securely to an
# insecure server.

send "$argv start-single-node --insecure\r"
eexpect "initialized new cluster"

spawn /bin/bash
set client_spawn_id $spawn_id

send "PS1=':''/# '\r"
eexpect ":/# "

start_test "Connecting a secure RPC client to an insecure server"
send "$argv quit --certs-dir=$certs_dir\r"
eexpect "ERROR: cannot establish secure connection to insecure server."
eexpect "Maybe use --insecure?"
eexpect ":/# "
end_test

start_test "Connecting a secure SQL client to an insecure server"
send "$argv sql -e 'select 1' --certs-dir=$certs_dir\r"
eexpect "ERROR: cannot establish secure connection to insecure server."
eexpect ":/# "
end_test

# Check what happens when attempting to connect insecurely to a secure
# server.

set spawn_id $shell_spawn_id
interrupt
interrupt
eexpect ":/# "

send "$argv start-single-node --listen-addr=localhost --certs-dir=$certs_dir\r"
eexpect "restarted pre-existing node"

set spawn_id $client_spawn_id

start_test "Connecting an insecure RPC client to a secure server"
send "$argv quit --insecure\r"
eexpect "ERROR: server closed the connection."
eexpect "remove --insecure"
eexpect ":/# "
end_test

start_test "Connecting an insecure SQL client to a secure server"
send "$argv sql -e 'select 1' --insecure\r"
eexpect "ERROR: SSL authentication error while connecting."
eexpect "remove --insecure"
eexpect ":/# "
end_test

# Check what happens when attempting to connect to something
# that is not a CockroachDB server.
set spawn_id $shell_spawn_id
interrupt
interrupt
eexpect ":/# "

start_test "Connecting an insecure RPC client to a non-CockroachDB server"
# In one shell, start a bogus server
send "python2 $fakeserv\r"
eexpect "ready"
# In the other shell, try to run cockroach quit
set spawn_id $client_spawn_id
send "$argv quit --insecure\r"
eexpect "insecure\r\n"
# Wait to see an HTTP/2.0 header on the fake server, then stop the server.
set spawn_id $shell_spawn_id
eexpect "connected"
eexpect "PRI * HTTP/2.0"
interrupt
eexpect ":/# "
# Check that cockroach quit becomes suitably confused.
set spawn_id $client_spawn_id
eexpect "ERROR: server closed the connection."
eexpect "Is this a CockroachDB node?"
eexpect ":/# "
set spawn_id $shell_spawn_id
end_test

start_test "Connecting an insecure SQL client to a non-CockroachDB server"
# In one shell, start a bogus server
send "python2 $fakeserv\r"
eexpect "ready"
# In the other shell, try to run cockroach sql
set spawn_id $client_spawn_id
send "$argv sql -e 'select 1' --insecure\r"
eexpect "insecure\r\n"
# In the first shell, wait for netcat to receive garbage,
# then kill it
set spawn_id $shell_spawn_id
eexpect "connected"
eexpect "cockroach sql"
interrupt
eexpect ":/# "
# Check that cockroach sql becomes suitably confused.
set spawn_id $client_spawn_id
eexpect "ERROR: server closed the connection."
eexpect "Is this a CockroachDB node?"
eexpect "EOF"
eexpect ":/# "
set spawn_id $shell_spawn_id
end_test


# clean up the background processes
set spawn_id $shell_spawn_id
send "exit\r"
eexpect eof

set spawn_id $client_spawn_id
send "exit\r"
eexpect eof
