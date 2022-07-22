#! /usr/bin/env expect -f
#
source [file join [file dirname $argv0] common.tcl]

set longname "this-is-a-very-long-directory-name-the-point-is-to-be-more-than-one-hundred-and-twenty-three-characters/and-we-also-need-to-break-it-into-two-parts"

spawn /bin/bash
set shell1_spawn_id $spawn_id
send "PS1=':''/# '\r"
eexpect ":/# "

send "mkdir -p $longname\r"
eexpect ":/# "

start_test "Check derived Unix socket name is printed in server output"
send "$argv start-single-node --insecure --listen-addr=`cat /etc/hostname`:0 --pid-file=logs/server_pid --socket-dir=.\r"
eexpect "CockroachDB node starting"
expect {
    -re "sql:.*@\[^:\]*:(\[^/\]*)/" { set sql_port $expect_out(1,string) }
    timeout { handle_timeout "sql port number" }
}
expect {
    -re "socket: *\\.s\\.PGSQL\\.$sql_port" { }
    timeout { handle_timeout "socket name" }
}
system "test -S .s.PGSQL.$sql_port"
system "test -r .s.PGSQL.$sql_port.lock"
end_test

spawn /bin/bash
set shell2_spawn_id $spawn_id
send "PS1=':''/# '\r"
eexpect ":/# "

start_test "Check that the socket is locked from reuse while the server is running."
# We use 127.0.0.1 so as to not overlap with the default address 0.0.0.0 selected above.
# We also need a different data directory to avoid a conflict there.
send "$argv start-single-node --insecure --listen-addr=127.0.0.1:$sql_port --http-addr=:0 --socket-dir=. -s=path=logs/other\r"
eexpect ERROR
eexpect "Socket appears locked by process"
eexpect ":/# "
end_test

start_test "Check that stopping the first process abruptly enables the 2nd process to start"
system "kill -9 `cat logs/server_pid`"
send "$argv start-single-node --insecure --listen-addr=127.0.0.1:$sql_port --http-addr=:0 --socket-dir=. -s=path=logs/other\r"
eexpect "CockroachDB node starting"
system "test -S .s.PGSQL.$sql_port"
system "test -r .s.PGSQL.$sql_port.lock"
end_test

# Stop the server that was started above.
interrupt
eexpect ":/# "
send "exit\r"
eexpect eof

set spawn_id $shell1_spawn_id
interrupt
eexpect ":/# "

start_test "Check that the socket-dir flag checks the length of the directory."
send "$argv start-single-node --insecure --socket-dir=$longname\r"
eexpect "value of --socket-dir is too long"
eexpect "socket directory name must be shorter"
eexpect ":/# "
end_test

set crdb [file normalize $argv]
send "export BASEDIR=\$PWD\r"
eexpect ":/# "
send "export PREVTMP=\$TMPDIR\r"
eexpect ":/# "

start_test "Check that --background complains about the directory name if there is no default."
send "cd $longname\r"
eexpect ":/# "
send "export TMPDIR=\$PWD\r"
eexpect ":/# "
send "$crdb start-single-node --insecure --background\r"
eexpect "no suitable directory found for the --background notify socket"
eexpect "use a shorter directory name"
eexpect ":/# "
end_test

start_test "Check that --background can use --socket-name if specified and set to sane default."
send "$crdb start-single-node --insecure --background --socket-dir=\$BASEDIR --pid-file=\$BASEDIR/server_pid\r"
eexpect ":/# "
# check the server is running.
system "$crdb sql --insecure -e 'select 1'"
stop_server $crdb
end_test

start_test "Check that --background can use TMPDIR if specified and set to sane default."
# NB: we use a single-command override of TMPDIR (as opposed to using 'export') so that
# the following test below can reuse the value set above.
send "TMPDIR=\$PREVTMP $crdb start-single-node --insecure --background --pid-file=\$BASEDIR/server_pid\r"
eexpect ":/# "
# check the server is running.
system "$crdb sql --insecure -e 'select 1'"
stop_server $crdb
end_test

start_test "Check that --background can use cwd if TMPDIR is invalid."
# NB: at this point TMPDIR is still long, as per previous test.
send "cd \$BASEDIR\r"
eexpect ":/# "
send "$crdb start-single-node --insecure --background --pid-file=server_pid\r"
eexpect ":/# "
# check the server is running.
system "$crdb sql --insecure -e 'select 1'"
stop_server $crdb
end_test
