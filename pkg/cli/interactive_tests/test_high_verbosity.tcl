#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

system "mkfifo pid_fifo || true; $argv start --insecure --verbosity 3 --pid-file=pid_fifo & cat pid_fifo > server_pid"

spawn /bin/bash
send "PS1=':''/# '\r"
eexpect ":/# "

# Check that queries using tables can complete without error.
send "echo 'create database d; create table d.t(x int); insert into d.t values(1); select x from d.t;' | $argv sql\r"
eexpect "1 row"
eexpect ":/# "
send "echo 'select x\[1\] from (select array\[1,2,3\]) as t(x);' | $argv sql\r"
eexpect "1 row"
eexpect ":/# "

# Check that the node is alive.
send "$argv node status\r"
eexpect ":/# "
send "echo \$?\r"
eexpect "\r\n0\r\n"
eexpect ":/# "

stop_server $argv
