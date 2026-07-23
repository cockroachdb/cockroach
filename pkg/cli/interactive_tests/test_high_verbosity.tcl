#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

system "mkfifo url_fifo || true; $argv start-single-node --insecure --vmodule=*=3 --pid-file=server_pid --listening-url-file=url_fifo -s=path=logs/db & cat url_fifo > server_url"

spawn /bin/bash
send "PS1=':''/# '\r"
eexpect ":/# "

start_test "Check that queries using tables can complete without error."
send "echo 'create database d; create table d.t(x int); insert into d.t values(1); select x from d.t;' | $argv sql\r"
eexpect "1 row"
eexpect ":/# "
send "echo 'select x\[1\] from (select array\[1,2,3\]) as t(x);' | $argv sql\r"
eexpect "1 row"
eexpect ":/# "
end_test

start_test "Check that the node is alive."
send "$argv node status\r"
eexpect ":/# "
send "echo \$?\r"
eexpect "\r\n0\r\n"
eexpect ":/# "
end_test

stop_server $argv
