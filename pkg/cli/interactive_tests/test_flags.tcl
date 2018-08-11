#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

spawn /bin/bash
send "PS1=':''/# '\r"
eexpect ":/# "

start_test "Check that --max-disk-temp-storage works."
send "$argv start-single-node --insecure --store=path=mystore --max-disk-temp-storage=10GiB\r"
eexpect "node starting"
interrupt
eexpect ":/# "
end_test

start_test "Check that --max-disk-temp-storage can be expressed as a percentage."
send "$argv start-single-node --insecure --store=path=mystore --max-disk-temp-storage=10%\r"
eexpect "node starting"
interrupt
eexpect ":/# "
end_test

start_test "Check that --max-disk-temp-storage percentage works when the store is in-memory."
send "$argv start-single-node --insecure --store=type=mem,size=1GB --max-disk-temp-storage=10%\r"
eexpect "node starting"
interrupt
eexpect ":/# "
end_test

start_test "Check that memory max flags do not exceed available RAM."
send "$argv start-single-node --insecure --cache=.40 --max-sql-memory=.40\r"
eexpect "WARNING: the sum of --max-sql-memory"
eexpect "is larger than"
eexpect "of total RAM"
eexpect "increased risk"
eexpect "node starting"
interrupt
eexpect ":/# "
end_test

start_test "Check that --host causes a deprecation warning."
send "$argv start-single-node --insecure --host=localhost\r"
eexpect "host has been deprecated, use --listen-addr/--advertise-addr instead."
eexpect "node starting"
interrupt
eexpect ":/# "
end_test

start_test "Check that server --port causes a deprecation warning."
send "$argv start-single-node --insecure --port=26257\r"
eexpect "port has been deprecated, use --listen-addr instead."
eexpect "node starting"
interrupt
eexpect ":/# "
end_test

start_test "Check that server --advertise-port causes a deprecation warning."
send "$argv start-single-node --insecure --advertise-port=12345\r"
eexpect "advertise-port has been deprecated, use --advertise-addr"
eexpect "node starting"
interrupt
eexpect ":/# "
end_test

start_test "Check that not using --host nor --advertise causes a user warning."
send "$argv start-single-node --insecure\r"
eexpect "WARNING: neither --listen-addr nor --advertise-addr was specified"
eexpect "node starting"
interrupt
eexpect ":/# "
end_test

start_test {Check that the "failed running SUBCOMMAND" message does not consider a flag the subcommand}
send "$argv --verbosity 2 start --garbage\r"
eexpect {Failed running "start"}
eexpect ":/# "
end_test

start_test {Check that the "failed running SUBCOMMAND" message handles nested subcommands}
send "$argv --verbosity 2 debug zip --garbage\r"
eexpect {Failed running "debug zip"}
eexpect ":/# "
end_test

start_test {Check that the "failed running SUBCOMMAND" message handles missing subcommands}
send "$argv --verbosity 2 --garbage\r"
eexpect {Failed running "cockroach"}
eexpect ":/# "
end_test

start_test "Check that start without --join reports a deprecation warning"
send "$argv start --insecure\r"
eexpect "running 'cockroach start' without --join is deprecated."
eexpect "node starting"
interrupt
eexpect ":/# "
end_test

start_test "Check that start-single-node disable replication properly"
start_server $argv
send "$argv zone get .default\r"
eexpect "num_replicas: 1"
eexpect ":/# "
stop_server $argv
end_test

send "exit 0\r"
eexpect eof
