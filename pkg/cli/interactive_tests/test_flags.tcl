#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

spawn /bin/bash
send "PS1=':''/# '\r"
eexpect ":/# "

start_test "Check that --max-disk-temp-storage works."
send "$argv start --insecure --store=path=mystore --max-disk-temp-storage=10GiB\r"
eexpect "node starting"
interrupt
eexpect ":/# "
end_test

start_test "Check that --max-disk-temp-storage can be expressed as a percentage."
send "$argv start --insecure --store=path=mystore --max-disk-temp-storage=10%\r"
eexpect "node starting"
interrupt
eexpect ":/# "
end_test

start_test "Check that --max-disk-temp-storage percentage works when the store is in-memory."
send "$argv start --insecure --store=type=mem,size=1GB --max-disk-temp-storage=10%\r"
eexpect "node starting"
interrupt
eexpect ":/# "
end_test

start_test "Check that memory max flags do not exceed available RAM."
send "$argv start --insecure --cache=.40 --max-sql-memory=.40\r"
eexpect "WARNING: the sum of --max-sql-memory"
eexpect "is larger than"
eexpect "of total RAM"
eexpect "increased risk"
eexpect "node starting"
interrupt
eexpect ":/# "
end_test

start_test "Check that not using --host nor --advertise causes a user warning."
send "$argv start --insecure\r"
eexpect "WARNING: neither --listen-addr nor --advertise-addr was specified"
eexpect "node starting"
interrupt
eexpect ":/# "
end_test

start_test "Check that --listening-url-file gets created with the right data"
send "$argv start --insecure --listening-url-file=foourl\r"
eexpect "node starting"
system "grep -q 'postgresql://.*@.*:\[0-9\]\[0-9\]*' foourl"
interrupt
eexpect ":/# "
end_test

start_test {Check that the "failed running SUBCOMMAND" message does not consider a flag the subcommand}
send "$argv --vmodule=*=2 start --garbage\r"
eexpect {Failed running "start"}
end_test

start_test {Check that the "failed running SUBCOMMAND" message handles nested subcommands}
send "$argv --vmodule=*=2 debug zip --garbage\r"
eexpect {Failed running "debug zip"}
end_test

start_test {Check that the "failed running SUBCOMMAND" message handles missing subcommands}
send "$argv --vmodule=*=2 --garbage\r"
eexpect {Failed running "cockroach"}
end_test

send "exit 0\r"
eexpect eof
