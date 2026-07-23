#! /usr/bin/env expect -f
#
source [file join [file dirname $argv0] common.tcl]

set storedir "logs/mystore"
set externdir "/some/extern"

spawn /bin/bash
send "PS1=':''/# '\r"
eexpect ":/# "

start_test "Check disabling external IO explicitly"

send "$argv start-single-node --insecure --store=$storedir --external-io-dir=''\r"
eexpect "external I/O path:   <disabled>"
interrupt
eexpect "shutdown completed"

end_test

start_test "Check setting external IO explicitly"

send "$argv start-single-node --insecure --store=$storedir --external-io-dir=$externdir\r"
eexpect "external I/O path:   $externdir"
interrupt
eexpect "shutdown completed"

end_test

start_test "Check implicit external I/O dir under store dir"

send "$argv start-single-node --insecure --store=$storedir\r"
eexpect "external I/O path:"
eexpect "$storedir/extern"
interrupt
eexpect "shutdown completed"

end_test
