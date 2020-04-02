#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_test "Check that demo says hello properly"
spawn $argv demo
# Be polite.
eexpect "Welcome"
# Warn the user that they won't get persistence.
eexpect "your changes to data stored in the demo session will not be saved!"
# Inform the necessary URL.
eexpect "Web UI: http:"
# Ensure same messages as cockroach sql.
eexpect "Server version"
eexpect "Cluster ID"
eexpect "brief introduction"
# Ensure user is root.
eexpect root@
# Ensure db is movr.
eexpect "movr>"
interrupt
eexpect eof
end_test

start_test "Check that demo insecure says hello properly"

# With env var.
set ::env(COCKROACH_INSECURE) "true"
spawn $argv demo
eexpect "Welcome"
eexpect "movr>"
interrupt
eexpect eof

# With command-line override.
set ::env(COCKROACH_INSECURE) "false"
spawn $argv demo --insecure=true
eexpect "Welcome"
eexpect "movr>"
interrupt
eexpect eof

end_test

start_test "Check that demo secure says hello properly"

# With env var.
set ::env(COCKROACH_INSECURE) "false"
spawn $argv demo
eexpect "Welcome"
eexpect "The user \"root\" with password \"admin\" has been created."
eexpect "movr>"
interrupt
eexpect eof

# With command-line override.
set ::env(COCKROACH_INSECURE) "true"
spawn $argv demo --insecure=false
eexpect "Welcome"
eexpect "The user \"root\" with password \"admin\" has been created."
eexpect "movr>"
interrupt
eexpect eof

end_test
