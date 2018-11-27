#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_test "Check that demo says hello properly"
spawn $argv demo
# Be polite.
eexpect "Welcome"
# Warn the user that they won't get persistence.
eexpect "Your changes will not be saved!"
# Inform the necessary URL.
eexpect "Web UI: http:"
# Ensure same messages as cockroach sql.
eexpect "Server version"
eexpect "Cluster ID"
eexpect "brief introduction"
# Ensure user is root.
eexpect root@
# Ensure db is defaultdb.
eexpect "defaultdb>"
end_test
