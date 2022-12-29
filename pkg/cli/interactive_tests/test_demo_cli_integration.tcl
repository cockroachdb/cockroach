#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

set ::env(COCKROACH_INSECURE) "false"

spawn $argv demo --empty --no-line-editor --multitenant=true
eexpect "Welcome"

start_test "Check that the cli connect instructions get printed out."
eexpect "defaultdb>"
send "\\demo ls\r"
eexpect "Application tenant"
eexpect "(cli)"
eexpect "cockroach sql"
eexpect " -u demo"
end_test

eexpect "defaultdb>"

set demo_spawn_id $spawn_id

set ::env(COCKROACH_CERTS_DIR) $env(HOME)/.cockroach-demo

start_test "Check that a SQL shell can connect to the app tenant without special arguments as demo"
spawn $argv sql --no-line-editor -u demo
eexpect "Welcome"
eexpect demo@
eexpect "defaultdb>"
send "table system.tenants;\r"
eexpect "ERROR"
eexpect "does not exist"
eexpect "defaultdb>"
send "\\q\r"
eexpect eof
end_test

start_test "Check that a SQL shell can connect to the app tenant without special arguments as root"
spawn $argv sql --no-line-editor -u root
eexpect "Welcome"
eexpect root@
eexpect "defaultdb>"
send "table system.tenants;\r"
eexpect "ERROR"
eexpect "does not exist"
eexpect "defaultdb>"
send "\\q\r"
eexpect eof
end_test

start_test "Check that a SQL shell can connect to the system tenant without special arguments as demo"
spawn $argv sql --no-line-editor -u demo -p 26258
eexpect "Welcome"
eexpect demo@
eexpect "defaultdb>"
send "table system.tenants;\r"
eexpect "2 rows"
eexpect "defaultdb>"
send "\\q\r"
eexpect eof
end_test

start_test "Check that a SQL shell can connect to the app tenant without special arguments as root"
spawn $argv sql --no-line-editor -u root -p 26258
eexpect "Welcome"
eexpect root@
eexpect "defaultdb>"
send "table system.tenants;\r"
eexpect "2 rows"
eexpect "defaultdb>"
send "\\q\r"
eexpect eof
end_test

set spawn_id $demo_spawn_id
send "\\q\r"
eexpect eof
