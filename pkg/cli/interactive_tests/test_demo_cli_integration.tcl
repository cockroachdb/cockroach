#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

set ::env(COCKROACH_INSECURE) "false"
set python "python2.7"

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

spawn /bin/bash
set shell_spawn_id $spawn_id
send "PS1=':''/# '\r"
eexpect ":/# "

start_test "Check that an auth cookie can be extracted for a demo session"
# From the system tenant.
send "$argv auth-session login root --certs-dir=\$HOME/.cockroach-demo -p 26258 --only-cookie >cookie_system.txt\r"
eexpect ":/# "
# From the app tenant.
send "$argv auth-session login root --certs-dir=\$HOME/.cockroach-demo --only-cookie >cookie_app.txt\r"
eexpect ":/# "

# Check that the cookies work.
set pyfile [file join [file dirname $argv0] test_auth_cookie.py]

send "$python $pyfile cookie_system.txt 'http://localhost:8080/_admin/v1/users?tenant_name=system'\r"
eexpect "username"
eexpect "demo"
send "$python $pyfile cookie_app.txt 'http://localhost:8080/_admin/v1/users?tenant_name=demo-tenant'\r"
eexpect "username"
eexpect "demo"
end_test


start_test "Check that login sessions are preserved across demo restarts."

set spawn_id $demo_spawn_id
send "\\q\r"
eexpect eof

spawn $argv demo --empty --no-line-editor --multitenant=true
set demo_spawn_id $spawn_id
eexpect "Welcome"
eexpect "defaultdb>"

set spawn_id $shell_spawn_id

send "$python $pyfile cookie_system.txt 'http://localhost:8080/_admin/v1/users?tenant_name=system'\r"
eexpect "username"
eexpect "demo"
send "$python $pyfile cookie_app.txt 'http://localhost:8080/_admin/v1/users?tenant_name=demo-tenant'\r"
eexpect "username"
eexpect "demo"
end_test

send "exit\r"
eexpect eof

set spawn_id $demo_spawn_id
send "\\q\r"
eexpect eof
