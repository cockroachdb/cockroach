#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

set ::env(COCKROACH_INSECURE) "false"
set python "python2.7"

spawn $argv demo --empty --no-line-editor --multitenant=true --log-dir=logs
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
send "show tenants;\r"
eexpect "ERROR"
eexpect "only the system tenant"
eexpect "defaultdb>"
send "\\q\r"
eexpect eof
end_test

start_test "Check that a SQL shell can connect to the app tenant without special arguments as root"
spawn $argv sql --no-line-editor -u root
eexpect "Welcome"
expect {
    "ATTENTION: YOU ARE CONNECTED TO THE SYSTEM TENANT" { exit 1 }
    root@ {}
}
eexpect "defaultdb>"
send "show tenants;\r"
eexpect "ERROR"
eexpect "only the system tenant"
eexpect "defaultdb>"
send "\\q\r"
eexpect eof

spawn $argv sql --no-line-editor -u root -d cluster:demoapp
eexpect "Welcome"
expect {
    "ATTENTION: YOU ARE CONNECTED TO THE SYSTEM TENANT" { exit 1 }
    root@ {}
}
eexpect "demoapp/defaultdb>"
send "\\q\r"
eexpect eof
end_test

start_test "Check that a SQL shell can connect to the system tenant without special arguments as demo"
spawn $argv sql --no-line-editor -u demo -d cluster:system
eexpect "Welcome"
eexpect "ATTENTION: YOU ARE CONNECTED TO THE SYSTEM TENANT"
eexpect demo@
eexpect "system/defaultdb>"
send "show tenants;\r"
eexpect "2 rows"
eexpect "system/defaultdb>"
send "\\q\r"
eexpect eof
end_test

start_test "Check that a SQL shell can connect to the system tenant without special arguments as root"
spawn $argv sql --no-line-editor -u root -d cluster:system
eexpect "Welcome"
eexpect "ATTENTION: YOU ARE CONNECTED TO THE SYSTEM TENANT"
eexpect root@
eexpect "system/defaultdb>"
send "show tenants;\r"
eexpect "2 rows"
eexpect "system/defaultdb>"
send "\\q\r"
eexpect eof
end_test

# We're going to check the autocerts feature and switching between tenants after the cluster
# option has been set in a URL.
spawn $argv sql --no-line-editor --url "postgresql://root@localhost:26257?options=-ccluster=demoapp"
eexpect "Welcome"
eexpect root@
eexpect "demoapp/defaultdb>"
send "use postgres;\r"
eexpect "demoapp/postgres>"

start_test "Check that the session can be switched to the system tenant with the connect command"

send "\\connect cluster:system - - - autocerts\r"
eexpect root@
eexpect "system/defaultdb>"

send "\\connect cluster:demoapp - - - autocerts\r"
eexpect root@
eexpect "demoapp/defaultdb>"

# Also check the DB name can still be specified.
send "\\connect cluster:system/postgres - - - autocerts\r"
eexpect root@
eexpect "system/postgres>"

end_test

send "\\q\r"
eexpect eof

start_test "Check that clients can use the unix socket to connect to the app tenant."
system "$argv sql -u root -e \"alter user root with password 'abc'\""
set ssldir $env(HOME)/.cockroach-demo
spawn $argv sql --no-line-editor --url "postgresql://root:abc@/defaultdb?host=$ssldir&port=26257"
eexpect "Welcome"
expect {
    "ATTENTION: YOU ARE CONNECTED TO THE SYSTEM TENANT" { exit 1 }
    root@ {}
}
eexpect "defaultdb>"
send "show tenants;\r"
eexpect "ERROR"
eexpect "only the system tenant"
eexpect "defaultdb>"
send "\\q\r"
eexpect eof
end_test

start_test "Check that clients can use the unix socket to connect to the system tenant."
system "$argv sql -d cluster:system/demo -u root -e \"alter user root with password 'abc'\""
set ssldir $env(HOME)/.cockroach-demo
spawn $argv sql --no-line-editor --url "postgresql://root:abc@/defaultdb?host=$ssldir&options=-ccluster%3Dsystem&port=26257"
eexpect "Welcome"
eexpect "ATTENTION: YOU ARE CONNECTED TO THE SYSTEM TENANT"
eexpect root@
eexpect "system/defaultdb>"
send "show tenants;\r"
eexpect "2 rows"
eexpect "system/defaultdb>"
send "\\q\r"
eexpect eof
end_test

spawn /bin/bash
set shell_spawn_id $spawn_id
send "PS1=':''/# '\r"
eexpect ":/# "

start_test "Check that an auth cookie can be extracted for a demo session"
# From the system tenant.
set sqlurl "postgresql://root@127.0.0.1:26257/?options=-ccluster%3Dsystem&sslmode=require&sslrootcert=$ssldir/ca.crt&sslcert=$ssldir/client.root.crt&sslkey=$ssldir/client.root.key"
send "$argv auth-session login root --url \"$sqlurl\" --only-cookie >cookie_system.txt\r"
eexpect ":/# "
# From the app tenant.
send "$argv auth-session login root --certs-dir=\$HOME/.cockroach-demo --only-cookie >cookie_app.txt\r"
eexpect ":/# "

# Check that the cookies work.
set pyfile [file join [file dirname $argv0] test_auth_cookie.py]

send "$python $pyfile cookie_system.txt 'http://localhost:8080/_admin/v1/users?cluster=system'\r"
eexpect "username"
eexpect "demo"
# No tenant name specified -> use default tenant.
send "$python $pyfile cookie_app.txt 'http://localhost:8080/_admin/v1/users'\r"
eexpect "username"
eexpect "demo"
end_test


start_test "Check that login sessions are preserved across demo restarts."

set spawn_id $demo_spawn_id
send "\\q\r"
eexpect eof

spawn $argv demo --empty --no-line-editor --multitenant=true --log-dir=logs
set demo_spawn_id $spawn_id
eexpect "Welcome"
eexpect "defaultdb>"

set spawn_id $shell_spawn_id

send "$python $pyfile cookie_system.txt 'http://localhost:8080/_admin/v1/users?cluster=system'\r"
eexpect "username"
eexpect "demo"
# No tenant name specified -> use default tenant.
send "$python $pyfile cookie_app.txt 'http://localhost:8080/_admin/v1/users'\r"
eexpect "username"
eexpect "demo"
end_test

send "exit\r"
eexpect eof

set spawn_id $demo_spawn_id
send "\\q\r"
eexpect eof


start_test "Check that the warning for the system tenant is not printed when there are no secondary tenants"
spawn $argv demo --empty --no-line-editor --multitenant=false --log-dir=logs
eexpect "Welcome"
expect {
    "ATTENTION: YOU ARE CONNECTED TO THE SYSTEM TENANT" { exit 1 }
    demo@ {}
}
eexpect "defaultdb>"
send "\\q\r"
eexpect eof
end_test

# Regression test for #95135.
start_test "Verify that the demo command did not leave tenant directories around."
system "if ls tenant-* >logs/tenants 2>&1; then echo Strays; cat logs/tenants; exit 1; fi"
end_test
