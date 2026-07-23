#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_test "Check that demo insecure says hello properly, multitenant"
spawn $argv demo --no-line-editor --insecure=true --multitenant=true --log-dir=logs
# Be polite.
eexpect "Welcome"
# Warn the user that they won't get persistence.
eexpect "your changes to data stored in the demo session will not be saved!"
# Check that the connection URL is printed.
eexpect "(webui)"
eexpect "http://127.0.0.1:8080"
eexpect "(sql)"
eexpect "root"
eexpect ":26257/movr"

# Ensure same messages as cockroach sql.
eexpect "Server version"
eexpect "Cluster ID"
eexpect "brief introduction"
# Ensure user is root.
eexpect root@
# Ensure db is movr.
eexpect "movr>"
send_eof
eexpect eof
end_test

start_test "Check that demo insecure says hello properly, singletenant"
spawn $argv demo --no-line-editor --insecure=true --multitenant=false --log-dir=logs
# Check that the connection URL is printed; and that we're still
# using default ports.
eexpect "(webui)"
eexpect "http://127.0.0.1:8080"
eexpect "(sql)"
eexpect "root"
eexpect ":26257/movr"
# Check that the system tenant's name is not printed in the prompt since we
# don't have any application tenants.
eexpect ":26257/movr>"
send_eof
eexpect eof
end_test

start_test "Check that demo insecure, env var, says hello properly"
# With env var.
set ::env(COCKROACH_INSECURE) "true"
spawn $argv demo --no-line-editor --no-example-database --log-dir=logs --multitenant=true
eexpect "Welcome"
eexpect "defaultdb>"
end_test

start_test "Check the detailed URLs, insecure mode"
# Show the URLs.
# Also check that the default port is used.
send "\\demo ls\r"
eexpect "(webui)"
eexpect "http://"
eexpect ":8080"
eexpect "Application tenant"
eexpect "(sql)"
eexpect "root@"
eexpect ":26257"
eexpect "sslmode=disable"

eexpect "System tenant"
eexpect "(sql)"
eexpect "root@"
eexpect ":26257"
eexpect "sslmode=disable"
eexpect "(sql/unix)"
eexpect "root:unused@"
eexpect "=26257"

eexpect "defaultdb>"
end_test

send_eof
eexpect eof

# With command-line override.
set ::env(COCKROACH_INSECURE) "false"
spawn $argv demo --no-line-editor --insecure=true --no-example-database --log-dir=logs
eexpect "Welcome"
eexpect "defaultdb>"

# Show the URLs.
send "\\demo ls\r"
eexpect "(webui)"
eexpect "http://"
eexpect "(sql)"
eexpect "root@"
eexpect "sslmode=disable"
eexpect "(sql/unix)"
eexpect "root:unused@"
eexpect "defaultdb>"

send_eof
eexpect eof

end_test

start_test "Check that demo secure says hello properly"

# With env var.
set ::env(COCKROACH_INSECURE) "false"
spawn $argv demo --no-line-editor --no-example-database --log-dir=logs --multitenant=true
eexpect "Welcome"

eexpect "(webui)"
eexpect "http://127.0.0.1:8080/demologin"
eexpect "(sql)"
eexpect "postgresql://demo:"
eexpect "sslmode=require"
eexpect "Username: \"demo\", password"
eexpect "Directory with certificate files"

eexpect "defaultdb>"

# Show the URLs.
# Also check that the default port is used.
send "\\demo ls\r"
eexpect "(webui)"
eexpect "http://127.0.0.1:8080/demologin"
eexpect "Application tenant"
eexpect "(sql)"
eexpect "postgresql://demo:"
eexpect ":26257"
eexpect "sslmode=require"
eexpect "sslrootcert="

eexpect "System tenant"
eexpect "(sql)"
eexpect "postgresql://demo:"
eexpect ":26257"
eexpect "sslmode=require"
eexpect "sslrootcert="
eexpect "(sql/unix)"
eexpect "demo:"
eexpect "=26257"
eexpect "defaultdb>"

send_eof
eexpect eof

# With command-line override.
set ::env(COCKROACH_INSECURE) "true"
spawn $argv demo --no-line-editor --insecure=false --no-example-database --log-dir=logs
eexpect "Welcome"
eexpect "Username: \"demo\", password"
eexpect "defaultdb>"

# Show the URLs.
send "\\demo ls\r"
eexpect "(webui)"
eexpect "http://"
eexpect "(sql)"
eexpect "demo:"
eexpect "sslmode=require"
eexpect "sslrootcert="
eexpect "(sql/unix)"
eexpect "demo:"
eexpect "defaultdb>"

send_eof
eexpect eof

end_test

start_test "Check that the user can override the password."
set ::env(COCKROACH_DEMO_PASSWORD) "hunter2"
spawn $argv demo --no-line-editor --insecure=false --no-example-database --log-dir=logs
eexpect "Connection parameters"
eexpect "(sql)"
eexpect "postgresql://demo:hunter2@"
eexpect "defaultdb>"
send_eof
eexpect eof
end_test

# Test that demo displays connection URLs for nodes in the cluster.
start_test "Check that node URLs are displayed"
spawn $argv demo --no-line-editor --insecure --no-example-database --log-dir=logs
# Check that we see our message.
eexpect "Connection parameters"
eexpect "(webui)"
eexpect "(sql)"
expect "root@"
send_eof
eexpect eof

# Start the test again with a multi node cluster.
spawn $argv demo --no-line-editor --insecure --nodes 3 --no-example-database --log-dir=logs

# Check that we get a message for each node.
eexpect "Connection parameters"
eexpect "(webui)"
eexpect "(sql)"
eexpect "defaultdb>"

send "\\demo ls\r"
eexpect "node 1"
eexpect "(webui)"
eexpect "(sql)"
eexpect "(sql/unix)"
eexpect "node 2"
eexpect "(webui)"
eexpect "(sql)"
eexpect "(sql/unix)"
eexpect "node 3"
eexpect "(webui)"
eexpect "(sql)"
eexpect "(sql/unix)"
eexpect "defaultdb>"

send_eof
eexpect eof

spawn $argv demo --no-line-editor --insecure=false --no-example-database --log-dir=logs
# Expect that security related tags are part of the connection URL.
eexpect "(sql)"
eexpect "sslmode=require"
eexpect "sslrootcert="
eexpect "defaultdb>"

end_test

start_test "Check that the info output includes demo node details"
send "\\info\r"
eexpect "Server version:"
eexpect "Cluster ID:"
eexpect "You are connected to database \"defaultdb\""
eexpect "You are connected to a demo cluster"
eexpect "Connection parameters"
eexpect "(webui)"
eexpect "(sql)"
eexpect "defaultdb>"
end_test

start_test "Check that invalid URL is rejected"
# Regression test for 83598
send "\\connect postgresql://foo:123/\r"
eexpect "using new connection URL"
eexpect "failed to connect"
eexpect " ?>"
send_eof
eexpect eof

end_test

start_test "Check that the port numbers can be overridden from the command line."

spawn $argv demo --no-line-editor --no-example-database --nodes 3 --http-port 8000 --log-dir=logs
eexpect "Welcome"
eexpect "defaultdb>"

# Show the URLs.
send "\\demo ls\r"
eexpect "http://"
eexpect ":8000"
eexpect "http://"
eexpect ":8001"
eexpect "http://"
eexpect ":8002"
eexpect "defaultdb>"

send_eof
eexpect eof

spawn $argv demo --no-line-editor --no-example-database --nodes 3 --sql-port 23000 --log-dir=logs --multitenant=true
eexpect "Welcome"
eexpect "defaultdb>"

# Show the URLs.
send "\\demo ls\r"
eexpect "node 1"

eexpect "Application tenant"
eexpect "(sql)"
eexpect ":23000"
eexpect "System tenant"
eexpect "(sql)"
eexpect ":23000"
eexpect "(sql/unix)"
eexpect "=23000"

eexpect "node 2"
eexpect "Application tenant"
eexpect "(sql)"
eexpect ":23001"
eexpect "System tenant"
eexpect "(sql)"
eexpect ":23001"
eexpect "(sql/unix)"
eexpect "=23001"


eexpect "node 3"
eexpect "Application tenant"
eexpect "(sql)"
eexpect ":23002"
eexpect "System tenant"
eexpect "(sql)"
eexpect ":23002"
eexpect "(sql/unix)"
eexpect "=23002"

eexpect "defaultdb>"

send_eof
eexpect eof


end_test

start_test "Check that demo populates the connection URL in a configured file"

spawn $argv demo --no-line-editor --no-example-database --listening-url-file=test.url --log-dir=logs
eexpect "Welcome"
eexpect "defaultdb>"

# Check the URL is valid. If the connection fails, the system command will fail too.
system "$argv sql --url `cat test.url` -e 'select 1'"

send_eof
eexpect eof

# Ditto, insecure
spawn $argv demo --no-line-editor --no-example-database --listening-url-file=test.url --insecure --log-dir=logs
eexpect "Welcome"
eexpect "defaultdb>"

# Check the URL is valid. If the connection fails, the system command will fail too.
system "$argv sql --url `cat test.url` -e 'select 1'"

send_eof
eexpect eof


end_test
