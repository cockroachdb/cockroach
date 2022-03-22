#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_test "Check that demo insecure says hello properly"
spawn $argv demo --insecure=true
# Be polite.
eexpect "Welcome"
# Warn the user that they won't get persistence.
eexpect "your changes to data stored in the demo session will not be saved!"

# Verify the URLs for both shared and tenant server.
eexpect "system tenant"
eexpect "(webui)"
eexpect "http://"
eexpect ":8081"
eexpect "(sql)"
eexpect "root"
eexpect ":26258/defaultdb"
eexpect "sslmode=disable"
eexpect "(sql/unix)"
eexpect "root:unused@/defaultdb"
eexpect "=26258"
eexpect "tenant 1"
eexpect "(sql)"
eexpect "root"
eexpect ":26257/movr"
eexpect "sslmode=disable"

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

start_test "Check that demo insecure says hello properly"

# With env var.
set ::env(COCKROACH_INSECURE) "true"
spawn $argv demo --no-example-database
eexpect "Welcome"
eexpect "defaultdb>"

# Show the URLs.
# Also check that the default port is used.
send "\\demo ls\r"
eexpect "system tenant"
eexpect "(webui)"
eexpect "http://"
eexpect ":8081"
eexpect "(sql)"
eexpect "root@"
eexpect ":26258"
eexpect "sslmode=disable"
eexpect "(sql/unix)"
eexpect "root:unused@"
eexpect "=26258"
eexpect "tenant 1"
eexpect "(sql)"
eexpect "root@"
eexpect ":26257"
eexpect "sslmode=disable"
eexpect "defaultdb>"

send_eof
eexpect eof

# With command-line override.
set ::env(COCKROACH_INSECURE) "false"
spawn $argv demo --insecure=true --no-example-database
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
spawn $argv demo --no-example-database
eexpect "Welcome"
eexpect "Username: \"demo\", password"
eexpect "Directory with certificate files"
eexpect "defaultdb>"

# Show the URLs.
# Also check that the default port is used.
send "\\demo ls\r"
eexpect "system tenant"
eexpect "(webui)"
eexpect "http://"
eexpect ":8081"
eexpect "(sql)"
eexpect "demo:"
eexpect ":26258"
eexpect "sslmode=require"
eexpect "(sql/unix)"
eexpect "demo:"
eexpect "=26258"
eexpect "tenant 1"
eexpect "(sql)"
eexpect "demo:"
eexpect ":26257"
eexpect "sslmode=require"
eexpect "defaultdb>"

send_eof
eexpect eof

# With command-line override.
set ::env(COCKROACH_INSECURE) "true"
spawn $argv demo --insecure=false --no-example-database
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
eexpect "(sql/unix)"
eexpect "demo:"
eexpect "defaultdb>"

send_eof
eexpect eof

end_test

# Test that demo displays connection URLs for nodes in the cluster.
start_test "Check that node URLs are displayed"
spawn $argv demo --insecure --no-example-database
# Check that we see our message.
eexpect "Connection parameters"
eexpect "(sql)"
eexpect "(sql/unix)"
expect root@
send_eof
eexpect eof

# Start the test again with a multi node cluster.
spawn $argv demo --insecure --nodes 3 --no-example-database

# Check that we get a message for each node.
eexpect "Connection parameters"
eexpect "(sql)"
eexpect "(sql/unix)"
eexpect "defaultdb>"

send "\\demo ls\r"
eexpect "node 1"
eexpect "(sql)"
eexpect "(sql/unix)"
eexpect "node 2"
eexpect "(sql)"
eexpect "(sql/unix)"
eexpect "node 3"
eexpect "(sql)"
eexpect "(sql/unix)"
eexpect "defaultdb>"

send_eof
eexpect eof

spawn $argv demo --insecure=false --no-example-database
# Expect that security related tags are part of the connection URL.
eexpect "(sql)"
eexpect "sslmode=require"
eexpect "defaultdb>"

send_eof
eexpect eof

end_test

start_test "Check that the port numbers can be overridden from the command line."

spawn $argv demo --no-example-database --nodes 3 --http-port 8000
eexpect "Welcome"
eexpect "defaultdb>"

# Show the URLs.
send "\\demo ls\r"
eexpect "http://"
eexpect ":8003"
eexpect "http://"
eexpect ":8004"
eexpect "http://"
eexpect ":8005"
eexpect "defaultdb>"

send_eof
eexpect eof

spawn $argv demo --no-example-database --nodes 3 --sql-port 23000
eexpect "Welcome"
eexpect "defaultdb>"

# Show the URLs.
send "\\demo ls\r"
eexpect "system tenant"
eexpect "node 1"
eexpect "(sql)"
eexpect ":23003"
eexpect "(sql/unix)"
eexpect "=23003"
eexpect "node 2"
eexpect "(sql)"
eexpect ":23004"
eexpect "(sql/unix)"
eexpect "=23004"
eexpect "node 3"
eexpect "(sql)"
eexpect ":23005"
eexpect "(sql/unix)"
eexpect "=23005"
eexpect "tenant 1"
eexpect "(sql)"
eexpect ":23000"
eexpect "tenant 2"
eexpect "(sql)"
eexpect ":23001"
eexpect "tenant 3"
eexpect "(sql)"
eexpect ":23002"
eexpect "defaultdb>"

send_eof
eexpect eof


end_test

start_test "Check that demo populates the connection URL in a configured file"

spawn $argv demo --no-example-database --listening-url-file=test.url
eexpect "Welcome"
eexpect "defaultdb>"

# Check the URL is valid. If the connection fails, the system command will fail too.
system "$argv sql --url `cat test.url` -e 'select 1'"

send_eof
eexpect eof

# Ditto, insecure
spawn $argv demo --no-example-database --listening-url-file=test.url --insecure
eexpect "Welcome"
eexpect "defaultdb>"

# Check the URL is valid. If the connection fails, the system command will fail too.
system "$argv sql --url `cat test.url` -e 'select 1'"

send_eof
eexpect eof


end_test
