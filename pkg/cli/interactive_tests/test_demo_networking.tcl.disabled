#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

set ::env(COCKROACH_INSECURE) "false"

start_test "Check that default demo has fixed ports, no multitenancy"
spawn $argv demo --empty --no-line-editor --multitenant=false --nodes 2
eexpect "Welcome"
eexpect "defaultdb>"
send "\\demo ls\r"

eexpect "node 1"
eexpect "(webui)"
eexpect ":8080/demologin"
eexpect "(sql)"
eexpect ":26257"
eexpect "(rpc)"
eexpect ":26357"

eexpect "node 2"
eexpect "(webui)"
eexpect ":8081/demologin"
eexpect "(sql)"
eexpect ":26258"
eexpect "(rpc)"
eexpect ":26358"

eexpect "defaultdb>"
send "\\q\r"
eexpect eof
end_test

start_test "Check that http port can be randomized, no multitenancy"
spawn $argv demo --empty --no-line-editor --multitenant=false --nodes 2 --http-port 0
eexpect "Welcome"
eexpect "defaultdb>"
send "\\demo ls\r"

eexpect "node 1"
eexpect "(webui)"
eexpect "(sql)"
eexpect ":26257"
eexpect "(rpc)"
eexpect ":26357"

eexpect "node 2"
eexpect "(webui)"
eexpect "(sql)"
eexpect ":26258"
eexpect "(rpc)"
eexpect ":26358"

eexpect "defaultdb>"
send "\\q\r"
eexpect eof
end_test

start_test "Check that sql port can be randomized, no multitenancy"
spawn $argv demo --empty --no-line-editor --multitenant=false --nodes 2 --sql-port 0
eexpect "Welcome"
eexpect "defaultdb>"
send "\\demo ls\r"

eexpect "node 1"
eexpect "(webui)"
eexpect ":8080/demologin"
eexpect "(sql)"
eexpect "(rpc)"

eexpect "node 2"
eexpect "(webui)"
eexpect ":8081/demologin"
eexpect "(sql)"
eexpect "(rpc)"

eexpect "defaultdb>"
send "\\q\r"
eexpect eof
end_test

start_test "Check that sql and http port can be randomized, no multitenancy"
spawn $argv demo --empty --no-line-editor --multitenant=false --nodes 2 --sql-port 0 --http-port 0
eexpect "Welcome"
eexpect "defaultdb>"
send "\\demo ls\r"

eexpect "node 1"
eexpect "(webui)"
eexpect "(sql)"
eexpect "(rpc)"

eexpect "node 2"
eexpect "(webui)"
eexpect "(sql)"
eexpect "(rpc)"

eexpect "defaultdb>"
send "\\q\r"
eexpect eof
end_test

start_test "Check that default demo has fixed ports, w/ multitenancy"
spawn $argv demo --empty --no-line-editor --multitenant=true --nodes 2
eexpect "Welcome"
eexpect "defaultdb>"
send "\\demo ls\r"

eexpect "node 1"
eexpect "(webui)"
eexpect ":8080/demologin"
# app tenant
eexpect "(sql)"
eexpect ":26257"
eexpect "(rpc)"
eexpect ":26357"
# system tenant
eexpect "(sql)"
eexpect ":26257"
eexpect "(rpc)"
eexpect ":26359"

eexpect "node 2"
eexpect "(webui)"
eexpect ":8081/demologin"
# app tenant
eexpect "(sql)"
eexpect ":26258"
eexpect "(rpc)"
eexpect ":26358"
# system tenant
eexpect "(sql)"
eexpect ":26258"
eexpect "(rpc)"
eexpect ":26360"

eexpect "defaultdb>"
send "\\q\r"
eexpect eof
end_test

start_test "Check that http port can be randomized, w/ multitenancy"
spawn $argv demo --empty --no-line-editor --multitenant=true --nodes 2 --http-port 0
eexpect "Welcome"
eexpect "defaultdb>"
send "\\demo ls\r"

eexpect "node 1"
eexpect "(webui)"
# app tenant
eexpect "(sql)"
eexpect ":26257"
eexpect "(rpc)"
eexpect ":26357"
# system tenant
eexpect "(sql)"
eexpect ":26257"
eexpect "(rpc)"
eexpect ":26359"

eexpect "node 2"
eexpect "(webui)"
# app tenant
eexpect "(sql)"
eexpect ":26258"
eexpect "(rpc)"
eexpect ":26358"
# system tenant
eexpect "(sql)"
eexpect ":26258"
eexpect "(rpc)"
eexpect ":26360"

eexpect "defaultdb>"
send "\\q\r"
eexpect eof
end_test

start_test "Check that sql port can be randomized, w/ multitenancy"
spawn $argv demo --empty --no-line-editor --multitenant=true --nodes 2 --sql-port 0
eexpect "Welcome"
eexpect "defaultdb>"
send "\\demo ls\r"

eexpect "node 1"
eexpect "(webui)"
eexpect ":8080/demologin"
# app tenant
eexpect "(sql)"
eexpect "(rpc)"
# system tenant
eexpect "(sql)"
eexpect "(rpc)"

eexpect "node 2"
eexpect "(webui)"
eexpect ":8081/demologin"
# app tenant
eexpect "(sql)"
eexpect "(rpc)"
# system tenant
eexpect "(sql)"
eexpect "(rpc)"

eexpect "defaultdb>"
send "\\q\r"
eexpect eof
end_test

start_test "Check that sql and http port can be randomized, w/ multitenancy"
spawn $argv demo --empty --no-line-editor --multitenant=true --nodes 2 --sql-port 0 --http-port 0
eexpect "Welcome"
eexpect "defaultdb>"
send "\\demo ls\r"

eexpect "node 1"
eexpect "(webui)"
# app tenant
eexpect "(sql)"
eexpect "(rpc)"
# system tenant
eexpect "(sql)"
eexpect "(rpc)"

eexpect "node 2"
eexpect "(webui)"
# app tenant
eexpect "(sql)"
eexpect "(rpc)"
# system tenant
eexpect "(sql)"
eexpect "(rpc)"

eexpect "defaultdb>"
send "\\q\r"
eexpect eof
end_test
