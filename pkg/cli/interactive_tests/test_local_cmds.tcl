#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_server $argv

spawn $argv sql
eexpect root@

# Check that \? prints the help text.
send "\\?\r"
eexpect "You are using"
eexpect "More documentation"
eexpect root@

# Check that \! invokes external commands.
send "\\! echo -n he; echo llo\r"
eexpect "hello"
eexpect "root@"

# Check that \q terminates the client.
send "\\q\r"
eexpect eof
spawn $argv sql
eexpect root@

# Check that \| reads statements.
send "\\| echo 'select '; echo '38 + 4;'\r"
eexpect 42
eexpect "1 row"
eexpect root@

# Check that \| does not execute upon encountering an error.
send "\\| echo 'create database dontcreate;'; exit 1\r"
eexpect "error in external command"
eexpect root@
send "drop database dontcreate;\r"
eexpect "database * does not exist"
eexpect root@

# Check that a buit-in command in between tokens of a statement is
# passed-through.
send "select\r"
eexpect " ->"

send "\\h\r"
eexpect " ->"

send "1;\r"
eexpect "syntax error*\\h"
eexpect root@

# Check that a built-in command in the middle of a token (eg a string)
# is passed-through.
send "select 'hello\r"
eexpect " ->"
send "\\h\r"
eexpect " ->"
send "world';\r"
eexpect "hello"
eexpect "\\h"
eexpect "world"
eexpect "1 row"
eexpect root@

# Finally terminate with Ctrl+C.
send "\003"
eexpect eof

stop_server $argv
