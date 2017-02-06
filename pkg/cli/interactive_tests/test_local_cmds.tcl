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
spawn $argv sql --format=tsv
eexpect root@

# Check that \| reads statements.
send "\\| echo 'select '; echo '38 + 4;'\r"
eexpect "1 row"
eexpect 42
eexpect root@

# Check that \| does not execute upon encountering an error.
send "\\| echo 'create database dontcreate;'; exit 1\r"
eexpect "error in external command"
eexpect root@
send "drop database dontcreate;\r"
eexpect "database * does not exist"
eexpect root@

# Check that a buit-in command in between tokens of a statement is
# processed locally.
send "select\r"
eexpect " ->"

send "\\h\r"
eexpect " ->"

send "1;\r"
eexpect "1 row"
eexpect root@

# Check that \set without argument prints the current options
send "\\set\r"
eexpect "4 rows"
eexpect "display_format\ttsv"
eexpect root@

# Check that \set display_format properly errors out
send "\\set display_format blabla\r"
eexpect "invalid table display format"
# check we don't see a stray "cannot change option during multi-line editing" tacked at the end
eexpect "html)\r\n"
eexpect root@

# Check that \set can change the display format
send "\\set display_format csv\r\\set\r"
eexpect "4 rows"
eexpect "display_format,csv"
eexpect root@

# Check that a built-in command in the middle of a token (eg a string)
# is processed locally.
send "select 'hello\r"
eexpect " ->"
send "\\h\r"
eexpect " ->"
send "world';\r"
eexpect "1 row"
eexpect "hello\\\\nworld"
eexpect root@

# Finally terminate with Ctrl+C.
interrupt
eexpect eof

spawn /bin/bash
send "PS1=':''/# '\r"
eexpect ":/# "

# Now check that non-interactive built-in commands are only accepted
# at the start of a statement.
send "(echo '\\set check_syntax'; echo 'select '; echo '\\help'; echo '1;') | $argv sql\r"
eexpect "statement ignored"
eexpect ":/# "

send "(echo '\\unset check_syntax'; echo 'select '; echo '\\help'; echo '1;') | $argv sql\r"
eexpect "pq: syntax error"
eexpect ":/# "

send "exit 0\r"
eexpect eof

stop_server $argv
