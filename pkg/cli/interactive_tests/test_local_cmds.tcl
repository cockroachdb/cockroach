#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_server $argv

spawn $argv sql
eexpect root@

start_test "Check that times are displayed by default on interactive terminals."
send "select 1;\r"
eexpect "(1 row)"
eexpect "Time:"
eexpect root@
end_test

start_test "Check that \\? prints the help text."
send "\\?\r"
eexpect "You are using"
eexpect "More documentation"
eexpect root@
end_test

start_test "Check that \\! invokes external commands."
send "\\! echo -n he; echo llo\r"
eexpect "hello"
eexpect "root@"
end_test

start_test "Check that \\q terminates the client."
send "\\q\r"
eexpect eof
spawn $argv sql --format=tsv
eexpect root@
end_test

start_test "Check that quit terminates the client."
send "quit\r"
eexpect eof
spawn $argv sql --format=tsv
eexpect root@
end_test

start_test "Check that quit does not terminate the client in the middle of a statement."
send "select\rquit\r;\r"
eexpect "column \"quit\" does not exist"
eexpect root@
end_test

start_test "Check that exit terminates the client."
send "exit\r"
eexpect eof
spawn $argv sql --format=tsv
eexpect root@
end_test

start_test "Check that \\| reads statements."
send "\\| echo 'select '; echo '38 + 4;'\r"
eexpect 42
eexpect root@
end_test

start_test "Check that \\| does not execute upon encountering an error."
send "\\| echo 'create database dontcreate;'; exit 1\r"
eexpect "error in external command"
eexpect root@
send "drop database dontcreate;\r"
eexpect "database * does not exist"
eexpect root@
end_test

start_test "Check that a buit-in command in between tokens of a statement is processed locally."
send "select\r"
eexpect " ->"

send "\\?\r"
eexpect " ->"

send "1;\r"
expect "Time"
eexpect root@
end_test

start_test "Check that \\set without argument prints the current options"
send "\\set\r"
eexpect "display_format\ttsv"
eexpect root@
end_test

start_test "Check that \\set display_format properly errors out"
send "\\set display_format blabla\r"
eexpect "invalid table display format"
# check we don't see a stray "cannot change option during multi-line editing" tacked at the end
eexpect "html, raw)\r\n"
eexpect root@
end_test

start_test "Check that \\set can change the display format"
send "\\set display_format csv\r\\set\r"
eexpect "display_format,csv"
eexpect root@

send "\\set display_format=tsv\r\\set\r"
eexpect "display_format\ttsv"
eexpect root@
end_test

start_test "Check that \\x toggles display format"
send "\\x\r\\set\r"
eexpect "Option*|*display_format"
eexpect "Value*|*records"
eexpect root@

send "\\x\r\\set\r"
eexpect "display_format*|*table"
eexpect root@
end_test

start_test "Check that \\x with on or off enables/disables records display format"
send "\\x on\r\\set\r"
eexpect "Option*|*display_format"
eexpect "Value*|*records"
eexpect root@

send "\\x off\r\\set\r"
eexpect "display_format*|*table"
eexpect root@
end_test

start_test "Check various ways to set a boolean flag."
send "\\set display_format=tsv\r"
send "\\set show_times=false\r\\set\r"
eexpect "show_times\tfalse"
eexpect root@
send "\\set show_times=true\r\\set\r"
eexpect "show_times\ttrue"
eexpect root@
send "\\set show_times=0\r\\set\r"
eexpect "show_times\tfalse"
eexpect root@
send "\\set show_times=1\r\\set\r"
eexpect "show_times\ttrue"
eexpect root@
send "\\set show_times=off\r\\set\r"
eexpect "show_times\tfalse"
eexpect root@
send "\\set show_times=on\r\\set\r"
eexpect "show_times\ttrue"
eexpect root@
send "\\set show_times=blah\r"
eexpect "invalid syntax"
eexpect root@
end_test


start_test "Check that a built-in command in the middle of a token (eg a string) is processed locally."
send "select 'hello\r"
eexpect " ->"
send "\\?\r"
eexpect " ->"
send "world' as woo;\r"
eexpect "hello\r\nworld"
eexpect "Time"
eexpect root@
end_test

start_test "Check that \\set can change the display of query times"
# check the override
send "\\unset show_times\r\\set\r"
eexpect "show_times\tfalse"
eexpect root@
send "select 1 as woo;\r"
eexpect "woo\r\n1\r\n"
expect {
    "Time:" {
	report "unexpected Time"
	exit 1
    }
    root@ {}
}
eexpect "/defaultdb> "
# restore
send "\\set show_times\r"
end_test

start_test "Check that \\h with invalid commands print a reminder."
send "\\h invalid\r"
eexpect "no help available"
eexpect "Try"
expect "with no argument"
eexpect root@
end_test

start_test "Check that \\h with no argument prints a full list."
send "\\h\r"
eexpect "TRUNCATE"
eexpect "SHOW"
eexpect "ROLLBACK"
eexpect root@
end_test

start_test "Check that \\h with a known statement prints details."
send "\\h select\r"
eexpect "Command:"
eexpect "SELECT"
eexpect "data manipulation"
eexpect "FROM"
eexpect "ORDER BY"
eexpect "See also"
eexpect root@
end_test

start_test "Check that \\h with a documented clause name prints details."
send "\\h <source>\r"
eexpect "Command:"
eexpect "<SOURCE>"
eexpect "data manipulation"
eexpect "JOIN"
eexpect "SHOW"
eexpect "See also"
eexpect root@
end_test

start_test "Check that \\hf without argument prints a list."
send "\\hf\r"
eexpect "abs"
eexpect "count"
eexpect "round"
eexpect root@
end_test

start_test "Check that \\hf with a valid function name prints that functions help."
send "\\hf version\r"
eexpect "Category:"
eexpect "Signature"
eexpect root@
end_test

start_test "Check that \\hf with an invalid function name prints an error."
send "\\hf invalid\r"
eexpect "no help available"
eexpect "Try"
eexpect "with no argument"
eexpect root@
end_test

start_test "Check that \\echo behaves well."
send "\\echo\r"
eexpect "\r\n"
eexpect "\r\n"
eexpect root@

send "\\echo hello  world\r"
# echo removes double spaces within the line. That's expected.
eexpect "hello world"
eexpect root@
end_test

start_test "Check that commands are also recognized with a final semicolon."
send "\\set;\r"
eexpect "display_format"
eexpect root@
send "\\h select;\r"
eexpect SELECT
eexpect Description
eexpect root@
end_test

# Finally terminate with Ctrl+C.
interrupt
eexpect eof

spawn /bin/bash
send "PS1=':''/# '\r"
eexpect ":/# "

start_test "Check that non-interactive built-in commands are only accepted at the start of a statement."
send "(echo '\\set check_syntax'; echo 'select '; echo '\\help'; echo ';') | $argv sql\r"
eexpect "statement ignored"
eexpect ":/# "

send "(echo '\\unset check_syntax'; echo 'select '; echo '\\help'; echo '1;') | $argv sql\r"
eexpect "ERROR: at or near"
eexpect "syntax error"
eexpect ":/# "
end_test

stop_server $argv

start_test "Check that client-side options can be overridden with set"

# First establish a baseline with all the defaults.
send "$argv demo --no-example-database\r"
eexpect root@
send "\\set display_format csv\r"
send "\\set\r"
eexpect "auto_trace,off"
eexpect "check_syntax,true"
eexpect "echo,false"
eexpect "errexit,false"
eexpect "prompt1,%n@"
eexpect "show_times,true"
eexpect root@
interrupt
eexpect ":/# "

# Then verify that the defaults can be overridden.
send "$argv demo --no-example-database --set=auto_trace=on --set=check_syntax=false --set=echo=true --set=errexit=true --set=prompt1=%n@haa --set=show_times=false\r"
eexpect root@
send "\\set display_format csv\r"
send "\\set\r"
eexpect "auto_trace,\"on"
eexpect "check_syntax,false"
eexpect "echo,true"
eexpect "errexit,true"
eexpect "prompt1,%n@haa"
eexpect "show_times,false"
eexpect root@
interrupt
eexpect ":/# "

end_test


send "exit 0\r"
eexpect eof
