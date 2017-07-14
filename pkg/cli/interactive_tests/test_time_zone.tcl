#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

# Set up the initial cluster.
start_server $argv

spawn $argv sql
eexpect root@

start_test "Check that the uninitialized default time zone is UTC"
send "SHOW TIME ZONE;\r"
eexpect "UTC"
eexpect root@
send "\\q\r"
eexpect eof
end_test

proc check_alt_tz {argv alttz check checktime} {
    global spawn_id

    # Re-launch a server with a different default time zone
    stop_server $argv
    report "BEGIN START ALT TZ SERVER"
    system "$argv start --insecure --sql-time-zone=$alttz --background -s=path=logs/db --pid-file=pid_fifo >>expect-cmd.log 2>&1 & cat pid_fifo >server_pid"
    report "END START ALT TZ SERVER"

    # Check the alt TZ with a client
    spawn $argv sql
    eexpect root@
    send "SHOW TIME ZONE;\r"
    eexpect $check
    eexpect root@

    send "SELECT '1970-01-01 00:00:00'::TIMESTAMPTZ::STRING AS result;\r"
    eexpect result
    eexpect $checktime
    eexpect root@

    send "\\q\r"
    eexpect eof
}

start_test "Check that the default can be overridden"
check_alt_tz $argv "Europe/Amsterdam" "Europe/Amsterdam" "1969-12-31 23:00:00+00:00"
check_alt_tz $argv "\"'Europe/Amsterdam'\"" "Europe/Amsterdam" "1969-12-31 23:00:00+00:00"
check_alt_tz $argv "gmt" "GMT" "1970-01-01 00:00:00+00:00"
check_alt_tz $argv "-5" " -5" "1970-01-01 05:00:00+00:00"
check_alt_tz $argv "-5.5" " -5.5" "1970-01-01 05:30:00+00:00"
check_alt_tz $argv "\"interval '-5 hour'\"" "'-5h'" "1970-01-01 05:00:00+00:00"
end_test

start_test "Check that invalid time zones are properly rejected"
stop_server $argv

spawn /bin/bash
send "PS1=':''/# '\r"
eexpect ":/# "

send "$argv start --insecure --sql-time-zone=someinvalidstring -spath=logs/db \r"
eexpect "failed to start server"
eexpect "cannot find time zone"
eexpect ":/# "
send "exit\r"
eexpect eof

end_test
