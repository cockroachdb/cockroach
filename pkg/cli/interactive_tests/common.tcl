# We are sending vt100 terminal sequences below, so inform readline
# accordingly.
set env(TERM) vt100

# Keep the history in a test location, so as to not override the
# developer's own history file.
set histfile ".cockroachdb_history_test"
set ::env(COCKROACH_SQL_CLI_HISTORY) $histfile
# Set client commands as insecure. The server uses --insecure.
set ::env(COCKROACH_INSECURE) "true"
system "rm -f $histfile"

# Everything in this test should be fast. Don't be tolerant for long
# waits.
set timeout 30

# When run via Docker the enclosing terminal has 0 columns and 0 rows,
# and this confuses readline. Ensure sane defaults here.
set stty_init "cols 80 rows 25"

# Convenience function to tag what's going on in log files.
proc report {text} {
    system "echo; echo `date '+.%y%m%d %H:%M:%S.%N'` EXPECT TEST: '$text' | tee -a cmd.log"
}

# Upon termination
proc report_log_files {} {
    system "(echo '==COMMAND OUTPUT=='; cat cmd.log; echo '==DB LOG=='; cat cockroach-data/logs/cockroach.log; echo '==END==') || true"
}

# Catch signals
proc mysig {} {
    report "EXPECT KILLED BY SIGNAL"
    report_log_files
    exit 130
}
trap mysig SIGINT
trap mysig SIGTERM

# Convenience functions to tag a test
proc start_test {text} {
    report "START TEST: $text"
}
proc end_test {} {
    report "END TEST"
}

# Convenience wrapper function, which ensures that all expects are
# mandatory (i.e. with a mandatory fail if the expected output doesn't
# show up fast).
proc handle_timeout {text} {
    report "TIMEOUT WAITING FOR \"$text\""
    report_log_files
    exit 1
}
proc eexpect {text} {
    expect {
	$text {}
	timeout { handle_timeout $text }
    }
}

# Convenience function that sends Ctrl+C to the monitored process.
proc interrupt {} {
    report "INTERRUPT TO FOREGROUND PROCESS"
    send "\003"
    sleep 0.4
}

# Convenience functions to start/shutdown the server.
# Preserves the invariant that the server's PID is saved
# in `server_pid`.
proc start_server {argv} {
    report "BEGIN START SERVER"
    system "mkfifo pid_fifo || true; $argv start --insecure --pid-file=pid_fifo --background >>cmd.log 2>&1 & cat pid_fifo > server_pid"
    report "START SERVER DONE"
}
proc stop_server {argv} {
    report "BEGIN STOP SERVER"
    system "set -e; if kill -CONT `cat server_pid`; then $argv quit || true & sleep 1; kill -9 `cat server_pid` || true; else $argv quit || true; fi"
    report "END STOP SERVER"
}
