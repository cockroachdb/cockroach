# We are sending vt100 terminal sequences below, so inform readline
# accordingly.
set env(TERM) vt100

system "mkdir -p logs"

# Keep the history in a test location, so as to not override the
# developer's own history file when running out of Docker.
set histfile "cockroach_sql_history"

set ::env(COCKROACH_SKIP_ENABLING_DIAGNOSTIC_REPORTING) "true"
set ::env(COCKROACH_CONNECT_TIMEOUT) 15
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
    system "echo; echo \$(date '+.%y%m%d %H:%M:%S.%N') EXPECT TEST: '$text' | tee -a logs/expect-cmd.log"
    # We really want to have all files erasable outside of the container
    # even though the commands here run with uid 0.
    # Docker is obnoxious in that it doesn't support setting `umask`.
    # Also CockroachDB doesn't honor umask anyway.
    # So we simply come after the fact and adjust the permissions.
    #
    # The find may race with a cockroach process shutting down in the
    # background; cockroach might be deleting files as they are being
    # found, causing chmod to not find its target file. We ignore
    # these errors.
    system "find logs -exec chmod a+rw '{}' \\; || true"
}

# Catch signals
proc mysig {} {
    report "EXPECT KILLED BY SIGNAL"
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

# Convenience function that sends Ctrl+D to the monitored process.
# Leaves some upfront delay to let the readline process the time
# to initialize the key binding.
proc send_eof {} {
    report "EOF TO FOREGROUND PROCESS"
    sleep 0.4
    send "\004"
}

# Convenience functions to start/shutdown the server.
# Preserves the invariant that the server's PID is saved
# in `server_pid`.
proc start_server {argv} {
    report "BEGIN START SERVER"
    # Note: when changing this command line, update the telemetry tests
    # in test_flags.tcl.
    system "$argv start-single-node --insecure --max-sql-memory=128MB --pid-file=server_pid --listening-url-file=server_url --background -s=path=logs/db >>logs/expect-cmd.log 2>&1;
            $argv sql --insecure -e 'select 1'"
    report "START SERVER DONE"
}
proc stop_server {argv} {
    report "BEGIN STOP SERVER"
    # Trigger a normal shutdown.
    # If after 30 seconds the server hasn't shut down, kill the process and trigger an error.
    # Note: kill -CONT tests whether the PID exists (SIGCONT is a no-op for the process).
    system "kill -TERM `cat server_pid` 2>/dev/null;
            for i in `seq 1 30`; do
              kill -CONT `cat server_pid` 2>/dev/null || exit 0
              echo still waiting
              sleep 1
            done
            echo 'server still running?'
            # Send an unclean shutdown signal to trigger a stack trace dump.
            kill -ABRT `cat server_pid` 2>/dev/null
            # Sleep to increase the probability that the stack trace actually
            # makes it to disk before we force-kill the process.
            sleep 1
            kill -KILL `cat server_pid` 2>/dev/null
            exit 1"

    report "END STOP SERVER"
}

proc flush_server_logs {} {
    report "BEGIN FLUSH LOGS"
    system "kill -HUP `cat server_pid` 2>/dev/null"
    # Wait for flush to occur.
    system "for i in `seq 1 3`; do
              grep 'hangup received, flushing logs' logs/db/logs/cockroach.log && exit 0;
              echo still waiting
              sleep 1
            done
            echo 'server failed to flush logs?'
            exit 1"
    report "END FLUSH LOGS"
}

proc force_stop_server {argv} {
    report "BEGIN FORCE STOP SERVER"
    system "kill -KILL `cat server_pid`"
    report "END FORCE STOP SERVER"
}
