Please follow the steps below to help us help you.

1. Please supply the header (i.e. the first few lines) of your most recent
   log file **for each node in your cluster**. On most unix-based systems
   running with defaults, this boils down to the output of

     grep -F '[config]' cockroach-data/logs/cockroach.INFO

   When log files are not available, supply the output of `cockroach version`
   and all flags/environment variables passed to `cockroach start` instead.

2. Please describe the issue you observed:

- What did you do?

- What did you expect to see?

- What did you see instead?
