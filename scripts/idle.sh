#!/bin/bash

# Crontab:
# * * * * * /root/idle.sh 2>&1 >> /root/idle.log

who=tscho

# bail if I'm logged in or I've signaled I want to stay running.
if w | grep -q $who || [[ -f /.active ]]; then exit; fi

echo "shutdown in 10 min" | wall
sleep 600

if w | grep -q $who || [[ -f /.active ]]; then exit; fi

/sbin/shutdown -h now
