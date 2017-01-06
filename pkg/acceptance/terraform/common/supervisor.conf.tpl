; This is the cockroach supervisor config template.
; It is first rendered by terraform, filling in stores, join_address, node_address,
; and cockroach_port.

[inet_http_server]
port=*:9001

[supervisord]
logfile=%(here)s/logs/supervisor.log
pidfile=%(here)s/supervisor.pid
childlogdir=%(here)s/logs
directory=%(here)s

; the below section must remain in the config file for RPC
; (supervisorctl/web interface) to work, additional interfaces may be
; added by defining them in separate rpcinterface: sections
[rpcinterface:supervisor]
supervisor.rpcinterface_factory = supervisor.rpcinterface:make_main_rpcinterface

[supervisorctl]
serverurl=http://127.0.0.1:9001 ; use an http:// url to specify an inet socket

[program:cockroach]
directory=%(here)s
command=%(here)s/cockroach start --logtostderr=true ${stores} --insecure --join=${join_address} --cache=2GiB ${cockroach_flags}
process_name=%(program_name)s
numprocs=1
autostart=false
autorestart=false
startsecs=2
startretries=0
stopwaitsecs=90
stderr_logfile=%(here)s/logs/%(program_name)s.stderr
stdout_logfile=%(here)s/logs/%(program_name)s.stdout
environment=${cockroach_env}
stderr_logfile_maxbytes=500MB
stderr_logfile_backups=10

[program:block_writer]
directory=%(here)s
command=%(here)s/block_writer --tolerate-errors --min-block-bytes=8 --max-block-bytes=128 --benchmark-name ${benchmark_name} 'postgres://root@$localhost:${cockroach_port}/?sslmode=disable'
process_name=%(program_name)s
numprocs=1
autostart=false
autorestart=false
startsecs=2
startretries=0
stderr_logfile=%(here)s/logs/%(program_name)s.stderr
stdout_logfile=%(here)s/logs/%(program_name)s.stdout

[program:photos]
directory=%(here)s
command=%(here)s/photos --users 1 --benchmark-name ${benchmark_name} --db postgres://root@localhost:${cockroach_port}/photos?sslmode=disable
process_name=%(program_name)s
numprocs=1
autostart=false
autorestart=false
startsecs=2
startretries=0
stderr_logfile=%(here)s/logs/%(program_name)s.stderr
stdout_logfile=%(here)s/logs/%(program_name)s.stdout
