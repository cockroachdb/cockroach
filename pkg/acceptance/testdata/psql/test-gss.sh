#!/usr/bin/env bash

set -euo pipefail

sudo sh -c 'echo 127.0.0.1 my.ex >> /etc/hosts'
sudo krb5kdc
sudo kadmin.local -q "addprinc -pw changeme tester@MY.EX"
echo changeme | kinit tester@MY.EX

sleep 99999999999

cd /mnt/data/go
go test
