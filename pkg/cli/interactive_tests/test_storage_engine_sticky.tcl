#! /usr/bin/env expect -f
#
source [file join [file dirname $argv0] common.tcl]

set storedir "encryption_store"
set keydir "$storedir/keys"

spawn /bin/bash
send "PS1=':''/# '\r"
eexpect ":/# "

start_test "Generate encryption keys."
send "mkdir -p $keydir\n"
send "$argv gen encryption-key -s 128 $keydir/aes-128.key\r"
eexpect "successfully created AES-128 key: $keydir/aes-128.key"
end_test

start_test "Start normal node with default engine."
send "$argv start-single-node --insecure --store=$storedir\r"
eexpect "storage engine: *pebble"
interrupt
eexpect "shutdown completed"
end_test

start_test "Restart normal node with non-default engine specified."
send "$argv start-single-node --insecure --store=$storedir --storage-engine=rocksdb\r"
eexpect "storage engine: *rocksdb"
interrupt
eexpect "shutdown completed"
end_test

start_test "Restart normal node; should resort to non-default engine."
send "$argv start-single-node --insecure --store=$storedir\r"
eexpect "storage engine: *rocksdb"
interrupt
eexpect "shutdown completed"
end_test

start_test "Restart normal node with default engine specified."
send "$argv start-single-node --insecure --store=$storedir --storage-engine=pebble\r"
eexpect "storage engine: *pebble"
interrupt
eexpect "shutdown completed"
end_test

start_test "Restart with AES-128."
send "$argv start-single-node --insecure --store=$storedir --enterprise-encryption=path=$storedir,key=$keydir/aes-128.key,old-key=plain\r"
eexpect "storage engine: *pebble"
interrupt
eexpect "shutdown completed"
send "$argv debug encryption-status $storedir --enterprise-encryption=path=$storedir,key=$keydir/aes-128.key,old-key=plain\r"
eexpect "    \"Active\": true,\r\n    \"Type\": \"AES128_CTR\","
end_test

start_test "Restart with AES-128 and specify non-default engine."
send "$argv start-single-node --insecure --store=$storedir --enterprise-encryption=path=$storedir,key=$keydir/aes-128.key,old-key=plain --storage-engine=rocksdb\r"
eexpect "storage engine: *rocksdb"
interrupt
eexpect "shutdown completed"
send "$argv debug encryption-status $storedir --enterprise-encryption=path=$storedir,key=$keydir/aes-128.key,old-key=plain\r"
eexpect "    \"Active\": true,\r\n    \"Type\": \"AES128_CTR\","
end_test

start_test "Restart with AES-128 and engine unspecified; should resolve to non-default engine."
send "$argv start-single-node --insecure --store=$storedir --enterprise-encryption=path=$storedir,key=$keydir/aes-128.key,old-key=plain\r"
eexpect "storage engine: *rocksdb"
interrupt
eexpect "shutdown completed"
send "$argv debug encryption-status $storedir --enterprise-encryption=path=$storedir,key=$keydir/aes-128.key,old-key=plain\r"
eexpect "    \"Active\": true,\r\n    \"Type\": \"AES128_CTR\","
end_test
