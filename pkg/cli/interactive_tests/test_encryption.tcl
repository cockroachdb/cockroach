#! /usr/bin/env expect -f
#
source [file join [file dirname $argv0] common.tcl]

set storedir "encryption_store"
set keydir "$storedir/keys"

spawn /bin/bash
send "PS1=':''/# '\r"
eexpect ":/# "

proc file_has_size {filepath size} {
  if {! [file exist $filepath]} {
    report "MISSING EXPECTED FILE: $filepath"
    exit 1
  }
  set fsize [file size $filepath]
  if { $fsize != $size } {
		report "WRONG FILE SIZE FOR: $filepath. EXPECTED $size, GOT $fsize"
		exit 1
	}
}

start_test "Generate encryption keys."
send "mkdir -p $keydir\n"
send "$argv gen encryption-key -s 128 $keydir/aes-128.key\r"
eexpect "successfully created AES-128 key: $keydir/aes-128.key"
send "$argv gen encryption-key -s 192 $keydir/aes-192.key\r"
eexpect "successfully created AES-192 key: $keydir/aes-192.key"
send "$argv gen encryption-key -s 256 $keydir/aes-256.key\r"
eexpect "successfully created AES-256 key: $keydir/aes-256.key"
file_has_size "$keydir/aes-128.key" "48"
file_has_size "$keydir/aes-192.key" "56"
file_has_size "$keydir/aes-256.key" "64"
end_test

start_test "Start normal node."
send "$argv start-single-node --insecure --store=$storedir\r"
eexpect "node starting"
interrupt
eexpect "shutdown completed"
send "$argv debug encryption-status $storedir\r"
eexpect ""
end_test

start_test "Run pebble debug tool."
send "$argv debug pebble db lsm $storedir --store=$storedir\r"
eexpect "__level_____count____size___score______in__ingest(sz_cnt)____move(sz_cnt)___write(sz_cnt)____read___r-amp___w-amp\r"
end_test

start_test "Restart with plaintext."
send "$argv start-single-node --insecure --store=$storedir --enterprise-encryption=path=$storedir,key=plain,old-key=plain\r"
eexpect "node starting"
interrupt
eexpect "shutdown completed"
send "$argv debug encryption-status $storedir --enterprise-encryption=path=$storedir,key=plain,old-key=plain\r"
eexpect "    \"Active\": true,\r\n    \"Type\": \"Plaintext\","
# Try starting without the encryption flag.
send "$argv start-single-node --insecure --store=$storedir\r"
eexpect "encryption was used on this store before, but no encryption flags specified."
end_test

start_test "Restart with AES-128."
send "$argv start-single-node --insecure --store=$storedir --enterprise-encryption=path=$storedir,key=$keydir/aes-128.key,old-key=plain\r"
eexpect "node starting"
interrupt
eexpect "shutdown completed"
send "$argv debug encryption-status $storedir --enterprise-encryption=path=$storedir,key=$keydir/aes-128.key,old-key=plain\r"
eexpect "    \"Active\": true,\r\n    \"Type\": \"AES128_CTR\","
# Try starting without the encryption flag.
send "$argv start-single-node --insecure --store=$storedir\r"
eexpect "encryption was used on this store before, but no encryption flags specified."
# Try with the wrong key.
send "$argv start-single-node --insecure --store=$storedir --enterprise-encryption=path=$storedir,key=$keydir/aes-192.key,old-key=plain\r"
eexpect "store key ID * was not found"
end_test

start_test "Restart with AES-256."
send "$argv start-single-node --insecure --store=$storedir --enterprise-encryption=path=$storedir,key=$keydir/aes-256.key,old-key=$keydir/aes-128.key\r"
eexpect "node starting"
interrupt
eexpect "shutdown completed"
send "$argv debug encryption-status $storedir --enterprise-encryption=path=$storedir,key=$keydir/aes-256.key,old-key=plain\r"
eexpect "    \"Active\": true,\r\n    \"Type\": \"AES256_CTR\","
# Startup again, but don't specify the old key, it's no longer in use.
send "$argv start-single-node --insecure --store=$storedir --enterprise-encryption=path=$storedir,key=$keydir/aes-256.key,old-key=plain\r"
eexpect "node starting"
interrupt
# Try starting without the encryption flag.
send "$argv start-single-node --insecure --store=$storedir\r"
eexpect "encryption was used on this store before, but no encryption flags specified."
# Try with the wrong key.
send "$argv start-single-node --insecure --store=$storedir --enterprise-encryption=path=$storedir,key=$keydir/aes-192.key,old-key=plain\r"
eexpect "store key ID * was not found"
end_test

start_test "Run pebble debug tool with AES-256."
send "$argv debug pebble db lsm $storedir --store=$storedir --enterprise-encryption=path=$storedir,key=$keydir/aes-256.key,old-key=$keydir/aes-256.key\r"
eexpect "__level_____count____size___score______in__ingest(sz_cnt)____move(sz_cnt)___write(sz_cnt)____read___r-amp___w-amp\r"
# Try running without the encryption flag.
send "$argv debug pebble db lsm $storedir --store=$storedir\r"
eexpect "encryption was used on this store before, but no encryption flags specified."
end_test
