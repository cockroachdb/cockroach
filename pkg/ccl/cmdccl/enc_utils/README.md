This is a small utility to read encrypted rocksdb files.


Given a key and a cockroach node started with:
```
$ openssl rand 48 > aes-128.key 
$ cockroach start \
  --enterprise-encryption=path=cockroach-data,key=aes-128.key,old-key=plain
```

We can run this utility by pointing it to the key and data directory:
```
$ go run main.go --db-dir cockroach-data --store-key aes-128.key
I180119 13:40:58.021531 1 ccl/cmdccl/enc_utils/main.go:125  store key: AES128_CTR len: 16
I180119 13:40:58.021575 1 ccl/cmdccl/enc_utils/main.go:77  file registry version: Base
I180119 13:40:58.021594 1 ccl/cmdccl/enc_utils/main.go:92  file registry contains 20 entries
I180119 13:40:58.021612 1 ccl/cmdccl/enc_utils/main.go:198  decrypting COCKROACHDB_DATA_KEYS with AES128_CTR key 74bc2e29...
I180119 13:40:58.021627 1 ccl/cmdccl/enc_utils/main.go:142  data key registry contains 1 data key(s)
I180119 13:40:58.021642 1 ccl/cmdccl/enc_utils/main.go:198  decrypting CURRENT with AES128_CTR key 75b27bcc...
I180119 13:40:58.021650 1 ccl/cmdccl/enc_utils/main.go:151  current: MANIFEST-000008
I180119 13:40:58.021733 1 ccl/cmdccl/enc_utils/main.go:198  decrypting OPTIONS-000005 with AES128_CTR key 75b27bcc...
I180119 13:40:58.021757 1 ccl/cmdccl/enc_utils/main.go:167  options file: OPTIONS-000005 starts with: # This is a RocksDB option file.
#
# For detailed file format spec, please refer to the example file
```
