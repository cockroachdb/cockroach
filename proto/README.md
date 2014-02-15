# Cockroach Protobuffer API

This defines the low-level key-value API

## Hacking

The Go build tools don't support code generation very well. The approach taken here is to commit the Go generated sources along side the .proto files. This is modeled after https://github.com/golang/groupcache/blob/master/groupcachepb/groupcache.pb.go by @bradfitz

To change the API, update the proto file, run make in this directory, commit both the proto and go files.

Other references to Go code generation:
* http://jteeuwen.nl/code/go/automatic_code_generation.html
* https://plus.google.com/u/0/105521491106709880714/posts/bnUmdCGgJKD