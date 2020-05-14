# Instructions for future spelunkers:

1. mv vendor ../vendor.bak
2. go mod vendor
3. make buildshort

This should hopefully fail with the errors at the bottom of this document.

# What we did so far: (Oliver)

1. run `go mod init github.com/cockroachdb/cockroach`
* check go.mod has no `go/src` in front of it.

2. replace the replaces with this:
```
replace github.com/gogo/protobuf => github.com/cockroachdb/gogoproto v1.2.1-0.20190102194534-ca10b809dba0

replace github.com/getsentry/raven-go => github.com/cockroachdb/raven-go v0.0.0-20170605202156-221b2b44fb33

replace github.com/olekukonko/tablewriter => github.com/cockroachdb/tablewriter v0.0.5-0.20200105123400-bd15540e8847

replace github.com/abourget/teamcity => github.com/cockroachdb/teamcity v0.0.0-20180905144921-8ca25c33eb11

replace vitess.io/vitess => github.com/cockroachdb/vitess v2.2.0-rc.1.0.20180830030426-1740ce8b3188+incompatible

replace gopkg.in/yaml.v2 => github.com/cockroachdb/yaml v0.0.0-20180705215940-0e2822948641
```

and delete the require definitions for the `cockroachdb/*` versions of these forks

3. replace `go.etcd.io/etcd v0.5.0-alpha.5.0.20190816082442-4a2b4c8f7e0a` with `go.etcd.io/etcd v0.5.0-alpha.5.0.20191009222652-bbe86b066c0c`
4. change Makefile vendor directories to not install from there anymore
5. delete dep from bin/.bootstrap
6. remove go modules off switch in pkg/testutils/buildutil/build.go
7. delete examples from c-deps/protobuf/examples

----

# Current Issue as of Oliver's work:

```
$ make buildshort
bin/prereqs ./pkg/sql/colexec/execgen/cmd/execgen > bin/execgen.d.tmp
bin/prereqs: go/build: importGo github.com/lib/pq/oid: exit status 1
error writing go.mod: open /Users/otan/go/pkg/mod/github.com/lib/pq@v1.3.1-0.20200116171513-9eb3fc897d6f/go.mod298498081.tmp: permission denied


make: Failed to remake makefile `bin/execgen_out.d'.
mkdir -p lib
go install -v docgen
bin/prereqs ./pkg/cmd/docgen > bin/docgen.d.tmp
ln -sf /Users/otan/go/native/x86_64-apple-darwin19.4.0/geos/lib/lib{geos,geos_c}.dylib lib
make: *** Waiting for unfinished jobs....
bin/prereqs: go/build: importGo github.com/andybalholm/cascadia: exit status 1
error writing go.mod: open /Users/otan/go/pkg/mod/github.com/!puerkito!bio/goquery@v1.5.0/go.mod298498081.tmp: permission denied
```

Also, when deleting the vendor directory and doing `go mod vendor`, `make buildshort` complains that the directories installed by `go mod vendor` are missing (because they have no go files inside them). Example, `./vendor/github.com/gogo/protobuf/protobuf` is missing because it contains only .proto files. 

----

# Continued by Jordan:

8. `go mod tidy`
9. bugfix to prereqs that was causing the permission denied issue above
10. manually vendor the proto-only packages
11. manually move everything to apd v2 and change the go mod version to point to that

# New current issue:

make build and make buildshort work up until linking, when we get this error:

```
# github.com/cockroachdb/cockroach/pkg/cmd/cockroach-short
/usr/local/Cellar/go@1.13/1.13.10_1/libexec/pkg/tool/darwin_amd64/link: running clang++ failed: exit status 1
Undefined symbols for architecture x86_64:
  "_tgetent", referenced from:
      _terminal_set in libedit.a(terminal.o)
  "_tgetflag", referenced from:
      _terminal_set in libedit.a(terminal.o)
  "_tgetnum", referenced from:
      _terminal_set in libedit.a(terminal.o)
  "_tgetstr", referenced from:
      _terminal_set in libedit.a(terminal.o)
      _terminal_echotc in libedit.a(terminal.o)
  "_tgoto", referenced from:
      _terminal_move_to_line in libedit.a(terminal.o)
      _terminal_move_to_char in libedit.a(terminal.o)
      _terminal_deletechars in libedit.a(terminal.o)
      _terminal_insertwrite in libedit.a(terminal.o)
      _terminal_echotc in libedit.a(terminal.o)
  "_tputs", referenced from:
      _terminal_tputs in libedit.a(terminal.o)
ld: symbol(s) not found for architecture x86_64
clang: error: linker command failed with exit code 1 (use -v to see invocation)

make: *** [cockroachshort] Error 2
```

Using `go build -ldflags=-v ./pkg/cmd/cockroach`, you can see that `-lncurses`
is not included, which causes the errors above. That linker flag is supposed to
be set by the `vendor/github.com/knz/go-libedit/unix/zcgo_flags_extra.go` file.
That file is properly generated as of this branch, but does not get "picked up"
by the linker flags somehow. Not sure why.
