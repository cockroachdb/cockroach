# So far.

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

# Current Issue

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

