module github.com/cockroachdb/cockroach

go 1.16

require (
	cloud.google.com/go/storage v1.10.0
	github.com/Azure/azure-sdk-for-go v33.4.0+incompatible
	github.com/Azure/azure-storage-blob-go v0.12.0
	github.com/Azure/go-autorest/autorest v0.11.1
	github.com/Azure/go-autorest/autorest/adal v0.9.5 // indirect
	github.com/Azure/go-autorest/autorest/azure/auth v0.4.2
	github.com/Azure/go-autorest/autorest/to v0.3.0
	github.com/Azure/go-autorest/autorest/validation v0.2.0 // indirect
	github.com/BurntSushi/toml v0.3.1
	github.com/DataDog/datadog-go v4.4.0+incompatible // indirect
	github.com/DataDog/zstd v1.4.8 // indirect
	github.com/MichaelTJones/walk v0.0.0-20161122175330-4748e29d5718
	github.com/Microsoft/go-winio v0.4.17 // indirect
	github.com/PuerkitoBio/goquery v1.5.1
	github.com/Shopify/sarama v1.29.0
	github.com/Shopify/toxiproxy v2.1.4+incompatible
	github.com/VividCortex/ewma v1.1.1
	github.com/abourget/teamcity v0.0.0-00010101000000-000000000000
	github.com/andy-kimball/arenaskl v0.0.0-20200617143215-f701008588b9
	github.com/andybalholm/cascadia v1.2.0 // indirect
	github.com/apache/arrow/go/arrow v0.0.0-20200610220642-670890229854
	github.com/apache/thrift v0.0.0-20181211084444-2b7365c54f82 // indirect
	github.com/armon/circbuf v0.0.0-20150827004946-bbbad097214e
	github.com/aws/aws-sdk-go v1.36.33
	github.com/axiomhq/hyperloglog v0.0.0-20181223111420-4b99d0c2c99e
	github.com/bazelbuild/rules_go v0.26.0
	github.com/biogo/store v0.0.0-20160505134755-913427a1d5e8
	github.com/bufbuild/buf v0.42.1
	github.com/cenkalti/backoff v2.1.1+incompatible
	github.com/client9/misspell v0.3.4
	github.com/cockroachdb/apd/v2 v2.0.2
	github.com/cockroachdb/circuitbreaker v2.2.2-0.20190114160014-a614b14ccf63+incompatible
	github.com/cockroachdb/cmux v0.0.0-20170110192607-30d10be49292
	github.com/cockroachdb/cockroach-go v0.0.0-20200504194139-73ffeee90b62
	github.com/cockroachdb/crlfmt v0.0.0-20210128092314-b3eff0b87c79
	github.com/cockroachdb/datadriven v1.0.1-0.20201212195501-e89bf9ee1861
	github.com/cockroachdb/errors v1.8.4
	github.com/cockroachdb/go-test-teamcity v0.0.0-20191211140407-cff980ad0a55
	github.com/cockroachdb/gostdlib v1.13.0
	github.com/cockroachdb/logtags v0.0.0-20190617123548-eb05cc24525f
	github.com/cockroachdb/pebble v0.0.0-20210622171231-4fcf40933159
	github.com/cockroachdb/redact v1.1.3
	github.com/cockroachdb/returncheck v0.0.0-20200612231554-92cdbca611dd
	github.com/cockroachdb/sentry-go v0.6.1-cockroachdb.2
	github.com/cockroachdb/stress v0.0.0-20170808184505-29b5d31b4c3a
	github.com/cockroachdb/ttycolor v0.0.0-20180709150743-a1d5aaeb377d
	github.com/codahale/hdrhistogram v0.0.0-20161010025455-3a0bb77429bd
	github.com/containerd/containerd v1.4.6
	github.com/coreos/go-oidc v2.2.1+incompatible
	github.com/dave/dst v0.24.0
	github.com/dgryski/go-metro v0.0.0-20180109044635-280f6062b5bc // indirect
	github.com/docker/distribution v2.7.1+incompatible
	github.com/docker/docker v20.10.6+incompatible
	github.com/docker/go-connections v0.4.0
	github.com/dustin/go-humanize v1.0.0
	github.com/edsrzf/mmap-go v1.0.0
	github.com/elastic/gosigar v0.10.0
	github.com/elazarl/go-bindata-assetfs v1.0.0
	github.com/emicklei/dot v0.15.0
	github.com/facebookgo/clock v0.0.0-20150410010913-600d898af40a
	github.com/ghemawat/stream v0.0.0-20171120220530-696b145b53b9
	github.com/go-sql-driver/mysql v1.5.0
	github.com/go-swagger/go-swagger v0.26.1
	github.com/gogo/googleapis v1.4.0 // indirect
	github.com/gogo/protobuf v1.3.2
	github.com/golang-commonmark/html v0.0.0-20180910111043-7d7c804e1d46 // indirect
	github.com/golang-commonmark/linkify v0.0.0-20180910111149-f05efb453a0e // indirect
	github.com/golang-commonmark/markdown v0.0.0-20180910011815-a8f139058164
	github.com/golang-commonmark/mdurl v0.0.0-20180910110917-8d018c6567d6 // indirect
	github.com/golang-commonmark/puny v0.0.0-20180910110745-050be392d8b8 // indirect
	github.com/golang/geo v0.0.0-20200319012246-673a6f80352d
	github.com/golang/glog v0.0.0-20210429001901-424d2337a529 // indirect
	github.com/golang/mock v1.5.0
	github.com/golang/protobuf v1.5.2
	github.com/golang/snappy v0.0.3
	github.com/google/btree v1.0.0
	github.com/google/flatbuffers v1.11.0
	github.com/google/go-cmp v0.5.5
	github.com/google/go-github v17.0.0+incompatible
	github.com/google/pprof v0.0.0-20200708004538-1a94d8640e99
	github.com/google/uuid v1.2.0 // indirect
	github.com/gorhill/cronexpr v0.0.0-20140423231348-a557574d6c02
	github.com/gorilla/mux v1.8.0
	github.com/goware/modvendor v0.3.0
	github.com/grpc-ecosystem/grpc-gateway v1.16.0
	github.com/grpc-ecosystem/grpc-opentracing v0.0.0-20180507213350-8e809c8a8645
	github.com/imdario/mergo v0.3.11 // indirect
	github.com/jackc/fake v0.0.0-20150926172116-812a484cc733 // indirect
	github.com/jackc/pgconn v1.8.0
	github.com/jackc/pgproto3/v2 v2.0.7
	github.com/jackc/pgx v3.6.2+incompatible
	github.com/jackc/pgx/v4 v4.6.0
	github.com/jaegertracing/jaeger v1.18.1
	github.com/jordanlewis/gcassert v0.0.0-20200706043056-bf61eb72ee48
	github.com/kevinburke/go-bindata v3.13.0+incompatible
	github.com/kisielk/errcheck v1.5.0
	github.com/kisielk/gotool v1.0.0
	github.com/knz/go-libedit v1.10.1
	github.com/knz/strtime v0.0.0-20200318182718-be999391ffa9
	github.com/kr/pretty v0.2.1
	github.com/kr/text v0.2.0
	github.com/leanovate/gopter v0.2.5-0.20190402064358-634a59d12406
	github.com/lib/pq v1.8.0
	github.com/lib/pq/auth/kerberos v0.0.0-20200720160335-984a6aa1ca46
	github.com/lightstep/lightstep-tracer-go v0.24.0
	github.com/linkedin/goavro/v2 v2.9.8
	github.com/lufia/iostat v1.0.0
	github.com/lusis/slack-test v0.0.0-20190426140909-c40012f20018 // indirect
	github.com/maruel/panicparse v1.1.2-0.20180806203336-f20d4c4d746f
	github.com/marusama/semaphore v0.0.0-20190110074507-6952cef993b2
	github.com/mattn/go-isatty v0.0.12
	github.com/mattn/goveralls v0.0.2
	github.com/matttproud/golang_protobuf_extensions v1.0.2-0.20181231171920-c182affec369 // indirect
	github.com/mibk/dupl v1.0.0
	github.com/mitchellh/reflectwalk v1.0.0
	github.com/mmatczuk/go_generics v0.0.0-20181212143635-0aaa050f9bab
	github.com/moby/term v0.0.0-20201216013528-df9cb8a40635 // indirect
	github.com/montanaflynn/stats v0.6.3
	github.com/morikuni/aec v1.0.0 // indirect
	github.com/nlopes/slack v0.4.0
	github.com/olekukonko/tablewriter v0.0.5-0.20200416053754-163badb3bac6
	github.com/onsi/gomega v1.10.3 // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/opencontainers/image-spec v1.0.1
	github.com/opennota/wd v0.0.0-20180911144301-b446539ab1e7 // indirect
	github.com/opentracing-contrib/go-observer v0.0.0-20170622124052-a52f23424492 // indirect
	github.com/opentracing/opentracing-go v1.2.0
	github.com/openzipkin-contrib/zipkin-go-opentracing v0.3.5
	github.com/peterbourgon/g2s v0.0.0-20170223122336-d4e7ad98afea // indirect
	github.com/petermattis/goid v0.0.0-20180202154549-b0b1615b78e5
	github.com/pierrre/geohash v1.0.0
	github.com/pkg/browser v0.0.0-20180916011732-0a3d74bf9ce4
	github.com/pmezard/go-difflib v1.0.0
	github.com/prometheus/client_golang v1.7.1
	github.com/prometheus/client_model v0.2.1-0.20210607210712-147c58e9608a
	github.com/prometheus/common v0.10.0
	github.com/prometheus/procfs v0.6.0 // indirect
	github.com/pseudomuto/protoc-gen-doc v1.3.2
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475
	github.com/russross/blackfriday v1.6.0 // indirect
	github.com/sasha-s/go-deadlock v0.2.0
	github.com/shirou/gopsutil v2.20.9+incompatible
	github.com/shopspring/decimal v1.2.0 // indirect
	github.com/spf13/cobra v1.1.3
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.7.0
	github.com/twpayne/go-geom v1.3.7-0.20210228220813-9d9885b50d3e
	github.com/wadey/gocovmerge v0.0.0-20160331181800-b5bfa59ec0ad
	github.com/xdg/scram v1.0.3
	github.com/zabawaba99/go-gitignore v0.0.0-20200117185801-39e6bddfb292
	go.etcd.io/etcd/raft/v3 v3.0.0-20210320072418-e51c697ec6e8
	golang.org/x/crypto v0.0.0-20210421170649-83a5a9bb288b
	golang.org/x/exp v0.0.0-20210514180818-737f94c0881e
	golang.org/x/lint v0.0.0-20210508222113-6edffad5e616
	golang.org/x/net v0.0.0-20210525063256-abc453219eb5
	golang.org/x/oauth2 v0.0.0-20201109201403-9fd604954f58
	golang.org/x/perf v0.0.0-20180704124530-6e6d33e29852
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/sys v0.0.0-20210603125802-9665404d3644
	golang.org/x/term v0.0.0-20201126162022-7de9c90e9dd1
	golang.org/x/text v0.3.6
	golang.org/x/time v0.0.0-20210220033141-f8bda1e9f3ba
	golang.org/x/tools v0.1.2
	google.golang.org/api v0.30.0
	google.golang.org/genproto v0.0.0-20210603172842-58e84a565dcf
	google.golang.org/grpc v1.38.0
	google.golang.org/grpc/examples v0.0.0-20210324172016-702608ffae4d // indirect
	google.golang.org/protobuf v1.26.0
	gopkg.in/DataDog/dd-trace-go.v1 v1.28.0
	gopkg.in/yaml.v2 v2.4.0
	gotest.tools/v3 v3.0.3 // indirect
	honnef.co/go/tools v0.2.0
	vitess.io/vitess v0.0.0-00010101000000-000000000000
)

replace github.com/olekukonko/tablewriter => github.com/cockroachdb/tablewriter v0.0.5-0.20200105123400-bd15540e8847

replace github.com/abourget/teamcity => github.com/cockroachdb/teamcity v0.0.0-20180905144921-8ca25c33eb11

// Temporary replacement until https://github.com/cockroachdb/cockroach/pull/67221#issuecomment-874236134
// has been addressed.
replace github.com/bufbuild/buf => github.com/cockroachdb/buf v0.42.2-0.20210706083726-883d34031660

replace honnef.co/go/tools => honnef.co/go/tools v0.0.1-2020.1.6

replace vitess.io/vitess => github.com/cockroachdb/vitess v0.0.0-20210218160543-54524729cc82

replace gopkg.in/yaml.v2 => github.com/cockroachdb/yaml v0.0.0-20180705215940-0e2822948641

replace github.com/knz/go-libedit => github.com/otan-cockroach/go-libedit v1.10.2-0.20201030151939-7cced08450e7

// Temporary workaround for #65320.
replace github.com/lib/pq => github.com/cockroachdb/pq v0.0.0-20210517091544-990dd3347596

// At the time of writing (i.e. as of this version below) the `etcd` repo is in the process of properly introducing
// modules, and as part of that uses an unsatisfiable version for this dependency (v3.0.0-00010101000000-000000000000).
// We just force it to the same SHA as the `go.etcd.io/etcd/raft/v3` module (they live in the same VCS root).
//
// While this is necessary, make sure that the require block above does not diverge.
replace go.etcd.io/etcd/pkg/v3 => go.etcd.io/etcd/pkg/v3 v3.0.0-20201109164711-01844fd28560

replace github.com/docker/docker => github.com/moby/moby v20.10.6+incompatible
