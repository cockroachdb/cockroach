module github.com/cockroachdb/cockroach

go 1.15

require (
	cloud.google.com/go/storage v1.10.0
	github.com/Azure/azure-sdk-for-go v33.4.0+incompatible
	github.com/Azure/azure-storage-blob-go v0.12.0
	github.com/Azure/go-autorest/autorest v0.10.2
	github.com/Azure/go-autorest/autorest/azure/auth v0.4.2
	github.com/Azure/go-autorest/autorest/to v0.3.0
	github.com/Azure/go-autorest/autorest/validation v0.2.0 // indirect
	github.com/BurntSushi/toml v0.3.1
	github.com/DataDog/datadog-go v4.4.0+incompatible // indirect
	github.com/DataDog/zstd v1.4.8 // indirect
	github.com/MichaelTJones/walk v0.0.0-20161122175330-4748e29d5718
	github.com/PuerkitoBio/goquery v1.5.1
	github.com/Shopify/sarama v1.27.2
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
	github.com/benesch/cgosymbolizer v0.0.0-20180702220239-70e1ee2b39d3
	github.com/biogo/store v0.0.0-20160505134755-913427a1d5e8
	github.com/cenkalti/backoff v2.1.1+incompatible
	github.com/client9/misspell v0.3.4
	github.com/cockroachdb/apd/v2 v2.0.2
	github.com/cockroachdb/circuitbreaker v2.2.2-0.20190114160014-a614b14ccf63+incompatible
	github.com/cockroachdb/cmux v0.0.0-20170110192607-30d10be49292
	github.com/cockroachdb/cockroach-go v0.0.0-20200504194139-73ffeee90b62
	github.com/cockroachdb/crlfmt v0.0.0-20210128092314-b3eff0b87c79
	github.com/cockroachdb/datadriven v1.0.1-0.20201212195501-e89bf9ee1861
	github.com/cockroachdb/errors v1.8.3
	github.com/cockroachdb/go-test-teamcity v0.0.0-20191211140407-cff980ad0a55
	github.com/cockroachdb/gostdlib v1.13.0
	github.com/cockroachdb/logtags v0.0.0-20190617123548-eb05cc24525f
	github.com/cockroachdb/pebble v0.0.0-20210406003833-3d4c32f510a8
	github.com/cockroachdb/redact v1.0.9
	github.com/cockroachdb/returncheck v0.0.0-20200612231554-92cdbca611dd
	github.com/cockroachdb/sentry-go v0.6.1-cockroachdb.2
	github.com/cockroachdb/stress v0.0.0-20170808184505-29b5d31b4c3a
	github.com/cockroachdb/ttycolor v0.0.0-20180709150743-a1d5aaeb377d
	github.com/codahale/hdrhistogram v0.0.0-20161010025455-3a0bb77429bd
	github.com/coreos/go-oidc v2.2.1+incompatible
	github.com/dave/dst v0.24.0
	github.com/dgryski/go-metro v0.0.0-20180109044635-280f6062b5bc // indirect
	github.com/docker/distribution v2.7.0+incompatible
	github.com/docker/docker v17.12.0-ce-rc1.0.20190115172544-0dc531243dd3+incompatible
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
	github.com/gofrs/uuid v3.3.0+incompatible // indirect
	github.com/gogo/protobuf v1.3.2
	github.com/golang-commonmark/html v0.0.0-20180910111043-7d7c804e1d46 // indirect
	github.com/golang-commonmark/linkify v0.0.0-20180910111149-f05efb453a0e // indirect
	github.com/golang-commonmark/markdown v0.0.0-20180910011815-a8f139058164
	github.com/golang-commonmark/mdurl v0.0.0-20180910110917-8d018c6567d6 // indirect
	github.com/golang-commonmark/puny v0.0.0-20180910110745-050be392d8b8 // indirect
	github.com/golang/geo v0.0.0-20200319012246-673a6f80352d
	github.com/golang/protobuf v1.4.3
	github.com/golang/snappy v0.0.3
	github.com/google/btree v1.0.0
	github.com/google/flatbuffers v1.11.0
	github.com/google/go-cmp v0.5.2
	github.com/google/go-github v17.0.0+incompatible
	github.com/google/pprof v0.0.0-20200708004538-1a94d8640e99
	github.com/google/uuid v1.2.0 // indirect
	github.com/gorhill/cronexpr v0.0.0-20140423231348-a557574d6c02
	github.com/gorilla/mux v1.8.0
	github.com/goware/modvendor v0.3.0
	github.com/grpc-ecosystem/grpc-gateway v1.13.0
	github.com/grpc-ecosystem/grpc-opentracing v0.0.0-20180507213350-8e809c8a8645
	github.com/ianlancetaylor/cgosymbolizer v0.0.0-20201002210021-dda951febc36 // indirect
	github.com/jackc/fake v0.0.0-20150926172116-812a484cc733 // indirect
	github.com/jackc/pgconn v1.8.0
	github.com/jackc/pgproto3/v2 v2.0.7
	github.com/jackc/pgx v3.6.2+incompatible
	github.com/jackc/pgx/v4 v4.6.0
	github.com/jaegertracing/jaeger v1.17.0
	github.com/jordanlewis/gcassert v0.0.0-20200706043056-bf61eb72ee48
	github.com/kevinburke/go-bindata v3.13.0+incompatible
	github.com/kisielk/errcheck v1.2.0
	github.com/kisielk/gotool v1.0.0
	github.com/klauspost/compress v1.11.13 // indirect
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
	github.com/lusis/go-slackbot v0.0.0-20180109053408-401027ccfef5 // indirect
	github.com/lusis/slack-test v0.0.0-20190426140909-c40012f20018 // indirect
	github.com/maruel/panicparse v1.1.2-0.20180806203336-f20d4c4d746f
	github.com/marusama/semaphore v0.0.0-20190110074507-6952cef993b2
	github.com/mattn/go-isatty v0.0.12
	github.com/mattn/goveralls v0.0.2
	github.com/mibk/dupl v1.0.0
	github.com/mitchellh/reflectwalk v1.0.0
	github.com/mmatczuk/go_generics v0.0.0-20181212143635-0aaa050f9bab
	github.com/montanaflynn/stats v0.6.3
	github.com/nlopes/slack v0.4.0
	github.com/olekukonko/tablewriter v0.0.5-0.20200416053754-163badb3bac6
	github.com/opennota/wd v0.0.0-20180911144301-b446539ab1e7 // indirect
	github.com/opentracing-contrib/go-observer v0.0.0-20170622124052-a52f23424492 // indirect
	github.com/opentracing/opentracing-go v1.2.0
	github.com/openzipkin-contrib/zipkin-go-opentracing v0.3.5
	github.com/peterbourgon/g2s v0.0.0-20170223122336-d4e7ad98afea // indirect
	github.com/petermattis/goid v0.0.0-20180202154549-b0b1615b78e5
	github.com/pierrec/lz4 v2.6.0+incompatible // indirect
	github.com/pierrre/geohash v1.0.0
	github.com/pkg/browser v0.0.0-20180916011732-0a3d74bf9ce4
	github.com/pmezard/go-difflib v1.0.0
	github.com/prometheus/client_golang v1.4.1
	github.com/prometheus/client_model v0.2.0
	github.com/prometheus/common v0.9.1
	github.com/prometheus/procfs v0.0.10 // indirect
	github.com/pseudomuto/protoc-gen-doc v1.3.2
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475
	github.com/sasha-s/go-deadlock v0.2.0
	github.com/shirou/gopsutil v2.20.9+incompatible
	github.com/shopspring/decimal v1.2.0 // indirect
	github.com/spf13/cobra v0.0.5
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.6.1
	github.com/twpayne/go-geom v1.3.7-0.20210228220813-9d9885b50d3e
	github.com/wadey/gocovmerge v0.0.0-20160331181800-b5bfa59ec0ad
	github.com/xdg/scram v0.0.0-20180814205039-7eeb5667e42c
	github.com/zabawaba99/go-gitignore v0.0.0-20200117185801-39e6bddfb292
	go.etcd.io/etcd/raft/v3 v3.0.0-20210320072418-e51c697ec6e8
	golang.org/x/crypto v0.0.0-20201221181555-eec23a3978ad
	golang.org/x/exp v0.0.0-20210405174845-4513512abef3
	golang.org/x/lint v0.0.0-20200302205851-738671d3881b
	golang.org/x/net v0.0.0-20210119194325-5f4716e94777
	golang.org/x/oauth2 v0.0.0-20201109201403-9fd604954f58
	golang.org/x/perf v0.0.0-20180704124530-6e6d33e29852
	golang.org/x/sync v0.0.0-20201020160332-67f06af15bc9
	golang.org/x/sys v0.0.0-20210403161142-5e06dd20ab57
	golang.org/x/term v0.0.0-20201126162022-7de9c90e9dd1
	golang.org/x/text v0.3.5
	golang.org/x/time v0.0.0-20191024005414-555d28b269f0
	golang.org/x/tools v0.1.0
	google.golang.org/api v0.30.0
	google.golang.org/grpc v1.31.0
	google.golang.org/grpc/examples v0.0.0-20210324172016-702608ffae4d // indirect
	google.golang.org/protobuf v1.25.0
	gopkg.in/DataDog/dd-trace-go.v1 v1.28.0
	gopkg.in/yaml.v2 v2.4.0
	honnef.co/go/tools v0.0.1-2020.1.6
	vitess.io/vitess v0.0.0-00010101000000-000000000000
)

replace github.com/gogo/protobuf => github.com/cockroachdb/gogoproto v1.2.1-0.20210111172841-8b6737fea948

replace github.com/grpc-ecosystem/grpc-gateway => github.com/cockroachdb/grpc-gateway v1.14.6-0.20200519165156-52697fc4a249

replace github.com/olekukonko/tablewriter => github.com/cockroachdb/tablewriter v0.0.5-0.20200105123400-bd15540e8847

replace github.com/abourget/teamcity => github.com/cockroachdb/teamcity v0.0.0-20180905144921-8ca25c33eb11

replace vitess.io/vitess => github.com/cockroachdb/vitess v0.0.0-20210218160543-54524729cc82

replace gopkg.in/yaml.v2 => github.com/cockroachdb/yaml v0.0.0-20180705215940-0e2822948641

replace github.com/knz/go-libedit => github.com/otan-cockroach/go-libedit v1.10.2-0.20201030151939-7cced08450e7

// At the time of writing (i.e. as of this version below) the `etcd` repo is in the process of properly introducing
// modules, and as part of that uses an unsatisfiable version for this dependency (v3.0.0-00010101000000-000000000000).
// We just force it to the same SHA as the `go.etcd.io/etcd/raft/v3` module (they live in the same VCS root).
//
// While this is necessary, make sure that the require block above does not diverge.
replace go.etcd.io/etcd/pkg/v3 => go.etcd.io/etcd/pkg/v3 v3.0.0-20201109164711-01844fd28560
