module github.com/cockroachdb/cockroach

go 1.17

require (
	cloud.google.com/go/kms v1.1.0
	cloud.google.com/go/pubsub v1.16.0
	cloud.google.com/go/storage v1.18.2
	github.com/Azure/azure-sdk-for-go v57.1.0+incompatible
	github.com/Azure/azure-storage-blob-go v0.14.0
	github.com/Azure/go-autorest/autorest v0.11.20
	github.com/Azure/go-autorest/autorest/azure/auth v0.5.8
	github.com/Azure/go-autorest/autorest/to v0.4.0
	github.com/BurntSushi/toml v0.4.1
	github.com/Masterminds/semver/v3 v3.1.1
	github.com/MichaelTJones/walk v0.0.0-20161122175330-4748e29d5718
	github.com/PuerkitoBio/goquery v1.5.1
	github.com/Shopify/sarama v1.29.0
	github.com/Shopify/toxiproxy v2.1.4+incompatible
	github.com/VividCortex/ewma v1.1.1
	github.com/abourget/teamcity v0.0.0-00010101000000-000000000000
	github.com/alessio/shellescape v1.4.1
	github.com/andy-kimball/arenaskl v0.0.0-20200617143215-f701008588b9
	github.com/andygrunwald/go-jira v1.14.0
	github.com/apache/arrow/go/arrow v0.0.0-20200923215132-ac86123a3f01
	github.com/armon/circbuf v0.0.0-20150827004946-bbbad097214e
	github.com/aws/aws-sdk-go v1.40.37
	github.com/aws/aws-sdk-go-v2 v1.9.1
	github.com/aws/aws-sdk-go-v2/config v1.8.2
	github.com/aws/aws-sdk-go-v2/service/ec2 v1.18.0
	github.com/aws/aws-sdk-go-v2/service/iam v1.10.0
	github.com/axiomhq/hyperloglog v0.0.0-20181223111420-4b99d0c2c99e
	github.com/bazelbuild/rules_go v0.26.0
	github.com/biogo/store v0.0.0-20160505134755-913427a1d5e8
	github.com/buchgr/bazel-remote v1.3.3
	github.com/bufbuild/buf v0.56.0
	github.com/cenkalti/backoff v2.2.1+incompatible
	github.com/client9/misspell v0.3.4
	github.com/cockroachdb/apd/v3 v3.1.0
	github.com/cockroachdb/circuitbreaker v2.2.2-0.20190114160014-a614b14ccf63+incompatible
	github.com/cockroachdb/cmux v0.0.0-20170110192607-30d10be49292
	github.com/cockroachdb/cockroach-go/v2 v2.2.6
	github.com/cockroachdb/crlfmt v0.0.0-20210128092314-b3eff0b87c79
	github.com/cockroachdb/datadriven v1.0.1
	github.com/cockroachdb/errors v1.8.9
	github.com/cockroachdb/go-test-teamcity v0.0.0-20191211140407-cff980ad0a55
	github.com/cockroachdb/gostdlib v1.13.0
	github.com/cockroachdb/logtags v0.0.0-20211118104740-dabe8e521a4f
	github.com/cockroachdb/pebble v0.0.0-20220301234049-69a82fe41c31
	github.com/cockroachdb/redact v1.1.3
	github.com/cockroachdb/returncheck v0.0.0-20200612231554-92cdbca611dd
	github.com/cockroachdb/stress v0.0.0-20220217190341-94cf65c2a29f
	github.com/cockroachdb/tools v0.0.0-20211112185054-642e51449b40
	github.com/cockroachdb/ttycolor v0.0.0-20210902133924-c7d7dcdde4e8
	github.com/codahale/hdrhistogram v0.0.0-20161010025455-3a0bb77429bd
	github.com/containerd/containerd v1.5.4
	github.com/coreos/go-oidc v2.2.1+incompatible
	github.com/dave/dst v0.24.0
	github.com/docker/distribution v2.7.1+incompatible
	github.com/docker/docker v20.10.8+incompatible
	github.com/docker/go-connections v0.4.0
	github.com/dustin/go-humanize v1.0.0
	github.com/edsrzf/mmap-go v1.0.0
	github.com/elastic/gosigar v0.14.1
	github.com/emicklei/dot v0.15.0
	github.com/facebookgo/clock v0.0.0-20150410010913-600d898af40a
	github.com/fraugster/parquet-go v0.10.0
	github.com/fsnotify/fsnotify v1.5.1
	github.com/getsentry/sentry-go v0.12.0
	github.com/ghemawat/stream v0.0.0-20171120220530-696b145b53b9
	github.com/go-sql-driver/mysql v1.6.0
	github.com/go-swagger/go-swagger v0.26.1
	github.com/gogo/protobuf v1.3.2
	github.com/golang-commonmark/markdown v0.0.0-20180910011815-a8f139058164
	github.com/golang/geo v0.0.0-20200319012246-673a6f80352d
	github.com/golang/mock v1.6.0
	github.com/golang/protobuf v1.5.2
	github.com/golang/snappy v0.0.4
	github.com/google/btree v1.0.1
	github.com/google/flatbuffers v2.0.0+incompatible
	github.com/google/go-cmp v0.5.6
	github.com/google/go-github v17.0.0+incompatible
	github.com/google/pprof v0.0.0-20210827144239-02619b876842
	github.com/google/shlex v0.0.0-20191202100458-e7afc7fbc510
	github.com/google/skylark v0.0.0-20181101142754-a5f7082aabed
	github.com/gorilla/mux v1.8.0
	github.com/goware/modvendor v0.5.0
	github.com/grpc-ecosystem/grpc-gateway v1.16.0
	github.com/irfansharif/recorder v0.0.0-20211218081646-a21b46510fd6
	github.com/jackc/pgconn v1.10.0
	github.com/jackc/pgproto3/v2 v2.1.1
	github.com/jackc/pgtype v1.8.1
	github.com/jackc/pgx/v4 v4.13.0
	github.com/jaegertracing/jaeger v1.18.1
	github.com/jordan-wright/email v4.0.1-0.20210109023952-943e75fe5223+incompatible
	github.com/jordanlewis/gcassert v0.0.0-20210709222130-81f5df3faab8
	github.com/kevinburke/go-bindata v3.13.0+incompatible
	github.com/kisielk/errcheck v1.6.1-0.20210625163953-8ddee489636a
	github.com/kisielk/gotool v1.0.0
	github.com/knz/go-libedit v1.10.1
	github.com/knz/strtime v0.0.0-20200318182718-be999391ffa9
	github.com/kr/pretty v0.3.0
	github.com/kr/text v0.2.0
	github.com/kylelemons/godebug v1.1.0
	github.com/leanovate/gopter v0.2.5-0.20190402064358-634a59d12406
	github.com/lib/pq v1.10.2
	github.com/lib/pq/auth/kerberos v0.0.0-20200720160335-984a6aa1ca46
	github.com/linkedin/goavro/v2 v2.10.0
	github.com/lufia/iostat v1.2.1
	github.com/maruel/panicparse/v2 v2.2.0
	github.com/marusama/semaphore v0.0.0-20190110074507-6952cef993b2
	github.com/mattn/go-isatty v0.0.14
	github.com/mattn/goveralls v0.0.2
	github.com/mibk/dupl v1.0.0
	github.com/mitchellh/reflectwalk v1.0.0
	github.com/mmatczuk/go_generics v0.0.0-20181212143635-0aaa050f9bab
	github.com/montanaflynn/stats v0.6.3
	github.com/olekukonko/tablewriter v0.0.5-0.20200416053754-163badb3bac6
	github.com/opencontainers/image-spec v1.0.1
	github.com/petermattis/goid v0.0.0-20211229010228-4d14c490ee36
	github.com/pierrre/geohash v1.0.0
	github.com/pkg/browser v0.0.0-20180916011732-0a3d74bf9ce4
	github.com/pmezard/go-difflib v1.0.0
	github.com/prometheus/client_golang v1.12.0
	github.com/prometheus/client_model v0.2.1-0.20210607210712-147c58e9608a
	github.com/prometheus/common v0.32.1
	github.com/prometheus/prometheus v1.8.2-0.20210914090109-37468d88dce8
	github.com/pseudomuto/protoc-gen-doc v1.3.2
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475
	github.com/robfig/cron/v3 v3.0.1
	github.com/sasha-s/go-deadlock v0.3.1
	github.com/shirou/gopsutil/v3 v3.21.12
	github.com/slack-go/slack v0.9.5
	github.com/spf13/cobra v1.2.1
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.7.0
	github.com/twpayne/go-geom v1.4.1
	github.com/wadey/gocovmerge v0.0.0-20160331181800-b5bfa59ec0ad
	github.com/xdg-go/scram v1.0.2
	github.com/xdg-go/stringprep v1.0.2
	github.com/zabawaba99/go-gitignore v0.0.0-20200117185801-39e6bddfb292
	go.etcd.io/etcd/raft/v3 v3.0.0-20210320072418-e51c697ec6e8
	go.opentelemetry.io/otel v1.0.0-RC3
	go.opentelemetry.io/otel/exporters/jaeger v1.0.0-RC3
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.0.0-RC3
	go.opentelemetry.io/otel/exporters/zipkin v1.0.0-RC3
	go.opentelemetry.io/otel/sdk v1.0.0-RC3
	go.opentelemetry.io/otel/trace v1.0.0-RC3
	golang.org/x/crypto v0.0.0-20220112180741-5e0467b6c7ce
	golang.org/x/exp v0.0.0-20220104160115-025e73f80486
	golang.org/x/lint v0.0.0-20210508222113-6edffad5e616
	golang.org/x/net v0.0.0-20220121210141-e204ce36a2ba
	golang.org/x/oauth2 v0.0.0-20211104180415-d3ed0bb246c8
	golang.org/x/perf v0.0.0-20180704124530-6e6d33e29852
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/sys v0.0.0-20220209214540-3681064d5158
	golang.org/x/term v0.0.0-20210927222741-03fcf44c2211
	golang.org/x/text v0.3.7
	golang.org/x/time v0.0.0-20210723032227-1f47c861a9ac
	golang.org/x/tools v0.1.8-0.20211029000441-d6a9af8af023
	google.golang.org/api v0.65.0
	google.golang.org/genproto v0.0.0-20220118154757-00ab72f36ad5
	google.golang.org/grpc v1.43.0
	google.golang.org/protobuf v1.27.1
	gopkg.in/yaml.v2 v2.4.0
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b
	honnef.co/go/tools v0.2.1
	vitess.io/vitess v0.0.0-00010101000000-000000000000
)

require (
	cloud.google.com/go v0.100.2 // indirect
	cloud.google.com/go/compute v1.1.0 // indirect
	cloud.google.com/go/iam v0.1.1 // indirect
	github.com/Azure/azure-pipeline-go v0.2.3 // indirect
	github.com/Azure/go-ansiterm v0.0.0-20170929234023-d6e3b3328b78 // indirect
	github.com/Azure/go-autorest v14.2.0+incompatible // indirect
	github.com/Azure/go-autorest/autorest/adal v0.9.15 // indirect
	github.com/Azure/go-autorest/autorest/date v0.3.0 // indirect
	github.com/Azure/go-autorest/autorest/validation v0.3.1 // indirect
	github.com/Azure/go-autorest/logger v0.2.1 // indirect
	github.com/Azure/go-autorest/tracing v0.6.0 // indirect
	github.com/Masterminds/goutils v1.1.0 // indirect
	github.com/Masterminds/semver v1.5.0 // indirect
	github.com/Masterminds/sprig v2.22.0+incompatible // indirect
	github.com/Microsoft/go-winio v0.4.17 // indirect
	github.com/PuerkitoBio/purell v1.1.1 // indirect
	github.com/PuerkitoBio/urlesc v0.0.0-20170810143723-de5bf2ad4578 // indirect
	github.com/abbot/go-http-auth v0.4.1-0.20181019201920-860ed7f246ff // indirect
	github.com/alexbrainman/sspi v0.0.0-20180613141037-e580b900e9f5 // indirect
	github.com/apache/thrift v0.16.0 // indirect
	github.com/asaskevich/govalidator v0.0.0-20200907205600-7a23bdc65eef // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.4.2 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.5.1 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.2.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.3.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.4.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.7.1 // indirect
	github.com/aws/smithy-go v1.8.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bgentry/go-netrc v0.0.0-20140422174119-9fd32a8b3d3d // indirect
	github.com/cenkalti/backoff/v4 v4.1.1 // indirect
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dimchansky/utfbom v1.1.1 // indirect
	github.com/djherbis/atime v1.1.0 // indirect
	github.com/docker/go-units v0.4.0 // indirect
	github.com/eapache/go-resiliency v1.2.0 // indirect
	github.com/eapache/go-xerial-snappy v0.0.0-20180814174437-776d5712da21 // indirect
	github.com/eapache/queue v1.1.0 // indirect
	github.com/fatih/structs v1.1.0 // indirect
	github.com/felixge/httpsnoop v1.0.1 // indirect
	github.com/ghodss/yaml v1.0.0 // indirect
	github.com/go-kit/log v0.1.0 // indirect
	github.com/go-logfmt/logfmt v0.5.1 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/go-openapi/analysis v0.20.0 // indirect
	github.com/go-openapi/errors v0.20.0 // indirect
	github.com/go-openapi/inflect v0.19.0 // indirect
	github.com/go-openapi/jsonpointer v0.19.5 // indirect
	github.com/go-openapi/jsonreference v0.19.5 // indirect
	github.com/go-openapi/loads v0.20.2 // indirect
	github.com/go-openapi/runtime v0.19.29 // indirect
	github.com/go-openapi/spec v0.20.3 // indirect
	github.com/go-openapi/strfmt v0.20.2 // indirect
	github.com/go-openapi/swag v0.19.15 // indirect
	github.com/go-openapi/validate v0.20.2 // indirect
	github.com/go-stack/stack v1.8.0 // indirect
	github.com/gofrs/flock v0.8.1 // indirect
	github.com/gofrs/uuid v4.0.0+incompatible // indirect
	github.com/gogo/googleapis v1.4.1 // indirect
	github.com/gogo/status v1.1.0 // indirect
	github.com/golang-jwt/jwt v3.2.2+incompatible // indirect
	github.com/golang-jwt/jwt/v4 v4.0.0 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/google/go-querystring v1.0.0 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/googleapis/gax-go/v2 v2.1.1 // indirect
	github.com/gorilla/handlers v1.5.1 // indirect
	github.com/gorilla/websocket v1.4.2 // indirect
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0 // indirect
	github.com/hashicorp/go-uuid v1.0.2 // indirect
	github.com/hashicorp/hcl v1.0.0 // indirect
	github.com/huandu/xstrings v1.3.0 // indirect
	github.com/ianlancetaylor/demangle v0.0.0-20200824232613-28f6c0f3b639 // indirect
	github.com/imdario/mergo v0.3.11 // indirect
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/jackc/chunkreader/v2 v2.0.1 // indirect
	github.com/jackc/pgio v1.0.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20200714003250-2b9c44734f2b // indirect
	github.com/jackc/puddle v1.1.3 // indirect
	github.com/jcmturner/aescts/v2 v2.0.0 // indirect
	github.com/jcmturner/dnsutils/v2 v2.0.0 // indirect
	github.com/jcmturner/gofork v1.0.0 // indirect
	github.com/jcmturner/goidentity/v6 v6.0.1 // indirect
	github.com/jcmturner/gokrb5/v8 v8.4.2 // indirect
	github.com/jcmturner/rpc/v2 v2.0.3 // indirect
	github.com/jessevdk/go-flags v1.5.0 // indirect
	github.com/jhump/protoreflect v1.9.1-0.20210817181203-db1a327a393e // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.14.2 // indirect
	github.com/klauspost/cpuid/v2 v2.0.9 // indirect
	github.com/klauspost/pgzip v1.2.5 // indirect
	github.com/lufia/plan9stats v0.0.0-20211012122336-39d0f177ccd0 // indirect
	github.com/magiconair/properties v1.8.5 // indirect
	github.com/mailru/easyjson v0.7.6 // indirect
	github.com/mattn/go-ieproxy v0.0.1 // indirect
	github.com/mattn/go-runewidth v0.0.10 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.2-0.20181231171920-c182affec369 // indirect
	github.com/minio/md5-simd v1.1.2 // indirect
	github.com/minio/minio-go/v7 v7.0.21 // indirect
	github.com/minio/sha256-simd v1.0.0 // indirect
	github.com/mitchellh/copystructure v1.0.0 // indirect
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/mitchellh/mapstructure v1.4.1 // indirect
	github.com/moby/term v0.0.0-20201216013528-df9cb8a40635 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/morikuni/aec v1.0.0 // indirect
	github.com/mostynb/go-grpc-compression v1.1.12 // indirect
	github.com/mwitkow/go-proto-validators v0.0.0-20180403085117-0950a7990007 // indirect
	github.com/oklog/ulid v1.3.1 // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/openzipkin/zipkin-go v0.2.5 // indirect
	github.com/pelletier/go-toml v1.9.3 // indirect
	github.com/pierrec/lz4 v2.6.0+incompatible // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pkg/profile v1.6.0 // indirect
	github.com/power-devops/perfstat v0.0.0-20210106213030-5aafc221ea8c // indirect
	github.com/pquerna/cachecontrol v0.0.0-20200921180117-858c6e7e6b7e // indirect
	github.com/prometheus/procfs v0.7.3 // indirect
	github.com/pseudomuto/protokit v0.2.0 // indirect
	github.com/rivo/uniseg v0.1.0 // indirect
	github.com/rogpeppe/go-internal v1.8.1 // indirect
	github.com/rs/xid v1.3.0 // indirect
	github.com/russross/blackfriday v1.6.0 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/sirupsen/logrus v1.8.1 // indirect
	github.com/slok/go-http-metrics v0.10.0 // indirect
	github.com/spf13/afero v1.6.0 // indirect
	github.com/spf13/cast v1.3.1 // indirect
	github.com/spf13/jwalterweatherman v1.1.0 // indirect
	github.com/spf13/viper v1.8.1 // indirect
	github.com/subosito/gotenv v1.2.0 // indirect
	github.com/tklauser/go-sysconf v0.3.9 // indirect
	github.com/tklauser/numcpus v0.3.0 // indirect
	github.com/toqueteos/webbrowser v1.2.0 // indirect
	github.com/trivago/tgo v1.0.7 // indirect
	github.com/twitchtv/twirp v8.1.0+incompatible // indirect
	github.com/twpayne/go-kml v1.5.2 // indirect
	github.com/urfave/cli/v2 v2.3.0 // indirect
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/yusufpapurcu/wmi v1.2.2 // indirect
	go.mongodb.org/mongo-driver v1.5.1 // indirect
	go.opencensus.io v0.23.0 // indirect
	go.opentelemetry.io/proto/otlp v0.9.0 // indirect
	go.uber.org/atomic v1.9.0 // indirect
	go.uber.org/multierr v1.7.0 // indirect
	go.uber.org/zap v1.19.0 // indirect
	golang.org/x/mod v0.6.0-dev.0.20211013180041-c96bc1413d57 // indirect
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	gopkg.in/square/go-jose.v2 v2.5.1 // indirect
)

require (
	github.com/Azure/go-autorest/autorest/azure/cli v0.4.3 // indirect
	github.com/DataDog/zstd v1.5.0 // indirect
	github.com/andybalholm/cascadia v1.2.0 // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.1 // indirect
	github.com/dgryski/go-metro v0.0.0-20180109044635-280f6062b5bc // indirect
	github.com/envoyproxy/protoc-gen-validate v0.6.2 // indirect
	github.com/golang-commonmark/html v0.0.0-20180910111043-7d7c804e1d46 // indirect
	github.com/golang-commonmark/linkify v0.0.0-20180910111149-f05efb453a0e // indirect
	github.com/golang-commonmark/mdurl v0.0.0-20180910110917-8d018c6567d6 // indirect
	github.com/golang-commonmark/puny v0.0.0-20180910110745-050be392d8b8 // indirect
	github.com/golang/glog v0.0.0-20210429001901-424d2337a529 // indirect
	github.com/mattn/go-zglob v0.0.3 // indirect
	github.com/opennota/wd v0.0.0-20180911144301-b446539ab1e7 // indirect
	github.com/peterbourgon/g2s v0.0.0-20170223122336-d4e7ad98afea // indirect
	// The indicated commit is required on top of v1.0.0-RC3 because
	// it fixes an import comment that otherwise breaks our prereqs tool.
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.0.0-RC3.0.20210907151655-df2bdbbadb26 // indirect
	google.golang.org/grpc/examples v0.0.0-20210324172016-702608ffae4d // indirect
	gopkg.in/ini.v1 v1.66.3 // indirect
)

replace github.com/olekukonko/tablewriter => github.com/cockroachdb/tablewriter v0.0.5-0.20200105123400-bd15540e8847

replace github.com/abourget/teamcity => github.com/cockroachdb/teamcity v0.0.0-20180905144921-8ca25c33eb11

replace vitess.io/vitess => github.com/cockroachdb/vitess v0.0.0-20210218160543-54524729cc82

replace gopkg.in/yaml.v2 => github.com/cockroachdb/yaml v0.0.0-20210825132133-2d6955c8edbc

replace github.com/knz/go-libedit => github.com/otan-cockroach/go-libedit v1.10.2-0.20201030151939-7cced08450e7

// At the time of writing (i.e. as of this version below) the `etcd` repo is in the process of properly introducing
// modules, and as part of that uses an unsatisfiable version for this dependency (v3.0.0-00010101000000-000000000000).
// We just force it to the same SHA as the `go.etcd.io/etcd/raft/v3` module (they live in the same VCS root).
//
// While this is necessary, make sure that the require block above does not diverge.
replace go.etcd.io/etcd/pkg/v3 => go.etcd.io/etcd/pkg/v3 v3.0.0-20201109164711-01844fd28560

replace github.com/docker/docker => github.com/moby/moby v20.10.6+incompatible

replace github.com/maruel/panicparse/v2 => github.com/cockroachdb/panicparse/v2 v2.0.0-20211103220158-604c82a44f1e
