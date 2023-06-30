module github.com/cockroachdb/cockroach

go 1.19

// The following dependencies are key infrastructure dependencies and
// should be updated as their own commit (i.e. not bundled with a dep
// upgrade to something else), and reviewed by a broader community of
// reviewers.
require (
	github.com/gogo/googleapis v1.4.1 // indirect
	github.com/gogo/protobuf v1.3.2
	github.com/golang/geo v0.0.0-20200319012246-673a6f80352d
	github.com/golang/mock v1.6.0
	github.com/golang/protobuf v1.5.2
	github.com/golang/snappy v0.0.4
	github.com/google/btree v1.0.1
	github.com/google/pprof v0.0.0-20210827144239-02619b876842
	github.com/google/uuid v1.3.0
	golang.org/x/crypto v0.6.0
	golang.org/x/exp v0.0.0-20220104160115-025e73f80486
	golang.org/x/exp/typeparams v0.0.0-20220713135740-79cabaa25d75 // indirect
	golang.org/x/mod v0.7.0 // indirect
	golang.org/x/net v0.7.0
	golang.org/x/oauth2 v0.0.0-20221014153046-6fdb5e3db783
	golang.org/x/sync v0.1.0
	golang.org/x/sys v0.5.0
	golang.org/x/text v0.7.0
	golang.org/x/time v0.1.0
	golang.org/x/tools v0.1.12
	google.golang.org/api v0.103.0
	google.golang.org/genproto v0.0.0-20221202195650-67e5cbc046fd
	google.golang.org/grpc v1.51.0
	google.golang.org/protobuf v1.28.1
)

// If any of the following dependencies get updated as a side-effect
// of another change, be sure to request extra scrutiny from
// the disaster recovery team.
require (
	cloud.google.com/go/kms v1.6.0
	cloud.google.com/go/pubsub v1.28.0
	cloud.google.com/go/storage v1.27.0
	github.com/Azure/azure-sdk-for-go v57.1.0+incompatible
	github.com/Azure/go-autorest/autorest v0.11.20
	github.com/Azure/go-autorest/autorest/azure/auth v0.5.8
	github.com/Azure/go-autorest/autorest/azure/cli v0.4.3 // indirect
	github.com/Azure/go-autorest/autorest/to v0.4.0
	github.com/aws/aws-sdk-go v1.40.37
	github.com/aws/aws-sdk-go-v2 v1.17.3
	github.com/aws/aws-sdk-go-v2/config v1.15.3
	github.com/aws/aws-sdk-go-v2/credentials v1.11.2 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.12.3 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.1.27 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.4.21 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.3.10 // indirect
	github.com/aws/aws-sdk-go-v2/service/databasemigrationservice v1.18.3
	github.com/aws/aws-sdk-go-v2/service/ec2 v1.34.0
	github.com/aws/aws-sdk-go-v2/service/iam v1.18.3
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.9.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/rds v1.18.4
	github.com/aws/aws-sdk-go-v2/service/sso v1.11.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.16.3 // indirect
)

// If any of the following dependencies get update as a side-effect
// of another change, be sure to request extra scrutiny from
// the SQL team.
require (
	github.com/jackc/chunkreader/v2 v2.0.1 // indirect
	github.com/jackc/pgconn v1.13.1-0.20221001150415-49cbf4659151
	github.com/jackc/pgio v1.0.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgproto3/v2 v2.3.1
	github.com/jackc/pgservicefile v0.0.0-20221227161230-091c0ba34f0a // indirect
	github.com/jackc/pgtype v1.11.0
	github.com/jackc/pgx/v4 v4.16.1
	github.com/jackc/puddle v1.2.1 // indirect
)

require (
	github.com/Azure/azure-sdk-for-go/sdk/azcore v1.3.0
	github.com/Azure/azure-sdk-for-go/sdk/azidentity v1.1.0
	github.com/Azure/azure-sdk-for-go/sdk/keyvault/azkeys v0.9.0
	github.com/Azure/azure-sdk-for-go/sdk/storage/azblob v0.6.1
	github.com/BurntSushi/toml v0.4.1
	github.com/Masterminds/semver/v3 v3.1.1
	github.com/MichaelTJones/walk v0.0.0-20161122175330-4748e29d5718
	github.com/NYTimes/gziphandler v0.0.0-20170623195520-56545f4a5d46
	github.com/PuerkitoBio/goquery v1.5.1
	github.com/Shopify/sarama v1.38.1
	github.com/VividCortex/ewma v1.1.1
	github.com/abourget/teamcity v0.0.0-00010101000000-000000000000
	github.com/alessio/shellescape v1.4.1
	github.com/andy-kimball/arenaskl v0.0.0-20200617143215-f701008588b9
	github.com/andygrunwald/go-jira v1.14.0
	github.com/apache/arrow/go/arrow v0.0.0-20200923215132-ac86123a3f01
	github.com/armon/circbuf v0.0.0-20150827004946-bbbad097214e
	github.com/aws/aws-sdk-go-v2/service/secretsmanager v1.18.2
	github.com/axiomhq/hyperloglog v0.0.0-20181223111420-4b99d0c2c99e
	github.com/bazelbuild/rules_go v0.26.0
	github.com/biogo/store v0.0.0-20160505134755-913427a1d5e8
	github.com/blevesearch/snowballstem v0.9.0
	github.com/buchgr/bazel-remote v1.3.3
	github.com/bufbuild/buf v0.56.0
	github.com/cenkalti/backoff v2.2.1+incompatible
	github.com/charmbracelet/bubbles v0.15.1-0.20230123181021-a6a12c4a31eb
	github.com/client9/misspell v0.3.4
	github.com/cockroachdb/apd/v3 v3.2.0
	github.com/cockroachdb/circuitbreaker v2.2.2-0.20190114160014-a614b14ccf63+incompatible
	github.com/cockroachdb/cmux v0.0.0-20170110192607-30d10be49292
	github.com/cockroachdb/cockroach-go/v2 v2.3.3
	github.com/cockroachdb/crlfmt v0.0.0-20221214225007-b2fc5c302548
	github.com/cockroachdb/datadriven v1.0.2
	github.com/cockroachdb/errors v1.9.1
	github.com/cockroachdb/go-test-teamcity v0.0.0-20191211140407-cff980ad0a55
	github.com/cockroachdb/gostdlib v1.19.0
	github.com/cockroachdb/logtags v0.0.0-20230118201751-21c54148d20b
	github.com/cockroachdb/pebble v0.0.0-20230630144507-204919aa5169
	github.com/cockroachdb/redact v1.1.4
	github.com/cockroachdb/returncheck v0.0.0-20200612231554-92cdbca611dd
	github.com/cockroachdb/stress v0.0.0-20220803192808-1806698b1b7b
	github.com/cockroachdb/tools v0.0.0-20211112185054-642e51449b40
	github.com/cockroachdb/ttycolor v0.0.0-20210902133924-c7d7dcdde4e8
	github.com/codahale/hdrhistogram v0.0.0-20161010025455-3a0bb77429bd
	github.com/containerd/containerd v1.5.4
	github.com/coreos/go-oidc v2.2.1+incompatible
	github.com/dave/dst v0.24.0
	github.com/davecgh/go-spew v1.1.1
	github.com/docker/distribution v2.7.1+incompatible
	github.com/docker/docker v20.10.17+incompatible
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
	github.com/go-openapi/strfmt v0.20.2
	github.com/go-sql-driver/mysql v1.6.0
	github.com/go-swagger/go-swagger v0.26.1
	github.com/gogo/status v1.1.0
	github.com/golang-commonmark/markdown v0.0.0-20180910011815-a8f139058164
	github.com/google/flatbuffers v2.0.0+incompatible
	github.com/google/go-cmp v0.5.9
	github.com/google/go-github v17.0.0+incompatible
	github.com/google/go-github/v42 v42.0.0
	github.com/google/shlex v0.0.0-20191202100458-e7afc7fbc510
	github.com/google/skylark v0.0.0-20181101142754-a5f7082aabed
	github.com/googleapis/gax-go/v2 v2.7.0
	github.com/gorilla/mux v1.8.0
	github.com/goware/modvendor v0.5.0
	github.com/grpc-ecosystem/grpc-gateway v1.16.0
	github.com/guptarohit/asciigraph v0.5.5
	github.com/irfansharif/recorder v0.0.0-20211218081646-a21b46510fd6
	github.com/jackc/pgx/v5 v5.3.1
	github.com/jaegertracing/jaeger v1.18.1
	github.com/jordan-wright/email v4.0.1-0.20210109023952-943e75fe5223+incompatible
	github.com/jordanlewis/gcassert v0.0.0-20221027203946-81f097ad35a0
	github.com/kevinburke/go-bindata v3.13.0+incompatible
	github.com/kisielk/errcheck v1.6.1-0.20210625163953-8ddee489636a
	github.com/kisielk/gotool v1.0.0
	github.com/klauspost/compress v1.15.15
	github.com/klauspost/pgzip v1.2.5
	github.com/knz/bubbline v0.0.0-20230422210153-e176cdfe1c43
	github.com/knz/go-libedit v1.10.2-0.20230308124748-6f1b59dd42bc
	github.com/knz/strtime v0.0.0-20200318182718-be999391ffa9
	github.com/kr/pretty v0.3.0
	github.com/kr/text v0.2.0
	github.com/kylelemons/godebug v1.1.0
	github.com/leanovate/gopter v0.2.5-0.20190402064358-634a59d12406
	github.com/lestrrat-go/jwx v1.2.25
	github.com/lib/pq v1.10.7
	github.com/linkedin/goavro/v2 v2.12.0
	github.com/lufia/iostat v1.2.1
	github.com/maruel/panicparse/v2 v2.2.2
	github.com/marusama/semaphore v0.0.0-20190110074507-6952cef993b2
	github.com/mattn/go-isatty v0.0.16
	github.com/mattn/goveralls v0.0.2
	github.com/mibk/dupl v1.0.0
	github.com/mitchellh/reflectwalk v1.0.0
	github.com/mkungla/bexp/v3 v3.0.1
	github.com/mmatczuk/go_generics v0.0.0-20181212143635-0aaa050f9bab
	github.com/montanaflynn/stats v0.6.6
	github.com/mozillazg/go-slugify v0.2.0
	github.com/nightlyone/lockfile v1.0.0
	github.com/olekukonko/tablewriter v0.0.5-0.20200416053754-163badb3bac6
	github.com/opencontainers/image-spec v1.0.2
	github.com/otan/gopgkrb5 v1.0.3
	github.com/petermattis/goid v0.0.0-20211229010228-4d14c490ee36
	github.com/pierrec/lz4/v4 v4.1.17
	github.com/pierrre/geohash v1.0.0
	github.com/pires/go-proxyproto v0.7.0
	github.com/pkg/browser v0.0.0-20210115035449-ce105d075bb4
	github.com/pmezard/go-difflib v1.0.0
	github.com/pressly/goose/v3 v3.5.3
	github.com/prometheus/client_golang v1.12.1
	github.com/prometheus/client_model v0.2.1-0.20210607210712-147c58e9608a
	github.com/prometheus/common v0.32.1
	github.com/prometheus/prometheus v1.8.2-0.20210914090109-37468d88dce8
	github.com/pseudomuto/protoc-gen-doc v1.3.2
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475
	github.com/robfig/cron/v3 v3.0.1
	github.com/sasha-s/go-deadlock v0.3.1
	github.com/shirou/gopsutil/v3 v3.21.12
	github.com/slack-go/slack v0.9.5
	github.com/spf13/afero v1.6.0
	github.com/spf13/cobra v1.2.1
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.8.2
	github.com/twpayne/go-geom v1.4.2
	github.com/wadey/gocovmerge v0.0.0-20160331181800-b5bfa59ec0ad
	github.com/xdg-go/pbkdf2 v1.0.0
	github.com/xdg-go/scram v1.1.2
	github.com/xdg-go/stringprep v1.0.4
	github.com/zabawaba99/go-gitignore v0.0.0-20200117185801-39e6bddfb292
	go.etcd.io/raft/v3 v3.0.0-20230315220435-5fe1c31c5158
	go.opentelemetry.io/otel v1.0.0-RC3
	go.opentelemetry.io/otel/exporters/jaeger v1.0.0-RC3
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.0.0-RC3
	go.opentelemetry.io/otel/exporters/zipkin v1.0.0-RC3
	go.opentelemetry.io/otel/sdk v1.0.0-RC3
	go.opentelemetry.io/otel/trace v1.0.0-RC3
	golang.org/x/perf v0.0.0-20230113213139-801c7ef9e5c5
	golang.org/x/term v0.5.0
	gopkg.in/yaml.v2 v2.4.0
	gopkg.in/yaml.v3 v3.0.1
	honnef.co/go/tools v0.3.2
	vitess.io/vitess v0.0.0-00010101000000-000000000000
)

require (
	cloud.google.com/go v0.107.0 // indirect
	cloud.google.com/go/compute v1.13.0 // indirect
	cloud.google.com/go/compute/metadata v0.2.1 // indirect
	cloud.google.com/go/iam v0.8.0 // indirect
	cloud.google.com/go/longrunning v0.3.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/internal v1.1.1 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/keyvault/internal v0.7.0 // indirect
	github.com/Azure/go-ansiterm v0.0.0-20210617225240-d185dfc1b5a1 // indirect
	github.com/Azure/go-autorest v14.2.0+incompatible // indirect
	github.com/Azure/go-autorest/autorest/adal v0.9.15 // indirect
	github.com/Azure/go-autorest/autorest/date v0.3.0 // indirect
	github.com/Azure/go-autorest/autorest/validation v0.3.1 // indirect
	github.com/Azure/go-autorest/logger v0.2.1 // indirect
	github.com/Azure/go-autorest/tracing v0.6.0 // indirect
	github.com/AzureAD/microsoft-authentication-library-for-go v0.5.1 // indirect
	github.com/Masterminds/goutils v1.1.0 // indirect
	github.com/Masterminds/semver v1.5.0 // indirect
	github.com/Masterminds/sprig v2.22.0+incompatible // indirect
	github.com/Microsoft/go-winio v0.5.2 // indirect
	github.com/PuerkitoBio/purell v1.1.1 // indirect
	github.com/PuerkitoBio/urlesc v0.0.0-20170810143723-de5bf2ad4578 // indirect
	github.com/abbot/go-http-auth v0.4.1-0.20181019201920-860ed7f246ff // indirect
	github.com/aclements/go-moremath v0.0.0-20210112150236-f10218a38794 // indirect
	github.com/alexbrainman/sspi v0.0.0-20210105120005-909beea2cc74 // indirect
	github.com/apache/thrift v0.16.0 // indirect
	github.com/asaskevich/govalidator v0.0.0-20200907205600-7a23bdc65eef // indirect
	github.com/atotto/clipboard v0.1.4 // indirect
	github.com/aws/smithy-go v1.13.5 // indirect
	github.com/aymanbagabas/go-osc52 v1.0.3 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bgentry/go-netrc v0.0.0-20140422174119-9fd32a8b3d3d // indirect
	github.com/cenkalti/backoff/v4 v4.1.3 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/charmbracelet/bubbletea v0.23.1 // indirect
	github.com/charmbracelet/lipgloss v0.6.0 // indirect
	github.com/decred/dcrd/dcrec/secp256k1/v4 v4.1.0 // indirect
	github.com/dimchansky/utfbom v1.1.1 // indirect
	github.com/djherbis/atime v1.1.0 // indirect
	github.com/docker/go-units v0.4.0 // indirect
	github.com/eapache/go-resiliency v1.3.0 // indirect
	github.com/eapache/go-xerial-snappy v0.0.0-20230111030713-bf00bc1b83b6 // indirect
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
	github.com/go-openapi/swag v0.19.15 // indirect
	github.com/go-openapi/validate v0.20.2 // indirect
	github.com/go-stack/stack v1.8.0 // indirect
	github.com/goccy/go-json v0.9.11 // indirect
	github.com/gofrs/flock v0.8.1 // indirect
	github.com/gofrs/uuid v4.0.0+incompatible // indirect
	github.com/golang-jwt/jwt v3.2.2+incompatible // indirect
	github.com/golang-jwt/jwt/v4 v4.2.0 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/google/go-querystring v1.1.0 // indirect
	github.com/google/safehtml v0.0.2 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.2.0 // indirect
	github.com/gorilla/handlers v1.5.1 // indirect
	github.com/gorilla/websocket v1.4.2 // indirect
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/go-uuid v1.0.3 // indirect
	github.com/hashicorp/hcl v1.0.0 // indirect
	github.com/huandu/xstrings v1.3.0 // indirect
	github.com/ianlancetaylor/demangle v0.0.0-20200824232613-28f6c0f3b639 // indirect
	github.com/imdario/mergo v0.3.13 // indirect
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/jackc/puddle/v2 v2.2.0 // indirect
	github.com/jcmturner/aescts/v2 v2.0.0 // indirect
	github.com/jcmturner/dnsutils/v2 v2.0.0 // indirect
	github.com/jcmturner/gofork v1.7.6 // indirect
	github.com/jcmturner/goidentity/v6 v6.0.1 // indirect
	github.com/jcmturner/gokrb5/v8 v8.4.3 // indirect
	github.com/jcmturner/rpc/v2 v2.0.3 // indirect
	github.com/jessevdk/go-flags v1.5.0 // indirect
	github.com/jhump/protoreflect v1.9.1-0.20210817181203-db1a327a393e // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/cpuid/v2 v2.0.9 // indirect
	github.com/lestrrat-go/backoff/v2 v2.0.8 // indirect
	github.com/lestrrat-go/blackmagic v1.0.1 // indirect
	github.com/lestrrat-go/httpcc v1.0.1 // indirect
	github.com/lestrrat-go/iter v1.0.2 // indirect
	github.com/lestrrat-go/option v1.0.0 // indirect
	github.com/lufia/plan9stats v0.0.0-20211012122336-39d0f177ccd0 // indirect
	github.com/magiconair/properties v1.8.5 // indirect
	github.com/mailru/easyjson v0.7.6 // indirect
	github.com/mattn/go-localereader v0.0.1 // indirect
	github.com/mattn/go-runewidth v0.0.14 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.2-0.20181231171920-c182affec369 // indirect
	github.com/minio/md5-simd v1.1.2 // indirect
	github.com/minio/minio-go/v7 v7.0.21 // indirect
	github.com/minio/sha256-simd v1.0.0 // indirect
	github.com/mitchellh/copystructure v1.0.0 // indirect
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/moby/term v0.0.0-20210619224110-3f7ff695adc6 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/morikuni/aec v1.0.0 // indirect
	github.com/mostynb/go-grpc-compression v1.1.12 // indirect
	github.com/mozillazg/go-unidecode v0.2.0 // indirect
	github.com/muesli/termenv v0.13.0 // indirect
	github.com/mwitkow/go-proto-validators v0.0.0-20180403085117-0950a7990007 // indirect
	github.com/oklog/ulid v1.3.1 // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/openzipkin/zipkin-go v0.2.5 // indirect
	github.com/pelletier/go-toml v1.9.3 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pkg/profile v1.6.0 // indirect
	github.com/power-devops/perfstat v0.0.0-20210106213030-5aafc221ea8c // indirect
	github.com/pquerna/cachecontrol v0.0.0-20200921180117-858c6e7e6b7e // indirect
	github.com/prometheus/procfs v0.7.3 // indirect
	github.com/pseudomuto/protokit v0.2.0 // indirect
	github.com/rivo/uniseg v0.2.0 // indirect
	github.com/rogpeppe/go-internal v1.9.0 // indirect
	github.com/rs/xid v1.3.0 // indirect
	github.com/russross/blackfriday v1.6.0 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/sahilm/fuzzy v0.1.0 // indirect
	github.com/sirupsen/logrus v1.9.0 // indirect
	github.com/slok/go-http-metrics v0.10.0 // indirect
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
	github.com/yusufpapurcu/wmi v1.2.2 // indirect
	go.mongodb.org/mongo-driver v1.5.1 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.opentelemetry.io/proto/otlp v0.9.0 // indirect
	go.uber.org/atomic v1.10.0 // indirect
	go.uber.org/multierr v1.7.0 // indirect
	go.uber.org/zap v1.19.0 // indirect
	golang.org/x/xerrors v0.0.0-20220907171357-04be3eba64a2 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	gopkg.in/square/go-jose.v2 v2.5.1 // indirect
)

require (
	github.com/DataDog/zstd v1.5.0 // indirect
	github.com/andybalholm/cascadia v1.2.0 // indirect
	github.com/containerd/console v1.0.3 // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.1 // indirect
	github.com/dgryski/go-metro v0.0.0-20180109044635-280f6062b5bc // indirect
	github.com/envoyproxy/protoc-gen-validate v0.6.2 // indirect
	github.com/golang-commonmark/html v0.0.0-20180910111043-7d7c804e1d46 // indirect
	github.com/golang-commonmark/linkify v0.0.0-20180910111149-f05efb453a0e // indirect
	github.com/golang-commonmark/mdurl v0.0.0-20180910110917-8d018c6567d6 // indirect
	github.com/golang-commonmark/puny v0.0.0-20180910110745-050be392d8b8 // indirect
	github.com/golang/glog v0.0.0-20210429001901-424d2337a529 // indirect
	github.com/lucasb-eyer/go-colorful v1.2.0 // indirect
	github.com/mattn/go-zglob v0.0.3 // indirect
	github.com/muesli/ansi v0.0.0-20211031195517-c9f0611b6c70 // indirect
	github.com/muesli/cancelreader v0.2.2 // indirect
	github.com/muesli/reflow v0.3.0 // indirect
	github.com/opennota/wd v0.0.0-20180911144301-b446539ab1e7 // indirect
	github.com/peterbourgon/g2s v0.0.0-20170223122336-d4e7ad98afea // indirect
	// The indicated commit is required on top of v1.0.0-RC3 because
	// it fixes an import comment that otherwise breaks our prereqs tool.
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.0.0-RC3.0.20210907151655-df2bdbbadb26 // indirect
	google.golang.org/grpc/examples v0.0.0-20210324172016-702608ffae4d // indirect
	gopkg.in/ini.v1 v1.66.3 // indirect
)

// Until this PR is merged: https://github.com/charmbracelet/bubbletea/pull/397
replace github.com/charmbracelet/bubbletea => github.com/cockroachdb/bubbletea v0.23.1-bracketed-paste2

replace github.com/olekukonko/tablewriter => github.com/cockroachdb/tablewriter v0.0.5-0.20200105123400-bd15540e8847

replace github.com/abourget/teamcity => github.com/cockroachdb/teamcity v0.0.0-20180905144921-8ca25c33eb11

replace vitess.io/vitess => github.com/cockroachdb/vitess v0.0.0-20210218160543-54524729cc82

replace gopkg.in/yaml.v2 => github.com/cockroachdb/yaml v0.0.0-20210825132133-2d6955c8edbc

replace github.com/docker/docker => github.com/moby/moby v20.10.6+incompatible

// Take etcd-io/raft from the branch corresponding to release-23.1 in our fork.
replace go.etcd.io/raft/v3 => github.com/cockroachdb/raft/v3 v3.0.0-20230615130413-b160e656b5ae

replace golang.org/x/time => github.com/cockroachdb/x-time v0.3.1-0.20230525123634-71747adb5d5c
