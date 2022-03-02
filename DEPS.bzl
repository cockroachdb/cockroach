load("@bazel_gazelle//:deps.bzl", "go_repository")

# PRO-TIP: You can inject temorary changes to any of these dependencies by
# by pointing to an alternate remote to clone from. Delete the `sha256`,
# `strip_prefix`, and ` + `urls` parameters, and add `vcs = "git"` as well as a
# custom `remote` and `commit`. For example:
#     go_repository(
#        name = "com_github_cockroachdb_sentry_go",
#        build_file_proto_mode = "disable_global",
#        importpath = "github.com/cockroachdb/sentry-go",
#        vcs = "git",
#        remote = "https://github.com/rickystewart/sentry-go",  # Custom fork.
#        commit = "6c8e10aca9672de108063d4953399bd331b54037",  # Custom commit.
#    )
# The `remote` can be EITHER a URL, or an absolute local path to a clone, such
# as `/Users/ricky/go/src/github.com/cockroachdb/sentry-go`. Bazel will clone
# from the remote and check out the commit you specify.

def go_deps():
    # NOTE: We ensure that we pin to these specific dependencies by calling
    # this function FIRST, before calls to pull in dependencies for
    # third-party libraries (e.g. rules_go, gazelle, etc.)
    go_repository(
        name = "co_honnef_go_tools",
        build_file_proto_mode = "disable_global",
        importpath = "honnef.co/go/tools",
        sha256 = "b327a6e9565db3b835f2b224b9023f5dd9fb236e94285f002f0ac740ad2ac43b",
        strip_prefix = "honnef.co/go/tools@v0.2.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/honnef.co/go/tools/co_honnef_go_tools-v0.2.1.zip",
        ],
    )
    go_repository(
        name = "com_github_abbot_go_http_auth",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/abbot/go-http-auth",
        sha256 = "c0e46a64d55a47d790205df3b6d52f3ef5e354da5ce5088f376c977000610198",
        strip_prefix = "github.com/abbot/go-http-auth@v0.4.1-0.20181019201920-860ed7f246ff",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/abbot/go-http-auth/com_github_abbot_go_http_auth-v0.4.1-0.20181019201920-860ed7f246ff.zip",
        ],
    )
    go_repository(
        name = "com_github_abourget_teamcity",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/abourget/teamcity",
        sha256 = "9df6b028c9fb5bff7bdad844bda504356945fd6d3cd583c50f68d8b8e85060f6",
        strip_prefix = "github.com/cockroachdb/teamcity@v0.0.0-20180905144921-8ca25c33eb11",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/cockroachdb/teamcity/com_github_cockroachdb_teamcity-v0.0.0-20180905144921-8ca25c33eb11.zip",
        ],
    )
    go_repository(
        name = "com_github_afex_hystrix_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/afex/hystrix-go",
        sha256 = "c0e0ea63b57e95784eeeb18ab8988ac2c3d3a17dc729d557c963f391f372301c",
        strip_prefix = "github.com/afex/hystrix-go@v0.0.0-20180502004556-fa1af6a1f4f5",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/afex/hystrix-go/com_github_afex_hystrix_go-v0.0.0-20180502004556-fa1af6a1f4f5.zip",
        ],
    )
    go_repository(
        name = "com_github_agnivade_levenshtein",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/agnivade/levenshtein",
        sha256 = "cb0e7f070ba2b6a10e1c600d71f06508404801ff45046853001b83be6ebedac3",
        strip_prefix = "github.com/agnivade/levenshtein@v1.0.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/agnivade/levenshtein/com_github_agnivade_levenshtein-v1.0.1.zip",
        ],
    )
    go_repository(
        name = "com_github_ajg_form",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ajg/form",
        sha256 = "b063b07639670ce9b6a0065b4dc35ef9e4cebc0c601be27f5494a3e6a87eb78b",
        strip_prefix = "github.com/ajg/form@v1.5.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/ajg/form/com_github_ajg_form-v1.5.1.zip",
        ],
    )
    go_repository(
        name = "com_github_ajstarks_svgo",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ajstarks/svgo",
        sha256 = "cdf6900823539ab02e7f6e0edcdb4c12b3dcec97068a350e564ff622132ae7fc",
        strip_prefix = "github.com/ajstarks/svgo@v0.0.0-20180226025133-644b8db467af",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/ajstarks/svgo/com_github_ajstarks_svgo-v0.0.0-20180226025133-644b8db467af.zip",
        ],
    )
    go_repository(
        name = "com_github_akavel_rsrc",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/akavel/rsrc",
        sha256 = "13954a09edc3a680d633c5ea7b4be902df3a70ca1720b349faadca44dc0c7ecc",
        strip_prefix = "github.com/akavel/rsrc@v0.8.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/akavel/rsrc/com_github_akavel_rsrc-v0.8.0.zip",
        ],
    )
    go_repository(
        name = "com_github_alecthomas_template",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/alecthomas/template",
        sha256 = "25e3be7192932d130d0af31ce5bcddae887647ba4afcfb32009c3b9b79dbbdb3",
        strip_prefix = "github.com/alecthomas/template@v0.0.0-20190718012654-fb15b899a751",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/alecthomas/template/com_github_alecthomas_template-v0.0.0-20190718012654-fb15b899a751.zip",
        ],
    )
    go_repository(
        name = "com_github_alecthomas_units",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/alecthomas/units",
        sha256 = "461b05eb19b6023664d6502cb4fbf093a99a9ee2a36c43b4da5ca1287c495ff7",
        strip_prefix = "github.com/alecthomas/units@v0.0.0-20210208195552-ff826a37aa15",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/alecthomas/units/com_github_alecthomas_units-v0.0.0-20210208195552-ff826a37aa15.zip",
        ],
    )
    go_repository(
        name = "com_github_alessio_shellescape",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/alessio/shellescape",
        sha256 = "e28d444e73b803a15cf83e6179149d34c6c132baa60cb8137e5f0aea50a543bf",
        strip_prefix = "github.com/alessio/shellescape@v1.4.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/alessio/shellescape/com_github_alessio_shellescape-v1.4.1.zip",
        ],
    )
    go_repository(
        name = "com_github_alexbrainman_sspi",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/alexbrainman/sspi",
        sha256 = "596bc141f8557e6eaad5950b835dfa61f8894bf6fdad00555725526fffb9eada",
        strip_prefix = "github.com/alexbrainman/sspi@v0.0.0-20180613141037-e580b900e9f5",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/alexbrainman/sspi/com_github_alexbrainman_sspi-v0.0.0-20180613141037-e580b900e9f5.zip",
        ],
    )
    go_repository(
        name = "com_github_alexflint_go_filemutex",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/alexflint/go-filemutex",
        sha256 = "f3517f75266ac4651b0b421dd970a68d5645c929062f2d67b9e1e4685562b690",
        strip_prefix = "github.com/alexflint/go-filemutex@v0.0.0-20171022225611-72bdc8eae2ae",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/alexflint/go-filemutex/com_github_alexflint_go_filemutex-v0.0.0-20171022225611-72bdc8eae2ae.zip",
        ],
    )
    go_repository(
        name = "com_github_andreasbriese_bbloom",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/AndreasBriese/bbloom",
        sha256 = "8b8c016e041592d4ca8cbd2a8c68e0dd0ba1b7a8f96fab7422c8e373b1835a2d",
        strip_prefix = "github.com/AndreasBriese/bbloom@v0.0.0-20190825152654-46b345b51c96",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/AndreasBriese/bbloom/com_github_andreasbriese_bbloom-v0.0.0-20190825152654-46b345b51c96.zip",
        ],
    )
    go_repository(
        name = "com_github_andreyvit_diff",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/andreyvit/diff",
        sha256 = "d39614ff930006640ec15865bca0bb6bf8e1ed145bccf30bab08b88c1d90f670",
        strip_prefix = "github.com/andreyvit/diff@v0.0.0-20170406064948-c7f18ee00883",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/andreyvit/diff/com_github_andreyvit_diff-v0.0.0-20170406064948-c7f18ee00883.zip",
        ],
    )
    go_repository(
        name = "com_github_andy_kimball_arenaskl",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/andy-kimball/arenaskl",
        sha256 = "a3d6ee002f3d47e1a0188c7ee908e2ee424b1124753fba88080000faac8480b0",
        strip_prefix = "github.com/andy-kimball/arenaskl@v0.0.0-20200617143215-f701008588b9",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/andy-kimball/arenaskl/com_github_andy_kimball_arenaskl-v0.0.0-20200617143215-f701008588b9.zip",
        ],
    )
    go_repository(
        name = "com_github_andybalholm_brotli",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/andybalholm/brotli",
        sha256 = "60d5aa0132b481cde660dbf77de8d139f2baf2ce1a6e25c980f31020efcf42b2",
        strip_prefix = "github.com/andybalholm/brotli@v1.0.2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/andybalholm/brotli/com_github_andybalholm_brotli-v1.0.2.zip",
        ],
    )
    go_repository(
        name = "com_github_andybalholm_cascadia",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/andybalholm/cascadia",
        sha256 = "8cc5679e5070e2076369e2f7a24341cf0ccb139f49cccf153f72902f24876d81",
        strip_prefix = "github.com/andybalholm/cascadia@v1.2.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/andybalholm/cascadia/com_github_andybalholm_cascadia-v1.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_andygrunwald_go_jira",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/andygrunwald/go-jira",
        sha256 = "89fc3ece1f9d367e211845ef4f33bed49273af58fdf65c561eb67903d3b72979",
        strip_prefix = "github.com/andygrunwald/go-jira@v1.14.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/andygrunwald/go-jira/com_github_andygrunwald_go_jira-v1.14.0.zip",
        ],
    )
    go_repository(
        name = "com_github_antihax_optional",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/antihax/optional",
        sha256 = "15ab4d41bdbb72ee0ac63db616cdefc7671c79e13d0f73b58355a6a88219c97f",
        strip_prefix = "github.com/antihax/optional@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/antihax/optional/com_github_antihax_optional-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_aokoli_goutils",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/aokoli/goutils",
        sha256 = "96ee68caaf3f4e673e27c97659b4ea10a4fd81dbf24fabd2dc01e187a772355c",
        strip_prefix = "github.com/aokoli/goutils@v1.0.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/aokoli/goutils/com_github_aokoli_goutils-v1.0.1.zip",
        ],
    )
    go_repository(
        name = "com_github_apache_arrow_go_arrow",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/apache/arrow/go/arrow",
        sha256 = "5018a8784061fd3a5e52069fb321ebe2d96181d4a6f2d594cb60ff3787998580",
        strip_prefix = "github.com/apache/arrow/go/arrow@v0.0.0-20200923215132-ac86123a3f01",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/apache/arrow/go/arrow/com_github_apache_arrow_go_arrow-v0.0.0-20200923215132-ac86123a3f01.zip",
        ],
    )
    go_repository(
        name = "com_github_apache_thrift",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/apache/thrift",
        sha256 = "50d5c610df30fa2a6039394d5142382b7d9938870dfb12ef46bddfa3da250893",
        strip_prefix = "github.com/apache/thrift@v0.16.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/apache/thrift/com_github_apache_thrift-v0.16.0.zip",
        ],
    )
    go_repository(
        name = "com_github_araddon_dateparse",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/araddon/dateparse",
        sha256 = "8fe74d5b36a9aa66c1350657009296469f8aa5b6925acd178044cd248023fc5a",
        strip_prefix = "github.com/araddon/dateparse@v0.0.0-20210429162001-6b43995a97de",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/araddon/dateparse/com_github_araddon_dateparse-v0.0.0-20210429162001-6b43995a97de.zip",
        ],
    )
    go_repository(
        name = "com_github_armon_circbuf",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/armon/circbuf",
        sha256 = "3819cde26cd4b25c4043dc9384da7b0c1c29fd06e6e3a38604f4a6933fc017ed",
        strip_prefix = "github.com/armon/circbuf@v0.0.0-20150827004946-bbbad097214e",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/armon/circbuf/com_github_armon_circbuf-v0.0.0-20150827004946-bbbad097214e.zip",
        ],
    )
    go_repository(
        name = "com_github_armon_consul_api",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/armon/consul-api",
        sha256 = "091b79667f16ae245785956c490fe05ee26970a89f8ecdbe858ae3510d725088",
        strip_prefix = "github.com/armon/consul-api@v0.0.0-20180202201655-eb2c6b5be1b6",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/armon/consul-api/com_github_armon_consul_api-v0.0.0-20180202201655-eb2c6b5be1b6.zip",
        ],
    )
    go_repository(
        name = "com_github_armon_go_metrics",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/armon/go-metrics",
        sha256 = "dba0cd2b5d068409eb4acbb1cf14544252068339fcf49e7dc7f3a778bb843d53",
        strip_prefix = "github.com/armon/go-metrics@v0.3.3",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/armon/go-metrics/com_github_armon_go_metrics-v0.3.3.zip",
        ],
    )
    go_repository(
        name = "com_github_armon_go_radix",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/armon/go-radix",
        sha256 = "df93c816505baf12c3efe61328dc6f8fa42438f68f80b0b3725cae957d021c90",
        strip_prefix = "github.com/armon/go-radix@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/armon/go-radix/com_github_armon_go_radix-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_armon_go_socks5",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/armon/go-socks5",
        sha256 = "f473e6dce826a0552639833cf72cfaa8bc7141daa7b537622d7f78eacfd9dfb3",
        strip_prefix = "github.com/armon/go-socks5@v0.0.0-20160902184237-e75332964ef5",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/armon/go-socks5/com_github_armon_go_socks5-v0.0.0-20160902184237-e75332964ef5.zip",
        ],
    )
    go_repository(
        name = "com_github_aryann_difflib",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/aryann/difflib",
        sha256 = "973aae50e3d85569e1f0f6cbca78bf9b5896ce53d0534422a7db46f947b50764",
        strip_prefix = "github.com/aryann/difflib@v0.0.0-20170710044230-e206f873d14a",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/aryann/difflib/com_github_aryann_difflib-v0.0.0-20170710044230-e206f873d14a.zip",
        ],
    )
    go_repository(
        name = "com_github_asaskevich_govalidator",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/asaskevich/govalidator",
        sha256 = "9b846e311015bbb7854cf001d5717d6fd91a580ac6315977ae7395256e18d729",
        strip_prefix = "github.com/asaskevich/govalidator@v0.0.0-20200907205600-7a23bdc65eef",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/asaskevich/govalidator/com_github_asaskevich_govalidator-v0.0.0-20200907205600-7a23bdc65eef.zip",
        ],
    )
    go_repository(
        name = "com_github_aws_aws_lambda_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/aws/aws-lambda-go",
        sha256 = "8cfc5400798abd2840f456c75265f8fba4ae488e32ca2af9a5c8073fb219ea82",
        strip_prefix = "github.com/aws/aws-lambda-go@v1.13.3",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/aws/aws-lambda-go/com_github_aws_aws_lambda_go-v1.13.3.zip",
        ],
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/aws/aws-sdk-go",
        sha256 = "c0c481d28af88f621fb3fdeacc1e5d32f69a1bb83d0ee959f95ce89e4e2d0494",
        strip_prefix = "github.com/aws/aws-sdk-go@v1.40.37",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/aws/aws-sdk-go/com_github_aws_aws_sdk_go-v1.40.37.zip",
        ],
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/aws/aws-sdk-go-v2",
        sha256 = "546b4f58c58aedf988945dc4709cf3af67a639ed94e0cf4df58389e9f9b345d8",
        strip_prefix = "github.com/aws/aws-sdk-go-v2@v1.9.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/aws/aws-sdk-go-v2/com_github_aws_aws_sdk_go_v2-v1.9.1.zip",
        ],
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_config",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/aws/aws-sdk-go-v2/config",
        sha256 = "7a9129c2cf9cdd5f2e74cae969c23a4212f040f08a73b03880c2d34faad0f887",
        strip_prefix = "github.com/aws/aws-sdk-go-v2/config@v1.8.2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/aws/aws-sdk-go-v2/config/com_github_aws_aws_sdk_go_v2_config-v1.8.2.zip",
        ],
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_credentials",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/aws/aws-sdk-go-v2/credentials",
        sha256 = "b8d47052b7527e524589fcaf2e25e7cd8bbb4ebc176262704555d8fa9270a695",
        strip_prefix = "github.com/aws/aws-sdk-go-v2/credentials@v1.4.2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/aws/aws-sdk-go-v2/credentials/com_github_aws_aws_sdk_go_v2_credentials-v1.4.2.zip",
        ],
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_feature_ec2_imds",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/aws/aws-sdk-go-v2/feature/ec2/imds",
        sha256 = "74cd015671e1446d67633aca7eab28f28a77d0cb103d96f167eef40beeeda3e1",
        strip_prefix = "github.com/aws/aws-sdk-go-v2/feature/ec2/imds@v1.5.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/aws/aws-sdk-go-v2/feature/ec2/imds/com_github_aws_aws_sdk_go_v2_feature_ec2_imds-v1.5.1.zip",
        ],
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_internal_ini",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/aws/aws-sdk-go-v2/internal/ini",
        sha256 = "e5ae7a3f32399258561cab83199c1ec9d30d32e771ce7a877c64c208a6cea27f",
        strip_prefix = "github.com/aws/aws-sdk-go-v2/internal/ini@v1.2.3",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/aws/aws-sdk-go-v2/internal/ini/com_github_aws_aws_sdk_go_v2_internal_ini-v1.2.3.zip",
        ],
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_service_ec2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/aws/aws-sdk-go-v2/service/ec2",
        sha256 = "384c3420c234109162eb46a07ea406323a0d4368828d1263012aa843816bd198",
        strip_prefix = "github.com/aws/aws-sdk-go-v2/service/ec2@v1.18.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/aws/aws-sdk-go-v2/service/ec2/com_github_aws_aws_sdk_go_v2_service_ec2-v1.18.0.zip",
        ],
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_service_iam",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/aws/aws-sdk-go-v2/service/iam",
        sha256 = "4181fee25ea8b9b9375fea74bc2e6abc2e7116e2db528c12986c14d2ecc675b0",
        strip_prefix = "github.com/aws/aws-sdk-go-v2/service/iam@v1.10.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/aws/aws-sdk-go-v2/service/iam/com_github_aws_aws_sdk_go_v2_service_iam-v1.10.0.zip",
        ],
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_service_internal_presigned_url",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/aws/aws-sdk-go-v2/service/internal/presigned-url",
        sha256 = "1705ae1c40d49cbed56d416850c84eeca9338dfbb014bc7b8021658153e1f6f6",
        strip_prefix = "github.com/aws/aws-sdk-go-v2/service/internal/presigned-url@v1.3.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/aws/aws-sdk-go-v2/service/internal/presigned-url/com_github_aws_aws_sdk_go_v2_service_internal_presigned_url-v1.3.1.zip",
        ],
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_service_sso",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/aws/aws-sdk-go-v2/service/sso",
        sha256 = "0f9fbe0b009919a72907e198a72ad72fd406ad9272395b4f2bf50ae9d6478b2d",
        strip_prefix = "github.com/aws/aws-sdk-go-v2/service/sso@v1.4.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/aws/aws-sdk-go-v2/service/sso/com_github_aws_aws_sdk_go_v2_service_sso-v1.4.1.zip",
        ],
    )
    go_repository(
        name = "com_github_aws_aws_sdk_go_v2_service_sts",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/aws/aws-sdk-go-v2/service/sts",
        sha256 = "046ae61b43524dd22bf8ce8d2bbf51e337cf91889db47b83121b88658c58a0fd",
        strip_prefix = "github.com/aws/aws-sdk-go-v2/service/sts@v1.7.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/aws/aws-sdk-go-v2/service/sts/com_github_aws_aws_sdk_go_v2_service_sts-v1.7.1.zip",
        ],
    )
    go_repository(
        name = "com_github_aws_smithy_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/aws/smithy-go",
        sha256 = "32b0e7b2c239ee27904c46ac4910725ad45737c118601c161ab7d2a9948197b5",
        strip_prefix = "github.com/aws/smithy-go@v1.8.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/aws/smithy-go/com_github_aws_smithy_go-v1.8.0.zip",
        ],
    )
    go_repository(
        name = "com_github_axiomhq_hyperloglog",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/axiomhq/hyperloglog",
        sha256 = "812834322ee2ca50dc36f91f9ac3f2cde4631af2f9c330b1271c78b46024a540",
        strip_prefix = "github.com/axiomhq/hyperloglog@v0.0.0-20181223111420-4b99d0c2c99e",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/axiomhq/hyperloglog/com_github_axiomhq_hyperloglog-v0.0.0-20181223111420-4b99d0c2c99e.zip",
        ],
    )
    go_repository(
        name = "com_github_aymerick_raymond",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/aymerick/raymond",
        sha256 = "0a759716a73b587a436b3b4a95416a58bb1ffa1decf2cd7a92f1eeb2f9c654c1",
        strip_prefix = "github.com/aymerick/raymond@v2.0.3-0.20180322193309-b565731e1464+incompatible",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/aymerick/raymond/com_github_aymerick_raymond-v2.0.3-0.20180322193309-b565731e1464+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_azure_azure_pipeline_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Azure/azure-pipeline-go",
        sha256 = "5881763a04c7705d2f348bfe18ec56266499265487d99fe448ded9874f4f3672",
        strip_prefix = "github.com/Azure/azure-pipeline-go@v0.2.3",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/Azure/azure-pipeline-go/com_github_azure_azure_pipeline_go-v0.2.3.zip",
        ],
    )
    go_repository(
        name = "com_github_azure_azure_sdk_for_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Azure/azure-sdk-for-go",
        sha256 = "7232ccaf96ab411dce27e8c61f4ec0e20835f60e192ffc7b56b7c3a308e29978",
        strip_prefix = "github.com/Azure/azure-sdk-for-go@v57.1.0+incompatible",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/Azure/azure-sdk-for-go/com_github_azure_azure_sdk_for_go-v57.1.0+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_azure_azure_storage_blob_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Azure/azure-storage-blob-go",
        sha256 = "b327f049492efbfbf81173148cd96b9009dcbc1a5bfce48c2d186c77bd296397",
        strip_prefix = "github.com/Azure/azure-storage-blob-go@v0.14.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/Azure/azure-storage-blob-go/com_github_azure_azure_storage_blob_go-v0.14.0.zip",
        ],
    )
    go_repository(
        name = "com_github_azure_go_ansiterm",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Azure/go-ansiterm",
        sha256 = "b7a14d18891abef8b8a2e622f62a3cebeac32f9f1223dc9d62a6f8769861aaf2",
        strip_prefix = "github.com/Azure/go-ansiterm@v0.0.0-20170929234023-d6e3b3328b78",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/Azure/go-ansiterm/com_github_azure_go_ansiterm-v0.0.0-20170929234023-d6e3b3328b78.zip",
        ],
    )
    go_repository(
        name = "com_github_azure_go_autorest",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Azure/go-autorest",
        sha256 = "89ac786da5b108e594bb1fbbf8f39a822fc1d994be1ff7cc6e860e8b45f3d80c",
        strip_prefix = "github.com/Azure/go-autorest@v14.2.0+incompatible",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/Azure/go-autorest/com_github_azure_go_autorest-v14.2.0+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_azure_go_autorest_autorest",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Azure/go-autorest/autorest",
        sha256 = "b5a184bbbec884260a5f4edf39bfd8fe5dc11c70199bcb4a69cb8f3e86b65d87",
        strip_prefix = "github.com/Azure/go-autorest/autorest@v0.11.20",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/Azure/go-autorest/autorest/com_github_azure_go_autorest_autorest-v0.11.20.zip",
        ],
    )
    go_repository(
        name = "com_github_azure_go_autorest_autorest_adal",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Azure/go-autorest/autorest/adal",
        sha256 = "791f1d559e2c4d99f4d29465fd71f5589e368e2087701b78970ad8dcc7be6299",
        strip_prefix = "github.com/Azure/go-autorest/autorest/adal@v0.9.15",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/Azure/go-autorest/autorest/adal/com_github_azure_go_autorest_autorest_adal-v0.9.15.zip",
        ],
    )
    go_repository(
        name = "com_github_azure_go_autorest_autorest_azure_auth",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Azure/go-autorest/autorest/azure/auth",
        sha256 = "7b50ba475d5a8dfcdd37fb5b53ece9e6d6150257d55c279347653ee143518c6f",
        strip_prefix = "github.com/Azure/go-autorest/autorest/azure/auth@v0.5.8",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/Azure/go-autorest/autorest/azure/auth/com_github_azure_go_autorest_autorest_azure_auth-v0.5.8.zip",
        ],
    )
    go_repository(
        name = "com_github_azure_go_autorest_autorest_azure_cli",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Azure/go-autorest/autorest/azure/cli",
        sha256 = "53e5162b6d72f1ab39119713cf3862a287e9fc61439d06d30378daf3e7bf1b7d",
        strip_prefix = "github.com/Azure/go-autorest/autorest/azure/cli@v0.4.3",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/Azure/go-autorest/autorest/azure/cli/com_github_azure_go_autorest_autorest_azure_cli-v0.4.3.zip",
        ],
    )
    go_repository(
        name = "com_github_azure_go_autorest_autorest_date",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Azure/go-autorest/autorest/date",
        sha256 = "7b59c0421eaf8549f20d17aab7e3e4621e1798de1119dac65a04c110d110d64d",
        strip_prefix = "github.com/Azure/go-autorest/autorest/date@v0.3.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/Azure/go-autorest/autorest/date/com_github_azure_go_autorest_autorest_date-v0.3.0.zip",
        ],
    )
    go_repository(
        name = "com_github_azure_go_autorest_autorest_mocks",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Azure/go-autorest/autorest/mocks",
        sha256 = "ccf8e9538ec800b2b9f4f2abed30e1bafe1e26487a9d0239af286de60c9ec0b0",
        strip_prefix = "github.com/Azure/go-autorest/autorest/mocks@v0.4.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/Azure/go-autorest/autorest/mocks/com_github_azure_go_autorest_autorest_mocks-v0.4.1.zip",
        ],
    )
    go_repository(
        name = "com_github_azure_go_autorest_autorest_to",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Azure/go-autorest/autorest/to",
        sha256 = "d5b92f83195b4cdc690d1f015a52678ba1300485049ef27489b112a1dc056e93",
        strip_prefix = "github.com/Azure/go-autorest/autorest/to@v0.4.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/Azure/go-autorest/autorest/to/com_github_azure_go_autorest_autorest_to-v0.4.0.zip",
        ],
    )
    go_repository(
        name = "com_github_azure_go_autorest_autorest_validation",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Azure/go-autorest/autorest/validation",
        sha256 = "70c6a2f246af440cb891028ffe32546fe31de53ffab5f2f93f6c1652efd549c3",
        strip_prefix = "github.com/Azure/go-autorest/autorest/validation@v0.3.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/Azure/go-autorest/autorest/validation/com_github_azure_go_autorest_autorest_validation-v0.3.1.zip",
        ],
    )
    go_repository(
        name = "com_github_azure_go_autorest_logger",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Azure/go-autorest/logger",
        sha256 = "90c84e126b503027f69d232f4ce5758ae01d08ea729c71539ebff851f2477b49",
        strip_prefix = "github.com/Azure/go-autorest/logger@v0.2.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/Azure/go-autorest/logger/com_github_azure_go_autorest_logger-v0.2.1.zip",
        ],
    )
    go_repository(
        name = "com_github_azure_go_autorest_tracing",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Azure/go-autorest/tracing",
        sha256 = "b7296ba64ecae67c83ae1c89da47c6f65c2ff0807027e301daccab32673914b3",
        strip_prefix = "github.com/Azure/go-autorest/tracing@v0.6.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/Azure/go-autorest/tracing/com_github_azure_go_autorest_tracing-v0.6.0.zip",
        ],
    )
    go_repository(
        name = "com_github_bazelbuild_remote_apis",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/bazelbuild/remote-apis",
        sha256 = "b0e976769bbe7c63773b50b407c952c0d1525b6d26b3585e16e09cb1cd195e75",
        strip_prefix = "github.com/bazelbuild/remote-apis@v0.0.0-20200708200203-1252343900d9",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/bazelbuild/remote-apis/com_github_bazelbuild_remote_apis-v0.0.0-20200708200203-1252343900d9.zip",
        ],
    )
    go_repository(
        name = "com_github_bazelbuild_rules_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/bazelbuild/rules_go",
        sha256 = "0f69d51e54c1012f62434b68e6d49e2e1c1371a493926da58063e8461aa2b9ff",
        strip_prefix = "github.com/bazelbuild/rules_go@v0.26.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/bazelbuild/rules_go/com_github_bazelbuild_rules_go-v0.26.0.zip",
        ],
    )
    go_repository(
        name = "com_github_benbjohnson_clock",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/benbjohnson/clock",
        sha256 = "d04e441d7f577f7861db72305478105dc75fd7030307a0fa325e328500283445",
        strip_prefix = "github.com/benbjohnson/clock@v1.1.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/benbjohnson/clock/com_github_benbjohnson_clock-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_benbjohnson_immutable",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/benbjohnson/immutable",
        sha256 = "0647fb6491c6606945e04f2c83b4163b9cf3584550f387695c32f262efa198b7",
        strip_prefix = "github.com/benbjohnson/immutable@v0.2.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/benbjohnson/immutable/com_github_benbjohnson_immutable-v0.2.1.zip",
        ],
    )
    go_repository(
        name = "com_github_benbjohnson_tmpl",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/benbjohnson/tmpl",
        sha256 = "7341fd268e36500455f8f0efab16db29525f2483c0fc8dca1e81f9c42a10b633",
        strip_prefix = "github.com/benbjohnson/tmpl@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/benbjohnson/tmpl/com_github_benbjohnson_tmpl-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_beorn7_perks",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/beorn7/perks",
        sha256 = "25bd9e2d94aca770e6dbc1f53725f84f6af4432f631d35dd2c46f96ef0512f1a",
        strip_prefix = "github.com/beorn7/perks@v1.0.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/beorn7/perks/com_github_beorn7_perks-v1.0.1.zip",
        ],
    )
    go_repository(
        name = "com_github_bgentry_go_netrc",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/bgentry/go-netrc",
        sha256 = "59fbb1e8e307ccd7052f77186990d744284b186e8b1c5ebdfb12405ae8d7f935",
        strip_prefix = "github.com/bgentry/go-netrc@v0.0.0-20140422174119-9fd32a8b3d3d",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/bgentry/go-netrc/com_github_bgentry_go_netrc-v0.0.0-20140422174119-9fd32a8b3d3d.zip",
        ],
    )
    go_repository(
        name = "com_github_bgentry_speakeasy",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/bgentry/speakeasy",
        sha256 = "d4bfd48b9bf68c87f92c94478ac910bcdab272e15eb909d58f1fb939233f75f0",
        strip_prefix = "github.com/bgentry/speakeasy@v0.1.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/bgentry/speakeasy/com_github_bgentry_speakeasy-v0.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_biogo_store",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/biogo/store",
        sha256 = "26551f8829c5ada84a68ef240732375be6747252aba423cf5c88bc03002c3600",
        strip_prefix = "github.com/biogo/store@v0.0.0-20160505134755-913427a1d5e8",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/biogo/store/com_github_biogo_store-v0.0.0-20160505134755-913427a1d5e8.zip",
        ],
    )
    go_repository(
        name = "com_github_bitly_go_hostpool",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/bitly/go-hostpool",
        sha256 = "9a55584d7fa2c1639d0ea11cd5b437786c2eadc2401d825e699ad6445fc8e476",
        strip_prefix = "github.com/bitly/go-hostpool@v0.0.0-20171023180738-a3a6125de932",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/bitly/go-hostpool/com_github_bitly_go_hostpool-v0.0.0-20171023180738-a3a6125de932.zip",
        ],
    )
    go_repository(
        name = "com_github_bitly_go_simplejson",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/bitly/go-simplejson",
        sha256 = "53930281dc7fba8947c1b1f07c82952a38dcaefae23bd3c8e71d70a6daa6cb40",
        strip_prefix = "github.com/bitly/go-simplejson@v0.5.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/bitly/go-simplejson/com_github_bitly_go_simplejson-v0.5.0.zip",
        ],
    )
    go_repository(
        name = "com_github_bketelsen_crypt",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/bketelsen/crypt",
        sha256 = "ab24f8c0386cc7fce86f4e6680c32214e1e597980bd80127ac84e71ace6763da",
        strip_prefix = "github.com/bketelsen/crypt@v0.0.4",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/bketelsen/crypt/com_github_bketelsen_crypt-v0.0.4.zip",
        ],
    )
    go_repository(
        name = "com_github_blang_semver",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/blang/semver",
        sha256 = "8d032399cf835b93f7cf641b5477a31a002059eed7888a775f97bd3e9677ad3c",
        strip_prefix = "github.com/blang/semver@v3.5.1+incompatible",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/blang/semver/com_github_blang_semver-v3.5.1+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_bmizerany_assert",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/bmizerany/assert",
        sha256 = "2532a167df77ade7e8012f07c0e3db4d4c15abdb7ffa7b05e1d961408da9a539",
        strip_prefix = "github.com/bmizerany/assert@v0.0.0-20160611221934-b7ed37b82869",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/bmizerany/assert/com_github_bmizerany_assert-v0.0.0-20160611221934-b7ed37b82869.zip",
        ],
    )
    go_repository(
        name = "com_github_bmizerany_pat",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/bmizerany/pat",
        sha256 = "ed04bed4d193e25371ebc6524984da4af9ece5c107fcc82d5aa4914b726706d2",
        strip_prefix = "github.com/bmizerany/pat@v0.0.0-20170815010413-6226ea591a40",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/bmizerany/pat/com_github_bmizerany_pat-v0.0.0-20170815010413-6226ea591a40.zip",
        ],
    )
    go_repository(
        name = "com_github_bmizerany_perks",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/bmizerany/perks",
        sha256 = "b78e7083e73b6c2d63a30d073515b2a03dbe3115171601009211208ee0c6046e",
        strip_prefix = "github.com/bmizerany/perks@v0.0.0-20141205001514-d9a9656a3a4b",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/bmizerany/perks/com_github_bmizerany_perks-v0.0.0-20141205001514-d9a9656a3a4b.zip",
        ],
    )
    go_repository(
        name = "com_github_boltdb_bolt",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/boltdb/bolt",
        sha256 = "ecaf17b0dbe7c85a017704c72667b2526b492b1a753ce7302a27dd2fb2e6ee79",
        strip_prefix = "github.com/boltdb/bolt@v1.3.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/boltdb/bolt/com_github_boltdb_bolt-v1.3.1.zip",
        ],
    )
    go_repository(
        name = "com_github_bonitoo_io_go_sql_bigquery",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/bonitoo-io/go-sql-bigquery",
        sha256 = "44b0e33b6573e46aa1c15837a8d9433a1e07ca232be3ea89879cad11bdfea617",
        strip_prefix = "github.com/bonitoo-io/go-sql-bigquery@v0.3.4-1.4.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/bonitoo-io/go-sql-bigquery/com_github_bonitoo_io_go_sql_bigquery-v0.3.4-1.4.0.zip",
        ],
    )
    go_repository(
        name = "com_github_broady_gogeohash",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/broady/gogeohash",
        sha256 = "e68cf873bace15902feaee2b42a139428e816e120a213901b4792f9006e38984",
        strip_prefix = "github.com/broady/gogeohash@v0.0.0-20120525094510-7b2c40d64042",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/broady/gogeohash/com_github_broady_gogeohash-v0.0.0-20120525094510-7b2c40d64042.zip",
        ],
    )
    go_repository(
        name = "com_github_bshuster_repo_logrus_logstash_hook",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/bshuster-repo/logrus-logstash-hook",
        sha256 = "743aace72067cc91c91cc4137566372220db5e5c487e2fd8fb04b6e23fe29d07",
        strip_prefix = "github.com/bshuster-repo/logrus-logstash-hook@v0.4.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/bshuster-repo/logrus-logstash-hook/com_github_bshuster_repo_logrus_logstash_hook-v0.4.1.zip",
        ],
    )
    go_repository(
        name = "com_github_bsm_sarama_cluster",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/bsm/sarama-cluster",
        sha256 = "5926505c631af623184a1c95453f33f340724c207d5bdffcd5619f2121853a57",
        strip_prefix = "github.com/bsm/sarama-cluster@v2.1.13+incompatible",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/bsm/sarama-cluster/com_github_bsm_sarama_cluster-v2.1.13+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_buchgr_bazel_remote",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/buchgr/bazel-remote",
        patch_args = ["-p1"],
        patches = [
            "@cockroach//build/patches:com_github_buchgr_bazel_remote.patch",
        ],
        sha256 = "7ab70784fddbc59e956501b2bc15a30c36baedb34df0d26009607d80c9e129e2",
        strip_prefix = "github.com/buchgr/bazel-remote@v1.3.3",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/buchgr/bazel-remote/com_github_buchgr_bazel_remote-v1.3.3.zip",
        ],
    )
    go_repository(
        name = "com_github_bufbuild_buf",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/bufbuild/buf",
        sha256 = "a0bf6cfe3b90c931022d2700b62f1b272d7e2ab62096c3df24551e3f5a575a52",
        strip_prefix = "github.com/bufbuild/buf@v0.56.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/bufbuild/buf/com_github_bufbuild_buf-v0.56.0.zip",
        ],
    )
    go_repository(
        name = "com_github_buger_jsonparser",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/buger/jsonparser",
        sha256 = "336c5facdeef6466131bf24c06f3ae1a40d038a7e6fdc3fcf3ab308ff50300c1",
        strip_prefix = "github.com/buger/jsonparser@v0.0.0-20200322175846-f7e751efca13",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/buger/jsonparser/com_github_buger_jsonparser-v0.0.0-20200322175846-f7e751efca13.zip",
        ],
    )
    go_repository(
        name = "com_github_bugsnag_bugsnag_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/bugsnag/bugsnag-go",
        sha256 = "e3b275ae2552bd1fa60f9cf728232ee4bde66afa0da772c20cb0a105818cf1bf",
        strip_prefix = "github.com/bugsnag/bugsnag-go@v0.0.0-20141110184014-b1d153021fcd",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/bugsnag/bugsnag-go/com_github_bugsnag_bugsnag_go-v0.0.0-20141110184014-b1d153021fcd.zip",
        ],
    )
    go_repository(
        name = "com_github_bugsnag_osext",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/bugsnag/osext",
        sha256 = "b29adb2508906ea508ba91f404ba33e709d43e037cec96d550335b8e756108bf",
        strip_prefix = "github.com/bugsnag/osext@v0.0.0-20130617224835-0dd3f918b21b",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/bugsnag/osext/com_github_bugsnag_osext-v0.0.0-20130617224835-0dd3f918b21b.zip",
        ],
    )
    go_repository(
        name = "com_github_bugsnag_panicwrap",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/bugsnag/panicwrap",
        sha256 = "c88454a2204604baecd45fa06bab717034e501b406c15470dba4bc8902356401",
        strip_prefix = "github.com/bugsnag/panicwrap@v0.0.0-20151223152923-e2c28503fcd0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/bugsnag/panicwrap/com_github_bugsnag_panicwrap-v0.0.0-20151223152923-e2c28503fcd0.zip",
        ],
    )
    go_repository(
        name = "com_github_burntsushi_toml",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/BurntSushi/toml",
        sha256 = "3f2cf2328cdfac39db5ae69c84201f961b50d15792f77de0abe87089836a932c",
        strip_prefix = "github.com/BurntSushi/toml@v0.4.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/BurntSushi/toml/com_github_burntsushi_toml-v0.4.1.zip",
        ],
    )
    go_repository(
        name = "com_github_burntsushi_xgb",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/BurntSushi/xgb",
        sha256 = "f52962c7fbeca81ea8a777d1f8b1f1d25803dc437fbb490f253344232884328e",
        strip_prefix = "github.com/BurntSushi/xgb@v0.0.0-20160522181843-27f122750802",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/BurntSushi/xgb/com_github_burntsushi_xgb-v0.0.0-20160522181843-27f122750802.zip",
        ],
    )
    go_repository(
        name = "com_github_burntsushi_xgbutil",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/BurntSushi/xgbutil",
        sha256 = "680bb03650f0f43760cab53ec7b3b159ea489f04f379bbba25b5a8d77a2de2e0",
        strip_prefix = "github.com/BurntSushi/xgbutil@v0.0.0-20160919175755-f7c97cef3b4e",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/BurntSushi/xgbutil/com_github_burntsushi_xgbutil-v0.0.0-20160919175755-f7c97cef3b4e.zip",
        ],
    )
    go_repository(
        name = "com_github_c_bata_go_prompt",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/c-bata/go-prompt",
        sha256 = "ffe765d86d90afdf8519def13cb027c94a1fbafea7a18e9625210786663436c4",
        strip_prefix = "github.com/c-bata/go-prompt@v0.2.2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/c-bata/go-prompt/com_github_c_bata_go_prompt-v0.2.2.zip",
        ],
    )
    go_repository(
        name = "com_github_cactus_go_statsd_client_statsd",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cactus/go-statsd-client/statsd",
        sha256 = "7823c66e8c56c950643954712edbc9974669a10fe0041b67c9bd81dabd0538c0",
        strip_prefix = "github.com/cactus/go-statsd-client/statsd@v0.0.0-20191106001114-12b4e2b38748",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/cactus/go-statsd-client/statsd/com_github_cactus_go_statsd_client_statsd-v0.0.0-20191106001114-12b4e2b38748.zip",
        ],
    )
    go_repository(
        name = "com_github_casbin_casbin_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/casbin/casbin/v2",
        sha256 = "753df5c3fa5de68592e95fd55427f264dc7590a0bf781a77eb56ae721d6d3351",
        strip_prefix = "github.com/casbin/casbin/v2@v2.1.2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/casbin/casbin/v2/com_github_casbin_casbin_v2-v2.1.2.zip",
        ],
    )
    go_repository(
        name = "com_github_cenkalti_backoff",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cenkalti/backoff",
        sha256 = "f8196815a1b4d25e5b8158029d5264801fc8aa5ff128ccf30752fd169693d43b",
        strip_prefix = "github.com/cenkalti/backoff@v2.2.1+incompatible",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/cenkalti/backoff/com_github_cenkalti_backoff-v2.2.1+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_cenkalti_backoff_v3",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cenkalti/backoff/v3",
        sha256 = "c69bf77e7b43cb3935d763c24af3810d9869a664bbcd26ffad9d3dc1bf602006",
        strip_prefix = "github.com/cenkalti/backoff/v3@v3.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/cenkalti/backoff/v3/com_github_cenkalti_backoff_v3-v3.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_cenkalti_backoff_v4",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cenkalti/backoff/v4",
        sha256 = "de69f5db190ee0f2c441e50e4bf607853ab99512a183a5713803888ced502dde",
        strip_prefix = "github.com/cenkalti/backoff/v4@v4.1.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/cenkalti/backoff/v4/com_github_cenkalti_backoff_v4-v4.1.1.zip",
        ],
    )
    go_repository(
        name = "com_github_census_instrumentation_opencensus_proto",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/census-instrumentation/opencensus-proto",
        sha256 = "b3c09f3e635d47b4138695a547d1f2c7138f382cbe5a8b5865b66a8e08233461",
        strip_prefix = "github.com/census-instrumentation/opencensus-proto@v0.2.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/census-instrumentation/opencensus-proto/com_github_census_instrumentation_opencensus_proto-v0.2.1.zip",
        ],
    )
    go_repository(
        name = "com_github_certifi_gocertifi",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/certifi/gocertifi",
        sha256 = "11d525844c3dd711fb0ae31acc9ebd8a4d602215f14ff24ad1764ecb48464849",
        strip_prefix = "github.com/certifi/gocertifi@v0.0.0-20200922220541-2c3bb06c6054",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/certifi/gocertifi/com_github_certifi_gocertifi-v0.0.0-20200922220541-2c3bb06c6054.zip",
        ],
    )
    go_repository(
        name = "com_github_cespare_xxhash",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cespare/xxhash",
        sha256 = "fe98c56670b21631f7fd3305a29a3b17e86a6cce3876a2119460717a18538e2e",
        strip_prefix = "github.com/cespare/xxhash@v1.1.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/cespare/xxhash/com_github_cespare_xxhash-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_cespare_xxhash_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cespare/xxhash/v2",
        sha256 = "ac641ac8330df61f677c87448fc679c47886087d2933a1e431556f423782ea38",
        strip_prefix = "github.com/cespare/xxhash/v2@v2.1.2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/cespare/xxhash/v2/com_github_cespare_xxhash_v2-v2.1.2.zip",
        ],
    )
    go_repository(
        name = "com_github_checkpoint_restore_go_criu_v4",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/checkpoint-restore/go-criu/v4",
        sha256 = "e0413b7c67d41b9e77207db083387d2eb300356541f9048c7ff0670ba7ace524",
        strip_prefix = "github.com/checkpoint-restore/go-criu/v4@v4.1.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/checkpoint-restore/go-criu/v4/com_github_checkpoint_restore_go_criu_v4-v4.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_chzyer_logex",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/chzyer/logex",
        sha256 = "2c94771c1e335a2c58a96444b3768b8e00297747d6ce7e7c14bab2e8b39d91bd",
        strip_prefix = "github.com/chzyer/logex@v1.1.10",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/chzyer/logex/com_github_chzyer_logex-v1.1.10.zip",
        ],
    )
    go_repository(
        name = "com_github_chzyer_readline",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/chzyer/readline",
        sha256 = "3dc842677887278fb33d25078d375ae6a7a94bb77a8d205ee2230b581b6947a6",
        strip_prefix = "github.com/chzyer/readline@v0.0.0-20180603132655-2972be24d48e",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/chzyer/readline/com_github_chzyer_readline-v0.0.0-20180603132655-2972be24d48e.zip",
        ],
    )
    go_repository(
        name = "com_github_chzyer_test",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/chzyer/test",
        sha256 = "ad8550bed3c4a94bbef57b9fc5bb15806eaceda00925716404320580d60e2f7d",
        strip_prefix = "github.com/chzyer/test@v0.0.0-20180213035817-a1ea475d72b1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/chzyer/test/com_github_chzyer_test-v0.0.0-20180213035817-a1ea475d72b1.zip",
        ],
    )
    go_repository(
        name = "com_github_cilium_ebpf",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cilium/ebpf",
        sha256 = "36e38f5c91cda71526cb171e48a4f2a05e7c60a1fc7269f0cae6471ed64709a5",
        strip_prefix = "github.com/cilium/ebpf@v0.4.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/cilium/ebpf/com_github_cilium_ebpf-v0.4.0.zip",
        ],
    )
    go_repository(
        name = "com_github_circonus_labs_circonus_gometrics",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/circonus-labs/circonus-gometrics",
        sha256 = "d8081141497e3cd34844df66af016c7900d58b324fb689e17e57bc053d91c9ba",
        strip_prefix = "github.com/circonus-labs/circonus-gometrics@v2.3.1+incompatible",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/circonus-labs/circonus-gometrics/com_github_circonus_labs_circonus_gometrics-v2.3.1+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_circonus_labs_circonusllhist",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/circonus-labs/circonusllhist",
        sha256 = "4dc805d9735dd9ca9b8875c0ad23126abb5bc969c5a40c61b5bc891808dbdcb6",
        strip_prefix = "github.com/circonus-labs/circonusllhist@v0.1.3",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/circonus-labs/circonusllhist/com_github_circonus_labs_circonusllhist-v0.1.3.zip",
        ],
    )
    go_repository(
        name = "com_github_clbanning_x2j",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/clbanning/x2j",
        sha256 = "747daafe80e4ac504626c01a1d28b1a64b785586975a47b50d62853a444b72a0",
        strip_prefix = "github.com/clbanning/x2j@v0.0.0-20191024224557-825249438eec",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/clbanning/x2j/com_github_clbanning_x2j-v0.0.0-20191024224557-825249438eec.zip",
        ],
    )
    go_repository(
        name = "com_github_client9_misspell",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/client9/misspell",
        sha256 = "a3af206372e131dd10a68ac470c66a1b18eaf51c6afacb55b2e2a06e39b90728",
        strip_prefix = "github.com/client9/misspell@v0.3.4",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/client9/misspell/com_github_client9_misspell-v0.3.4.zip",
        ],
    )
    go_repository(
        name = "com_github_cloudykit_fastprinter",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/CloudyKit/fastprinter",
        sha256 = "7e6015de3e986e5de8bf7310887bb0d8c1c33d66c5aacbd706aeec524dfda765",
        strip_prefix = "github.com/CloudyKit/fastprinter@v0.0.0-20200109182630-33d98a066a53",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/CloudyKit/fastprinter/com_github_cloudykit_fastprinter-v0.0.0-20200109182630-33d98a066a53.zip",
        ],
    )
    go_repository(
        name = "com_github_cloudykit_jet",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/CloudyKit/jet",
        sha256 = "9191d5a10096ea10bbb4ea576131ba943a6ec600fd7358237e21f995240ec72f",
        strip_prefix = "github.com/CloudyKit/jet@v2.1.3-0.20180809161101-62edd43e4f88+incompatible",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/CloudyKit/jet/com_github_cloudykit_jet-v2.1.3-0.20180809161101-62edd43e4f88+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_cloudykit_jet_v3",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/CloudyKit/jet/v3",
        sha256 = "eba40af7c0be5a2c4b0cdff2475ae6e16cb7f1acb7531a02b77de06b9b4a527a",
        strip_prefix = "github.com/CloudyKit/jet/v3@v3.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/CloudyKit/jet/v3/com_github_cloudykit_jet_v3-v3.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_cncf_udpa_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cncf/udpa/go",
        sha256 = "a449fa94e58117a79c17577e39f72f695c4876f74cbd9142d512278192ca90aa",
        strip_prefix = "github.com/cncf/udpa/go@v0.0.0-20210930031921-04548b0d99d4",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/cncf/udpa/go/com_github_cncf_udpa_go-v0.0.0-20210930031921-04548b0d99d4.zip",
        ],
    )
    go_repository(
        name = "com_github_cncf_xds_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cncf/xds/go",
        sha256 = "eb86281e1e9cf6f83d25edbec0e3fd690570efa3414866d14c6a32b44931375d",
        strip_prefix = "github.com/cncf/xds/go@v0.0.0-20211011173535-cb28da3451f1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/cncf/xds/go/com_github_cncf_xds_go-v0.0.0-20211011173535-cb28da3451f1.zip",
        ],
    )
    go_repository(
        name = "com_github_cockroachdb_apd",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cockroachdb/apd",
        sha256 = "fef7ec2fae220f84bfacb17fbfc1b04a666ab7f6fc04f3ff6d2b1e05c380777d",
        strip_prefix = "github.com/cockroachdb/apd@v1.1.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/cockroachdb/apd/com_github_cockroachdb_apd-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_cockroachdb_apd_v3",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cockroachdb/apd/v3",
        sha256 = "0571a4bf35a79df25f1e4a0b807ce3e30f7edbf69412d0272affaa925e25f625",
        strip_prefix = "github.com/cockroachdb/apd/v3@v3.1.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/cockroachdb/apd/v3/com_github_cockroachdb_apd_v3-v3.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_cockroachdb_circuitbreaker",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cockroachdb/circuitbreaker",
        sha256 = "52fdb5ba6a60e9a2f1db42d5b3c4c13cc5bb3947d5ce7f1bba9b0a14de71813a",
        strip_prefix = "github.com/cockroachdb/circuitbreaker@v2.2.2-0.20190114160014-a614b14ccf63+incompatible",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/cockroachdb/circuitbreaker/com_github_cockroachdb_circuitbreaker-v2.2.2-0.20190114160014-a614b14ccf63+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_cockroachdb_cmux",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cockroachdb/cmux",
        sha256 = "88f6f9cf33eb535658540b46f6222f029398e590a3ff9cc873d7d561ac6debf0",
        strip_prefix = "github.com/cockroachdb/cmux@v0.0.0-20170110192607-30d10be49292",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/cockroachdb/cmux/com_github_cockroachdb_cmux-v0.0.0-20170110192607-30d10be49292.zip",
        ],
    )
    go_repository(
        name = "com_github_cockroachdb_cockroach_go_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cockroachdb/cockroach-go/v2",
        sha256 = "c69c60f98ba1e3449bb60985b44fcfb0e8ae0fd4e3974abb1c997a0b9d5faca0",
        strip_prefix = "github.com/cockroachdb/cockroach-go/v2@v2.2.6",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/cockroachdb/cockroach-go/v2/com_github_cockroachdb_cockroach_go_v2-v2.2.6.zip",
        ],
    )
    go_repository(
        name = "com_github_cockroachdb_crlfmt",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cockroachdb/crlfmt",
        sha256 = "452219ca74191eedc6f44a5088b5d64e6f75168b438ac9eafe2256b0db8dbcad",
        strip_prefix = "github.com/cockroachdb/crlfmt@v0.0.0-20210128092314-b3eff0b87c79",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/cockroachdb/crlfmt/com_github_cockroachdb_crlfmt-v0.0.0-20210128092314-b3eff0b87c79.zip",
        ],
    )
    go_repository(
        name = "com_github_cockroachdb_datadriven",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cockroachdb/datadriven",
        sha256 = "0f8fe6199b1c7c44cbd4ce9ac0f3f9e1d2f02263f409361e49445fad4a410ab8",
        strip_prefix = "github.com/cockroachdb/datadriven@v1.0.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/cockroachdb/datadriven/com_github_cockroachdb_datadriven-v1.0.1.zip",
        ],
    )
    go_repository(
        name = "com_github_cockroachdb_errors",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cockroachdb/errors",
        patch_args = ["-p1"],
        patches = [
            "@cockroach//build/patches:com_github_cockroachdb_errors.patch",
        ],
        sha256 = "65cad26bfd3df564cfcbdc99f14b72b1a62d53d71956f60499a51801518e1fc3",
        strip_prefix = "github.com/cockroachdb/errors@v1.8.9",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/cockroachdb/errors/com_github_cockroachdb_errors-v1.8.9.zip",
        ],
    )
    go_repository(
        name = "com_github_cockroachdb_go_test_teamcity",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cockroachdb/go-test-teamcity",
        sha256 = "bac30148e525b79d004da84d16453ddd2d5cd20528e9187f1d7dac708335674b",
        strip_prefix = "github.com/cockroachdb/go-test-teamcity@v0.0.0-20191211140407-cff980ad0a55",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/cockroachdb/go-test-teamcity/com_github_cockroachdb_go_test_teamcity-v0.0.0-20191211140407-cff980ad0a55.zip",
        ],
    )
    go_repository(
        name = "com_github_cockroachdb_gostdlib",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cockroachdb/gostdlib",
        sha256 = "b3d43d8f95edf65f73a5348f29e1159823cac64b148f8d3bb48340bf55d70872",
        strip_prefix = "github.com/cockroachdb/gostdlib@v1.13.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/cockroachdb/gostdlib/com_github_cockroachdb_gostdlib-v1.13.0.zip",
        ],
    )
    go_repository(
        name = "com_github_cockroachdb_logtags",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cockroachdb/logtags",
        sha256 = "1972c3f171f118add3fd9e64bcea6cbb9959a3b7fa0ada308e8a7310813fea74",
        strip_prefix = "github.com/cockroachdb/logtags@v0.0.0-20211118104740-dabe8e521a4f",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/cockroachdb/logtags/com_github_cockroachdb_logtags-v0.0.0-20211118104740-dabe8e521a4f.zip",
        ],
    )
    go_repository(
        name = "com_github_cockroachdb_pebble",
        build_directives = ["gazelle:build_tags invariants"],
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cockroachdb/pebble",
        patch_args = ["-p1"],
        patches = [
            "@cockroach//build/patches:com_github_cockroachdb_pebble.patch",
        ],
        sha256 = "5b306806fca1c0defafe031c4cd842934aec44394bc44dc24bc805bc3fd609b8",
        strip_prefix = "github.com/cockroachdb/pebble@v0.0.0-20220301234049-69a82fe41c31",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/cockroachdb/pebble/com_github_cockroachdb_pebble-v0.0.0-20220301234049-69a82fe41c31.zip",
        ],
    )
    go_repository(
        name = "com_github_cockroachdb_redact",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cockroachdb/redact",
        sha256 = "7778b1e4485e4f17f35e5e592d87eb99c29e173ac9507801d000ad76dd0c261e",
        strip_prefix = "github.com/cockroachdb/redact@v1.1.3",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/cockroachdb/redact/com_github_cockroachdb_redact-v1.1.3.zip",
        ],
    )
    go_repository(
        name = "com_github_cockroachdb_returncheck",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cockroachdb/returncheck",
        sha256 = "ce92ba4352deec995b1f2eecf16eba7f5d51f5aa245a1c362dfe24c83d31f82b",
        strip_prefix = "github.com/cockroachdb/returncheck@v0.0.0-20200612231554-92cdbca611dd",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/cockroachdb/returncheck/com_github_cockroachdb_returncheck-v0.0.0-20200612231554-92cdbca611dd.zip",
        ],
    )
    go_repository(
        name = "com_github_cockroachdb_sentry_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cockroachdb/sentry-go",
        sha256 = "fbb2207d02aecfdd411b1357efe1192dbb827959e36b7cab7491731ac55935c9",
        strip_prefix = "github.com/cockroachdb/sentry-go@v0.6.1-cockroachdb.2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/cockroachdb/sentry-go/com_github_cockroachdb_sentry_go-v0.6.1-cockroachdb.2.zip",
        ],
    )
    go_repository(
        name = "com_github_cockroachdb_stress",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cockroachdb/stress",
        sha256 = "a516f84f8e1ad554357b7e60083dddf092d5ac1e80e311de47c46eeac28a4453",
        strip_prefix = "github.com/cockroachdb/stress@v0.0.0-20220217190341-94cf65c2a29f",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/cockroachdb/stress/com_github_cockroachdb_stress-v0.0.0-20220217190341-94cf65c2a29f.zip",
        ],
    )
    go_repository(
        name = "com_github_cockroachdb_tools",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cockroachdb/tools",
        sha256 = "37a3737dd23768b4997b2f0341d625658f5862cdbf808f7fbf3a7f9fd25913a7",
        strip_prefix = "github.com/cockroachdb/tools@v0.0.0-20211112185054-642e51449b40",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/cockroachdb/tools/com_github_cockroachdb_tools-v0.0.0-20211112185054-642e51449b40.zip",
        ],
    )
    go_repository(
        name = "com_github_cockroachdb_ttycolor",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cockroachdb/ttycolor",
        sha256 = "1260533510c89abd6d8af573a40f0246f6865d5091144dea509b2c48e7c61614",
        strip_prefix = "github.com/cockroachdb/ttycolor@v0.0.0-20210902133924-c7d7dcdde4e8",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/cockroachdb/ttycolor/com_github_cockroachdb_ttycolor-v0.0.0-20210902133924-c7d7dcdde4e8.zip",
        ],
    )
    go_repository(
        name = "com_github_codahale_hdrhistogram",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/codahale/hdrhistogram",
        sha256 = "e7e117da64da2f921b1f9dc57c524430a7f74a78c4b0bad718d85b08e8374e78",
        strip_prefix = "github.com/codahale/hdrhistogram@v0.0.0-20161010025455-3a0bb77429bd",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/codahale/hdrhistogram/com_github_codahale_hdrhistogram-v0.0.0-20161010025455-3a0bb77429bd.zip",
        ],
    )
    go_repository(
        name = "com_github_codefor_geohash",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Codefor/geohash",
        sha256 = "1f9d85fc86919143b53f8c3078fd4d2ed0271faf2eabba4460d7709f4f94c1e7",
        strip_prefix = "github.com/Codefor/geohash@v0.0.0-20140723084247-1b41c28e3a9d",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/Codefor/geohash/com_github_codefor_geohash-v0.0.0-20140723084247-1b41c28e3a9d.zip",
        ],
    )
    go_repository(
        name = "com_github_codegangsta_cli",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/codegangsta/cli",
        sha256 = "a5a237b9582cf92f0263865760457443bc061da1b1cda4dbf122e9ae53c8303b",
        strip_prefix = "github.com/codegangsta/cli@v1.20.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/codegangsta/cli/com_github_codegangsta_cli-v1.20.0.zip",
        ],
    )
    go_repository(
        name = "com_github_codegangsta_inject",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/codegangsta/inject",
        sha256 = "0a324d56992bffd288fa70a6d10eb9b8a9467665b0b1eb749ac6ae80e8977ee2",
        strip_prefix = "github.com/codegangsta/inject@v0.0.0-20150114235600-33e0aa1cb7c0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/codegangsta/inject/com_github_codegangsta_inject-v0.0.0-20150114235600-33e0aa1cb7c0.zip",
        ],
    )
    go_repository(
        name = "com_github_containerd_aufs",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/containerd/aufs",
        sha256 = "66f63be768b16cdee12c6d040d4aa7c0e8af306c7620b0c18c1f636c8499ae52",
        strip_prefix = "github.com/containerd/aufs@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/containerd/aufs/com_github_containerd_aufs-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_containerd_btrfs",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/containerd/btrfs",
        sha256 = "b6098229f65fe790a52e4db949ae020d9ff00fcd82b90ecb1337291a5aa8f9da",
        strip_prefix = "github.com/containerd/btrfs@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/containerd/btrfs/com_github_containerd_btrfs-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_containerd_cgroups",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/containerd/cgroups",
        sha256 = "68ef11daf1ad2ade3fa34665536c2cd51cd3e38fd0af4993fca6e1455a6d560f",
        strip_prefix = "github.com/containerd/cgroups@v1.0.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/containerd/cgroups/com_github_containerd_cgroups-v1.0.1.zip",
        ],
    )
    go_repository(
        name = "com_github_containerd_console",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/containerd/console",
        sha256 = "53af3db950dc4c31bbf3aade4b15750996c31efb34157c643232c8fcc0d1939c",
        strip_prefix = "github.com/containerd/console@v1.0.2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/containerd/console/com_github_containerd_console-v1.0.2.zip",
        ],
    )
    go_repository(
        name = "com_github_containerd_containerd",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/containerd/containerd",
        sha256 = "a88697b3fab925d50c5d0f10d6820ea42ca96b38dc52231ae7d4158c921e5190",
        strip_prefix = "github.com/containerd/containerd@v1.5.4",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/containerd/containerd/com_github_containerd_containerd-v1.5.4.zip",
        ],
    )
    go_repository(
        name = "com_github_containerd_continuity",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/containerd/continuity",
        sha256 = "c0fd35ecdee36368382bb9a95ea66a50a0b1f64a3bc7de57f57d4b0b8d58237b",
        strip_prefix = "github.com/containerd/continuity@v0.1.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/containerd/continuity/com_github_containerd_continuity-v0.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_containerd_fifo",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/containerd/fifo",
        sha256 = "6c0505fda39dbb0216326f55d9cdf7dd8a39367c0cf91d3e344fb177bfd7d639",
        strip_prefix = "github.com/containerd/fifo@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/containerd/fifo/com_github_containerd_fifo-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_containerd_go_cni",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/containerd/go-cni",
        sha256 = "a4d7aa24c80d4dc1c6ec75534db6c3dbca5be628469f266f36f675e071ebb3dc",
        strip_prefix = "github.com/containerd/go-cni@v1.0.2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/containerd/go-cni/com_github_containerd_go_cni-v1.0.2.zip",
        ],
    )
    go_repository(
        name = "com_github_containerd_go_runc",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/containerd/go-runc",
        sha256 = "61097092c5da43412b54044ede11e84920c52f774c880537df3e1c302ce08951",
        strip_prefix = "github.com/containerd/go-runc@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/containerd/go-runc/com_github_containerd_go_runc-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_containerd_imgcrypt",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/containerd/imgcrypt",
        sha256 = "20cf0651cc892db6c9905451ca9efd85621cffde866c2cbf385765dfcb9509f6",
        strip_prefix = "github.com/containerd/imgcrypt@v1.1.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/containerd/imgcrypt/com_github_containerd_imgcrypt-v1.1.1.zip",
        ],
    )
    go_repository(
        name = "com_github_containerd_nri",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/containerd/nri",
        sha256 = "db29e38861b46a6a1ae4b65072e8d1272e20d80c2e80d0615d93eaa5271126b7",
        strip_prefix = "github.com/containerd/nri@v0.1.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/containerd/nri/com_github_containerd_nri-v0.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_containerd_ttrpc",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/containerd/ttrpc",
        sha256 = "34df7d5959857d33b749ce3e49527f1980a704e080f63cbbf93a4ced1fcc6e1a",
        strip_prefix = "github.com/containerd/ttrpc@v1.0.2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/containerd/ttrpc/com_github_containerd_ttrpc-v1.0.2.zip",
        ],
    )
    go_repository(
        name = "com_github_containerd_typeurl",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/containerd/typeurl",
        sha256 = "af05054fc77d5d141066fda5ba24db4c1b7e418b934ab349d6ccd163e548f13b",
        strip_prefix = "github.com/containerd/typeurl@v1.0.2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/containerd/typeurl/com_github_containerd_typeurl-v1.0.2.zip",
        ],
    )
    go_repository(
        name = "com_github_containerd_zfs",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/containerd/zfs",
        sha256 = "e69004239428b28be0d99ac2f43e53b3b5838d89cf045223c3801b70cc1bacae",
        strip_prefix = "github.com/containerd/zfs@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/containerd/zfs/com_github_containerd_zfs-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_containernetworking_cni",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/containernetworking/cni",
        sha256 = "9cc37c3a71e267e0c78539203ed22ad2c03a711c70be3bd4727890d4e66b10b9",
        strip_prefix = "github.com/containernetworking/cni@v0.8.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/containernetworking/cni/com_github_containernetworking_cni-v0.8.1.zip",
        ],
    )
    go_repository(
        name = "com_github_containernetworking_plugins",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/containernetworking/plugins",
        sha256 = "4315bad50ebab3475340d074f1bd5e6070977a7502bfdd727f75742d6f225900",
        strip_prefix = "github.com/containernetworking/plugins@v0.9.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/containernetworking/plugins/com_github_containernetworking_plugins-v0.9.1.zip",
        ],
    )
    go_repository(
        name = "com_github_containers_ocicrypt",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/containers/ocicrypt",
        sha256 = "b31f83f8dbbdee1e855287a3624e239ba53257d6c35cf14aee4a75d79f1dfe27",
        strip_prefix = "github.com/containers/ocicrypt@v1.1.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/containers/ocicrypt/com_github_containers_ocicrypt-v1.1.1.zip",
        ],
    )
    go_repository(
        name = "com_github_coreos_bbolt",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/coreos/bbolt",
        sha256 = "097e7c6cf2dc9c50a0c8827f451bd3cba44c2cbf086d4fb684f2dfada9bfa841",
        strip_prefix = "github.com/coreos/bbolt@v1.3.2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/coreos/bbolt/com_github_coreos_bbolt-v1.3.2.zip",
        ],
    )
    go_repository(
        name = "com_github_coreos_etcd",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/coreos/etcd",
        sha256 = "c32b3fc5dba0eeb8533d628489cf862c4eb360644d79c597bcc6290f3d74b046",
        strip_prefix = "github.com/coreos/etcd@v3.3.13+incompatible",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/coreos/etcd/com_github_coreos_etcd-v3.3.13+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_coreos_go_etcd",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/coreos/go-etcd",
        sha256 = "4b226732835b9298af65db5d075024a5971aa11ef4b456899a3830bccd435b07",
        strip_prefix = "github.com/coreos/go-etcd@v2.0.0+incompatible",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/coreos/go-etcd/com_github_coreos_go_etcd-v2.0.0+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_coreos_go_iptables",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/coreos/go-iptables",
        sha256 = "07e31b1e6c7dec3e1c22b90b02bb1fe610884ad20ca46d623d949c71b3dcb55f",
        strip_prefix = "github.com/coreos/go-iptables@v0.5.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/coreos/go-iptables/com_github_coreos_go_iptables-v0.5.0.zip",
        ],
    )
    go_repository(
        name = "com_github_coreos_go_oidc",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/coreos/go-oidc",
        sha256 = "b997f93fbff8a4aed3bb2d78a3bf115ba4f06b1d1e4b9ef4cc9d1f63d3ce4036",
        strip_prefix = "github.com/coreos/go-oidc@v2.2.1+incompatible",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/coreos/go-oidc/com_github_coreos_go_oidc-v2.2.1+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_coreos_go_semver",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/coreos/go-semver",
        sha256 = "b2fc075395ffc34cff4b964681d0ae3cd22096cfcadd2970eeaa877596ceb210",
        strip_prefix = "github.com/coreos/go-semver@v0.3.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/coreos/go-semver/com_github_coreos_go_semver-v0.3.0.zip",
        ],
    )
    go_repository(
        name = "com_github_coreos_go_systemd",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/coreos/go-systemd",
        sha256 = "22237f0aed3ab6018a1025c65f4f45b4c05f9aa0c0bb9ec880294273b9a15bf2",
        strip_prefix = "github.com/coreos/go-systemd@v0.0.0-20190719114852-fd7a80b32e1f",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/coreos/go-systemd/com_github_coreos_go_systemd-v0.0.0-20190719114852-fd7a80b32e1f.zip",
        ],
    )
    go_repository(
        name = "com_github_coreos_go_systemd_v22",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/coreos/go-systemd/v22",
        sha256 = "01134ae89bf4a91c17eeb1f8425e1064f9bde64cf3ce0c9cf546a9fa1ee25e64",
        strip_prefix = "github.com/coreos/go-systemd/v22@v22.3.2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/coreos/go-systemd/v22/com_github_coreos_go_systemd_v22-v22.3.2.zip",
        ],
    )
    go_repository(
        name = "com_github_coreos_pkg",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/coreos/pkg",
        sha256 = "7fe161d49439a9b4136c932233cb4b803b9e3ac7ee46f39ce247defc4f4ea8d7",
        strip_prefix = "github.com/coreos/pkg@v0.0.0-20180928190104-399ea9e2e55f",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/coreos/pkg/com_github_coreos_pkg-v0.0.0-20180928190104-399ea9e2e55f.zip",
        ],
    )
    go_repository(
        name = "com_github_corpix_uarand",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/corpix/uarand",
        sha256 = "e2220522fbb3b8f21b44e7b6aecf52177738d82809ca4ab6918043ed5b19857c",
        strip_prefix = "github.com/corpix/uarand@v0.1.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/corpix/uarand/com_github_corpix_uarand-v0.1.1.zip",
        ],
    )
    go_repository(
        name = "com_github_cpuguy83_go_md2man",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cpuguy83/go-md2man",
        sha256 = "b9b153bb97e2a702ec5c41f6815985d4295524cdf4f2a9e5633f98e9739f4d6e",
        strip_prefix = "github.com/cpuguy83/go-md2man@v1.0.10",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/cpuguy83/go-md2man/com_github_cpuguy83_go_md2man-v1.0.10.zip",
        ],
    )
    go_repository(
        name = "com_github_cpuguy83_go_md2man_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cpuguy83/go-md2man/v2",
        sha256 = "f5ded1130031e98dc4bdf17e45ac89ad2eeae3083d6150b6d5f3fc75b9e9cd6b",
        strip_prefix = "github.com/cpuguy83/go-md2man/v2@v2.0.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/cpuguy83/go-md2man/v2/com_github_cpuguy83_go_md2man_v2-v2.0.1.zip",
        ],
    )
    go_repository(
        name = "com_github_creack_pty",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/creack/pty",
        sha256 = "d6594fd4844c242a5c7d6e9b25516182460cffa820e47e8ffb8eea625991986c",
        strip_prefix = "github.com/creack/pty@v1.1.11",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/creack/pty/com_github_creack_pty-v1.1.11.zip",
        ],
    )
    go_repository(
        name = "com_github_crossdock_crossdock_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/crossdock/crossdock-go",
        sha256 = "f8a2ed6cd39e4f3e8108b8987f72bf6746276ada6fd3fcc62015bdbdd097f1a3",
        strip_prefix = "github.com/crossdock/crossdock-go@v0.0.0-20160816171116-049aabb0122b",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/crossdock/crossdock-go/com_github_crossdock_crossdock_go-v0.0.0-20160816171116-049aabb0122b.zip",
        ],
    )
    go_repository(
        name = "com_github_cyberdelia_go_metrics_graphite",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cyberdelia/go-metrics-graphite",
        sha256 = "38a34b96a597d50553e367de6e4eb3488e83dc37cae3930b38d9b48695b08b0c",
        strip_prefix = "github.com/cyberdelia/go-metrics-graphite@v0.0.0-20161219230853-39f87cc3b432",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/cyberdelia/go-metrics-graphite/com_github_cyberdelia_go_metrics_graphite-v0.0.0-20161219230853-39f87cc3b432.zip",
        ],
    )
    go_repository(
        name = "com_github_cyberdelia_templates",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cyberdelia/templates",
        sha256 = "a0ed6b8037d36222f63128f6064ed5b0e461fa9798c3592440a08875154d6c72",
        strip_prefix = "github.com/cyberdelia/templates@v0.0.0-20141128023046-ca7fffd4298c",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/cyberdelia/templates/com_github_cyberdelia_templates-v0.0.0-20141128023046-ca7fffd4298c.zip",
        ],
    )
    go_repository(
        name = "com_github_cyphar_filepath_securejoin",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/cyphar/filepath-securejoin",
        sha256 = "d022873dbb9e8d3b7a43c9dedbea54dfc9a6c15f9632ba522a1257e8b948c100",
        strip_prefix = "github.com/cyphar/filepath-securejoin@v0.2.2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/cyphar/filepath-securejoin/com_github_cyphar_filepath_securejoin-v0.2.2.zip",
        ],
    )
    go_repository(
        name = "com_github_d2g_dhcp4",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/d2g/dhcp4",
        sha256 = "15df9468cf548a626e1319e92d550432512c4319cf555bf278ea9215de3504e3",
        strip_prefix = "github.com/d2g/dhcp4@v0.0.0-20170904100407-a1d1b6c41b1c",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/d2g/dhcp4/com_github_d2g_dhcp4-v0.0.0-20170904100407-a1d1b6c41b1c.zip",
        ],
    )
    go_repository(
        name = "com_github_d2g_dhcp4client",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/d2g/dhcp4client",
        sha256 = "cad5e5d2e85d2f4b68835ea63472f24a6627d6f87058358df4b47902374a6a8b",
        strip_prefix = "github.com/d2g/dhcp4client@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/d2g/dhcp4client/com_github_d2g_dhcp4client-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_d2g_dhcp4server",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/d2g/dhcp4server",
        sha256 = "b2370ecaf825f0dc748a234fb676fbd9d24ac6d28eaa9d7c3a8f807a2badf11d",
        strip_prefix = "github.com/d2g/dhcp4server@v0.0.0-20181031114812-7d4a0a7f59a5",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/d2g/dhcp4server/com_github_d2g_dhcp4server-v0.0.0-20181031114812-7d4a0a7f59a5.zip",
        ],
    )
    go_repository(
        name = "com_github_d2g_hardwareaddr",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/d2g/hardwareaddr",
        sha256 = "bdc3b033b884101d5aa56c79a82c05e1e30af5bec7c7beda317230c3fa400c5e",
        strip_prefix = "github.com/d2g/hardwareaddr@v0.0.0-20190221164911-e7d9fbe030e4",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/d2g/hardwareaddr/com_github_d2g_hardwareaddr-v0.0.0-20190221164911-e7d9fbe030e4.zip",
        ],
    )
    go_repository(
        name = "com_github_daaku_go_zipexe",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/daaku/go.zipexe",
        sha256 = "74d7a0242c03c3c03220e56a59da5f97d3478743250740df538e05e6b609f553",
        strip_prefix = "github.com/daaku/go.zipexe@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/daaku/go.zipexe/com_github_daaku_go_zipexe-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_data_dog_go_sqlmock",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/DATA-DOG/go-sqlmock",
        sha256 = "fe11397cc3551d3d4b015f2759862ce980d9930b3d807f8375a89b5180ae9f8e",
        strip_prefix = "github.com/DATA-DOG/go-sqlmock@v1.4.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/DATA-DOG/go-sqlmock/com_github_data_dog_go_sqlmock-v1.4.1.zip",
        ],
    )
    go_repository(
        name = "com_github_datadog_datadog_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/DataDog/datadog-go",
        sha256 = "ede4a024d3c106b2f57ca04d7bfc7610e0c83f4d8a3bace2cf87b42fd5cf66cd",
        strip_prefix = "github.com/DataDog/datadog-go@v3.2.0+incompatible",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/DataDog/datadog-go/com_github_datadog_datadog_go-v3.2.0+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_datadog_zstd",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/DataDog/zstd",
        sha256 = "00989c1f3f3426aa6ed41f641193ff6a0d81031ba0b7fd81250e7e923d2f18a4",
        strip_prefix = "github.com/DataDog/zstd@v1.5.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/DataDog/zstd/com_github_datadog_zstd-v1.5.0.zip",
        ],
    )
    go_repository(
        name = "com_github_dave_dst",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/dave/dst",
        sha256 = "73f53e4faffd0d5f77cc88d9fcc0fb9ee53232f301e01da6766962f9fe92c7b6",
        strip_prefix = "github.com/dave/dst@v0.24.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/dave/dst/com_github_dave_dst-v0.24.0.zip",
        ],
    )
    go_repository(
        name = "com_github_dave_gopackages",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/dave/gopackages",
        sha256 = "b953698eb72bd0cf6579f6b8cdc8238572063784da9d443ec70705d210b0c182",
        strip_prefix = "github.com/dave/gopackages@v0.0.0-20170318123100-46e7023ec56e",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/dave/gopackages/com_github_dave_gopackages-v0.0.0-20170318123100-46e7023ec56e.zip",
        ],
    )
    go_repository(
        name = "com_github_dave_jennifer",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/dave/jennifer",
        sha256 = "85b37a1b99b7d67664389b8c11b7174f521a396bb59d4e0e766df16336a7f112",
        strip_prefix = "github.com/dave/jennifer@v1.2.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/dave/jennifer/com_github_dave_jennifer-v1.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_dave_kerr",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/dave/kerr",
        sha256 = "58bfff20a2f687e0f607887e88ff1044fe22186765e93b794511b1a0a625eaa1",
        strip_prefix = "github.com/dave/kerr@v0.0.0-20170318121727-bc25dd6abe8e",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/dave/kerr/com_github_dave_kerr-v0.0.0-20170318121727-bc25dd6abe8e.zip",
        ],
    )
    go_repository(
        name = "com_github_dave_rebecca",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/dave/rebecca",
        sha256 = "74c7f193fcc4a165903e3761dbff05e73e6fcd92f8cf0861029487e65da40439",
        strip_prefix = "github.com/dave/rebecca@v0.9.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/dave/rebecca/com_github_dave_rebecca-v0.9.1.zip",
        ],
    )
    go_repository(
        name = "com_github_davecgh_go_spew",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/davecgh/go-spew",
        sha256 = "6b44a843951f371b7010c754ecc3cabefe815d5ced1c5b9409fb2d697e8a890d",
        strip_prefix = "github.com/davecgh/go-spew@v1.1.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/davecgh/go-spew/com_github_davecgh_go_spew-v1.1.1.zip",
        ],
    )
    go_repository(
        name = "com_github_deepmap_oapi_codegen",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/deepmap/oapi-codegen",
        sha256 = "a89ac7cc533495fb5aa9caf2f763394af143928bf38a351495d93e220744dc4e",
        strip_prefix = "github.com/deepmap/oapi-codegen@v1.6.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/deepmap/oapi-codegen/com_github_deepmap_oapi_codegen-v1.6.0.zip",
        ],
    )
    go_repository(
        name = "com_github_denisenkom_go_mssqldb",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/denisenkom/go-mssqldb",
        sha256 = "47f3f67715836b61575d2c09bc1b5ab0fea2f270ca0fd37e9da66537e4c0aab0",
        strip_prefix = "github.com/denisenkom/go-mssqldb@v0.10.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/denisenkom/go-mssqldb/com_github_denisenkom_go_mssqldb-v0.10.0.zip",
        ],
    )
    go_repository(
        name = "com_github_dennwc_varint",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/dennwc/varint",
        sha256 = "2918e66c0fb5a82dbfc8cca1ed34cb8ccff8188e876c0ca25f85b8247e53626f",
        strip_prefix = "github.com/dennwc/varint@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/dennwc/varint/com_github_dennwc_varint-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_denverdino_aliyungo",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/denverdino/aliyungo",
        sha256 = "a95aea20a342798881b676d44c0d42c486f646cf066b96093fa15ca1f3a1123f",
        strip_prefix = "github.com/denverdino/aliyungo@v0.0.0-20190125010748-a747050bb1ba",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/denverdino/aliyungo/com_github_denverdino_aliyungo-v0.0.0-20190125010748-a747050bb1ba.zip",
        ],
    )
    go_repository(
        name = "com_github_dgraph_io_badger",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/dgraph-io/badger",
        sha256 = "8329ae390aebec6ae360356e77a2743357ad4e0d0bd4c3ae03b7d17e01ad70aa",
        strip_prefix = "github.com/dgraph-io/badger@v1.6.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/dgraph-io/badger/com_github_dgraph_io_badger-v1.6.0.zip",
        ],
    )
    go_repository(
        name = "com_github_dgrijalva_jwt_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/dgrijalva/jwt-go",
        sha256 = "26b028eb2d9ee3aef26a96d6790e101f4088ef901008ebab17096966bf6522ad",
        strip_prefix = "github.com/dgrijalva/jwt-go@v3.2.0+incompatible",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/dgrijalva/jwt-go/com_github_dgrijalva_jwt_go-v3.2.0+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_dgrijalva_jwt_go_v4",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/dgrijalva/jwt-go/v4",
        sha256 = "9453f2b0484885c192b0c777195f911b599d1a424def0eb9387ef619d5bd7f4a",
        strip_prefix = "github.com/dgrijalva/jwt-go/v4@v4.0.0-preview1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/dgrijalva/jwt-go/v4/com_github_dgrijalva_jwt_go_v4-v4.0.0-preview1.zip",
        ],
    )
    go_repository(
        name = "com_github_dgryski_go_bitstream",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/dgryski/go-bitstream",
        sha256 = "52765898078b5dca28ebced04b05cff943a3b3538a371c16568c97f05d669f23",
        strip_prefix = "github.com/dgryski/go-bitstream@v0.0.0-20180413035011-3522498ce2c8",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/dgryski/go-bitstream/com_github_dgryski_go_bitstream-v0.0.0-20180413035011-3522498ce2c8.zip",
        ],
    )
    go_repository(
        name = "com_github_dgryski_go_farm",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/dgryski/go-farm",
        sha256 = "bdf602cab00a24c2898aabad0b40c7b1d76a29cf8dd3319ef87046a5f4b1726f",
        strip_prefix = "github.com/dgryski/go-farm@v0.0.0-20200201041132-a6ae2369ad13",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/dgryski/go-farm/com_github_dgryski_go_farm-v0.0.0-20200201041132-a6ae2369ad13.zip",
        ],
    )
    go_repository(
        name = "com_github_dgryski_go_metro",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/dgryski/go-metro",
        sha256 = "3f97b3cdeaee7b4fbf4fa06b7c52e3ee6bca461a100077892e861c6c8fc03722",
        strip_prefix = "github.com/dgryski/go-metro@v0.0.0-20180109044635-280f6062b5bc",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/dgryski/go-metro/com_github_dgryski_go_metro-v0.0.0-20180109044635-280f6062b5bc.zip",
        ],
    )
    go_repository(
        name = "com_github_dgryski_go_sip13",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/dgryski/go-sip13",
        sha256 = "55a0be7d50eab4c3daba9204a88554209c2065019d01ac78725155dd705e3fa9",
        strip_prefix = "github.com/dgryski/go-sip13@v0.0.0-20200911182023-62edffca9245",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/dgryski/go-sip13/com_github_dgryski_go_sip13-v0.0.0-20200911182023-62edffca9245.zip",
        ],
    )
    go_repository(
        name = "com_github_digitalocean_godo",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/digitalocean/godo",
        sha256 = "9a41cecefd19a707f3f6810beba85f7da129059f2fb5f45aa8c9630f8a435332",
        strip_prefix = "github.com/digitalocean/godo@v1.65.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/digitalocean/godo/com_github_digitalocean_godo-v1.65.0.zip",
        ],
    )
    go_repository(
        name = "com_github_dimchansky_utfbom",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/dimchansky/utfbom",
        sha256 = "0c1a11101602d5f57ac3e790c0b72e09ff87d8d535535f43fbee9e6a42327350",
        strip_prefix = "github.com/dimchansky/utfbom@v1.1.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/dimchansky/utfbom/com_github_dimchansky_utfbom-v1.1.1.zip",
        ],
    )
    go_repository(
        name = "com_github_djherbis_atime",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/djherbis/atime",
        sha256 = "195cebcceb6d76328f5e5d373154b5c46a6a9bf6b27a88f9c0158276a07c7c41",
        strip_prefix = "github.com/djherbis/atime@v1.1.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/djherbis/atime/com_github_djherbis_atime-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_dnaeon_go_vcr",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/dnaeon/go-vcr",
        sha256 = "8f586f95ce5567ef2ae702cf98e56a09ea0cc6171f5cd959e6fcf7502e00dabc",
        strip_prefix = "github.com/dnaeon/go-vcr@v1.0.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/dnaeon/go-vcr/com_github_dnaeon_go_vcr-v1.0.1.zip",
        ],
    )
    go_repository(
        name = "com_github_docker_distribution",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/docker/distribution",
        sha256 = "be78bc43d74873b67afe05a6b244490088680dab75bdfaf26d0fd4d054595bc7",
        strip_prefix = "github.com/docker/distribution@v2.7.1+incompatible",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/docker/distribution/com_github_docker_distribution-v2.7.1+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_docker_docker",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/docker/docker",
        sha256 = "b96610497d8d7b41eff039cd4b4ba595eedb3944b653b813f087364805e3db76",
        strip_prefix = "github.com/moby/moby@v20.10.6+incompatible",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/moby/moby/com_github_moby_moby-v20.10.6+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_docker_go_connections",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/docker/go-connections",
        sha256 = "570ebcee7e6fd844e00c89eeab2b1922081d6969df76078dfe4ffacd3db56ada",
        strip_prefix = "github.com/docker/go-connections@v0.4.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/docker/go-connections/com_github_docker_go_connections-v0.4.0.zip",
        ],
    )
    go_repository(
        name = "com_github_docker_go_events",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/docker/go-events",
        sha256 = "0f654eb0e7e07c237a229935ea3488728ddb5b082af2918b64452a1129dccae3",
        strip_prefix = "github.com/docker/go-events@v0.0.0-20190806004212-e31b211e4f1c",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/docker/go-events/com_github_docker_go_events-v0.0.0-20190806004212-e31b211e4f1c.zip",
        ],
    )
    go_repository(
        name = "com_github_docker_go_metrics",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/docker/go-metrics",
        sha256 = "4efab3706215f5b2d29ba823d3991fd6e2f81c02ce45ef0c73c019ebc90e020b",
        strip_prefix = "github.com/docker/go-metrics@v0.0.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/docker/go-metrics/com_github_docker_go_metrics-v0.0.1.zip",
        ],
    )
    go_repository(
        name = "com_github_docker_go_units",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/docker/go-units",
        sha256 = "0f2be7dce7b1a0ba6a4a786eb144a3398e9a61afc0eec5799a1520d9906fc58c",
        strip_prefix = "github.com/docker/go-units@v0.4.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/docker/go-units/com_github_docker_go_units-v0.4.0.zip",
        ],
    )
    go_repository(
        name = "com_github_docker_libtrust",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/docker/libtrust",
        sha256 = "7f7a72aae4276536e665d8dfdab7219231fbb402dec16ba79ccdb633a4692482",
        strip_prefix = "github.com/docker/libtrust@v0.0.0-20150114040149-fa567046d9b1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/docker/libtrust/com_github_docker_libtrust-v0.0.0-20150114040149-fa567046d9b1.zip",
        ],
    )
    go_repository(
        name = "com_github_docker_spdystream",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/docker/spdystream",
        sha256 = "70964f9eef29843634539b8d6e09c8b51ed6aa96b5deda28b7a44613327a22f2",
        strip_prefix = "github.com/docker/spdystream@v0.0.0-20160310174837-449fdfce4d96",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/docker/spdystream/com_github_docker_spdystream-v0.0.0-20160310174837-449fdfce4d96.zip",
        ],
    )
    go_repository(
        name = "com_github_docopt_docopt_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/docopt/docopt-go",
        sha256 = "00aad861d150c62598ca4fb01cfbe15c2eefb5186df7e5d4a59286dcf09556c8",
        strip_prefix = "github.com/docopt/docopt-go@v0.0.0-20180111231733-ee0de3bc6815",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/docopt/docopt-go/com_github_docopt_docopt_go-v0.0.0-20180111231733-ee0de3bc6815.zip",
        ],
    )
    go_repository(
        name = "com_github_dustin_go_humanize",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/dustin/go-humanize",
        sha256 = "e01916e082a6646ea12d7800d77af43045c27284ff2a0a77e3484509989cc107",
        strip_prefix = "github.com/dustin/go-humanize@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/dustin/go-humanize/com_github_dustin_go_humanize-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_dvyukov_go_fuzz",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/dvyukov/go-fuzz",
        sha256 = "0a4c4bc0a550c729115d74f6a636e5802894b33bc50aa8af99c4a70196d5990b",
        strip_prefix = "github.com/dvyukov/go-fuzz@v0.0.0-20210103155950-6a8e9d1f2415",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/dvyukov/go-fuzz/com_github_dvyukov_go_fuzz-v0.0.0-20210103155950-6a8e9d1f2415.zip",
        ],
    )
    go_repository(
        name = "com_github_eapache_go_resiliency",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/eapache/go-resiliency",
        sha256 = "39333303f947a85e0c35e9969d56e05776034b1ae91e75cbf9211ead5870d982",
        strip_prefix = "github.com/eapache/go-resiliency@v1.2.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/eapache/go-resiliency/com_github_eapache_go_resiliency-v1.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_eapache_go_xerial_snappy",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/eapache/go-xerial-snappy",
        sha256 = "785264afffdcfe50573a1cb0df85ff4186e9e7e4e3a04513752f52d3da1054af",
        strip_prefix = "github.com/eapache/go-xerial-snappy@v0.0.0-20180814174437-776d5712da21",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/eapache/go-xerial-snappy/com_github_eapache_go_xerial_snappy-v0.0.0-20180814174437-776d5712da21.zip",
        ],
    )
    go_repository(
        name = "com_github_eapache_queue",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/eapache/queue",
        sha256 = "1dc1b4972e8505c4763c65424b19604c65c944911d16c18c5cbd35aae45626fb",
        strip_prefix = "github.com/eapache/queue@v1.1.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/eapache/queue/com_github_eapache_queue-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_eclipse_paho_mqtt_golang",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/eclipse/paho.mqtt.golang",
        sha256 = "d36337c4b5a2752b91bcd437bd74e0907bf6c9e6c611dab88407bcca8462e918",
        strip_prefix = "github.com/eclipse/paho.mqtt.golang@v1.2.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/eclipse/paho.mqtt.golang/com_github_eclipse_paho_mqtt_golang-v1.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_edsrzf_mmap_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/edsrzf/mmap-go",
        sha256 = "851a1d4d6e30f97ab23b7e4a6a7da9d1842f126d738f7386010c6ee7bf82518e",
        strip_prefix = "github.com/edsrzf/mmap-go@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/edsrzf/mmap-go/com_github_edsrzf_mmap_go-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_eknkc_amber",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/eknkc/amber",
        sha256 = "b1dde9f3713742ad0961825a2d962bd99d9390daf8596e7680dfb5f395e54e22",
        strip_prefix = "github.com/eknkc/amber@v0.0.0-20171010120322-cdade1c07385",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/eknkc/amber/com_github_eknkc_amber-v0.0.0-20171010120322-cdade1c07385.zip",
        ],
    )
    go_repository(
        name = "com_github_elastic_gosigar",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/elastic/gosigar",
        sha256 = "f3462c11be4f93f4ddb6c629c90a1db2b5dcedcd2a6af02a2f0b7aca1d65e7ba",
        strip_prefix = "github.com/elastic/gosigar@v0.14.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/elastic/gosigar/com_github_elastic_gosigar-v0.14.1.zip",
        ],
    )
    go_repository(
        name = "com_github_elazarl_go_bindata_assetfs",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/elazarl/go-bindata-assetfs",
        sha256 = "ee91e4dedf0efd24ddf201e8f8b62f0b79a64efd0d205b30bcd9fa95f905cd15",
        strip_prefix = "github.com/elazarl/go-bindata-assetfs@v1.0.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/elazarl/go-bindata-assetfs/com_github_elazarl_go_bindata_assetfs-v1.0.1.zip",
        ],
    )
    go_repository(
        name = "com_github_elazarl_goproxy",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/elazarl/goproxy",
        sha256 = "21b7d89eab7acb25fb1b3affa494281fae8c31becf1ee2c6009da7249320d328",
        strip_prefix = "github.com/elazarl/goproxy@v0.0.0-20180725130230-947c36da3153",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/elazarl/goproxy/com_github_elazarl_goproxy-v0.0.0-20180725130230-947c36da3153.zip",
        ],
    )
    go_repository(
        name = "com_github_emicklei_dot",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/emicklei/dot",
        sha256 = "b298e957fd7e38cd76b3953e47afcfd673a4f051884818a294ec4703476b6a39",
        strip_prefix = "github.com/emicklei/dot@v0.15.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/emicklei/dot/com_github_emicklei_dot-v0.15.0.zip",
        ],
    )
    go_repository(
        name = "com_github_emicklei_go_restful",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/emicklei/go-restful",
        sha256 = "cf4eb1c8f654dfae915d2d77e7ec679e11f15f737454a232293bb14bd7401162",
        strip_prefix = "github.com/emicklei/go-restful@v2.12.0+incompatible",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/emicklei/go-restful/com_github_emicklei_go_restful-v2.12.0+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_emicklei_go_restful_v3",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/emicklei/go-restful/v3",
        sha256 = "3fc4643688db4a2eab6a44505a9f29ab7171ed174e0bbfb9ecae245deb722a10",
        strip_prefix = "github.com/emicklei/go-restful/v3@v3.7.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/emicklei/go-restful/v3/com_github_emicklei_go_restful_v3-v3.7.1.zip",
        ],
    )
    go_repository(
        name = "com_github_envoyproxy_go_control_plane",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/envoyproxy/go-control-plane",
        sha256 = "41dc70a8e658cb8945fa0de289d25dd7a608e99929bae144776781401dec507a",
        strip_prefix = "github.com/envoyproxy/go-control-plane@v0.9.10-0.20210907150352-cf90f659a021",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/envoyproxy/go-control-plane/com_github_envoyproxy_go_control_plane-v0.9.10-0.20210907150352-cf90f659a021.zip",
        ],
    )
    go_repository(
        name = "com_github_envoyproxy_protoc_gen_validate",
        build_file_proto_mode = "disable_global",
        build_naming_convention = "go_default_library",
        importpath = "github.com/envoyproxy/protoc-gen-validate",
        sha256 = "6e705cabc09d7d130e0d24acf474c0654a62d1e292f1ea2fb558b5565f43f7f3",
        strip_prefix = "github.com/envoyproxy/protoc-gen-validate@v0.6.2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/envoyproxy/protoc-gen-validate/com_github_envoyproxy_protoc_gen_validate-v0.6.2.zip",
        ],
    )
    go_repository(
        name = "com_github_etcd_io_bbolt",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/etcd-io/bbolt",
        sha256 = "6630d7aad4b10f76aea88ee6d9086a1edffe371651cc2432edfd0de6beb99120",
        strip_prefix = "github.com/etcd-io/bbolt@v1.3.3",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/etcd-io/bbolt/com_github_etcd_io_bbolt-v1.3.3.zip",
        ],
    )
    go_repository(
        name = "com_github_evanphx_json_patch",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/evanphx/json-patch",
        sha256 = "4a3bf63543b0745ffa4940aa13f7dde43087582aabfbf8eb752180f031f23d18",
        strip_prefix = "github.com/evanphx/json-patch@v4.11.0+incompatible",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/evanphx/json-patch/com_github_evanphx_json_patch-v4.11.0+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_facebookgo_clock",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/facebookgo/clock",
        sha256 = "5d6b671bd5afef8459fb7561d19bcf7c7f378da9943722d36676735b3c6272fa",
        strip_prefix = "github.com/facebookgo/clock@v0.0.0-20150410010913-600d898af40a",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/facebookgo/clock/com_github_facebookgo_clock-v0.0.0-20150410010913-600d898af40a.zip",
        ],
    )
    go_repository(
        name = "com_github_fanixk_geohash",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/fanixk/geohash",
        sha256 = "74ff24640609365d393ddddb47b32cdd21d7de9274ae23cce1b2ac8b1cf74339",
        strip_prefix = "github.com/fanixk/geohash@v0.0.0-20150324002647-c1f9b5fa157a",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/fanixk/geohash/com_github_fanixk_geohash-v0.0.0-20150324002647-c1f9b5fa157a.zip",
        ],
    )
    go_repository(
        name = "com_github_fasthttp_contrib_websocket",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/fasthttp-contrib/websocket",
        sha256 = "9d11b15b5b6c4d0508bd6afad73ec4d33a90218068ff8a8283d7ea27c22ba9af",
        strip_prefix = "github.com/fasthttp-contrib/websocket@v0.0.0-20160511215533-1f3b11f56072",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/fasthttp-contrib/websocket/com_github_fasthttp_contrib_websocket-v0.0.0-20160511215533-1f3b11f56072.zip",
        ],
    )
    go_repository(
        name = "com_github_fasthttp_router",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/fasthttp/router",
        sha256 = "5876bc20a22eac5552dd94ff0b06a876269bf9359968d5642a98622a8b2bf033",
        strip_prefix = "github.com/fasthttp/router@v1.4.4",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/fasthttp/router/com_github_fasthttp_router-v1.4.4.zip",
        ],
    )
    go_repository(
        name = "com_github_fatih_color",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/fatih/color",
        sha256 = "33d21fd662beb497f642ffbb42305261004d849fe7028ce037e2b15475b4ecd1",
        strip_prefix = "github.com/fatih/color@v1.9.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/fatih/color/com_github_fatih_color-v1.9.0.zip",
        ],
    )
    go_repository(
        name = "com_github_fatih_structs",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/fatih/structs",
        sha256 = "a361ecc95ad12000c66ee143d26b2aa0a4e5de3b045fd5d18a52564622a59148",
        strip_prefix = "github.com/fatih/structs@v1.1.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/fatih/structs/com_github_fatih_structs-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_felixge_httpsnoop",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/felixge/httpsnoop",
        sha256 = "dc0c57cc57f116de113c320cffcbc61fa437d19a729deec373cce786dd89a029",
        strip_prefix = "github.com/felixge/httpsnoop@v1.0.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/felixge/httpsnoop/com_github_felixge_httpsnoop-v1.0.1.zip",
        ],
    )
    go_repository(
        name = "com_github_flosch_pongo2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/flosch/pongo2",
        sha256 = "814b52f668d2e2528fe9af917506cda4894d22c927283cfb8aaf6857503dfc5a",
        strip_prefix = "github.com/flosch/pongo2@v0.0.0-20190707114632-bbf5a6c351f4",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/flosch/pongo2/com_github_flosch_pongo2-v0.0.0-20190707114632-bbf5a6c351f4.zip",
        ],
    )
    go_repository(
        name = "com_github_fogleman_gg",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/fogleman/gg",
        sha256 = "75b657490d88ac3bad9af07ec4acfe57a995944c50eeb1f167467cf82ff814c5",
        strip_prefix = "github.com/fogleman/gg@v1.2.1-0.20190220221249-0403632d5b90",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/fogleman/gg/com_github_fogleman_gg-v1.2.1-0.20190220221249-0403632d5b90.zip",
        ],
    )
    go_repository(
        name = "com_github_form3tech_oss_jwt_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/form3tech-oss/jwt-go",
        sha256 = "6780fef32d854a318af431efd0c680a1cb4ddc50d36d6b4c239baf381004efae",
        strip_prefix = "github.com/form3tech-oss/jwt-go@v3.2.3+incompatible",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/form3tech-oss/jwt-go/com_github_form3tech_oss_jwt_go-v3.2.3+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_fortytw2_leaktest",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/fortytw2/leaktest",
        sha256 = "867e6d131510751ba6055c51e7746b0056a6b3dcb1a1b2dfdc694251cd7eb8b3",
        strip_prefix = "github.com/fortytw2/leaktest@v1.3.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/fortytw2/leaktest/com_github_fortytw2_leaktest-v1.3.0.zip",
        ],
    )
    go_repository(
        name = "com_github_foxcpp_go_mockdns",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/foxcpp/go-mockdns",
        sha256 = "981c5e71776a97a6de21552728fd2ff04ab9f2057836f133a33cc06c13cbb724",
        strip_prefix = "github.com/foxcpp/go-mockdns@v0.0.0-20201212160233-ede2f9158d15",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/foxcpp/go-mockdns/com_github_foxcpp_go_mockdns-v0.0.0-20201212160233-ede2f9158d15.zip",
        ],
    )
    go_repository(
        name = "com_github_franela_goblin",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/franela/goblin",
        sha256 = "e4ef81939ecb582e5716af6ae8b20ecf899f1351b7c53cb6799edf2a29a43714",
        strip_prefix = "github.com/franela/goblin@v0.0.0-20200105215937-c9ffbefa60db",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/franela/goblin/com_github_franela_goblin-v0.0.0-20200105215937-c9ffbefa60db.zip",
        ],
    )
    go_repository(
        name = "com_github_franela_goreq",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/franela/goreq",
        sha256 = "4f0deb16b3d3acf93ac2e699fe189cf2632fe833bdd5d64f5a54787fed62d19a",
        strip_prefix = "github.com/franela/goreq@v0.0.0-20171204163338-bcd34c9993f8",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/franela/goreq/com_github_franela_goreq-v0.0.0-20171204163338-bcd34c9993f8.zip",
        ],
    )
    go_repository(
        name = "com_github_frankban_quicktest",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/frankban/quicktest",
        sha256 = "28d4b3dc3a66f7c838f7667370df1cd88cc330eac227c55c3c2cd2ecd666c4c5",
        strip_prefix = "github.com/frankban/quicktest@v1.11.3",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/frankban/quicktest/com_github_frankban_quicktest-v1.11.3.zip",
        ],
    )
    go_repository(
        name = "com_github_fraugster_parquet_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/fraugster/parquet-go",
        sha256 = "a9d995d60a99dac8709c614f3d2795ef4d8d79c82991b40fa703006078babf75",
        strip_prefix = "github.com/fraugster/parquet-go@v0.10.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/fraugster/parquet-go/com_github_fraugster_parquet_go-v0.10.0.zip",
        ],
    )
    go_repository(
        name = "com_github_fsnotify_fsnotify",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/fsnotify/fsnotify",
        sha256 = "f38d7e395bc45f08a34e9591c9c4900031f81c1ddc7d761a785cbbb9aaee0db0",
        strip_prefix = "github.com/fsnotify/fsnotify@v1.5.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/fsnotify/fsnotify/com_github_fsnotify_fsnotify-v1.5.1.zip",
        ],
    )
    go_repository(
        name = "com_github_fullsailor_pkcs7",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/fullsailor/pkcs7",
        sha256 = "ba36a8fc855d6eecef329d26f8e82132e38d45d06f79f88d3b0bde6d718c8fb2",
        strip_prefix = "github.com/fullsailor/pkcs7@v0.0.0-20190404230743-d7302db945fa",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/fullsailor/pkcs7/com_github_fullsailor_pkcs7-v0.0.0-20190404230743-d7302db945fa.zip",
        ],
    )
    go_repository(
        name = "com_github_garyburd_redigo",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/garyburd/redigo",
        sha256 = "7ed5f8194388955d2f086c170960cb096ee28d421b32bd12328d5f2a2b0ad488",
        strip_prefix = "github.com/garyburd/redigo@v0.0.0-20150301180006-535138d7bcd7",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/garyburd/redigo/com_github_garyburd_redigo-v0.0.0-20150301180006-535138d7bcd7.zip",
        ],
    )
    go_repository(
        name = "com_github_gavv_httpexpect",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gavv/httpexpect",
        sha256 = "3db05c59a5c70d11b9452727c529be6934ddf8b42f4bfdc3138441055f1529b1",
        strip_prefix = "github.com/gavv/httpexpect@v2.0.0+incompatible",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/gavv/httpexpect/com_github_gavv_httpexpect-v2.0.0+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_geertjohan_go_incremental",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/GeertJohan/go.incremental",
        sha256 = "ce46b3b717f8d2927046bcfb99c6f490b1b547a681e6b23240ac2c2292a891e8",
        strip_prefix = "github.com/GeertJohan/go.incremental@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/GeertJohan/go.incremental/com_github_geertjohan_go_incremental-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_geertjohan_go_rice",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/GeertJohan/go.rice",
        sha256 = "2fc48b9422bf356c18ed3fe32ec52f6a8b87ac168f83d2eed249afaebcc3eeb8",
        strip_prefix = "github.com/GeertJohan/go.rice@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/GeertJohan/go.rice/com_github_geertjohan_go_rice-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_getkin_kin_openapi",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/getkin/kin-openapi",
        sha256 = "e3a00cb5828f8922087a0a74aad06c6177fa2eab44763a19aeec38f7fab7834b",
        strip_prefix = "github.com/getkin/kin-openapi@v0.53.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/getkin/kin-openapi/com_github_getkin_kin_openapi-v0.53.0.zip",
        ],
    )
    go_repository(
        name = "com_github_getsentry_raven_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/getsentry/raven-go",
        sha256 = "eaffe69939612cd05f95e1846b8ddb4043655571be34cdb6412a66b41b6826eb",
        strip_prefix = "github.com/getsentry/raven-go@v0.2.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/getsentry/raven-go/com_github_getsentry_raven_go-v0.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_getsentry_sentry_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/getsentry/sentry-go",
        sha256 = "1d127ea620f897c64de60982882cea34a4bb9c1481a9c9a379226af7cfa454ba",
        strip_prefix = "github.com/getsentry/sentry-go@v0.12.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/getsentry/sentry-go/com_github_getsentry_sentry_go-v0.12.0.zip",
        ],
    )
    go_repository(
        name = "com_github_ghemawat_stream",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ghemawat/stream",
        sha256 = "9c0a42cacc8e22024b58db15127886a6f8ddbcfbf89d4d062bfdc43dc40d80d5",
        strip_prefix = "github.com/ghemawat/stream@v0.0.0-20171120220530-696b145b53b9",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/ghemawat/stream/com_github_ghemawat_stream-v0.0.0-20171120220530-696b145b53b9.zip",
        ],
    )
    go_repository(
        name = "com_github_ghodss_yaml",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ghodss/yaml",
        sha256 = "c3f295d23c02c0b35e4d3b29053586e737cf9642df9615da99c0bda9bbacc624",
        strip_prefix = "github.com/ghodss/yaml@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/ghodss/yaml/com_github_ghodss_yaml-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_gin_contrib_sse",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gin-contrib/sse",
        sha256 = "512c8672f26405172077e764c4817ed8f66edc632d1bed205b5e1b8d282816ab",
        strip_prefix = "github.com/gin-contrib/sse@v0.1.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/gin-contrib/sse/com_github_gin_contrib_sse-v0.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_gin_gonic_gin",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gin-gonic/gin",
        sha256 = "1a69e288dc03d6f5e072767ec38fd24cf44d569168e9cdca65633629248b1571",
        strip_prefix = "github.com/gin-gonic/gin@v1.7.4",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/gin-gonic/gin/com_github_gin_gonic_gin-v1.7.4.zip",
        ],
    )
    go_repository(
        name = "com_github_globalsign_mgo",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/globalsign/mgo",
        sha256 = "c07f09e0c93e6410076edfd621d2decbd361361c536c3e33ba097fa51708f360",
        strip_prefix = "github.com/globalsign/mgo@v0.0.0-20181015135952-eeefdecb41b8",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/globalsign/mgo/com_github_globalsign_mgo-v0.0.0-20181015135952-eeefdecb41b8.zip",
        ],
    )
    go_repository(
        name = "com_github_glycerine_go_unsnap_stream",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/glycerine/go-unsnap-stream",
        sha256 = "9a66d6f9bb1a268f4b824d6fe7adcd55dc17ed504683bdf2dbf67b32028d9b88",
        strip_prefix = "github.com/glycerine/go-unsnap-stream@v0.0.0-20180323001048-9f0cb55181dd",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/glycerine/go-unsnap-stream/com_github_glycerine_go_unsnap_stream-v0.0.0-20180323001048-9f0cb55181dd.zip",
        ],
    )
    go_repository(
        name = "com_github_glycerine_goconvey",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/glycerine/goconvey",
        sha256 = "a4f9edbfc4bc20d04916a73c0b9acf0fc0cdcf16e3c667a1982aac42e56889f9",
        strip_prefix = "github.com/glycerine/goconvey@v0.0.0-20190410193231-58a59202ab31",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/glycerine/goconvey/com_github_glycerine_goconvey-v0.0.0-20190410193231-58a59202ab31.zip",
        ],
    )
    go_repository(
        name = "com_github_go_check_check",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-check/check",
        sha256 = "55ed8316526c1ba82e3e607d17aa98f3b8b0a139ca9c224ee2a3e9e1b582608e",
        strip_prefix = "github.com/go-check/check@v0.0.0-20180628173108-788fd7840127",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/go-check/check/com_github_go_check_check-v0.0.0-20180628173108-788fd7840127.zip",
        ],
    )
    go_repository(
        name = "com_github_go_chi_chi",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-chi/chi",
        sha256 = "b18ec574b5d476df20b181724fdb46180d277a4040dbbbd45e277cc4ce7d04ec",
        strip_prefix = "github.com/go-chi/chi@v4.1.2+incompatible",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/go-chi/chi/com_github_go_chi_chi-v4.1.2+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_go_chi_chi_v5",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-chi/chi/v5",
        sha256 = "742c2be182586a7c77aa0e062b8a427db8ed539222afcdceb1f12ac093a303cd",
        strip_prefix = "github.com/go-chi/chi/v5@v5.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/go-chi/chi/v5/com_github_go_chi_chi_v5-v5.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_go_errors_errors",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-errors/errors",
        sha256 = "bdbee3143e1798eadff4df919479c28ec2d3299a97d445917bc64d6eb6a3b95a",
        strip_prefix = "github.com/go-errors/errors@v1.0.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/go-errors/errors/com_github_go_errors_errors-v1.0.1.zip",
        ],
    )
    go_repository(
        name = "com_github_go_gl_glfw",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-gl/glfw",
        sha256 = "96c694c42e7b866ea8e26dc48b612c4daa8582ce61fdeefbe92c1a4c46163169",
        strip_prefix = "github.com/go-gl/glfw@v0.0.0-20190409004039-e6da0acd62b1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/go-gl/glfw/com_github_go_gl_glfw-v0.0.0-20190409004039-e6da0acd62b1.zip",
        ],
    )
    go_repository(
        name = "com_github_go_gl_glfw_v3_3_glfw",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-gl/glfw/v3.3/glfw",
        sha256 = "2f6a1963397cb7c3df66257a45d75fae860aa9b9eec17825d8101c1e1313da5b",
        strip_prefix = "github.com/go-gl/glfw/v3.3/glfw@v0.0.0-20200222043503-6f7a984d4dc4",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/go-gl/glfw/v3.3/glfw/com_github_go_gl_glfw_v3_3_glfw-v0.0.0-20200222043503-6f7a984d4dc4.zip",
        ],
    )
    go_repository(
        name = "com_github_go_ini_ini",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-ini/ini",
        sha256 = "2ec52de9f1c96133e9f81b8250fdc99ca0729c0d429e318d7c8836b7a6ba5f60",
        strip_prefix = "github.com/go-ini/ini@v1.25.4",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/go-ini/ini/com_github_go_ini_ini-v1.25.4.zip",
        ],
    )
    go_repository(
        name = "com_github_go_kit_kit",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-kit/kit",
        sha256 = "dbdc933092b036483ca332f8c7c13e8b7d029192e79354d4f5a581ef3c364816",
        strip_prefix = "github.com/go-kit/kit@v0.10.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/go-kit/kit/com_github_go_kit_kit-v0.10.0.zip",
        ],
    )
    go_repository(
        name = "com_github_go_kit_log",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-kit/log",
        sha256 = "e0676df7357654a000008dfad3b6b211cba3595f32d3e220edd63a4c9d0d9254",
        strip_prefix = "github.com/go-kit/log@v0.1.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/go-kit/log/com_github_go_kit_log-v0.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_go_logfmt_logfmt",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-logfmt/logfmt",
        sha256 = "9e030cd09b584e59a2f5baaa24cf600520757d732af0f8993cc412dd3086703a",
        strip_prefix = "github.com/go-logfmt/logfmt@v0.5.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/go-logfmt/logfmt/com_github_go_logfmt_logfmt-v0.5.1.zip",
        ],
    )
    go_repository(
        name = "com_github_go_logr_logr",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-logr/logr",
        sha256 = "be0f7eff89f0e40ff70715d864e0adbfc2be25b37d161c1ba71acd051c5716ea",
        strip_prefix = "github.com/go-logr/logr@v0.4.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/go-logr/logr/com_github_go_logr_logr-v0.4.0.zip",
        ],
    )
    go_repository(
        name = "com_github_go_martini_martini",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-martini/martini",
        sha256 = "0561a4dadd68dbc1b38c09ed95bbfc5073b0a7708b9a787d38533ebd48040ec2",
        strip_prefix = "github.com/go-martini/martini@v0.0.0-20170121215854-22fa46961aab",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/go-martini/martini/com_github_go_martini_martini-v0.0.0-20170121215854-22fa46961aab.zip",
        ],
    )
    go_repository(
        name = "com_github_go_ole_go_ole",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-ole/go-ole",
        sha256 = "95b192df81ca16f0fb7d2d98ff6596d70256d73e49e899c55fabd511fd6768ef",
        strip_prefix = "github.com/go-ole/go-ole@v1.2.6",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/go-ole/go-ole/com_github_go_ole_go_ole-v1.2.6.zip",
        ],
    )
    go_repository(
        name = "com_github_go_openapi_analysis",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-openapi/analysis",
        sha256 = "b44ec427451be7514c03db1064b0955cbf93aa4c1db32b90319ad5f7989384a8",
        strip_prefix = "github.com/go-openapi/analysis@v0.20.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/go-openapi/analysis/com_github_go_openapi_analysis-v0.20.0.zip",
        ],
    )
    go_repository(
        name = "com_github_go_openapi_errors",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-openapi/errors",
        sha256 = "2bd57d937226751b2896ab022d26f6e7b7f866cb79f118f301f1939d94346152",
        strip_prefix = "github.com/go-openapi/errors@v0.20.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/go-openapi/errors/com_github_go_openapi_errors-v0.20.0.zip",
        ],
    )
    go_repository(
        name = "com_github_go_openapi_inflect",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-openapi/inflect",
        sha256 = "fbcca36e347a2f560f50ac1c9c63f7d6cd97c8dff9800f08f370b5ce09b77c57",
        strip_prefix = "github.com/go-openapi/inflect@v0.19.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/go-openapi/inflect/com_github_go_openapi_inflect-v0.19.0.zip",
        ],
    )
    go_repository(
        name = "com_github_go_openapi_jsonpointer",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-openapi/jsonpointer",
        sha256 = "e72055b331770686985b6a99a7e1ba6c71701175ab0ffb2bfa355665be6c5c85",
        strip_prefix = "github.com/go-openapi/jsonpointer@v0.19.5",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/go-openapi/jsonpointer/com_github_go_openapi_jsonpointer-v0.19.5.zip",
        ],
    )
    go_repository(
        name = "com_github_go_openapi_jsonreference",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-openapi/jsonreference",
        sha256 = "807e01410541971cb5620ecc98a91162f8e00e5a5c7087cf6ff43a2e1cfa2c8b",
        strip_prefix = "github.com/go-openapi/jsonreference@v0.19.5",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/go-openapi/jsonreference/com_github_go_openapi_jsonreference-v0.19.5.zip",
        ],
    )
    go_repository(
        name = "com_github_go_openapi_loads",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-openapi/loads",
        sha256 = "8c7ba6dc8b7c3346cafa3551ac3b459021e6003d62480c7d1a202a2b435c673d",
        strip_prefix = "github.com/go-openapi/loads@v0.20.2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/go-openapi/loads/com_github_go_openapi_loads-v0.20.2.zip",
        ],
    )
    go_repository(
        name = "com_github_go_openapi_runtime",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-openapi/runtime",
        sha256 = "25996438d5f0bac5619e23cad1279d799a521366f3ee68039ca1e16fb9385489",
        strip_prefix = "github.com/go-openapi/runtime@v0.19.29",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/go-openapi/runtime/com_github_go_openapi_runtime-v0.19.29.zip",
        ],
    )
    go_repository(
        name = "com_github_go_openapi_spec",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-openapi/spec",
        sha256 = "eb5bb0a603d782c0ef1040dadfcfae7a5e04c08f33580c8718d70d0514d9e0b3",
        strip_prefix = "github.com/go-openapi/spec@v0.20.3",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/go-openapi/spec/com_github_go_openapi_spec-v0.20.3.zip",
        ],
    )
    go_repository(
        name = "com_github_go_openapi_strfmt",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-openapi/strfmt",
        sha256 = "1145dde362d67d82caeb344b334d31f9b07957a455bf31d61237b6ada7273db2",
        strip_prefix = "github.com/go-openapi/strfmt@v0.20.2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/go-openapi/strfmt/com_github_go_openapi_strfmt-v0.20.2.zip",
        ],
    )
    go_repository(
        name = "com_github_go_openapi_swag",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-openapi/swag",
        sha256 = "58785e99946002b10858ccfc9fd318691c15e74b87a107a1ba2509886475b4a0",
        strip_prefix = "github.com/go-openapi/swag@v0.19.15",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/go-openapi/swag/com_github_go_openapi_swag-v0.19.15.zip",
        ],
    )
    go_repository(
        name = "com_github_go_openapi_validate",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-openapi/validate",
        sha256 = "03c6a6579b340f4b579e8ca798bb41663c567118b933eaeb6602c1fa10ad3732",
        strip_prefix = "github.com/go-openapi/validate@v0.20.2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/go-openapi/validate/com_github_go_openapi_validate-v0.20.2.zip",
        ],
    )
    go_repository(
        name = "com_github_go_playground_assert_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-playground/assert/v2",
        sha256 = "46db6b505ff9818c50f924c6aec007dbbc4d86267fdf2d470ef4be12a40fd4cb",
        strip_prefix = "github.com/go-playground/assert/v2@v2.0.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/go-playground/assert/v2/com_github_go_playground_assert_v2-v2.0.1.zip",
        ],
    )
    go_repository(
        name = "com_github_go_playground_locales",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-playground/locales",
        sha256 = "9c4b65273e135b1bdb9bafc7c0b5180a6c5936f54edecbc8807c57a9d107c6b9",
        strip_prefix = "github.com/go-playground/locales@v0.13.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/go-playground/locales/com_github_go_playground_locales-v0.13.0.zip",
        ],
    )
    go_repository(
        name = "com_github_go_playground_universal_translator",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-playground/universal-translator",
        sha256 = "cfd4a4f76c27794e7b925fb672ee7576d89aa5e22efdc7fec4fc80b467abe06f",
        strip_prefix = "github.com/go-playground/universal-translator@v0.17.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/go-playground/universal-translator/com_github_go_playground_universal_translator-v0.17.0.zip",
        ],
    )
    go_repository(
        name = "com_github_go_playground_validator_v10",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-playground/validator/v10",
        sha256 = "79cba8341d27fc1e1e2528c01b4e01b7d43ccfef2c06ad1cac73b2647b5302aa",
        strip_prefix = "github.com/go-playground/validator/v10@v10.4.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/go-playground/validator/v10/com_github_go_playground_validator_v10-v10.4.1.zip",
        ],
    )
    go_repository(
        name = "com_github_go_resty_resty_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-resty/resty/v2",
        sha256 = "43ac9ff350b1fb126a0f9f9341eaf4c281d661341e22efd7198f3ccd7a009f2a",
        strip_prefix = "github.com/go-resty/resty/v2@v2.1.1-0.20191201195748-d7b97669fe48",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/go-resty/resty/v2/com_github_go_resty_resty_v2-v2.1.1-0.20191201195748-d7b97669fe48.zip",
        ],
    )
    go_repository(
        name = "com_github_go_sql_driver_mysql",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-sql-driver/mysql",
        sha256 = "07f052b8f3fb4c1bb8caaf2fdb95c0f13e4261c72494a16900728af9f2eee706",
        strip_prefix = "github.com/go-sql-driver/mysql@v1.6.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/go-sql-driver/mysql/com_github_go_sql_driver_mysql-v1.6.0.zip",
        ],
    )
    go_repository(
        name = "com_github_go_stack_stack",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-stack/stack",
        sha256 = "78c2667c710f811307038634ffa43af442619acfeaf1efb593aa4e0ded9df48f",
        strip_prefix = "github.com/go-stack/stack@v1.8.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/go-stack/stack/com_github_go_stack_stack-v1.8.0.zip",
        ],
    )
    go_repository(
        name = "com_github_go_swagger_go_swagger",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-swagger/go-swagger",
        sha256 = "465a017241a1a533766d214479ec330db4191ad796341fbb9f97a498a63ada4d",
        strip_prefix = "github.com/go-swagger/go-swagger@v0.26.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/go-swagger/go-swagger/com_github_go_swagger_go_swagger-v0.26.1.zip",
        ],
    )
    go_repository(
        name = "com_github_go_swagger_scan_repo_boundary",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-swagger/scan-repo-boundary",
        sha256 = "51aed4b67bce9d988d64ca6be9de2169f709a29d5ea83e78ffb1c2432b346ec6",
        strip_prefix = "github.com/go-swagger/scan-repo-boundary@v0.0.0-20180623220736-973b3573c013",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/go-swagger/scan-repo-boundary/com_github_go_swagger_scan_repo_boundary-v0.0.0-20180623220736-973b3573c013.zip",
        ],
    )
    go_repository(
        name = "com_github_go_test_deep",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-test/deep",
        sha256 = "9d1f4cfdb8e02af475903ea172741e4298661ca327a0dcf6c5b3e4d9d93b8bf0",
        strip_prefix = "github.com/go-test/deep@v1.0.4",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/go-test/deep/com_github_go_test_deep-v1.0.4.zip",
        ],
    )
    go_repository(
        name = "com_github_go_zookeeper_zk",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/go-zookeeper/zk",
        sha256 = "6f91aecf62ffb4d7468eb14372d1e43b8620eb341964b5001e85151b46caed4f",
        strip_prefix = "github.com/go-zookeeper/zk@v1.0.2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/go-zookeeper/zk/com_github_go_zookeeper_zk-v1.0.2.zip",
        ],
    )
    go_repository(
        name = "com_github_gobuffalo_attrs",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gobuffalo/attrs",
        sha256 = "8fa6e4f71f4f4ce772f2e7b5dd3975f0c079ab1b81f1fabdc80356d3a56b834c",
        strip_prefix = "github.com/gobuffalo/attrs@v0.0.0-20190224210810-a9411de4debd",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/gobuffalo/attrs/com_github_gobuffalo_attrs-v0.0.0-20190224210810-a9411de4debd.zip",
        ],
    )
    go_repository(
        name = "com_github_gobuffalo_depgen",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gobuffalo/depgen",
        sha256 = "b86c4272426beb18fc37acdaae2cda504fbfe41304d214c5f09070ec0d98390b",
        strip_prefix = "github.com/gobuffalo/depgen@v0.1.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/gobuffalo/depgen/com_github_gobuffalo_depgen-v0.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_gobuffalo_envy",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gobuffalo/envy",
        sha256 = "46f9f290cd8415de5779b76cc18e5622ab655c2ac2eab85f3b671e7d8cea929c",
        strip_prefix = "github.com/gobuffalo/envy@v1.7.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/gobuffalo/envy/com_github_gobuffalo_envy-v1.7.0.zip",
        ],
    )
    go_repository(
        name = "com_github_gobuffalo_flect",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gobuffalo/flect",
        sha256 = "48ef00b7e5018cee90fa148321b2427f2e0e274682c2f8d1b77587e24d6ee69a",
        strip_prefix = "github.com/gobuffalo/flect@v0.1.3",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/gobuffalo/flect/com_github_gobuffalo_flect-v0.1.3.zip",
        ],
    )
    go_repository(
        name = "com_github_gobuffalo_genny",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gobuffalo/genny",
        sha256 = "71c6f6aad1c31e965cbdb506d6ed224a428ecd26a1dee579df30fe7ed7165432",
        strip_prefix = "github.com/gobuffalo/genny@v0.1.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/gobuffalo/genny/com_github_gobuffalo_genny-v0.1.1.zip",
        ],
    )
    go_repository(
        name = "com_github_gobuffalo_gitgen",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gobuffalo/gitgen",
        sha256 = "c79975f91dd2fd691d70e29678034eb2dc94b5da2f01b0790a919de9d2a632ac",
        strip_prefix = "github.com/gobuffalo/gitgen@v0.0.0-20190315122116-cc086187d211",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/gobuffalo/gitgen/com_github_gobuffalo_gitgen-v0.0.0-20190315122116-cc086187d211.zip",
        ],
    )
    go_repository(
        name = "com_github_gobuffalo_gogen",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gobuffalo/gogen",
        sha256 = "753056c63f43accfe43c446e9bc11e156c079fdbda0589103c13f0e831072bc2",
        strip_prefix = "github.com/gobuffalo/gogen@v0.1.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/gobuffalo/gogen/com_github_gobuffalo_gogen-v0.1.1.zip",
        ],
    )
    go_repository(
        name = "com_github_gobuffalo_logger",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gobuffalo/logger",
        sha256 = "0ef4ed0706260582fa0c90278400fcdcff9be3b475f8a19d103c9be9de0e4c73",
        strip_prefix = "github.com/gobuffalo/logger@v0.0.0-20190315122211-86e12af44bc2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/gobuffalo/logger/com_github_gobuffalo_logger-v0.0.0-20190315122211-86e12af44bc2.zip",
        ],
    )
    go_repository(
        name = "com_github_gobuffalo_mapi",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gobuffalo/mapi",
        sha256 = "44a37e32207496271cb4ede9649534859de91f0d2ea7638ccd382bf61120e438",
        strip_prefix = "github.com/gobuffalo/mapi@v1.0.2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/gobuffalo/mapi/com_github_gobuffalo_mapi-v1.0.2.zip",
        ],
    )
    go_repository(
        name = "com_github_gobuffalo_packd",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gobuffalo/packd",
        sha256 = "9a7e7f84ecd9cc9f4aa1835eaae9d99cdf3c35d2ca4af799b61290cdc301de1e",
        strip_prefix = "github.com/gobuffalo/packd@v0.1.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/gobuffalo/packd/com_github_gobuffalo_packd-v0.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_gobuffalo_packr_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gobuffalo/packr/v2",
        sha256 = "0170bbcef28f04575fc8cefa2715e833ccb4e158ef5e0d99c07184ce100c146a",
        strip_prefix = "github.com/gobuffalo/packr/v2@v2.2.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/gobuffalo/packr/v2/com_github_gobuffalo_packr_v2-v2.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_gobuffalo_syncx",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gobuffalo/syncx",
        sha256 = "ad9a571b43d72ecce24b8bed85636091710f22d8b06051e1e19ef2051f3e00da",
        strip_prefix = "github.com/gobuffalo/syncx@v0.0.0-20190224160051-33c29581e754",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/gobuffalo/syncx/com_github_gobuffalo_syncx-v0.0.0-20190224160051-33c29581e754.zip",
        ],
    )
    go_repository(
        name = "com_github_gobwas_httphead",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gobwas/httphead",
        sha256 = "5a43ed4a7cd2b063b634f0df5311c0dfa6576683bfc1339f2c5b1b1127fc392b",
        strip_prefix = "github.com/gobwas/httphead@v0.0.0-20180130184737-2c6c146eadee",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/gobwas/httphead/com_github_gobwas_httphead-v0.0.0-20180130184737-2c6c146eadee.zip",
        ],
    )
    go_repository(
        name = "com_github_gobwas_pool",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gobwas/pool",
        sha256 = "52604b1456b92bb310461167a3e6515562f0f4214f01ed6440e3105f78be188f",
        strip_prefix = "github.com/gobwas/pool@v0.2.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/gobwas/pool/com_github_gobwas_pool-v0.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_gobwas_ws",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gobwas/ws",
        sha256 = "f9e5c26e83278f19958c68be7b76ad6711c806b6dae766fad7692d2af867bedd",
        strip_prefix = "github.com/gobwas/ws@v1.0.2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/gobwas/ws/com_github_gobwas_ws-v1.0.2.zip",
        ],
    )
    go_repository(
        name = "com_github_gocql_gocql",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gocql/gocql",
        sha256 = "40095e622040db188068b66258742938a5b083f6696b46b4a40c0391f0dafcec",
        strip_prefix = "github.com/gocql/gocql@v0.0.0-20200228163523-cd4b606dd2fb",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/gocql/gocql/com_github_gocql_gocql-v0.0.0-20200228163523-cd4b606dd2fb.zip",
        ],
    )
    go_repository(
        name = "com_github_godbus_dbus",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/godbus/dbus",
        sha256 = "4949c4ae117d0c306a28bb05a7da82be3c4a76676939e6ec60540335c8eff90d",
        strip_prefix = "github.com/godbus/dbus@v0.0.0-20190422162347-ade71ed3457e",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/godbus/dbus/com_github_godbus_dbus-v0.0.0-20190422162347-ade71ed3457e.zip",
        ],
    )
    go_repository(
        name = "com_github_godbus_dbus_v5",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/godbus/dbus/v5",
        sha256 = "23a23f08bea48e6e49a46a4015b64adbb1692dc6ddf0d83c2f0c2027cb8e31c8",
        strip_prefix = "github.com/godbus/dbus/v5@v5.0.4",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/godbus/dbus/v5/com_github_godbus_dbus_v5-v5.0.4.zip",
        ],
    )
    go_repository(
        name = "com_github_gofrs_flock",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gofrs/flock",
        sha256 = "9ace5b0a05672937904fba1fcb86cb45e7f701e508faeb5f612e243340351dfa",
        strip_prefix = "github.com/gofrs/flock@v0.8.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/gofrs/flock/com_github_gofrs_flock-v0.8.1.zip",
        ],
    )
    go_repository(
        name = "com_github_gofrs_uuid",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gofrs/uuid",
        sha256 = "8cadafda9aea197d34898d6945692173ac1d8abf3b559c4e5a59a577fc60f55e",
        strip_prefix = "github.com/gofrs/uuid@v4.0.0+incompatible",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/gofrs/uuid/com_github_gofrs_uuid-v4.0.0+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_gogo_googleapis",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gogo/googleapis",
        sha256 = "34110f4fe52daa66bf190e6c5be70e2e384ceca3cb1bce3e20f32994ede5a141",
        strip_prefix = "github.com/gogo/googleapis@v1.4.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/gogo/googleapis/com_github_gogo_googleapis-v1.4.1.zip",
        ],
    )
    go_repository(
        name = "com_github_gogo_protobuf",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gogo/protobuf",
        patch_args = ["-p1"],
        patches = [
            "@cockroach//build/patches:com_github_gogo_protobuf.patch",
        ],
        sha256 = "dd2b73f163c8183941626360196c8f844addd95423d341a0412e1b22d0104ff7",
        strip_prefix = "github.com/gogo/protobuf@v1.3.2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/gogo/protobuf/com_github_gogo_protobuf-v1.3.2.zip",
        ],
    )
    go_repository(
        name = "com_github_gogo_status",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gogo/status",
        sha256 = "c042d3555c9f490a75d44ad4c3dff367f9512e6d189252f8765f4837b11b12b1",
        strip_prefix = "github.com/gogo/status@v1.1.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/gogo/status/com_github_gogo_status-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_golang_commonmark_html",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golang-commonmark/html",
        sha256 = "7f6687aa6ce780217750ee31711506bc919e140d205fd1693bfb5c81b5ab5a5d",
        strip_prefix = "github.com/golang-commonmark/html@v0.0.0-20180910111043-7d7c804e1d46",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/golang-commonmark/html/com_github_golang_commonmark_html-v0.0.0-20180910111043-7d7c804e1d46.zip",
        ],
    )
    go_repository(
        name = "com_github_golang_commonmark_linkify",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golang-commonmark/linkify",
        sha256 = "03d5797db955119a0d607167509727a60884ba2ebf7590732a262cd153ca14a9",
        strip_prefix = "github.com/golang-commonmark/linkify@v0.0.0-20180910111149-f05efb453a0e",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/golang-commonmark/linkify/com_github_golang_commonmark_linkify-v0.0.0-20180910111149-f05efb453a0e.zip",
        ],
    )
    go_repository(
        name = "com_github_golang_commonmark_markdown",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golang-commonmark/markdown",
        sha256 = "86887f9a455d8c2a48dd7b2d69352cde7730c35bab2c9edfd347886a106f5e89",
        strip_prefix = "github.com/golang-commonmark/markdown@v0.0.0-20180910011815-a8f139058164",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/golang-commonmark/markdown/com_github_golang_commonmark_markdown-v0.0.0-20180910011815-a8f139058164.zip",
        ],
    )
    go_repository(
        name = "com_github_golang_commonmark_mdurl",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golang-commonmark/mdurl",
        sha256 = "b7386081771d71200f34972369cf4bcea3eb1dc6dcb1cf3906b692c586c22557",
        strip_prefix = "github.com/golang-commonmark/mdurl@v0.0.0-20180910110917-8d018c6567d6",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/golang-commonmark/mdurl/com_github_golang_commonmark_mdurl-v0.0.0-20180910110917-8d018c6567d6.zip",
        ],
    )
    go_repository(
        name = "com_github_golang_commonmark_puny",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golang-commonmark/puny",
        sha256 = "1296aef61f597df70d851197dacc258fc8f3e80d0a7180e7470bd1a2f2dcbd08",
        strip_prefix = "github.com/golang-commonmark/puny@v0.0.0-20180910110745-050be392d8b8",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/golang-commonmark/puny/com_github_golang_commonmark_puny-v0.0.0-20180910110745-050be392d8b8.zip",
        ],
    )
    go_repository(
        name = "com_github_golang_freetype",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golang/freetype",
        sha256 = "cdcb9e6a14933dcbf167b44dcd5083fc6a2e52c4fae8fb79747c691efeb7d84e",
        strip_prefix = "github.com/golang/freetype@v0.0.0-20170609003504-e2365dfdc4a0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/golang/freetype/com_github_golang_freetype-v0.0.0-20170609003504-e2365dfdc4a0.zip",
        ],
    )
    go_repository(
        name = "com_github_golang_geo",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golang/geo",
        sha256 = "f19bf757263775cf21790a1821cc8ac1b853fe41dd2499d9e6a434f01bd12332",
        strip_prefix = "github.com/golang/geo@v0.0.0-20200319012246-673a6f80352d",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/golang/geo/com_github_golang_geo-v0.0.0-20200319012246-673a6f80352d.zip",
        ],
    )
    go_repository(
        name = "com_github_golang_glog",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golang/glog",
        sha256 = "677f7912d32cb417cfb59cbab9b9e924224a53679aa384b0526df2eda96d8775",
        strip_prefix = "github.com/golang/glog@v0.0.0-20210429001901-424d2337a529",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/golang/glog/com_github_golang_glog-v0.0.0-20210429001901-424d2337a529.zip",
        ],
    )
    go_repository(
        name = "com_github_golang_groupcache",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golang/groupcache",
        sha256 = "b27034e8fc013627543e1ad098cfc65329f2896df3da5cf3266cc9166f93f3a5",
        strip_prefix = "github.com/golang/groupcache@v0.0.0-20210331224755-41bb18bfe9da",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/golang/groupcache/com_github_golang_groupcache-v0.0.0-20210331224755-41bb18bfe9da.zip",
        ],
    )
    go_repository(
        name = "com_github_golang_jwt_jwt",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golang-jwt/jwt",
        sha256 = "28d6dd7cc77d0a960699196e9c2170731f65d624d675888d2ababe7e8a422955",
        strip_prefix = "github.com/golang-jwt/jwt@v3.2.2+incompatible",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/golang-jwt/jwt/com_github_golang_jwt_jwt-v3.2.2+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_golang_jwt_jwt_v4",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golang-jwt/jwt/v4",
        sha256 = "51d00fb75dfa0f4ff7b5597d842c43f53573eca8c58f8bc89b229b9b4853a4ab",
        strip_prefix = "github.com/golang-jwt/jwt/v4@v4.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/golang-jwt/jwt/v4/com_github_golang_jwt_jwt_v4-v4.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_golang_mock",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golang/mock",
        sha256 = "fa25916b546f90da49418f436e3a61e4c5dae898cf3c82b0007b5a6fab74261b",
        strip_prefix = "github.com/golang/mock@v1.6.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/golang/mock/com_github_golang_mock-v1.6.0.zip",
        ],
    )
    go_repository(
        name = "com_github_golang_protobuf",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golang/protobuf",
        patch_args = ["-p1"],
        patches = [
            "@cockroach//build/patches:com_github_golang_protobuf.patch",
        ],
        sha256 = "5d1c817bebc1202ab3b42a418e584e0008e8027baf212ce69c2ae3e9e7b8c64b",
        strip_prefix = "github.com/golang/protobuf@v1.5.2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/golang/protobuf/com_github_golang_protobuf-v1.5.2.zip",
        ],
    )
    go_repository(
        name = "com_github_golang_snappy",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golang/snappy",
        sha256 = "ea4545ca44ee990554094df6de440386a440a5bd99106e048939409d63beb423",
        strip_prefix = "github.com/golang/snappy@v0.0.4",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/golang/snappy/com_github_golang_snappy-v0.0.4.zip",
        ],
    )
    go_repository(
        name = "com_github_golang_sql_civil",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golang-sql/civil",
        sha256 = "22fcd1e01cabf6ec75c6b6c8e443de029611c9dd5cc4673818d52dac465ac688",
        strip_prefix = "github.com/golang-sql/civil@v0.0.0-20190719163853-cb61b32ac6fe",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/golang-sql/civil/com_github_golang_sql_civil-v0.0.0-20190719163853-cb61b32ac6fe.zip",
        ],
    )
    go_repository(
        name = "com_github_golangci_lint_1",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/golangci/lint-1",
        sha256 = "2806ffd1a35b26a29b4cea86eb5ae421636b317e33e261fc1c20f9cf8fec2db5",
        strip_prefix = "github.com/golangci/lint-1@v0.0.0-20181222135242-d2cdd8c08219",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/golangci/lint-1/com_github_golangci_lint_1-v0.0.0-20181222135242-d2cdd8c08219.zip",
        ],
    )
    go_repository(
        name = "com_github_gomodule_redigo",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gomodule/redigo",
        sha256 = "f665942b590c65e87284d681ea2784d0b9873c644756f4716a9972dc0d8e804e",
        strip_prefix = "github.com/gomodule/redigo@v1.7.1-0.20190724094224-574c33c3df38",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/gomodule/redigo/com_github_gomodule_redigo-v1.7.1-0.20190724094224-574c33c3df38.zip",
        ],
    )
    go_repository(
        name = "com_github_google_btree",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/google/btree",
        sha256 = "9b9f66ca4eb36bb1867b5ff9134fb2eb9fe9717d44e28836f2e977f9c03b4128",
        strip_prefix = "github.com/google/btree@v1.0.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/google/btree/com_github_google_btree-v1.0.1.zip",
        ],
    )
    go_repository(
        name = "com_github_google_flatbuffers",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/google/flatbuffers",
        sha256 = "0afae89460c24ba56d23f35573ba49713eef3230aeb8c14bf90c771f97efa695",
        strip_prefix = "github.com/google/flatbuffers@v2.0.0+incompatible",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/google/flatbuffers/com_github_google_flatbuffers-v2.0.0+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_google_go_cmp",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/google/go-cmp",
        sha256 = "32c6bb53a2f214fecd43ca0a436758488d088a9ac23e391ef4b502eda0591147",
        strip_prefix = "github.com/google/go-cmp@v0.5.6",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/google/go-cmp/com_github_google_go_cmp-v0.5.6.zip",
        ],
    )
    go_repository(
        name = "com_github_google_go_github",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/google/go-github",
        sha256 = "9831222a466bec73a21627e0c3525da9cadd969468e31d10ecae8580b0568d0e",
        strip_prefix = "github.com/google/go-github@v17.0.0+incompatible",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/google/go-github/com_github_google_go_github-v17.0.0+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_google_go_github_v27",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/google/go-github/v27",
        sha256 = "c0bb2e2b9d8b610fd1d4b9fa8a3636a5337f19aecec33e76aecbf32ae4e192bb",
        strip_prefix = "github.com/google/go-github/v27@v27.0.4",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/google/go-github/v27/com_github_google_go_github_v27-v27.0.4.zip",
        ],
    )
    go_repository(
        name = "com_github_google_go_querystring",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/google/go-querystring",
        sha256 = "1c0a0b81b921ee270e47e05cf0bf8df4475de850671e553c07740849068d4f9f",
        strip_prefix = "github.com/google/go-querystring@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/google/go-querystring/com_github_google_go_querystring-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_google_gofuzz",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/google/gofuzz",
        sha256 = "5c41453c0e2df199e899097e95d75f19fdda591e977233f47fab15b84e352b04",
        strip_prefix = "github.com/google/gofuzz@v1.1.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/google/gofuzz/com_github_google_gofuzz-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_google_martian",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/google/martian",
        sha256 = "5bdd2ebd37dda1c0cf786db27707966c8624b288641da704b0e31c96b393ce70",
        strip_prefix = "github.com/google/martian@v2.1.0+incompatible",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/google/martian/com_github_google_martian-v2.1.0+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_google_martian_v3",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/google/martian/v3",
        sha256 = "d1450c8806be91afda7d68d9eee117de58aa04e89d01dbd328455c294379c6c0",
        strip_prefix = "github.com/google/martian/v3@v3.2.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/google/martian/v3/com_github_google_martian_v3-v3.2.1.zip",
        ],
    )
    go_repository(
        name = "com_github_google_pprof",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/google/pprof",
        sha256 = "ef4516ff75385445e0afd1bfd452506cc8f791fe3bf71c9f5bcff0814f301c2e",
        strip_prefix = "github.com/google/pprof@v0.0.0-20210827144239-02619b876842",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/google/pprof/com_github_google_pprof-v0.0.0-20210827144239-02619b876842.zip",
        ],
    )
    go_repository(
        name = "com_github_google_renameio",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/google/renameio",
        sha256 = "b8510bb34078691a20b8e4902d371afe0eb171b2daf953f67cb3960d1926ccf3",
        strip_prefix = "github.com/google/renameio@v0.1.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/google/renameio/com_github_google_renameio-v0.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_google_shlex",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/google/shlex",
        sha256 = "1bf70bdb4c889b47b1976370832da79060c36cad282f278f279603200623775c",
        strip_prefix = "github.com/google/shlex@v0.0.0-20191202100458-e7afc7fbc510",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/google/shlex/com_github_google_shlex-v0.0.0-20191202100458-e7afc7fbc510.zip",
        ],
    )
    go_repository(
        name = "com_github_google_skylark",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/google/skylark",
        sha256 = "401bbeea49fb3939c4a7246da4154d411d4612881b510657cae4a5bfa05f8c21",
        strip_prefix = "github.com/google/skylark@v0.0.0-20181101142754-a5f7082aabed",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/google/skylark/com_github_google_skylark-v0.0.0-20181101142754-a5f7082aabed.zip",
        ],
    )
    go_repository(
        name = "com_github_google_uuid",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/google/uuid",
        sha256 = "0a5fcc05ea492afeaca984a012485f6a15e2259b32f1206d6f36a88c88afc607",
        strip_prefix = "github.com/google/uuid@v1.3.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/google/uuid/com_github_google_uuid-v1.3.0.zip",
        ],
    )
    go_repository(
        name = "com_github_googleapis_gax_go_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/googleapis/gax-go/v2",
        sha256 = "b8f7a0fd38ce167551e21a0c29c9bb3ebb4ccf179ecebe6c5b2504ce7f1baddd",
        strip_prefix = "github.com/googleapis/gax-go/v2@v2.1.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/googleapis/gax-go/v2/com_github_googleapis_gax_go_v2-v2.1.1.zip",
        ],
    )
    go_repository(
        name = "com_github_googleapis_gnostic",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/googleapis/gnostic",
        sha256 = "50fab68c592e8c8038b48b3c7b68d8f56297a58da28194ace2a43a9866c4025b",
        strip_prefix = "github.com/googleapis/gnostic@v0.5.5",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/googleapis/gnostic/com_github_googleapis_gnostic-v0.5.5.zip",
        ],
    )
    go_repository(
        name = "com_github_gophercloud_gophercloud",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gophercloud/gophercloud",
        sha256 = "aca35069a500cfa1694c16908985bfc88bd52e455a2ad3d901399352240c7424",
        strip_prefix = "github.com/gophercloud/gophercloud@v0.20.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/gophercloud/gophercloud/com_github_gophercloud_gophercloud-v0.20.0.zip",
        ],
    )
    go_repository(
        name = "com_github_gopherjs_gopherjs",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gopherjs/gopherjs",
        sha256 = "096bf06513b3607377446f9864eab5099652c0985c1614b7e89ca92cd8989178",
        strip_prefix = "github.com/gopherjs/gopherjs@v0.0.0-20181103185306-d547d1d9531e",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/gopherjs/gopherjs/com_github_gopherjs_gopherjs-v0.0.0-20181103185306-d547d1d9531e.zip",
        ],
    )
    go_repository(
        name = "com_github_gordonklaus_ineffassign",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gordonklaus/ineffassign",
        sha256 = "ca53f10fdaec7dd8a835c69dee8fe2c0189cb6da38cb0a601310d7e756f15d09",
        strip_prefix = "github.com/gordonklaus/ineffassign@v0.0.0-20200309095847-7953dde2c7bf",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/gordonklaus/ineffassign/com_github_gordonklaus_ineffassign-v0.0.0-20200309095847-7953dde2c7bf.zip",
        ],
    )
    go_repository(
        name = "com_github_gorilla_context",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gorilla/context",
        sha256 = "4ec8e01fe741a931edeebdee9348ffb49b5cc565ca245551d0d20b67062e6f0b",
        strip_prefix = "github.com/gorilla/context@v1.1.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/gorilla/context/com_github_gorilla_context-v1.1.1.zip",
        ],
    )
    go_repository(
        name = "com_github_gorilla_handlers",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gorilla/handlers",
        sha256 = "700cb5572cef0b4c251fc63550d3a656d53b91cec845f19b6a16bdbc6795beec",
        strip_prefix = "github.com/gorilla/handlers@v1.5.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/gorilla/handlers/com_github_gorilla_handlers-v1.5.1.zip",
        ],
    )
    go_repository(
        name = "com_github_gorilla_mux",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gorilla/mux",
        sha256 = "7641911e00af9c91f089868333067c9cb9a58702d2c9ea821ee374940091c385",
        strip_prefix = "github.com/gorilla/mux@v1.8.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/gorilla/mux/com_github_gorilla_mux-v1.8.0.zip",
        ],
    )
    go_repository(
        name = "com_github_gorilla_securecookie",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gorilla/securecookie",
        sha256 = "dd83a4230e11568159756bbea4d343c88df0cd1415bbbc7cd5badad6cd2ed903",
        strip_prefix = "github.com/gorilla/securecookie@v1.1.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/gorilla/securecookie/com_github_gorilla_securecookie-v1.1.1.zip",
        ],
    )
    go_repository(
        name = "com_github_gorilla_sessions",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gorilla/sessions",
        sha256 = "2c6aeebfef8062537fd7778067e5e99d4c13f79ac63114e905c97040a6e6b523",
        strip_prefix = "github.com/gorilla/sessions@v1.2.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/gorilla/sessions/com_github_gorilla_sessions-v1.2.1.zip",
        ],
    )
    go_repository(
        name = "com_github_gorilla_websocket",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gorilla/websocket",
        sha256 = "d0d1728deaa06dac190bf4964c9c6395923403eae337cb3305d6dda18ef07337",
        strip_prefix = "github.com/gorilla/websocket@v1.4.2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/gorilla/websocket/com_github_gorilla_websocket-v1.4.2.zip",
        ],
    )
    go_repository(
        name = "com_github_goware_modvendor",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/goware/modvendor",
        sha256 = "0782a6fdb917d025c21b92e06799c1a29b050a92c9711da10babc7af57c92b4e",
        strip_prefix = "github.com/goware/modvendor@v0.5.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/goware/modvendor/com_github_goware_modvendor-v0.5.0.zip",
        ],
    )
    go_repository(
        name = "com_github_gregjones_httpcache",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/gregjones/httpcache",
        sha256 = "2930b770ec363219f32947ec67b36ccb629058849618c82bc4d891f856df49e1",
        strip_prefix = "github.com/gregjones/httpcache@v0.0.0-20180305231024-9cad4c3443a7",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/gregjones/httpcache/com_github_gregjones_httpcache-v0.0.0-20180305231024-9cad4c3443a7.zip",
        ],
    )
    go_repository(
        name = "com_github_grpc_ecosystem_go_grpc_middleware",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/grpc-ecosystem/go-grpc-middleware",
        sha256 = "29612c2745026cd4dcd312797fe62f88c125c89d2914513cbc37efcf0bb4da54",
        strip_prefix = "github.com/grpc-ecosystem/go-grpc-middleware@v1.2.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/grpc-ecosystem/go-grpc-middleware/com_github_grpc_ecosystem_go_grpc_middleware-v1.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_grpc_ecosystem_go_grpc_prometheus",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/grpc-ecosystem/go-grpc-prometheus",
        sha256 = "124dfc63aa52611a2882417e685c0452d4d99d64c13836a6a6747675e911fc17",
        strip_prefix = "github.com/grpc-ecosystem/go-grpc-prometheus@v1.2.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/grpc-ecosystem/go-grpc-prometheus/com_github_grpc_ecosystem_go_grpc_prometheus-v1.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_grpc_ecosystem_grpc_gateway",
        build_file_proto_mode = "disable_global",
        build_naming_convention = "go_default_library",
        importpath = "github.com/grpc-ecosystem/grpc-gateway",
        patch_args = ["-p1"],
        patches = [
            "@cockroach//build/patches:com_github_grpc_ecosystem_grpc_gateway.patch",
        ],
        sha256 = "377b03aef288b34ed894449d3ddba40d525dd7fb55de6e79045cdf499e7fe565",
        strip_prefix = "github.com/grpc-ecosystem/grpc-gateway@v1.16.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/grpc-ecosystem/grpc-gateway/com_github_grpc_ecosystem_grpc_gateway-v1.16.0.zip",
        ],
    )
    go_repository(
        name = "com_github_hailocab_go_hostpool",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hailocab/go-hostpool",
        sha256 = "faf2b985681cda77ab928976b620b790585e364b6aff351483227d474db85e9a",
        strip_prefix = "github.com/hailocab/go-hostpool@v0.0.0-20160125115350-e80d13ce29ed",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/hailocab/go-hostpool/com_github_hailocab_go_hostpool-v0.0.0-20160125115350-e80d13ce29ed.zip",
        ],
    )
    go_repository(
        name = "com_github_hashicorp_consul_api",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/consul/api",
        sha256 = "a84081dcb2361b540bb787871abedc0f9569c09637f5b5c40e973500a4402a82",
        strip_prefix = "github.com/hashicorp/consul/api@v1.10.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/hashicorp/consul/api/com_github_hashicorp_consul_api-v1.10.1.zip",
        ],
    )
    go_repository(
        name = "com_github_hashicorp_consul_sdk",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/consul/sdk",
        sha256 = "cf29fff6c000ee67eda1b8cacec9648d06944e3cdbb80e2e22dc0165708974c6",
        strip_prefix = "github.com/hashicorp/consul/sdk@v0.8.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/hashicorp/consul/sdk/com_github_hashicorp_consul_sdk-v0.8.0.zip",
        ],
    )
    go_repository(
        name = "com_github_hashicorp_errwrap",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/errwrap",
        sha256 = "ccdf4c90f894d8a5fde4e79d5828c5d27a13e9f7ce3006dd72ce76e6e17cdeb2",
        strip_prefix = "github.com/hashicorp/errwrap@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/hashicorp/errwrap/com_github_hashicorp_errwrap-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_hashicorp_go_cleanhttp",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/go-cleanhttp",
        sha256 = "e3cc9964b0bc80c6156d6fb064abcb62ff8c00df8be8009b6f6d3aefc2776a23",
        strip_prefix = "github.com/hashicorp/go-cleanhttp@v0.5.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/hashicorp/go-cleanhttp/com_github_hashicorp_go_cleanhttp-v0.5.1.zip",
        ],
    )
    go_repository(
        name = "com_github_hashicorp_go_hclog",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/go-hclog",
        sha256 = "c10a48312d0a1ae070bc894005efdb1bc23ad615d950b4e0975c30dfc01147c9",
        strip_prefix = "github.com/hashicorp/go-hclog@v0.14.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/hashicorp/go-hclog/com_github_hashicorp_go_hclog-v0.14.0.zip",
        ],
    )
    go_repository(
        name = "com_github_hashicorp_go_immutable_radix",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/go-immutable-radix",
        sha256 = "5245859054e0edcc7b017e11c671116b3994e5316695a78bac9b2495a115abc1",
        strip_prefix = "github.com/hashicorp/go-immutable-radix@v1.2.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/hashicorp/go-immutable-radix/com_github_hashicorp_go_immutable_radix-v1.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_hashicorp_go_msgpack",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/go-msgpack",
        sha256 = "fb47605669b0ddd75292aac788208475fecd54e0ea3e9a282d8a98ae8c60d1f5",
        strip_prefix = "github.com/hashicorp/go-msgpack@v0.5.5",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/hashicorp/go-msgpack/com_github_hashicorp_go_msgpack-v0.5.5.zip",
        ],
    )
    go_repository(
        name = "com_github_hashicorp_go_multierror",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/go-multierror",
        sha256 = "3c60b77bb039c4431734883c65d645436a57c2653d00ae4cd7ea4f0303bc38e2",
        strip_prefix = "github.com/hashicorp/go-multierror@v1.1.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/hashicorp/go-multierror/com_github_hashicorp_go_multierror-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_hashicorp_go_net",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/go.net",
        sha256 = "71564aa3cb6e2820ee31e4d9e264e4ed889c7916f958b2f54c6f3004d4fcd8d2",
        strip_prefix = "github.com/hashicorp/go.net@v0.0.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/hashicorp/go.net/com_github_hashicorp_go_net-v0.0.1.zip",
        ],
    )
    go_repository(
        name = "com_github_hashicorp_go_plugin",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/go-plugin",
        sha256 = "bdcedbf701bf0e294e15a46806c520e3c5e40072a1dba1cce5cad959f219f9a3",
        strip_prefix = "github.com/hashicorp/go-plugin@v1.3.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/hashicorp/go-plugin/com_github_hashicorp_go_plugin-v1.3.0.zip",
        ],
    )
    go_repository(
        name = "com_github_hashicorp_go_retryablehttp",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/go-retryablehttp",
        sha256 = "1560044c4deed91fa2a27874216ed4580afbabd37f53232d2364b131c915d94f",
        strip_prefix = "github.com/hashicorp/go-retryablehttp@v0.5.3",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/hashicorp/go-retryablehttp/com_github_hashicorp_go_retryablehttp-v0.5.3.zip",
        ],
    )
    go_repository(
        name = "com_github_hashicorp_go_rootcerts",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/go-rootcerts",
        sha256 = "864a48e642e87a273fb5ef60bb3575bd74a7090510f93143163fa6700be31948",
        strip_prefix = "github.com/hashicorp/go-rootcerts@v1.0.2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/hashicorp/go-rootcerts/com_github_hashicorp_go_rootcerts-v1.0.2.zip",
        ],
    )
    go_repository(
        name = "com_github_hashicorp_go_sockaddr",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/go-sockaddr",
        sha256 = "50c1b60863b0cd31d03b26d3975f76cab55466666c067cd1823481a61f19af33",
        strip_prefix = "github.com/hashicorp/go-sockaddr@v1.0.2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/hashicorp/go-sockaddr/com_github_hashicorp_go_sockaddr-v1.0.2.zip",
        ],
    )
    go_repository(
        name = "com_github_hashicorp_go_syslog",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/go-syslog",
        sha256 = "a0ca8b61ea365e9ecdca513b94f200aef3ff68b4c95d9dabc88ca25fcb33bce6",
        strip_prefix = "github.com/hashicorp/go-syslog@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/hashicorp/go-syslog/com_github_hashicorp_go_syslog-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_hashicorp_go_uuid",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/go-uuid",
        sha256 = "30e419ebb4658e789be8ef1f5629faccc15d6571c6914a51afdcbaf74a5862b8",
        strip_prefix = "github.com/hashicorp/go-uuid@v1.0.2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/hashicorp/go-uuid/com_github_hashicorp_go_uuid-v1.0.2.zip",
        ],
    )
    go_repository(
        name = "com_github_hashicorp_go_version",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/go-version",
        sha256 = "a3231adb6bf029750970de2955e82e41e4c062b94eb73683e9111aa0c0841008",
        strip_prefix = "github.com/hashicorp/go-version@v1.2.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/hashicorp/go-version/com_github_hashicorp_go_version-v1.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_hashicorp_golang_lru",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/golang-lru",
        sha256 = "7b2a8b1739c858727fca497a6415323edb801dc97b8aca04f7bac4ab9fb5c66b",
        strip_prefix = "github.com/hashicorp/golang-lru@v0.5.4",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/hashicorp/golang-lru/com_github_hashicorp_golang_lru-v0.5.4.zip",
        ],
    )
    go_repository(
        name = "com_github_hashicorp_hcl",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/hcl",
        sha256 = "54149a2e5121b3e81f961c79210e63d6798eb63de28d2599ee59ade1fa76c82b",
        strip_prefix = "github.com/hashicorp/hcl@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/hashicorp/hcl/com_github_hashicorp_hcl-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_hashicorp_logutils",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/logutils",
        sha256 = "0e88424578d1d6b7793b63d30c180a353ce8041701d25dc7c3bcd9841c36db5b",
        strip_prefix = "github.com/hashicorp/logutils@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/hashicorp/logutils/com_github_hashicorp_logutils-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_hashicorp_mdns",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/mdns",
        sha256 = "0f4b33961638b1273ace80b64c6fc7e54a1064484b2a1e182ab3d38a35dbc94f",
        strip_prefix = "github.com/hashicorp/mdns@v1.0.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/hashicorp/mdns/com_github_hashicorp_mdns-v1.0.1.zip",
        ],
    )
    go_repository(
        name = "com_github_hashicorp_memberlist",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/memberlist",
        sha256 = "8de4e6391d17ffee7722c3fa96589049ae7b6b4c2b59b757ecb9bb779f5307c6",
        strip_prefix = "github.com/hashicorp/memberlist@v0.2.4",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/hashicorp/memberlist/com_github_hashicorp_memberlist-v0.2.4.zip",
        ],
    )
    go_repository(
        name = "com_github_hashicorp_serf",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/serf",
        sha256 = "b26c9916768043e9480615b4032f9f7d18ee2cdad6a7f75436570610df30fadf",
        strip_prefix = "github.com/hashicorp/serf@v0.9.5",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/hashicorp/serf/com_github_hashicorp_serf-v0.9.5.zip",
        ],
    )
    go_repository(
        name = "com_github_hashicorp_yamux",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hashicorp/yamux",
        sha256 = "d8a888d6a4ecbc09f2f3663cb47aa2d064298eeb1491f4761a43ae95e93ba035",
        strip_prefix = "github.com/hashicorp/yamux@v0.0.0-20190923154419-df201c70410d",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/hashicorp/yamux/com_github_hashicorp_yamux-v0.0.0-20190923154419-df201c70410d.zip",
        ],
    )
    go_repository(
        name = "com_github_hdrhistogram_hdrhistogram_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/HdrHistogram/hdrhistogram-go",
        sha256 = "12281e93a8cf983caec3286e00eadb1475bddad684226015ab96979012f55ac9",
        strip_prefix = "github.com/HdrHistogram/hdrhistogram-go@v1.1.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/HdrHistogram/hdrhistogram-go/com_github_hdrhistogram_hdrhistogram_go-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_hetznercloud_hcloud_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hetznercloud/hcloud-go",
        sha256 = "c530755603bfe3c79ee4327f896057d6c2fe93792d8a77c42c22b90547c52e7c",
        strip_prefix = "github.com/hetznercloud/hcloud-go@v1.32.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/hetznercloud/hcloud-go/com_github_hetznercloud_hcloud_go-v1.32.0.zip",
        ],
    )
    go_repository(
        name = "com_github_howeyc_gopass",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/howeyc/gopass",
        sha256 = "83560b6c9a6220bcbb4ad2f043e5a190ab11a013b77c1bbff9a3a67ed74d4b37",
        strip_prefix = "github.com/howeyc/gopass@v0.0.0-20190910152052-7cb4b85ec19c",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/howeyc/gopass/com_github_howeyc_gopass-v0.0.0-20190910152052-7cb4b85ec19c.zip",
        ],
    )
    go_repository(
        name = "com_github_hpcloud_tail",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hpcloud/tail",
        sha256 = "3cba484748e2e2919d72663599b8cc6454058976fbca96f9ac78d84f195b922a",
        strip_prefix = "github.com/hpcloud/tail@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/hpcloud/tail/com_github_hpcloud_tail-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_huandu_xstrings",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/huandu/xstrings",
        sha256 = "cdd580467bc14ea0ad6856782d8aeb3d954e9c3499f4a771b6813174d2713d0d",
        strip_prefix = "github.com/huandu/xstrings@v1.3.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/huandu/xstrings/com_github_huandu_xstrings-v1.3.0.zip",
        ],
    )
    go_repository(
        name = "com_github_hudl_fargo",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hudl/fargo",
        sha256 = "040aa24d7c5cdf43ed18767d4dff7d5533c65f58f45424f38eed51a5956445cf",
        strip_prefix = "github.com/hudl/fargo@v1.3.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/hudl/fargo/com_github_hudl_fargo-v1.3.0.zip",
        ],
    )
    go_repository(
        name = "com_github_hydrogen18_memlistener",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/hydrogen18/memlistener",
        sha256 = "5bc8454edabbdc6c5b849a51717f825be7d009d79bdc5d3c3a333b869a58c141",
        strip_prefix = "github.com/hydrogen18/memlistener@v0.0.0-20200120041712-dcc25e7acd91",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/hydrogen18/memlistener/com_github_hydrogen18_memlistener-v0.0.0-20200120041712-dcc25e7acd91.zip",
        ],
    )
    go_repository(
        name = "com_github_iancoleman_strcase",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/iancoleman/strcase",
        sha256 = "cb5027fec91d36426f0978a6c42ab52d8735fa3e1711be0127feda70a9a9fd05",
        strip_prefix = "github.com/iancoleman/strcase@v0.2.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/iancoleman/strcase/com_github_iancoleman_strcase-v0.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_ianlancetaylor_demangle",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ianlancetaylor/demangle",
        sha256 = "f5709a6601f0b4dc17da3baba1a50cc95ebf3efdd040aecc7fcf4d0e89bab066",
        strip_prefix = "github.com/ianlancetaylor/demangle@v0.0.0-20200824232613-28f6c0f3b639",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/ianlancetaylor/demangle/com_github_ianlancetaylor_demangle-v0.0.0-20200824232613-28f6c0f3b639.zip",
        ],
    )
    go_repository(
        name = "com_github_icrowley_fake",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/icrowley/fake",
        sha256 = "b503a0bc24e79b470d85701a11294430274f1203977e931008a88543f9f56fd4",
        strip_prefix = "github.com/icrowley/fake@v0.0.0-20180203215853-4178557ae428",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/icrowley/fake/com_github_icrowley_fake-v0.0.0-20180203215853-4178557ae428.zip",
        ],
    )
    go_repository(
        name = "com_github_imdario_mergo",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/imdario/mergo",
        sha256 = "47332eb559e993749cc31292807b3a639a470032ec603fd3c15fbe46f82192f6",
        strip_prefix = "github.com/imdario/mergo@v0.3.11",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/imdario/mergo/com_github_imdario_mergo-v0.3.11.zip",
        ],
    )
    go_repository(
        name = "com_github_imkira_go_interpol",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/imkira/go-interpol",
        sha256 = "de5111f7694700ea056beeb7c1ca1a827075d423422f251076ee17bd869477d9",
        strip_prefix = "github.com/imkira/go-interpol@v1.1.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/imkira/go-interpol/com_github_imkira_go_interpol-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_inconshreveable_mousetrap",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/inconshreveable/mousetrap",
        sha256 = "c3fa0813e78f5cef10dc0e9912c43e68e06ff970a98e98c4050fe14dbbfd18c7",
        strip_prefix = "github.com/inconshreveable/mousetrap@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/inconshreveable/mousetrap/com_github_inconshreveable_mousetrap-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_influxdata_flux",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/influxdata/flux",
        sha256 = "0cfc34ed0de87e3b1b47a656034761bbba5556c68de21f8989865955a85350cc",
        strip_prefix = "github.com/influxdata/flux@v0.120.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/influxdata/flux/com_github_influxdata_flux-v0.120.1.zip",
        ],
    )
    go_repository(
        name = "com_github_influxdata_httprouter",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/influxdata/httprouter",
        sha256 = "ef9333cac30fec1fab0a006632394fd4289a7a9ae559c9ed8f45ecf0233cfb4b",
        strip_prefix = "github.com/influxdata/httprouter@v1.3.1-0.20191122104820-ee83e2772f69",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/influxdata/httprouter/com_github_influxdata_httprouter-v1.3.1-0.20191122104820-ee83e2772f69.zip",
        ],
    )
    go_repository(
        name = "com_github_influxdata_influxdb",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/influxdata/influxdb",
        sha256 = "1289665beb354ab4b48ad1fdd07713dc03178fb8b4f7d0ebc8a2e4dbbc6a068a",
        strip_prefix = "github.com/influxdata/influxdb@v1.9.3",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/influxdata/influxdb/com_github_influxdata_influxdb-v1.9.3.zip",
        ],
    )
    go_repository(
        name = "com_github_influxdata_influxdb1_client",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/influxdata/influxdb1-client",
        sha256 = "71a73ab9f209f44dd50e35f0f61dd9b25e6a2df2a661ce0468c5cfb5615e1f09",
        strip_prefix = "github.com/influxdata/influxdb1-client@v0.0.0-20191209144304-8bf82d3c094d",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/influxdata/influxdb1-client/com_github_influxdata_influxdb1_client-v0.0.0-20191209144304-8bf82d3c094d.zip",
        ],
    )
    go_repository(
        name = "com_github_influxdata_influxdb_client_go_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/influxdata/influxdb-client-go/v2",
        sha256 = "62ca3e004452948177e4632f2990281206fac0d41aedcffb6e75ce18d72186a8",
        strip_prefix = "github.com/influxdata/influxdb-client-go/v2@v2.3.1-0.20210518120617-5d1fff431040",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/influxdata/influxdb-client-go/v2/com_github_influxdata_influxdb_client_go_v2-v2.3.1-0.20210518120617-5d1fff431040.zip",
        ],
    )
    go_repository(
        name = "com_github_influxdata_influxql",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/influxdata/influxql",
        sha256 = "06bb8421a2faeb74a4494abcf40ca09acc6066c11f2a65c8f437e791a0a864c5",
        strip_prefix = "github.com/influxdata/influxql@v1.1.1-0.20210223160523-b6ab99450c93",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/influxdata/influxql/com_github_influxdata_influxql-v1.1.1-0.20210223160523-b6ab99450c93.zip",
        ],
    )
    go_repository(
        name = "com_github_influxdata_line_protocol",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/influxdata/line-protocol",
        sha256 = "5136447167086a6b8ac748f3d74b716940f87240342381c1ac64889a085127c1",
        strip_prefix = "github.com/influxdata/line-protocol@v0.0.0-20200327222509-2487e7298839",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/influxdata/line-protocol/com_github_influxdata_line_protocol-v0.0.0-20200327222509-2487e7298839.zip",
        ],
    )
    go_repository(
        name = "com_github_influxdata_pkg_config",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/influxdata/pkg-config",
        sha256 = "4cdbff4816958d1540610fb02311231dffe34b3221abfa0fdd3c72e63113baca",
        strip_prefix = "github.com/influxdata/pkg-config@v0.2.7",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/influxdata/pkg-config/com_github_influxdata_pkg_config-v0.2.7.zip",
        ],
    )
    go_repository(
        name = "com_github_influxdata_promql_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/influxdata/promql/v2",
        sha256 = "b928626f2eb81eed0046ef23a83a77a28dd140d369a0d2538c94e85d1055877f",
        strip_prefix = "github.com/influxdata/promql/v2@v2.12.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/influxdata/promql/v2/com_github_influxdata_promql_v2-v2.12.0.zip",
        ],
    )
    go_repository(
        name = "com_github_influxdata_roaring",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/influxdata/roaring",
        sha256 = "7b38a79854fee9589bd94c707a3a93697660ad831642d30729a2dfbecd57beeb",
        strip_prefix = "github.com/influxdata/roaring@v0.4.13-0.20180809181101-fc520f41fab6",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/influxdata/roaring/com_github_influxdata_roaring-v0.4.13-0.20180809181101-fc520f41fab6.zip",
        ],
    )
    go_repository(
        name = "com_github_influxdata_tdigest",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/influxdata/tdigest",
        sha256 = "8428b1a86f73f701ac1f4fba74ba02c5cb6b2adaf8fa13282d5a60e5f3071b0c",
        strip_prefix = "github.com/influxdata/tdigest@v0.0.2-0.20210216194612-fc98d27c9e8b",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/influxdata/tdigest/com_github_influxdata_tdigest-v0.0.2-0.20210216194612-fc98d27c9e8b.zip",
        ],
    )
    go_repository(
        name = "com_github_influxdata_usage_client",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/influxdata/usage-client",
        sha256 = "6a33ba80b3d59a7aeaba3d32a71033f729b6de8e746ab6133f97fba9810532df",
        strip_prefix = "github.com/influxdata/usage-client@v0.0.0-20160829180054-6d3895376368",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/influxdata/usage-client/com_github_influxdata_usage_client-v0.0.0-20160829180054-6d3895376368.zip",
        ],
    )
    go_repository(
        name = "com_github_irfansharif_recorder",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/irfansharif/recorder",
        sha256 = "4a2f085d5339eba18558059c51110de1ff6d9ab8389ece8818fd2f62b7b2e7ab",
        strip_prefix = "github.com/irfansharif/recorder@v0.0.0-20211218081646-a21b46510fd6",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/irfansharif/recorder/com_github_irfansharif_recorder-v0.0.0-20211218081646-a21b46510fd6.zip",
        ],
    )
    go_repository(
        name = "com_github_iris_contrib_blackfriday",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/iris-contrib/blackfriday",
        sha256 = "936679f49251da75fde84b8f38884dbce89747b96f8206f7a4675bfcc7dd165d",
        strip_prefix = "github.com/iris-contrib/blackfriday@v2.0.0+incompatible",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/iris-contrib/blackfriday/com_github_iris_contrib_blackfriday-v2.0.0+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_iris_contrib_go_uuid",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/iris-contrib/go.uuid",
        sha256 = "c6bae86643c2d6047c68c25226a1e75c5331c03466532ee6c943705743949bd9",
        strip_prefix = "github.com/iris-contrib/go.uuid@v2.0.0+incompatible",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/iris-contrib/go.uuid/com_github_iris_contrib_go_uuid-v2.0.0+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_iris_contrib_i18n",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/iris-contrib/i18n",
        sha256 = "f9d637c5c887210f906f1228682239d98312b99e6d5192bec64faf597a3bab9c",
        strip_prefix = "github.com/iris-contrib/i18n@v0.0.0-20171121225848-987a633949d0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/iris-contrib/i18n/com_github_iris_contrib_i18n-v0.0.0-20171121225848-987a633949d0.zip",
        ],
    )
    go_repository(
        name = "com_github_iris_contrib_jade",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/iris-contrib/jade",
        sha256 = "1d5fb817f516b6ac581ef083ee8b80540a6fe8de7ed8273f78c653e4a777a7f1",
        strip_prefix = "github.com/iris-contrib/jade@v1.1.3",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/iris-contrib/jade/com_github_iris_contrib_jade-v1.1.3.zip",
        ],
    )
    go_repository(
        name = "com_github_iris_contrib_pongo2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/iris-contrib/pongo2",
        sha256 = "9b991986eabd245f0d09a7e1098eafdb3c86b0a7fb115c30f29e94ae9c845d3d",
        strip_prefix = "github.com/iris-contrib/pongo2@v0.0.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/iris-contrib/pongo2/com_github_iris_contrib_pongo2-v0.0.1.zip",
        ],
    )
    go_repository(
        name = "com_github_iris_contrib_schema",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/iris-contrib/schema",
        sha256 = "d0887d45474f3aa30ff0fd329e98341e795be2c6e861bd92c30a7f97f6e57385",
        strip_prefix = "github.com/iris-contrib/schema@v0.0.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/iris-contrib/schema/com_github_iris_contrib_schema-v0.0.1.zip",
        ],
    )
    go_repository(
        name = "com_github_j_keck_arping",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/j-keck/arping",
        sha256 = "6001c94a8c4eed55718f627346cb685cce67369ca5c29ae059f58f7abd8bd8a7",
        strip_prefix = "github.com/j-keck/arping@v0.0.0-20160618110441-2cf9dc699c56",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/j-keck/arping/com_github_j_keck_arping-v0.0.0-20160618110441-2cf9dc699c56.zip",
        ],
    )
    go_repository(
        name = "com_github_jackc_chunkreader",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jackc/chunkreader",
        sha256 = "e204c917e2652ffe047f5c8b031192757321f568654e3df8408bf04178df1408",
        strip_prefix = "github.com/jackc/chunkreader@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/jackc/chunkreader/com_github_jackc_chunkreader-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_jackc_chunkreader_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jackc/chunkreader/v2",
        sha256 = "6e3f4b7d9647f31061f6446ae10de71fc1407e64f84cd0949afac0cd231e8dd2",
        strip_prefix = "github.com/jackc/chunkreader/v2@v2.0.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/jackc/chunkreader/v2/com_github_jackc_chunkreader_v2-v2.0.1.zip",
        ],
    )
    go_repository(
        name = "com_github_jackc_pgconn",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jackc/pgconn",
        sha256 = "ca2544257ff60423ca23a1560bf3cda188f3cbb32b861ff9d123b2760af50104",
        strip_prefix = "github.com/jackc/pgconn@v1.10.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/jackc/pgconn/com_github_jackc_pgconn-v1.10.0.zip",
        ],
    )
    go_repository(
        name = "com_github_jackc_pgio",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jackc/pgio",
        sha256 = "1a83c03d53f6a40339364cafcbbabb44238203c79ca0c9b98bf582d0df0e0468",
        strip_prefix = "github.com/jackc/pgio@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/jackc/pgio/com_github_jackc_pgio-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_jackc_pgmock",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jackc/pgmock",
        sha256 = "0fffd0a7a67dbdfafa04297e51028c6d2d08cd6691f3b6d78d7ae6502d3d4cf2",
        strip_prefix = "github.com/jackc/pgmock@v0.0.0-20210724152146-4ad1a8207f65",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/jackc/pgmock/com_github_jackc_pgmock-v0.0.0-20210724152146-4ad1a8207f65.zip",
        ],
    )
    go_repository(
        name = "com_github_jackc_pgpassfile",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jackc/pgpassfile",
        sha256 = "1cc79fb0b80f54b568afd3f4648dd1c349f746ad7c379df8d7f9e0eb1cac938b",
        strip_prefix = "github.com/jackc/pgpassfile@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/jackc/pgpassfile/com_github_jackc_pgpassfile-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_jackc_pgproto3",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jackc/pgproto3",
        sha256 = "e3766bee50ed74e49a067b2c4797a2c69015cf104bf3f3624cd483a9e940b4ee",
        strip_prefix = "github.com/jackc/pgproto3@v1.1.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/jackc/pgproto3/com_github_jackc_pgproto3-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_jackc_pgproto3_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jackc/pgproto3/v2",
        sha256 = "0d948d69271a981bcf89d8f093f91246b5bb87f8ce323cafb9998b4d7762bcb3",
        strip_prefix = "github.com/jackc/pgproto3/v2@v2.1.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/jackc/pgproto3/v2/com_github_jackc_pgproto3_v2-v2.1.1.zip",
        ],
    )
    go_repository(
        name = "com_github_jackc_pgservicefile",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jackc/pgservicefile",
        sha256 = "8422a25b9d2b0be05c66ee1ccfdbaab144ce98f1ac678bc647064c560d4cd6e2",
        strip_prefix = "github.com/jackc/pgservicefile@v0.0.0-20200714003250-2b9c44734f2b",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/jackc/pgservicefile/com_github_jackc_pgservicefile-v0.0.0-20200714003250-2b9c44734f2b.zip",
        ],
    )
    go_repository(
        name = "com_github_jackc_pgtype",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jackc/pgtype",
        sha256 = "f402686a83fa781357059d0c03b79a9a8686349cb9ceed4f7067ae27afb7265a",
        strip_prefix = "github.com/jackc/pgtype@v1.8.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/jackc/pgtype/com_github_jackc_pgtype-v1.8.1.zip",
        ],
    )
    go_repository(
        name = "com_github_jackc_pgx_v4",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jackc/pgx/v4",
        sha256 = "ca77ca40d8c4a2544223728867a0996fd6a8dbdac43eb4bfc98cdda8148aa50c",
        strip_prefix = "github.com/jackc/pgx/v4@v4.13.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/jackc/pgx/v4/com_github_jackc_pgx_v4-v4.13.0.zip",
        ],
    )
    go_repository(
        name = "com_github_jackc_puddle",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jackc/puddle",
        sha256 = "2106d95f2e74cd3a80c5ae06a7e904e52d6c5c617db67fd4491af3a77a29e677",
        strip_prefix = "github.com/jackc/puddle@v1.1.3",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/jackc/puddle/com_github_jackc_puddle-v1.1.3.zip",
        ],
    )
    go_repository(
        name = "com_github_jaegertracing_jaeger",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jaegertracing/jaeger",
        sha256 = "256a95b2a52a66494aca6d354224bb450ff38ce3ea1890af46a7c8dc39203891",
        strip_prefix = "github.com/jaegertracing/jaeger@v1.18.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/jaegertracing/jaeger/com_github_jaegertracing_jaeger-v1.18.1.zip",
        ],
    )
    go_repository(
        name = "com_github_jcmturner_aescts_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jcmturner/aescts/v2",
        sha256 = "717a211ad4aac248cf33cadde73059c13f8e9462123a0ab2fed5c5e61f7739d7",
        strip_prefix = "github.com/jcmturner/aescts/v2@v2.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/jcmturner/aescts/v2/com_github_jcmturner_aescts_v2-v2.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_jcmturner_dnsutils_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jcmturner/dnsutils/v2",
        sha256 = "f9188186b672e547cfaef66107aa62d65054c5d4f10d4dcd1ff157d6bf8c275d",
        strip_prefix = "github.com/jcmturner/dnsutils/v2@v2.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/jcmturner/dnsutils/v2/com_github_jcmturner_dnsutils_v2-v2.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_jcmturner_gofork",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jcmturner/gofork",
        sha256 = "5e015dd9b038f1dded0b2ded77e529d2f6ba0bed228a98831af5a3610eefcb52",
        strip_prefix = "github.com/jcmturner/gofork@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/jcmturner/gofork/com_github_jcmturner_gofork-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_jcmturner_goidentity_v6",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jcmturner/goidentity/v6",
        sha256 = "243e6fd6ea9f3094eea32c55febade6d8aaa1b563db655b0c5327940e4719beb",
        strip_prefix = "github.com/jcmturner/goidentity/v6@v6.0.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/jcmturner/goidentity/v6/com_github_jcmturner_goidentity_v6-v6.0.1.zip",
        ],
    )
    go_repository(
        name = "com_github_jcmturner_gokrb5_v8",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jcmturner/gokrb5/v8",
        sha256 = "eecd7120363321bb6b58b015395089958720271b3211659d802447d417af5970",
        strip_prefix = "github.com/jcmturner/gokrb5/v8@v8.4.2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/jcmturner/gokrb5/v8/com_github_jcmturner_gokrb5_v8-v8.4.2.zip",
        ],
    )
    go_repository(
        name = "com_github_jcmturner_rpc_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jcmturner/rpc/v2",
        sha256 = "90c595355e5e2c9dc1e1ae71a88491a04c34d8791180098da103217cbf5f5574",
        strip_prefix = "github.com/jcmturner/rpc/v2@v2.0.3",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/jcmturner/rpc/v2/com_github_jcmturner_rpc_v2-v2.0.3.zip",
        ],
    )
    go_repository(
        name = "com_github_jessevdk_go_flags",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jessevdk/go-flags",
        sha256 = "9886379a8c31f9021ce68490e2a21bdbea7e5fe95533229650e1ac1571dcd78a",
        strip_prefix = "github.com/jessevdk/go-flags@v1.5.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/jessevdk/go-flags/com_github_jessevdk_go_flags-v1.5.0.zip",
        ],
    )
    go_repository(
        name = "com_github_jhump_protoreflect",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jhump/protoreflect",
        sha256 = "919843c24904e6855775ea7e248654582a1703bd879b608a9bcc5e4a726e0288",
        strip_prefix = "github.com/jhump/protoreflect@v1.9.1-0.20210817181203-db1a327a393e",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/jhump/protoreflect/com_github_jhump_protoreflect-v1.9.1-0.20210817181203-db1a327a393e.zip",
        ],
    )
    go_repository(
        name = "com_github_jinzhu_inflection",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jinzhu/inflection",
        sha256 = "cf1087a6f6653ed5f366f85cf0110bbbf581d4e9bc8a4d1a9b56765d94b546c3",
        strip_prefix = "github.com/jinzhu/inflection@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/jinzhu/inflection/com_github_jinzhu_inflection-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_jinzhu_now",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jinzhu/now",
        sha256 = "8c56b1bd3f10b4597c75e1a880bc37c02df633b2afa8d576ab484d681c4c9609",
        strip_prefix = "github.com/jinzhu/now@v1.1.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/jinzhu/now/com_github_jinzhu_now-v1.1.1.zip",
        ],
    )
    go_repository(
        name = "com_github_jmespath_go_jmespath",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jmespath/go-jmespath",
        sha256 = "d1f77b6790d7c4321a74260f3675683d3ac06b0a614b5f83e870beae0a8b2867",
        strip_prefix = "github.com/jmespath/go-jmespath@v0.4.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/jmespath/go-jmespath/com_github_jmespath_go_jmespath-v0.4.0.zip",
        ],
    )
    go_repository(
        name = "com_github_jmespath_go_jmespath_internal_testify",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jmespath/go-jmespath/internal/testify",
        sha256 = "338f73832eb2a63ab0c912197e653c7b62426fc4387e0a76ab0d43c65e29b3e1",
        strip_prefix = "github.com/jmespath/go-jmespath/internal/testify@v1.5.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/jmespath/go-jmespath/internal/testify/com_github_jmespath_go_jmespath_internal_testify-v1.5.1.zip",
        ],
    )
    go_repository(
        name = "com_github_jmoiron_sqlx",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jmoiron/sqlx",
        sha256 = "84cf22439d71b3dfe534639a128f4459664976271941c2a33bef5c5aa4f7ed70",
        strip_prefix = "github.com/jmoiron/sqlx@v1.3.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/jmoiron/sqlx/com_github_jmoiron_sqlx-v1.3.1.zip",
        ],
    )
    go_repository(
        name = "com_github_joho_godotenv",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/joho/godotenv",
        sha256 = "acef5a394fbd1193f52d0d19690b0bfe82728d18dd3bf67730dc5031c22d563f",
        strip_prefix = "github.com/joho/godotenv@v1.3.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/joho/godotenv/com_github_joho_godotenv-v1.3.0.zip",
        ],
    )
    go_repository(
        name = "com_github_joker_hpp",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Joker/hpp",
        sha256 = "790dc3cfb8e51ff22f29d74b5b58782999e267e86290bc2b52485ccf9c8d2792",
        strip_prefix = "github.com/Joker/hpp@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/Joker/hpp/com_github_joker_hpp-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_joker_jade",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Joker/jade",
        sha256 = "3fc31c80e93cb053cd4fce60a7288e3760f9fe5c571ec4c2d32c4f9bf6c487e7",
        strip_prefix = "github.com/Joker/jade@v1.0.1-0.20190614124447-d475f43051e7",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/Joker/jade/com_github_joker_jade-v1.0.1-0.20190614124447-d475f43051e7.zip",
        ],
    )
    go_repository(
        name = "com_github_jonboulle_clockwork",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jonboulle/clockwork",
        sha256 = "930d355d1ced60a668bcbca6154bb5671120ba11a34119505d1c0677f7bbbf97",
        strip_prefix = "github.com/jonboulle/clockwork@v0.1.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/jonboulle/clockwork/com_github_jonboulle_clockwork-v0.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_jordan_wright_email",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jordan-wright/email",
        sha256 = "6d35fa83ea02cfacd0e1ba9c9061381b963215cef84c8bf83ad5944cb304c390",
        strip_prefix = "github.com/jordan-wright/email@v4.0.1-0.20210109023952-943e75fe5223+incompatible",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/jordan-wright/email/com_github_jordan_wright_email-v4.0.1-0.20210109023952-943e75fe5223+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_jordanlewis_gcassert",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jordanlewis/gcassert",
        sha256 = "1f227f6d9c9ae6c913cad0ebbe6b15f6d6af265a8f51dd9caba955123987fab8",
        strip_prefix = "github.com/jordanlewis/gcassert@v0.0.0-20210709222130-81f5df3faab8",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/jordanlewis/gcassert/com_github_jordanlewis_gcassert-v0.0.0-20210709222130-81f5df3faab8.zip",
        ],
    )
    go_repository(
        name = "com_github_josharian_intern",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/josharian/intern",
        sha256 = "5679bfd11c14adccdb45bd1a0f9cf4b445b95caeed6fb507ba96ecced11c248d",
        strip_prefix = "github.com/josharian/intern@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/josharian/intern/com_github_josharian_intern-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_jpillora_backoff",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jpillora/backoff",
        sha256 = "f856692c725143c49b9cceabfbca8bc93d3dbde84a0aaa53fb26ed3774c220cc",
        strip_prefix = "github.com/jpillora/backoff@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/jpillora/backoff/com_github_jpillora_backoff-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_json_iterator_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/json-iterator/go",
        sha256 = "d001ea57081afd0e378467c8f4a9b6a51259996bb8bb763f78107eaf12f99501",
        strip_prefix = "github.com/json-iterator/go@v1.1.12",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/json-iterator/go/com_github_json_iterator_go-v1.1.12.zip",
        ],
    )
    go_repository(
        name = "com_github_jstemmer_go_junit_report",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jstemmer/go-junit-report",
        sha256 = "fbd2196e4a50a88f8c352f76325f4ba72338ecec7b6cb7535317ce9e3aa40284",
        strip_prefix = "github.com/jstemmer/go-junit-report@v0.9.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/jstemmer/go-junit-report/com_github_jstemmer_go_junit_report-v0.9.1.zip",
        ],
    )
    go_repository(
        name = "com_github_jsternberg_zap_logfmt",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jsternberg/zap-logfmt",
        sha256 = "04fcbcf1ef6f09169218324a5b3a8453ffecb62c3669e2102ffc96cf599ef876",
        strip_prefix = "github.com/jsternberg/zap-logfmt@v1.2.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/jsternberg/zap-logfmt/com_github_jsternberg_zap_logfmt-v1.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_jtolds_gls",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jtolds/gls",
        sha256 = "2f51f8cb610e846dc4bd9b3c0fbf6bebab24bb06d866db7804e123a61b0bd9ec",
        strip_prefix = "github.com/jtolds/gls@v4.20.0+incompatible",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/jtolds/gls/com_github_jtolds_gls-v4.20.0+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_juju_errors",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/juju/errors",
        sha256 = "b97a8b6ca1e8cc6fba715b4187a25d9ae53122edbdcaf1154e36249e6d297393",
        strip_prefix = "github.com/juju/errors@v0.0.0-20181118221551-089d3ea4e4d5",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/juju/errors/com_github_juju_errors-v0.0.0-20181118221551-089d3ea4e4d5.zip",
        ],
    )
    go_repository(
        name = "com_github_juju_loggo",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/juju/loggo",
        sha256 = "64c21c4a3810a5d0e940fd11a46daa08bde2f951b59fb330ff06ab9634cc4863",
        strip_prefix = "github.com/juju/loggo@v0.0.0-20180524022052-584905176618",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/juju/loggo/com_github_juju_loggo-v0.0.0-20180524022052-584905176618.zip",
        ],
    )
    go_repository(
        name = "com_github_juju_testing",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/juju/testing",
        sha256 = "a66b521a6b60dd443b86a42d0274209e385d7f3e71db775b8c2000bcfd0c6649",
        strip_prefix = "github.com/juju/testing@v0.0.0-20180920084828-472a3e8b2073",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/juju/testing/com_github_juju_testing-v0.0.0-20180920084828-472a3e8b2073.zip",
        ],
    )
    go_repository(
        name = "com_github_julienschmidt_httprouter",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/julienschmidt/httprouter",
        sha256 = "e457dccd7015f340664e3b8cfd41997471382da2f4a743ee55be539abc6ca1f9",
        strip_prefix = "github.com/julienschmidt/httprouter@v1.3.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/julienschmidt/httprouter/com_github_julienschmidt_httprouter-v1.3.0.zip",
        ],
    )
    go_repository(
        name = "com_github_julusian_godocdown",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Julusian/godocdown",
        sha256 = "1bd26f1d29b20d40b3eb0a5678691a2e6e153c473efe079b8b1bbd97a7cc1f57",
        strip_prefix = "github.com/Julusian/godocdown@v0.0.0-20170816220326-6d19f8ff2df8",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/Julusian/godocdown/com_github_julusian_godocdown-v0.0.0-20170816220326-6d19f8ff2df8.zip",
        ],
    )
    go_repository(
        name = "com_github_jung_kurt_gofpdf",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jung-kurt/gofpdf",
        sha256 = "f0fa70ade137185bbff2f016831a2a456eaadc8d14bc7bf24f0229211820c078",
        strip_prefix = "github.com/jung-kurt/gofpdf@v1.0.3-0.20190309125859-24315acbbda5",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/jung-kurt/gofpdf/com_github_jung_kurt_gofpdf-v1.0.3-0.20190309125859-24315acbbda5.zip",
        ],
    )
    go_repository(
        name = "com_github_justinas_alice",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/justinas/alice",
        sha256 = "b2d65d6a613d0fe33b4595b69855ab9d55bcfeee506a19d07d4585c566fe6587",
        strip_prefix = "github.com/justinas/alice@v1.2.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/justinas/alice/com_github_justinas_alice-v1.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_jwilder_encoding",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/jwilder/encoding",
        sha256 = "91ab650780db18684a70137cbb34189c171c29a23aab48816c8bca74dbb012e9",
        strip_prefix = "github.com/jwilder/encoding@v0.0.0-20170811194829-b4e1701a28ef",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/jwilder/encoding/com_github_jwilder_encoding-v0.0.0-20170811194829-b4e1701a28ef.zip",
        ],
    )
    go_repository(
        name = "com_github_k0kubun_colorstring",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/k0kubun/colorstring",
        sha256 = "32a2eac0ffb69c6882b32ccfcdd76968cb9dfee9d9dc3d469fc405775399167c",
        strip_prefix = "github.com/k0kubun/colorstring@v0.0.0-20150214042306-9440f1994b88",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/k0kubun/colorstring/com_github_k0kubun_colorstring-v0.0.0-20150214042306-9440f1994b88.zip",
        ],
    )
    go_repository(
        name = "com_github_karrick_godirwalk",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/karrick/godirwalk",
        sha256 = "80518abce2eb573be7f1c529024f9a04cae142cd44995c59ccbffde40a5563d4",
        strip_prefix = "github.com/karrick/godirwalk@v1.10.3",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/karrick/godirwalk/com_github_karrick_godirwalk-v1.10.3.zip",
        ],
    )
    go_repository(
        name = "com_github_kataras_golog",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/kataras/golog",
        sha256 = "a7c666acc9f33f622d141f67cbc007072b80940f18b4dabc26d50a425d8dda62",
        strip_prefix = "github.com/kataras/golog@v0.0.10",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/kataras/golog/com_github_kataras_golog-v0.0.10.zip",
        ],
    )
    go_repository(
        name = "com_github_kataras_iris_v12",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/kataras/iris/v12",
        sha256 = "60f944610994584b428ac0bf18f8cae914eab7ca9ac7d40cc762825ecef902e9",
        strip_prefix = "github.com/kataras/iris/v12@v12.1.8",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/kataras/iris/v12/com_github_kataras_iris_v12-v12.1.8.zip",
        ],
    )
    go_repository(
        name = "com_github_kataras_neffos",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/kataras/neffos",
        sha256 = "06da0648f8f8aeb261a4d6da332d87004fc02718e4643d468b6315a40ef68c44",
        strip_prefix = "github.com/kataras/neffos@v0.0.14",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/kataras/neffos/com_github_kataras_neffos-v0.0.14.zip",
        ],
    )
    go_repository(
        name = "com_github_kataras_pio",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/kataras/pio",
        sha256 = "ccec5930116d9b46e338e39fd15404b737d1a7baffb65b6247280a37a9ef9e23",
        strip_prefix = "github.com/kataras/pio@v0.0.2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/kataras/pio/com_github_kataras_pio-v0.0.2.zip",
        ],
    )
    go_repository(
        name = "com_github_kataras_sitemap",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/kataras/sitemap",
        sha256 = "5e86f8ab488803b503071dfa6b678ad7eb61f1155ddc2b8844798f04d3a927d0",
        strip_prefix = "github.com/kataras/sitemap@v0.0.5",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/kataras/sitemap/com_github_kataras_sitemap-v0.0.5.zip",
        ],
    )
    go_repository(
        name = "com_github_kevinburke_go_bindata",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/kevinburke/go-bindata",
        sha256 = "f087b3a77624a113883bac519ebd1a4de07b70ab2ebe73e61e52325ac30777e0",
        strip_prefix = "github.com/kevinburke/go-bindata@v3.13.0+incompatible",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/kevinburke/go-bindata/com_github_kevinburke_go_bindata-v3.13.0+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_kisielk_errcheck",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/kisielk/errcheck",
        patch_args = ["-p1"],
        patches = [
            "@cockroach//build/patches:com_github_kisielk_errcheck.patch",
        ],
        sha256 = "99d3220891162cb684f8e05d54f3d0dc58abdd496a2f0cfda7fd4a28917a719e",
        strip_prefix = "github.com/kisielk/errcheck@v1.6.1-0.20210625163953-8ddee489636a",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/kisielk/errcheck/com_github_kisielk_errcheck-v1.6.1-0.20210625163953-8ddee489636a.zip",
        ],
    )
    go_repository(
        name = "com_github_kisielk_gotool",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/kisielk/gotool",
        sha256 = "089dbba6e3aa09944fdb40d72acc86694e8bdde01cfc0f40fe0248309eb80a3f",
        strip_prefix = "github.com/kisielk/gotool@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/kisielk/gotool/com_github_kisielk_gotool-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_klauspost_compress",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/klauspost/compress",
        sha256 = "25990a3b573b4568fa9c98c880e82abe5fe4dc6a3c784935923643ef28791acd",
        strip_prefix = "github.com/klauspost/compress@v1.14.2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/klauspost/compress/com_github_klauspost_compress-v1.14.2.zip",
        ],
    )
    go_repository(
        name = "com_github_klauspost_cpuid",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/klauspost/cpuid",
        sha256 = "f61266e43d5c247fdb55d843e2d93974717c1052cba9f331b181f60c4cf687d9",
        strip_prefix = "github.com/klauspost/cpuid@v1.3.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/klauspost/cpuid/com_github_klauspost_cpuid-v1.3.1.zip",
        ],
    )
    go_repository(
        name = "com_github_klauspost_cpuid_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/klauspost/cpuid/v2",
        sha256 = "52c716413296dce2b1698c6cdbc4c53927ce4aee2a60980daf9672e6b6a3b4cb",
        strip_prefix = "github.com/klauspost/cpuid/v2@v2.0.9",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/klauspost/cpuid/v2/com_github_klauspost_cpuid_v2-v2.0.9.zip",
        ],
    )
    go_repository(
        name = "com_github_klauspost_crc32",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/klauspost/crc32",
        sha256 = "6b632853a19f039138f251f94dbbdfdb72809adc3a02da08e4301d3d48275b06",
        strip_prefix = "github.com/klauspost/crc32@v0.0.0-20161016154125-cb6bfca970f6",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/klauspost/crc32/com_github_klauspost_crc32-v0.0.0-20161016154125-cb6bfca970f6.zip",
        ],
    )
    go_repository(
        name = "com_github_klauspost_pgzip",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/klauspost/pgzip",
        sha256 = "1143b6417d4bb46d26dc8e6223407b84b6cd5f32e5d705cd4a9fb142220ce4ba",
        strip_prefix = "github.com/klauspost/pgzip@v1.2.5",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/klauspost/pgzip/com_github_klauspost_pgzip-v1.2.5.zip",
        ],
    )
    go_repository(
        name = "com_github_knetic_govaluate",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Knetic/govaluate",
        sha256 = "d1d4ac5b4f5759726368f68b0d47f3c17c6d8689243ec66272311359d28a865b",
        strip_prefix = "github.com/Knetic/govaluate@v3.0.1-0.20171022003610-9aa49832a739+incompatible",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/Knetic/govaluate/com_github_knetic_govaluate-v3.0.1-0.20171022003610-9aa49832a739+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_knz_go_libedit",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/knz/go-libedit",
        patch_args = ["-p1"],
        patches = [
            "@cockroach//build/patches:com_github_knz_go_libedit.patch",
        ],
        sha256 = "d2ae0f8e43c49f917a2cadf52178c0efe1336fda5b8410a3d0f1270ae05d2532",
        strip_prefix = "github.com/otan-cockroach/go-libedit@v1.10.2-0.20201030151939-7cced08450e7",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/otan-cockroach/go-libedit/com_github_otan_cockroach_go_libedit-v1.10.2-0.20201030151939-7cced08450e7.zip",
        ],
    )
    go_repository(
        name = "com_github_knz_strtime",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/knz/strtime",
        sha256 = "c1e1b06c339798387413af1444f06f31a483d4f5278ab3a91b6cd5d7cd8d91a1",
        strip_prefix = "github.com/knz/strtime@v0.0.0-20200318182718-be999391ffa9",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/knz/strtime/com_github_knz_strtime-v0.0.0-20200318182718-be999391ffa9.zip",
        ],
    )
    go_repository(
        name = "com_github_konsorten_go_windows_terminal_sequences",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/konsorten/go-windows-terminal-sequences",
        sha256 = "429b01413b972b108ea86bbde3d5e660913f3e8099190d07ccfb2f186bc6d837",
        strip_prefix = "github.com/konsorten/go-windows-terminal-sequences@v1.0.3",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/konsorten/go-windows-terminal-sequences/com_github_konsorten_go_windows_terminal_sequences-v1.0.3.zip",
        ],
    )
    go_repository(
        name = "com_github_kr_fs",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/kr/fs",
        sha256 = "d376bd98e81aea34585fc3b04bab76363e9e87cde69383964e57e9779f2af81e",
        strip_prefix = "github.com/kr/fs@v0.1.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/kr/fs/com_github_kr_fs-v0.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_kr_logfmt",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/kr/logfmt",
        sha256 = "ebd95653aaca6182184a1b9b309a65d55eb4c7c833c5e790aee11efd73d4722c",
        strip_prefix = "github.com/kr/logfmt@v0.0.0-20140226030751-b84e30acd515",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/kr/logfmt/com_github_kr_logfmt-v0.0.0-20140226030751-b84e30acd515.zip",
        ],
    )
    go_repository(
        name = "com_github_kr_pretty",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/kr/pretty",
        sha256 = "3ac65e185f956d889d77485173fadcc30e959b6bcfdaa8acafaec5f4dac5cd48",
        strip_prefix = "github.com/kr/pretty@v0.3.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/kr/pretty/com_github_kr_pretty-v0.3.0.zip",
        ],
    )
    go_repository(
        name = "com_github_kr_pty",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/kr/pty",
        sha256 = "d66e6fbc65e772289a7ff8c58ab2cdfb886253053b0cea11ba3ca1738b2d6bc6",
        strip_prefix = "github.com/kr/pty@v1.1.8",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/kr/pty/com_github_kr_pty-v1.1.8.zip",
        ],
    )
    go_repository(
        name = "com_github_kr_text",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/kr/text",
        sha256 = "368eb318f91a5b67be905c47032ab5c31a1d49a97848b1011a0d0a2122b30ba4",
        strip_prefix = "github.com/kr/text@v0.2.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/kr/text/com_github_kr_text-v0.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_krishicks_yaml_patch",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/krishicks/yaml-patch",
        sha256 = "7aaf59809fc6a58e0d182293b974378740962887c8fbc95445921fcebd1fb3ae",
        strip_prefix = "github.com/krishicks/yaml-patch@v0.0.10",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/krishicks/yaml-patch/com_github_krishicks_yaml_patch-v0.0.10.zip",
        ],
    )
    go_repository(
        name = "com_github_kylelemons_godebug",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/kylelemons/godebug",
        sha256 = "dbbd0ce8c2f4932bb03704d73026b21af12bd68d5b8f4798dbf10a487a2b6d13",
        strip_prefix = "github.com/kylelemons/godebug@v1.1.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/kylelemons/godebug/com_github_kylelemons_godebug-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_labstack_echo_v4",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/labstack/echo/v4",
        sha256 = "1a09d9e413b2a1336b5295614dc993bc5eebfddc9ac023aa67fa91f36dfe6832",
        strip_prefix = "github.com/labstack/echo/v4@v4.6.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/labstack/echo/v4/com_github_labstack_echo_v4-v4.6.1.zip",
        ],
    )
    go_repository(
        name = "com_github_labstack_gommon",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/labstack/gommon",
        sha256 = "2783ed1c24d09a5539bc35954f71f41d270d78dc656be256c98a8ede2cbbe451",
        strip_prefix = "github.com/labstack/gommon@v0.3.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/labstack/gommon/com_github_labstack_gommon-v0.3.0.zip",
        ],
    )
    go_repository(
        name = "com_github_leanovate_gopter",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/leanovate/gopter",
        sha256 = "67c9724f8c25304bdef375d15c39f98621e0448b5f3c2f55bf66e07b52a67128",
        strip_prefix = "github.com/leanovate/gopter@v0.2.5-0.20190402064358-634a59d12406",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/leanovate/gopter/com_github_leanovate_gopter-v0.2.5-0.20190402064358-634a59d12406.zip",
        ],
    )
    go_repository(
        name = "com_github_leodido_go_urn",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/leodido/go-urn",
        sha256 = "8a854d784171000a69b79babb2cd3da9b8fccb1e1b6bb102c7a6d2b52380d08a",
        strip_prefix = "github.com/leodido/go-urn@v1.2.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/leodido/go-urn/com_github_leodido_go_urn-v1.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_lib_pq",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/lib/pq",
        sha256 = "0f50cfc8d4ed4bbb39767aacc04d6b23e1105d2fa50dcb8e4ae204b2c90018f0",
        strip_prefix = "github.com/lib/pq@v1.10.2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/lib/pq/com_github_lib_pq-v1.10.2.zip",
        ],
    )
    go_repository(
        name = "com_github_lib_pq_auth_kerberos",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/lib/pq/auth/kerberos",
        sha256 = "26113d1ad535ca1b52b9f093025ea658c6d3f14b617386a0c38115bfaceffdd4",
        strip_prefix = "github.com/lib/pq/auth/kerberos@v0.0.0-20200720160335-984a6aa1ca46",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/lib/pq/auth/kerberos/com_github_lib_pq_auth_kerberos-v0.0.0-20200720160335-984a6aa1ca46.zip",
        ],
    )
    go_repository(
        name = "com_github_lightstep_lightstep_tracer_common_golang_gogo",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/lightstep/lightstep-tracer-common/golang/gogo",
        sha256 = "1bf5cd77739238376e20a64307ef850da518861421a44ce7a10a27bc3bef4874",
        strip_prefix = "github.com/lightstep/lightstep-tracer-common/golang/gogo@v0.0.0-20190605223551-bc2310a04743",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/lightstep/lightstep-tracer-common/golang/gogo/com_github_lightstep_lightstep_tracer_common_golang_gogo-v0.0.0-20190605223551-bc2310a04743.zip",
        ],
    )
    go_repository(
        name = "com_github_lightstep_lightstep_tracer_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/lightstep/lightstep-tracer-go",
        sha256 = "b90e4c08ddd881bf09dfef53affd03c9d3b246edf64e055dbea549bd31268131",
        strip_prefix = "github.com/lightstep/lightstep-tracer-go@v0.18.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/lightstep/lightstep-tracer-go/com_github_lightstep_lightstep_tracer_go-v0.18.1.zip",
        ],
    )
    go_repository(
        name = "com_github_linkedin_goavro_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/linkedin/goavro/v2",
        sha256 = "5a0852f154b7dc68b999326cea27c9439faedbf8f09f94d1cb43e53a623f6bfc",
        strip_prefix = "github.com/linkedin/goavro/v2@v2.10.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/linkedin/goavro/v2/com_github_linkedin_goavro_v2-v2.10.0.zip",
        ],
    )
    go_repository(
        name = "com_github_linode_linodego",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/linode/linodego",
        sha256 = "b026a65eb731408b69aa6f51ecc25cc3d1e97dcc1f1e4e9dcd82d936472a4349",
        strip_prefix = "github.com/linode/linodego@v0.32.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/linode/linodego/com_github_linode_linodego-v0.32.0.zip",
        ],
    )
    go_repository(
        name = "com_github_lufia_iostat",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/lufia/iostat",
        sha256 = "964e9c5528a9de240d77d17df387bae1d59ddedd25734542ae8d70a27c59199e",
        strip_prefix = "github.com/lufia/iostat@v1.2.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/lufia/iostat/com_github_lufia_iostat-v1.2.1.zip",
        ],
    )
    go_repository(
        name = "com_github_lufia_plan9stats",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/lufia/plan9stats",
        sha256 = "94730432c565c238cb839bb4fa55bec3b3a19b592af0f7d1418a26a48f8359c6",
        strip_prefix = "github.com/lufia/plan9stats@v0.0.0-20211012122336-39d0f177ccd0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/lufia/plan9stats/com_github_lufia_plan9stats-v0.0.0-20211012122336-39d0f177ccd0.zip",
        ],
    )
    go_repository(
        name = "com_github_lyft_protoc_gen_star",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/lyft/protoc-gen-star",
        sha256 = "dabbc4c4e579794053260cdebc105e2f6d3c57002ee5fc3abbc03d5109c477cc",
        strip_prefix = "github.com/lyft/protoc-gen-star@v0.5.3",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/lyft/protoc-gen-star/com_github_lyft_protoc_gen_star-v0.5.3.zip",
        ],
    )
    go_repository(
        name = "com_github_lyft_protoc_gen_validate",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/lyft/protoc-gen-validate",
        sha256 = "86cd7276113087955c832bc1a2d7a8acfed59404375616d6753c4b284d4cd46c",
        strip_prefix = "github.com/lyft/protoc-gen-validate@v0.0.13",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/lyft/protoc-gen-validate/com_github_lyft_protoc_gen_validate-v0.0.13.zip",
        ],
    )
    go_repository(
        name = "com_github_magiconair_properties",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/magiconair/properties",
        sha256 = "fa056b3c72df6a36c991e9f22285818b07e377bf07c7beb441d9a097b2d6263e",
        strip_prefix = "github.com/magiconair/properties@v1.8.5",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/magiconair/properties/com_github_magiconair_properties-v1.8.5.zip",
        ],
    )
    go_repository(
        name = "com_github_mailru_easyjson",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mailru/easyjson",
        sha256 = "ebdd6d7a8b3d19c039b1a69b77c63b7039d2fe88bcf10b39278c8f7d6060e784",
        strip_prefix = "github.com/mailru/easyjson@v0.7.6",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/mailru/easyjson/com_github_mailru_easyjson-v0.7.6.zip",
        ],
    )
    go_repository(
        name = "com_github_markbates_oncer",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/markbates/oncer",
        sha256 = "959dec2377586af9c354b5667c303f0b506cb480b11f3ecdafc54ff1ec015e62",
        strip_prefix = "github.com/markbates/oncer@v0.0.0-20181203154359-bf2de49a0be2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/markbates/oncer/com_github_markbates_oncer-v0.0.0-20181203154359-bf2de49a0be2.zip",
        ],
    )
    go_repository(
        name = "com_github_markbates_safe",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/markbates/safe",
        sha256 = "d5a98e8242318d4e88844ddbbfebe91f67f41e5aa1f6a96a58fa2fa94e0ae9ef",
        strip_prefix = "github.com/markbates/safe@v1.0.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/markbates/safe/com_github_markbates_safe-v1.0.1.zip",
        ],
    )
    go_repository(
        name = "com_github_marstr_guid",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/marstr/guid",
        sha256 = "7db3cd8020c72ba260d1a20183bf5a030c696d6442eccaff2b31f72b194fc571",
        strip_prefix = "github.com/marstr/guid@v1.1.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/marstr/guid/com_github_marstr_guid-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_martini_contrib_auth",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/martini-contrib/auth",
        sha256 = "b3b2a267bb6ef227960e4391cc2bc868d0e6bceb2cb32372242c60f28f643cb2",
        strip_prefix = "github.com/martini-contrib/auth@v0.0.0-20150219114609-fa62c19b7ae8",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/martini-contrib/auth/com_github_martini_contrib_auth-v0.0.0-20150219114609-fa62c19b7ae8.zip",
        ],
    )
    go_repository(
        name = "com_github_martini_contrib_gzip",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/martini-contrib/gzip",
        sha256 = "803830ec3e7c75b135f0215579834192d01ce43da81934d903ed4ff9fa4dac9b",
        strip_prefix = "github.com/martini-contrib/gzip@v0.0.0-20151124214156-6c035326b43f",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/martini-contrib/gzip/com_github_martini_contrib_gzip-v0.0.0-20151124214156-6c035326b43f.zip",
        ],
    )
    go_repository(
        name = "com_github_martini_contrib_render",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/martini-contrib/render",
        sha256 = "2edd7f64b2f1f053f86a51856cd0f02b1f762af61a458a2e282dab76ad093d70",
        strip_prefix = "github.com/martini-contrib/render@v0.0.0-20150707142108-ec18f8345a11",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/martini-contrib/render/com_github_martini_contrib_render-v0.0.0-20150707142108-ec18f8345a11.zip",
        ],
    )
    go_repository(
        name = "com_github_maruel_panicparse_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/maruel/panicparse/v2",
        sha256 = "a51ff19f83c9a1e468b5d722d0857eb468d0f9baa29365e700c2e6ec24bd47a7",
        strip_prefix = "github.com/cockroachdb/panicparse/v2@v2.0.0-20211103220158-604c82a44f1e",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/cockroachdb/panicparse/v2/com_github_cockroachdb_panicparse_v2-v2.0.0-20211103220158-604c82a44f1e.zip",
        ],
    )
    go_repository(
        name = "com_github_marusama_semaphore",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/marusama/semaphore",
        sha256 = "2bc0cfc69824299ce542fd221820905ded92a3e236428f0f157887c081eb367d",
        strip_prefix = "github.com/marusama/semaphore@v0.0.0-20190110074507-6952cef993b2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/marusama/semaphore/com_github_marusama_semaphore-v0.0.0-20190110074507-6952cef993b2.zip",
        ],
    )
    go_repository(
        name = "com_github_masterminds_glide",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Masterminds/glide",
        sha256 = "566d15dde45716f355157ceee512833c7b91e9d25cb609a9bf30f811f61dded7",
        strip_prefix = "github.com/Masterminds/glide@v0.13.2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/Masterminds/glide/com_github_masterminds_glide-v0.13.2.zip",
        ],
    )
    go_repository(
        name = "com_github_masterminds_goutils",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Masterminds/goutils",
        sha256 = "b9520e8d2775ac1ff3fbf18c93dbc4b921133f957ae274f5b047965e9359d27d",
        strip_prefix = "github.com/Masterminds/goutils@v1.1.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/Masterminds/goutils/com_github_masterminds_goutils-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_masterminds_semver",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Masterminds/semver",
        sha256 = "15f6b54a695c15ffb205d5719e5ed50fab9ba9a739e1b4bdf3a0a319f51a7202",
        strip_prefix = "github.com/Masterminds/semver@v1.5.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/Masterminds/semver/com_github_masterminds_semver-v1.5.0.zip",
        ],
    )
    go_repository(
        name = "com_github_masterminds_semver_v3",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Masterminds/semver/v3",
        sha256 = "0a46c7403dfeda09b0821e851f8e1cec8f1ea4276281e42ea399da5bc5bf0704",
        strip_prefix = "github.com/Masterminds/semver/v3@v3.1.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/Masterminds/semver/v3/com_github_masterminds_semver_v3-v3.1.1.zip",
        ],
    )
    go_repository(
        name = "com_github_masterminds_sprig",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Masterminds/sprig",
        sha256 = "1b4d772334cc94e5703291b5f0fe4ac4965ac265424b1060baf18ef5ff9d845c",
        strip_prefix = "github.com/Masterminds/sprig@v2.22.0+incompatible",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/Masterminds/sprig/com_github_masterminds_sprig-v2.22.0+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_masterminds_vcs",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Masterminds/vcs",
        sha256 = "c89f09e1c75a80c29d8be82b75a1015b9e811d25d93617f773962f2765d7c9c7",
        strip_prefix = "github.com/Masterminds/vcs@v1.13.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/Masterminds/vcs/com_github_masterminds_vcs-v1.13.0.zip",
        ],
    )
    go_repository(
        name = "com_github_matryer_moq",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/matryer/moq",
        sha256 = "b9fb2bc3d0894dfaa3cc4298f49c97346ccb66f2f0e6911f4f224ffc9acc3972",
        strip_prefix = "github.com/matryer/moq@v0.0.0-20190312154309-6cfb0558e1bd",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/matryer/moq/com_github_matryer_moq-v0.0.0-20190312154309-6cfb0558e1bd.zip",
        ],
    )
    go_repository(
        name = "com_github_mattn_go_colorable",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mattn/go-colorable",
        sha256 = "2a78bab4fa8a66b3ae48fcd50fc5970db4011a8cee0a3ad7c13da0fb624c95e5",
        strip_prefix = "github.com/mattn/go-colorable@v0.1.11",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/mattn/go-colorable/com_github_mattn_go_colorable-v0.1.11.zip",
        ],
    )
    go_repository(
        name = "com_github_mattn_go_ieproxy",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mattn/go-ieproxy",
        sha256 = "2a1ad026c677bddef4136c76da19db2204d568b4c2246b2e00a79b47d8c01192",
        strip_prefix = "github.com/mattn/go-ieproxy@v0.0.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/mattn/go-ieproxy/com_github_mattn_go_ieproxy-v0.0.1.zip",
        ],
    )
    go_repository(
        name = "com_github_mattn_go_isatty",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mattn/go-isatty",
        sha256 = "6b8f017658d900de873fc567237548ef2054082aed64669655e51beeefee83c7",
        strip_prefix = "github.com/mattn/go-isatty@v0.0.14",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/mattn/go-isatty/com_github_mattn_go_isatty-v0.0.14.zip",
        ],
    )
    go_repository(
        name = "com_github_mattn_go_runewidth",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mattn/go-runewidth",
        sha256 = "030dcf3ea6cb39b34269f119ee5fe5e9d5834daf409e1f3f48c065a45326f4a6",
        strip_prefix = "github.com/mattn/go-runewidth@v0.0.10",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/mattn/go-runewidth/com_github_mattn_go_runewidth-v0.0.10.zip",
        ],
    )
    go_repository(
        name = "com_github_mattn_go_shellwords",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mattn/go-shellwords",
        sha256 = "d9b59db554053d4a244f9ca5c233773f7cf512778d95919c78dc47234eacceee",
        strip_prefix = "github.com/mattn/go-shellwords@v1.0.3",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/mattn/go-shellwords/com_github_mattn_go_shellwords-v1.0.3.zip",
        ],
    )
    go_repository(
        name = "com_github_mattn_go_sqlite3",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mattn/go-sqlite3",
        sha256 = "703e6cca7d67a5a886fab6c5f837e922e71566b215651c069f13ee137b2964b3",
        strip_prefix = "github.com/mattn/go-sqlite3@v1.14.6",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/mattn/go-sqlite3/com_github_mattn_go_sqlite3-v1.14.6.zip",
        ],
    )
    go_repository(
        name = "com_github_mattn_go_tty",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mattn/go-tty",
        sha256 = "e7384ae06bb54cc8f615d86e6397b11849be12c270d66460856f3fc6ad72aacb",
        strip_prefix = "github.com/mattn/go-tty@v0.0.0-20180907095812-13ff1204f104",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/mattn/go-tty/com_github_mattn_go_tty-v0.0.0-20180907095812-13ff1204f104.zip",
        ],
    )
    go_repository(
        name = "com_github_mattn_go_zglob",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mattn/go-zglob",
        sha256 = "8ef2dfc44aa352edd72e50287b7ac836c4c48fa439ca2648d8c1a4067f49e504",
        strip_prefix = "github.com/mattn/go-zglob@v0.0.3",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/mattn/go-zglob/com_github_mattn_go_zglob-v0.0.3.zip",
        ],
    )
    go_repository(
        name = "com_github_mattn_goveralls",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mattn/goveralls",
        sha256 = "3df5b7ebfb61edd9a098895aae7009a927a2fe91f73f38f48467a7b9e6c006f7",
        strip_prefix = "github.com/mattn/goveralls@v0.0.2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/mattn/goveralls/com_github_mattn_goveralls-v0.0.2.zip",
        ],
    )
    go_repository(
        name = "com_github_matttproud_golang_protobuf_extensions",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/matttproud/golang_protobuf_extensions",
        sha256 = "232df417d378e10bb1dcdd533c493647a247f62d9fe2b13c636d9647ff13384a",
        strip_prefix = "github.com/matttproud/golang_protobuf_extensions@v1.0.2-0.20181231171920-c182affec369",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/matttproud/golang_protobuf_extensions/com_github_matttproud_golang_protobuf_extensions-v1.0.2-0.20181231171920-c182affec369.zip",
        ],
    )
    go_repository(
        name = "com_github_mediocregopher_mediocre_go_lib",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mediocregopher/mediocre-go-lib",
        sha256 = "6b9950c36810c23dfe38c9de790da689af70811f520f161fc9325b202c71fab3",
        strip_prefix = "github.com/mediocregopher/mediocre-go-lib@v0.0.0-20181029021733-cb65787f37ed",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/mediocregopher/mediocre-go-lib/com_github_mediocregopher_mediocre_go_lib-v0.0.0-20181029021733-cb65787f37ed.zip",
        ],
    )
    go_repository(
        name = "com_github_mediocregopher_radix_v3",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mediocregopher/radix/v3",
        sha256 = "5be7566cd32610078fa12461b09b674061efb955b2400625ba8ebf3f6182c287",
        strip_prefix = "github.com/mediocregopher/radix/v3@v3.4.2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/mediocregopher/radix/v3/com_github_mediocregopher_radix_v3-v3.4.2.zip",
        ],
    )
    go_repository(
        name = "com_github_mgutz_ansi",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mgutz/ansi",
        sha256 = "2e0c063f9597cb225904292981732f10298e95aa22a1b815297e318ba103dc1d",
        strip_prefix = "github.com/mgutz/ansi@v0.0.0-20200706080929-d51e80ef957d",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/mgutz/ansi/com_github_mgutz_ansi-v0.0.0-20200706080929-d51e80ef957d.zip",
        ],
    )
    go_repository(
        name = "com_github_mibk_dupl",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mibk/dupl",
        sha256 = "73f61090c1cbee024b771fc60804cbedc5c2861f232bd34eff719afd9ac6e098",
        strip_prefix = "github.com/mibk/dupl@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/mibk/dupl/com_github_mibk_dupl-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_michaeltjones_walk",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/MichaelTJones/walk",
        sha256 = "fd1f0195976f587977eb26c9795f7989c02c0701bdff6f2d155a617ee69fdf6e",
        strip_prefix = "github.com/MichaelTJones/walk@v0.0.0-20161122175330-4748e29d5718",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/MichaelTJones/walk/com_github_michaeltjones_walk-v0.0.0-20161122175330-4748e29d5718.zip",
        ],
    )
    go_repository(
        name = "com_github_microcosm_cc_bluemonday",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/microcosm-cc/bluemonday",
        sha256 = "9cfac37098da75ab1c278740e8f0f7741891d8843e14afb256574596ad786f83",
        strip_prefix = "github.com/microcosm-cc/bluemonday@v1.0.2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/microcosm-cc/bluemonday/com_github_microcosm_cc_bluemonday-v1.0.2.zip",
        ],
    )
    go_repository(
        name = "com_github_microsoft_go_winio",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Microsoft/go-winio",
        sha256 = "d475d8f45f8e4de4c5f10fa8390a03e47596ea391dbbf5abdb1adc4f8bead50c",
        strip_prefix = "github.com/Microsoft/go-winio@v0.4.17",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/Microsoft/go-winio/com_github_microsoft_go_winio-v0.4.17.zip",
        ],
    )
    go_repository(
        name = "com_github_microsoft_hcsshim",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Microsoft/hcsshim",
        sha256 = "71a2ca0d284b6cd843db1b4b4ecd367af25194c1b66e4b50990c448706b0b777",
        strip_prefix = "github.com/Microsoft/hcsshim@v0.8.18",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/Microsoft/hcsshim/com_github_microsoft_hcsshim-v0.8.18.zip",
        ],
    )
    go_repository(
        name = "com_github_microsoft_hcsshim_test",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Microsoft/hcsshim/test",
        sha256 = "576f610aff477764e340aca7381f68ad980c410fee028f1d629c0cb6dc865fe9",
        strip_prefix = "github.com/Microsoft/hcsshim/test@v0.0.0-20210227013316-43a75bb4edd3",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/Microsoft/hcsshim/test/com_github_microsoft_hcsshim_test-v0.0.0-20210227013316-43a75bb4edd3.zip",
        ],
    )
    go_repository(
        name = "com_github_miekg_dns",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/miekg/dns",
        sha256 = "98eaddff5c30e475850f8f9c170bfb1adf33f0aaeeb280f71e77808a1dd902aa",
        strip_prefix = "github.com/miekg/dns@v1.1.43",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/miekg/dns/com_github_miekg_dns-v1.1.43.zip",
        ],
    )
    go_repository(
        name = "com_github_miekg_pkcs11",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/miekg/pkcs11",
        sha256 = "d06d401db6104517f860ebd4b523a7219fcf9cd16d7f54c930a62894f2067073",
        strip_prefix = "github.com/miekg/pkcs11@v1.0.3",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/miekg/pkcs11/com_github_miekg_pkcs11-v1.0.3.zip",
        ],
    )
    go_repository(
        name = "com_github_mileusna_useragent",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mileusna/useragent",
        sha256 = "169eabdbd206177d55bcf544ec99437d5e10cea4104f8d542aa16515202e584f",
        strip_prefix = "github.com/mileusna/useragent@v0.0.0-20190129205925-3e331f0949a5",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/mileusna/useragent/com_github_mileusna_useragent-v0.0.0-20190129205925-3e331f0949a5.zip",
        ],
    )
    go_repository(
        name = "com_github_minio_md5_simd",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/minio/md5-simd",
        sha256 = "f829d35a6e6897db415af8888c4b074d1a253aee0e8fb7054b4d95477a81c3d6",
        strip_prefix = "github.com/minio/md5-simd@v1.1.2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/minio/md5-simd/com_github_minio_md5_simd-v1.1.2.zip",
        ],
    )
    go_repository(
        name = "com_github_minio_minio_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/minio/minio-go",
        sha256 = "329d7e50f7e20014fa563aa8ff7a789106660e4b6fed87b2ca17fe3387cecb86",
        strip_prefix = "github.com/minio/minio-go@v0.0.0-20190131015406-c8a261de75c1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/minio/minio-go/com_github_minio_minio_go-v0.0.0-20190131015406-c8a261de75c1.zip",
        ],
    )
    go_repository(
        name = "com_github_minio_minio_go_v7",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/minio/minio-go/v7",
        sha256 = "826952231f5c7622b7c2d4b5180a4e12cf3379bd3842e1ef4934dfe115786218",
        strip_prefix = "github.com/minio/minio-go/v7@v7.0.21",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/minio/minio-go/v7/com_github_minio_minio_go_v7-v7.0.21.zip",
        ],
    )
    go_repository(
        name = "com_github_minio_sha256_simd",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/minio/sha256-simd",
        sha256 = "62edc1481390c3421ff5a54b80c49acab85331348124d560a5e410074d19c3e5",
        strip_prefix = "github.com/minio/sha256-simd@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/minio/sha256-simd/com_github_minio_sha256_simd-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_mistifyio_go_zfs",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mistifyio/go-zfs",
        sha256 = "fd1f35f187aa04233a178daa1158039578bcb4966b9f038e4d27a6fff2ea3503",
        strip_prefix = "github.com/mistifyio/go-zfs@v2.1.2-0.20190413222219-f784269be439+incompatible",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/mistifyio/go-zfs/com_github_mistifyio_go_zfs-v2.1.2-0.20190413222219-f784269be439+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_mitchellh_cli",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mitchellh/cli",
        sha256 = "521d0ee631576325f75092c56264b4f310bba32b46999ed2024d937dd3a41824",
        strip_prefix = "github.com/mitchellh/cli@v1.1.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/mitchellh/cli/com_github_mitchellh_cli-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_mitchellh_copystructure",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mitchellh/copystructure",
        sha256 = "4a2c9eb367a7781864e8edbd3b11781897766bcf6120f77a717d54a575392eee",
        strip_prefix = "github.com/mitchellh/copystructure@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/mitchellh/copystructure/com_github_mitchellh_copystructure-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_mitchellh_go_homedir",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mitchellh/go-homedir",
        sha256 = "fffec361fc7e776bb71433560c285ee2982d2c140b8f5bfba0db6033c0ade184",
        strip_prefix = "github.com/mitchellh/go-homedir@v1.1.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/mitchellh/go-homedir/com_github_mitchellh_go_homedir-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_mitchellh_go_ps",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mitchellh/go-ps",
        sha256 = "f2f0400b1d5e136419daed275c27a930b0f5447ac12bb8acd3ddbe39547b2834",
        strip_prefix = "github.com/mitchellh/go-ps@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/mitchellh/go-ps/com_github_mitchellh_go_ps-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_mitchellh_go_testing_interface",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mitchellh/go-testing-interface",
        sha256 = "3af316747f951819b19cf55fbaaa592f1d3f19ab078e183c6cd7ca591e6791a8",
        strip_prefix = "github.com/mitchellh/go-testing-interface@v1.14.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/mitchellh/go-testing-interface/com_github_mitchellh_go_testing_interface-v1.14.0.zip",
        ],
    )
    go_repository(
        name = "com_github_mitchellh_go_wordwrap",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mitchellh/go-wordwrap",
        sha256 = "9ea185f97dfe616da351b63b229a5a212b14ac0e23bd3f943e39590eadb38031",
        strip_prefix = "github.com/mitchellh/go-wordwrap@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/mitchellh/go-wordwrap/com_github_mitchellh_go_wordwrap-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_mitchellh_gox",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mitchellh/gox",
        sha256 = "70c976edc82b069d55c4b05409be9e91d85c20238a5e38c60fbb0b03b43c9550",
        strip_prefix = "github.com/mitchellh/gox@v0.4.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/mitchellh/gox/com_github_mitchellh_gox-v0.4.0.zip",
        ],
    )
    go_repository(
        name = "com_github_mitchellh_iochan",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mitchellh/iochan",
        sha256 = "f3eede01adb24c22945bf71b4f84ae25e3744a12b9d8bd7c016705adc0d778b8",
        strip_prefix = "github.com/mitchellh/iochan@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/mitchellh/iochan/com_github_mitchellh_iochan-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_mitchellh_mapstructure",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mitchellh/mapstructure",
        sha256 = "bfe856861145c25f6f26c2985760e86a4634e2a28bb9b4582ff0e8a9d2621e10",
        strip_prefix = "github.com/mitchellh/mapstructure@v1.4.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/mitchellh/mapstructure/com_github_mitchellh_mapstructure-v1.4.1.zip",
        ],
    )
    go_repository(
        name = "com_github_mitchellh_osext",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mitchellh/osext",
        sha256 = "d8e6e5f6bd749cfa0c1c17c40f5dc0fd19e4a0a83245f46bde23bea4e65d1a20",
        strip_prefix = "github.com/mitchellh/osext@v0.0.0-20151018003038-5e2d6d41470f",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/mitchellh/osext/com_github_mitchellh_osext-v0.0.0-20151018003038-5e2d6d41470f.zip",
        ],
    )
    go_repository(
        name = "com_github_mitchellh_reflectwalk",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mitchellh/reflectwalk",
        sha256 = "318ab84e22d4554a7540c7ebc9b4fb607e2608578c3a5bb72434203988048145",
        strip_prefix = "github.com/mitchellh/reflectwalk@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/mitchellh/reflectwalk/com_github_mitchellh_reflectwalk-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_mjibson_esc",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mjibson/esc",
        sha256 = "9f090786bd43dddb5c0d798b449d5e8aede4cb7d106f56dcac0aebd8fd1929cc",
        strip_prefix = "github.com/mjibson/esc@v0.2.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/mjibson/esc/com_github_mjibson_esc-v0.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_mmatczuk_go_generics",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mmatczuk/go_generics",
        sha256 = "18c1e95c93f1f82be0184bc13bf49eb4350c7a4ff524b1bf440b3eb9ff14acc9",
        strip_prefix = "github.com/mmatczuk/go_generics@v0.0.0-20181212143635-0aaa050f9bab",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/mmatczuk/go_generics/com_github_mmatczuk_go_generics-v0.0.0-20181212143635-0aaa050f9bab.zip",
        ],
    )
    go_repository(
        name = "com_github_mmcloughlin_geohash",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mmcloughlin/geohash",
        sha256 = "7162856858d9bb3c411d4b42ad19dfff579341ddf0580122e3f1ac3be05c7441",
        strip_prefix = "github.com/mmcloughlin/geohash@v0.9.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/mmcloughlin/geohash/com_github_mmcloughlin_geohash-v0.9.0.zip",
        ],
    )
    go_repository(
        name = "com_github_moby_locker",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/moby/locker",
        sha256 = "f07361346d12a24e168db7fb2f21281883bee6060f1aedf7507bccf20c4a793f",
        strip_prefix = "github.com/moby/locker@v1.0.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/moby/locker/com_github_moby_locker-v1.0.1.zip",
        ],
    )
    go_repository(
        name = "com_github_moby_spdystream",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/moby/spdystream",
        sha256 = "9db6d001a80f4c3cb332bb8a1bb9260908e1ffa9a20491e9bc05358263eed278",
        strip_prefix = "github.com/moby/spdystream@v0.2.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/moby/spdystream/com_github_moby_spdystream-v0.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_moby_sys_mountinfo",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/moby/sys/mountinfo",
        sha256 = "0f32aa52a3be8bf724fe26307d144ffc4155bebbf1f4445388cc5fe9fe33bcc6",
        strip_prefix = "github.com/moby/sys/mountinfo@v0.4.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/moby/sys/mountinfo/com_github_moby_sys_mountinfo-v0.4.1.zip",
        ],
    )
    go_repository(
        name = "com_github_moby_sys_symlink",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/moby/sys/symlink",
        sha256 = "55ff27f7b35dc22c63c5ea4d79a127d45a18df2fcd2f22ca16f9a424c4b4cf51",
        strip_prefix = "github.com/moby/sys/symlink@v0.1.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/moby/sys/symlink/com_github_moby_sys_symlink-v0.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_moby_term",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/moby/term",
        sha256 = "f01dbf40f90d9023cd67543a3bc2d64c58ac9f4d931e1ba5bebd80ce9ab9ca47",
        strip_prefix = "github.com/moby/term@v0.0.0-20201216013528-df9cb8a40635",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/moby/term/com_github_moby_term-v0.0.0-20201216013528-df9cb8a40635.zip",
        ],
    )
    go_repository(
        name = "com_github_modern_go_concurrent",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/modern-go/concurrent",
        sha256 = "91ef49599bec459869d94ff3dec128871ab66bd2dfa61041f1e1169f9b4a8073",
        strip_prefix = "github.com/modern-go/concurrent@v0.0.0-20180306012644-bacd9c7ef1dd",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/modern-go/concurrent/com_github_modern_go_concurrent-v0.0.0-20180306012644-bacd9c7ef1dd.zip",
        ],
    )
    go_repository(
        name = "com_github_modern_go_reflect2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/modern-go/reflect2",
        sha256 = "f46f41409c2e74293f82cfe6c70b5d582bff8ada0106a7d3ff5706520c50c21c",
        strip_prefix = "github.com/modern-go/reflect2@v1.0.2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/modern-go/reflect2/com_github_modern_go_reflect2-v1.0.2.zip",
        ],
    )
    go_repository(
        name = "com_github_montanaflynn_stats",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/montanaflynn/stats",
        sha256 = "25069347054502d9ab97531f0757b916124ba9966ead38f36f98812b37a6acd9",
        strip_prefix = "github.com/montanaflynn/stats@v0.6.3",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/montanaflynn/stats/com_github_montanaflynn_stats-v0.6.3.zip",
        ],
    )
    go_repository(
        name = "com_github_morikuni_aec",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/morikuni/aec",
        sha256 = "c14eeff6945b854edd8b91a83ac760fbd95068f33dc17d102c18f2e8e86bcced",
        strip_prefix = "github.com/morikuni/aec@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/morikuni/aec/com_github_morikuni_aec-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_mostynb_go_grpc_compression",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mostynb/go-grpc-compression",
        sha256 = "a260a65018fbde39f8b3b996bbb1b6f76f1ea5db26f8892842b249ba7cd5f318",
        strip_prefix = "github.com/mostynb/go-grpc-compression@v1.1.12",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/mostynb/go-grpc-compression/com_github_mostynb_go_grpc_compression-v1.1.12.zip",
        ],
    )
    go_repository(
        name = "com_github_moul_http2curl",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/moul/http2curl",
        sha256 = "3600be3621038727f856bf7403d3ef0ffcc2a6729716bab67b592dcd19b3fee2",
        strip_prefix = "github.com/moul/http2curl@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/moul/http2curl/com_github_moul_http2curl-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_mozilla_tls_observatory",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mozilla/tls-observatory",
        sha256 = "0798e35f31fdea023c3ded1e0d217295d932ed47f628c7e7c08f54e03da98ca8",
        strip_prefix = "github.com/mozilla/tls-observatory@v0.0.0-20190404164649-a3c1b6cfecfd",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/mozilla/tls-observatory/com_github_mozilla_tls_observatory-v0.0.0-20190404164649-a3c1b6cfecfd.zip",
        ],
    )
    go_repository(
        name = "com_github_mrunalp_fileutils",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mrunalp/fileutils",
        sha256 = "202a6e33b519ddcbece708c3779845114bb7324d4c9ff9899f7c4f80f1e7b1bf",
        strip_prefix = "github.com/mrunalp/fileutils@v0.5.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/mrunalp/fileutils/com_github_mrunalp_fileutils-v0.5.0.zip",
        ],
    )
    go_repository(
        name = "com_github_mschoch_smat",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mschoch/smat",
        sha256 = "488e193897c7d8e3b3758cbeb8a5bc1b58b9619f3f14288a2ea9e0baa5ed9b3e",
        strip_prefix = "github.com/mschoch/smat@v0.0.0-20160514031455-90eadee771ae",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/mschoch/smat/com_github_mschoch_smat-v0.0.0-20160514031455-90eadee771ae.zip",
        ],
    )
    go_repository(
        name = "com_github_munnerz_goautoneg",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/munnerz/goautoneg",
        sha256 = "3d7ce17916779890be02ea6b3dd6345c3c30c1df502ad9d8b5b9b310e636afd9",
        strip_prefix = "github.com/munnerz/goautoneg@v0.0.0-20191010083416-a7dc8b61c822",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/munnerz/goautoneg/com_github_munnerz_goautoneg-v0.0.0-20191010083416-a7dc8b61c822.zip",
        ],
    )
    go_repository(
        name = "com_github_mwitkow_go_conntrack",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mwitkow/go-conntrack",
        sha256 = "d6fc513490d5c73e3f64ede3cf18ba973a4f8ef4c39c9816cc6080e39c8c480a",
        strip_prefix = "github.com/mwitkow/go-conntrack@v0.0.0-20190716064945-2f068394615f",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/mwitkow/go-conntrack/com_github_mwitkow_go_conntrack-v0.0.0-20190716064945-2f068394615f.zip",
        ],
    )
    go_repository(
        name = "com_github_mwitkow_go_proto_validators",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mwitkow/go-proto-validators",
        sha256 = "a2a5bbb770b5455f12a1ed512704db70f845dfdf29bf96b641e66afbc0893c5e",
        strip_prefix = "github.com/mwitkow/go-proto-validators@v0.0.0-20180403085117-0950a7990007",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/mwitkow/go-proto-validators/com_github_mwitkow_go_proto_validators-v0.0.0-20180403085117-0950a7990007.zip",
        ],
    )
    go_repository(
        name = "com_github_mxk_go_flowrate",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/mxk/go-flowrate",
        sha256 = "bd0701ef9115469a661c07a3e9c2e572114126eb2d098b01eda34ebf62548492",
        strip_prefix = "github.com/mxk/go-flowrate@v0.0.0-20140419014527-cca7078d478f",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/mxk/go-flowrate/com_github_mxk_go_flowrate-v0.0.0-20140419014527-cca7078d478f.zip",
        ],
    )
    go_repository(
        name = "com_github_nats_io_jwt",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/nats-io/jwt",
        sha256 = "d0ab8bb735649df606874454eafc44c9d0b5ae41a0453875260c351e209b1719",
        strip_prefix = "github.com/nats-io/jwt@v0.3.2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/nats-io/jwt/com_github_nats_io_jwt-v0.3.2.zip",
        ],
    )
    go_repository(
        name = "com_github_nats_io_nats_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/nats-io/nats.go",
        sha256 = "34a735d158d70685faad1fc3153f08da0ddc21c0ae42f6a0cb09430d638364b2",
        strip_prefix = "github.com/nats-io/nats.go@v1.9.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/nats-io/nats.go/com_github_nats_io_nats_go-v1.9.1.zip",
        ],
    )
    go_repository(
        name = "com_github_nats_io_nats_server_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/nats-io/nats-server/v2",
        sha256 = "cf6c4affe3eae3f43b67b5ecc401f41819280136d6ef9209198a5f44b62d3280",
        strip_prefix = "github.com/nats-io/nats-server/v2@v2.1.2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/nats-io/nats-server/v2/com_github_nats_io_nats_server_v2-v2.1.2.zip",
        ],
    )
    go_repository(
        name = "com_github_nats_io_nkeys",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/nats-io/nkeys",
        sha256 = "291930a7abcd84edcaffc2cadc75aeb830ebf561144313ead6b8e1fcc03b124f",
        strip_prefix = "github.com/nats-io/nkeys@v0.1.3",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/nats-io/nkeys/com_github_nats_io_nkeys-v0.1.3.zip",
        ],
    )
    go_repository(
        name = "com_github_nats_io_nuid",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/nats-io/nuid",
        sha256 = "809d144fbd16f91651a433e28d2008d339e19dafc450c5995e2ed92f1c17c1f3",
        strip_prefix = "github.com/nats-io/nuid@v1.0.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/nats-io/nuid/com_github_nats_io_nuid-v1.0.1.zip",
        ],
    )
    go_repository(
        name = "com_github_nbutton23_zxcvbn_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/nbutton23/zxcvbn-go",
        sha256 = "d9a08288eb990834ea161adbd57757a449d664ee254dc8c33444663e6596f4d8",
        strip_prefix = "github.com/nbutton23/zxcvbn-go@v0.0.0-20180912185939-ae427f1e4c1d",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/nbutton23/zxcvbn-go/com_github_nbutton23_zxcvbn_go-v0.0.0-20180912185939-ae427f1e4c1d.zip",
        ],
    )
    go_repository(
        name = "com_github_ncw_swift",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ncw/swift",
        sha256 = "38cc53277c66456f267963ad9613cd168f252d9bef58de95dcee5202ceecb3e3",
        strip_prefix = "github.com/ncw/swift@v1.0.47",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/ncw/swift/com_github_ncw_swift-v1.0.47.zip",
        ],
    )
    go_repository(
        name = "com_github_ngdinhtoan_glide_cleanup",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ngdinhtoan/glide-cleanup",
        sha256 = "e008e980d1a5335baaae1d10df2786ea1aea0d9774f8a46d19886a828edde4f3",
        strip_prefix = "github.com/ngdinhtoan/glide-cleanup@v0.2.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/ngdinhtoan/glide-cleanup/com_github_ngdinhtoan_glide_cleanup-v0.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_niemeyer_pretty",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/niemeyer/pretty",
        sha256 = "2dcb7053faf11c28cad7d84fcfa3dd7f93e3d236b39d83cff0934f691f860d7a",
        strip_prefix = "github.com/niemeyer/pretty@v0.0.0-20200227124842-a10e7caefd8e",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/niemeyer/pretty/com_github_niemeyer_pretty-v0.0.0-20200227124842-a10e7caefd8e.zip",
        ],
    )
    go_repository(
        name = "com_github_nishanths_predeclared",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/nishanths/predeclared",
        sha256 = "f3a40ab7d3e0570570e7bc41a6cc7b08b3e23df5ef5f08553ef622a3752d6e03",
        strip_prefix = "github.com/nishanths/predeclared@v0.0.0-20200524104333-86fad755b4d3",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/nishanths/predeclared/com_github_nishanths_predeclared-v0.0.0-20200524104333-86fad755b4d3.zip",
        ],
    )
    go_repository(
        name = "com_github_nkovacs_streamquote",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/nkovacs/streamquote",
        sha256 = "679a789b4b1409ea81054cb12e5f8441199f5fb17d4a2d3510c51f3aa5f3f0cc",
        strip_prefix = "github.com/nkovacs/streamquote@v0.0.0-20170412213628-49af9bddb229",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/nkovacs/streamquote/com_github_nkovacs_streamquote-v0.0.0-20170412213628-49af9bddb229.zip",
        ],
    )
    go_repository(
        name = "com_github_nvveen_gotty",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Nvveen/Gotty",
        sha256 = "362ac7b59d74231419471b65b60079d167785b97fd4aa0de71575088cd192b1e",
        strip_prefix = "github.com/Nvveen/Gotty@v0.0.0-20120604004816-cd527374f1e5",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/Nvveen/Gotty/com_github_nvveen_gotty-v0.0.0-20120604004816-cd527374f1e5.zip",
        ],
    )
    go_repository(
        name = "com_github_nxadm_tail",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/nxadm/tail",
        sha256 = "c9bb9d05b3afd1bacc35e7d305a22b07cd7db38f5fabd4ccd95a9227c5709890",
        strip_prefix = "github.com/nxadm/tail@v1.4.4",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/nxadm/tail/com_github_nxadm_tail-v1.4.4.zip",
        ],
    )
    go_repository(
        name = "com_github_nytimes_gziphandler",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/NYTimes/gziphandler",
        sha256 = "0c2cce989a022621fd3445948136b5ffab40e36a9ef167fc5d6df79077359e50",
        strip_prefix = "github.com/NYTimes/gziphandler@v0.0.0-20170623195520-56545f4a5d46",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/NYTimes/gziphandler/com_github_nytimes_gziphandler-v0.0.0-20170623195520-56545f4a5d46.zip",
        ],
    )
    go_repository(
        name = "com_github_oklog_oklog",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/oklog/oklog",
        sha256 = "b37d032de5b0dd5e96063c06b77fcb29a692a07bd52a4d99a361f2fef68822ec",
        strip_prefix = "github.com/oklog/oklog@v0.3.2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/oklog/oklog/com_github_oklog_oklog-v0.3.2.zip",
        ],
    )
    go_repository(
        name = "com_github_oklog_run",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/oklog/run",
        sha256 = "d6f69fc71aa155043f926c2a98fc1e5b3a8ebab422f2f36d785cfba38a7ebee4",
        strip_prefix = "github.com/oklog/run@v1.1.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/oklog/run/com_github_oklog_run-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_oklog_ulid",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/oklog/ulid",
        sha256 = "40e502c064a922d5eb7f2bc2cda9c6a2a929ec0fc76c9aae4db54fb7b6b611ae",
        strip_prefix = "github.com/oklog/ulid@v1.3.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/oklog/ulid/com_github_oklog_ulid-v1.3.1.zip",
        ],
    )
    go_repository(
        name = "com_github_olekukonko_tablewriter",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/olekukonko/tablewriter",
        sha256 = "79daf1c29ec50cdd8dd1ea33f8a814963646a45a2ebe22742d652579340ebde0",
        strip_prefix = "github.com/cockroachdb/tablewriter@v0.0.5-0.20200105123400-bd15540e8847",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/cockroachdb/tablewriter/com_github_cockroachdb_tablewriter-v0.0.5-0.20200105123400-bd15540e8847.zip",
        ],
    )
    go_repository(
        name = "com_github_olivere_elastic",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/olivere/elastic",
        sha256 = "e8b5e1de9482bf73e64bbec74f9f19c6b2fb7b41c6a3f129634d7ce4d4d8b9f0",
        strip_prefix = "github.com/olivere/elastic@v6.2.27+incompatible",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/olivere/elastic/com_github_olivere_elastic-v6.2.27+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_oneofone_xxhash",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/OneOfOne/xxhash",
        sha256 = "b4a7d4dd033e96312c06b43d42a6425e00837c0254741bcd569c9a0909b26f9d",
        strip_prefix = "github.com/OneOfOne/xxhash@v1.2.2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/OneOfOne/xxhash/com_github_oneofone_xxhash-v1.2.2.zip",
        ],
    )
    go_repository(
        name = "com_github_onsi_ginkgo",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/onsi/ginkgo",
        sha256 = "adfa06497641d0609d7b6e72a6087f898a0912cae0c2e86da90dace1b62045d4",
        strip_prefix = "github.com/onsi/ginkgo@v1.14.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/onsi/ginkgo/com_github_onsi_ginkgo-v1.14.0.zip",
        ],
    )
    go_repository(
        name = "com_github_onsi_gomega",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/onsi/gomega",
        sha256 = "2c3224f2611b61f9b966394ebb39ef3244c3419c813fe87fd1a80f60e94a6872",
        strip_prefix = "github.com/onsi/gomega@v1.10.3",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/onsi/gomega/com_github_onsi_gomega-v1.10.3.zip",
        ],
    )
    go_repository(
        name = "com_github_op_go_logging",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/op/go-logging",
        sha256 = "c506eace74028656eb28677a4c162f9c023ce2f9c0207354ba80cca89f11b461",
        strip_prefix = "github.com/op/go-logging@v0.0.0-20160315200505-970db520ece7",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/op/go-logging/com_github_op_go_logging-v0.0.0-20160315200505-970db520ece7.zip",
        ],
    )
    go_repository(
        name = "com_github_opencontainers_go_digest",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/opencontainers/go-digest",
        sha256 = "615efb31ff6cd71035b8aa38c3659d8b4da46f3cd92ac807cb50449adfe37c86",
        strip_prefix = "github.com/opencontainers/go-digest@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/opencontainers/go-digest/com_github_opencontainers_go_digest-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_opencontainers_image_spec",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/opencontainers/image-spec",
        sha256 = "ebb2dca711a137fbfb717158b0368792f834000f4308d9ea259d06c6804c677c",
        strip_prefix = "github.com/opencontainers/image-spec@v1.0.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/opencontainers/image-spec/com_github_opencontainers_image_spec-v1.0.1.zip",
        ],
    )
    go_repository(
        name = "com_github_opencontainers_runc",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/opencontainers/runc",
        sha256 = "a93e8183c3abf68deec3958c5c88382c7dfc9403b8ba6fe3467b6456373864a3",
        strip_prefix = "github.com/opencontainers/runc@v1.0.0-rc93",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/opencontainers/runc/com_github_opencontainers_runc-v1.0.0-rc93.zip",
        ],
    )
    go_repository(
        name = "com_github_opencontainers_runtime_spec",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/opencontainers/runtime-spec",
        sha256 = "b5b116e59245c3a7ce1b750db1a2140eae0a448fc57023c4de81e1fd00bf586e",
        strip_prefix = "github.com/opencontainers/runtime-spec@v1.0.3-0.20200929063507-e6143ca7d51d",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/opencontainers/runtime-spec/com_github_opencontainers_runtime_spec-v1.0.3-0.20200929063507-e6143ca7d51d.zip",
        ],
    )
    go_repository(
        name = "com_github_opencontainers_runtime_tools",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/opencontainers/runtime-tools",
        sha256 = "49e4ed2cb59461d3af837a3d624096b1fb8f3f0aa021e11c2d3025cca83d862f",
        strip_prefix = "github.com/opencontainers/runtime-tools@v0.0.0-20181011054405-1d69bd0f9c39",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/opencontainers/runtime-tools/com_github_opencontainers_runtime_tools-v0.0.0-20181011054405-1d69bd0f9c39.zip",
        ],
    )
    go_repository(
        name = "com_github_opencontainers_selinux",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/opencontainers/selinux",
        sha256 = "3f052d891fccea2ec3b7118187cacdf45003167e5ca123058975d14cd51aa095",
        strip_prefix = "github.com/opencontainers/selinux@v1.8.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/opencontainers/selinux/com_github_opencontainers_selinux-v1.8.0.zip",
        ],
    )
    go_repository(
        name = "com_github_opennota_wd",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/opennota/wd",
        sha256 = "f7976f0639a473690c38576162f76c9b253e0867e6dea81257375baa56cf79e9",
        strip_prefix = "github.com/opennota/wd@v0.0.0-20180911144301-b446539ab1e7",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/opennota/wd/com_github_opennota_wd-v0.0.0-20180911144301-b446539ab1e7.zip",
        ],
    )
    go_repository(
        name = "com_github_opentracing_basictracer_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/opentracing/basictracer-go",
        sha256 = "a908957c8e55b7b036b4761fb64c643806fcb9b59d4e7c6fcd03fca1105a9156",
        strip_prefix = "github.com/opentracing/basictracer-go@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/opentracing/basictracer-go/com_github_opentracing_basictracer_go-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_opentracing_contrib_go_grpc",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/opentracing-contrib/go-grpc",
        sha256 = "51f4dabc672c3d1f4c91eb09f033bab42620e55ee477c01751adeee0b6524f89",
        strip_prefix = "github.com/opentracing-contrib/go-grpc@v0.0.0-20180928155321-4b5a12d3ff02",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/opentracing-contrib/go-grpc/com_github_opentracing_contrib_go_grpc-v0.0.0-20180928155321-4b5a12d3ff02.zip",
        ],
    )
    go_repository(
        name = "com_github_opentracing_contrib_go_observer",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/opentracing-contrib/go-observer",
        sha256 = "50023eee1ef04412410f43d8b5dcf3ef481c0fc39067add27799654705fa84b2",
        strip_prefix = "github.com/opentracing-contrib/go-observer@v0.0.0-20170622124052-a52f23424492",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/opentracing-contrib/go-observer/com_github_opentracing_contrib_go_observer-v0.0.0-20170622124052-a52f23424492.zip",
        ],
    )
    go_repository(
        name = "com_github_opentracing_contrib_go_stdlib",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/opentracing-contrib/go-stdlib",
        sha256 = "4ed9796a724963db8c0f052747a86262faa16b46a67a794cdda2814f47736a44",
        strip_prefix = "github.com/opentracing-contrib/go-stdlib@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/opentracing-contrib/go-stdlib/com_github_opentracing_contrib_go_stdlib-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_opentracing_opentracing_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/opentracing/opentracing-go",
        sha256 = "9b1a75e9a454a0cf01a26c18e48cd321e3b300943ac5adb9098ba033dbd40db5",
        strip_prefix = "github.com/opentracing/opentracing-go@v1.2.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/opentracing/opentracing-go/com_github_opentracing_opentracing_go-v1.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_openzipkin_contrib_zipkin_go_opentracing",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/openzipkin-contrib/zipkin-go-opentracing",
        sha256 = "74763b01a30fa2f7116f0408c792b4db50bb01200cfe5f3f8b351ac638d1adb4",
        strip_prefix = "github.com/openzipkin-contrib/zipkin-go-opentracing@v0.4.5",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/openzipkin-contrib/zipkin-go-opentracing/com_github_openzipkin_contrib_zipkin_go_opentracing-v0.4.5.zip",
        ],
    )
    go_repository(
        name = "com_github_openzipkin_zipkin_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/openzipkin/zipkin-go",
        sha256 = "337535c088bd6f7a479e21747044286f66490871948989d52f7812bc4cca955e",
        strip_prefix = "github.com/openzipkin/zipkin-go@v0.2.5",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/openzipkin/zipkin-go/com_github_openzipkin_zipkin_go-v0.2.5.zip",
        ],
    )
    go_repository(
        name = "com_github_ory_dockertest_v3",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ory/dockertest/v3",
        sha256 = "afc2d1a0dc4f70add02e7dbd682646b749fad48dffd69c252bfb44d419f7dc6f",
        strip_prefix = "github.com/ory/dockertest/v3@v3.6.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/ory/dockertest/v3/com_github_ory_dockertest_v3-v3.6.0.zip",
        ],
    )
    go_repository(
        name = "com_github_oxtoacart_bpool",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/oxtoacart/bpool",
        sha256 = "6816ec3a6f197cbee0ba6ddb9ec70958bc28870e59864b24e43da0c858079a1b",
        strip_prefix = "github.com/oxtoacart/bpool@v0.0.0-20190530202638-03653db5a59c",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/oxtoacart/bpool/com_github_oxtoacart_bpool-v0.0.0-20190530202638-03653db5a59c.zip",
        ],
    )
    go_repository(
        name = "com_github_pact_foundation_pact_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pact-foundation/pact-go",
        sha256 = "e753f63d70bf56300c60fe87817d04935bd41693fef06d273ec70014cccabd3b",
        strip_prefix = "github.com/pact-foundation/pact-go@v1.0.4",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/pact-foundation/pact-go/com_github_pact_foundation_pact_go-v1.0.4.zip",
        ],
    )
    go_repository(
        name = "com_github_pascaldekloe_goe",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pascaldekloe/goe",
        sha256 = "37b73886f1eec9b093143e7b03f547b90ab55d8d5c9aa3966e90f9df2d07353c",
        strip_prefix = "github.com/pascaldekloe/goe@v0.1.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/pascaldekloe/goe/com_github_pascaldekloe_goe-v0.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_patrickmn_go_cache",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/patrickmn/go-cache",
        sha256 = "d5d1c13e3c9cfeb04a943f656333ec68627dd6ce136af67e2aa5881ad7353c55",
        strip_prefix = "github.com/patrickmn/go-cache@v2.1.0+incompatible",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/patrickmn/go-cache/com_github_patrickmn_go_cache-v2.1.0+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_paulbellamy_ratecounter",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/paulbellamy/ratecounter",
        sha256 = "fb012856582335cdac02ee17c08692d75d539158a82eda3a26fb8a51d4ef27e6",
        strip_prefix = "github.com/paulbellamy/ratecounter@v0.2.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/paulbellamy/ratecounter/com_github_paulbellamy_ratecounter-v0.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_pborman_uuid",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pborman/uuid",
        sha256 = "b888ff5d33651a1f5f6b8094acc434dd6dc284e2fe5052754a7993cebd539437",
        strip_prefix = "github.com/pborman/uuid@v1.2.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/pborman/uuid/com_github_pborman_uuid-v1.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_pelletier_go_toml",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pelletier/go-toml",
        sha256 = "a52f4a0d5d8f6e98bbfc493ec947e1be44b8f304005d2059ecc4c5938b712961",
        strip_prefix = "github.com/pelletier/go-toml@v1.9.3",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/pelletier/go-toml/com_github_pelletier_go_toml-v1.9.3.zip",
        ],
    )
    go_repository(
        name = "com_github_performancecopilot_speed",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/performancecopilot/speed",
        sha256 = "44150a760ccfe232d3ce6bf40e537342d01f78ddac18b795f623d004257c00b0",
        strip_prefix = "github.com/performancecopilot/speed@v3.0.0+incompatible",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/performancecopilot/speed/com_github_performancecopilot_speed-v3.0.0+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_peterbourgon_diskv",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/peterbourgon/diskv",
        sha256 = "1eeff260bd1ad71cd1611078995db99e1c7eba28628e7d6f24c79039536ea1cb",
        strip_prefix = "github.com/peterbourgon/diskv@v2.0.1+incompatible",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/peterbourgon/diskv/com_github_peterbourgon_diskv-v2.0.1+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_peterbourgon_g2s",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/peterbourgon/g2s",
        sha256 = "41526f42b4fe3019581ab3745afea18271d7f037eb55a6e9fb3e32fd09ff9b8d",
        strip_prefix = "github.com/peterbourgon/g2s@v0.0.0-20170223122336-d4e7ad98afea",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/peterbourgon/g2s/com_github_peterbourgon_g2s-v0.0.0-20170223122336-d4e7ad98afea.zip",
        ],
    )
    go_repository(
        name = "com_github_peterh_liner",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/peterh/liner",
        sha256 = "0d96c450f9c55a8102f4ae7fd8a583ebfaeba23e3939d6b6284306a82a21430f",
        strip_prefix = "github.com/peterh/liner@v1.0.1-0.20180619022028-8c1271fcf47f",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/peterh/liner/com_github_peterh_liner-v1.0.1-0.20180619022028-8c1271fcf47f.zip",
        ],
    )
    go_repository(
        name = "com_github_petermattis_goid",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/petermattis/goid",
        sha256 = "9f536c5d39d6a3c851670ec585e1c876fe31f3402556d215ebbaffcecbacb30a",
        strip_prefix = "github.com/petermattis/goid@v0.0.0-20211229010228-4d14c490ee36",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/petermattis/goid/com_github_petermattis_goid-v0.0.0-20211229010228-4d14c490ee36.zip",
        ],
    )
    go_repository(
        name = "com_github_philhofer_fwd",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/philhofer/fwd",
        sha256 = "b4e79b1f5fdfe8c44bf6dae3dd593c62862930114411a30968f304084de1d0b3",
        strip_prefix = "github.com/philhofer/fwd@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/philhofer/fwd/com_github_philhofer_fwd-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_pierrec_lz4",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pierrec/lz4",
        sha256 = "3442d00156073088858844e4e25753b683ef1d39a5ff9f912079f7f8a1d72a61",
        strip_prefix = "github.com/pierrec/lz4@v2.6.0+incompatible",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/pierrec/lz4/com_github_pierrec_lz4-v2.6.0+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_pierrre_compare",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pierrre/compare",
        sha256 = "99af9543f52487c6e7015721def85aa2d9eb7661e37b151f1db91875dcda2ee7",
        strip_prefix = "github.com/pierrre/compare@v1.0.2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/pierrre/compare/com_github_pierrre_compare-v1.0.2.zip",
        ],
    )
    go_repository(
        name = "com_github_pierrre_geohash",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pierrre/geohash",
        sha256 = "8c94a7e1f93170b53cf6e9d615967c24ff5342d5182d510f4829b3f39e249b4d",
        strip_prefix = "github.com/pierrre/geohash@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/pierrre/geohash/com_github_pierrre_geohash-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_pingcap_errors",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pingcap/errors",
        sha256 = "df62e548162429501a88d936a3e8330f2379ddfcd4d23c22b78bc1b157e05b97",
        strip_prefix = "github.com/pingcap/errors@v0.11.4",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/pingcap/errors/com_github_pingcap_errors-v0.11.4.zip",
        ],
    )
    go_repository(
        name = "com_github_pires_go_proxyproto",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pires/go-proxyproto",
        sha256 = "ad00aa7f321a1b560d2c6fb33f0bfaf2a0ce056a25c9a756cbd198b655fcbbf9",
        strip_prefix = "github.com/pires/go-proxyproto@v0.0.0-20191211124218-517ecdf5bb2b",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/pires/go-proxyproto/com_github_pires_go_proxyproto-v0.0.0-20191211124218-517ecdf5bb2b.zip",
        ],
    )
    go_repository(
        name = "com_github_pkg_browser",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pkg/browser",
        sha256 = "b845f84fbf08bba75401a4eff94c01c9e2c668fa1b43016e835bd60c6a8b4e87",
        strip_prefix = "github.com/pkg/browser@v0.0.0-20180916011732-0a3d74bf9ce4",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/pkg/browser/com_github_pkg_browser-v0.0.0-20180916011732-0a3d74bf9ce4.zip",
        ],
    )
    go_repository(
        name = "com_github_pkg_diff",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pkg/diff",
        sha256 = "f35b23fdd2b9522ddd46cc5c0161b4f0765c514475d5d4ca2a86aca31388c8bd",
        strip_prefix = "github.com/pkg/diff@v0.0.0-20210226163009-20ebb0f2a09e",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/pkg/diff/com_github_pkg_diff-v0.0.0-20210226163009-20ebb0f2a09e.zip",
        ],
    )
    go_repository(
        name = "com_github_pkg_errors",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pkg/errors",
        sha256 = "d4c36b8bcd0616290a3913215e0f53b931bd6e00670596f2960df1b44af2bd07",
        strip_prefix = "github.com/pkg/errors@v0.9.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/pkg/errors/com_github_pkg_errors-v0.9.1.zip",
        ],
    )
    go_repository(
        name = "com_github_pkg_profile",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pkg/profile",
        sha256 = "a31530cc1be940d949f8c3ae285cf877858c9e71b0a4da457787a4fee80711b9",
        strip_prefix = "github.com/pkg/profile@v1.6.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/pkg/profile/com_github_pkg_profile-v1.6.0.zip",
        ],
    )
    go_repository(
        name = "com_github_pkg_sftp",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pkg/sftp",
        sha256 = "4e30f0455865434be7b83d4010ab97667217dafd0017caa651faafa2cc6aed64",
        strip_prefix = "github.com/pkg/sftp@v1.10.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/pkg/sftp/com_github_pkg_sftp-v1.10.1.zip",
        ],
    )
    go_repository(
        name = "com_github_pkg_term",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pkg/term",
        sha256 = "165bb00eeab26fe65c64e0e13bc29abc7ea18ac28d288e2218c137cd0bd91d9b",
        strip_prefix = "github.com/pkg/term@v0.0.0-20180730021639-bffc007b7fd5",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/pkg/term/com_github_pkg_term-v0.0.0-20180730021639-bffc007b7fd5.zip",
        ],
    )
    go_repository(
        name = "com_github_pmezard_go_difflib",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pmezard/go-difflib",
        sha256 = "de04cecc1a4b8d53e4357051026794bcbc54f2e6a260cfac508ce69d5d6457a0",
        strip_prefix = "github.com/pmezard/go-difflib@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/pmezard/go-difflib/com_github_pmezard_go_difflib-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_posener_complete",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/posener/complete",
        sha256 = "88b48005b995dc6592fa6fda08130488c83f63bcaa4ccb0fb8e926fee63112ec",
        strip_prefix = "github.com/posener/complete@v1.2.3",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/posener/complete/com_github_posener_complete-v1.2.3.zip",
        ],
    )
    go_repository(
        name = "com_github_power_devops_perfstat",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/power-devops/perfstat",
        sha256 = "3ef206586f26201742728d9ae351348179ae94bb8b0c7913aa1cdf0f13e24fd8",
        strip_prefix = "github.com/power-devops/perfstat@v0.0.0-20210106213030-5aafc221ea8c",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/power-devops/perfstat/com_github_power_devops_perfstat-v0.0.0-20210106213030-5aafc221ea8c.zip",
        ],
    )
    go_repository(
        name = "com_github_pquerna_cachecontrol",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pquerna/cachecontrol",
        sha256 = "aee5feeaf00551b3448ba6ab0d56314924cbe2aff3eb56257839b528502c4b1a",
        strip_prefix = "github.com/pquerna/cachecontrol@v0.0.0-20200921180117-858c6e7e6b7e",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/pquerna/cachecontrol/com_github_pquerna_cachecontrol-v0.0.0-20200921180117-858c6e7e6b7e.zip",
        ],
    )
    go_repository(
        name = "com_github_prashantv_protectmem",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/prashantv/protectmem",
        sha256 = "53d930afbb812eb68b665dcbd96ac371ff600c8821cf5e43628ab283457881e9",
        strip_prefix = "github.com/prashantv/protectmem@v0.0.0-20171002184600-e20412882b3a",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/prashantv/protectmem/com_github_prashantv_protectmem-v0.0.0-20171002184600-e20412882b3a.zip",
        ],
    )
    go_repository(
        name = "com_github_prometheus_alertmanager",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/prometheus/alertmanager",
        sha256 = "1c51abe35f12ebc11de46e0d888c93fe8e85b146ced1c2ab2a49dd97cf2b1c6a",
        strip_prefix = "github.com/prometheus/alertmanager@v0.23.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/prometheus/alertmanager/com_github_prometheus_alertmanager-v0.23.0.zip",
        ],
    )
    go_repository(
        name = "com_github_prometheus_client_golang",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/prometheus/client_golang",
        sha256 = "9776139bdb0080e048ddad860ba49e9cca0f7b7736ab219ca0d69da4900d830d",
        strip_prefix = "github.com/prometheus/client_golang@v1.12.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/prometheus/client_golang/com_github_prometheus_client_golang-v1.12.0.zip",
        ],
    )
    go_repository(
        name = "com_github_prometheus_client_model",
        build_file_proto_mode = "package",
        importpath = "github.com/prometheus/client_model",
        patch_args = ["-p1"],
        patches = [
            "@cockroach//build/patches:com_github_prometheus_client_model.patch",
        ],
        sha256 = "44fc58fe25ed9b122b6755e8d356d5f199592f959af3b87a3b636c6eb82b43c5",
        strip_prefix = "github.com/prometheus/client_model@v0.2.1-0.20210607210712-147c58e9608a",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/prometheus/client_model/com_github_prometheus_client_model-v0.2.1-0.20210607210712-147c58e9608a.zip",
        ],
    )
    go_repository(
        name = "com_github_prometheus_common",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/prometheus/common",
        sha256 = "9630b9bbfcf1746a01c8aff0262271c60cf891fa1cd48a8ed9149de557dc98d6",
        strip_prefix = "github.com/prometheus/common@v0.32.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/prometheus/common/com_github_prometheus_common-v0.32.1.zip",
        ],
    )
    go_repository(
        name = "com_github_prometheus_common_sigv4",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/prometheus/common/sigv4",
        sha256 = "e76ec796837158dc2624343f88da4ba3c5d9d4b45e66b359358eba5db39846dd",
        strip_prefix = "github.com/prometheus/common/sigv4@v0.1.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/prometheus/common/sigv4/com_github_prometheus_common_sigv4-v0.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_prometheus_exporter_toolkit",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/prometheus/exporter-toolkit",
        sha256 = "bac6a6c26e51c687abaf14e06b4a99eaa876380d917ff6b9bce38461ee4f95aa",
        strip_prefix = "github.com/prometheus/exporter-toolkit@v0.6.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/prometheus/exporter-toolkit/com_github_prometheus_exporter_toolkit-v0.6.1.zip",
        ],
    )
    go_repository(
        name = "com_github_prometheus_procfs",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/prometheus/procfs",
        sha256 = "3e37c9f62738a493dd28283179d802749050353df0bca161df7cec4ed43e6f1a",
        strip_prefix = "github.com/prometheus/procfs@v0.7.3",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/prometheus/procfs/com_github_prometheus_procfs-v0.7.3.zip",
        ],
    )
    go_repository(
        name = "com_github_prometheus_prometheus",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/prometheus/prometheus",
        sha256 = "934ceb931a2f3065c3aae015afcb49a9ed52043dfe29ae41a3a3e6299db1448a",
        strip_prefix = "github.com/prometheus/prometheus@v1.8.2-0.20210914090109-37468d88dce8",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/prometheus/prometheus/com_github_prometheus_prometheus-v1.8.2-0.20210914090109-37468d88dce8.zip",
        ],
    )
    go_repository(
        name = "com_github_prometheus_statsd_exporter",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/prometheus/statsd_exporter",
        sha256 = "aa848ade6fb019df4f7992808a1d6aa48d6b8276017970af4aabc1bd337c2dc3",
        strip_prefix = "github.com/prometheus/statsd_exporter@v0.21.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/prometheus/statsd_exporter/com_github_prometheus_statsd_exporter-v0.21.0.zip",
        ],
    )
    go_repository(
        name = "com_github_prometheus_tsdb",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/prometheus/tsdb",
        sha256 = "360ce38297a00439c4bfbbded7b750f6d45582d581cde70572a003bcbc8b0c7b",
        strip_prefix = "github.com/prometheus/tsdb@v0.7.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/prometheus/tsdb/com_github_prometheus_tsdb-v0.7.1.zip",
        ],
    )
    go_repository(
        name = "com_github_pseudomuto_protoc_gen_doc",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pseudomuto/protoc-gen-doc",
        patch_args = ["-p1"],
        patches = [
            "@cockroach//build/patches:com_github_pseudomuto_protoc_gen_doc.patch",
        ],
        sha256 = "ecf627d6f5b4e55d4844dda45612cbd152f0bc4dbe2ba182c7bc3ad1dc63ce5f",
        strip_prefix = "github.com/pseudomuto/protoc-gen-doc@v1.3.2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/pseudomuto/protoc-gen-doc/com_github_pseudomuto_protoc_gen_doc-v1.3.2.zip",
        ],
    )
    go_repository(
        name = "com_github_pseudomuto_protokit",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/pseudomuto/protokit",
        sha256 = "16d5fe0f6ac5bebbf9f2f05fde72f28bbf05bb18baef045b9ae79c2585f4e127",
        strip_prefix = "github.com/pseudomuto/protokit@v0.2.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/pseudomuto/protokit/com_github_pseudomuto_protokit-v0.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_puerkitobio_goquery",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/PuerkitoBio/goquery",
        sha256 = "9d5bbc466dc4fac7ad872f69eeb9dcf6ddfd925821c4699226fbdeae117839a2",
        strip_prefix = "github.com/PuerkitoBio/goquery@v1.5.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/PuerkitoBio/goquery/com_github_puerkitobio_goquery-v1.5.1.zip",
        ],
    )
    go_repository(
        name = "com_github_puerkitobio_purell",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/PuerkitoBio/purell",
        sha256 = "59e636760d7f2ab41c2f80c1784b1c73d381d44888d1999228dedd634ddcf5ed",
        strip_prefix = "github.com/PuerkitoBio/purell@v1.1.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/PuerkitoBio/purell/com_github_puerkitobio_purell-v1.1.1.zip",
        ],
    )
    go_repository(
        name = "com_github_puerkitobio_urlesc",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/PuerkitoBio/urlesc",
        sha256 = "1793124273dd94e7089e95716d40529bcf70b9e87162d60218f68dde4d6aeb9d",
        strip_prefix = "github.com/PuerkitoBio/urlesc@v0.0.0-20170810143723-de5bf2ad4578",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/PuerkitoBio/urlesc/com_github_puerkitobio_urlesc-v0.0.0-20170810143723-de5bf2ad4578.zip",
        ],
    )
    go_repository(
        name = "com_github_rcrowley_go_metrics",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/rcrowley/go-metrics",
        sha256 = "e4dbd20c185cb05019fd7d4a361266bd5d182938f49fd9577df4d12c16dc81c3",
        strip_prefix = "github.com/rcrowley/go-metrics@v0.0.0-20201227073835-cf1acfcdf475",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/rcrowley/go-metrics/com_github_rcrowley_go_metrics-v0.0.0-20201227073835-cf1acfcdf475.zip",
        ],
    )
    go_repository(
        name = "com_github_remyoudompheng_bigfft",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/remyoudompheng/bigfft",
        sha256 = "04fb0a6c44cd9665764d43b3ed7c223c60cdae140e46b9fd1581887310d198a9",
        strip_prefix = "github.com/remyoudompheng/bigfft@v0.0.0-20170806203942-52369c62f446",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/remyoudompheng/bigfft/com_github_remyoudompheng_bigfft-v0.0.0-20170806203942-52369c62f446.zip",
        ],
    )
    go_repository(
        name = "com_github_retailnext_hllpp",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/retailnext/hllpp",
        sha256 = "7863938cb01dfe9d4495df3c6608bedceec2d1195da05612f3c1b0e27d37729d",
        strip_prefix = "github.com/retailnext/hllpp@v1.0.1-0.20180308014038-101a6d2f8b52",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/retailnext/hllpp/com_github_retailnext_hllpp-v1.0.1-0.20180308014038-101a6d2f8b52.zip",
        ],
    )
    go_repository(
        name = "com_github_rivo_uniseg",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/rivo/uniseg",
        sha256 = "cb701df81f36acfbb2627a78662fdcaa150ee1ac00d0796a7f3eafbdb6218128",
        strip_prefix = "github.com/rivo/uniseg@v0.1.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/rivo/uniseg/com_github_rivo_uniseg-v0.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_robertkrimen_godocdown",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/robertkrimen/godocdown",
        sha256 = "789ed4a63a797e0dbac7c358eafa8fec4c9885f67ee61da941af4bad2d8c3b55",
        strip_prefix = "github.com/robertkrimen/godocdown@v0.0.0-20130622164427-0bfa04905481",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/robertkrimen/godocdown/com_github_robertkrimen_godocdown-v0.0.0-20130622164427-0bfa04905481.zip",
        ],
    )
    go_repository(
        name = "com_github_robfig_cron_v3",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/robfig/cron/v3",
        sha256 = "ebe6454642220832a451b8cc50eae5f9150fd8d36b90b242a5de27676be86c70",
        strip_prefix = "github.com/robfig/cron/v3@v3.0.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/robfig/cron/v3/com_github_robfig_cron_v3-v3.0.1.zip",
        ],
    )
    go_repository(
        name = "com_github_rogpeppe_fastuuid",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/rogpeppe/fastuuid",
        sha256 = "f9b8293f5e20270e26fb4214ca7afec864de92c73d03ff62b5ee29d1db4e72a1",
        strip_prefix = "github.com/rogpeppe/fastuuid@v1.2.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/rogpeppe/fastuuid/com_github_rogpeppe_fastuuid-v1.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_rogpeppe_go_internal",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/rogpeppe/go-internal",
        sha256 = "d3d86854181b9fa008f294a1798d550337be44c8285dba9f2854f308d23931d2",
        strip_prefix = "github.com/rogpeppe/go-internal@v1.8.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/rogpeppe/go-internal/com_github_rogpeppe_go_internal-v1.8.1.zip",
        ],
    )
    go_repository(
        name = "com_github_rs_cors",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/rs/cors",
        sha256 = "9aeb6b48d7ba5d34187b40adaed8280f0690e6d9b4fd6132eccbd62aa2c0efd9",
        strip_prefix = "github.com/rs/cors@v1.8.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/rs/cors/com_github_rs_cors-v1.8.0.zip",
        ],
    )
    go_repository(
        name = "com_github_rs_xid",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/rs/xid",
        sha256 = "809ed1d8845fe5d73f6973e9b7a33eefd786cc97b1aebe493243e420b7c89958",
        strip_prefix = "github.com/rs/xid@v1.3.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/rs/xid/com_github_rs_xid-v1.3.0.zip",
        ],
    )
    go_repository(
        name = "com_github_rs_zerolog",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/rs/zerolog",
        sha256 = "bb7aa934fc89548dabf71c27427eab5920c9014af6cd06f53175411df09a4241",
        strip_prefix = "github.com/rs/zerolog@v1.21.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/rs/zerolog/com_github_rs_zerolog-v1.21.0.zip",
        ],
    )
    go_repository(
        name = "com_github_russross_blackfriday",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/russross/blackfriday",
        sha256 = "8dbd018a896577afef870d49adc9a7cbdcef54f6edd97dcbbe1b53e7cd6d66d4",
        strip_prefix = "github.com/russross/blackfriday@v1.6.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/russross/blackfriday/com_github_russross_blackfriday-v1.6.0.zip",
        ],
    )
    go_repository(
        name = "com_github_russross_blackfriday_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/russross/blackfriday/v2",
        sha256 = "7852750d58a053ce38b01f2c203208817564f552ebf371b2b630081d7004c6ae",
        strip_prefix = "github.com/russross/blackfriday/v2@v2.1.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/russross/blackfriday/v2/com_github_russross_blackfriday_v2-v2.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_ryanuber_columnize",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ryanuber/columnize",
        sha256 = "ff687e133db2e470640e511c90cf474154941537a94cd97bb0cf7a28a7d00dc7",
        strip_prefix = "github.com/ryanuber/columnize@v2.1.0+incompatible",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/ryanuber/columnize/com_github_ryanuber_columnize-v2.1.0+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_safchain_ethtool",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/safchain/ethtool",
        sha256 = "d334d35faf29091158a17c695830d15da359e7fb01d779fcec17fc787ef72d1e",
        strip_prefix = "github.com/safchain/ethtool@v0.0.0-20190326074333-42ed695e3de8",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/safchain/ethtool/com_github_safchain_ethtool-v0.0.0-20190326074333-42ed695e3de8.zip",
        ],
    )
    go_repository(
        name = "com_github_samuel_go_zookeeper",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/samuel/go-zookeeper",
        sha256 = "13c5ccd4ada8ba049a48ae6fdf5b4de54894e56e8f017187466a8cb95ce1faf3",
        strip_prefix = "github.com/samuel/go-zookeeper@v0.0.0-20200724154423-2164a8ac840e",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/samuel/go-zookeeper/com_github_samuel_go_zookeeper-v0.0.0-20200724154423-2164a8ac840e.zip",
        ],
    )
    go_repository(
        name = "com_github_sap_go_hdb",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/SAP/go-hdb",
        sha256 = "273de28a254c39e9f24293b864c1d664488e4a5d44d535755a5e5b68ae7eed8d",
        strip_prefix = "github.com/SAP/go-hdb@v0.14.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/SAP/go-hdb/com_github_sap_go_hdb-v0.14.1.zip",
        ],
    )
    go_repository(
        name = "com_github_sasha_s_go_deadlock",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/sasha-s/go-deadlock",
        sha256 = "82eaa020f254a21d5025b6cae9a908315ffa382f941ef228431c10177b9657d4",
        strip_prefix = "github.com/sasha-s/go-deadlock@v0.3.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/sasha-s/go-deadlock/com_github_sasha_s_go_deadlock-v0.3.1.zip",
        ],
    )
    go_repository(
        name = "com_github_satori_go_uuid",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/satori/go.uuid",
        sha256 = "31af2e17e052a9cf74182b335ecb9abadaf9235e09e5cc6a55ebd06d355f1dd2",
        strip_prefix = "github.com/satori/go.uuid@v1.2.1-0.20181028125025-b2ce2384e17b",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/satori/go.uuid/com_github_satori_go_uuid-v1.2.1-0.20181028125025-b2ce2384e17b.zip",
        ],
    )
    go_repository(
        name = "com_github_savsgio_gotils",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/savsgio/gotils",
        sha256 = "2d92113b0b2a2c22947d56f51129cf58013e4aaecd2cadfd84d7693c4186ac58",
        strip_prefix = "github.com/savsgio/gotils@v0.0.0-20210921075833-21a6215cb0e4",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/savsgio/gotils/com_github_savsgio_gotils-v0.0.0-20210921075833-21a6215cb0e4.zip",
        ],
    )
    go_repository(
        name = "com_github_scaleway_scaleway_sdk_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/scaleway/scaleway-sdk-go",
        sha256 = "d3085a949bf12cb3afe81309662d3657b600b2f2178c9f05fbc9cd5a6a3f4c3d",
        strip_prefix = "github.com/scaleway/scaleway-sdk-go@v1.0.0-beta.7.0.20210223165440-c65ae3540d44",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/scaleway/scaleway-sdk-go/com_github_scaleway_scaleway_sdk_go-v1.0.0-beta.7.0.20210223165440-c65ae3540d44.zip",
        ],
    )
    go_repository(
        name = "com_github_schollz_closestmatch",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/schollz/closestmatch",
        sha256 = "f267729efc7a639bb816e2586a17237a8c8e7ff327c0c3dd58766d1433ad2d3a",
        strip_prefix = "github.com/schollz/closestmatch@v2.1.0+incompatible",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/schollz/closestmatch/com_github_schollz_closestmatch-v2.1.0+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_sclevine_agouti",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/sclevine/agouti",
        sha256 = "b20c8a6a2c1fda0ae6a9cd6d319e78a7a5afea4bc90810cd46b99246d8219d23",
        strip_prefix = "github.com/sclevine/agouti@v3.0.0+incompatible",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/sclevine/agouti/com_github_sclevine_agouti-v3.0.0+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_scylladb_termtables",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/scylladb/termtables",
        sha256 = "0afd3a75417e6b708c62329cea7f2822162c8bf6ada0de094cdc032461b8f196",
        strip_prefix = "github.com/scylladb/termtables@v0.0.0-20191203121021-c4c0b6d42ff4",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/scylladb/termtables/com_github_scylladb_termtables-v0.0.0-20191203121021-c4c0b6d42ff4.zip",
        ],
    )
    go_repository(
        name = "com_github_sean__seed",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/sean-/seed",
        sha256 = "0bc8e6e0a07e554674b0bb92ef4eb7de1650056b50878eed8d5d631aec9b6362",
        strip_prefix = "github.com/sean-/seed@v0.0.0-20170313163322-e2103e2c3529",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/sean-/seed/com_github_sean__seed-v0.0.0-20170313163322-e2103e2c3529.zip",
        ],
    )
    go_repository(
        name = "com_github_seccomp_libseccomp_golang",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/seccomp/libseccomp-golang",
        sha256 = "5989692d87ef4c377fbc60d441795a90d9453b9e357d019e44d9033ab39ca888",
        strip_prefix = "github.com/seccomp/libseccomp-golang@v0.9.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/seccomp/libseccomp-golang/com_github_seccomp_libseccomp_golang-v0.9.1.zip",
        ],
    )
    go_repository(
        name = "com_github_sectioneight_md_to_godoc",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/sectioneight/md-to-godoc",
        sha256 = "8b605818df307b414d0a680f147f0baeb37c9166df9e111ede5531cf50124203",
        strip_prefix = "github.com/sectioneight/md-to-godoc@v0.0.0-20161108233149-55e43be6c335",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/sectioneight/md-to-godoc/com_github_sectioneight_md_to_godoc-v0.0.0-20161108233149-55e43be6c335.zip",
        ],
    )
    go_repository(
        name = "com_github_securego_gosec",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/securego/gosec",
        sha256 = "e0adea3cd40ba9d690b8054ff1341cf7d035084f50273a4f7bbac803fec3453a",
        strip_prefix = "github.com/securego/gosec@v0.0.0-20200203094520-d13bb6d2420c",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/securego/gosec/com_github_securego_gosec-v0.0.0-20200203094520-d13bb6d2420c.zip",
        ],
    )
    go_repository(
        name = "com_github_segmentio_kafka_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/segmentio/kafka-go",
        sha256 = "b2a88eb5b65fbb75dac0ba5e721cd2cb8e39275d1702a0f97e3c4807d78e8b48",
        strip_prefix = "github.com/segmentio/kafka-go@v0.2.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/segmentio/kafka-go/com_github_segmentio_kafka_go-v0.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_sergi_go_diff",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/sergi/go-diff",
        sha256 = "a9c0203d5188745f59c29e170e8b1a7e7c8bd007634bce75932ffac042e43eac",
        strip_prefix = "github.com/sergi/go-diff@v1.1.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/sergi/go-diff/com_github_sergi_go_diff-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_shirou_gopsutil_v3",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/shirou/gopsutil/v3",
        sha256 = "ea6f8b430cee40870d8d454aaa5d4c22e84d217a2548a3f755b91a96b1c67a88",
        strip_prefix = "github.com/shirou/gopsutil/v3@v3.21.12",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/shirou/gopsutil/v3/com_github_shirou_gopsutil_v3-v3.21.12.zip",
        ],
    )
    go_repository(
        name = "com_github_shopify_goreferrer",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Shopify/goreferrer",
        sha256 = "e47cdf750e6aa39707b90e62f4f87e97abb8d64b2525a16c021c82efb24f9969",
        strip_prefix = "github.com/Shopify/goreferrer@v0.0.0-20181106222321-ec9c9a553398",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/Shopify/goreferrer/com_github_shopify_goreferrer-v0.0.0-20181106222321-ec9c9a553398.zip",
        ],
    )
    go_repository(
        name = "com_github_shopify_logrus_bugsnag",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Shopify/logrus-bugsnag",
        sha256 = "a4cc3fa4b7b493b36b96ea035caa7afcf7307b0c4efc5e523a46597e171b95ce",
        strip_prefix = "github.com/Shopify/logrus-bugsnag@v0.0.0-20171204204709-577dee27f20d",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/Shopify/logrus-bugsnag/com_github_shopify_logrus_bugsnag-v0.0.0-20171204204709-577dee27f20d.zip",
        ],
    )
    go_repository(
        name = "com_github_shopify_sarama",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Shopify/sarama",
        sha256 = "e40e234e595aee956281eb80bcdda92342f2686180313cfcb379e7bcd1d49b58",
        strip_prefix = "github.com/Shopify/sarama@v1.29.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/Shopify/sarama/com_github_shopify_sarama-v1.29.0.zip",
        ],
    )
    go_repository(
        name = "com_github_shopify_toxiproxy",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/Shopify/toxiproxy",
        sha256 = "9427e70698ee6a906904dfa0652624f640619acef40652a1e5490e13b31e7f61",
        strip_prefix = "github.com/Shopify/toxiproxy@v2.1.4+incompatible",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/Shopify/toxiproxy/com_github_shopify_toxiproxy-v2.1.4+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_shopspring_decimal",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/shopspring/decimal",
        sha256 = "65c34c248e7f736cadf03a7caa0c0870d15499eb593f933fe106c96c2b7699a7",
        strip_prefix = "github.com/shopspring/decimal@v1.2.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/shopspring/decimal/com_github_shopspring_decimal-v1.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_shurcool_httpfs",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/shurcooL/httpfs",
        sha256 = "a2079dbd8c236262ecbb22312467265fbbddd9b5ee789531c5f7f24fbdda174b",
        strip_prefix = "github.com/shurcooL/httpfs@v0.0.0-20190707220628-8d4bc4ba7749",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/shurcooL/httpfs/com_github_shurcool_httpfs-v0.0.0-20190707220628-8d4bc4ba7749.zip",
        ],
    )
    go_repository(
        name = "com_github_shurcool_sanitized_anchor_name",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/shurcooL/sanitized_anchor_name",
        sha256 = "0af034323e0627a9e94367f87aa50ce29e5b165d54c8da2926cbaffd5834f757",
        strip_prefix = "github.com/shurcooL/sanitized_anchor_name@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/shurcooL/sanitized_anchor_name/com_github_shurcool_sanitized_anchor_name-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_shurcool_vfsgen",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/shurcooL/vfsgen",
        sha256 = "98198ecd8f122d1266ff2db193f1aae8a88f2f299bfc34b06ef356694cca537d",
        strip_prefix = "github.com/shurcooL/vfsgen@v0.0.0-20200824052919-0d455de96546",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/shurcooL/vfsgen/com_github_shurcool_vfsgen-v0.0.0-20200824052919-0d455de96546.zip",
        ],
    )
    go_repository(
        name = "com_github_sirupsen_logrus",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/sirupsen/logrus",
        sha256 = "8caec32e0aba1b95effc69d645b50f937137f78f0d0534b149f017dc0164a354",
        strip_prefix = "github.com/sirupsen/logrus@v1.8.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/sirupsen/logrus/com_github_sirupsen_logrus-v1.8.1.zip",
        ],
    )
    go_repository(
        name = "com_github_sjmudd_stopwatch",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/sjmudd/stopwatch",
        sha256 = "69e0ed207172b04161ccb26977f4c657fbee77296eca0b7ff84f8b1f2c2a6847",
        strip_prefix = "github.com/sjmudd/stopwatch@v0.0.0-20170613150411-f380bf8a9be1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/sjmudd/stopwatch/com_github_sjmudd_stopwatch-v0.0.0-20170613150411-f380bf8a9be1.zip",
        ],
    )
    go_repository(
        name = "com_github_slack_go_slack",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/slack-go/slack",
        sha256 = "2b1cc2d4107c7017f1348beefeb23db1cdbecf94b32590da364daed420371cde",
        strip_prefix = "github.com/slack-go/slack@v0.9.5",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/slack-go/slack/com_github_slack_go_slack-v0.9.5.zip",
        ],
    )
    go_repository(
        name = "com_github_slok_go_http_metrics",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/slok/go-http-metrics",
        sha256 = "bf2e2b626e4fbd9735165494c574f2474f400786d8bd96b6b4648eba352c817b",
        strip_prefix = "github.com/slok/go-http-metrics@v0.10.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/slok/go-http-metrics/com_github_slok_go_http_metrics-v0.10.0.zip",
        ],
    )
    go_repository(
        name = "com_github_smartystreets_assertions",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/smartystreets/assertions",
        sha256 = "bf12bc33290d3e1e6f4cfe89aad0ad40c0acbfb378ce11e8157569aaf1526c04",
        strip_prefix = "github.com/smartystreets/assertions@v0.0.0-20190116191733-b6c0e53d7304",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/smartystreets/assertions/com_github_smartystreets_assertions-v0.0.0-20190116191733-b6c0e53d7304.zip",
        ],
    )
    go_repository(
        name = "com_github_smartystreets_goconvey",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/smartystreets/goconvey",
        sha256 = "a931413713a303a958a9c3ac31305498905fb91465e725552472462130396dda",
        strip_prefix = "github.com/smartystreets/goconvey@v1.6.4",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/smartystreets/goconvey/com_github_smartystreets_goconvey-v1.6.4.zip",
        ],
    )
    go_repository(
        name = "com_github_snowflakedb_gosnowflake",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/snowflakedb/gosnowflake",
        sha256 = "a39ab3850d25f162e2ed4bf920c0fba1559e1c5ec41e1ca35f44600a2e9a971d",
        strip_prefix = "github.com/snowflakedb/gosnowflake@v1.3.4",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/snowflakedb/gosnowflake/com_github_snowflakedb_gosnowflake-v1.3.4.zip",
        ],
    )
    go_repository(
        name = "com_github_soheilhy_cmux",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/soheilhy/cmux",
        sha256 = "6d6cadade0e186f84b5f8e7ddf8f4256601b21e49b0ca49fd003a7e570ae1885",
        strip_prefix = "github.com/soheilhy/cmux@v0.1.4",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/soheilhy/cmux/com_github_soheilhy_cmux-v0.1.4.zip",
        ],
    )
    go_repository(
        name = "com_github_sony_gobreaker",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/sony/gobreaker",
        sha256 = "eab9bf8f98b16b051d7d13c4f5c70d6d1039347e380e0a12cb9ff6e33200d784",
        strip_prefix = "github.com/sony/gobreaker@v0.4.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/sony/gobreaker/com_github_sony_gobreaker-v0.4.1.zip",
        ],
    )
    go_repository(
        name = "com_github_spaolacci_murmur3",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/spaolacci/murmur3",
        sha256 = "7fca345fffeb6d4ef1ffb8a9130a715f6b6e71c0e0dcc5eacf73995601b69766",
        strip_prefix = "github.com/spaolacci/murmur3@v0.0.0-20180118202830-f09979ecbc72",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/spaolacci/murmur3/com_github_spaolacci_murmur3-v0.0.0-20180118202830-f09979ecbc72.zip",
        ],
    )
    go_repository(
        name = "com_github_spf13_afero",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/spf13/afero",
        sha256 = "234791203d4bb7405bf5ef7f5cecc6e64fdfa9fc5a12d85b6ec7ecdb86cc0834",
        strip_prefix = "github.com/spf13/afero@v1.6.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/spf13/afero/com_github_spf13_afero-v1.6.0.zip",
        ],
    )
    go_repository(
        name = "com_github_spf13_cast",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/spf13/cast",
        sha256 = "9431fba3679d68cb98976c0f87e20520555835ecf772182991a37831426f219e",
        strip_prefix = "github.com/spf13/cast@v1.3.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/spf13/cast/com_github_spf13_cast-v1.3.1.zip",
        ],
    )
    go_repository(
        name = "com_github_spf13_cobra",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/spf13/cobra",
        sha256 = "9dad7255db19d7e264232543b42a0f5f04c4561df708702f1d895ff7c66818e5",
        strip_prefix = "github.com/spf13/cobra@v1.2.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/spf13/cobra/com_github_spf13_cobra-v1.2.1.zip",
        ],
    )
    go_repository(
        name = "com_github_spf13_jwalterweatherman",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/spf13/jwalterweatherman",
        sha256 = "43cc5f056caf66dc8225dca36637bfc18509521b103a69ca76fbc2b6519194a3",
        strip_prefix = "github.com/spf13/jwalterweatherman@v1.1.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/spf13/jwalterweatherman/com_github_spf13_jwalterweatherman-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_spf13_pflag",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/spf13/pflag",
        sha256 = "fc6e704f2f6a84ddcdce6de0404e5340fa20c8676181bf5d381b17888107ba84",
        strip_prefix = "github.com/spf13/pflag@v1.0.5",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/spf13/pflag/com_github_spf13_pflag-v1.0.5.zip",
        ],
    )
    go_repository(
        name = "com_github_spf13_viper",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/spf13/viper",
        sha256 = "3223fb4fb8308f36d504b12653027403fa277cc03f49141e6539edb6991f7fef",
        strip_prefix = "github.com/spf13/viper@v1.8.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/spf13/viper/com_github_spf13_viper-v1.8.1.zip",
        ],
    )
    go_repository(
        name = "com_github_stefanberger_go_pkcs11uri",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/stefanberger/go-pkcs11uri",
        sha256 = "a9e09db495594e9f0e6b4c625ce12b026a14fa54a6478de762904594f545cb1a",
        strip_prefix = "github.com/stefanberger/go-pkcs11uri@v0.0.0-20201008174630-78d3cae3a980",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/stefanberger/go-pkcs11uri/com_github_stefanberger_go_pkcs11uri-v0.0.0-20201008174630-78d3cae3a980.zip",
        ],
    )
    go_repository(
        name = "com_github_stephens2424_writerset",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/stephens2424/writerset",
        sha256 = "a5444ddf04cda5666c4511e5ca793a80372d560376c4193a1fa2e2294d0760dc",
        strip_prefix = "github.com/stephens2424/writerset@v1.0.2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/stephens2424/writerset/com_github_stephens2424_writerset-v1.0.2.zip",
        ],
    )
    go_repository(
        name = "com_github_stoewer_go_strcase",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/stoewer/go-strcase",
        sha256 = "8f24d2c36a4bc9d78cb8aa046183c098e8acad1d812adb7dafc9c29f4e2affd0",
        strip_prefix = "github.com/stoewer/go-strcase@v1.2.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/stoewer/go-strcase/com_github_stoewer_go_strcase-v1.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_streadway_amqp",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/streadway/amqp",
        sha256 = "66bd109504bf565a4a777c20a8cf6a1c5d05cd87b59baa50da8b6f2b0da4c494",
        strip_prefix = "github.com/streadway/amqp@v0.0.0-20190827072141-edfb9018d271",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/streadway/amqp/com_github_streadway_amqp-v0.0.0-20190827072141-edfb9018d271.zip",
        ],
    )
    go_repository(
        name = "com_github_streadway_handy",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/streadway/handy",
        sha256 = "f770ed96081220a9cbc5e975a06c2858b4f3d02820cb9902982116af491b171f",
        strip_prefix = "github.com/streadway/handy@v0.0.0-20190108123426-d5acb3125c2a",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/streadway/handy/com_github_streadway_handy-v0.0.0-20190108123426-d5acb3125c2a.zip",
        ],
    )
    go_repository(
        name = "com_github_streadway_quantile",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/streadway/quantile",
        sha256 = "45156bab62475784e2eacb349570c86bcf245a84d97825ce9ee2bf604a4438d5",
        strip_prefix = "github.com/streadway/quantile@v0.0.0-20150917103942-b0c588724d25",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/streadway/quantile/com_github_streadway_quantile-v0.0.0-20150917103942-b0c588724d25.zip",
        ],
    )
    go_repository(
        name = "com_github_stretchr_objx",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/stretchr/objx",
        sha256 = "5517d43cfb7e628b9c2c64010b934e346cd24726e3d6eaf02b7f86e10752e968",
        strip_prefix = "github.com/stretchr/objx@v0.2.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/stretchr/objx/com_github_stretchr_objx-v0.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_stretchr_testify",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/stretchr/testify",
        sha256 = "5a46ccebeff510df3e2f6d3842ee79d3f68d0e7b1554cd6ee93390d68b6c6b34",
        strip_prefix = "github.com/stretchr/testify@v1.7.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/stretchr/testify/com_github_stretchr_testify-v1.7.0.zip",
        ],
    )
    go_repository(
        name = "com_github_subosito_gotenv",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/subosito/gotenv",
        sha256 = "21474df92536f36de6f91dfbf466995289445cc4e5a5900d9c40ae8776b8b0cf",
        strip_prefix = "github.com/subosito/gotenv@v1.2.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/subosito/gotenv/com_github_subosito_gotenv-v1.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_syndtr_gocapability",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/syndtr/gocapability",
        sha256 = "91ff91da1936e17aa68fc13756e40ba4db1d7c9375a4ef0969fe19c9aa281195",
        strip_prefix = "github.com/syndtr/gocapability@v0.0.0-20200815063812-42c35b437635",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/syndtr/gocapability/com_github_syndtr_gocapability-v0.0.0-20200815063812-42c35b437635.zip",
        ],
    )
    go_repository(
        name = "com_github_tchap_go_patricia",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/tchap/go-patricia",
        sha256 = "948494017eae153a8c2d4ae9b450fd42abcb2578211f1c28e69ab71a2f27814d",
        strip_prefix = "github.com/tchap/go-patricia@v2.2.6+incompatible",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/tchap/go-patricia/com_github_tchap_go_patricia-v2.2.6+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_tebeka_selenium",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/tebeka/selenium",
        sha256 = "1bcf27d3675f057bf2af7d73db1d06b932537ba46fa5bd4be6855105c31106d7",
        strip_prefix = "github.com/tebeka/selenium@v0.9.9",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/tebeka/selenium/com_github_tebeka_selenium-v0.9.9.zip",
        ],
    )
    go_repository(
        name = "com_github_the42_cartconvert",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/the42/cartconvert",
        sha256 = "a254c587b6ad690e45269f161fa52e26406bafc14f94442684063df8c953cbf3",
        strip_prefix = "github.com/the42/cartconvert@v0.0.0-20131203171324-aae784c392b8",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/the42/cartconvert/com_github_the42_cartconvert-v0.0.0-20131203171324-aae784c392b8.zip",
        ],
    )
    go_repository(
        name = "com_github_tidwall_pretty",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/tidwall/pretty",
        sha256 = "3b25a1a0fe7688989326aaa1ca1c74c972b30152ef2a756fbf2d217a827fc07d",
        strip_prefix = "github.com/tidwall/pretty@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/tidwall/pretty/com_github_tidwall_pretty-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_tinylib_msgp",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/tinylib/msgp",
        sha256 = "5f95bcd71857878008dd8f1aca59e672f9e07122ff9689bcf3bc9b8b859ba4e2",
        strip_prefix = "github.com/tinylib/msgp@v1.1.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/tinylib/msgp/com_github_tinylib_msgp-v1.1.1.zip",
        ],
    )
    go_repository(
        name = "com_github_tklauser_go_sysconf",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/tklauser/go-sysconf",
        sha256 = "2233c2cf18d4d37354eb333ec8a2dca63c24a09cfbc3b90cd06a8b3afe70893f",
        strip_prefix = "github.com/tklauser/go-sysconf@v0.3.9",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/tklauser/go-sysconf/com_github_tklauser_go_sysconf-v0.3.9.zip",
        ],
    )
    go_repository(
        name = "com_github_tklauser_numcpus",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/tklauser/numcpus",
        sha256 = "a5569abed62bb2d5f5f322f23fe8cae888fe98704442d59ed9e7aabfed423899",
        strip_prefix = "github.com/tklauser/numcpus@v0.3.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/tklauser/numcpus/com_github_tklauser_numcpus-v0.3.0.zip",
        ],
    )
    go_repository(
        name = "com_github_tmc_grpc_websocket_proxy",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/tmc/grpc-websocket-proxy",
        sha256 = "dadf62266d259ffb6aa1d707892b97fa36c3f39df5cae99f54d3ef7682995376",
        strip_prefix = "github.com/tmc/grpc-websocket-proxy@v0.0.0-20190109142713-0ad062ec5ee5",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/tmc/grpc-websocket-proxy/com_github_tmc_grpc_websocket_proxy-v0.0.0-20190109142713-0ad062ec5ee5.zip",
        ],
    )
    go_repository(
        name = "com_github_tomihiltunen_geohash_golang",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/TomiHiltunen/geohash-golang",
        sha256 = "508c58cbcd4d2cec576cfd3450077413ba31cac31f825d8660d8d11c783501a0",
        strip_prefix = "github.com/TomiHiltunen/geohash-golang@v0.0.0-20150112065804-b3e4e625abfb",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/TomiHiltunen/geohash-golang/com_github_tomihiltunen_geohash_golang-v0.0.0-20150112065804-b3e4e625abfb.zip",
        ],
    )
    go_repository(
        name = "com_github_toqueteos_webbrowser",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/toqueteos/webbrowser",
        sha256 = "1227d3ebeab16d8232a304a10b087984a96ad30f7439b6687bab2f5747d308cf",
        strip_prefix = "github.com/toqueteos/webbrowser@v1.2.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/toqueteos/webbrowser/com_github_toqueteos_webbrowser-v1.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_trivago_tgo",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/trivago/tgo",
        sha256 = "795b3a41901f6b694195d6be9c6e7730a971fbc0ec4cd236e73cc845aca6cb7e",
        strip_prefix = "github.com/trivago/tgo@v1.0.7",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/trivago/tgo/com_github_trivago_tgo-v1.0.7.zip",
        ],
    )
    go_repository(
        name = "com_github_tv42_httpunix",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/tv42/httpunix",
        sha256 = "8246ebc82e0d9d3142f5aeb50d4fcd67f3f435fb5464120c356a4e5d57ef4aa0",
        strip_prefix = "github.com/tv42/httpunix@v0.0.0-20150427012821-b75d8614f926",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/tv42/httpunix/com_github_tv42_httpunix-v0.0.0-20150427012821-b75d8614f926.zip",
        ],
    )
    go_repository(
        name = "com_github_twitchtv_twirp",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/twitchtv/twirp",
        sha256 = "6a5499c6572cf367ac9c2bd7913abef5bc8ef9de5e7194d12452863ddcec6104",
        strip_prefix = "github.com/twitchtv/twirp@v8.1.0+incompatible",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/twitchtv/twirp/com_github_twitchtv_twirp-v8.1.0+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_twpayne_go_geom",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/twpayne/go-geom",
        sha256 = "31c2358ae2f1c10a406c529737c0928971fa3e17f4f807a45b02cf3eebacd03a",
        strip_prefix = "github.com/twpayne/go-geom@v1.4.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/twpayne/go-geom/com_github_twpayne_go_geom-v1.4.1.zip",
        ],
    )
    go_repository(
        name = "com_github_twpayne_go_kml",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/twpayne/go-kml",
        sha256 = "f67a698f9a02c889a1f6ff4e0a0625ec2359057674c0f25cf8c862ae519e382e",
        strip_prefix = "github.com/twpayne/go-kml@v1.5.2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/twpayne/go-kml/com_github_twpayne_go_kml-v1.5.2.zip",
        ],
    )
    go_repository(
        name = "com_github_twpayne_go_polyline",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/twpayne/go-polyline",
        sha256 = "1794c8b7368bd16dec9cdb7b9be394c2030a1cc706a51edc318490667d9a5a97",
        strip_prefix = "github.com/twpayne/go-polyline@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/twpayne/go-polyline/com_github_twpayne_go_polyline-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_twpayne_go_waypoint",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/twpayne/go-waypoint",
        sha256 = "8163a963b71e0723d694f87eeb15e3a7f9a32ad5fd189a1b78cf0aa293d300c8",
        strip_prefix = "github.com/twpayne/go-waypoint@v0.0.0-20200706203930-b263a7f6e4e8",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/twpayne/go-waypoint/com_github_twpayne_go_waypoint-v0.0.0-20200706203930-b263a7f6e4e8.zip",
        ],
    )
    go_repository(
        name = "com_github_uber_athenadriver",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/uber/athenadriver",
        sha256 = "6ac94915e7d83bae55c968c4b750a4dee2ca6a57a5ed2bb8f9203735e452080d",
        strip_prefix = "github.com/uber/athenadriver@v1.1.4",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/uber/athenadriver/com_github_uber_athenadriver-v1.1.4.zip",
        ],
    )
    go_repository(
        name = "com_github_uber_go_atomic",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/uber-go/atomic",
        sha256 = "f380292d46ebec89bf53939e4d7d19d617327cbcdf2978e30e6c39bc77df5e73",
        strip_prefix = "github.com/uber-go/atomic@v1.4.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/uber-go/atomic/com_github_uber_go_atomic-v1.4.0.zip",
        ],
    )
    go_repository(
        name = "com_github_uber_go_tally",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/uber-go/tally",
        sha256 = "f1d6e97da887bf4a704dace304fdb46cc69a03969c779638f6f805ecfa7aa27c",
        strip_prefix = "github.com/uber-go/tally@v3.3.15+incompatible",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/uber-go/tally/com_github_uber_go_tally-v3.3.15+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_uber_jaeger_client_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/uber/jaeger-client-go",
        sha256 = "a6933446c0bdeccfdb60361df9945138b1821dee1dfd5ec27f4fd832550e80fb",
        strip_prefix = "github.com/uber/jaeger-client-go@v2.29.1+incompatible",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/uber/jaeger-client-go/com_github_uber_jaeger_client_go-v2.29.1+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_uber_jaeger_lib",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/uber/jaeger-lib",
        sha256 = "b43fc0c89c3c54498ae6108453ca2af987e074680742dd79bdceda94685a7efb",
        strip_prefix = "github.com/uber/jaeger-lib@v2.4.1+incompatible",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/uber/jaeger-lib/com_github_uber_jaeger_lib-v2.4.1+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_uber_tchannel_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/uber/tchannel-go",
        sha256 = "64a37a5e89dd111ab943d94a1670f9addc0d2d41d34d630c95b0a756df916e01",
        strip_prefix = "github.com/uber/tchannel-go@v1.16.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/uber/tchannel-go/com_github_uber_tchannel_go-v1.16.0.zip",
        ],
    )
    go_repository(
        name = "com_github_ugorji_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ugorji/go",
        sha256 = "d02959e71c59b273d5b099697c058426941a862feef66c191c63e2934db7a2ff",
        strip_prefix = "github.com/ugorji/go@v1.1.7",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/ugorji/go/com_github_ugorji_go-v1.1.7.zip",
        ],
    )
    go_repository(
        name = "com_github_ugorji_go_codec",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/ugorji/go/codec",
        sha256 = "8d482061c55b4c4fbf78de9fbf98a8d1b295f5904769679c73a2dc0b06a1a102",
        strip_prefix = "github.com/ugorji/go/codec@v1.1.7",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/ugorji/go/codec/com_github_ugorji_go_codec-v1.1.7.zip",
        ],
    )
    go_repository(
        name = "com_github_urfave_cli",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/urfave/cli",
        sha256 = "c974a314e3abfdd6340f4e0c423969238544cf6513ef41385f834cbe122a57e5",
        strip_prefix = "github.com/urfave/cli@v1.22.2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/urfave/cli/com_github_urfave_cli-v1.22.2.zip",
        ],
    )
    go_repository(
        name = "com_github_urfave_cli_v2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/urfave/cli/v2",
        sha256 = "bef25aedf2f3ac498094ec9cd216bca61ddf5f2eb7b1ecd850bbfb6053fe4103",
        strip_prefix = "github.com/urfave/cli/v2@v2.3.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/urfave/cli/v2/com_github_urfave_cli_v2-v2.3.0.zip",
        ],
    )
    go_repository(
        name = "com_github_urfave_negroni",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/urfave/negroni",
        sha256 = "7b50615961d34d748866565b8885edd7013e33812acdbaed47502d7cc73a4bbd",
        strip_prefix = "github.com/urfave/negroni@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/urfave/negroni/com_github_urfave_negroni-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_valyala_bytebufferpool",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/valyala/bytebufferpool",
        sha256 = "7f59f32c568539afee9a21a665a4156962b019beaac8404e26ba37af056b4f1e",
        strip_prefix = "github.com/valyala/bytebufferpool@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/valyala/bytebufferpool/com_github_valyala_bytebufferpool-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_valyala_fasthttp",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/valyala/fasthttp",
        sha256 = "b6270b17ff9ef1d1c58d86099d64d7aef0b78c7e7cf48ae4ddf29c8dca933a92",
        strip_prefix = "github.com/valyala/fasthttp@v1.31.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/valyala/fasthttp/com_github_valyala_fasthttp-v1.31.0.zip",
        ],
    )
    go_repository(
        name = "com_github_valyala_fasttemplate",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/valyala/fasttemplate",
        sha256 = "d8186bd1cfea049449decf7ce2568f33f0d2b5350fe8cf62956fa25762eaba5c",
        strip_prefix = "github.com/valyala/fasttemplate@v1.2.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/valyala/fasttemplate/com_github_valyala_fasttemplate-v1.2.1.zip",
        ],
    )
    go_repository(
        name = "com_github_valyala_tcplisten",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/valyala/tcplisten",
        sha256 = "07066d5b879a94d6bc1feed20ad4003c62865975dd1f4c062673178be406206a",
        strip_prefix = "github.com/valyala/tcplisten@v0.0.0-20161114210144-ceec8f93295a",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/valyala/tcplisten/com_github_valyala_tcplisten-v0.0.0-20161114210144-ceec8f93295a.zip",
        ],
    )
    go_repository(
        name = "com_github_vektah_gqlparser",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/vektah/gqlparser",
        sha256 = "cdd0119855b98641e7af60dce5b2848b31f8ef03dfcf097c06912309b86fc97c",
        strip_prefix = "github.com/vektah/gqlparser@v1.1.2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/vektah/gqlparser/com_github_vektah_gqlparser-v1.1.2.zip",
        ],
    )
    go_repository(
        name = "com_github_vektra_mockery",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/vektra/mockery",
        sha256 = "b1268e9da9a6c808d28a76f725df57a44f2c209a6224491239f843e04d5a4558",
        strip_prefix = "github.com/vektra/mockery@v0.0.0-20181123154057-e78b021dcbb5",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/vektra/mockery/com_github_vektra_mockery-v0.0.0-20181123154057-e78b021dcbb5.zip",
        ],
    )
    go_repository(
        name = "com_github_vishvananda_netlink",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/vishvananda/netlink",
        sha256 = "7d4eb2060dac62357d038661e285a4eabec668e21c5a568c229cdc97ef54cbb0",
        strip_prefix = "github.com/vishvananda/netlink@v1.1.1-0.20201029203352-d40f9887b852",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/vishvananda/netlink/com_github_vishvananda_netlink-v1.1.1-0.20201029203352-d40f9887b852.zip",
        ],
    )
    go_repository(
        name = "com_github_vishvananda_netns",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/vishvananda/netns",
        sha256 = "4e3dd4f80e263e250c91b38a424381c93523fe494f82a00d4ae421afd45dbef1",
        strip_prefix = "github.com/vishvananda/netns@v0.0.0-20200728191858-db3c7e526aae",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/vishvananda/netns/com_github_vishvananda_netns-v0.0.0-20200728191858-db3c7e526aae.zip",
        ],
    )
    go_repository(
        name = "com_github_vividcortex_ewma",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/VividCortex/ewma",
        sha256 = "eebee7c0f20e96abbda1611ed2a3d26b4c2c10393caa6a2dfd1605763a5c1a12",
        strip_prefix = "github.com/VividCortex/ewma@v1.1.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/VividCortex/ewma/com_github_vividcortex_ewma-v1.1.1.zip",
        ],
    )
    go_repository(
        name = "com_github_vividcortex_gohistogram",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/VividCortex/gohistogram",
        sha256 = "16ebeceeb7e4066f90edbfb90282cd90d4dad0f71339199551de3fbdc7e8c545",
        strip_prefix = "github.com/VividCortex/gohistogram@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/VividCortex/gohistogram/com_github_vividcortex_gohistogram-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_wadey_gocovmerge",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/wadey/gocovmerge",
        sha256 = "9f5952330bf701f65988725ec1f3a34ebf3c79c1db5a70e48b48e0797f470c28",
        strip_prefix = "github.com/wadey/gocovmerge@v0.0.0-20160331181800-b5bfa59ec0ad",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/wadey/gocovmerge/com_github_wadey_gocovmerge-v0.0.0-20160331181800-b5bfa59ec0ad.zip",
        ],
    )
    go_repository(
        name = "com_github_willf_bitset",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/willf/bitset",
        sha256 = "9fd0ee4e781c0cfe6df5db67dbbcda1c7adb7cea73c0afc068aa495c7e8027f6",
        strip_prefix = "github.com/willf/bitset@v1.1.11",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/willf/bitset/com_github_willf_bitset-v1.1.11.zip",
        ],
    )
    go_repository(
        name = "com_github_xdg_go_pbkdf2",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/xdg-go/pbkdf2",
        sha256 = "c22c803b9e69744dc4e33c5607652b87d61ee6926c54a67c6260b827e2040fec",
        strip_prefix = "github.com/xdg-go/pbkdf2@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/xdg-go/pbkdf2/com_github_xdg_go_pbkdf2-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_xdg_go_scram",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/xdg-go/scram",
        sha256 = "c7846f2609f2efc1d248301c2f261ff6cb1be971fa77de5790bda1fcd14fcceb",
        strip_prefix = "github.com/xdg-go/scram@v1.0.2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/xdg-go/scram/com_github_xdg_go_scram-v1.0.2.zip",
        ],
    )
    go_repository(
        name = "com_github_xdg_go_stringprep",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/xdg-go/stringprep",
        sha256 = "4f4d7c8ba09ab5fa288ff86902c2f4000f3a85cd55d9ff75a04ade18c8e33ad9",
        strip_prefix = "github.com/xdg-go/stringprep@v1.0.2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/xdg-go/stringprep/com_github_xdg_go_stringprep-v1.0.2.zip",
        ],
    )
    go_repository(
        name = "com_github_xdg_scram",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/xdg/scram",
        sha256 = "56875f465f0ed3170846db4d300328b9c769c35a3c59a479c8b9ac659765e48c",
        strip_prefix = "github.com/xdg/scram@v1.0.3",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/xdg/scram/com_github_xdg_scram-v1.0.3.zip",
        ],
    )
    go_repository(
        name = "com_github_xdg_stringprep",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/xdg/stringprep",
        sha256 = "7cb9711fd7b3c1518e1fbd4e39be11737d7006a5e4a59f1ceb4ba9c205eb90fa",
        strip_prefix = "github.com/xdg/stringprep@v1.0.3",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/xdg/stringprep/com_github_xdg_stringprep-v1.0.3.zip",
        ],
    )
    go_repository(
        name = "com_github_xeipuuv_gojsonpointer",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/xeipuuv/gojsonpointer",
        sha256 = "5b1a4bcc8e003f214c92b3fa52959d9eb0e3af1c0c529efa55815db951146e48",
        strip_prefix = "github.com/xeipuuv/gojsonpointer@v0.0.0-20180127040702-4e3ac2762d5f",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/xeipuuv/gojsonpointer/com_github_xeipuuv_gojsonpointer-v0.0.0-20180127040702-4e3ac2762d5f.zip",
        ],
    )
    go_repository(
        name = "com_github_xeipuuv_gojsonreference",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/xeipuuv/gojsonreference",
        sha256 = "7ec98f4df894413f4dc58c8df330ca8b24ff425b05a8e1074c3028c99f7e45e7",
        strip_prefix = "github.com/xeipuuv/gojsonreference@v0.0.0-20180127040603-bd5ef7bd5415",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/xeipuuv/gojsonreference/com_github_xeipuuv_gojsonreference-v0.0.0-20180127040603-bd5ef7bd5415.zip",
        ],
    )
    go_repository(
        name = "com_github_xeipuuv_gojsonschema",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/xeipuuv/gojsonschema",
        sha256 = "55c8ce068257aa0d263aad7470113dafcd50f955ee754fc853c2fdcd31ad096f",
        strip_prefix = "github.com/xeipuuv/gojsonschema@v1.2.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/xeipuuv/gojsonschema/com_github_xeipuuv_gojsonschema-v1.2.0.zip",
        ],
    )
    go_repository(
        name = "com_github_xiang90_probing",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/xiang90/probing",
        sha256 = "437bdc666239fda4581b592b068001f08269c68c70699a721bff9334412d4181",
        strip_prefix = "github.com/xiang90/probing@v0.0.0-20190116061207-43a291ad63a2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/xiang90/probing/com_github_xiang90_probing-v0.0.0-20190116061207-43a291ad63a2.zip",
        ],
    )
    go_repository(
        name = "com_github_xlab_treeprint",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/xlab/treeprint",
        sha256 = "4334f3a6e37e92cdd18688a59710663a0f3bff61b225f236fa1be8875e483152",
        strip_prefix = "github.com/xlab/treeprint@v1.1.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/xlab/treeprint/com_github_xlab_treeprint-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "com_github_xordataexchange_crypt",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/xordataexchange/crypt",
        sha256 = "46dc29ef77d77a2bc3e7bd70c94dbaeec0062dd3bd6fcacbaab785c15dcd625b",
        strip_prefix = "github.com/xordataexchange/crypt@v0.0.3-0.20170626215501-b2862e3d0a77",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/xordataexchange/crypt/com_github_xordataexchange_crypt-v0.0.3-0.20170626215501-b2862e3d0a77.zip",
        ],
    )
    go_repository(
        name = "com_github_yalp_jsonpath",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/yalp/jsonpath",
        sha256 = "2cb9c5b63fa0616fbcf73bc1c652f930212d243fdf5f73d1379921deff6dc051",
        strip_prefix = "github.com/yalp/jsonpath@v0.0.0-20180802001716-5cc68e5049a0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/yalp/jsonpath/com_github_yalp_jsonpath-v0.0.0-20180802001716-5cc68e5049a0.zip",
        ],
    )
    go_repository(
        name = "com_github_youmark_pkcs8",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/youmark/pkcs8",
        sha256 = "0aa5f6c2169751b272e7bb04c2951a17beb6bae2111553b74fd1f50c5ea18688",
        strip_prefix = "github.com/youmark/pkcs8@v0.0.0-20181117223130-1be2e3e5546d",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/youmark/pkcs8/com_github_youmark_pkcs8-v0.0.0-20181117223130-1be2e3e5546d.zip",
        ],
    )
    go_repository(
        name = "com_github_yudai_gojsondiff",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/yudai/gojsondiff",
        sha256 = "90c457b595a661a25760d9f10cfda3fec27f7213c0e7026a5b97b30168e8f2d1",
        strip_prefix = "github.com/yudai/gojsondiff@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/yudai/gojsondiff/com_github_yudai_gojsondiff-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "com_github_yudai_golcs",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/yudai/golcs",
        sha256 = "ab50327aa849e409b14f5373543635fb53476792b65a1914f6f90c46fc64ee44",
        strip_prefix = "github.com/yudai/golcs@v0.0.0-20170316035057-ecda9a501e82",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/yudai/golcs/com_github_yudai_golcs-v0.0.0-20170316035057-ecda9a501e82.zip",
        ],
    )
    go_repository(
        name = "com_github_yudai_pp",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/yudai/pp",
        sha256 = "ecfda4152182e295f2b21a7b2726e2865a9415fc135a955ce42e039db29e7a20",
        strip_prefix = "github.com/yudai/pp@v2.0.1+incompatible",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/yudai/pp/com_github_yudai_pp-v2.0.1+incompatible.zip",
        ],
    )
    go_repository(
        name = "com_github_yuin_goldmark",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/yuin/goldmark",
        sha256 = "ba4763c06e40aaa865a0b90a1603f0be13174cc880acfad9640089c6ca9bd086",
        strip_prefix = "github.com/yuin/goldmark@v1.4.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/yuin/goldmark/com_github_yuin_goldmark-v1.4.1.zip",
        ],
    )
    go_repository(
        name = "com_github_yusufpapurcu_wmi",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/yusufpapurcu/wmi",
        sha256 = "5fe3e595564a38f7ba71acf4646ebdf542a1da8fa3afb116afb0fbec0cffe9b1",
        strip_prefix = "github.com/yusufpapurcu/wmi@v1.2.2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/yusufpapurcu/wmi/com_github_yusufpapurcu_wmi-v1.2.2.zip",
        ],
    )
    go_repository(
        name = "com_github_yvasiyarov_go_metrics",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/yvasiyarov/go-metrics",
        sha256 = "1f5232fe57c3b7eb0f106cc757441191ef139cb437883a787c180fc3ad46c43f",
        strip_prefix = "github.com/yvasiyarov/go-metrics@v0.0.0-20140926110328-57bccd1ccd43",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/yvasiyarov/go-metrics/com_github_yvasiyarov_go_metrics-v0.0.0-20140926110328-57bccd1ccd43.zip",
        ],
    )
    go_repository(
        name = "com_github_yvasiyarov_gorelic",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/yvasiyarov/gorelic",
        sha256 = "8e81ca0272c35235d450c0061620cc178df6554bb9d5be5d828c80e08e97fd66",
        strip_prefix = "github.com/yvasiyarov/gorelic@v0.0.0-20141212073537-a9bba5b9ab50",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/yvasiyarov/gorelic/com_github_yvasiyarov_gorelic-v0.0.0-20141212073537-a9bba5b9ab50.zip",
        ],
    )
    go_repository(
        name = "com_github_yvasiyarov_newrelic_platform_go",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/yvasiyarov/newrelic_platform_go",
        sha256 = "67a98b32ee13f9d4f8f8de52c332e4c4eceea9144ccb5141167a2c40db201658",
        strip_prefix = "github.com/yvasiyarov/newrelic_platform_go@v0.0.0-20140908184405-b21fdbd4370f",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/yvasiyarov/newrelic_platform_go/com_github_yvasiyarov_newrelic_platform_go-v0.0.0-20140908184405-b21fdbd4370f.zip",
        ],
    )
    go_repository(
        name = "com_github_z_division_go_zookeeper",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/z-division/go-zookeeper",
        sha256 = "b0a67a3bb3cfbb1be18618b84b02588979795966e040f18c5bb4be036888cabd",
        strip_prefix = "github.com/z-division/go-zookeeper@v0.0.0-20190128072838-6d7457066b9b",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/z-division/go-zookeeper/com_github_z_division_go_zookeeper-v0.0.0-20190128072838-6d7457066b9b.zip",
        ],
    )
    go_repository(
        name = "com_github_zabawaba99_go_gitignore",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/zabawaba99/go-gitignore",
        sha256 = "6c837b93e1c73e53123941c8e866de1deae6b645cc49a7d30d493c146178f8e8",
        strip_prefix = "github.com/zabawaba99/go-gitignore@v0.0.0-20200117185801-39e6bddfb292",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/zabawaba99/go-gitignore/com_github_zabawaba99_go_gitignore-v0.0.0-20200117185801-39e6bddfb292.zip",
        ],
    )
    go_repository(
        name = "com_github_zenazn_goji",
        build_file_proto_mode = "disable_global",
        importpath = "github.com/zenazn/goji",
        sha256 = "0807a255d9d715d18427a6eedd8e4f5a22670b09e5f45fddd229c1ae38da25a9",
        strip_prefix = "github.com/zenazn/goji@v0.9.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/zenazn/goji/com_github_zenazn_goji-v0.9.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go",
        sha256 = "79e47d6cd3f86477d6ae148d52189150160847e10b4ddd11a0ec77fcebf49e80",
        strip_prefix = "cloud.google.com/go@v0.100.2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/cloud.google.com/go/com_google_cloud_go-v0.100.2.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_bigquery",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/bigquery",
        sha256 = "892b10929d4cf89b0c8c9895c17cdeff6c0d28f65e4ffaae6725997572e21249",
        strip_prefix = "cloud.google.com/go/bigquery@v1.8.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/cloud.google.com/go/bigquery/com_google_cloud_go_bigquery-v1.8.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_bigtable",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/bigtable",
        sha256 = "b52d66d4d3761a8e6b5a0e8b7c76f07e0a76ea5c4f563062e440ca3ea8e24ec5",
        strip_prefix = "cloud.google.com/go/bigtable@v1.3.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/cloud.google.com/go/bigtable/com_google_cloud_go_bigtable-v1.3.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_compute",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/compute",
        sha256 = "218c735fda51e41253d69dccbb09be8c494bcfa6062895d4734f1e7c0f4cf0ec",
        strip_prefix = "cloud.google.com/go/compute@v1.1.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/cloud.google.com/go/compute/com_google_cloud_go_compute-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_datastore",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/datastore",
        sha256 = "7dae563dc7c2e544b229f3032103ac05687120e8d25521aef04b45f346ec5ffb",
        strip_prefix = "cloud.google.com/go/datastore@v1.1.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/cloud.google.com/go/datastore/com_google_cloud_go_datastore-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_firestore",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/firestore",
        sha256 = "41156d229c53c4e5e084524f3fecbb2a70d0ec9ac98af7a30413ebd9e8eb4a9e",
        strip_prefix = "cloud.google.com/go/firestore@v1.1.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/cloud.google.com/go/firestore/com_google_cloud_go_firestore-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_iam",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/iam",
        sha256 = "fd7412140440b51b39f605defa8ac062fb2b65dee94d1096db8a62709ae34b8f",
        strip_prefix = "cloud.google.com/go/iam@v0.1.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/cloud.google.com/go/iam/com_google_cloud_go_iam-v0.1.1.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_kms",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/kms",
        sha256 = "8cb6a77ae7336f22aff5287d2f531dab4ca418593bf897d6aaa05a5ced3b0b0a",
        strip_prefix = "cloud.google.com/go/kms@v1.1.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/cloud.google.com/go/kms/com_google_cloud_go_kms-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_pubsub",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/pubsub",
        sha256 = "98134d072e18ee03b96eaf208934bd0fc9431a747a2d62181b86b89c9beceafd",
        strip_prefix = "cloud.google.com/go/pubsub@v1.16.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/cloud.google.com/go/pubsub/com_google_cloud_go_pubsub-v1.16.0.zip",
        ],
    )
    go_repository(
        name = "com_google_cloud_go_storage",
        build_file_proto_mode = "disable_global",
        importpath = "cloud.google.com/go/storage",
        sha256 = "998027df933c89d2a8c5ab535eec479e3210317b13cd9d9b64062c722a48523a",
        strip_prefix = "cloud.google.com/go/storage@v1.18.2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/cloud.google.com/go/storage/com_google_cloud_go_storage-v1.18.2.zip",
        ],
    )
    go_repository(
        name = "com_shuralyov_dmitri_gpu_mtl",
        build_file_proto_mode = "disable_global",
        importpath = "dmitri.shuralyov.com/gpu/mtl",
        sha256 = "d9992a60371571861985f191ac38954db2cc1bd36e834058ad50f989ccc8fa06",
        strip_prefix = "dmitri.shuralyov.com/gpu/mtl@v0.0.0-20201218220906-28db891af037",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/dmitri.shuralyov.com/gpu/mtl/com_shuralyov_dmitri_gpu_mtl-v0.0.0-20201218220906-28db891af037.zip",
        ],
    )
    go_repository(
        name = "com_sourcegraph_sourcegraph_appdash",
        build_file_proto_mode = "disable_global",
        importpath = "sourcegraph.com/sourcegraph/appdash",
        sha256 = "bd2492d9db05362c2fecd0b3d0f6002c89a6d90d678fb93b4158298ab883736f",
        strip_prefix = "sourcegraph.com/sourcegraph/appdash@v0.0.0-20190731080439-ebfcffb1b5c0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/sourcegraph.com/sourcegraph/appdash/com_sourcegraph_sourcegraph_appdash-v0.0.0-20190731080439-ebfcffb1b5c0.zip",
        ],
    )
    go_repository(
        name = "in_gopkg_airbrake_gobrake_v2",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/airbrake/gobrake.v2",
        sha256 = "2db903664908e5a9afafefba94821b9579bbf271e2929c1f0b7b1fdd23f7bbcf",
        strip_prefix = "gopkg.in/airbrake/gobrake.v2@v2.0.9",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/gopkg.in/airbrake/gobrake.v2/in_gopkg_airbrake_gobrake_v2-v2.0.9.zip",
        ],
    )
    go_repository(
        name = "in_gopkg_alecthomas_kingpin_v2",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/alecthomas/kingpin.v2",
        sha256 = "638080591aefe7d2642f2575b627d534c692606f02ea54ba89f42db112ba8839",
        strip_prefix = "gopkg.in/alecthomas/kingpin.v2@v2.2.6",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/gopkg.in/alecthomas/kingpin.v2/in_gopkg_alecthomas_kingpin_v2-v2.2.6.zip",
        ],
    )
    go_repository(
        name = "in_gopkg_asn1_ber_v1",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/asn1-ber.v1",
        sha256 = "fee158570ba9cbfc11156afbe9b9ab0833ab00d0f1a2a2af29a6325984a79903",
        strip_prefix = "gopkg.in/asn1-ber.v1@v1.0.0-20181015200546-f715ec2f112d",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/gopkg.in/asn1-ber.v1/in_gopkg_asn1_ber_v1-v1.0.0-20181015200546-f715ec2f112d.zip",
        ],
    )
    go_repository(
        name = "in_gopkg_check_v1",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/check.v1",
        sha256 = "f555684e5c5dacc2850dddb345fef1b8f93f546b72685589789da6d2b062710e",
        strip_prefix = "gopkg.in/check.v1@v1.0.0-20201130134442-10cb98267c6c",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/gopkg.in/check.v1/in_gopkg_check_v1-v1.0.0-20201130134442-10cb98267c6c.zip",
        ],
    )
    go_repository(
        name = "in_gopkg_cheggaaa_pb_v1",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/cheggaaa/pb.v1",
        sha256 = "12c7e316faacb5cfa5d0851b6e576391b2517a36d5221f42443cd8435394d9fe",
        strip_prefix = "gopkg.in/cheggaaa/pb.v1@v1.0.25",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/gopkg.in/cheggaaa/pb.v1/in_gopkg_cheggaaa_pb_v1-v1.0.25.zip",
        ],
    )
    go_repository(
        name = "in_gopkg_datadog_dd_trace_go_v1",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/DataDog/dd-trace-go.v1",
        sha256 = "2ebcc818df0b2d560a61037da4492ae7effbaed67de94339a1d3a72728d2cb09",
        strip_prefix = "gopkg.in/DataDog/dd-trace-go.v1@v1.17.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/gopkg.in/DataDog/dd-trace-go.v1/in_gopkg_datadog_dd_trace_go_v1-v1.17.0.zip",
        ],
    )
    go_repository(
        name = "in_gopkg_errgo_v2",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/errgo.v2",
        sha256 = "6b8954819a20ec52982a206fd3eb94629ff53c5790aa77534e6d8daf7de01bee",
        strip_prefix = "gopkg.in/errgo.v2@v2.1.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/gopkg.in/errgo.v2/in_gopkg_errgo_v2-v2.1.0.zip",
        ],
    )
    go_repository(
        name = "in_gopkg_fsnotify_fsnotify_v1",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/fsnotify/fsnotify.v1",
        sha256 = "6f74f844c970ff3059d1639c8a850d9ba7029dd059b5d9a305f87bd307c05491",
        strip_prefix = "gopkg.in/fsnotify/fsnotify.v1@v1.4.7",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/gopkg.in/fsnotify/fsnotify.v1/in_gopkg_fsnotify_fsnotify_v1-v1.4.7.zip",
        ],
    )
    go_repository(
        name = "in_gopkg_fsnotify_v1",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/fsnotify.v1",
        sha256 = "ce003d540f42b3c0a3dec385deb387b255b536b25ea4438baa65b89458b28f75",
        strip_prefix = "gopkg.in/fsnotify.v1@v1.4.7",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/gopkg.in/fsnotify.v1/in_gopkg_fsnotify_v1-v1.4.7.zip",
        ],
    )
    go_repository(
        name = "in_gopkg_gcfg_v1",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/gcfg.v1",
        sha256 = "06cdad29610507bafb35e2e73d64fd7aa6c5c2ce1e5feff30a622af5475bca3b",
        strip_prefix = "gopkg.in/gcfg.v1@v1.2.3",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/gopkg.in/gcfg.v1/in_gopkg_gcfg_v1-v1.2.3.zip",
        ],
    )
    go_repository(
        name = "in_gopkg_gemnasium_logrus_airbrake_hook_v2",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/gemnasium/logrus-airbrake-hook.v2",
        sha256 = "ce35c69d2a1f49d8672447bced4833c02cc7af036aa9df94d5a6a0f5d871cccd",
        strip_prefix = "gopkg.in/gemnasium/logrus-airbrake-hook.v2@v2.1.2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/gopkg.in/gemnasium/logrus-airbrake-hook.v2/in_gopkg_gemnasium_logrus_airbrake_hook_v2-v2.1.2.zip",
        ],
    )
    go_repository(
        name = "in_gopkg_go_playground_assert_v1",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/go-playground/assert.v1",
        sha256 = "11da2f608d82304df2384a2301e0155fe72e8414e1a17776f1966c3a4c403bc4",
        strip_prefix = "gopkg.in/go-playground/assert.v1@v1.2.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/gopkg.in/go-playground/assert.v1/in_gopkg_go_playground_assert_v1-v1.2.1.zip",
        ],
    )
    go_repository(
        name = "in_gopkg_go_playground_validator_v8",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/go-playground/validator.v8",
        sha256 = "fea7482c7122c2573d964b7d294a78f2162fa206ccd4b808d0c82f3d87b4d159",
        strip_prefix = "gopkg.in/go-playground/validator.v8@v8.18.2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/gopkg.in/go-playground/validator.v8/in_gopkg_go_playground_validator_v8-v8.18.2.zip",
        ],
    )
    go_repository(
        name = "in_gopkg_go_playground_validator_v9",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/go-playground/validator.v9",
        sha256 = "1c54e86e418da6789520d7ed9d0b53727c539b6a73ea8538f8b85f6bbcf352ad",
        strip_prefix = "gopkg.in/go-playground/validator.v9@v9.29.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/gopkg.in/go-playground/validator.v9/in_gopkg_go_playground_validator_v9-v9.29.1.zip",
        ],
    )
    go_repository(
        name = "in_gopkg_inconshreveable_log15_v2",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/inconshreveable/log15.v2",
        sha256 = "799307ed46ca30ca0ac2dc0332f3673814b8ff6cc1ee905a462ccfd438e8e695",
        strip_prefix = "gopkg.in/inconshreveable/log15.v2@v2.0.0-20180818164646-67afb5ed74ec",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/gopkg.in/inconshreveable/log15.v2/in_gopkg_inconshreveable_log15_v2-v2.0.0-20180818164646-67afb5ed74ec.zip",
        ],
    )
    go_repository(
        name = "in_gopkg_inf_v0",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/inf.v0",
        sha256 = "08abac18c95cc43b725d4925f63309398d618beab68b4669659b61255e5374a0",
        strip_prefix = "gopkg.in/inf.v0@v0.9.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/gopkg.in/inf.v0/in_gopkg_inf_v0-v0.9.1.zip",
        ],
    )
    go_repository(
        name = "in_gopkg_ini_v1",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/ini.v1",
        sha256 = "a4a3f632adece28b596b54e5e71f986158188e29e58b4d84b3abd3ed240f5826",
        strip_prefix = "gopkg.in/ini.v1@v1.66.3",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/gopkg.in/ini.v1/in_gopkg_ini_v1-v1.66.3.zip",
        ],
    )
    go_repository(
        name = "in_gopkg_jcmturner_aescts_v1",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/jcmturner/aescts.v1",
        sha256 = "8bfd83c7204032fb16946202d5d643bd9a7e618005bd39578f29030a7d51dcf9",
        strip_prefix = "gopkg.in/jcmturner/aescts.v1@v1.0.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/gopkg.in/jcmturner/aescts.v1/in_gopkg_jcmturner_aescts_v1-v1.0.1.zip",
        ],
    )
    go_repository(
        name = "in_gopkg_jcmturner_dnsutils_v1",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/jcmturner/dnsutils.v1",
        sha256 = "4fb8b6a5471cb6dda1d0aabd1e01e4d54cb5ee83c395849916392b19153f5203",
        strip_prefix = "gopkg.in/jcmturner/dnsutils.v1@v1.0.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/gopkg.in/jcmturner/dnsutils.v1/in_gopkg_jcmturner_dnsutils_v1-v1.0.1.zip",
        ],
    )
    go_repository(
        name = "in_gopkg_jcmturner_goidentity_v3",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/jcmturner/goidentity.v3",
        sha256 = "1be44bee93d9080ce89f40827c57e8a396b7c801e2d19a1f5446a4325afa755e",
        strip_prefix = "gopkg.in/jcmturner/goidentity.v3@v3.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/gopkg.in/jcmturner/goidentity.v3/in_gopkg_jcmturner_goidentity_v3-v3.0.0.zip",
        ],
    )
    go_repository(
        name = "in_gopkg_jcmturner_gokrb5_v7",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/jcmturner/gokrb5.v7",
        sha256 = "0d54c32510f4ab41729761fda5b448c5124917752485711f3d0c0810460134b8",
        strip_prefix = "gopkg.in/jcmturner/gokrb5.v7@v7.5.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/gopkg.in/jcmturner/gokrb5.v7/in_gopkg_jcmturner_gokrb5_v7-v7.5.0.zip",
        ],
    )
    go_repository(
        name = "in_gopkg_jcmturner_rpc_v1",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/jcmturner/rpc.v1",
        sha256 = "83d897b60ecb5a66d25232b775ed04c182ca8e02431f351b3768d4d2876d07ae",
        strip_prefix = "gopkg.in/jcmturner/rpc.v1@v1.1.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/gopkg.in/jcmturner/rpc.v1/in_gopkg_jcmturner_rpc_v1-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "in_gopkg_ldap_v2",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/ldap.v2",
        sha256 = "44fbb28e1a7b33d08edd31957f9fea15744979a97392d89a894306a610ed78f1",
        strip_prefix = "gopkg.in/ldap.v2@v2.5.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/gopkg.in/ldap.v2/in_gopkg_ldap_v2-v2.5.0.zip",
        ],
    )
    go_repository(
        name = "in_gopkg_mgo_v2",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/mgo.v2",
        sha256 = "86c056ac7d51d59bb158bb740e774c0f80b28c8ce8db56d619a569aa96b2cd03",
        strip_prefix = "gopkg.in/mgo.v2@v2.0.0-20180705113604-9856a29383ce",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/gopkg.in/mgo.v2/in_gopkg_mgo_v2-v2.0.0-20180705113604-9856a29383ce.zip",
        ],
    )
    go_repository(
        name = "in_gopkg_natefinch_lumberjack_v2",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/natefinch/lumberjack.v2",
        sha256 = "8c268e36660d6ce36af808d74b9be80207c05463679703e93d857e954c637aaa",
        strip_prefix = "gopkg.in/natefinch/lumberjack.v2@v2.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/gopkg.in/natefinch/lumberjack.v2/in_gopkg_natefinch_lumberjack_v2-v2.0.0.zip",
        ],
    )
    go_repository(
        name = "in_gopkg_resty_v1",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/resty.v1",
        sha256 = "43487bb0bb40626d16502b1fe9e719cf751e7a5b4e4233276971873e7863d3cf",
        strip_prefix = "gopkg.in/resty.v1@v1.12.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/gopkg.in/resty.v1/in_gopkg_resty_v1-v1.12.0.zip",
        ],
    )
    go_repository(
        name = "in_gopkg_square_go_jose_v2",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/square/go-jose.v2",
        sha256 = "1eca83b44bbb8ec53ad5643e0e3c2c9a646e3411f7bd9c3cd4fb16895d72a9f9",
        strip_prefix = "gopkg.in/square/go-jose.v2@v2.5.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/gopkg.in/square/go-jose.v2/in_gopkg_square_go_jose_v2-v2.5.1.zip",
        ],
    )
    go_repository(
        name = "in_gopkg_src_d_go_billy_v4",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/src-d/go-billy.v4",
        sha256 = "389c7137c3424429eb3454b382a19a4c5050b397f80a9be112a50e65d8a0e353",
        strip_prefix = "gopkg.in/src-d/go-billy.v4@v4.3.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/gopkg.in/src-d/go-billy.v4/in_gopkg_src_d_go_billy_v4-v4.3.0.zip",
        ],
    )
    go_repository(
        name = "in_gopkg_tomb_v1",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/tomb.v1",
        sha256 = "34898dc0e38ba7a792ab74a3e0fa113116313fd9142ffb444b011fd392762186",
        strip_prefix = "gopkg.in/tomb.v1@v1.0.0-20141024135613-dd632973f1e7",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/gopkg.in/tomb.v1/in_gopkg_tomb_v1-v1.0.0-20141024135613-dd632973f1e7.zip",
        ],
    )
    go_repository(
        name = "in_gopkg_warnings_v0",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/warnings.v0",
        sha256 = "c412b1f704c1e8ba59b6cfdb1072f8be847c03f77d6507c692913d6d9454e51c",
        strip_prefix = "gopkg.in/warnings.v0@v0.1.2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/gopkg.in/warnings.v0/in_gopkg_warnings_v0-v0.1.2.zip",
        ],
    )
    go_repository(
        name = "in_gopkg_yaml_v2",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/yaml.v2",
        sha256 = "98f901d1a2446ea98010e56f8f0587f2f790704ea56d14417803602b214e5697",
        strip_prefix = "github.com/cockroachdb/yaml@v0.0.0-20210825132133-2d6955c8edbc",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/cockroachdb/yaml/com_github_cockroachdb_yaml-v0.0.0-20210825132133-2d6955c8edbc.zip",
        ],
    )
    go_repository(
        name = "in_gopkg_yaml_v3",
        build_file_proto_mode = "disable_global",
        importpath = "gopkg.in/yaml.v3",
        sha256 = "5169b5625d3c351f13e8a4ec4802f709072701b441ed92181c6051ece53615a9",
        strip_prefix = "gopkg.in/yaml.v3@v3.0.0-20210107192922-496545a6307b",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/gopkg.in/yaml.v3/in_gopkg_yaml_v3-v3.0.0-20210107192922-496545a6307b.zip",
        ],
    )
    go_repository(
        name = "io_etcd_go_bbolt",
        build_file_proto_mode = "disable_global",
        importpath = "go.etcd.io/bbolt",
        sha256 = "cbb488f86631df05edb2aa3e2c25e0d5994d46a118fc5f2150932fa6ed469cff",
        strip_prefix = "go.etcd.io/bbolt@v1.3.5",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/go.etcd.io/bbolt/io_etcd_go_bbolt-v1.3.5.zip",
        ],
    )
    go_repository(
        name = "io_etcd_go_etcd",
        build_file_proto_mode = "disable_global",
        importpath = "go.etcd.io/etcd",
        sha256 = "d982ee501979b41b68625693bad77d15e4ae79ab9d0eae5f6028205f96a74e49",
        strip_prefix = "go.etcd.io/etcd@v0.5.0-alpha.5.0.20200910180754-dd1b699fc489",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/go.etcd.io/etcd/io_etcd_go_etcd-v0.5.0-alpha.5.0.20200910180754-dd1b699fc489.zip",
        ],
    )
    go_repository(
        name = "io_etcd_go_etcd_api_v3",
        build_file_proto_mode = "disable_global",
        importpath = "go.etcd.io/etcd/api/v3",
        sha256 = "8754587bf6d4b1bc889d519355ea8899e093d8550e0d98730f8570d608f998f9",
        strip_prefix = "go.etcd.io/etcd/api/v3@v3.5.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/go.etcd.io/etcd/api/v3/io_etcd_go_etcd_api_v3-v3.5.0.zip",
        ],
    )
    go_repository(
        name = "io_etcd_go_etcd_client_pkg_v3",
        build_file_proto_mode = "disable_global",
        importpath = "go.etcd.io/etcd/client/pkg/v3",
        sha256 = "c0ca209767c5734c6ed023888ba5be02aab5bd3c4d018999467f2bfa8bf65ee3",
        strip_prefix = "go.etcd.io/etcd/client/pkg/v3@v3.5.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/go.etcd.io/etcd/client/pkg/v3/io_etcd_go_etcd_client_pkg_v3-v3.5.0.zip",
        ],
    )
    go_repository(
        name = "io_etcd_go_etcd_client_v2",
        build_file_proto_mode = "disable_global",
        importpath = "go.etcd.io/etcd/client/v2",
        sha256 = "91fcb507fe8c193844b56bfb6c8741aaeb6ffa11ee9043de2af0f141173679f3",
        strip_prefix = "go.etcd.io/etcd/client/v2@v2.305.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/go.etcd.io/etcd/client/v2/io_etcd_go_etcd_client_v2-v2.305.0.zip",
        ],
    )
    go_repository(
        name = "io_etcd_go_etcd_pkg_v3",
        build_file_proto_mode = "disable_global",
        importpath = "go.etcd.io/etcd/pkg/v3",
        sha256 = "1700dfed48becf82ccfe6865fe59daac2121d48f60b7c4bf090f0ff2320d33d4",
        strip_prefix = "go.etcd.io/etcd/pkg/v3@v3.0.0-20201109164711-01844fd28560",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/go.etcd.io/etcd/pkg/v3/io_etcd_go_etcd_pkg_v3-v3.0.0-20201109164711-01844fd28560.zip",
        ],
    )
    go_repository(
        name = "io_etcd_go_etcd_raft_v3",
        build_file_proto_mode = "disable_global",
        importpath = "go.etcd.io/etcd/raft/v3",
        patch_args = ["-p1"],
        patches = [
            "@cockroach//build/patches:io_etcd_go_etcd_raft_v3.patch",
        ],
        sha256 = "62faedd81e10061a4e0d7476865a62b84121ea462514afeaa1b9d66cc53b5a4b",
        strip_prefix = "go.etcd.io/etcd/raft/v3@v3.0.0-20210320072418-e51c697ec6e8",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/go.etcd.io/etcd/raft/v3/io_etcd_go_etcd_raft_v3-v3.0.0-20210320072418-e51c697ec6e8.zip",
        ],
    )
    go_repository(
        name = "io_goji",
        build_file_proto_mode = "disable_global",
        importpath = "goji.io",
        sha256 = "1ea69b28e356cb91381ce2339004fcf144ad1b268c9e3497c9ef304751ae0bb3",
        strip_prefix = "goji.io@v2.0.2+incompatible",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/goji.io/io_goji-v2.0.2+incompatible.zip",
        ],
    )
    go_repository(
        name = "io_gorm_driver_postgres",
        build_file_proto_mode = "disable_global",
        importpath = "gorm.io/driver/postgres",
        sha256 = "a35ed3044796b91cf9ead206329ceb34b256da3044a3528a3aa185cd128f8ac8",
        strip_prefix = "gorm.io/driver/postgres@v1.0.8",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/gorm.io/driver/postgres/io_gorm_driver_postgres-v1.0.8.zip",
        ],
    )
    go_repository(
        name = "io_gorm_gorm",
        build_file_proto_mode = "disable_global",
        importpath = "gorm.io/gorm",
        sha256 = "4048b3c1d2cd0b8372755fcf9bc33d621c1e532e4e2b791c17036e02ac9b0694",
        strip_prefix = "gorm.io/gorm@v1.21.4",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/gorm.io/gorm/io_gorm_gorm-v1.21.4.zip",
        ],
    )
    go_repository(
        name = "io_k8s_api",
        build_file_proto_mode = "disable_global",
        importpath = "k8s.io/api",
        sha256 = "f7bdf8655b5078d05124dacb2eeaf2e2dc356334f174b623d3afa9130517e272",
        strip_prefix = "k8s.io/api@v0.22.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/k8s.io/api/io_k8s_api-v0.22.1.zip",
        ],
    )
    go_repository(
        name = "io_k8s_apiextensions_apiserver",
        build_file_proto_mode = "disable_global",
        importpath = "k8s.io/apiextensions-apiserver",
        sha256 = "f3be44b21eaea21dbc2655f207f838a94e4ed63b24e5ce4f1d688c329b53c9ff",
        strip_prefix = "k8s.io/apiextensions-apiserver@v0.17.3",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/k8s.io/apiextensions-apiserver/io_k8s_apiextensions_apiserver-v0.17.3.zip",
        ],
    )
    go_repository(
        name = "io_k8s_apimachinery",
        build_file_proto_mode = "disable_global",
        importpath = "k8s.io/apimachinery",
        sha256 = "2097d287ef9b837b18951bea7cca7cab8bdd3d25fd4c30210fc8cb8fcbbf2ca0",
        strip_prefix = "k8s.io/apimachinery@v0.22.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/k8s.io/apimachinery/io_k8s_apimachinery-v0.22.1.zip",
        ],
    )
    go_repository(
        name = "io_k8s_apiserver",
        build_file_proto_mode = "disable_global",
        importpath = "k8s.io/apiserver",
        sha256 = "b5fc2f9e9476511badce4bd460880113a9bee48fc9a22747003eb75327c9b98b",
        strip_prefix = "k8s.io/apiserver@v0.20.6",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/k8s.io/apiserver/io_k8s_apiserver-v0.20.6.zip",
        ],
    )
    go_repository(
        name = "io_k8s_client_go",
        build_file_proto_mode = "disable_global",
        importpath = "k8s.io/client-go",
        sha256 = "cf2ce10509391b8633c8368881197c296e9ea060df227c0e002bc2df1b07dc31",
        strip_prefix = "k8s.io/client-go@v0.22.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/k8s.io/client-go/io_k8s_client_go-v0.22.1.zip",
        ],
    )
    go_repository(
        name = "io_k8s_code_generator",
        build_file_proto_mode = "disable_global",
        importpath = "k8s.io/code-generator",
        sha256 = "59dd76fe046441365da49abb1eb22db7e368508f2622730b0bf125b044bbf07d",
        strip_prefix = "k8s.io/code-generator@v0.17.3",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/k8s.io/code-generator/io_k8s_code_generator-v0.17.3.zip",
        ],
    )
    go_repository(
        name = "io_k8s_component_base",
        build_file_proto_mode = "disable_global",
        importpath = "k8s.io/component-base",
        sha256 = "b42b00af9459d285c9372c30d605204b4ec64e90f2eaeb17e9e1bf700bbc84cf",
        strip_prefix = "k8s.io/component-base@v0.20.6",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/k8s.io/component-base/io_k8s_component_base-v0.20.6.zip",
        ],
    )
    go_repository(
        name = "io_k8s_cri_api",
        build_file_proto_mode = "disable_global",
        importpath = "k8s.io/cri-api",
        sha256 = "7c3559a06c58744fd8a4491a809257b979c7ac8def5dda9973648e38ff05471c",
        strip_prefix = "k8s.io/cri-api@v0.20.6",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/k8s.io/cri-api/io_k8s_cri_api-v0.20.6.zip",
        ],
    )
    go_repository(
        name = "io_k8s_gengo",
        build_file_proto_mode = "disable_global",
        importpath = "k8s.io/gengo",
        sha256 = "2591d39f698cdb50c870a8b97706f5c2b4d2819bd95e9b5c3ff57aca905264e1",
        strip_prefix = "k8s.io/gengo@v0.0.0-20200413195148-3a45101e95ac",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/k8s.io/gengo/io_k8s_gengo-v0.0.0-20200413195148-3a45101e95ac.zip",
        ],
    )
    go_repository(
        name = "io_k8s_klog",
        build_file_proto_mode = "disable_global",
        importpath = "k8s.io/klog",
        sha256 = "a564b06078ddf014c5b793a7d36643d6fda31fc131e36b95cdea94ff838b99be",
        strip_prefix = "k8s.io/klog@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/k8s.io/klog/io_k8s_klog-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "io_k8s_klog_v2",
        build_file_proto_mode = "disable_global",
        importpath = "k8s.io/klog/v2",
        sha256 = "ccf4ab19ef25cf4208c03f8656161d989bb4fa5075f8ce04afb29656d96d8606",
        strip_prefix = "k8s.io/klog/v2@v2.10.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/k8s.io/klog/v2/io_k8s_klog_v2-v2.10.0.zip",
        ],
    )
    go_repository(
        name = "io_k8s_kube_openapi",
        build_file_proto_mode = "disable_global",
        importpath = "k8s.io/kube-openapi",
        sha256 = "face91ada098a926d588b0e96b40715f68dfc375e17b6c10274ae6f20849b55c",
        strip_prefix = "k8s.io/kube-openapi@v0.0.0-20210421082810-95288971da7e",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/k8s.io/kube-openapi/io_k8s_kube_openapi-v0.0.0-20210421082810-95288971da7e.zip",
        ],
    )
    go_repository(
        name = "io_k8s_kubernetes",
        build_file_proto_mode = "disable_global",
        importpath = "k8s.io/kubernetes",
        sha256 = "f065c08345beaa714fa5c81a548e2015babd496729f333721948b341eef9eb36",
        strip_prefix = "k8s.io/kubernetes@v1.13.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/k8s.io/kubernetes/io_k8s_kubernetes-v1.13.0.zip",
        ],
    )
    go_repository(
        name = "io_k8s_sigs_apiserver_network_proxy_konnectivity_client",
        build_file_proto_mode = "disable_global",
        importpath = "sigs.k8s.io/apiserver-network-proxy/konnectivity-client",
        sha256 = "70c5b8d90b0c0d03d4b75dd2af46664965f20d7f54a9cfc340256128eba457d2",
        strip_prefix = "sigs.k8s.io/apiserver-network-proxy/konnectivity-client@v0.0.15",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/sigs.k8s.io/apiserver-network-proxy/konnectivity-client/io_k8s_sigs_apiserver_network_proxy_konnectivity_client-v0.0.15.zip",
        ],
    )
    go_repository(
        name = "io_k8s_sigs_structured_merge_diff",
        build_file_proto_mode = "disable_global",
        importpath = "sigs.k8s.io/structured-merge-diff",
        sha256 = "a7c7f139cf93c42d9954f02b1f3d393f988cf6b8a423b3baf0f13716dab95141",
        strip_prefix = "sigs.k8s.io/structured-merge-diff@v1.0.1-0.20191108220359-b1b620dd3f06",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/sigs.k8s.io/structured-merge-diff/io_k8s_sigs_structured_merge_diff-v1.0.1-0.20191108220359-b1b620dd3f06.zip",
        ],
    )
    go_repository(
        name = "io_k8s_sigs_structured_merge_diff_v2",
        build_file_proto_mode = "disable_global",
        importpath = "sigs.k8s.io/structured-merge-diff/v2",
        sha256 = "42c1a3be55e05ee4419bb4833419723a32bfa272e27ee3344efb3f570548c43b",
        strip_prefix = "sigs.k8s.io/structured-merge-diff/v2@v2.0.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/sigs.k8s.io/structured-merge-diff/v2/io_k8s_sigs_structured_merge_diff_v2-v2.0.1.zip",
        ],
    )
    go_repository(
        name = "io_k8s_sigs_structured_merge_diff_v4",
        build_file_proto_mode = "disable_global",
        importpath = "sigs.k8s.io/structured-merge-diff/v4",
        sha256 = "b32af97dadd79179a8f62aaf4ef1e0562e051be77053a60c7a4e724a5cbd00ce",
        strip_prefix = "sigs.k8s.io/structured-merge-diff/v4@v4.1.2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/sigs.k8s.io/structured-merge-diff/v4/io_k8s_sigs_structured_merge_diff_v4-v4.1.2.zip",
        ],
    )
    go_repository(
        name = "io_k8s_sigs_yaml",
        build_file_proto_mode = "disable_global",
        importpath = "sigs.k8s.io/yaml",
        sha256 = "55ed08c5df448a033bf7e2c2912d4daa85b856a05c854b0c87ccc85c7f3fbfc7",
        strip_prefix = "sigs.k8s.io/yaml@v1.2.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/sigs.k8s.io/yaml/io_k8s_sigs_yaml-v1.2.0.zip",
        ],
    )
    go_repository(
        name = "io_k8s_utils",
        build_file_proto_mode = "disable_global",
        importpath = "k8s.io/utils",
        sha256 = "0af5b5d14e8b80fcf9cb01dbc35d7627cccd6c87573cf2a3249c218a2b72ce89",
        strip_prefix = "k8s.io/utils@v0.0.0-20210707171843-4b05e18ac7d9",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/k8s.io/utils/io_k8s_utils-v0.0.0-20210707171843-4b05e18ac7d9.zip",
        ],
    )
    go_repository(
        name = "io_opencensus_go",
        build_file_proto_mode = "disable_global",
        importpath = "go.opencensus.io",
        sha256 = "81c78beb84872084d6d5ddc0a0bffc47294412898472c891a29cfcb66f3fa2d8",
        strip_prefix = "go.opencensus.io@v0.23.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/go.opencensus.io/io_opencensus_go-v0.23.0.zip",
        ],
    )
    go_repository(
        name = "io_opencensus_go_contrib_exporter_prometheus",
        build_file_proto_mode = "disable_global",
        importpath = "contrib.go.opencensus.io/exporter/prometheus",
        sha256 = "e5dc381a98aad09e887f5232b00147308ff806e9189fbf901736ccded75a3357",
        strip_prefix = "contrib.go.opencensus.io/exporter/prometheus@v0.4.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/contrib.go.opencensus.io/exporter/prometheus/io_opencensus_go_contrib_exporter_prometheus-v0.4.0.zip",
        ],
    )
    go_repository(
        name = "io_opentelemetry_go_otel",
        build_file_proto_mode = "disable_global",
        importpath = "go.opentelemetry.io/otel",
        sha256 = "e909ee92d1e37586550fe693701ef8064b58794a3ed5cc61351a07fac6b40ee0",
        strip_prefix = "go.opentelemetry.io/otel@v1.0.0-RC3",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/go.opentelemetry.io/otel/io_opentelemetry_go_otel-v1.0.0-RC3.zip",
        ],
    )
    go_repository(
        name = "io_opentelemetry_go_otel_exporters_jaeger",
        build_file_proto_mode = "disable_global",
        importpath = "go.opentelemetry.io/otel/exporters/jaeger",
        sha256 = "89e57276d1f4c79e2353bda09fdeb6eb5c47af0cd974a490637ce1f71744830c",
        strip_prefix = "go.opentelemetry.io/otel/exporters/jaeger@v1.0.0-RC3",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/go.opentelemetry.io/otel/exporters/jaeger/io_opentelemetry_go_otel_exporters_jaeger-v1.0.0-RC3.zip",
        ],
    )
    go_repository(
        name = "io_opentelemetry_go_otel_exporters_otlp_otlptrace",
        build_file_proto_mode = "disable_global",
        importpath = "go.opentelemetry.io/otel/exporters/otlp/otlptrace",
        sha256 = "fc6bd34099ffc6bc523dab02b678ebaa324c06dc0f95ddcbacc7633705ded1f0",
        strip_prefix = "go.opentelemetry.io/otel/exporters/otlp/otlptrace@v1.0.0-RC3.0.20210907151655-df2bdbbadb26",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/go.opentelemetry.io/otel/exporters/otlp/otlptrace/io_opentelemetry_go_otel_exporters_otlp_otlptrace-v1.0.0-RC3.0.20210907151655-df2bdbbadb26.zip",
        ],
    )
    go_repository(
        name = "io_opentelemetry_go_otel_exporters_otlp_otlptrace_otlptracegrpc",
        build_file_proto_mode = "disable_global",
        importpath = "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc",
        sha256 = "b6cddcb6cbee0e0dc31b6a125e270db8aadb6017f1449b5d0874c2963b12d2e9",
        strip_prefix = "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc@v1.0.0-RC3",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc/io_opentelemetry_go_otel_exporters_otlp_otlptrace_otlptracegrpc-v1.0.0-RC3.zip",
        ],
    )
    go_repository(
        name = "io_opentelemetry_go_otel_exporters_zipkin",
        build_file_proto_mode = "disable_global",
        importpath = "go.opentelemetry.io/otel/exporters/zipkin",
        sha256 = "4e4074dc5fa0ae55cddbba06d4c266c23b0461bd5056ab69055f203b506fa64b",
        strip_prefix = "go.opentelemetry.io/otel/exporters/zipkin@v1.0.0-RC3",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/go.opentelemetry.io/otel/exporters/zipkin/io_opentelemetry_go_otel_exporters_zipkin-v1.0.0-RC3.zip",
        ],
    )
    go_repository(
        name = "io_opentelemetry_go_otel_metric",
        build_file_proto_mode = "disable_global",
        importpath = "go.opentelemetry.io/otel/metric",
        sha256 = "d7ae3abbdcf9ea48ff23a477f324cb3595c77f3eb83f6acde5c0c9300e23fedb",
        strip_prefix = "go.opentelemetry.io/otel/metric@v0.20.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/go.opentelemetry.io/otel/metric/io_opentelemetry_go_otel_metric-v0.20.0.zip",
        ],
    )
    go_repository(
        name = "io_opentelemetry_go_otel_sdk",
        build_file_proto_mode = "disable_global",
        importpath = "go.opentelemetry.io/otel/sdk",
        sha256 = "8f4a796143524ee6905d2f53ec402a314228457038298ef106e72bbb1b97a674",
        strip_prefix = "go.opentelemetry.io/otel/sdk@v1.0.0-RC3",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/go.opentelemetry.io/otel/sdk/io_opentelemetry_go_otel_sdk-v1.0.0-RC3.zip",
        ],
    )
    go_repository(
        name = "io_opentelemetry_go_otel_trace",
        build_file_proto_mode = "disable_global",
        importpath = "go.opentelemetry.io/otel/trace",
        sha256 = "534dc0f9e0d8b540ff3b73e52dd0554960f7b5818cacbb1e17113ef1225da5bd",
        strip_prefix = "go.opentelemetry.io/otel/trace@v1.0.0-RC3",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/go.opentelemetry.io/otel/trace/io_opentelemetry_go_otel_trace-v1.0.0-RC3.zip",
        ],
    )
    go_repository(
        name = "io_opentelemetry_go_proto_otlp",
        build_file_proto_mode = "disable_global",
        importpath = "go.opentelemetry.io/proto/otlp",
        patch_args = ["-p1"],
        patches = [
            "@cockroach//build/patches:io_opentelemetry_go_proto_otlp.patch",
        ],
        sha256 = "1a91376c923da07bee23439e8430c32736f6330532df85d3bd1ada90305097d7",
        strip_prefix = "go.opentelemetry.io/proto/otlp@v0.9.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/go.opentelemetry.io/proto/otlp/io_opentelemetry_go_proto_otlp-v0.9.0.zip",
        ],
    )
    go_repository(
        name = "io_rsc_binaryregexp",
        build_file_proto_mode = "disable_global",
        importpath = "rsc.io/binaryregexp",
        sha256 = "b3e706aa278fa7f880d32fa1cc40ef8282d1fc7d6e00356579ed0db88f3b0047",
        strip_prefix = "rsc.io/binaryregexp@v0.2.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/rsc.io/binaryregexp/io_rsc_binaryregexp-v0.2.0.zip",
        ],
    )
    go_repository(
        name = "io_rsc_pdf",
        build_file_proto_mode = "disable_global",
        importpath = "rsc.io/pdf",
        sha256 = "79bf310e399cf0e2d8aa61536750d2a6999c5ca884e7a27faf88d3701cd5ba8f",
        strip_prefix = "rsc.io/pdf@v0.1.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/rsc.io/pdf/io_rsc_pdf-v0.1.1.zip",
        ],
    )
    go_repository(
        name = "io_rsc_quote_v3",
        build_file_proto_mode = "disable_global",
        importpath = "rsc.io/quote/v3",
        sha256 = "b434cbbfc32c17b5228d0b0eddeaea89bef4ec9bd90b5c8fc55b64f8ce13eeb9",
        strip_prefix = "rsc.io/quote/v3@v3.1.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/rsc.io/quote/v3/io_rsc_quote_v3-v3.1.0.zip",
        ],
    )
    go_repository(
        name = "io_rsc_sampler",
        build_file_proto_mode = "disable_global",
        importpath = "rsc.io/sampler",
        sha256 = "da202b0da803ab2661ab98a680bba4f64123a326e540c25582b6cdbb9dc114aa",
        strip_prefix = "rsc.io/sampler@v1.3.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/rsc.io/sampler/io_rsc_sampler-v1.3.0.zip",
        ],
    )
    go_repository(
        name = "io_vitess_vitess",
        build_file_proto_mode = "disable_global",
        importpath = "vitess.io/vitess",
        sha256 = "71f14e67f9396930d978d85c47b853f5cc4ce340e53cf88bf7d731b8428b2f77",
        strip_prefix = "github.com/cockroachdb/vitess@v0.0.0-20210218160543-54524729cc82",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/github.com/cockroachdb/vitess/com_github_cockroachdb_vitess-v0.0.0-20210218160543-54524729cc82.zip",
        ],
    )
    go_repository(
        name = "org_bazil_fuse",
        build_file_proto_mode = "disable_global",
        importpath = "bazil.org/fuse",
        sha256 = "c4f8d08b812e14a7689471372b43e43a0d6c984cdf3d9e541750d69398442e5a",
        strip_prefix = "bazil.org/fuse@v0.0.0-20160811212531-371fbbdaa898",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/bazil.org/fuse/org_bazil_fuse-v0.0.0-20160811212531-371fbbdaa898.zip",
        ],
    )
    go_repository(
        name = "org_collectd",
        build_file_proto_mode = "disable_global",
        importpath = "collectd.org",
        sha256 = "18974a8911a7e89cdeb35f25daddf37eb5026fd42a54a4116fa0fd5af457ae4c",
        strip_prefix = "collectd.org@v0.3.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/collectd.org/org_collectd-v0.3.0.zip",
        ],
    )
    go_repository(
        name = "org_golang_google_api",
        build_file_proto_mode = "disable_global",
        importpath = "google.golang.org/api",
        sha256 = "8e97065d27143c3b9d80bcc2afd9d97a038a020a7d1be3699350780cbe187dc2",
        strip_prefix = "google.golang.org/api@v0.65.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/google.golang.org/api/org_golang_google_api-v0.65.0.zip",
        ],
    )
    go_repository(
        name = "org_golang_google_appengine",
        build_file_proto_mode = "disable_global",
        importpath = "google.golang.org/appengine",
        sha256 = "79f80dfac18681788f1414e21a4a7734eff4cdf992070be9163103eb8d9f92cd",
        strip_prefix = "google.golang.org/appengine@v1.6.7",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/google.golang.org/appengine/org_golang_google_appengine-v1.6.7.zip",
        ],
    )
    go_repository(
        name = "org_golang_google_cloud",
        build_file_proto_mode = "disable_global",
        importpath = "google.golang.org/cloud",
        sha256 = "b1d5595a11b88273665d35d4316edbd4545731c979d046c82844fafef2039c2a",
        strip_prefix = "google.golang.org/cloud@v0.0.0-20151119220103-975617b05ea8",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/google.golang.org/cloud/org_golang_google_cloud-v0.0.0-20151119220103-975617b05ea8.zip",
        ],
    )
    go_repository(
        name = "org_golang_google_genproto",
        build_file_proto_mode = "disable_global",
        importpath = "google.golang.org/genproto",
        sha256 = "3c82d071d0f035a28367f01ad0672585d094813560ffdb715142996795c881e2",
        strip_prefix = "google.golang.org/genproto@v0.0.0-20220118154757-00ab72f36ad5",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/google.golang.org/genproto/org_golang_google_genproto-v0.0.0-20220118154757-00ab72f36ad5.zip",
        ],
    )
    go_repository(
        name = "org_golang_google_grpc",
        build_file_proto_mode = "disable_global",
        importpath = "google.golang.org/grpc",
        sha256 = "19fa6e227e62e3ae9791ab81b8a784e93cc68860b7fe0e85dd8d3cfbc1b24398",
        strip_prefix = "google.golang.org/grpc@v1.43.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/google.golang.org/grpc/org_golang_google_grpc-v1.43.0.zip",
        ],
    )
    go_repository(
        name = "org_golang_google_grpc_cmd_protoc_gen_go_grpc",
        build_file_proto_mode = "disable_global",
        importpath = "google.golang.org/grpc/cmd/protoc-gen-go-grpc",
        sha256 = "13877d86cbfa30bde4d62fef2bc58dd56377dcb502c16cf78197f6934193009a",
        strip_prefix = "google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.1.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/google.golang.org/grpc/cmd/protoc-gen-go-grpc/org_golang_google_grpc_cmd_protoc_gen_go_grpc-v1.1.0.zip",
        ],
    )
    go_repository(
        name = "org_golang_google_grpc_examples",
        build_file_proto_mode = "disable_global",
        importpath = "google.golang.org/grpc/examples",
        sha256 = "f5cad7b05a93557c91864a02890a35c6bc5c394897222978cff2b880a78f7a11",
        strip_prefix = "google.golang.org/grpc/examples@v0.0.0-20210324172016-702608ffae4d",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/google.golang.org/grpc/examples/org_golang_google_grpc_examples-v0.0.0-20210324172016-702608ffae4d.zip",
        ],
    )
    go_repository(
        name = "org_golang_google_protobuf",
        build_file_proto_mode = "disable_global",
        importpath = "google.golang.org/protobuf",
        sha256 = "93180890add31a55b56d9c083a397c1dacc3571afa83981d0daa16f0afaf51d0",
        strip_prefix = "google.golang.org/protobuf@v1.27.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/google.golang.org/protobuf/org_golang_google_protobuf-v1.27.1.zip",
        ],
    )
    go_repository(
        name = "org_golang_x_arch",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/arch",
        sha256 = "9f67b677a3fefc503111d9aa7df8bacd2677411b0fcb982eb1654aa6d14cc3f8",
        strip_prefix = "golang.org/x/arch@v0.0.0-20180920145803-b19384d3c130",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/golang.org/x/arch/org_golang_x_arch-v0.0.0-20180920145803-b19384d3c130.zip",
        ],
    )
    go_repository(
        name = "org_golang_x_crypto",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/crypto",
        sha256 = "c8c5cf6be93366723efffb4d1a71dde0da4a77886186c890de1b6821ee30cb3d",
        strip_prefix = "golang.org/x/crypto@v0.0.0-20220112180741-5e0467b6c7ce",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/golang.org/x/crypto/org_golang_x_crypto-v0.0.0-20220112180741-5e0467b6c7ce.zip",
        ],
    )
    go_repository(
        name = "org_golang_x_exp",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/exp",
        sha256 = "50e096afbb8e0f073519dd05f6573aefe410a829c87a7c1b64efb8c4a3948c50",
        strip_prefix = "golang.org/x/exp@v0.0.0-20220104160115-025e73f80486",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/golang.org/x/exp/org_golang_x_exp-v0.0.0-20220104160115-025e73f80486.zip",
        ],
    )
    go_repository(
        name = "org_golang_x_image",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/image",
        sha256 = "4a44b498934a95e8f84e8374530de0cab38d81fcd558898d4880c3c5ce1efe47",
        strip_prefix = "golang.org/x/image@v0.0.0-20190802002840-cff245a6509b",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/golang.org/x/image/org_golang_x_image-v0.0.0-20190802002840-cff245a6509b.zip",
        ],
    )
    go_repository(
        name = "org_golang_x_lint",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/lint",
        sha256 = "0a4a5ebd2b1d79e7f480cbf5a54b45a257ae1ec9d11f01688efc5c35268d4603",
        strip_prefix = "golang.org/x/lint@v0.0.0-20210508222113-6edffad5e616",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/golang.org/x/lint/org_golang_x_lint-v0.0.0-20210508222113-6edffad5e616.zip",
        ],
    )
    go_repository(
        name = "org_golang_x_mobile",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/mobile",
        sha256 = "1fd2f665cdb7f64b80e2e1224941d1ecad10bd99327c6fc8906183d2ef3816df",
        strip_prefix = "golang.org/x/mobile@v0.0.0-20201217150744-e6ae53a27f4f",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/golang.org/x/mobile/org_golang_x_mobile-v0.0.0-20201217150744-e6ae53a27f4f.zip",
        ],
    )
    go_repository(
        name = "org_golang_x_mod",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/mod",
        sha256 = "6e5454f23b4ebc6c18c8db07bc168c71938269deb92c22c9ce4810903680fccb",
        strip_prefix = "golang.org/x/mod@v0.6.0-dev.0.20211013180041-c96bc1413d57",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/golang.org/x/mod/org_golang_x_mod-v0.6.0-dev.0.20211013180041-c96bc1413d57.zip",
        ],
    )
    go_repository(
        name = "org_golang_x_net",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/net",
        sha256 = "75a58bccc53fccc844e501fa7a21daf3230f55dba680df730099918a6e90aa79",
        strip_prefix = "golang.org/x/net@v0.0.0-20220121210141-e204ce36a2ba",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/golang.org/x/net/org_golang_x_net-v0.0.0-20220121210141-e204ce36a2ba.zip",
        ],
    )
    go_repository(
        name = "org_golang_x_oauth2",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/oauth2",
        sha256 = "7317b4f8c045bf2840b67cf2981cee14facc6d38a86b2781b62b32c0600f607c",
        strip_prefix = "golang.org/x/oauth2@v0.0.0-20211104180415-d3ed0bb246c8",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/golang.org/x/oauth2/org_golang_x_oauth2-v0.0.0-20211104180415-d3ed0bb246c8.zip",
        ],
    )
    go_repository(
        name = "org_golang_x_perf",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/perf",
        sha256 = "a2c7d02cc94c4ba767b6322f70ddcba4941cb5f60fed1bada3aa7a4d3a8128f1",
        strip_prefix = "golang.org/x/perf@v0.0.0-20180704124530-6e6d33e29852",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/golang.org/x/perf/org_golang_x_perf-v0.0.0-20180704124530-6e6d33e29852.zip",
        ],
    )
    go_repository(
        name = "org_golang_x_sync",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/sync",
        sha256 = "92a8663af035a6831dcc9cc9d895a1f4bb51fc21efe97f04fa0676fc14992a46",
        strip_prefix = "golang.org/x/sync@v0.0.0-20210220032951-036812b2e83c",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/golang.org/x/sync/org_golang_x_sync-v0.0.0-20210220032951-036812b2e83c.zip",
        ],
    )
    go_repository(
        name = "org_golang_x_sys",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/sys",
        sha256 = "84289ab5dec00ea6f7b911c392b59198d046eabe43185cba45cff79f5b1a9233",
        strip_prefix = "golang.org/x/sys@v0.0.0-20220209214540-3681064d5158",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/golang.org/x/sys/org_golang_x_sys-v0.0.0-20220209214540-3681064d5158.zip",
        ],
    )
    go_repository(
        name = "org_golang_x_term",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/term",
        sha256 = "3adf713afa49fe26580ffe4adb1f4fb2f4921c945301aa5a9fb6d34031fa30cd",
        strip_prefix = "golang.org/x/term@v0.0.0-20210927222741-03fcf44c2211",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/golang.org/x/term/org_golang_x_term-v0.0.0-20210927222741-03fcf44c2211.zip",
        ],
    )
    go_repository(
        name = "org_golang_x_text",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/text",
        sha256 = "e1a9115e61a38da8bdc893d0ba83b65f89cc1114f152a98eb572c5ea6551e8d4",
        strip_prefix = "golang.org/x/text@v0.3.7",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/golang.org/x/text/org_golang_x_text-v0.3.7.zip",
        ],
    )
    go_repository(
        name = "org_golang_x_time",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/time",
        sha256 = "e5d8ade42804ec7d96a632c031dde7db087e8bc4cd5dfd6f38df03ce4f16d9b6",
        strip_prefix = "golang.org/x/time@v0.0.0-20210723032227-1f47c861a9ac",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/golang.org/x/time/org_golang_x_time-v0.0.0-20210723032227-1f47c861a9ac.zip",
        ],
    )
    go_repository(
        name = "org_golang_x_tools",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/tools",
        sha256 = "4ca90457aebec9b546bff39c89669c7a8185b70c7dc24248f5bb577c0041624b",
        strip_prefix = "golang.org/x/tools@v0.1.8-0.20211029000441-d6a9af8af023",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/golang.org/x/tools/org_golang_x_tools-v0.1.8-0.20211029000441-d6a9af8af023.zip",
        ],
    )
    go_repository(
        name = "org_golang_x_xerrors",
        build_file_proto_mode = "disable_global",
        importpath = "golang.org/x/xerrors",
        sha256 = "380aaa3368ae165628bb487cf4e695a55c1420072b8b7a6f59c122f6a23bb255",
        strip_prefix = "golang.org/x/xerrors@v0.0.0-20200804184101-5ec99f83aff1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/golang.org/x/xerrors/org_golang_x_xerrors-v0.0.0-20200804184101-5ec99f83aff1.zip",
        ],
    )
    go_repository(
        name = "org_gonum_v1_gonum",
        build_file_proto_mode = "disable_global",
        importpath = "gonum.org/v1/gonum",
        sha256 = "57ecefd9c1ab5a40ed9e37e824597e523e85e78022cd8a4fc5533ff785f49863",
        strip_prefix = "gonum.org/v1/gonum@v0.8.2",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/gonum.org/v1/gonum/org_gonum_v1_gonum-v0.8.2.zip",
        ],
    )
    go_repository(
        name = "org_gonum_v1_netlib",
        build_file_proto_mode = "disable_global",
        importpath = "gonum.org/v1/netlib",
        sha256 = "ed4dca5026c9ab5410d23bbe21c089433ca58a19bd2902311c6a91791142a687",
        strip_prefix = "gonum.org/v1/netlib@v0.0.0-20190331212654-76723241ea4e",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/gonum.org/v1/netlib/org_gonum_v1_netlib-v0.0.0-20190331212654-76723241ea4e.zip",
        ],
    )
    go_repository(
        name = "org_gonum_v1_plot",
        build_file_proto_mode = "disable_global",
        importpath = "gonum.org/v1/plot",
        sha256 = "2d4cadb4bafb5bbfe1f614d7e402c670446fccd154bc4c6b1699e3dffde68ff4",
        strip_prefix = "gonum.org/v1/plot@v0.0.0-20190515093506-e2840ee46a6b",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/gonum.org/v1/plot/org_gonum_v1_plot-v0.0.0-20190515093506-e2840ee46a6b.zip",
        ],
    )
    go_repository(
        name = "org_modernc_cc",
        build_file_proto_mode = "disable_global",
        importpath = "modernc.org/cc",
        sha256 = "24711e9b28b0d79dd32438eeb7debd86b850350f5f7749b7af640422ecf6b93b",
        strip_prefix = "modernc.org/cc@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/modernc.org/cc/org_modernc_cc-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "org_modernc_golex",
        build_file_proto_mode = "disable_global",
        importpath = "modernc.org/golex",
        sha256 = "335133038991d7feaba5349ac2385db7b49601bba0904abf680803ee2d3c99df",
        strip_prefix = "modernc.org/golex@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/modernc.org/golex/org_modernc_golex-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "org_modernc_mathutil",
        build_file_proto_mode = "disable_global",
        importpath = "modernc.org/mathutil",
        sha256 = "766ad95195543fe1ac217ce9f54e1fb43119c25db2b89013b9ef5477ad2dd9d1",
        strip_prefix = "modernc.org/mathutil@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/modernc.org/mathutil/org_modernc_mathutil-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "org_modernc_strutil",
        build_file_proto_mode = "disable_global",
        importpath = "modernc.org/strutil",
        sha256 = "4bbca362df97450c6f24b90b7dc80b97ecf19e5f0f5954655b26f335a0b8f378",
        strip_prefix = "modernc.org/strutil@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/modernc.org/strutil/org_modernc_strutil-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "org_modernc_xc",
        build_file_proto_mode = "disable_global",
        importpath = "modernc.org/xc",
        sha256 = "ef80e60acacc023cd294eef2555bd348f74c1bcd22c8cfbbd2472cb91e35900d",
        strip_prefix = "modernc.org/xc@v1.0.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/modernc.org/xc/org_modernc_xc-v1.0.0.zip",
        ],
    )
    go_repository(
        name = "org_mongodb_go_mongo_driver",
        build_file_proto_mode = "disable_global",
        importpath = "go.mongodb.org/mongo-driver",
        sha256 = "446cff132e82c64af7ffcf48e268eb16ec81f694914aa6baecb06cbbae1be0d7",
        strip_prefix = "go.mongodb.org/mongo-driver@v1.5.1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/go.mongodb.org/mongo-driver/org_mongodb_go_mongo_driver-v1.5.1.zip",
        ],
    )
    go_repository(
        name = "org_mozilla_go_pkcs7",
        build_file_proto_mode = "disable_global",
        importpath = "go.mozilla.org/pkcs7",
        sha256 = "3c4c1667907ff3127e371d44696326bad9e965216d4257917ae28e8b82a9e08d",
        strip_prefix = "go.mozilla.org/pkcs7@v0.0.0-20200128120323-432b2356ecb1",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/go.mozilla.org/pkcs7/org_mozilla_go_pkcs7-v0.0.0-20200128120323-432b2356ecb1.zip",
        ],
    )
    go_repository(
        name = "org_uber_go_atomic",
        build_file_proto_mode = "disable_global",
        importpath = "go.uber.org/atomic",
        sha256 = "6a1b34a0510c5d0d09f141b3afcd40e6500a2005f5f36578e29b5fc6dc64c24a",
        strip_prefix = "go.uber.org/atomic@v1.9.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/go.uber.org/atomic/org_uber_go_atomic-v1.9.0.zip",
        ],
    )
    go_repository(
        name = "org_uber_go_automaxprocs",
        build_file_proto_mode = "disable_global",
        importpath = "go.uber.org/automaxprocs",
        sha256 = "8f3ac8ce408b75928367ef26bbcb40dc98bbd197e2e9c51129859b2e6073542b",
        strip_prefix = "go.uber.org/automaxprocs@v1.3.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/go.uber.org/automaxprocs/org_uber_go_automaxprocs-v1.3.0.zip",
        ],
    )
    go_repository(
        name = "org_uber_go_goleak",
        build_file_proto_mode = "disable_global",
        importpath = "go.uber.org/goleak",
        sha256 = "cb024defccc4ab71e4175e527e5ca475fbb54c4b2bad91e4292d55431f3606ec",
        strip_prefix = "go.uber.org/goleak@v1.1.10",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/go.uber.org/goleak/org_uber_go_goleak-v1.1.10.zip",
        ],
    )
    go_repository(
        name = "org_uber_go_multierr",
        build_file_proto_mode = "disable_global",
        importpath = "go.uber.org/multierr",
        sha256 = "abee21bbd1cb62b0721680430ef8e098717299d10b4382876b9aa40664e6556c",
        strip_prefix = "go.uber.org/multierr@v1.7.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/go.uber.org/multierr/org_uber_go_multierr-v1.7.0.zip",
        ],
    )
    go_repository(
        name = "org_uber_go_tools",
        build_file_proto_mode = "disable_global",
        importpath = "go.uber.org/tools",
        sha256 = "988dba9c5074080240d33d98e8ce511532f728698db7a9a4ac316c02c94030d6",
        strip_prefix = "go.uber.org/tools@v0.0.0-20190618225709-2cfd321de3ee",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/go.uber.org/tools/org_uber_go_tools-v0.0.0-20190618225709-2cfd321de3ee.zip",
        ],
    )
    go_repository(
        name = "org_uber_go_zap",
        build_file_proto_mode = "disable_global",
        importpath = "go.uber.org/zap",
        sha256 = "6437824258873fed421b7975b8e4cafd1be80cdc15e553beaa887b499dd01420",
        strip_prefix = "go.uber.org/zap@v1.19.0",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/go.uber.org/zap/org_uber_go_zap-v1.19.0.zip",
        ],
    )
    go_repository(
        name = "tools_gotest",
        build_file_proto_mode = "disable_global",
        importpath = "gotest.tools",
        sha256 = "55fab831b2660201183b54d742602563d4e17e7125ee75788a309a4f6cb7285e",
        strip_prefix = "gotest.tools@v2.2.0+incompatible",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/gotest.tools/tools_gotest-v2.2.0+incompatible.zip",
        ],
    )
    go_repository(
        name = "tools_gotest_v3",
        build_file_proto_mode = "disable_global",
        importpath = "gotest.tools/v3",
        sha256 = "9c1e4b8a1477c52441aafc2025a4b4e8bc300a9817c5549c0dc7fffef34bdaef",
        strip_prefix = "gotest.tools/v3@v3.0.3",
        urls = [
            "https://storage.googleapis.com/cockroach-godeps/gomod/gotest.tools/v3/tools_gotest_v3-v3.0.3.zip",
        ],
    )
