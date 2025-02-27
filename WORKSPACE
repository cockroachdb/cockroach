# Define the top level namespace. This lets everything be addressable using
# `@com_github_cockroachdb_cockroach//...`.
workspace(name = "com_github_cockroachdb_cockroach")

# Load the things that let us load other things.
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

# Load go bazel tools. This gives us access to the go bazel SDK/toolchains.
http_archive(
    name = "io_bazel_rules_go",
    sha256 = "fc43f42615292c9c3ca6dc211b19ce45986562b0efbca3635e83a70eb011e293",
    strip_prefix = "cockroachdb-rules_go-59db5f5",
    urls = [
        # cockroachdb/rules_go as of 59db5f5a4a65d6671dd62dd4aea42757b09a1fa3
        # (upstream release-0.50 plus a few patches).
        "https://storage.googleapis.com/public-bazel-artifacts/bazel/cockroachdb-rules_go-v0.27.0-530-g59db5f5.tar.gz",
    ],
)

# Like the above, but for JS.
http_archive(
    name = "aspect_rules_js",
    sha256 = "2cfb3875e1231cefd3fada6774f2c0c5a99db0070e0e48ea398acbff7c6c765b",
    strip_prefix = "rules_js-1.42.3",
    url = "https://storage.googleapis.com/public-bazel-artifacts/js/rules_js-v1.42.3.tar.gz",
)

http_archive(
    name = "aspect_rules_ts",
    sha256 = "ace5b609603d9b5b875d56c9c07182357c4ee495030f40dcefb10d443ba8c208",
    strip_prefix = "rules_ts-1.4.0",
    url = "https://storage.googleapis.com/public-bazel-artifacts/js/rules_ts-v1.4.0.tar.gz",
)

# NOTE: aspect_rules_webpack exists for webpack, but it's incompatible with webpack v4.
http_archive(
    name = "aspect_rules_jest",
    sha256 = "d3bb833f74b8ad054e6bff5e41606ff10a62880cc99e4d480f4bdfa70add1ba7",
    strip_prefix = "rules_jest-0.18.4",
    url = "https://storage.googleapis.com/public-bazel-artifacts/js/rules_jest-v0.18.4.tar.gz",
)

# Load gazelle. This lets us auto-generate BUILD.bazel files throughout the
# repo.
http_archive(
    name = "bazel_gazelle",
    sha256 = "b760f7fe75173886007f7c2e616a21241208f3d90e8657dc65d36a771e916b6a",
    urls = [
        "https://storage.googleapis.com/public-bazel-artifacts/bazel/bazel-gazelle-v0.39.1.tar.gz",
    ],
)

# Load up cockroachdb's go dependencies (the ones listed under go.mod). The
# `DEPS.bzl` file is kept up to date using `build/bazelutil/bazel-generate.sh`.
load("//:DEPS.bzl", "go_deps")

# VERY IMPORTANT that we call into this function to prefer our pinned versions
# of the dependencies to any that might be pulled in via functions like
# `go_rules_dependencies`, `gazelle_dependencies`, etc.
# gazelle:repository_macro DEPS.bzl%go_deps
go_deps()

####### THIRD-PARTY DEPENDENCIES #######
# Below we need to call into various helper macros to pull dependencies for
# helper libraries like rules_go, rules_js, and rules_foreign_cc. However,
# calling into those helper macros can cause the build to pull from sources not
# under CRDB's control. To avoid this, we pre-emptively declare each repository
# as an http_archive/go_repository *before* calling into the macro where it
# would otherwise be defined. In doing so we "override" the URL the macro will
# point to.
#
# When upgrading any of these helper libraries, you will have to manually
# inspect the definition of the macro to see what's changed. If the helper
# library has defined any new dependencies, check whether we've already defined
# that repository somewhere (either in this file or in `DEPS.bzl`). If it is
# already defined somewhere, then add a note like "$REPO handled in DEPS.bzl"
# for future maintainers. Otherwise, mirror the .tar.gz and add an http_archive
# pointing to the mirror. For dependencies that were updated, check whether we
# need to pull a new version of that dependency and mirror it and update the URL
# accordingly.

###############################
# begin rules_go dependencies #
###############################

# For those rules_go dependencies that are NOT handled in DEPS.bzl, we point to
# CRDB mirrors.

# Ref: https://github.com/bazelbuild/rules_go/blob/master/go/private/repositories.bzl

http_archive(
    name = "platforms",
    sha256 = "218efe8ee736d26a3572663b374a253c012b716d8af0c07e842e82f238a0a7ee",
    urls = [
        "https://storage.googleapis.com/public-bazel-artifacts/bazel/platforms-0.0.10.tar.gz",
    ],
)

http_archive(
    name = "bazel_skylib",
    sha256 = "4ede85dfaa97c5662c3fb2042a7ac322d5f029fdc7a6b9daa9423b746e8e8fc0",
    strip_prefix = "bazelbuild-bazel-skylib-6a17363",
    urls = [
        "https://storage.googleapis.com/public-bazel-artifacts/bazel/bazelbuild-bazel-skylib-1.3.0-0-g6a17363.tar.gz",
    ],
)

# org_golang_x_sys handled in DEPS.bzl.
# org_golang_x_tools handled in DEPS.bzl.
# org_golang_x_tools_go_vcs handled in DEPS.bzl.
# org_golang_x_xerrors handled in DEPS.bzl.

http_archive(
    name = "rules_cc",
    sha256 = "92a89a2bbe6c6db2a8b87da4ce723aff6253656e8417f37e50d362817c39b98b",
    strip_prefix = "rules_cc-88ef31b429631b787ceb5e4556d773b20ad797c8",
    urls = [
        "https://storage.googleapis.com/public-bazel-artifacts/bazel/88ef31b429631b787ceb5e4556d773b20ad797c8.zip",
    ],
)

# com_github_golang_protobuf handled in DEPS.bzl.
# com_github_mwitkow_go_proto_validators handled in DEPS.bzl.
# com_github_gogo_protobuf handled in DEPS.bzl.
# org_golang_google_genproto handled in DEPS.bzl.
# org_golang_google_grpc_cmd_protoc_gen_go_grpc handled in DEPS.bzl.
# org_golang_google_protobuf handled in DEPS.bzl.

http_archive(
    name = "go_googleapis",
    patch_args = [
        "-E",
        "-p1",
    ],
    patches = [
        "@com_github_cockroachdb_cockroach//build/patches:go_googleapis.patch",
    ],
    sha256 = "ba694861340e792fd31cb77274eacaf6e4ca8bda97707898f41d8bebfd8a4984",
    strip_prefix = "googleapis-83c3605afb5a39952bf0a0809875d41cf2a558ca",
    # master, as of 2022-12-05
    urls = [
        "https://storage.googleapis.com/public-bazel-artifacts/bazel/googleapis-83c3605afb5a39952bf0a0809875d41cf2a558ca.zip",
    ],
)

load("@go_googleapis//:repository_rules.bzl", "switched_rules_by_language")

switched_rules_by_language(
    name = "com_google_googleapis_imports",
)

# com_github_golang_mock handled in DEPS.bzl.

# Load the go dependencies and invoke them.
load(
    "@io_bazel_rules_go//go:deps.bzl",
    "go_download_sdk",
    "go_host_sdk",
    "go_local_sdk",
    "go_register_nogo",
    "go_register_toolchains",
    "go_rules_dependencies",
)

# To point to a mirrored artifact, use:
#
go_download_sdk(
    name = "go_sdk",
    sdks = {
        "darwin_amd64": ("go1.23.6.darwin-amd64.tar.gz", "8d5656e3a674997e27d55fdedb1283fd47d12dee0c040fb2468e6ccfc5229b2b"),
        "darwin_arm64": ("go1.23.6.darwin-arm64.tar.gz", "461e2f9826e947a668331e98cec7e176faae97c65da2fa1a048136fdc5e25b6e"),
        "linux_amd64": ("go1.23.6.linux-amd64.tar.gz", "c1a4f4a6d77f3a8d0605b274019c10f281a26a70019f5e62de3722ec2e27d3f7"),
        "linux_arm64": ("go1.23.6.linux-arm64.tar.gz", "01efad79ef61c8ae3fe2c330f539fb8e30bd4b61c15dc23f4bc6e4fae499aa27"),
        "linux_s390x": ("go1.23.6.linux-s390x.tar.gz", "3a486d1916871b329cf2ca8fe95afe4ebf8c5f655c7fa9dd46fd8ecfdf2499ca"),
        "windows_amd64": ("go1.23.6.windows-amd64.tar.gz", "936455995b7bbc64190c12db3da31a3951775e26d15fd3987333f34c2478087e"),
    },
    urls = ["https://storage.googleapis.com/public-bazel-artifacts/go/20250227-181556/{}"],
    version = "1.23.6",
)

# To point to a local SDK path, use the following instead. We'll call the
# directory into which you cloned the Go repository $GODIR[1]. You'll have to
# first run ./make.bash from $GODIR/src to pick up any custom changes.
#
# [1]: https://go.dev/doc/contribute#testing
#
#   go_local_sdk(
#       name = "go_sdk",
#       path = "<path to $GODIR>",
#   )

# To use your whatever your local SDK is, use the following instead:
#
#   go_host_sdk(name = "go_sdk")

go_rules_dependencies()

go_register_toolchains()
go_register_nogo(nogo = "@com_github_cockroachdb_cockroach//:crdb_nogo")

###############################
# end rules_go dependencies #
###############################

###################################
# begin rules_js dependencies #
###################################

# Install rules_js dependencies

# bazel_skylib handled above.

# The rules_nodejs "core" module.
http_archive(
    name = "rules_nodejs",
    sha256 = "764a3b3757bb8c3c6a02ba3344731a3d71e558220adcb0cf7e43c9bba2c37ba8",
    urls = ["https://storage.googleapis.com/public-bazel-artifacts/js/rules_nodejs-core-5.8.2.tar.gz"],
)

http_archive(
    name = "bazel_features",
    sha256 = "1aabce613b3ed83847b47efa69eb5dc9aa3ae02539309792a60e705ca4ab92a5",
    strip_prefix = "bazel_features-0.2.0",
    url = "https://storage.googleapis.com/public-bazel-artifacts/bazel/bazel_features-v0.2.0.tar.gz",
)

# NOTE: After upgrading this library, run `build/scripts/build-bazel-lib-helpers.sh`.
# The script will print the path to a directory where the binaries are stored,
# a directory with a path of the form `aspect-bazel-lib-utils-20250224-115548`.
# Upload this directory into `gs://public-bazel-artifacts/js`, then update
# the URL and SHA's in `build/nodejs.bzl`.
# Do this AFTER, not BEFORE, upgrading the library.
http_archive(
    name = "aspect_bazel_lib",
    sha256 = "d0529773764ac61184eb3ad3c687fb835df5bee01afedf07f0cf1a45515c96bc",
    strip_prefix = "bazel-lib-1.42.3",
    url = "https://storage.googleapis.com/public-bazel-artifacts/bazel/bazel-lib-v1.42.3.tar.gz",
)

# Load custom toolchains.
load("//build/toolchains:REPOSITORIES.bzl", "toolchain_dependencies")

toolchain_dependencies()

# Configure nodeJS.
load("//build:nodejs.bzl", "declare_nodejs_repos")

declare_nodejs_repos()

# NOTE: The version is expected to match up to what version of typescript we
# use for all packages in pkg/ui.
# TODO(ricky): We should add a lint check to ensure it does match.
load("@aspect_rules_ts//ts/private:npm_repositories.bzl", ts_http_archive = "http_archive_version")

ts_http_archive(
    name = "npm_typescript",
    build_file = "@aspect_rules_ts//ts:BUILD.typescript",
    # v5.1.6 isn't known to rules_ts 1.4.0 (nor to any published rules_ts version as-of 7 Aug 2023).
    integrity = "sha512-zaWCozRZ6DLEWAWFrVDz1H6FVXzUSfTy5FUMWsQlU8Ym5JP9eO4xkTIROFCQvhQf61z6O/G6ugw3SgAnvvm+HA==",
    urls = ["https://storage.googleapis.com/cockroach-npm-deps/typescript/-/typescript-{}.tgz"],
    version = "5.1.6",
)

# NOTE: The version is expected to match up to what version we use in db-console.
# TODO(ricky): We should add a lint check to ensure it does match.
load("@aspect_rules_js//npm:repositories.bzl", "npm_import", "npm_translate_lock")

npm_import(
    name = "pnpm",
    # Declare an @pnpm//:pnpm rule that can be called externally.
    # Copied from https://github.com/aspect-build/rules_js/blob/14724d9b27b2c45f088aa003c091cbe628108170/npm/private/pnpm_repository.bzl#L27-L30
    extra_build_content = "\n".join([
        """load("@aspect_rules_js//js:defs.bzl", "js_binary")""",
        """js_binary(name = "pnpm", entry_point = "package/dist/pnpm.cjs", visibility = ["//visibility:public"])""",
    ]),
    integrity = "sha512-W6elL7Nww0a/MCICkzpkbxW6f99TQuX4DuJoDjWp39X08PKDkEpg4cgj3d6EtgYADcdQWl/eM8NdlLJVE3RgpA==",
    package = "pnpm",
    url = "https://storage.googleapis.com/cockroach-npm-deps/pnpm/-/pnpm-8.5.1.tgz",
    version = "8.5.1",
)

load("@aspect_rules_js//js:repositories.bzl", "rules_js_dependencies")

rules_js_dependencies()

npm_translate_lock(
    name = "npm",
    data = [
        "//pkg/ui:package.json",
        "//pkg/ui:pnpm-workspace.yaml",
        "//pkg/ui/patches:topojson@3.0.2.patch",
        "//pkg/ui/workspaces/cluster-ui:package.json",
        "//pkg/ui/workspaces/crdb-api-client:package.json",
        "//pkg/ui/workspaces/db-console:package.json",
        "//pkg/ui/workspaces/db-console/src/js:package.json",
        "//pkg/ui/workspaces/e2e-tests:package.json",
        "//pkg/ui/workspaces/eslint-plugin-crdb:package.json",
    ],
    npmrc = "//pkg/ui:.npmrc.bazel",
    patch_args = {
        "*": ["-p1"],
    },
    pnpm_lock = "//pkg/ui:pnpm-lock.yaml",
    # public_hoist_packages should contain the same packages defined in .npmrc file > public-hoist-pattern.
    public_hoist_packages = {
        # `antd` components inherit prop types from rc-* components which types aren't hoisted to be
        # publicly accessible but it still needed to properly resolve types by Typescript.
        "rc-table": ["pkg/ui/workspaces/cluster-ui"],
    },
    verify_node_modules_ignored = "//:.bazelignore",
)

load("@npm//:repositories.bzl", "npm_repositories")

npm_repositories()

#################################
# end rules_js dependencies #
#################################

##############################
# begin gazelle dependencies #
##############################

# Load gazelle dependencies.
load(
    "@bazel_gazelle//:deps.bzl",
    "gazelle_dependencies",
    "go_repository",
)

# Ref: https://github.com/bazelbuild/bazel-gazelle/blob/master/deps.bzl

# bazel_skylib handled above.

http_archive(
    name = "rules_license",
    urls = [
        "https://storage.googleapis.com/public-bazel-artifacts/bazel/rules_license-1.0.0.tar.gz",
    ],
    sha256 = "26d4021f6898e23b82ef953078389dd49ac2b5618ac564ade4ef87cced147b38",
)

# keep
go_repository(
    name = "com_github_bazelbuild_buildtools",
    importpath = "github.com/bazelbuild/buildtools",
    sha256 = "7929c8fc174f8ab03361796f1417eb0eb5ae4b2a12707238694bec2954145ce4",
    strip_prefix = "bazelbuild-buildtools-b163fcf",
    urls = [
        "https://storage.googleapis.com/public-bazel-artifacts/bazel/bazelbuild-buildtools-v6.3.3-0-gb163fcf.tar.gz",
    ],
)

# com_github_bazelbuild_rules_go handled in DEPS.bzl.

# keep
go_repository(
    name = "com_github_bmatcuk_doublestar_v4",
    importpath = "github.com/bmatcuk/doublestar/v4",
    sha256 = "d11c3b3a45574f89d6a6b2f50e53feea50df60407b35f36193bf5815d32c79d1",
    strip_prefix = "bmatcuk-doublestar-f7a8118",
    urls = [
        "https://storage.googleapis.com/public-bazel-artifacts/bazel/bmatcuk-doublestar-v4.0.1-0-gf7a8118.tar.gz",
    ],
)

# com_github_fsnotify_fsnotify handled in DEPS.bzl.
# com_github_gogo_protobuf handled in DEPS.bzl.
# com_github_golang_mock handled in DEPS.bzl.
# com_github_golang_protobuf handled in DEPS.bzl.
# com_github_google_go_cmp handled in DEPS.bzl.
# com_github_pelletier_go_toml handled in DEPS.bzl.
# com_github_pmezard_go_difflib handled in DEPS.bzl.

# keep
go_repository(
    name = "net_starlark_go",
    importpath = "go.starlark.net",
    sha256 = "a35c6468e0e0921833a63290161ff903295eaaf5915200bbce272cbc8dfd1c1c",
    strip_prefix = "google-starlark-go-e043a3d",
    urls = [
        "https://storage.googleapis.com/public-bazel-artifacts/bazel/google-starlark-go-e043a3d.tar.gz",
    ],
)

# org_golang_google_genproto handled in DEPS.bzl.
# org_golang_google_grpc handled in DEPS.bzl.
# org_golang_google_grpc_cmd_protoc_gen_go_grpc handled in DEPS.bzl.
# org_golang_google_protobuf handled in DEPS.bzl.
# org_golang_x_mod handled in DEPS.bzl.
# org_golang_x_net handled in DEPS.bzl.
# org_golang_x_sync handled in DEPS.bzl.
# org_golang_x_sys handled in DEPS.bzl.
# org_golang_x_text handled in DEPS.bzl.
# org_golang_x_tools handled in DEPS.bzl.
# org_golang_x_tools_go_vcs handled in DEPS.bzl.

gazelle_dependencies()

############################
# end gazelle dependencies #
############################

###############################
# begin protobuf dependencies #
###############################

# Load the protobuf dependency.
#
# Ref: https://github.com/bazelbuild/rules_go/blob/0.19.0/go/workspace.rst#proto-dependencies
#      https://github.com/bazelbuild/bazel-gazelle/issues/591
#      https://github.com/protocolbuffers/protobuf/blob/main/protobuf_deps.bzl
http_archive(
    name = "com_google_protobuf",
    sha256 = "6d4e7fe1cbd958dee69ce9becbf8892d567f082b6782d3973a118d0aa00807a8",
    strip_prefix = "cockroachdb-protobuf-3f5d91f",
    urls = [
        # Code as of 3f5d91f2e169d890164d3401b8f4a9453fff5538 (crl-release-3.9, 3.9.2 plus a few patches).
        "https://storage.googleapis.com/public-bazel-artifacts/bazel/cockroachdb-protobuf-3f5d91f.tar.gz",
    ],
)

http_archive(
    name = "zlib",
    build_file = "@com_google_protobuf//:third_party/zlib.BUILD",
    sha256 = "c3e5e9fdd5004dcb542feda5ee4f0ff0744628baf8ed2dd5d66f8ca1197cb1a1",
    strip_prefix = "zlib-1.2.11",
    urls = [
        "https://storage.googleapis.com/public-bazel-artifacts/zlib/zlib-1.2.11.tar.gz",
    ],
)

# NB: we don't use six for anything. We're just including it here so we don't
# incidentally pull it from pypi.
http_archive(
    name = "six",
    build_file = "@com_google_protobuf//:six.BUILD",
    sha256 = "105f8d68616f8248e24bf0e9372ef04d3cc10104f1980f54d57b2ce73a5ad56a",
    urls = [
        "https://storage.googleapis.com/public-bazel-artifacts/python/six-1.10.0.tar.gz",
    ],
)

# rules_cc handled above.

# NB: we don't use rules_java for anything. We're just including it here so we
# don't incidentally pull it from github.
http_archive(
    name = "rules_java",
    sha256 = "f5a3e477e579231fca27bf202bb0e8fbe4fc6339d63b38ccb87c2760b533d1c3",
    strip_prefix = "rules_java-981f06c3d2bd10225e85209904090eb7b5fb26bd",
    urls = [
        "https://storage.googleapis.com/public-bazel-artifacts/bazel/rules_java-981f06c3d2bd10225e85209904090eb7b5fb26bd.tar.gz",
    ],
)

http_archive(
    name = "rules_proto",
    sha256 = "6fb6767d1bef535310547e03247f7518b03487740c11b6c6adb7952033fe1295",
    strip_prefix = "rules_proto-6.0.2",
    url = "https://storage.googleapis.com/public-bazel-artifacts/bazel/rules_proto-6.0.2.tar.gz",
)

load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")

protobuf_deps()

#############################
# end protobuf dependencies #
#############################

# Loading c-deps third party dependencies.
load("//c-deps:REPOSITORIES.bzl", "c_deps")

c_deps()

#######################################
# begin rules_foreign_cc dependencies #
#######################################

# Load the bazel utility that lets us build C/C++ projects using
# cmake/make/etc. We point to our fork which adds BSD support
# (https://github.com/bazelbuild/rules_foreign_cc/pull/387) and sysroot
# support (https://github.com/bazelbuild/rules_foreign_cc/pull/532).
#
# TODO(irfansharif): Point to an upstream SHA once maintainers pick up the
# aforementioned PRs.
#
# Ref: https://github.com/bazelbuild/rules_foreign_cc/blob/main/foreign_cc/repositories.bzl
http_archive(
    name = "rules_foreign_cc",
    sha256 = "03afebfc3f173666a3820a29512265c710c3a08d0082ba77469779d3e3af5a11",
    strip_prefix = "cockroachdb-rules_foreign_cc-8d34d77",
    urls = [
        # As of commit 8d34d777736b2d895e4e4fbb755deb424ae1f6c7 (release 0.7.0 plus a couple patches)
        "https://storage.googleapis.com/public-bazel-artifacts/bazel/cockroachdb-rules_foreign_cc-8d34d77.tar.gz",
    ],
)

load("@rules_foreign_cc//foreign_cc:repositories.bzl", "rules_foreign_cc_dependencies")

# bazel_skylib is handled above.

rules_foreign_cc_dependencies(
    register_built_tools = False,
    register_default_tools = False,
    register_preinstalled_tools = True,
)

#####################################
# end rules_foreign_cc dependencies #
#####################################

################################
# begin rules_pkg dependencies #
################################

http_archive(
    name = "rules_pkg",
    sha256 = "8a298e832762eda1830597d64fe7db58178aa84cd5926d76d5b744d6558941c2",
    urls = [
        "https://storage.googleapis.com/public-bazel-artifacts/bazel/rules_pkg-0.7.0.tar.gz",
    ],
)
# Ref: https://github.com/bazelbuild/rules_pkg/blob/main/pkg/deps.bzl

# bazel_skylib handled above.
http_archive(
    name = "rules_python",
    sha256 = "b6d46438523a3ec0f3cead544190ee13223a52f6a6765a29eae7b7cc24cc83a0",
    urls = ["https://storage.googleapis.com/public-bazel-artifacts/bazel/rules_python-0.1.0.tar.gz"],
)

# rules_license handled above.

load("@rules_pkg//pkg:deps.bzl", "rules_pkg_dependencies")

rules_pkg_dependencies()

##############################
# end rules_pkg dependencies #
##############################

################################
# begin rules_oci dependencies #
################################

http_archive(
    name = "rules_oci",
    sha256 = "21a7d14f6ddfcb8ca7c5fc9ffa667c937ce4622c7d2b3e17aea1ffbc90c96bed",
    strip_prefix = "rules_oci-1.4.0",
    url = "https://storage.googleapis.com/public-bazel-artifacts/bazel/rules_oci-v1.4.0.tar.gz",
)

# bazel_skylib handled above.
# aspect_bazel_lib handled above.

load("@rules_oci//oci:dependencies.bzl", "rules_oci_dependencies")

rules_oci_dependencies()

# TODO: This will pull from an upstream location: specifically it will download
# `crane` from https://github.com/google/go-containerregistry/... Before this is
# used in CI or anything production-ready, this should be mirrored. rules_oci
# doesn't support this mirroring yet so we'd have to submit a patch.
load("@rules_oci//oci:repositories.bzl", "LATEST_CRANE_VERSION", "oci_register_toolchains")

oci_register_toolchains(
    name = "oci",
    crane_version = LATEST_CRANE_VERSION,
)

##############################
# end rules_oci dependencies #
##############################

register_toolchains(
    "//build/toolchains:cross_x86_64_linux_toolchain",
    "//build/toolchains:cross_x86_64_linux_arm_toolchain",
    "//build/toolchains:cross_x86_64_s390x_toolchain",
    "//build/toolchains:cross_x86_64_macos_toolchain",
    "//build/toolchains:cross_x86_64_macos_arm_toolchain",
    "//build/toolchains:cross_x86_64_windows_toolchain",
    "//build/toolchains:cross_arm64_linux_toolchain",
    "//build/toolchains:cross_arm64_linux_arm_toolchain",
    "//build/toolchains:cross_arm64_s390x_toolchain",
    "//build/toolchains:cross_arm64_windows_toolchain",
    "//build/toolchains:cross_arm64_macos_toolchain",
    "//build/toolchains:cross_arm64_macos_arm_toolchain",
    "@copy_directory_toolchains//:darwin_amd64_toolchain",
    "@copy_directory_toolchains//:darwin_arm64_toolchain",
    "@copy_directory_toolchains//:linux_amd64_toolchain",
    "@copy_directory_toolchains//:linux_arm64_toolchain",
    "@copy_directory_toolchains//:windows_amd64_toolchain",
    "@copy_to_directory_toolchains//:darwin_amd64_toolchain",
    "@copy_to_directory_toolchains//:darwin_arm64_toolchain",
    "@copy_to_directory_toolchains//:linux_amd64_toolchain",
    "@copy_to_directory_toolchains//:linux_arm64_toolchain",
    "@copy_to_directory_toolchains//:windows_amd64_toolchain",
    "@nodejs_toolchains//:darwin_amd64_toolchain",
    "@nodejs_toolchains//:darwin_arm64_toolchain",
    "@nodejs_toolchains//:linux_amd64_toolchain",
    "@nodejs_toolchains//:linux_arm64_toolchain",
    "@nodejs_toolchains//:windows_amd64_toolchain",
)

http_archive(
    name = "com_github_cockroachdb_sqllogictest",
    build_file_content = """
filegroup(
    name = "testfiles",
    srcs = glob(["test/**/*.test"]),
    visibility = ["//visibility:public"],
)""",
    sha256 = "f7e0d659fbefb65f32d4c5d146cba4c73c43e0e96f9b217a756c82be17451f97",
    strip_prefix = "sqllogictest-96138842571462ed9a697bff590828d8f6356a2f",
    urls = [
        "https://storage.googleapis.com/public-bazel-artifacts/bazel/sqllogictest-96138842571462ed9a697bff590828d8f6356a2f.tar.gz",
    ],
)

http_archive(
    name = "railroadjar",
    build_file_content = """exports_files(["rr.war"])""",
    sha256 = "d2791cd7a44ea5be862f33f5a9b3d40aaad9858455828ebade7007ad7113fb41",
    urls = [
        "https://storage.googleapis.com/public-bazel-artifacts/java/railroad/rr-1.63-java8.zip",
    ],
)

# Cockroach binaries for use by mixed-version logictests.
load("//pkg/sql/logictest:REPOSITORIES.bzl", "cockroach_binaries_for_testing")

cockroach_binaries_for_testing()

load("//build/bazelutil:repositories.bzl", "distdir_repositories")

distdir_repositories()

load("//build:pgo.bzl", "pgo_profile")

pgo_profile(
    name = "pgo_profile",
    url = "https://storage.googleapis.com/cockroach-profiles/20250109191252.pprof",
    sha256 = "d9e541652962eaddc7dbc1fabfe9695296362d1fdfca261658f754a9383742bf",
)

# Download and register the FIPS enabled Go toolchain at the end to avoid toolchain conflicts for gazelle.
go_download_sdk(
    name = "go_sdk_fips",
    # In the golang-fips toolchain, FIPS-ready crypto packages are used by default, regardless of build tags.
    # The boringcrypto experiment does almost nothing in this toolchain, but it does enable the use of the
    # crypto/boring.Enabled() method which is the only application-visible way to inspect whether FIPS mode
    # is working correctly.
    #
    # The golang-fips toolchain also supports an experiment `strictfipsruntime` which causes a panic at startup
    # if the kernel is in FIPS mode but OpenSSL cannot be loaded. We do not currently use this experiment
    # because A) we also want to detect the case when the kernel is not in FIPS mode and B) we want to be
    # able to provide additional diagnostic information such as the expected version of OpenSSL.
    experiments = ["boringcrypto"],
    sdks = {
        "linux_amd64": ("go1.23.6fips.linux-amd64.tar.gz", "fb7e0eaa3ee0dbdb94de61ca7d761f6f436719ec0a63f5850dc14de880ef0f37"),
    },
    urls = ["https://storage.googleapis.com/public-bazel-artifacts/go/20250227-181556/{}"],
    version = "1.23.6fips",
)
