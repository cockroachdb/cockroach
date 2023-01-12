# Define the top level namespace. This lets everything be addressable using
# `@com_github_cockroachdb_cockroach//...`.
workspace(
    name = "com_github_cockroachdb_cockroach",
    managed_directories = {
        "@yarn_vendor": ["pkg/ui/yarn-vendor"],
        "@npm_eslint_plugin_crdb": ["pkg/ui/workspaces/eslint-plugin-crdb/node_modules"],
        "@npm_protos": ["pkg/ui/workspaces/db-console/src/js/node_modules"],
        "@npm_cluster_ui": ["pkg/ui/workspaces/cluster_ui/node_modules"],
        "@npm_db_console": ["pkg/ui/workspaces/db-console/node_modules"],
        "@npm_e2e_tests": ["pkg/ui/workspaces/e2e-tests/node_modules"],
    },
)

# Load the things that let us load other things.
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

# Load go bazel tools. This gives us access to the go bazel SDK/toolchains.
http_archive(
    name = "io_bazel_rules_go",
    sha256 = "8a11a59c977f90b09b05cc91901c36fd566683824993c76fa79bc0927e67726f",
    strip_prefix = "cockroachdb-rules_go-c7e85b7",
    urls = [
        # cockroachdb/rules_go as of c7e85b7266f5eb686354c8e6f362ca3baaf47199
        # (upstream release-0.34 plus a few patches).
        "https://storage.googleapis.com/public-bazel-artifacts/bazel/cockroachdb-rules_go-v0.27.0-167-gc7e85b7.tar.gz",
    ],
)

# Like the above, but for nodeJS.
http_archive(
    name = "build_bazel_rules_nodejs",
    sha256 = "7f3f747db3f924547b9ffdf86da6c604335ad95e09d4e5a69fdcfdb505099421",
    strip_prefix = "cockroachdb-rules_nodejs-59a92cc",
    # As of 59a92ccbcd2f5c40cf2368bbb9f7b102491f537b, crl-5.5.0 in our
    # rules_nodejs fork.
    urls = ["https://storage.googleapis.com/public-bazel-artifacts/bazel/cockroachdb-rules_nodejs-5.5.0-1-g59a92cc.tar.gz"],
)

# The rules_nodejs "core" module. We use the same source archive as the non-core
# module above, because otherwise it'll pull from upstream.
http_archive(
    name = "rules_nodejs",
    sha256 = "7f3f747db3f924547b9ffdf86da6c604335ad95e09d4e5a69fdcfdb505099421",
    strip_prefix = "cockroachdb-rules_nodejs-59a92cc",
    urls = ["https://storage.googleapis.com/public-bazel-artifacts/bazel/cockroachdb-rules_nodejs-5.5.0-1-g59a92cc.tar.gz"],
)

# Load gazelle. This lets us auto-generate BUILD.bazel files throughout the
# repo.
http_archive(
    name = "bazel_gazelle",
    sha256 = "5982e5463f171da99e3bdaeff8c0f48283a7a5f396ec5282910b9e8a49c0dd7e",
    urls = [
        "https://storage.googleapis.com/public-bazel-artifacts/bazel/bazel-gazelle-v0.25.0.tar.gz",
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
# helper libraries like rules_go, rules_nodejs, and rules_foreign_cc. However,
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
    sha256 = "079945598e4b6cc075846f7fd6a9d0857c33a7afc0de868c2ccb96405225135d",
    urls = [
        "https://storage.googleapis.com/public-bazel-artifacts/bazel/platforms-0.0.4.tar.gz",
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
# org_golang_x_xerrors handled in DEPS.bzl.

http_archive(
    name = "rules_cc",
    sha256 = "92a89a2bbe6c6db2a8b87da4ce723aff6253656e8417f37e50d362817c39b98b",
    strip_prefix = "rules_cc-88ef31b429631b787ceb5e4556d773b20ad797c8",
    urls = [
        "https://storage.googleapis.com/public-bazel-artifacts/bazel/88ef31b429631b787ceb5e4556d773b20ad797c8.zip",
    ],
)

# org_golang_google_protobuf handled in DEPS.bzl.
# com_github_golang_protobuf handled in DEPS.bzl.
# com_github_mwitkow_go_proto_validators handled in DEPS.bzl.
# com_github_gogo_protobuf handled in DEPS.bzl.
# org_golang_google_genproto handled in DEPS.bzl.

http_archive(
    name = "go_googleapis",
    patch_args = [
        "-E",
        "-p1",
    ],
    patches = [
        "@io_bazel_rules_go//third_party:go_googleapis-deletebuild.patch",
        "@io_bazel_rules_go//third_party:go_googleapis-directives.patch",
        "@io_bazel_rules_go//third_party:go_googleapis-gazelle.patch",
        "@com_github_cockroachdb_cockroach//build/patches:go_googleapis.patch",
    ],
    sha256 = "73831cbb41f2750f3181d126bbabcd3e58b5188e131ecbc309793fa54d5439c9",
    strip_prefix = "googleapis-53377c165584e84c410a0905d9effb3fe5df2806",
    # master, as of 2022-07-19
    # NB: You may have to update this when bumping rules_go. Bumping to the same
    # version in rules_go (go/private/repositories.bzl) is probably what you
    # want to do.
    urls = [
        "https://storage.googleapis.com/public-bazel-artifacts/bazel/53377c165584e84c410a0905d9effb3fe5df2806.zip",
    ],
)

# com_github_golang_mock handled in DEPS.bzl.

# Load the go dependencies and invoke them.
load(
    "@io_bazel_rules_go//go:deps.bzl",
    "go_download_sdk",
    "go_host_sdk",
    "go_local_sdk",
    "go_register_toolchains",
    "go_rules_dependencies",
)

# To point to a mirrored artifact, use:
#
go_download_sdk(
    name = "go_sdk",
    sdks = {
        "darwin_amd64": ("go1.19.1.darwin-amd64.tar.gz", "96a164130f532c0ed65e437aaf9cc66b518f0b887d5830b2dc01ebfee9d58f52"),
        "darwin_arm64": ("go1.19.1.darwin-arm64.tar.gz", "e46aecce83a9289be16ce4ba9b8478a5b89b8aa0230171d5c6adbc0c66640548"),
        "freebsd_amd64": ("go1.19.1.freebsd-amd64.tar.gz", "db5b8f232e12c655cc6cde6af1adf4d27d842541807802d747c86161e89efa0a"),
        "linux_amd64": ("go1.19.1.linux-amd64.tar.gz", "b8c00cd587c49beef8943887d52d77aeda66a30e94effbc1e6d39e1c80f01d37"),
        "linux_arm64": ("go1.19.1.linux-arm64.tar.gz", "49d7c2badb24de8dd75e6c709d4f26d0b5e9509da2fa8c9d79929952b2607c55"),
        "windows_amd64": ("go1.19.1.windows-amd64.tar.gz", "a507d42a457175a50695cf5df8efc64309dec5aa2ebf28d8d28bcd8317d6350c"),
    },
    urls = ["https://storage.googleapis.com/public-bazel-artifacts/go/20220907-175858/{}"],
    version = "1.19.1",
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

go_register_toolchains(nogo = "@com_github_cockroachdb_cockroach//:crdb_nogo")

###############################
# end rules_go dependencies #
###############################

###################################
# begin rules_nodejs dependencies #
###################################

# Install rules_nodejs dependencies

# bazel_skylib handled above.
# rules_nodejs handled above.
load("@build_bazel_rules_nodejs//:repositories.bzl", "build_bazel_rules_nodejs_dependencies")
build_bazel_rules_nodejs_dependencies()

# Configure nodeJS.
load("@build_bazel_rules_nodejs//:index.bzl", "node_repositories", "yarn_install")
load("@build_bazel_rules_nodejs//nodejs:yarn_repositories.bzl", "yarn_repositories")

node_repositories(
    node_repositories = {
        "16.13.0-darwin_arm64": ("node-v16.13.0-darwin-arm64.tar.gz", "node-v16.13.0-darwin-arm64", "46d83fc0bd971db5050ef1b15afc44a6665dee40bd6c1cbaec23e1b40fa49e6d"),
        "16.13.0-darwin_amd64": ("node-v16.13.0-darwin-x64.tar.gz", "node-v16.13.0-darwin-x64", "37e09a8cf2352f340d1204c6154058d81362fef4ec488b0197b2ce36b3f0367a"),
        "16.13.0-linux_arm64": ("node-v16.13.0-linux-arm64.tar.xz", "node-v16.13.0-linux-arm64", "93a0d03f9f802353cb7052bc97a02cd9642b49fa985671cdc16c99936c86d7d2"),
        "16.13.0-linux_amd64": ("node-v16.13.0-linux-x64.tar.xz", "node-v16.13.0-linux-x64", "a876ce787133149abd1696afa54b0b5bc5ce3d5ae359081d407ff776e39b7ba8"),
        "16.13.0-windows_amd64": ("node-v16.13.0-win-x64.zip", "node-v16.13.0-win-x64", "5a39ec5d4786c2814a6c04488bebac6423c2aaa12832b24f0882456f2e4674e1"),
    },
    node_urls = [
        "https://storage.googleapis.com/public-bazel-artifacts/js/node/v{version}/{filename}",
    ],
    node_version = "16.13.0",
)

yarn_repositories(
    name = "yarn",
    yarn_releases = {
        "1.22.11": ("yarn-v1.22.11.tar.gz", "yarn-v1.22.11", "2c320de14a6014f62d29c34fec78fdbb0bc71c9ccba48ed0668de452c1f5fe6c"),
    },
    yarn_urls = [
        "https://storage.googleapis.com/public-bazel-artifacts/js/yarn/v{version}/{filename}",
    ],
    yarn_version = "1.22.11",
)

load("//build/bazelutil:seed_yarn_cache.bzl", "seed_yarn_cache")
seed_yarn_cache(name = "yarn_cache")

# Install external dependencies for NPM packages in pkg/ui/ as separate bazel
# repositories, to avoid version conflicts between those packages.
# Unfortunately Bazel's rules_nodejs does not support yarn workspaces, so
# packages have isolated dependencies and must be installed as isolated
# Bazel repositories.
yarn_install(
    name = "npm_eslint_plugin_crdb",
    args = [
        "--offline",
        "--ignore-optional",
    ],
    data = [
      "//pkg/ui:.yarnrc",
      "@yarn_cache//:.seed",
    ],
    package_json = "//pkg/ui/workspaces/eslint-plugin-crdb:package.json",
    strict_visibility = False,
    yarn_lock = "//pkg/ui/workspaces/eslint-plugin-crdb:yarn.lock",
    symlink_node_modules = True,
)

yarn_install(
    name = "npm_e2e_tests",
    args = [
        "--offline",
    ],
    data = [
      "//pkg/ui:.yarnrc",
      "@yarn_cache//:.seed",
    ],
    package_json = "//pkg/ui/workspaces/e2e-tests:package.json",
    strict_visibility = False,
    yarn_lock = "//pkg/ui/workspaces/e2e-tests:yarn.lock",
    symlink_node_modules = True,
)

yarn_install(
    name = "npm_protos",
    args = [
        "--offline",
        "--ignore-optional",
    ],
    data = [
      "//pkg/ui:.yarnrc",
      "@yarn_cache//:.seed",
    ],
    package_path = "/",
    package_json = "//pkg/ui/workspaces/db-console/src/js:package.json",
    strict_visibility = False,
    yarn_lock = "//pkg/ui/workspaces/db-console/src/js:yarn.lock",
)

yarn_install(
    name = "npm_db_console",
    args = [
        "--offline",
        "--ignore-optional",
    ],
    data = [
      "//pkg/ui:.yarnrc",
      "@yarn_cache//:.seed",
    ],
    package_json = "//pkg/ui/workspaces/db-console:package.json",
    yarn_lock = "//pkg/ui/workspaces/db-console:yarn.lock",
    strict_visibility = False,
    symlink_node_modules = True,
)

yarn_install(
    name = "npm_cluster_ui",
    args = [
        "--verbose",
        "--offline",
        "--ignore-optional",
    ],
    data = [
      "//pkg/ui:.yarnrc",
      "@yarn_cache//:.seed",
    ],
    package_json = "//pkg/ui/workspaces/cluster-ui:package.json",
    strict_visibility = False,
    yarn_lock = "//pkg/ui/workspaces/cluster-ui:yarn.lock",
    symlink_node_modules = True,
)

#################################
# end rules_nodejs dependencies #
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
# co_honnef_go_tools handled in DEPS.bzl.

# keep
go_repository(
    name = "com_github_bazelbuild_buildtools",
    importpath = "github.com/bazelbuild/buildtools",
    sha256 = "a9ef5103739dfb5ed2a5b47ab1654842a89695812e4af09e57d7015a5caf97e0",
    strip_prefix = "buildtools",
    urls = [
        "https://storage.googleapis.com/public-bazel-artifacts/gomod/github.com/bazelbuild/buildtools/v0.0.0-20200718160251-b1667ff58f71/buildtools-v0.0.0-20200718160251-b1667ff58f71.tar.gz",
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

# com_github_burntsushi_toml handled in DEPS.bzl.
# com_github_census_instrumentation_opencensus_proto handled in DEPS.bzl.
# com_github_chzyer_logex handled in DEPS.bzl.
# com_github_chzyer_readline handled in DEPS.bzl.
# com_github_chzyer_test handled in DEPS.bzl.
# com_github_client9_misspell handled in DEPS.bzl.
# com_github_davecgh_go_spew handled in DEPS.bzl.
# com_github_envoyproxy_go_control_plane handled in DEPS.bzl.
# com_github_envoyproxy_protoc_gen_validate handled in DEPS.bzl.
# com_github_fsnotify_fsnotify handled in DEPS.bzl.
# com_github_golang_glog handled in DEPS.bzl.
# com_github_golang_mock handled in DEPS.bzl.
# com_github_golang_protobuf handled in DEPS.bzl.
# com_github_google_go_cmp handled in DEPS.bzl.
# com_github_pelletier_go_toml handled in DEPS.bzl.
# com_github_pmezard_go_difflib handled in DEPS.bzl.
# com_github_prometheus_client_model handled in DEPS.bzl.
# com_github_yuin_goldmark handled in DEPS.bzl.
# com_google_cloud_go handled in DEPS.bzl.
# in_gopkg_check_v1 handled in DEPS.bzl.
# in_gopkg_yaml_v2 handled in DEPS.bzl.

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
# org_golang_google_protobuf handled in DEPS.bzl.
# org_golang_x_crypto handled in DEPS.bzl.
# org_golang_x_exp handled in DEPS.bzl.
# org_golang_x_lint handled in DEPS.bzl.
# org_golang_x_mod handled in DEPS.bzl.
# org_golang_x_net handled in DEPS.bzl.
# org_golang_x_oauth2 handled in DEPS.bzl.
# org_golang_x_sync handled in DEPS.bzl.
# org_golang_x_sys handled in DEPS.bzl.
# org_golang_x_text handled in DEPS.bzl.
# org_golang_x_tools handled in DEPS.bzl.
# org_golang_x_xerrors handled in DEPS.bzl.

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
    sha256 = "88b0a90433866b44bb4450d4c30bc5738b8c4f9c9ba14e9661deb123f56a833d",
    strip_prefix = "rules_proto-b0cc14be5da05168b01db282fe93bdf17aa2b9f4",
    urls = [
        "https://storage.googleapis.com/public-bazel-artifacts/bazel/rules_proto-b0cc14be5da05168b01db282fe93bdf17aa2b9f4.tar.gz",
    ],
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
    sha256 = "272ac2cde4efd316c8d7c0140dee411c89da104466701ac179286ef5a89c7b58",
    strip_prefix = "cockroachdb-rules_foreign_cc-6f7f1b1",
    urls = [
        # As of commit 6f7f1b1c6f911db5706c2fcbb3d5669d95974a34 (release 0.7.0 plus a couple patches)
        "https://storage.googleapis.com/public-bazel-artifacts/bazel/cockroachdb-rules_foreign_cc-6f7f1b1.tar.gz",
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

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
http_archive(
    name = "rules_pkg",
    urls = [
        "https://storage.googleapis.com/public-bazel-artifacts/bazel/rules_pkg-0.7.0.tar.gz",
    ],
    sha256 = "8a298e832762eda1830597d64fe7db58178aa84cd5926d76d5b744d6558941c2",
)
# Ref: https://github.com/bazelbuild/rules_pkg/blob/main/pkg/deps.bzl

# bazel_skylib handled above.
http_archive(
    name = "rules_python",
    urls = ["https://storage.googleapis.com/public-bazel-artifacts/bazel/rules_python-0.1.0.tar.gz"],
    sha256 = "b6d46438523a3ec0f3cead544190ee13223a52f6a6765a29eae7b7cc24cc83a0",
)
http_archive(
    name = "rules_license",
    urls = [
        "https://storage.googleapis.com/public-bazel-artifacts/bazel/rules_license-0.0.1.tar.gz",
    ],
    sha256 = "4865059254da674e3d18ab242e21c17f7e3e8c6b1f1421fffa4c5070f82e98b5",
)
load("@rules_pkg//pkg:deps.bzl", "rules_pkg_dependencies")
rules_pkg_dependencies()

##############################
# end rules_pkg dependencies #
##############################

# Load custom toolchains.
load("//build/toolchains:REPOSITORIES.bzl", "toolchain_dependencies")

toolchain_dependencies()

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
    "//build/toolchains:node_freebsd_toolchain",
)

http_archive(
    name = "bazel_gomock",
    sha256 = "692421b0c5e04ae4bc0bfff42fb1ce8671fe68daee2b8d8ea94657bb1fcddc0a",
    strip_prefix = "bazel_gomock-fde78c91cf1783cc1e33ba278922ba67a6ee2a84",
    urls = [
        "https://storage.googleapis.com/public-bazel-artifacts/bazel/bazel_gomock-fde78c91cf1783cc1e33ba278922ba67a6ee2a84.tar.gz",
    ],
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

load("//build/bazelutil:repositories.bzl", "distdir_repositories")
distdir_repositories()

# This is used only by rules_nodejs to find the local version of node.
new_local_repository(
    name = "nodejs_freebsd_amd64",
    path = "/usr/local",
    build_file_content = """exports_files[("bin/node")]""",
)
