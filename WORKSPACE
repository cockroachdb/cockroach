# Define the top level namespace. This lets everything be addressable using
# `@cockroach//...`.
workspace(
    name = "cockroach",
    managed_directories = {
        "@npm": ["pkg/ui/node_modules"],
    },
)

# Load the things that let us load other things.
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

# Load go bazel tools. This gives us access to the go bazel SDK/toolchains.
http_archive(
    name = "io_bazel_rules_go",
    sha256 = "f0af9c876235d2bc69bddb9c33126f78d95f0bc9cec2d5aa497d5cab6325e733",
    strip_prefix = "rules_go-4a7bcc9b9051eb6a12bcb03a9bf38138c4bbc2ea",
    urls = [
        "https://storage.googleapis.com/public-bazel-artifacts/bazel/rules_go-4a7bcc9b9051eb6a12bcb03a9bf38138c4bbc2ea.tar.gz",
    ],
)

# Like the above, but for nodeJS.
http_archive(
    name = "build_bazel_rules_nodejs",
    sha256 = "b32a4713b45095e9e1921a7fcb1adf584bc05959f3336e7351bcf77f015a2d7c",
    urls = [
        "https://storage.googleapis.com/public-bazel-artifacts/bazel/rules_nodejs-4.1.0.tar.gz",
    ],
)

# Load gazelle. This lets us auto-generate BUILD.bazel files throughout the
# repo.
http_archive(
    name = "bazel_gazelle",
    sha256 = "2f3cf903386c2a841522af9ee916f7c33fa27e113033a1ccae8e9dcedb09a99e",
    strip_prefix = "bazel-gazelle-0ac66c98675a24d58f89a614b84dcd920a7e1762",
    urls = [
        "https://storage.googleapis.com/public-bazel-artifacts/bazel/bazel-gazelle-0ac66c98675a24d58f89a614b84dcd920a7e1762.tar.gz",
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

http_archive(
    name = "platforms",
    sha256 = "079945598e4b6cc075846f7fd6a9d0857c33a7afc0de868c2ccb96405225135d",
    urls = [
        "https://storage.googleapis.com/public-bazel-artifacts/bazel/platforms-0.0.4.tar.gz",
    ],
)

http_archive(
    name = "bazel_skylib",
    sha256 = "1c531376ac7e5a180e0237938a2536de0c54d93f5c278634818e0efc952dd56c",
    urls = [
        "https://storage.googleapis.com/public-bazel-artifacts/bazel/bazel-skylib-1.0.3.tar.gz",
    ],
)

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
    ],
    sha256 = "711bc79bd40406dda685a8633f7478979baabaab19eeac664d53f7621866bebc",
    strip_prefix = "googleapis-d4cd8d96ed6eb5dd7c997aab68a1d6bb0825090c",
    # master, as of 2021-03-05
    urls = [
        "https://storage.googleapis.com/public-bazel-artifacts/bazel/d4cd8d96ed6eb5dd7c997aab68a1d6bb0825090c.zip",
    ],
)

# Load the go dependencies and invoke them.
load(
    "@io_bazel_rules_go//go:deps.bzl",
    "go_download_sdk",
    "go_register_toolchains",
    "go_rules_dependencies",
)

go_download_sdk(
    name = "go_sdk",
    urls = ["https://storage.googleapis.com/public-bazel-artifacts/go/{}"],
    version = "1.17.3",
)

go_rules_dependencies()

go_register_toolchains()

###############################
# end rules_go dependencies #
###############################

###################################
# begin rules_nodejs dependencies #
###################################

# Configure nodeJS.
load("@build_bazel_rules_nodejs//:index.bzl", "node_repositories", "yarn_install")

node_repositories(
    node_urls = [
        "https://storage.googleapis.com/public-bazel-artifacts/js/node/v{version}/{filename}",
    ],
    node_version = "14.17.5",
    package_json = ["//pkg/ui:package.json"],
    yarn_urls = [
        "https://storage.googleapis.com/public-bazel-artifacts/js/yarn/v{version}/{filename}",
    ],
    yarn_version = "1.22.11",
)

# install external dependencies for pkg/ui package
yarn_install(
    name = "npm",
    args = [
        "--ignore-optional",
        "--offline",
    ],
    package_json = "//pkg/ui:package.json",
    strict_visibility = False,
    yarn_lock = "//pkg/ui:yarn.lock",
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

# bazel_skylib handled above.

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
    name = "com_github_bmatcuk_doublestar",
    importpath = "github.com/bmatcuk/doublestar",
    sha256 = "50b02a6a30e186ba189c037901719248667b595b3131a4f6b29aebe3c874e83b",
    strip_prefix = "doublestar",
    urls = [
        "https://storage.googleapis.com/public-bazel-artifacts/gomod/github.com/bmatcuk/doublestar/v1.2.2/doublestar-1.2.2.tar.gz",
    ],
)

# com_github_burntsushi_toml handled in DEPS.bzl.
# com_github_davecgh_go_spew handled in DEPS.bzl.
# com_github_fsnotify_fsnotify handled in DEPS.bzl.
# com_github_google_go_cmp handled in DEPS.bzl.
# com_github_kr_pretty handled in DEPS.bzl.
# com_github_kr_pty handled in DEPS.bzl.
# com_github_kr_text handled in DEPS.bzl.
# com_github_pelletier_go_toml handled in DEPS.bzl.
# com_github_pmezard_go_difflib handled in DEPS.bzl.
# in_gopkg_check_v1 handled in DEPS.bzl.
# in_gopkg_yaml_v2 handled in DEPS.bzl.
# org_golang_x_crypto handled in DEPS.bzl.
# org_golang_x_mod handled in DEPS.bzl.
# org_golang_x_net handled in DEPS.bzl.
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
http_archive(
    name = "com_google_protobuf",
    sha256 = "071ccf561d127d5702910340cf038cb869aa239683544e1cca68a78ea865099e",
    strip_prefix = "protobuf-e809d75ecb5770fdc531081eef306b3e672bcdd2",
    urls = [
        "https://storage.googleapis.com/public-bazel-artifacts/bazel/protobuf-e809d75ecb5770fdc531081eef306b3e672bcdd2.tar.gz",
    ],
)

load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")

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
http_archive(
    name = "rules_foreign_cc",
    sha256 = "45d48c71f1b1eadc33a5ad15bbeca8ce42f9ef5ab5d7ee519fc4991e6a9e93d2",
    strip_prefix = "cockroachdb-rules_foreign_cc-f1cff45",
    urls = [
        "https://storage.googleapis.com/public-bazel-artifacts/bazel/cockroachdb-rules_foreign_cc-master20210730-1-gf1cff45.zip",
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

# Load custom toolchains.
load("//build/toolchains:REPOSITORIES.bzl", "toolchain_dependencies")

toolchain_dependencies()

register_toolchains(
    "//build/toolchains:cross_linux_toolchain",
    "//build/toolchains:cross_linux_arm_toolchain",
    "//build/toolchains:cross_macos_toolchain",
    "//build/toolchains:cross_windows_toolchain",
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
