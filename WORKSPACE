# Define the top level namespace. This lets everything be addressable using
# `@cockroach//...`.
workspace(
    name = "cockroach",
    managed_directories = {"@npm": ["node_modules"]},
)

# Load the things that let us load other things.
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

# Load go bazel tools. This gives us access to the go bazel SDK/toolchains.
git_repository(
    name = "io_bazel_rules_go",
    commit = "91c4e2b0f233c7031b191b93587f562ffe21f86f",
    remote = "https://github.com/cockroachdb/rules_go",
)

# Like the above, but for nodeJS.
http_archive(
    name = "build_bazel_rules_nodejs",
    sha256 = "6142e9586162b179fdd570a55e50d1332e7d9c030efd853453438d607569721d",
    urls = ["https://github.com/bazelbuild/rules_nodejs/releases/download/3.0.0/rules_nodejs-3.0.0.tar.gz"],
)

# Load gazelle. This lets us auto-generate BUILD.bazel files throughout the
# repo.
git_repository(
    name = "bazel_gazelle",
    commit = "0ac66c98675a24d58f89a614b84dcd920a7e1762",
    remote = "https://github.com/bazelbuild/bazel-gazelle",
    shallow_since = "1626107853 -0400",
)

# Load up cockroachdb's go dependencies (the ones listed under go.mod). The
# `DEPS.bzl` file is kept up to date using the `update-repos` Gazelle command
# (see `build/bazelutil/bazel-generate.sh`).
#
# gazelle:repository_macro DEPS.bzl%go_deps
load("//:DEPS.bzl", "go_deps")

# VERY IMPORTANT that we call into this function to prefer our pinned versions
# of the dependencies to any that might be pulled in via functions like
# `go_rules_dependencies`, `gazelle_dependencies`, etc.
go_deps()

# Load the go dependencies and invoke them.
load("@io_bazel_rules_go//go:deps.bzl", "go_register_toolchains", "go_rules_dependencies")

go_rules_dependencies()

go_register_toolchains(go_version = "1.16.6")

# Configure nodeJS.
load("@build_bazel_rules_nodejs//:index.bzl", "yarn_install")

yarn_install(
    name = "npm",
    package_json = "//pkg/ui:package.json",
    yarn_lock = "//pkg/ui:yarn.lock",
)

# Load gazelle dependencies.
load("@bazel_gazelle//:deps.bzl", "gazelle_dependencies")

gazelle_dependencies()

# Load the protobuf depedency.
#
# Ref: https://github.com/bazelbuild/rules_go/blob/0.19.0/go/workspace.rst#proto-dependencies
#      https://github.com/bazelbuild/bazel-gazelle/issues/591
git_repository(
    name = "com_google_protobuf",
    commit = "e809d75ecb5770fdc531081eef306b3e672bcdd2",
    remote = "https://github.com/cockroachdb/protobuf",
)

load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")

protobuf_deps()

# Loading c-deps third party dependencies.
load("//c-deps:REPOSITORIES.bzl", "c_deps")

c_deps()

# Load the bazel utility that lets us build C/C++ projects using
# cmake/make/etc. We point to our fork which adds BSD support
# (https://github.com/bazelbuild/rules_foreign_cc/pull/387) and sysroot
# support (https://github.com/bazelbuild/rules_foreign_cc/pull/532).
#
# TODO(irfansharif): Point to an upstream SHA once maintainers pick up the
# aforementioned PRs.
git_repository(
    name = "rules_foreign_cc",
    commit = "67211f9083234f51ef1d9c21a791ee93bc538143",
    remote = "https://github.com/cockroachdb/rules_foreign_cc",
)

load("@rules_foreign_cc//foreign_cc:repositories.bzl", "rules_foreign_cc_dependencies")

rules_foreign_cc_dependencies()

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
        "https://github.com/jmhodges/bazel_gomock/archive/fde78c91cf1783cc1e33ba278922ba67a6ee2a84.tar.gz",
    ],
)
