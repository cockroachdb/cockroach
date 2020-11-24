# Define the top level namespace. This lets everything be addressable using
# `@cockroach//...`.
workspace(name = "cockroach")

# Load the things that let us load other things.
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

# Load go bazel tools. This gives us access to the go bazel SDK/toolchains.
http_archive(
    name = "io_bazel_rules_go",
    sha256 = "ac03931e56c3b229c145f1a8b2a2ad3e8d8f1af57e43ef28a26123362a1e3c7e",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_go/releases/download/v0.24.4/rules_go-v0.24.4.tar.gz",
        "https://github.com/bazelbuild/rules_go/releases/download/v0.24.4/rules_go-v0.24.4.tar.gz",
    ],
)

# Load the go dependencies and invoke them.
load("@io_bazel_rules_go//go:deps.bzl", "go_rules_dependencies", "go_register_toolchains")
go_rules_dependencies()
go_register_toolchains()

# Load gazelle. This lets us auto-generate BUILD.bazel files throughout the
# repo.
#
# TODO(irfansharif): Point to a proper bazelle-gazelle release once
# https://github.com/bazelbuild/bazel-gazelle/pull/933 lands upstream.
git_repository(
    name = "bazel_gazelle",
    remote = "https://github.com/bazelbuild/bazel-gazelle",
    commit = "493b9adf67665beede36502c2094496af9f245a3",
)

# Load gazelle dependencies.
load("@bazel_gazelle//:deps.bzl", "gazelle_dependencies")
gazelle_dependencies()

# Load the protobuf depedency.
#
# Ref: https://github.com/bazelbuild/rules_go/blob/0.19.0/go/workspace.rst#proto-dependencies
#      https://github.com/bazelbuild/bazel-gazelle/issues/591
#
# TODO(irfansharif): We're not yet using this. We'll eventually need to when we
# try to generate proto files through bazel.
git_repository(
    name = "com_google_protobuf",
    commit = "09745575a923640154bcf307fba8aedff47f240a",
    remote = "https://github.com/protocolbuffers/protobuf",
)
load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")
protobuf_deps()

# Load up cockroachdb's go dependencies (the ones listed under go.mod). The
# `DEPS.bzl` file is kept upto date using the following:
#
#   bazel run //:gazelle -- update-repos -from_file=go.mod -to_macro=DEPS.bzl%go_deps
#
# gazelle:repository_macro DEPS.bzl%go_deps
load("//:DEPS.bzl", "go_deps")
go_deps()


# Loading c-deps third party dependencies.
load("//c-deps:REPOSITORIES.bzl", "c_deps")
c_deps()


# Load the bazel utility that lets us build C/C++ projects using
# cmake/make/etc. We point our fork which adds autoconf support
# (https://github.com/bazelbuild/rules_foreign_cc/pull/432) and BSD support
# (https://github.com/bazelbuild/rules_foreign_cc/pull/387).
#
# TODO(irfansharif): Point to an upstream SHA once maintainers pick up the
# aforementioned PRs.
git_repository(
   name = "rules_foreign_cc",
   commit = "8fdca4480f3fa9c084f4a73749a46fa17996beb1",
   remote = "https://github.com/cockroachdb/rules_foreign_cc",
)
load("@rules_foreign_cc//:workspace_definitions.bzl", "rules_foreign_cc_dependencies")
rules_foreign_cc_dependencies()
