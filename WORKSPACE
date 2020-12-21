# Define the top level namespace. This lets everything be addressable using
# `@cockroach//...`.
workspace(name = "cockroach")

# Load the things that let us load other things.
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

# Load go bazel tools. This gives us access to the go bazel SDK/toolchains.
http_archive(
    name = "io_bazel_rules_go",
    sha256 = "81eff5df9077783b18e93d0c7ff990d8ad7a3b8b3ca5b785e1c483aacdb342d7",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_go/releases/download/v0.24.9/rules_go-v0.24.9.tar.gz",
        "https://github.com/bazelbuild/rules_go/releases/download/v0.24.9/rules_go-v0.24.9.tar.gz",
    ],
)

# Load gazelle. This lets us auto-generate BUILD.bazel files throughout the
# repo.
#
# TODO(irfansharif): Point to a proper bazelle-gazelle release once
# https://github.com/bazelbuild/bazel-gazelle/pull/933 lands upstream.
git_repository(
    name = "bazel_gazelle",
    commit = "493b9adf67665beede36502c2094496af9f245a3",
    remote = "https://github.com/bazelbuild/bazel-gazelle",
)

# Override the location of some libraries; otherwise, rules_go will pull its own
# versions. Note these declarations must occur BEFORE the call to
# go_rules_dependencies().
#
# Ref: https://github.com/bazelbuild/rules_go/blob/master/go/dependencies.rst#overriding-dependencies
load("@bazel_gazelle//:deps.bzl", "go_repository")

go_repository(
    name = "com_github_gogo_protobuf",
    build_file_proto_mode = "disable_global",
    importpath = "github.com/gogo/protobuf",
    patch_args = ["-p1"],
    patches = [
        "@cockroach//build/patches:com_github_gogo_protobuf.patch",
    ],
    replace = "github.com/cockroachdb/gogoproto",
    sum = "h1:UoEUWRRZjeOfGenewnmzRCA0kWWRtZogNdidaXjZ/+c=",
    version = "v1.2.1-0.20190102194534-ca10b809dba0",
)

go_repository(
    name = "com_github_golang_protobuf",
    build_file_proto_mode = "disable_global",
    importpath = "github.com/golang/protobuf",
    patch_args = ["-p1"],
    patches = [
        "@cockroach//build/patches:com_github_golang_protobuf.patch",
    ],
    sum = "h1:+Z5KGCizgyZCbGh1KZqA0fcLLkwbsjIzS4aV2v7wJX0=",
    version = "v1.4.2",
)

go_repository(
    name = "org_golang_google_genproto",
    build_file_proto_mode = "disable_global",
    importpath = "google.golang.org/genproto",
    sum = "h1:jB9+PJSvu5tBfmJHy/OVapFdjDF3WvpkqRhxqrmzoEU=",
    version = "v0.0.0-20200218151345-dad8c97a84f5",
)

# Load the go dependencies and invoke them.
load("@io_bazel_rules_go//go:deps.bzl", "go_register_toolchains", "go_rules_dependencies")

go_rules_dependencies()

go_register_toolchains(go_version = "1.15.6")

# Load gazelle dependencies.
load("@bazel_gazelle//:deps.bzl", "gazelle_dependencies")

gazelle_dependencies()

# Load the protobuf depedency.
#
# Ref: https://github.com/bazelbuild/rules_go/blob/0.19.0/go/workspace.rst#proto-dependencies
#      https://github.com/bazelbuild/bazel-gazelle/issues/591
git_repository(
    name = "com_google_protobuf",
    commit = "09745575a923640154bcf307fba8aedff47f240a",
    remote = "https://github.com/protocolbuffers/protobuf",
)

load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")

protobuf_deps()

# Load up cockroachdb's go dependencies (the ones listed under go.mod). The
# `DEPS.bzl` file is kept up to date using the `update-repos` Gazelle command
# (see `make bazel-generate`).
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

# Gazelle silently ignores go_repository re-declarations. See
# https://github.com/bazelbuild/bazel-gazelle/issues/561. What that means for
# us, given we want to run a custom version of goyacc
# (https://github.com/cockroachdb/cockroach/pull/57384, sitting under
# @org_golang_x_tools by default), is that bazel doesn't actually pick it up.
#
# TODO(irfansharif): This is a wart that we can remove once
# https://github.com/golang/tools/pull/259/files is merged upstream. We could
# also generate a .patch file and apply it directly:
# https://github.com/bazelbuild/rules_go/blob/0.19.0/go/workspace.rst#overriding-dependencies
# It is a bit unfortunate though that depending on what the default modules we
# load above end up loading, we're no longer able to override it with our own
# fork. We'll need a longer term solution here.
go_repository(
    name = "com_github_cockroachdb_tools",
    build_file_proto_mode = "disable_global",
    importpath = "golang.org/x/tools",
    replace = "github.com/cockroachdb/tools",
    sum = "h1:TZHlzTz/OL/BaGcWzajnqbmf6Tn+399ylrK/at75zXM=",
    version = "v0.0.0-20201202174556-f18ddc082e74",
)
