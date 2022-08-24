load("@bazel_skylib//lib:paths.bzl", "paths")

# Ref: https://github.com/bazelbuild/bazel/blob/master/distdir.bzl

def _distdir_impl(rctx):
    for url in rctx.attr.files:
        rctx.download(
            url = url,
            output = paths.basename(url),
            sha256 = rctx.attr.files[url],
        )
    rctx.file("WORKSPACE", "")
    rctx.file("BUILD", """
load("@rules_pkg//:pkg.bzl", "pkg_tar")
pkg_tar(
  name="archives",
  srcs = glob(["**"], exclude=["BUILD", "WORKSPACE"]),
  package_dir = "distdir",
  visibility = ["//visibility:public"],
)
""")

distdir = repository_rule(
    implementation = _distdir_impl,
    attrs = {
        "files": attr.string_dict(),
    },
)
