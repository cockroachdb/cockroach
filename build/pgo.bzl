_build_bazel_content = """exports_files(["profile.pprof"])\n"""

def _impl(repository_ctx):
    repository_ctx.download(
        url = repository_ctx.attr.url,
        output = "profile.pprof",
        sha256 = repository_ctx.attr.sha256,
    )
    repository_ctx.file("BUILD.bazel", _build_bazel_content)

pgo_profile = repository_rule(
    implementation = _impl,
    attrs = {
        "url": attr.string(mandatory=True),
        "sha256": attr.string(mandatory=True),
    },
)
