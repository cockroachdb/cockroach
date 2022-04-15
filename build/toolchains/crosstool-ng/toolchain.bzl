def _impl(rctx):
    if rctx.attr.host == "x86_64":
        url = "https://storage.googleapis.com/public-bazel-artifacts/toolchains/crosstool-ng/{}/20220317-191459/{}.tar.gz".format(rctx.attr.host, rctx.attr.target)
    elif rctx.attr.host == "aarch64":
        url = "https://storage.googleapis.com/public-bazel-artifacts/toolchains/crosstool-ng/{}/20220316-165237/{}.tar.gz".format(rctx.attr.host, rctx.attr.target)
    rctx.download_and_extract(
        url = [url],
        sha256 = rctx.attr.tarball_sha256,
        stripPrefix = "x-tools/{}/".format(rctx.attr.target),
    )

    repo_path = str(rctx.path(""))

    rctx.template(
        "BUILD",
        Label("@cockroach//build:toolchains/crosstool-ng/BUILD.tmpl"),
        substitutions = {
            "%{host}": rctx.attr.host,
            "%{target}": rctx.attr.target,
        },
        executable = False,
    )
    rctx.template(
        "cc_toolchain_config.bzl",
        Label("@cockroach//build:toolchains/crosstool-ng/cc_toolchain_config.bzl.tmpl"),
        substitutions = {
            "%{target}": rctx.attr.target,
            "%{host}": rctx.attr.host,
            "%{repo_path}": repo_path,
        },
        executable = False,
    )

crosstool_toolchain_repo = repository_rule(
    implementation = _impl,
    attrs = {
        "host": attr.string(mandatory = True),
        "target": attr.string(mandatory = True),
        "tarball_sha256": attr.string(mandatory = True),
    },
)
