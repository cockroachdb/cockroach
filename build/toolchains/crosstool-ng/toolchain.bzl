def _impl(rctx):
    if rctx.attr.host == "x86_64":
        url = "https://storage.googleapis.com/public-bazel-artifacts/toolchains/crosstool-ng/{}/20230906-034412/{}.tar.gz".format(rctx.attr.host, rctx.attr.target)
    elif rctx.attr.host == "aarch64":
        url = "https://storage.googleapis.com/public-bazel-artifacts/toolchains/crosstool-ng/{}/20220711-204538/{}.tar.gz".format(rctx.attr.host, rctx.attr.target)
    rctx.download_and_extract(
        url = [url],
        sha256 = rctx.attr.tarball_sha256,
        stripPrefix = "x-tools/{}/".format(rctx.attr.target),
    )

    rctx.template(
        "BUILD",
        Label("@com_github_cockroachdb_cockroach//build:toolchains/crosstool-ng/BUILD.tmpl"),
        substitutions = {
            "%{host}": rctx.attr.host,
            "%{target}": rctx.attr.target,
        },
        executable = False,
    )
    rctx.template(
        "cc_toolchain_config.bzl",
        Label("@com_github_cockroachdb_cockroach//build:toolchains/crosstool-ng/cc_toolchain_config.bzl.tmpl"),
        substitutions = {
            "%{target}": rctx.attr.target,
            "%{host}": rctx.attr.host,
            "%{repo_name}": rctx.name,
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
