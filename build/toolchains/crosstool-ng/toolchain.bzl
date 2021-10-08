def _impl(rctx):
    rctx.download_and_extract(
        url = [
            "https://storage.googleapis.com/public-bazel-artifacts/toolchains/crosstool-ng/20210601-231954/{}.tar.gz".format(
                rctx.attr.target),
        ],
        sha256 = rctx.attr.tarball_sha256,
        stripPrefix = "x-tools/{}/".format(rctx.attr.target),
    )

    repo_path = str(rctx.path(""))

    rctx.template("BUILD",
                  Label("@cockroach//build:toolchains/crosstool-ng/BUILD.tmpl"),
                  substitutions = {
                      "%{target}": rctx.attr.target,
                  },
                  executable = False)
    rctx.template("cc_toolchain_config.bzl",
                  Label("@cockroach//build:toolchains/crosstool-ng/cc_toolchain_config.bzl.tmpl"),
                  substitutions = {
                      "%{target}": rctx.attr.target,
                      "%{repo_path}": repo_path,
                  },
                  executable = False)

crosstool_toolchain_repo = repository_rule(
    implementation = _impl,
    attrs = {
        "target": attr.string(mandatory = True),
        "tarball_sha256": attr.string(mandatory = True),
    },
)
