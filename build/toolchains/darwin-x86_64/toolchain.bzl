def _impl(rctx):
    rctx.download_and_extract(
        url = [
            "https://storage.googleapis.com/public-bazel-artifacts/toolchains/crosstool-ng/20210601-231954/x86_64-apple-darwin19.tar.gz",
        ],
        sha256 = "79ecc64d57f05cc4eccb3e57ce19fe016a3ba24c00fbe2435650f58168df8937",
        stripPrefix = "x-tools/x86_64-apple-darwin19/",
    )

    repo_path = str(rctx.path(""))

    rctx.template("BUILD",
                  Label("@cockroach//build:toolchains/darwin-x86_64/BUILD.tmpl"),
                  executable = False)
    rctx.template("cc_toolchain_config.bzl",
                  Label("@cockroach//build:toolchains/darwin-x86_64/cc_toolchain_config.bzl.tmpl"),
                  substitutions = {
                      "%{repo_path}": repo_path,
                  },
                  executable = False)

macos_toolchain_repo = repository_rule(
    implementation = _impl,
)
