def _impl(rctx):
    rctx.download_and_extract(
        url = [
            "https://storage.googleapis.com/public-bazel-artifacts/toolchains/clang/10.0.0/clang%2Bllvm-10.0.0-x86_64-apple-darwin.tar.xz",
        ],
        sha256 = "633a833396bf2276094c126b072d52b59aca6249e7ce8eae14c728016edb5e61",
        stripPrefix = "clang+llvm-10.0.0-x86_64-apple-darwin/",
    )

    # If this doesn't succeed, we won't be able to get the sysroot.
    result = rctx.execute(["/usr/bin/xcodebuild", "-version"])
    if result.return_code:
        fail("XCode appears to not be installed: Got stdout {1}, stderr {2}".format(
            result.stdout, result.stderr))

    # We need the path to the macOS SDK to use as the sysroot.
    result = rctx.execute(["/usr/bin/xcrun", "--sdk", "macosx", "--show-sdk-path"])
    if result.return_code:
        fail("Could not find path to macOS SDK: Got stdout {1}, stderr {2}".format(
            result.stdout, result.stderr))
    repo_path = str(rctx.path(""))
    sdk_path = result.stdout.strip()

    rctx.template("BUILD",
                  Label("@cockroach//build:toolchains/dev/darwin-x86_64/BUILD.darwin-x86_64"),
                  executable = False)
    rctx.template("cc_toolchain_config.bzl",
                  Label("@cockroach//build:toolchains/dev/darwin-x86_64/cc_toolchain_config.bzl.tmpl"),
                  substitutions = {
                      "%{repo_path}": repo_path,
                      "%{sdk_path}": sdk_path,
                  },
                  executable = False)

dev_darwin_x86_repo = repository_rule(
    implementation = _impl,
)
