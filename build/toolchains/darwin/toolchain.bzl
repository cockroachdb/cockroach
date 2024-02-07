def _impl(rctx):
    if rctx.attr.host == "arm64":
        url = "https://storage.googleapis.com/public-bazel-artifacts/toolchains/osxcross/aarch64/20220317-160719/x86_64-apple-darwin21.2.tar.gz"
        sha256 = "a0fda00934d9f6f17cdd62ce685d0a12751c34686df14085b72e40d5803e93a6"
    elif rctx.attr.host == "x86_64":
        url = "https://storage.googleapis.com/public-bazel-artifacts/toolchains/osxcross/x86_64/20220317-165434/x86_64-apple-darwin21.2.tar.gz"
        sha256 = "751365dbfb5db66fe8e9f47fcf82cbbd7d1c176b79112ab91945d1be1d160dd5"
    rctx.download_and_extract(
        url = [url],
        sha256 = sha256,
        stripPrefix = "x-tools/x86_64-apple-darwin21.2/",
    )

    rctx.template(
        "BUILD",
        Label("@com_github_cockroachdb_cockroach//build:toolchains/darwin/BUILD.tmpl"),
        substitutions = {
            "%{host}": rctx.attr.host,
            "%{target}": rctx.attr.target,
        },
        executable = False,
    )
    rctx.template(
        "cc_toolchain_config.bzl",
        Label("@com_github_cockroachdb_cockroach//build:toolchains/darwin/cc_toolchain_config.bzl.tmpl"),
        substitutions = {
            "%{host}": rctx.attr.host,
            "%{repo_name}": rctx.name,
            "%{target}": rctx.attr.target,
        },
        executable = False,
    )

macos_toolchain_repo = repository_rule(
    implementation = _impl,
    attrs = {
        "host": attr.string(mandatory = True),
        "target": attr.string(mandatory = True),
    },
)

def macos_toolchain_repos_for_host(host):
    macos_toolchain_repo(
        name = "cross_{}_macos_arm_toolchain".format(host),
        host = host,
        target = 'arm64',
    )
    macos_toolchain_repo(
        name = "cross_{}_macos_toolchain".format(host),
        host = host,
        target = 'x86_64',
    )

def macos_toolchain_repos():
    macos_toolchain_repos_for_host('arm64')
    macos_toolchain_repos_for_host('x86_64')
