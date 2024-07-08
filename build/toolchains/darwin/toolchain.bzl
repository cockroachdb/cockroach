def _impl(rctx):
    if rctx.attr.host == "arm64":
        url = "https://storage.googleapis.com/public-bazel-artifacts/toolchains/osxcross/aarch64/20240711-185211/x86_64-apple-darwin21.2.tar.gz"
        sha256 = "e33b81567df830a6b488ed3ad35b5c212fa113cfd1c73d61ffc04f1c7070e827"
    elif rctx.attr.host == "x86_64":
        url = "https://storage.googleapis.com/public-bazel-artifacts/toolchains/osxcross/x86_64/20240711-185136/x86_64-apple-darwin21.2.tar.gz"
        sha256 = "64068ed6aab8d7344a79981b2444f8602b082deb7a9c3cd9a554b78100d2d7b4"
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
