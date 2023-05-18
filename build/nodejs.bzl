load("@rules_nodejs//nodejs:repositories.bzl", "node_repositories")
load("@rules_nodejs//nodejs/private:nodejs_repo_host_os_alias.bzl", "nodejs_repo_host_os_alias")
load("@rules_nodejs//nodejs/private:toolchains_repo.bzl", "toolchains_repo")

_NODE_VERSION = "16.14.2"
_VERSIONS = {
    "darwin_amd64": ("node-v16.14.2-darwin-x64.tar.gz", "node-v16.14.2-darwin-x64", "d3076ca7fcc7269c8ff9b03fe7d1c277d913a7e84a46a14eff4af7791ff9d055"),
    "darwin_arm64": ("node-v16.14.2-darwin-arm64.tar.gz", "node-v16.14.2-darwin-arm64", "a66d9217d2003bd416d3dd06dfd2c7a044c4c9ff2e43a27865790bd0d59c682d"),
    "linux_amd64": ("node-v16.14.2-linux-x64.tar.xz", "node-v16.14.2-linux-x64", "e40c6f81bfd078976d85296b5e657be19e06862497741ad82902d0704b34bb1b"),
    "linux_arm64": ("node-v16.14.2-linux-arm64.tar.xz", "node-v16.14.2-linux-arm64", "f7c5a573c06a520d6c2318f6ae204141b8420386553a692fc359f8ae3d88df96"),
    "windows_amd64": ("node-v16.14.2-win-x64.zip", "node-v16.14.2-win-x64", "4731da4fbb2015d414e871fa9118cabb643bdb6dbdc8a69a3ed563266ac93229"),
}

# Helper function used in WORKSPACE.
# Note this function basically re-implements the functionality of the nodejs_register_toolchains function
# in @rules_js//nodejs:repositories.bzl.
# We do this to have more control over how this stuff is created, to use our
# node mirrors, etc.
def declare_nodejs_repos():
    for name in _VERSIONS:
        node_repositories(
            name = "nodejs_" + name,
            node_repositories = {
                _NODE_VERSION + "-" + name: _VERSIONS[name]
            },
            node_urls = [
                "https://storage.googleapis.com/public-bazel-artifacts/js/node/v{version}/{filename}",
            ],
            node_version = _NODE_VERSION,
            platform = name,
        )
    # This is used only by rules_nodejs to find the local version of node.
    native.new_local_repository(
        name = "nodejs_freebsd_amd64",
        build_file_content = """exports_files[("bin/node")]""",
        path = "/usr/local",
    )
    nodejs_repo_host_os_alias(name = "nodejs", user_node_repository_name = "nodejs")
    nodejs_repo_host_os_alias(name = "nodejs_host", user_node_repository_name = "nodejs")
    # TODO: Need to add freebsd support here?
    toolchains_repo(name = "nodejs_toolchains", user_node_repository_name = "nodejs")
