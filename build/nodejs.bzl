load("@rules_nodejs//nodejs:repositories.bzl", "node_repositories")
load("@rules_nodejs//nodejs/private:nodejs_repo_host_os_alias.bzl", "nodejs_repo_host_os_alias")
load("@rules_nodejs//nodejs/private:nodejs_toolchains_repo.bzl", "nodejs_toolchains_repo")
load(":copy_directory.bzl", "copy_directory_platform_repo", "copy_directory_toolchains_repo", "copy_to_directory_platform_repo", "copy_to_directory_toolchains_repo", "COPY_DIRECTORY_VERSIONS", "COPY_TO_DIRECTORY_VERSIONS")
load(":coreutils.bzl", "COREUTILS_PLATFORMS", "coreutils_platform_repo", "coreutils_toolchains_repo", _DEFAULT_COREUTILS_VERSION = "DEFAULT_COREUTILS_VERSION")
load(":tar.bzl", "BSDTAR_PLATFORMS", "bsdtar_binary_repo", "tar_toolchains_repo")
load(":yq.bzl", "YQ_PLATFORMS", "yq_host_alias_repo", "yq_platform_repo", "yq_toolchains_repo", _DEFAULT_YQ_VERSION = "DEFAULT_YQ_VERSION")

_NODE_VERSION = "22.11.0"
_NODE_VERSIONS = {
    "darwin_arm64": ("node-v22.11.0-darwin-arm64.tar.gz", "node-v22.11.0-darwin-arm64", "2e89afe6f4e3aa6c7e21c560d8a0453d84807e97850bbb819b998531a22bdfde"),
    "linux_amd64": ("node-v22.11.0-linux-x64.tar.xz", "node-v22.11.0-linux-x64", "83bf07dd343002a26211cf1fcd46a9d9534219aad42ee02847816940bf610a72"),
    "linux_arm64": ("node-v22.11.0-linux-arm64.tar.xz", "node-v22.11.0-linux-arm64", "6031d04b98f59ff0f7cb98566f65b115ecd893d3b7870821171708cdbaf7ae6e"),
    "linux_ppc64le": ("node-v22.11.0-linux-ppc64le.tar.xz", "node-v22.11.0-linux-ppc64le", "d1d49d7d611b104b6d616e18ac439479d8296aa20e3741432de0e85f4735a81e"),
    "linux_s390x": ("node-v22.11.0-linux-s390x.tar.xz", "node-v22.11.0-linux-s390x", "f474ed77d6b13d66d07589aee1c2b9175be4c1b165483e608ac1674643064a99"),
    "windows_amd64": ("node-v22.11.0-win-x64.zip", "node-v22.11.0-win-x64", "905373a059aecaf7f48c1ce10ffbd5334457ca00f678747f19db5ea7d256c236"),
}

# Helper function used in WORKSPACE.
# Note this function basically re-implements the functionality of the nodejs_register_toolchains function
# in @rules_js//nodejs:repositories.bzl.
# We do this to have more control over how this stuff is created, to use our
# node mirrors, etc.
def declare_nodejs_repos():
    for name in _NODE_VERSIONS:
        node_repositories(
            name = "nodejs_" + name,
            node_repositories = {
                _NODE_VERSION + "-" + name: _NODE_VERSIONS[name]
            },
            node_urls = [
                "https://storage.googleapis.com/public-bazel-artifacts/js/node/v{version}/{filename}",
            ],
            node_version = _NODE_VERSION,
            platform = name,
        )
    nodejs_repo_host_os_alias(name = "nodejs", user_node_repository_name = "nodejs")
    nodejs_repo_host_os_alias(name = "nodejs_host", user_node_repository_name = "nodejs")
    nodejs_toolchains_repo(name = "nodejs_toolchains", user_node_repository_name = "nodejs")
    # NB: npm_import has weird behavior where it transparently makes these
    # copy_directory/copy_to_directory repos for you if you do not set up the
    # repositories beforehand. This is weird behavior that will hopefully be
    # fixed in a later version. For now it's important this function be called
    # before we call into npm_import() in WORKSPACE.
    # Ref: https://github.com/aspect-build/rules_js/blob/a043b6cd1138e608272c2feef8905baf85d86b97/npm/private/npm_import.bzl#L1121
    for plat in COPY_DIRECTORY_VERSIONS:
        copy_directory_platform_repo(
            name = "copy_directory_" + plat,
            platform = plat,
        )
    for plat in COPY_TO_DIRECTORY_VERSIONS:
        copy_to_directory_platform_repo(
            name = "copy_to_directory_" + plat,
            platform = plat,
        )
    copy_directory_toolchains_repo(
        name = "copy_directory_toolchains",
        user_repository_name = "copy_directory",
    )
    copy_to_directory_toolchains_repo(
        name = "copy_to_directory_toolchains",
        user_repository_name = "copy_to_directory",
    )


DEFAULT_TAR_REPOSITORY = "bsd_tar"

def register_tar_toolchains(name = DEFAULT_TAR_REPOSITORY):
    for [platform, _] in BSDTAR_PLATFORMS.items():
        bsdtar_binary_repo(
            name = "%s_%s" % (name, platform),
            platform = platform,
        )

    tar_toolchains_repo(
        name = "%s_toolchains" % name,
        user_repository_name = name,
    )

DEFAULT_YQ_REPOSITORY = "yq"
DEFAULT_YQ_VERSION = _DEFAULT_YQ_VERSION

def register_yq_toolchains(name = DEFAULT_YQ_REPOSITORY, version = DEFAULT_YQ_VERSION):
    for [platform, _] in YQ_PLATFORMS.items():
        yq_platform_repo(
            name = "%s_%s" % (name, platform),
            platform = platform,
            version = version,
        )

    yq_host_alias_repo(name = name)

    yq_toolchains_repo(
        name = "%s_toolchains" % name,
        user_repository_name = name,
    )

DEFAULT_COREUTILS_REPOSITORY = "coreutils"
DEFAULT_COREUTILS_VERSION = _DEFAULT_COREUTILS_VERSION

def register_coreutils_toolchains(name = DEFAULT_COREUTILS_REPOSITORY, version = DEFAULT_COREUTILS_VERSION):
    """Registers coreutils toolchain and repositories

    Args:
        name: override the prefix for the generated toolchain repositories
        version: the version of coreutils to execute (see https://github.com/uutils/coreutils/releases)
        register: whether to call through to native.register_toolchains.
            Should be True for WORKSPACE users, but false when used under bzlmod extension
    """
    for [platform, _] in COREUTILS_PLATFORMS.items():
        coreutils_platform_repo(
            name = "%s_%s" % (name, platform),
            platform = platform,
            version = version,
        )

    coreutils_toolchains_repo(
        name = "%s_toolchains" % name,
        user_repository_name = name,
    )
