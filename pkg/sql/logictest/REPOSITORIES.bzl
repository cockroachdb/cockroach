load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

# When updating `cockroach_releases.yaml`, update the _PREDECESSOR_VERSION
# and the SHA256 sums in _CONFIGS.

_PREDECESSOR_VERSION = "23.1.9"
CONFIG_LINUX_AMD64 = "linux-amd64"
CONFIG_LINUX_ARM64 = "linux-arm64"
CONFIG_DARWIN_AMD64 = "darwin-10.9-amd64"
CONFIG_DARWIN_ARM64 = "darwin-11.0-arm64"
_CONFIGS = [
    (CONFIG_LINUX_AMD64, "df01d5587839ef77b14e0d2f94f088d9b104ed8ad4ba0ea6a6061bec8079a4e2"),
    (CONFIG_LINUX_ARM64, "7e12d237c5f7f97c9ea03c82cf823cc2cb110997ecdb75f45bf754b25393373a"),
    (CONFIG_DARWIN_AMD64, "4a29775a236de1c7168f949f3b519954c2fa68658c7790aae8f1fae57fae9f91"),
    (CONFIG_DARWIN_ARM64, "7f02619f057f868c11b37616a0b4c58ef43aa7904ae69a478e980422916c3cae"),
]

def _munge_name(s):
    return s.replace("-", "_").replace(".", "_")

def _repo_name(config_name):
    return "cockroach_binary_v{}_{}".format(
        _munge_name(_PREDECESSOR_VERSION),
        _munge_name(config_name))

def _file_name(config_name):
    return "cockroach-v{}.{}/cockroach".format(
        _PREDECESSOR_VERSION, config_name)

def target(config_name):
    return "@{}//:{}".format(_repo_name(config_name),
                             _file_name(config_name))

def cockroach_binaries_for_testing():
    for config in _CONFIGS:
        config_name, shasum = config
        file_name = _file_name(config_name)
        http_archive(
            name = _repo_name(config_name),
            build_file_content = """exports_files(["{}"])""".format(file_name),
            sha256 = shasum,
            urls = [
                "https://binaries.cockroachdb.com/{}".format(
                    file_name.removesuffix("/cockroach")) + ".tgz",
            ],
        )
