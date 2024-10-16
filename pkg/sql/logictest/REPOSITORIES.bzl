# DO NOT EDIT THIS FILE MANUALLY! Use `release update-releases-file`.
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

CONFIG_LINUX_AMD64 = "linux-amd64"
CONFIG_LINUX_ARM64 = "linux-arm64"
CONFIG_DARWIN_AMD64 = "darwin-10.9-amd64"
CONFIG_DARWIN_ARM64 = "darwin-11.0-arm64"

_CONFIGS = [
    ("24.1.5", [
        (CONFIG_DARWIN_AMD64, "b6aba8395510ac2506c6cb82e2661d6d3476ff7c132016fdc823b165cbea3549"),
        (CONFIG_DARWIN_ARM64, "7b2cc8e3a53945d97bc5afd4b7457ff4962633bae9b71945ffd6e2659fa2bf5a"),
        (CONFIG_LINUX_AMD64, "731f9ade47b19119136049816edd12167423cb993ee19349fa6ce51157b9fbfc"),
        (CONFIG_LINUX_ARM64, "7ed4d67c60f1b54ed522fbdecfb4907904be6e043df6e6596bfb2894e7d82f87"),
    ]),
    ("24.2.3", [
        (CONFIG_DARWIN_AMD64, "f3d59ed7367c8b4d8420bd1cae9f50a58114d18945ef984805403d44943447d0"),
        (CONFIG_DARWIN_ARM64, "5e70e89ef21217a80a532499f5b07618269f1ad1399732d4a55c09a71554f048"),
        (CONFIG_LINUX_AMD64, "637d0ada1db52e57f5cbbe19a7defcff0d538d43b771ae8da7ceba326686d64c"),
        (CONFIG_LINUX_ARM64, "2892c8d34e89909b871baf9c1b147c827f3b3b78285602aac33789f79fdfa210"),
    ]),
]

def _munge_name(s):
    return s.replace("-", "_").replace(".", "_")

def _repo_name(version, config_name):
    return "cockroach_binary_v{}_{}".format(
        _munge_name(version),
        _munge_name(config_name))

def _file_name(version, config_name):
    return "cockroach-v{}.{}/cockroach".format(
        version, config_name)

def target(config_name):
    targets = []
    for versionAndConfigs in _CONFIGS:
        version, _ = versionAndConfigs
        targets.append("@{}//:{}".format(_repo_name(version, config_name),
                                         _file_name(version, config_name)))
    return targets

def cockroach_binaries_for_testing():
    for versionAndConfigs in _CONFIGS:
        version, configs = versionAndConfigs
        for config in configs:
            config_name, shasum = config
            file_name = _file_name(version, config_name)
            http_archive(
                name = _repo_name(version, config_name),
                build_file_content = """exports_files(["{}"])""".format(file_name),
                sha256 = shasum,
                urls = [
                    "https://binaries.cockroachdb.com/{}".format(
                        file_name.removesuffix("/cockroach")) + ".tgz",
                ],
            )
