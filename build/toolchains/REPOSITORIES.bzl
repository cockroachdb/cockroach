load(
    "//build/toolchains:crosstool-ng/toolchain.bzl",
    _crosstool_toolchain_repo = "crosstool_toolchain_repo",
)
load(
    "//build/toolchains:darwin/toolchain.bzl",
    _macos_toolchain_repos = "macos_toolchain_repos",
)

def toolchain_dependencies():
    _crosstool_toolchain_repo(
        name = "toolchain_cross_aarch64-unknown-linux-gnu",
        host = "x86_64",
        target = "aarch64-unknown-linux-gnu",
        tarball_sha256 = "7973a5d75079e86ac949a3723300e150adda3ae97ff012fea6bd5afb81315a55",
    )
    _crosstool_toolchain_repo(
        name = "toolchain_cross_powerpc64le-ibm-linux-gnu",
        host = "x86_64",
        target = "powerpc64le-unknown-linux-gnu",
        tarball_sha256 = "220b97e14d0df73e9903b013757d348ed5eef547f8239c0716e41d082fd064a2",
    )
    _crosstool_toolchain_repo(
        name = "toolchain_cross_s390x-ibm-linux-gnu",
        host = "x86_64",
        target = "s390x-ibm-linux-gnu",
        tarball_sha256 = "1c59273427e06ebacfdb5aa65e5225f85d2d901d8a01310f14b3db4ebf31eccc",
    )
    _crosstool_toolchain_repo(
        name = "toolchain_cross_x86_64-unknown-linux-gnu",
        host = "x86_64",
        target = "x86_64-unknown-linux-gnu",
        tarball_sha256 = "c04be20895d693c051ecfc27de3c4c73441d9b93b25f631a2259b1ba2e0db1f7",
    )
    _crosstool_toolchain_repo(
        name = "toolchain_cross_x86_64-w64-mingw32",
        host = "x86_64",
        target = "x86_64-w64-mingw32",
        tarball_sha256 = "85fda11c5ce03b4a4998f53b2755cebaa174da46077fe91898b413e85858479a",
    )
    _crosstool_toolchain_repo(
        name = "armtoolchain_cross_aarch64-unknown-linux-gnu",
        host = "aarch64",
        target = "aarch64-unknown-linux-gnu",
        tarball_sha256 = "85ca24b636afab22e56765d88b9c597b6d1fd232031207bdf837806010d85390",
    )
    _crosstool_toolchain_repo(
        name = "armtoolchain_cross_powerpc64le-ibm-linux-gnu",
        host = "aarch64",
        target = "powerpc64le-unknown-linux-gnu",
        tarball_sha256 = "b1c3c151837ed538815794a4f7e8c51b80604b73d802ac601e158b882490ed03",
    )
    _crosstool_toolchain_repo(
        name = "armtoolchain_cross_s390x-ibm-linux-gnu",
        host = "aarch64",
        target = "s390x-ibm-linux-gnu",
        tarball_sha256 = "4ee3e9991b7dabb72724d16e018a50187059df849f7dfa646c2f598ff531956c",
    )
    _crosstool_toolchain_repo(
        name = "armtoolchain_cross_x86_64-unknown-linux-gnu",
        host = "aarch64",
        target = "x86_64-unknown-linux-gnu",
        tarball_sha256 = "0bfeb0348ae4a3b2a9014e6f6f6bd15592df137c5e2e3088be5bac40bc703c94",
    )
    _crosstool_toolchain_repo(
        name = "armtoolchain_cross_x86_64-w64-mingw32",
        host = "aarch64",
        target = "x86_64-w64-mingw32",
        tarball_sha256 = "a828e15f9c5b036a1066111c89a2744e3ffb513c8da30a6a23ef2543df60c0d2",
    )
    _macos_toolchain_repos()
