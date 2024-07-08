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
        tarball_sha256 = "4dbdcecc4c072288e3d12346ae08c9f0f79f9a37f7fd7a520c812adfb30e9962",
    )
    _crosstool_toolchain_repo(
        name = "toolchain_cross_s390x-ibm-linux-gnu",
        host = "x86_64",
        target = "s390x-ibm-linux-gnu",
        tarball_sha256 = "18ca3ad7dfaaae5137b0829791cfd5a34b6ba8bbeaf4fbf0e226f08e006d8401",
    )
    _crosstool_toolchain_repo(
        name = "toolchain_cross_x86_64-unknown-linux-gnu",
        host = "x86_64",
        target = "x86_64-unknown-linux-gnu",
        tarball_sha256 = "12901de7c14dc61c856714c4ce2ae594bddb65ff9d09246289164225243e5610",
    )
    _crosstool_toolchain_repo(
        name = "toolchain_cross_x86_64-w64-mingw32",
        host = "x86_64",
        target = "x86_64-w64-mingw32",
        tarball_sha256 = "18ca3ad7dfaaae5137b0829791cfd5a34b6ba8bbeaf4fbf0e226f08e006d8401",
    )
    _crosstool_toolchain_repo(
        name = "armtoolchain_cross_aarch64-unknown-linux-gnu",
        host = "aarch64",
        target = "aarch64-unknown-linux-gnu",
        tarball_sha256 = "f6a40fe8fc9e788a070dc5fdfb637c687c6eb10778122cf4a3f028b5f6d9a77f",
    )
    _crosstool_toolchain_repo(
        name = "armtoolchain_cross_s390x-ibm-linux-gnu",
        host = "aarch64",
        target = "s390x-ibm-linux-gnu",
        tarball_sha256 = "94d7869f4a52caa87641c05d5f3add9e4f86817c26d25c205b678ac1f0a275e1",
    )
    _crosstool_toolchain_repo(
        name = "armtoolchain_cross_x86_64-unknown-linux-gnu",
        host = "aarch64",
        target = "x86_64-unknown-linux-gnu",
        tarball_sha256 = "bcfc3532f036ee7b22f8e06cb8ea675837a7ac615d9f77f6f48b2a1e7d14f3c3",
    )
    _crosstool_toolchain_repo(
        name = "armtoolchain_cross_x86_64-w64-mingw32",
        host = "aarch64",
        target = "x86_64-w64-mingw32",
        tarball_sha256 = "18f8e8297b4d70f7fc8be7531f45d4dad4fdaab174cff498ff8cd9f8a7492005",
    )
    _macos_toolchain_repos()
