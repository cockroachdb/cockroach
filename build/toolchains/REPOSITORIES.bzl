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
        tarball_sha256 = "b2b1d06ae9e85e37f49f542991b335e46a47ac94f8025b771a05a65b9ca62f3a",
    )
    _crosstool_toolchain_repo(
        name = "toolchain_cross_powerpc64le-ibm-linux-gnu",
        host = "x86_64",
        target = "powerpc64le-unknown-linux-gnu",
        tarball_sha256 = "1ae3401fee8cb49ae242f620d6a53e1bbad15e3069332f60c94f29865a6c8b71",
    )
    _crosstool_toolchain_repo(
        name = "toolchain_cross_s390x-ibm-linux-gnu",
        host = "x86_64",
        target = "s390x-ibm-linux-gnu",
        tarball_sha256 = "f41e748fdae269749274edc102af9153c1023bf5528cea82b3732a0d19d8fe8b",
    )
    _crosstool_toolchain_repo(
        name = "toolchain_cross_x86_64-unknown-linux-gnu",
        host = "x86_64",
        target = "x86_64-unknown-linux-gnu",
        tarball_sha256 = "1986c7cbb33c21257854d3c578e01c0db9056ef3c7d67ef6d9ea65c7cd58d266",
    )
    _crosstool_toolchain_repo(
        name = "toolchain_cross_x86_64-w64-mingw32",
        host = "x86_64",
        target = "x86_64-w64-mingw32",
        tarball_sha256 = "b95222934197b586799e83c80961396350f6c0a2958a3e735f50a32f031e819b",
    )
    _crosstool_toolchain_repo(
        name = "armtoolchain_cross_aarch64-unknown-linux-gnu",
        host = "aarch64",
        target = "aarch64-unknown-linux-gnu",
        tarball_sha256 = "b029a282a70e1332c343ee9d80c5168e2fe62169c7ea07cb5fb9e8761f30c5f7",
    )
    _crosstool_toolchain_repo(
        name = "armtoolchain_cross_powerpc64le-ibm-linux-gnu",
        host = "aarch64",
        target = "powerpc64le-unknown-linux-gnu",
        tarball_sha256 = "1891e6ec3d90e491ca9a846714310228a288266f21a22218fd94360e4f389dd6",
    )
    _crosstool_toolchain_repo(
        name = "armtoolchain_cross_s390x-ibm-linux-gnu",
        host = "aarch64",
        target = "s390x-ibm-linux-gnu",
        tarball_sha256 = "17387aff94369156242ffe75c1681155d09535f49c8d24916723e4435a7c7cd5",
    )
    _crosstool_toolchain_repo(
        name = "armtoolchain_cross_x86_64-unknown-linux-gnu",
        host = "aarch64",
        target = "x86_64-unknown-linux-gnu",
        tarball_sha256 = "897b32086361db47ce3a55a387e5865117d531a30045d5423d607332163b9792",
    )
    _crosstool_toolchain_repo(
        name = "armtoolchain_cross_x86_64-w64-mingw32",
        host = "aarch64",
        target = "x86_64-w64-mingw32",
        tarball_sha256 = "0e9e6792b05a76f9c0f082e15fc12b851b91851e8c19ef7ddaf3bce1877f75b1",
    )
    _macos_toolchain_repos()
