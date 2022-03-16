load(
    "//build/toolchains:dev/darwin-x86_64/toolchain.bzl",
    _dev_darwin_x86_repo = "dev_darwin_x86_repo",
)
load(
    "//build/toolchains:crosstool-ng/toolchain.bzl",
    _crosstool_toolchain_repo = "crosstool_toolchain_repo",
)
load(
    "//build/toolchains:darwin/toolchain.bzl",
    _macos_toolchain_repos = "macos_toolchain_repos",
)

def toolchain_dependencies():
    _dev_darwin_x86_repo(name = "toolchain_dev_darwin_x86-64")
    _crosstool_toolchain_repo(
        name = "toolchain_cross_aarch64-unknown-linux-gnu",
        host = "x86_64",
        target = "aarch64-unknown-linux-gnu",
        tarball_sha256 = "76efd6ee539a0a358c03a6405c95c2f758935bbe70572e030fad86fb328b7d7b",
    )
    _crosstool_toolchain_repo(
        name = "toolchain_cross_s390x-ibm-linux-gnu",
        host = "x86_64",
        target = "s390x-ibm-linux-gnu",
        tarball_sha256 = "3917b7ba50b30d4907d05a3060e79e931e541cb7096913285e4fc0b06ccc98fe",
    )
    _crosstool_toolchain_repo(
        name = "toolchain_cross_x86_64-unknown-linux-gnu",
        host = "x86_64",
        target = "x86_64-unknown-linux-gnu",
        tarball_sha256 = "267331e9c9cefdcb079a1487b346014f956e6b95332286fadb43d41b6cdd8ce0",
    )
    _crosstool_toolchain_repo(
        name = "toolchain_cross_x86_64-w64-mingw32",
        host = "x86_64",
        target = "x86_64-w64-mingw32",
        tarball_sha256 = "6b2a41d551f91d43b00ce086c60bb589e0ae95da2b14cc3fc6e2f2bf4494b21d",
    )
    _crosstool_toolchain_repo(
        name = "armtoolchain_cross_aarch64-unknown-linux-gnu",
        host = "aarch64",
        target = "aarch64-unknown-linux-gnu",
        tarball_sha256 = "8e8455a9f5cec46aa6f8a7ffa53b6fb470f98ae169c49a8fe65ed2fbeb83e82f",
    )
    _crosstool_toolchain_repo(
        name = "armtoolchain_cross_s390x-ibm-linux-gnu",
        host = "aarch64",
        target = "s390x-ibm-linux-gnu",
        tarball_sha256 = "013e7ad3294b7f77b4937e988e0766bd0eb02186876c9e12f51314af38a43597",
    )
    _crosstool_toolchain_repo(
        name = "armtoolchain_cross_x86_64-unknown-linux-gnu",
        host = "aarch64",
        target = "x86_64-unknown-linux-gnu",
        tarball_sha256 = "a74aa2fb685b23e32a099f4c0bd32618640fc6f1070237edfd3f0c7f613ea983",
    )
    _crosstool_toolchain_repo(
        name = "armtoolchain_cross_x86_64-w64-mingw32",
        host = "aarch64",
        target = "x86_64-w64-mingw32",
        tarball_sha256 = "6baff5e7658215aabfbc6d37374f4fcdd1b62269828d55ebef841248a99373cc",
    )
    _macos_toolchain_repos()
