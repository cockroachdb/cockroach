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
        tarball_sha256 = "f9b073774826747cf2a91514d5ab27e3ba7f0c7b63acaf80a5ed58c82b08fd44",
    )
    _crosstool_toolchain_repo(
        name = "toolchain_cross_s390x-ibm-linux-gnu",
        host = "x86_64",
        target = "s390x-ibm-linux-gnu",
        tarball_sha256 = "027d7d3b89d0c9745243610b9c12aa26f5605884b058934645cb344927228dab",
    )
    _crosstool_toolchain_repo(
        name = "toolchain_cross_x86_64-unknown-linux-gnu",
        host = "x86_64",
        target = "x86_64-unknown-linux-gnu",
        tarball_sha256 = "5f79da0a9e580bc0a869ca32c2e5a21990676ec567aabf54ccc1dec4c3f2c827",
    )
    _crosstool_toolchain_repo(
        name = "toolchain_cross_x86_64-w64-mingw32",
        host = "x86_64",
        target = "x86_64-w64-mingw32",
        tarball_sha256 = "94e64e0e8de05706dfd5ab2f1fee6e7f75280e35b09b5628980805d27939b418",
    )
    _crosstool_toolchain_repo(
        name = "armtoolchain_cross_aarch64-unknown-linux-gnu",
        host = "aarch64",
        target = "aarch64-unknown-linux-gnu",
        tarball_sha256 = "1e7e5ccc142a528ea4e79a002824af8c15225393d42e452259ed4e99ea45eabe",
    )
    _crosstool_toolchain_repo(
        name = "armtoolchain_cross_s390x-ibm-linux-gnu",
        host = "aarch64",
        target = "s390x-ibm-linux-gnu",
        tarball_sha256 = "76ede410bba820ff9e5e10d68802abc8cf809720fc035a93b1e16f40e987bfd4",
    )
    _crosstool_toolchain_repo(
        name = "armtoolchain_cross_x86_64-unknown-linux-gnu",
        host = "aarch64",
        target = "x86_64-unknown-linux-gnu",
        tarball_sha256 = "7b6101ec5b55e8d10004734823bca221b54b6163e0b500df25ede38f1cc27d2e",
    )
    _crosstool_toolchain_repo(
        name = "armtoolchain_cross_x86_64-w64-mingw32",
        host = "aarch64",
        target = "x86_64-w64-mingw32",
        tarball_sha256 = "1bca59b5aac70bdbbb17499141f2f261dbfeb4accb14cd9a3f699579b5372dc4",
    )
    _macos_toolchain_repos()
