load("//build/toolchains:dev/darwin-x86_64/toolchain.bzl",
     _dev_darwin_x86_repo = "dev_darwin_x86_repo")
load("//build/toolchains:crosstool-ng/toolchain.bzl",
     _crosstool_toolchain_repo = "crosstool_toolchain_repo")
load("//build/toolchains:darwin-x86_64/toolchain.bzl",
     _macos_toolchain_repo = "macos_toolchain_repo")

def toolchain_dependencies():
    _dev_darwin_x86_repo(name = "toolchain_dev_darwin_x86-64")
    _crosstool_toolchain_repo(
        name = "toolchain_cross_aarch64-unknown-linux-gnu",
        target = "aarch64-unknown-linux-gnu",
        tarball_sha256 = "ed7ebe618794c0a64aec742d1bf9274302f86a8a81505758c97dc99dab5fd6ab",
    )
    _crosstool_toolchain_repo(
        name = "toolchain_cross_s390x-ibm-linux-gnu",
        target = "s390x-ibm-linux-gnu",
        tarball_sha256 = "93c34d3111e38882fd88f38df33243c52466f703d78e7dd8ac0260c9e1ca35c6",
    )
    _crosstool_toolchain_repo(
        name = "toolchain_cross_x86_64-unknown-linux-gnu",
        target = "x86_64-unknown-linux-gnu",
        tarball_sha256 = "38f06a929fcc3d1405fe229aa8bc30e57ca78312f4e07e10a68cd3568a64412e",
    )
    _crosstool_toolchain_repo(
        name = "toolchain_cross_x86_64-w64-mingw32",
        target = "x86_64-w64-mingw32",
        tarball_sha256 = "6900b96f7bbd86ba96c4c9704eab6fcb2241fdb5df0a8b9cb3416505a6ef19f7",
    )
    _macos_toolchain_repo(name = "toolchain_cross_x86_64-apple-darwin19")
