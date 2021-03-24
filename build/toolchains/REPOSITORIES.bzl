load("//build:toolchains/dev/darwin-x86_64/toolchain.bzl",
     _dev_darwin_x86_repo = "dev_darwin_x86_repo")

def toolchain_dependencies():
    _dev_darwin_x86_repo(name = "toolchain_dev_darwin_x86-64")
