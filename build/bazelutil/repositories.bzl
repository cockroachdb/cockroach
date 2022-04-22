load(":distdir.bzl", "distdir")
load(":distdir_files.bzl", "DISTDIR_FILES")

def distdir_repositories():
    distdir(
        name = "distdir",
        files = DISTDIR_FILES,
    )
