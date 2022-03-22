# stringer lets us define the equivalent of `//go:generate stringer` files
# within bazel sandbox.
def stringer(src, typ, name, additional_args=[]):
    native.genrule(
        name = name,
        srcs = [src],  # Accessed below using `$<`.
        outs = [typ.lower() + "_string.go"],
        # golang.org/x/tools executes commands via
        # golang.org/x/sys/execabs which requires all PATH lookups to
        # result in absolute paths. To account for this, we resolve the
        # relative path returned by location to an absolute path.
        cmd = """\
GO_REL_PATH=`dirname $(location @go_sdk//:bin/go)`
GO_ABS_PATH=`cd $$GO_REL_PATH && pwd`
# Set GOPATH to something to workaround https://github.com/golang/go/issues/43938
env PATH=$$GO_ABS_PATH HOME=$(GENDIR) GOPATH=/nonexist-gopath \
$(location @com_github_cockroachdb_tools//cmd/stringer:stringer) -output=$@ -type={typ} {args} $<
""".format(
         typ = typ,
         args = " ".join(additional_args),
        ),
        visibility = [":__pkg__", "//pkg/gen:__pkg__"],
        tools = [
            "@go_sdk//:bin/go",
            "@com_github_cockroachdb_tools//cmd/stringer",
        ],
    )
