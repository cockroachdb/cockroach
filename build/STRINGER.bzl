# stringer lets us define the equivalent of `//go:generate stringer` files
# within bazel sandbox.
def stringer(src, typ, name, additional_args=[]):
    native.genrule(
        name = name,
        srcs = [src],  # Accessed below using `$<`.
        outs = [typ.lower() + "_string.go"],
        cmd = """
$(location //pkg/build/bazel/util/tinystringer) -output=$@ -type={typ} {args} $<
""".format(
         typ = typ,
         args = " ".join(additional_args),
        ),
        visibility = [":__pkg__", "//pkg/gen:__pkg__"],
        tools = [
            "//pkg/build/bazel/util/tinystringer",
        ],
    )
