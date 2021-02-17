# stringer lets us define the equivalent of `//go:generate stringer` files
# within bazel sandbox.
def stringer(src, typ, name):
   native.genrule(
      name = name,
      srcs = [src], # Accessed below using `$<`.
      outs = [typ.lower() + "_string.go"],
      cmd = """
         HOME=$$TMPDIR/gohome \
           $(location @org_golang_x_tools//cmd/stringer:stringer) -output=$@ -type={} $<
      """.format(typ),
      tools = [
         "@org_golang_x_tools//cmd/stringer",
       ],
   )
