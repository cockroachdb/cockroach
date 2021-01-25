# Common function to create //go:generate stringer files within bazel sandbox

def stringer(file, typ, name):
   native.genrule(
      name = name, 
      srcs = [
          file,
      ],
      outs = [typ.lower() + "_string.go"],
      cmd = """
         env PATH=`dirname $(location @go_sdk//:bin/go)` HOME=$(GENDIR) \
         $(location @org_golang_x_tools//cmd/stringer:stringer) -output=$@ -type={} $<
      """.format(typ),
      tools = [
         "@go_sdk//:bin/go",
         "@org_golang_x_tools//cmd/stringer",
       ],
   )
