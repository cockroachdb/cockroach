# This file defines a couple of utility methods that are used to generate all
# code for the vectorized engine based on the templates. It should be loaded
# as an extension in relevant packages, and the two functions below should be
# invoked with the corresponding targets mapping (from .eg.go file to its
# _tmpl.go file).

# Define a file group for all the .eg.go targets.
def eg_go_filegroup(name, targets):
    native.filegroup(
        name = name,
        srcs = [':{}'.format(rule_name_for(target)) for target, _ in targets],
    )

# Define gen rules for individual eg.go files.
def gen_eg_go_rules(targets):
    # Define some aliases for ease of use.
    native.alias(
        name = "execgen",
        actual = "//pkg/sql/colexec/execgen/cmd/execgen",
    )
    native.alias(
        name = "goimports",
        actual = "@com_github_cockroachdb_gostdlib//x/tools/cmd/goimports",
    )

    for target, template in targets:
        name = rule_name_for(target)

        native.genrule(
            name = name,
            srcs = [template],
            outs = [target],
            # `$@` lets us substitute in the output path[1]. The symlink below
            # is frowned upon for genrules[2]. That said, when testing
            # pkg/sql/colexec through bazel it expects to find the template
            # files in a path other than what SRCS would suggest. We haven't
            # really investigated why. For now lets just symlink the relevant
            # files into the "right" path within the bazel sandbox[3].
            #
            # [1]: https://docs.bazel.build/versions/3.7.0/be/general.html#genrule_args
            # [2]: https://docs.bazel.build/versions/3.7.0/be/general.html#general-advice
            # [3]: https://github.com/cockroachdb/cockroach/pull/57027
            cmd = """
              ln -s external/cockroach/pkg pkg
              $(location :execgen) -template $(SRCS) \
                  -fmt=false pkg/sql/colexec/$@ > $@
              $(location :goimports) -w $@
              """,
            tools = [":execgen", ":goimports"],
        )

def rule_name_for(target):
    # e.g. 'vec_comparators.eg.go' -> 'gen-vec-comparators'
    return 'gen-{}'.format(target.replace('.eg.go', '').replace('_', '-'))
