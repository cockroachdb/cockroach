# This file defines a couple of utility methods that are used to generate all
# code for the vectorized engine based on the templates. It should be loaded
# as an extension in relevant packages, and the two functions below should be
# invoked with the corresponding targets mapping (from .eg.go file to its
# _tmpl.go file).

# Define a file group for all the .eg.go targets.
def eg_go_filegroup(name, targets):
    native.filegroup(
        name = name,
        srcs = [":{}".format(rule_name_for(target)) for target, _ in targets],
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
            tags = ["no-remote-exec"],
            cmd = """
GO_REL_PATH=`dirname $(location @go_sdk//:bin/go)`
GO_ABS_PATH=`cd $$GO_REL_PATH && pwd`
export PATH=$$GO_ABS_PATH:$$PATH
export HOME=$(GENDIR)
export GOPATH=/nonexist-gopath
export COCKROACH_INTERNAL_DISABLE_METAMORPHIC_TESTING=true
export GOROOT=
$(location :execgen) -template $(SRCS) \
        -fmt=false pkg/sql/colexec/$@ > $@
$(location :goimports) -w $@
""",
            tools = [
                "@go_sdk//:bin/go",
                ":execgen",
                ":goimports",
            ],
            visibility = [":__pkg__", "//pkg/gen:__pkg__"],
        )

def rule_name_for(target):
    # e.g. 'vec_comparators.eg.go' -> 'gen-vec-comparators'
    return "gen-{}".format(target.replace(".eg.go", "").replace("_", "-"))
