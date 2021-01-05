# NB: The helpers here were grafted from pkg/sql/colexec/COLEXEC.bzl.
# Future changes here may apply there.

# Map between target name and relevant template.
targets = [
    ('hash_any_not_null_agg.eg.go', 'any_not_null_agg_tmpl.go'),
    ('hash_avg_agg.eg.go', 'avg_agg_tmpl.go'),
    ('hash_bool_and_or_agg.eg.go', 'bool_and_or_agg_tmpl.go'),
    ('hash_concat_agg.eg.go', 'concat_agg_tmpl.go'),
    ('hash_count_agg.eg.go', 'count_agg_tmpl.go'),
    ('hash_default_agg.eg.go', 'default_agg_tmpl.go'),
    ('hash_min_max_agg.eg.go', 'min_max_agg_tmpl.go'),
    ('hash_sum_agg.eg.go', 'sum_agg_tmpl.go'),
    ('hash_sum_int_agg.eg.go', 'sum_agg_tmpl.go'),
    ('ordered_any_not_null_agg.eg.go', 'any_not_null_agg_tmpl.go'),
    ('ordered_avg_agg.eg.go', 'avg_agg_tmpl.go'),
    ('ordered_bool_and_or_agg.eg.go', 'bool_and_or_agg_tmpl.go'),
    ('ordered_concat_agg.eg.go', 'concat_agg_tmpl.go'),
    ('ordered_count_agg.eg.go', 'count_agg_tmpl.go'),
    ('ordered_default_agg.eg.go', 'default_agg_tmpl.go'),
    ('ordered_min_max_agg.eg.go', 'min_max_agg_tmpl.go'),
    ('ordered_sum_agg.eg.go', 'sum_agg_tmpl.go'),
    ('ordered_sum_int_agg.eg.go', 'sum_agg_tmpl.go'),
]

def rule_name_for(target):
    # e.g. 'vec_comparators.eg.go' -> 'gen-vec-comparators'
    return 'gen-{}'.format(target.replace('.eg.go', '').replace('_', '-'))

# Define a file group for all the .eg.go targets.
def eg_go_filegroup(name):
    native.filegroup(
        name = name,
        srcs = [':{}'.format(rule_name_for(target)) for target, _ in targets],
    )

# Define gen rules for individual eg.go files.
def gen_eg_go_rules():
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
