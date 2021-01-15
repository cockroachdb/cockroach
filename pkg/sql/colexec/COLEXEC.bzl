# Map between target name and relevant template.
targets = [
    ('and_or_projection.eg.go', 'and_or_projection_tmpl.go'),
    ('cast.eg.go', 'cast_tmpl.go'),
    ('const.eg.go', 'const_tmpl.go'),
    ('crossjoiner.eg.go', 'crossjoiner_tmpl.go'),
    ('default_cmp_expr.eg.go', 'default_cmp_expr_tmpl.go'),
    ('default_cmp_proj_ops.eg.go', 'default_cmp_proj_ops_tmpl.go'),
    ('default_cmp_sel_ops.eg.go', 'default_cmp_sel_ops_tmpl.go'),
    ('distinct.eg.go', 'distinct_tmpl.go'),
    ('hash_aggregator.eg.go', 'hash_aggregator_tmpl.go'),
    ('hash_utils.eg.go', 'hash_utils_tmpl.go'),
    ('hashjoiner.eg.go', 'hashjoiner_tmpl.go'),
    ('hashtable_distinct.eg.go', 'hashtable_tmpl.go'),
    ('hashtable_full_default.eg.go', 'hashtable_tmpl.go'),
    ('hashtable_full_deleting.eg.go', 'hashtable_tmpl.go'),
    ('is_null_ops.eg.go', 'is_null_ops_tmpl.go'),
    # ('like_ops.eg.go', ...); See `gen_like_ops_rule` below.
    ('mergejoinbase.eg.go', 'mergejoinbase_tmpl.go'),
    ('mergejoiner_exceptall.eg.go', 'mergejoiner_tmpl.go'),
    ('mergejoiner_fullouter.eg.go', 'mergejoiner_tmpl.go'),
    ('mergejoiner_inner.eg.go', 'mergejoiner_tmpl.go'),
    ('mergejoiner_intersectall.eg.go', 'mergejoiner_tmpl.go'),
    ('mergejoiner_leftanti.eg.go', 'mergejoiner_tmpl.go'),
    ('mergejoiner_leftouter.eg.go', 'mergejoiner_tmpl.go'),
    ('mergejoiner_leftsemi.eg.go', 'mergejoiner_tmpl.go'),
    ('mergejoiner_rightanti.eg.go', 'mergejoiner_tmpl.go'),
    ('mergejoiner_rightouter.eg.go', 'mergejoiner_tmpl.go'),
    ('mergejoiner_rightsemi.eg.go', 'mergejoiner_tmpl.go'),
    ('ordered_synchronizer.eg.go', 'ordered_synchronizer_tmpl.go'),
    ('proj_const_left_ops.eg.go', 'proj_const_ops_tmpl.go'),
    ('proj_const_right_ops.eg.go', 'proj_const_ops_tmpl.go'),
    ('proj_non_const_ops.eg.go', 'proj_non_const_ops_tmpl.go'),
    ('quicksort.eg.go', 'quicksort_tmpl.go'),
    ('rank.eg.go', 'rank_tmpl.go'),
    ('relative_rank.eg.go', 'relative_rank_tmpl.go'),
    ('row_number.eg.go', 'row_number_tmpl.go'),
    ('rowstovec.eg.go', 'rowstovec_tmpl.go'),
    ('select_in.eg.go', 'select_in_tmpl.go'),
    ('selection_ops.eg.go', 'selection_ops_tmpl.go'),
    ('sort.eg.go', 'sort_tmpl.go'),
    ('substring.eg.go', 'substring_tmpl.go'),
    ('values_differ.eg.go', 'values_differ_tmpl.go'),
    ('vec_comparators.eg.go', 'vec_comparators_tmpl.go'),
    ('window_peer_grouper.eg.go', 'window_peer_grouper_tmpl.go'),
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

# TODO(irfansharif): We should be able to use `gen_eg_go_rules` to generate
# like_ops.eg.go. It's special-cased here because execgen reads two separate
# template files[1] when generating like_ops.eg.go.
#
# [1]: https://github.com/cockroachdb/cockroach/blob/1f23ef2b4e/pkg/sql/colexec/execgen/cmd/execgen/like_ops_gen.go#L48
def gen_like_ops_rule(name, templates, target):
    native.genrule(
        name = name,
        srcs = templates,
        outs = [target],
        # See TODO above. We should ideally be using $(SRCS) to point to the
        # template file, but like_ops.eg.go needs access to two template files.
        # We point to the first, which is the template the generator is
        # registered with, but we also need to include the other in our srcs so
        # it's included in the sandbox when generating the file (-template only
        # expects one argument).
        cmd = """
          ln -s external/cockroach/pkg pkg
          $(location :execgen) -template $(location %s) \
              -fmt=false pkg/sql/colexec/$@ > $@
          $(location :goimports) -w $@
          """ % (templates[0]),
        tools = [":execgen", ":goimports"], # Make use of the same aliases defined above.
    )
