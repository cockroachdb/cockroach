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
    ('proj_like_ops.eg.go', 'proj_const_ops_tmpl.go'),
    ('proj_non_const_ops.eg.go', 'proj_non_const_ops_tmpl.go'),
    ('quicksort.eg.go', 'quicksort_tmpl.go'),
    ('rank.eg.go', 'rank_tmpl.go'),
    ('relative_rank.eg.go', 'relative_rank_tmpl.go'),
    ('row_number.eg.go', 'row_number_tmpl.go'),
    ('rowstovec.eg.go', 'rowstovec_tmpl.go'),
    ('select_in.eg.go', 'select_in_tmpl.go'),
    ('selection_ops.eg.go', 'selection_ops_tmpl.go'),
    ('sel_like_ops.eg.go', 'selection_ops_tmpl.go'),
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
            cmd = """
              $(location :execgen) -template $(SRCS) \
                  -fmt=false pkg/sql/colexec/$@ > $@
              $(location :goimports) -w $@
              """,
            tools = [":execgen", ":goimports"],
        )
