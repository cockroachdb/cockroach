# Norm Layout

This directory contains definitions for the normalization rules, their
custom helper functions, and their test files. There is a .opt file for each
operator containing the rules that match that operator. For every .opt file,
there is a test file in testdata that contains tests for each normalization
rule. Custom match and replace functions which are only used in one .opt file
are kept in the corresponding _funcs.go file. Functions which are used by 
multiple different rules or are general enough to be reusable belong in 
general_funcs.go.
