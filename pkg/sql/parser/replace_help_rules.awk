# Grammar rules expansion - see README.md

/\/\/ EXTEND WITH HELP:/ {
    # some_yacc_rule_name // EXTEND WITH HELP:  BLABLA
    # ^^^^^^^^^^^^^^^^^^^ take this
    prefix = substr($0, 1, index($0, "//")-1)
    # some_yacc_rule_name // EXTEND WITH HELP:  BLABLA
    #                                         ^^^^^^^^^ take this
    helpkey = substr($0, index($0, "HELP:")+6)
    rulename = prefix
    if (index($0, "|") > 0) {
        # non-terminal is prefixed by |, e.g. because
        # there was a non-terminal before that. Extract
        # only its name:
        #  | some_yacc_rule_name
        #    ^^^^^^^^^^^^^^^^^^^ take this
        rulename = substr(rulename, index($0, "|")+1)
    }
    printf "%s %%prec VALUES | %s HELPTOKEN %%prec UMINUS { return helpWith(sqllex, \"%s\") }\n", prefix, rulename, helpkey
    next
}
/\/\/ SHOW HELP:/ {
    # some_yacc_rule // SHOW HELP: BLABLA
    # ^^^^^^^^^^^^^^ take this
    prefix = substr($0, 1, index($0, "//")-1)
    # some_yacc_rule // SHOW HELP: BLABLA
    #                             ^^^^^^^^ take this
    helpkey = substr($0, index($0, "HELP:")+6)
    printf "%s { return helpWith(sqllex, \"%s\") }\n", prefix, helpkey
    next
}
{ print }
