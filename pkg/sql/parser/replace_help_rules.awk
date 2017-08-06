# Grammar rules expansion - see README.md

/\/\/ EXTEND WITH HELP:/ {
    prefix = substr($0, 1, index($0, "//")-1)
    helpkey = substr($0, index($0, "HELP:")+6)
    rulename = prefix
    if (index($0, "|") > 0) {
	rulename = substr(rulename, index($0, "|")+1)
    }
    printf "%s %prec VALUES | %s HELPTOKEN %%prec UMINUS { return helpWith(sqllex, \"%s\") }\n", prefix, rulename, helpkey
    next
}
/\/\/ SHOW HELP:/ {
    prefix = substr($0, 1, index($0, "//")-1)
    helpkey = substr($0, index($0, "HELP:")+6)
    printf "%s { return helpWith(sqllex, \"%s\") }\n", prefix, helpkey
    next
}
{ print }
