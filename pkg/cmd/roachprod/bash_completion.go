// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

// setBashCompletionFunction sets up a custom bash completion function to
// autocomplete cluster names in various commands.
func setBashCompletionFunction() {
	// Generate a list of commands that DON'T take a cluster argument.
	var s []string
	for _, cmd := range []*cobra.Command{createCmd, listCmd, syncCmd, gcCmd} {
		s = append(s, fmt.Sprintf("%s_%s", rootCmd.Name(), cmd.Name()))
	}
	excluded := strings.Join(s, " | ")

	rootCmd.BashCompletionFunction = fmt.Sprintf(
		`__custom_func()
{
    # only complete the 2nd arg, e.g. adminurl <foo>
    if ! [ $c -eq 2 ]; then
    	return
    fi
    
    # don't complete commands which do not accept a cluster/host arg
    case ${last_command} in
    	%s)
    		return
    		;;
    esac
    
    local hosts_out
    if hosts_out=$(roachprod cached-hosts --cluster="${cur}" 2>/dev/null); then
    		COMPREPLY=( $( compgen -W "${hosts_out[*]}" -- "$cur" ) )
    fi
}`,
		excluded,
	)
}
