// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package hba

import (
	"fmt"
	"net"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/settings/rulebasedscanner"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

// Parse parses the provided HBA configuration.
func Parse(input string) (*Conf, error) {
	tokens, err := rulebasedscanner.Tokenize(input)
	if err != nil {
		return nil, err
	}

	var entries []Entry
	for i, line := range tokens.Lines {
		entry, err := parseHbaLine(line)
		if err != nil {
			return nil, errors.Wrapf(
				pgerror.WithCandidateCode(err, pgcode.ConfigFile),
				"line %d", tokens.Linenos[i])
		}
		entries = append(entries, entry)
	}

	return &Conf{Entries: entries}, nil
}

// parseHbaLine parses one line of HBA configuration.
//
// Inspired from pg's src/backend/libpq/hba.c, parse_hba_line().
func parseHbaLine(inputLine rulebasedscanner.Line) (entry Entry, err error) {
	fieldIdx := 0

	entry.Input = inputLine.Input
	line := inputLine.Tokens
	// Read the connection type.
	if len(line[fieldIdx]) > 1 {
		return entry, errors.WithHint(
			errors.New("multiple values specified for connection type"),
			"Specify exactly one connection type per line.")
	}
	entry.ConnType, err = ParseConnType(line[fieldIdx][0].Value)
	if err != nil {
		return entry, err
	}

	// Get the databases.
	fieldIdx++
	if fieldIdx >= len(line) {
		return entry, errors.New("end-of-line before database specification")
	}
	entry.Database = line[fieldIdx]

	// Get the roles.
	fieldIdx++
	if fieldIdx >= len(line) {
		return entry, errors.New("end-of-line before role specification")
	}
	entry.User = line[fieldIdx]

	if entry.ConnType != ConnLocal {
		fieldIdx++
		if fieldIdx >= len(line) {
			return entry, errors.New("end-of-line before IP address specification")
		}
		tokens := line[fieldIdx]
		if len(tokens) > 1 {
			return entry, errors.WithHint(
				errors.New("multiple values specified for host address"),
				"Specify one address range per line.")
		}
		token := tokens[0]
		switch {
		case token.Value == "":
			return entry, errors.New("cannot use empty string as address")
		case token.IsKeyword("all"):
			entry.Address = token
		case token.IsKeyword("samehost"), token.IsKeyword("samenet"):
			return entry, unimplemented.Newf(
				fmt.Sprintf("hba-net-%s", token.Value),
				"address specification %s is not yet supported", errors.Safe(token.Value))
		default:
			// Split name/mask.
			addr := token.Value
			if strings.Contains(addr, "/") {
				_, ipnet, err := net.ParseCIDR(addr)
				if err != nil {
					return entry, err
				}
				entry.Address = ipnet
			} else {
				var ip net.IP
				hostname := addr
				if ip = net.ParseIP(addr); ip != nil {
					hostname = ""
				}
				if hostname != "" {
					entry.Address = rulebasedscanner.String{Value: addr, Quoted: token.Quoted}
				} else {
					// First field was an IP address.
					fieldIdx++
					if fieldIdx >= len(line) {
						return entry, errors.WithHint(
							errors.New("end-of-line before netmask specification"),
							"Specify an address range in CIDR notation, or provide a separate netmask.")
					}
					if len(line[fieldIdx]) > 1 {
						return entry, errors.New("multiple values specified for netmask")
					}
					maybeMask := net.ParseIP(line[fieldIdx][0].Value)
					if err := checkMask(maybeMask); err != nil {
						return entry, errors.Wrapf(err, "invalid IP mask \"%s\"", line[fieldIdx][0].Value)
					}
					// Do the address families match?
					if (maybeMask.To4() == nil) != (ip.To4() == nil) {
						return entry, errors.Newf("IP address and mask do not match")
					}
					mask := net.IPMask(maybeMask)
					entry.Address = &net.IPNet{IP: ip.Mask(mask), Mask: mask}
				}
			}
		}
	} /* entryType != local */

	// Get the authentication method.
	fieldIdx++
	if fieldIdx >= len(line) {
		return entry, errors.New("end-of-line before authentication method")
	}
	if len(line[fieldIdx]) > 1 {
		return entry, errors.WithHint(
			errors.New("multiple values specified for authentication method"),
			"Specify exactly one authentication method per line.")
	}
	entry.Method = line[fieldIdx][0]
	if entry.Method.Value == "" {
		return entry, errors.New("cannot use empty string as authentication method")
	}

	// Parse remaining arguments.
	for fieldIdx++; fieldIdx < len(line); fieldIdx++ {
		for tokenIdx := 0; tokenIdx < len(line[fieldIdx]); tokenIdx++ {
			tok := line[fieldIdx][tokenIdx]
			kv := strings.SplitN(tok.Value, "=", 2)
			// 1. Handle the case where the option does not have equal operator.
			// 2. Handle the case where token ends with equals operator and next token
			// having the value for option is absent.
			optionsError := errors.Newf("authentication option not in name=value format: %s", tok.Value)
			if len(kv) != 2 {
				return entry, optionsError
			}
			if len(kv[1]) == 0 {
				if (tokenIdx + 1) == len(line[fieldIdx]) {
					return entry, optionsError
				}
				kv[1], tok.Quoted = rulebasedscanner.Join(line[fieldIdx][tokenIdx+1:], ", "), true
				tokenIdx = len(line[fieldIdx])
			}
			entry.Options = append(entry.Options, [2]string{kv[0], kv[1]})
			entry.OptionQuotes = append(entry.OptionQuotes, tok.Quoted)
		}
	}

	return entry, nil
}

// checkMask verifies that maybeMask is a valid IP mask, that is,
// the value is all ones followed by all zeroes.
func checkMask(maybeMask net.IP) error {
	if maybeMask == nil {
		return errors.New("netmask not in IP numeric format")
	}
	if ip4 := maybeMask.To4(); ip4 != nil {
		maybeMask = ip4
	}
	i := 0
	// Skip over all leading ones.
	for ; i < len(maybeMask) && maybeMask[i] == '\xff'; i++ {
	}
	// Skip over the middle mixed ones/zeroes, if any.
	if i < len(maybeMask) {
		switch maybeMask[i] {
		case 0xff, 0xfe, 0xfc, 0xf8, 0xf0, 0xe0, 0xc0, 0x80:
			i++
		}
	}
	// Skip over all trailing zeroes.
	for ; i < len(maybeMask) && maybeMask[i] == '\x00'; i++ {
	}
	// If there's anything remaining, we don't have a proper mask.
	if i < len(maybeMask) {
		return errors.New("address is not a mask")
	}
	return nil
}

// ParseConnType parses the connection type field.
func ParseConnType(s string) (ConnType, error) {
	switch s {
	case "local":
		return ConnLocal, nil
	case "host":
		return ConnHostAny, nil
	case "hostssl":
		return ConnHostSSL, nil
	case "hostnossl":
		return ConnHostNoSSL, nil
	case "loopback":
		return ConnInternalLoopback, nil
	}
	return 0, errors.Newf("unknown connection type: %q", s)
}
