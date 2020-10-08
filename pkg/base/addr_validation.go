// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package base

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// ValidateAddrs controls the address fields in the Config object
// and "fills in" the blanks:
// - the host part of Addr and HTTPAddr is resolved to an IP address
//   if specified (it stays blank if blank to mean "all addresses").
// - the host part of AdvertiseAddr is filled in if blank, either
//   from Addr if non-empty or os.Hostname(). It is also checked
//   for resolvability.
// - non-numeric port numbers are resolved to numeric.
//
// The addresses fields must be guaranteed by the caller to either be
// completely empty, or have both a host part and a port part
// separated by a colon. In the latter case either can be empty to
// indicate it's left unspecified.
func (cfg *Config) ValidateAddrs(ctx context.Context) error {
	// Validate the advertise address.
	advHost, advPort, err := validateAdvertiseAddr(ctx,
		cfg.AdvertiseAddr, cfg.Addr, "", cliflags.ListenAddr)
	if err != nil {
		return invalidFlagErr(err, cliflags.AdvertiseAddr)
	}
	cfg.AdvertiseAddr = net.JoinHostPort(advHost, advPort)

	// Validate the RPC listen address.
	listenHost, listenPort, err := validateListenAddr(ctx, cfg.Addr, "")
	if err != nil {
		return invalidFlagErr(err, cliflags.ListenAddr)
	}
	cfg.Addr = net.JoinHostPort(listenHost, listenPort)

	// Validate the SQL advertise address. Use the provided advertise
	// addr as default.
	advSQLHost, advSQLPort, err := validateAdvertiseAddr(ctx,
		cfg.SQLAdvertiseAddr, cfg.SQLAddr, advHost, cliflags.ListenSQLAddr)
	if err != nil {
		return invalidFlagErr(err, cliflags.SQLAdvertiseAddr)
	}
	cfg.SQLAdvertiseAddr = net.JoinHostPort(advSQLHost, advSQLPort)

	// Validate the SQL listen address - use the resolved listen addr as default.
	sqlHost, sqlPort, err := validateListenAddr(ctx, cfg.SQLAddr, listenHost)
	if err != nil {
		return invalidFlagErr(err, cliflags.ListenSQLAddr)
	}
	cfg.SQLAddr = net.JoinHostPort(sqlHost, sqlPort)

	// Validate the HTTP advertise address. Use the provided advertise
	// addr as default.
	advHTTPHost, advHTTPPort, err := validateAdvertiseAddr(ctx,
		cfg.HTTPAdvertiseAddr, cfg.HTTPAddr, advHost, cliflags.ListenHTTPAddr)
	if err != nil {
		return errors.Wrap(err, "cannot compute public HTTP address")
	}
	cfg.HTTPAdvertiseAddr = net.JoinHostPort(advHTTPHost, advHTTPPort)

	// Validate the HTTP address -- use the resolved listen addr
	// as default.
	httpHost, httpPort, err := validateListenAddr(ctx, cfg.HTTPAddr, listenHost)
	if err != nil {
		return invalidFlagErr(err, cliflags.ListenHTTPAddr)
	}
	cfg.HTTPAddr = net.JoinHostPort(httpHost, httpPort)
	return nil
}

// UpdateAddrs updates the listen and advertise port numbers with
// those found during the call to net.Listen().
//
// After ValidateAddrs() the actual listen addr should be equal to the
// one requested; only the port number can change because of
// auto-allocation. We do check this equality here and report a
// warning if any discrepancy is found.
func UpdateAddrs(ctx context.Context, addr, advAddr *string, ln net.Addr) error {
	desiredHost, _, err := net.SplitHostPort(*addr)
	if err != nil {
		return err
	}

	// Update the listen port number and check the actual listen addr is
	// the one requested.
	lnAddr := ln.String()
	lnHost, lnPort, err := net.SplitHostPort(lnAddr)
	if err != nil {
		return err
	}
	requestedAll := (desiredHost == "" || desiredHost == "0.0.0.0" || desiredHost == "::")
	listenedAll := (lnHost == "" || lnHost == "0.0.0.0" || lnHost == "::")
	if (requestedAll && !listenedAll) || (!requestedAll && desiredHost != lnHost) {
		log.Warningf(ctx, "requested to listen on %q, actually listening on %q", desiredHost, lnHost)
	}
	*addr = net.JoinHostPort(lnHost, lnPort)

	// Update the advertised port number if it wasn't set to start
	// with. We don't touch the advertised host, as this may have
	// nothing to do with the listen address.
	advHost, advPort, err := net.SplitHostPort(*advAddr)
	if err != nil {
		return err
	}
	if advPort == "" || advPort == "0" {
		advPort = lnPort
	}
	*advAddr = net.JoinHostPort(advHost, advPort)
	return nil
}

// validateAdvertiseAddr validates and normalizes an address suitable
// for use in gossiping - for use by other nodes. This ensures
// that if the "host" part is empty, it gets filled in with
// the configured listen address if any, or the canonical host name.
func validateAdvertiseAddr(
	ctx context.Context, advAddr, listenAddr, defaultHost string, listenFlag cliflags.FlagInfo,
) (string, string, error) {
	listenHost, listenPort, err := getListenAddr(listenAddr, defaultHost)
	if err != nil {
		return "", "", invalidFlagErr(err, listenFlag)
	}

	advHost, advPort := "", ""
	if advAddr != "" {
		var err error
		advHost, advPort, err = net.SplitHostPort(advAddr)
		if err != nil {
			return "", "", err
		}
	}
	// If there was no port number, reuse the one from the listen
	// address.
	if advPort == "" || advPort == "0" {
		advPort = listenPort
	}
	// Resolve non-numeric to numeric.
	portNumber, err := net.DefaultResolver.LookupPort(ctx, "tcp", advPort)
	if err != nil {
		return "", "", err
	}
	advPort = strconv.Itoa(portNumber)

	// If the advertise host is empty, then we have two cases.
	if advHost == "" {
		if listenHost != "" {
			// If the listen address was non-empty (ie. explicit, not
			// "listen on all addresses"), use that.
			advHost = listenHost
		} else {
			// No specific listen address, use the canonical host name.
			var err error
			advHost, err = os.Hostname()
			if err != nil {
				return "", "", err
			}

			// As a sanity check, verify that the canonical host name
			// properly resolves. It's not the full story (it could resolve
			// locally but not elsewhere) but at least it prevents typos.
			_, err = net.DefaultResolver.LookupIPAddr(ctx, advHost)
			if err != nil {
				return "", "", err
			}
		}
	}
	return advHost, advPort, nil
}

// validateListenAddr validates and normalizes an address suitable for
// use with net.Listen(). This accepts an empty "host" part as "listen
// on all interfaces" and resolves host names to IP addresses.
func validateListenAddr(ctx context.Context, addr, defaultHost string) (string, string, error) {
	host, port, err := getListenAddr(addr, defaultHost)
	if err != nil {
		return "", "", err
	}
	return resolveAddr(ctx, host, port)
}

func getListenAddr(addr, defaultHost string) (string, string, error) {
	host, port := "", ""
	if addr != "" {
		var err error
		host, port, err = net.SplitHostPort(addr)
		if err != nil {
			return "", "", err
		}
	}
	if host == "" {
		host = defaultHost
	}
	if port == "" {
		port = "0"
	}
	return host, port, nil
}

// resolveAddr resolves non-numeric references to numeric references.
func resolveAddr(ctx context.Context, host, port string) (string, string, error) {
	resolver := net.DefaultResolver

	// Resolve the port number. This may translate service names
	// e.g. "postgresql" to a numeric value.
	portNumber, err := resolver.LookupPort(ctx, "tcp", port)
	if err != nil {
		return "", "", err
	}
	port = strconv.Itoa(portNumber)

	// Resolve the address.
	if host == "" {
		// Keep empty. This means "listen on all addresses".
		return host, port, nil
	}

	addr, err := LookupAddr(ctx, resolver, host)
	return addr, port, err
}

// LookupAddr resolves the given address/host to an IP address. If
// multiple addresses are resolved, it returns the first IPv4 address
// available if there is one, otherwise the first address.
func LookupAddr(ctx context.Context, resolver *net.Resolver, host string) (string, error) {
	// Resolve the IP address or hostname to an IP address.
	addrs, err := resolver.LookupIPAddr(ctx, host)
	if err != nil {
		return "", err
	}
	if len(addrs) == 0 {
		return "", fmt.Errorf("cannot resolve %q to an address", host)
	}

	// TODO(knz): the remainder function can be changed to return all
	// resolved addresses once the server is taught to listen on
	// multiple interfaces. #5816

	// LookupIPAddr() can return a mix of IPv6 and IPv4
	// addresses. Conventionally, the first resolved address is
	// "preferred"; however, for compatibility with previous CockroachDB
	// versions, we still prefer an IPv4 address if there is one.
	for _, addr := range addrs {
		if ip := addr.IP.To4(); ip != nil {
			return ip.String(), nil
		}
	}
	// No IPv4 address, return the first resolved address instead.
	return addrs[0].String(), nil
}

func invalidFlagErr(err error, flag cliflags.FlagInfo) error {
	return errors.Wrapf(err, "invalid --%s", flag.Name)
}
