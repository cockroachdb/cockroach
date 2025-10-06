// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"net"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/netutil/addr"
	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
	"github.com/cockroachdb/errors"
	"github.com/pires/go-proxyproto"
)

// ListenError is returned from Start when we fail to start listening on either
// the main Cockroach port or the HTTP port, so that the CLI can instruct the
// user on what might have gone wrong.
type ListenError struct {
	cause error
	Addr  string
}

// Error implements error.
func (l *ListenError) Error() string { return l.cause.Error() }

// Unwrap is because ListenError is a wrapper.
func (l *ListenError) Unwrap() error { return l.cause }

// ListenerFactoryForConfig return an RPCListenerFactory for the given
// configuration. If the configuration does not specify any secondary
// tenant port configuration, no factory is returned.
func ListenerFactoryForConfig(cfg *BaseConfig, portStartHint int) RPCListenerFactory {
	if cfg.Config.ApplicationInternalRPCPortMin > 0 {
		rlf := &rangeListenerFactory{
			startHint:  portStartHint,
			lowerBound: cfg.Config.ApplicationInternalRPCPortMin,
			upperBound: cfg.Config.ApplicationInternalRPCPortMax,
		}
		return rlf.ListenAndUpdateAddrs
	}
	return nil
}

// The rangeListenerFactory tries to listen on a port between
// lowerBound and upperBound.  The provided startHint allows the
// caller to specify an offset into the range to speed up port
// selection.
type rangeListenerFactory struct {
	startHint  int
	lowerBound int
	upperBound int
}

func (rlf *rangeListenerFactory) ListenAndUpdateAddrs(
	ctx context.Context,
	listenAddr, advertiseAddr *string,
	connName string,
	acceptProxyProtocolHeaders bool,
) (net.Listener, error) {
	h, _, err := addr.SplitHostPort(*listenAddr, "0")
	if err != nil {
		return nil, err
	}

	if rlf.lowerBound > rlf.upperBound {
		return nil, errors.AssertionFailedf("lower bound %d greater than upper bound %d", rlf.lowerBound, rlf.upperBound)
	}

	numCandidates := (rlf.upperBound - rlf.lowerBound) + 1
	nextPort := rlf.lowerBound + (rlf.startHint % numCandidates)

	var ln net.Listener
	for numAttempts := 0; numAttempts < numCandidates; numCandidates++ {
		nextAddr := net.JoinHostPort(h, strconv.Itoa(nextPort))
		ln, err = net.Listen("tcp", nextAddr)
		if err == nil {
			if acceptProxyProtocolHeaders {
				ln = &proxyproto.Listener{
					Listener: ln,
				}
			}
			if err := UpdateAddrs(ctx, listenAddr, advertiseAddr, ln.Addr()); err != nil {
				return nil, errors.Wrapf(err, "internal error: cannot parse %s listen address", connName)
			}
			return ln, nil
		}
		if !sysutil.IsAddrInUse(err) {
			return nil, err
		}

		nextPort = ((nextPort - rlf.lowerBound + 1) % numCandidates) + rlf.lowerBound
	}
	return nil, errors.Wrapf(err, "port range (%d, %d) exhausted", rlf.lowerBound, rlf.upperBound)
}

// ListenAndUpdateAddrs starts a TCP listener on the specified address
// then updates the address and advertised address fields based on the
// actual interface address resolved by the OS during the Listen()
// call.
func ListenAndUpdateAddrs(
	ctx context.Context,
	addr, advertiseAddr *string,
	connName string,
	acceptProxyProtocolHeaders bool,
) (net.Listener, error) {
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		return nil, &ListenError{
			cause: err,
			Addr:  *addr,
		}
	}
	if acceptProxyProtocolHeaders {
		ln = &proxyproto.Listener{
			Listener: ln,
		}
	}
	if err := UpdateAddrs(ctx, addr, advertiseAddr, ln.Addr()); err != nil {
		return nil, errors.Wrapf(err, "internal error: cannot parse %s listen address", connName)
	}
	return ln, nil
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
