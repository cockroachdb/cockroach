// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package tc contains utility methods for using the Linux tc (traffic control)
// command to mess with the network links between cockroach nodes running on
// the local machine.
//
// Requires passwordless sudo in order to run tc.
//
// Does not work on OS X due to the lack of the tc command (and even an
// alternative wouldn't work for the current use case of this code, which also
// requires being able to bind to multiple localhost addresses).
package tc

import (
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
)

const (
	rootHandle   = 1
	defaultClass = 1
)

// Controller provides a way to add artificial latency to local traffic.
type Controller struct {
	interfaces []string
	nextClass  int
}

// NewController creates and returns a controller that will modify the traffic
// routing for the provided interfaces.
func NewController(interfaces ...string) *Controller {
	return &Controller{
		interfaces: interfaces,
		nextClass:  defaultClass + 1,
	}
}

// Init prepares the local network interfaces so that we can later add per-node
// traffic shaping rules.
func (c *Controller) Init() error {
	for _, ifce := range c.interfaces {
		_, _ = exec.Command("sudo", strings.Split(fmt.Sprintf("tc qdisc del dev %s root", ifce), " ")...).Output()
		out, err := exec.Command("sudo", strings.Split(fmt.Sprintf("tc qdisc add dev %s root handle %d: htb default %d",
			ifce, rootHandle, defaultClass), " ")...).Output()
		if err != nil {
			return errors.Wrapf(err, "failed to create root tc qdisc for %q: %s", ifce, out)
		}
		// The 100mbit limitation is because classes of type htb (hierarchy token
		// bucket) need a bandwidth limit, and we want an arbitrarily high one. Feel
		// free to bump it up here and below if you think it's limiting you.
		out, err = exec.Command("sudo", strings.Split(fmt.Sprintf("tc class add dev %s parent %d: classid %d:%d htb rate 100mbit",
			ifce, rootHandle, rootHandle, defaultClass), " ")...).Output()
		if err != nil {
			return errors.Wrapf(err, "failed to create root tc class for %q: %s", ifce, out)
		}
	}
	return nil
}

// AddLatency adds artificial latency between the specified source and dest
// addresses.
func (c *Controller) AddLatency(srcIP, dstIP string, latency time.Duration) error {
	class := c.nextClass
	handle := class * 10
	c.nextClass++
	for _, ifce := range c.interfaces {
		out, err := exec.Command("sudo", strings.Split(fmt.Sprintf("tc class add dev %s parent %d: classid %d:%d htb rate 100mbit",
			ifce, rootHandle, rootHandle, class), " ")...).Output()
		if err != nil {
			return errors.Wrapf(err, "failed to add tc class %d: %s", class, out)
		}
		out, err = exec.Command("sudo", strings.Split(fmt.Sprintf("tc qdisc add dev %s parent %d:%d handle %d: netem delay %v",
			ifce, rootHandle, class, handle, latency), " ")...).Output()
		if err != nil {
			return errors.Wrapf(err, "failed to add tc netem delay of %v: %s", latency, out)
		}
		out, err = exec.Command("sudo", strings.Split(fmt.Sprintf("tc filter add dev %s parent %d: protocol ip u32 match ip src %s/32 match ip dst %s/32 flowid %d:%d",
			ifce, rootHandle, srcIP, dstIP, rootHandle, class), " ")...).Output()
		if err != nil {
			return errors.Wrapf(err, "failed to add tc filter rule between %s and %s: %s", srcIP, dstIP, out)
		}
	}
	return nil
}

// CleanUp resets all interfaces back to their default tc policies.
func (c *Controller) CleanUp() error {
	var err error
	for _, ifce := range c.interfaces {
		out, thisErr := exec.Command("sudo", strings.Split(fmt.Sprintf("tc qdisc del dev %s root", ifce), " ")...).Output()
		if err != nil {
			err = errors.CombineErrors(err, errors.Wrapf(
				thisErr, "failed to remove tc rules for %q -- you may have to remove them manually: %s", ifce, out))
		}
	}
	return err
}
