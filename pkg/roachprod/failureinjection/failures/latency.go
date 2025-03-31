// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package failures

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/errors"
)

// NetworkLatency is a failure mode that injects artificial network latency between nodes
// using tc and iptables.
type NetworkLatency struct {
	GenericFailure
	// For each ArtificialLatency rule, we create a new class and qdisc since it simplifies keeping
	// track of which rule to delete. Creating too many (hundreds) qdiscs can lead to increased cpu
	// usage, but we don't expect to have more than a few dozen rules for most use cases, e.g. a 50
	// node cluster simulating MR deployment only needs ~66 qdiscs per node.
	classesInUse       map[int]struct{}
	nextAvailableClass int
	// filterNameToClassMap keeps track of which ArtificialLatency rule is associated with which class.
	// We need this as the only way to remove specific TC filters/qdiscs is by knowing the class number.
	// A filter name is just the ArtificialLatency rule as a string. Adding the same exact rule twice
	// is disallowed.
	filterNameToClassMap map[string]int
}

// ArtificialLatency represents the one way artificial (added) latency from one group of nodes to another.
type ArtificialLatency struct {
	Source      install.Nodes
	Destination install.Nodes
	Delay       time.Duration
}

func (l *ArtificialLatency) String() string {
	return fmt.Sprintf("%d-%d-%s", l.Source, l.Destination, l.Delay)
}

type NetworkLatencyArgs struct {
	ArtificialLatencies []ArtificialLatency
}

func registerNetworkLatencyFailure(r *FailureRegistry) {
	r.add(NetworkLatencyName, NetworkLatencyArgs{}, MakeNetworkLatencyFailure)
}

func MakeNetworkLatencyFailure(
	clusterName string, l *logger.Logger, secure bool,
) (FailureMode, error) {
	c, err := roachprod.GetClusterFromCache(l, clusterName, install.SecureOption(secure))
	if err != nil {
		return nil, err
	}

	genericFailure := GenericFailure{c: c, runTitle: "latency"}
	return &NetworkLatency{
		GenericFailure:       genericFailure,
		classesInUse:         make(map[int]struct{}),
		nextAvailableClass:   1,
		filterNameToClassMap: make(map[string]int),
	}, nil
}

const NetworkLatencyName = "network-latency"

func (f *NetworkLatency) Description() string {
	return NetworkLatencyName
}

func (f *NetworkLatency) Setup(ctx context.Context, l *logger.Logger, args FailureArgs) error {
	interfaces, err := f.NetworkInterfaces(ctx, l)
	if err != nil {
		return err
	}
	cmd := failScriptEarlyCmd
	for _, iface := range interfaces {
		// Ignore the loopback interface since no CRDB traffic should go through it
		// in roachprod deployments.
		if iface == "lo" {
			continue
		}
		cmd += fmt.Sprintf(setupQdiscsCmd, iface)
	}
	l.Printf("Setting up root htb qdisc with cmd: %s", cmd)
	if err := f.Run(ctx, l, f.c.Nodes, cmd); err != nil {
		return err
	}
	return nil
}

const (
	failScriptEarlyCmd = `# ensure any failure fails the entire script.
set -e;`

	setupQdiscsCmd = `
NETWORK_IFACE=%[1]s

# Create a root htb qdisc that all other qdiscs will be attached to.
sudo tc qdisc replace dev ${NETWORK_IFACE} root handle 1: htb default 12	
`
	addFilterCmd = `
NETWORK_IFACE=%[1]s
CLASS=%[2]d
HANDLE=%[3]d
DELAY=%[4]s

# Create a class for the new latency rule. htb makes us set a bandwidth limit so set it
# arbitrarily high.
sudo tc class add dev ${NETWORK_IFACE} parent 1: classid 1:${CLASS} htb rate 10000mbit

# Attach a netem qdisc to the class with the specified delay.
sudo tc qdisc add dev ${NETWORK_IFACE} parent 1:${CLASS} handle ${HANDLE}: netem delay ${DELAY}

# Add a filter to mark all packets matching the IP and port to be routed
# to the netem qdisc we added above which actually adds the delay.
#
# We set the filter priority to the class number as a hack so we can easily
# remove the filter later in one shot. We don't really care about priority 
# since our rules are relatively simplistic. Without this, we would have to
# grep and parse the output of 'tc filter show'' to find the filter(s) we want to
# remove since a priority would be randomly assigned.
sudo tc filter add dev ${NETWORK_IFACE} parent 1: protocol ip prio ${CLASS} u32 \
match ip dst {ip:%[5]d}/32 \
match ip dport {pgport:%[5]d} 0xffff \
flowid 1:${CLASS}

# Same as above but with the public IP.
sudo tc filter add dev ${NETWORK_IFACE} parent 1: protocol ip prio ${CLASS} u32 \
match ip dst {ip:%[5]d:public}/32 \
match ip dport {pgport:%[5]d} 0xffff \
flowid 1:${CLASS}
`

	removeFilterCmd = `
NETWORK_IFACE=%[1]s
CLASS=%[2]d

# Delete in reverse order as added above to avoid resource is busy errors.
sudo tc filter del dev ${NETWORK_IFACE} parent 1: protocol ip prio ${CLASS} u32
sudo tc qdisc del dev ${NETWORK_IFACE} parent 1:${CLASS}
sudo tc class del dev ${NETWORK_IFACE} parent 1: classid 1:${CLASS}
`

	removeQdiscCmd = `
NETWORK_IFACE=%[1]s
sudo tc qdisc del dev ${NETWORK_IFACE} root
`
)

// findNextOpenClass returns the lowest available class number that
// can be used to add a new latency rule.
func (f *NetworkLatency) findNextOpenClass() int {
	class := f.nextAvailableClass
	f.classesInUse[class] = struct{}{}
	// Iterate until we find the next available class.
	for {
		if _, ok := f.classesInUse[f.nextAvailableClass]; !ok {
			break
		}
		f.nextAvailableClass++
	}
	return class
}

func (f *NetworkLatency) Inject(ctx context.Context, l *logger.Logger, args FailureArgs) error {
	latencies := args.(NetworkLatencyArgs).ArtificialLatencies
	for _, latency := range latencies {
		for _, dest := range latency.Destination {
			interfaces, err := f.NetworkInterfaces(ctx, l)
			if err != nil {
				return err
			}

			// Enforce we don't have duplicate rules, as it complicates the removal process of filters
			// and is something the user likely didn't intend.
			class := f.findNextOpenClass()
			if _, ok := f.filterNameToClassMap[latency.String()]; ok {
				return errors.Newf("failed trying to inject ArtificialLatency, rule already exists: %+v", latency)
			}
			f.filterNameToClassMap[latency.String()] = class
			handle := 10 * class

			cmd := failScriptEarlyCmd
			for _, iface := range interfaces {
				if iface == "lo" {
					continue
				}
				cmd += fmt.Sprintf(addFilterCmd, iface, class, handle, latency.Delay, dest)
			}
			l.Printf("Adding artificial latency from nodes %d to node %d with cmd: %s", latency.Source, dest, cmd)
			if err := f.Run(ctx, l, latency.Source, cmd); err != nil {
				return err
			}
		}
	}
	return nil
}

func (f *NetworkLatency) Restore(ctx context.Context, l *logger.Logger, args FailureArgs) error {
	latencies := args.(NetworkLatencyArgs).ArtificialLatencies
	for _, latency := range latencies {
		for _, dest := range latency.Destination {
			interfaces, err := f.NetworkInterfaces(ctx, l)
			if err != nil {
				return err
			}

			class, ok := f.filterNameToClassMap[latency.String()]
			if !ok {
				return errors.New("failed trying to restore latency failure, ArtificialLatency rule was not found: %+v")
			}

			cmd := failScriptEarlyCmd
			for _, iface := range interfaces {
				if iface == "lo" {
					continue
				}
				cmd = cmd + fmt.Sprintf(removeFilterCmd, iface, class)
			}
			l.Printf("Removing artificial latency from nodes %d to node %d with cmd: %s", latency.Source, dest, cmd)
			if err := f.Run(ctx, l, latency.Source, cmd); err != nil {
				return err
			}
			if class < f.nextAvailableClass {
				f.nextAvailableClass = class
			}
			delete(f.filterNameToClassMap, latency.String())
		}
	}
	return nil
}

func (f *NetworkLatency) Cleanup(ctx context.Context, l *logger.Logger, args FailureArgs) error {
	interfaces, err := f.NetworkInterfaces(ctx, l)
	if err != nil {
		return err
	}
	cmd := failScriptEarlyCmd
	for _, iface := range interfaces {
		if iface == "lo" {
			continue
		}
		cmd += fmt.Sprintf(removeQdiscCmd, iface)

	}
	l.Printf("Removing root htb qdisc with cmd: %s", cmd)
	if err = f.Run(ctx, l, f.c.Nodes, cmd); err != nil {
		return err
	}
	f.classesInUse = make(map[int]struct{})
	return nil
}

func (f *NetworkLatency) WaitForFailureToPropagate(
	ctx context.Context, l *logger.Logger, args FailureArgs,
) error {
	// TODO(Darryl): Monitor cluster (e.g. for replica convergence) and block until it's stable.
	return nil
}

func (f *NetworkLatency) WaitForFailureToRestore(
	ctx context.Context, l *logger.Logger, args FailureArgs,
) error {
	// TODO(Darryl): Monitor cluster (e.g. for replica convergence) and block until it's stable.
	return nil
}
