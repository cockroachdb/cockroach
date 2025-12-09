// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gce

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/errors"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/proto"
)

// buildLoadBalancerLabels creates the standard label set for load balancer resources.
// These labels are used for resource identification and filtering.
//
// Note: Only ForwardingRule supports labels among load balancer resources.
// HealthCheck, BackendService, and TargetTcpProxy do not have a Labels field.
// In the future, once all forwarding rules are labeled organically, we can use
// label-based filtering when listing resources for deletion.
func buildLoadBalancerLabels(clusterName string, port int) map[string]string {
	return map[string]string{
		vm.TagCluster:   serializeLabel(clusterName),
		vm.TagRoachprod: "true",
		"port":          strconv.Itoa(port),
	}
}

// buildHealthCheckConfig creates a TCP health check configuration for load balancer.
// This is extracted for testability.
func buildHealthCheckConfig(name string, port int) *computepb.HealthCheck {
	return &computepb.HealthCheck{
		Name: proto.String(name),
		Type: proto.String(computepb.HealthCheck_TCP.String()),
		TcpHealthCheck: &computepb.TCPHealthCheck{
			Port: proto.Int32(int32(port)),
		},
	}
}

// buildBackendServiceConfig creates a backend service configuration for load balancer.
// This is extracted for testability.
func buildBackendServiceConfig(name string, healthCheckSelfLink string) *computepb.BackendService {
	return &computepb.BackendService{
		Name:                proto.String(name),
		LoadBalancingScheme: proto.String("EXTERNAL_MANAGED"),
		Protocol:            proto.String("TCP"),
		HealthChecks:        []string{healthCheckSelfLink},
		TimeoutSec:          proto.Int32(300), // 5 minutes
		PortName:            proto.String("cockroach"),
	}
}

// buildBackendConfig creates a backend configuration for an instance group.
// This is extracted for testability.
func buildBackendConfig(instanceGroupURL string) *computepb.Backend {
	return &computepb.Backend{
		Group:          proto.String(instanceGroupURL),
		BalancingMode:  proto.String("CONNECTION"),
		MaxConnections: proto.Int32(9999999), // "unlimited" for round-robin behavior
	}
}

// buildTargetTcpProxyConfig creates a target TCP proxy configuration for load balancer.
// This is extracted for testability.
func buildTargetTcpProxyConfig(
	name string, backendServiceSelfLink string,
) *computepb.TargetTcpProxy {
	return &computepb.TargetTcpProxy{
		Name:        proto.String(name),
		Service:     proto.String(backendServiceSelfLink),
		ProxyHeader: proto.String("NONE"),
	}
}

// buildForwardingRuleConfig creates a forwarding rule configuration for load balancer.
// This is extracted for testability.
func buildForwardingRuleConfig(
	name string, proxySelfLink string, port int, labels map[string]string,
) *computepb.ForwardingRule {
	return &computepb.ForwardingRule{
		Name:       proto.String(name),
		Target:     proto.String(proxySelfLink),
		PortRange:  proto.String(strconv.Itoa(port)),
		IPProtocol: proto.String("TCP"),
		Labels:     labels,
	}
}

// buildNamedPortConfig creates a named port configuration for instance groups.
// This is extracted for testability.
func buildNamedPortConfig(portName string, port int) *computepb.NamedPort {
	return &computepb.NamedPort{
		Name: proto.String(portName),
		Port: proto.Int32(int32(port)),
	}
}

// buildLoadBalancerNameFilter creates a server-side filter for listing load balancer resources by cluster name.
// Returns a filter string like "name:{cluster}-*"
func buildLoadBalancerNameFilter(clusterName string) string {
	return fmt.Sprintf("name:%s-*", clusterName)
}

// CreateLoadBalancerWithContext creates a TCP load balancer for the given cluster using the GCP SDK.
// This matches the gcloud implementation which creates health checks, backend services, proxies,
// and forwarding rules in sequence.
func (p *Provider) CreateLoadBalancerWithContext(
	ctx context.Context, l *logger.Logger, vms vm.List, port int,
) error {
	// Set timeout for the entire load balancer creation (6 sequential operations)
	ctx, cancel := context.WithTimeout(ctx, 15*time.Minute)
	defer cancel()

	// Validate cluster is managed
	if !isManaged(vms) {
		return errors.New("load balancer creation is only supported for managed instance groups")
	}

	project := vms[0].Project
	clusterName, err := vms[0].ClusterName()
	if err != nil {
		return err
	}

	// Get instance groups for the cluster
	groups, err := p.listManagedInstanceGroups(ctx, project, instanceGroupName(clusterName))
	if err != nil {
		return err
	}
	if len(groups) == 0 {
		return errors.Errorf("no managed instance groups found for cluster %s", clusterName)
	}

	// Build standard labels for load balancer resources (only ForwardingRule supports labels)
	labels := buildLoadBalancerLabels(clusterName, port)

	// Step 1: Create TCP health check
	healthCheckName := loadBalancerResourceName(clusterName, port, "health-check")
	healthCheck := buildHealthCheckConfig(healthCheckName, port)

	healthCheckReq := &computepb.InsertHealthCheckRequest{
		Project:             project,
		HealthCheckResource: healthCheck,
	}

	op, err := p.computeHealthChecksClient.Insert(ctx, healthCheckReq)
	if err != nil {
		return errors.Wrapf(err, "failed to create health check %s", healthCheckName)
	}

	if err := op.Wait(ctx); err != nil {
		return errors.Wrapf(err, "failed to wait for health check creation %s", healthCheckName)
	}

	if opErr := op.Proto().GetError(); opErr != nil {
		return errors.Newf("health check creation failed for %s: %s", healthCheckName, opErr.String())
	}

	// Get the created health check to retrieve its self-link
	getHealthCheckReq := &computepb.GetHealthCheckRequest{
		Project:     project,
		HealthCheck: healthCheckName,
	}

	createdHealthCheck, err := p.computeHealthChecksClient.Get(ctx, getHealthCheckReq)
	if err != nil {
		return errors.Wrapf(err, "failed to get created health check %s", healthCheckName)
	}

	healthCheckSelfLink := createdHealthCheck.GetSelfLink()

	// Step 2: Create backend service
	loadBalancerName := loadBalancerResourceName(clusterName, port, "load-balancer")
	backendService := buildBackendServiceConfig(loadBalancerName, healthCheckSelfLink)

	backendServiceReq := &computepb.InsertBackendServiceRequest{
		Project:                project,
		BackendServiceResource: backendService,
	}

	op, err = p.computeBackendServicesClient.Insert(ctx, backendServiceReq)
	if err != nil {
		return errors.Wrapf(err, "failed to create backend service %s", loadBalancerName)
	}

	if err := op.Wait(ctx); err != nil {
		return errors.Wrapf(err, "failed to wait for backend service creation %s", loadBalancerName)
	}

	if opErr := op.Proto().GetError(); opErr != nil {
		return errors.Newf("backend service creation failed for %s: %s", loadBalancerName, opErr.String())
	}

	// Get the created backend service to retrieve its self-link
	getBackendServiceReq := &computepb.GetBackendServiceRequest{
		Project:        project,
		BackendService: loadBalancerName,
	}

	createdBackendService, err := p.computeBackendServicesClient.Get(ctx, getBackendServiceReq)
	if err != nil {
		return errors.Wrapf(err, "failed to get created backend service %s", loadBalancerName)
	}

	backendServiceSelfLink := createdBackendService.GetSelfLink()

	// Step 3: Add backends (instance groups) to the backend service
	// IMPORTANT: This must be done sequentially, not in parallel
	for _, group := range groups {
		// Note: Backend service expects the instance group URL, not the instance group manager URL
		// Instance Group Manager: .../instanceGroupManagers/name
		// Instance Group: .../instanceGroups/name (stored in InstanceGroup field)
		instanceGroupURL := group.GetInstanceGroup()

		backend := buildBackendConfig(instanceGroupURL)

		// Get the current backend service to retrieve its backends and fingerprint
		currentBackendService, err := p.computeBackendServicesClient.Get(ctx, getBackendServiceReq)
		if err != nil {
			return errors.Wrapf(err, "failed to get backend service %s for patching", loadBalancerName)
		}

		// Append the new backend to the existing backends
		updatedBackends := append(currentBackendService.GetBackends(), backend)

		// Patch the backend service with the updated backends
		patchReq := &computepb.PatchBackendServiceRequest{
			Project:        project,
			BackendService: loadBalancerName,
			BackendServiceResource: &computepb.BackendService{
				Backends:    updatedBackends,
				Fingerprint: currentBackendService.Fingerprint, // Required for optimistic locking
			},
		}

		op, err = p.computeBackendServicesClient.Patch(ctx, patchReq)
		if err != nil {
			return errors.Wrapf(err, "failed to add backend %s to service %s", instanceGroupURL, loadBalancerName)
		}

		if err := op.Wait(ctx); err != nil {
			return errors.Wrapf(err, "failed to wait for backend addition %s", instanceGroupURL)
		}

		if opErr := op.Proto().GetError(); opErr != nil {
			return errors.Newf("backend addition failed for %s: %s", instanceGroupURL, opErr.String())
		}
	}

	// Step 4: Create target TCP proxy
	proxyName := loadBalancerResourceName(clusterName, port, "proxy")
	proxy := buildTargetTcpProxyConfig(proxyName, backendServiceSelfLink)

	proxyReq := &computepb.InsertTargetTcpProxyRequest{
		Project:                project,
		TargetTcpProxyResource: proxy,
	}

	op, err = p.computeTargetTcpProxiesClient.Insert(ctx, proxyReq)
	if err != nil {
		return errors.Wrapf(err, "failed to create TCP proxy %s", proxyName)
	}

	if err := op.Wait(ctx); err != nil {
		return errors.Wrapf(err, "failed to wait for TCP proxy creation %s", proxyName)
	}

	if opErr := op.Proto().GetError(); opErr != nil {
		return errors.Newf("TCP proxy creation failed for %s: %s", proxyName, opErr.String())
	}

	// Get the created proxy to retrieve its self-link
	getProxyReq := &computepb.GetTargetTcpProxyRequest{
		Project:        project,
		TargetTcpProxy: proxyName,
	}

	createdProxy, err := p.computeTargetTcpProxiesClient.Get(ctx, getProxyReq)
	if err != nil {
		return errors.Wrapf(err, "failed to get created TCP proxy %s", proxyName)
	}

	proxySelfLink := createdProxy.GetSelfLink()

	// Step 5: Create forwarding rule
	forwardingRuleName := loadBalancerResourceName(clusterName, port, "forwarding-rule")
	forwardingRule := buildForwardingRuleConfig(forwardingRuleName, proxySelfLink, port, labels)

	forwardingRuleReq := &computepb.InsertGlobalForwardingRuleRequest{
		Project:                project,
		ForwardingRuleResource: forwardingRule,
	}

	op, err = p.computeGlobalForwardingRulesClient.Insert(ctx, forwardingRuleReq)
	if err != nil {
		return errors.Wrapf(err, "failed to create forwarding rule %s", forwardingRuleName)
	}

	if err := op.Wait(ctx); err != nil {
		return errors.Wrapf(err, "failed to wait for forwarding rule creation %s", forwardingRuleName)
	}

	if opErr := op.Proto().GetError(); opErr != nil {
		return errors.Newf("forwarding rule creation failed for %s: %s", forwardingRuleName, opErr.String())
	}

	// Step 6: Set named ports on instance groups (can be done in parallel)
	g := newLimitedErrorGroupWithContext(ctx)
	for _, group := range groups {
		groupName := group.GetName()
		groupZone := lastComponent(group.GetZone())

		namedPort := buildNamedPortConfig("cockroach", port)

		setNamedPortsReq := &computepb.SetNamedPortsInstanceGroupRequest{
			Project:       project,
			Zone:          groupZone,
			InstanceGroup: groupName,
			InstanceGroupsSetNamedPortsRequestResource: &computepb.InstanceGroupsSetNamedPortsRequest{
				NamedPorts: []*computepb.NamedPort{namedPort},
			},
		}

		g.GoCtx(func(ctx context.Context) error {
			op, err := p.computeInstanceGroupsClient.SetNamedPorts(ctx, setNamedPortsReq)
			if err != nil {
				return errors.Wrapf(err, "failed to set named ports on instance group %s", groupName)
			}

			if err := op.Wait(ctx); err != nil {
				return errors.Wrapf(err, "failed to wait for named ports setting on %s", groupName)
			}

			if opErr := op.Proto().GetError(); opErr != nil {
				return errors.Newf("named ports setting failed for %s: %s", groupName, opErr.String())
			}

			return nil
		})
	}

	return g.Wait()
}

// DeleteLoadBalancerWithContext deletes load balancer resources for a specific port using the GCP SDK.
// This matches the gcloud implementation which deletes resources in reverse dependency order.
func (p *Provider) DeleteLoadBalancerWithContext(
	ctx context.Context, l *logger.Logger, vms vm.List, port int,
) error {
	project := vms[0].Project
	clusterName, err := vms[0].ClusterName()
	if err != nil {
		return err
	}

	portFilter := strconv.Itoa(port)
	return p.deleteLoadBalancerResources(ctx, project, clusterName, portFilter)
}

// deleteLoadBalancerResources deletes load balancer resources for a cluster using the GCP SDK.
// This is the core deletion logic that can be called with just project and cluster name.
// The portFilter parameter is optional - if empty, all load balancers for the cluster are deleted.
//
// Server-side filtering is used to reduce the amount of data transferred from GCP.
// We filter by name prefix matching the cluster name pattern.
func (p *Provider) deleteLoadBalancerResources(
	ctx context.Context, project string, clusterName string, portFilter string,
) error {

	// Build name filter for server-side filtering
	// All load balancer resources follow the pattern: {cluster}-{port}-{resource-type}
	// We filter by cluster name prefix to reduce the data transferred
	nameFilter := buildLoadBalancerNameFilter(clusterName)

	// Step 1: List all load balancer resources in parallel with server-side filtering
	type lbResources struct {
		forwardingRules []*computepb.ForwardingRule
		proxies         []*computepb.TargetTcpProxy
		backendServices []*computepb.BackendService
		healthChecks    []*computepb.HealthCheck
	}

	var resources lbResources
	g := newLimitedErrorGroupWithContext(ctx)

	// List forwarding rules with name-based filtering
	g.GoCtx(func(ctx context.Context) error {
		req := &computepb.ListGlobalForwardingRulesRequest{
			Project: project,
			// TODO(golgeek): Forwarding rules support label-based filtering,
			// but we use name-based filtering because labels were previously not added
			// to forwarding rules created by roachprod. Once all forwarding rules
			// have the standard labels, we can switch to label-based filtering here.
			Filter: proto.String(nameFilter),
		}

		it := p.computeGlobalForwardingRulesClient.List(ctx, req)
		for {
			rule, err := it.Next()
			if errors.Is(err, iterator.Done) {
				break
			}
			if err != nil {
				return errors.Wrap(err, "failed to list forwarding rules")
			}

			resources.forwardingRules = append(resources.forwardingRules, rule)
		}
		return nil
	})

	// List TCP proxies with name-based filtering
	g.GoCtx(func(ctx context.Context) error {
		req := &computepb.ListTargetTcpProxiesRequest{
			Project: project,
			Filter:  proto.String(nameFilter),
		}

		it := p.computeTargetTcpProxiesClient.List(ctx, req)
		for {
			proxy, err := it.Next()
			if errors.Is(err, iterator.Done) {
				break
			}
			if err != nil {
				return errors.Wrap(err, "failed to list TCP proxies")
			}

			resources.proxies = append(resources.proxies, proxy)
		}
		return nil
	})

	// List backend services with name-based filtering
	g.GoCtx(func(ctx context.Context) error {
		req := &computepb.ListBackendServicesRequest{
			Project: project,
			Filter:  proto.String(nameFilter),
		}

		it := p.computeBackendServicesClient.List(ctx, req)
		for {
			service, err := it.Next()
			if errors.Is(err, iterator.Done) {
				break
			}
			if err != nil {
				return errors.Wrap(err, "failed to list backend services")
			}

			resources.backendServices = append(resources.backendServices, service)
		}
		return nil
	})

	// List health checks with name-based filtering
	g.GoCtx(func(ctx context.Context) error {
		req := &computepb.ListHealthChecksRequest{
			Project: project,
			Filter:  proto.String(nameFilter),
		}

		it := p.computeHealthChecksClient.List(ctx, req)
		for {
			check, err := it.Next()
			if errors.Is(err, iterator.Done) {
				break
			}
			if err != nil {
				return errors.Wrap(err, "failed to list health checks")
			}

			resources.healthChecks = append(resources.healthChecks, check)
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return err
	}

	// Step 2: Filter resources by cluster and port using existing loadBalancerNameParts function
	shouldExclude := func(name string, expectedResourceType string) bool {
		cluster, resourceType, resourcePort, ok := loadBalancerNameParts(name)
		if !ok || cluster != clusterName || resourceType != expectedResourceType {
			return true
		}
		if portFilter != "" && strconv.Itoa(resourcePort) != portFilter {
			return true
		}
		return false
	}

	var filteredRules []*computepb.ForwardingRule
	for _, rule := range resources.forwardingRules {
		if !shouldExclude(rule.GetName(), "forwarding-rule") {
			filteredRules = append(filteredRules, rule)
		}
	}

	var filteredProxies []*computepb.TargetTcpProxy
	for _, proxy := range resources.proxies {
		if !shouldExclude(proxy.GetName(), "proxy") {
			filteredProxies = append(filteredProxies, proxy)
		}
	}

	var filteredServices []*computepb.BackendService
	for _, service := range resources.backendServices {
		if !shouldExclude(service.GetName(), "load-balancer") {
			filteredServices = append(filteredServices, service)
		}
	}

	var filteredHealthChecks []*computepb.HealthCheck
	for _, check := range resources.healthChecks {
		if !shouldExclude(check.GetName(), "health-check") {
			filteredHealthChecks = append(filteredHealthChecks, check)
		}
	}

	// Step 3: Delete resources in correct order (sequential stages, parallel within stage)

	// Stage 1: Delete forwarding rules (parallel within stage)
	g = newLimitedErrorGroupWithContext(ctx)
	for _, rule := range filteredRules {
		ruleName := rule.GetName()

		g.GoCtx(func(ctx context.Context) error {
			req := &computepb.DeleteGlobalForwardingRuleRequest{
				Project:        project,
				ForwardingRule: ruleName,
			}

			op, err := p.computeGlobalForwardingRulesClient.Delete(ctx, req)
			if err != nil {
				return errors.Wrapf(err, "failed to delete forwarding rule %s", ruleName)
			}

			if err := op.Wait(ctx); err != nil {
				return errors.Wrapf(err, "failed to wait for forwarding rule deletion %s", ruleName)
			}

			if opErr := op.Proto().GetError(); opErr != nil {
				return errors.Newf("forwarding rule deletion failed for %s: %s", ruleName, opErr.String())
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	// Stage 2: Delete TCP proxies (parallel within stage)
	g = newLimitedErrorGroupWithContext(ctx)
	for _, proxy := range filteredProxies {
		proxyName := proxy.GetName()

		g.GoCtx(func(ctx context.Context) error {
			req := &computepb.DeleteTargetTcpProxyRequest{
				Project:        project,
				TargetTcpProxy: proxyName,
			}

			op, err := p.computeTargetTcpProxiesClient.Delete(ctx, req)
			if err != nil {
				return errors.Wrapf(err, "failed to delete TCP proxy %s", proxyName)
			}

			if err := op.Wait(ctx); err != nil {
				return errors.Wrapf(err, "failed to wait for TCP proxy deletion %s", proxyName)
			}

			if opErr := op.Proto().GetError(); opErr != nil {
				return errors.Newf("TCP proxy deletion failed for %s: %s", proxyName, opErr.String())
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	// Stage 3: Delete backend services (parallel within stage)
	g = newLimitedErrorGroupWithContext(ctx)
	for _, service := range filteredServices {
		serviceName := service.GetName()

		g.GoCtx(func(ctx context.Context) error {
			req := &computepb.DeleteBackendServiceRequest{
				Project:        project,
				BackendService: serviceName,
			}

			op, err := p.computeBackendServicesClient.Delete(ctx, req)
			if err != nil {
				return errors.Wrapf(err, "failed to delete backend service %s", serviceName)
			}

			if err := op.Wait(ctx); err != nil {
				return errors.Wrapf(err, "failed to wait for backend service deletion %s", serviceName)
			}

			if opErr := op.Proto().GetError(); opErr != nil {
				return errors.Newf("backend service deletion failed for %s: %s", serviceName, opErr.String())
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	// Stage 4: Delete health checks (parallel within stage)
	g = newLimitedErrorGroupWithContext(ctx)
	for _, check := range filteredHealthChecks {
		checkName := check.GetName()

		g.GoCtx(func(ctx context.Context) error {
			req := &computepb.DeleteHealthCheckRequest{
				Project:     project,
				HealthCheck: checkName,
			}

			op, err := p.computeHealthChecksClient.Delete(ctx, req)
			if err != nil {
				return errors.Wrapf(err, "failed to delete health check %s", checkName)
			}

			if err := op.Wait(ctx); err != nil {
				return errors.Wrapf(err, "failed to wait for health check deletion %s", checkName)
			}

			if opErr := op.Proto().GetError(); opErr != nil {
				return errors.Newf("health check deletion failed for %s: %s", checkName, opErr.String())
			}

			return nil
		})
	}

	return g.Wait()
}

// ListLoadBalancersWithContext lists all load balancers for the given cluster using the GCP SDK.
// This matches the gcloud implementation which lists forwarding rules and filters by cluster name.
func (p *Provider) ListLoadBalancersWithContext(
	ctx context.Context, l *logger.Logger, vms vm.List,
) ([]vm.ServiceAddress, error) {
	// Only managed instance groups support load balancers
	if !isManaged(vms) {
		return nil, nil
	}

	project := vms[0].Project
	clusterName, err := vms[0].ClusterName()
	if err != nil {
		return nil, err
	}

	// List all global forwarding rules
	req := &computepb.ListGlobalForwardingRulesRequest{
		Project: project,
	}

	it := p.computeGlobalForwardingRulesClient.List(ctx, req)

	var addresses []vm.ServiceAddress
	for {
		rule, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, errors.Wrap(err, "failed to list forwarding rules")
		}

		// Parse name using existing loadBalancerNameParts function
		cluster, resourceType, port, ok := loadBalancerNameParts(rule.GetName())
		if !ok {
			continue
		}

		if cluster == clusterName && resourceType == "forwarding-rule" {
			addresses = append(addresses, vm.ServiceAddress{
				IP:   rule.GetIPAddress(),
				Port: port,
			})
		}
	}

	return addresses, nil
}
