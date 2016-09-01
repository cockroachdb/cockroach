- Feature Name: Store Pool
- Status: completed
- Start Date: 2015-08-20
- RFC PR: [#2286](https://github.com/cockroachdb/cockroach/pull/2286),
          [#2336](https://github.com/cockroachdb/cockroach/pull/2336)
- Cockroach Issue: [#2149](https://github.com/cockroachdb/cockroach/issues/2149),
                   [#620](https://github.com/cockroachdb/cockroach/issues/620)

# Summary

Add a new `StorePool` service on each node that monitors all the stores and
reports on their current status and health. Based on just a store ID, the
pool will report the health of the store. Initially this health will only
be if the store is dead or not, but will expand to include other factors in the
future. This will also be the ideal location to add any calculations about
which store would be best suited to take on a new replica, subsuming some of
the work from the allocator.

This new service will work perfectly with #2153 and #2171

# Motivation

The decisions about when to add/remove replicas for rebalancing and repairing
require the knowledge about the health of other stores. There needs to be a
local source of truth for those decisions.

# Detailed design

## Configuration
Add a new configuration setting called `TimeUntilStoreDead` which contains
the number of seconds after which if a store was not heard from, it is
considered dead. The default value for this will be 5 minutes.

## Monitor
Add a new service called `StorePool` that starts when the node is started.
This new service will run until the stopper is called and have access to
gossip.

`StorePool` will maintain a map of store IDs to store descriptors and a variety
of heath statistic about the store. It will also maintain a `lastUpdatedTime`
which will be set whenever a store descriptor is updated. When this happens,
if the store was previously marked as dead, it will restored. To maintain this
map, a callback from gossip for store descriptors will be added. When this
`lastUpdatedTime` is longer than the `TimeUntilStoreDead`, the store is
considered dead and any replicas on this store may be removed. Note that that
the work to remove replicas is performed elsewhere.

Monitor will maintain a timespan `timeUntilNextDead` which is calculated by
taking the nearest `lastUpdatedTime` for all the stores and adding
`TimeUntilStoreDead` and the store ID associated with the timeout.

Monitor will trigger on `timeUntilNextDead` which when triggered checks to see
if that store has not been updated.
If the store hasn't been updated, it will mark it as dead.
Then it will calculate the next `timeUntilNextDead` to wake up the service.

# Drawbacks

Can't think of any right now. Perhaps that we're adding a new service, but it
should be very lightweight.

# Alternatives

1. Instead of creating a new store monitoring service, add all of this into
   gossip. Gossip already has most of the store information in it.
   - Gossip requires a good refactoring and I can see this as one of the first
   steps to do so. This will mean that you can get store lists from somewhere
   other than gossip and it will include more details as well. Adding these
   calculations into gossip seems cumbersome.
2. Instead of creating a store pool, create a node pool. This will allow each
   node to choose which store of theirs the new range should be assigned to and
   give the nodes more control over their internal systems.
   - Right now, this is the wrong direction, but in the longer team, giving the
   node more control might simplify the decision making for allocations,
   repairs and rebalances and each node can report their own version of
   capacity and free space.

# Unresolved questions

If RFCs #2153 and #2171 aren't implemented, should we consider another option?


