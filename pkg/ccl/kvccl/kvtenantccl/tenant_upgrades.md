# Upgrading Tenants

What does it even mean to think about upgrading tenants?
The upgrade protocol for KV nodes has become well-defined
over the course of 21.1. It involves tight coordination between
nodes to gain invariants about what features will be used and
what migrations can and have been run. SQL pods are inherently
much more ephemeral and less able to perform this coordination.
Nevertheless, we need some way to migrate data in tenants and we
need some claims about the features the tenants may end up using.

* Tenants have their own system tables, including settings.
* System tables and initial data get bootstrapped in CreateTenant.
    * We should probably write out an initial version into the settings table
      at that point.
    * For new tenants what should we initialize this to?
        * The best that the system tenant can do is its
          active version.
    * For pre-existing tenants in the 20.2->21.1 upgrade, cope
      with the lack of a value gracefully.

## What does the upgrade dance even look like?
* I assume we want to upgrade the KV servers *before* we upgrade the
 tenant pods, at least, sometimes.
* Would we ever want to upgrade the tenant pods before the KV servers?
   * Current thinking is no.
* One other consideration is the handshake between SQL pods and KV
 nodes.
   * Once the KV cluster has its version finalized, new RPC connections
     from SQL pods with an old cluster version will not be permitted.
     Nothing will break the old ones.
   * This implies that we need to leave the KV cluster in the mixed
     version state while we upgrade the sql pods to the newest code
     version.
     * For 21.1 this is what we'll do.
   * It's not clear that this is acceptable; clients will want to control
     when their SQL semantic change, but some clients will want the full
     functionality of the new version before others. If we can't activate
     the new version on the KV cluster until all the SQL pods are running
     the new code, we can't really do this; not all behaviors are preserved
     in the mixed version state.
   * What if we allowed old SQL pods to connect to KV nodes? This, on its
     face, seems like it would totally violate the principles of the
     version gating as the SQL pods are just using the vanilla KV API.
     Nothing would prevent an old SQL pod from sending some deprecated
     and assumed to be fully eliminated command.
       * Maybe this is too pessimistic. Maybe instead we need to say that
         KV migrations need occur only with a minimum of a 1 version delay.
         Today that is the case.


## Goals

* Get a migration to run inside a tenant because of
  SET CLUSTER SETTING VERSION = ...
* Allow tenants to be running at one version below the KV node
  for at least this release.
* Ensure that we have some notion of cluster version in SQL pods.
* In future versions, prevent running SQL pods which are too old
  for their SQL state.
  
## Plan

* Phase 0:
   * Allow tenants to run the migration hook and set the cluster setting
     up to the active cluster version of the attached KV node.
   * Not supported:
       * Any reliable claims about the versions of other node.
   * Do not do anything to permit SQL pods to continue to operate at the old
     version.
* Phase 0.1:
   * Allow the RPC handshake to permit connections from nodes
     all the way back to the previous version state (the minimum
     binary supported version) if that connection indicates it is
     a secondary tenant connection.
        * In the long term we may want to think harder about this.
          It relates to ideas about KV protocol versioning.
        * One oddty here is that a SQL pod won't know its version until
          after it is able to read it from its keyspace.
           * It can broadcast its binary version, I guess.
   * Add some tooling for the system tenant to check to make sure
     tenants have some version activated.


