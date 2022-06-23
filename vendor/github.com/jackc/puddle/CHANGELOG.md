# 1.2.1 (December 2, 2021)

* TryAcquire now does not block when background constructing resource

# 1.2.0 (November 20, 2021)

* Add TryAcquire (A. Jensen)
* Fix: remove memory leak / unintentionally pinned memory when shrinking slices (Alexander Staubo)
* Fix: Do not leave pool locked after panic from nil context

# 1.1.4 (September 11, 2021)

* Fix: Deadlock in CreateResource if pool was closed during resource acquisition (Dmitriy Matrenichev)

# 1.1.3 (December 3, 2020)

* Fix: Failed resource creation could cause concurrent Acquire to hang. (Evgeny Vanslov)

# 1.1.2 (September 26, 2020)

* Fix: Resource.Destroy no longer removes itself from the pool before its destructor has completed.
* Fix: Prevent crash when pool is closed while resource is being created.

# 1.1.1 (April 2, 2020)

* Pool.Close can be safely called multiple times
* AcquireAllIDle immediately returns nil if pool is closed
* CreateResource checks if pool is closed before taking any action
* Fix potential race condition when CreateResource and Close are called concurrently. CreateResource now checks if pool is closed before adding newly created resource to pool.

# 1.1.0 (February 5, 2020)

* Use runtime.nanotime for faster tracking of acquire time and last usage time.
* Track resource idle time to enable client health check logic. (Patrick Ellul)
* Add CreateResource to construct a new resource without acquiring it. (Patrick Ellul)
* Fix deadlock race when acquire is cancelled. (Michael Tharp)
