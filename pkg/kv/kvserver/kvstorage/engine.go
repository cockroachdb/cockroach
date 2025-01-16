package kvstorage

import "github.com/cockroachdb/cockroach/pkg/storage"

// SeparatedEngine is a stepping stone for separating the raft log and state
// machine storages. It allows callers of NewStore to pass in "two engines"
// masquerading as one. This is not the intended end state, where it will most
// likely make sense to pass in two engines. But for prototyping an
// experimentation, this approach avoids large amounts of refactoring.
type SeparatedEngine interface {
	storage.Engine
	LogEngine() storage.Engine
}
