package ccl_testing

type testValues struct {
	values map[string]interface{}
}

var singleton *testValues

// Get does a get.
func Get(key string) interface{} {
	return singleton.values[key]
}

// Set does a set.
func Set(key string, value interface{}) {
	singleton.values[key] = value
}

// init is called only once.
func init() {
	singleton = &testValues{
		values: make(map[string]interface{}),
	}
}
