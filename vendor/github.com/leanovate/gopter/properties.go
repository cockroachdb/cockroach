package gopter

import "testing"

// Properties is a collection of properties that should be checked in a test
type Properties struct {
	parameters *TestParameters
	props      map[string]Prop
	propNames  []string
}

// NewProperties create new Properties with given test parameters.
// If parameters is nil default test parameters will be used
func NewProperties(parameters *TestParameters) *Properties {
	if parameters == nil {
		parameters = DefaultTestParameters()
	}
	return &Properties{
		parameters: parameters,
		props:      make(map[string]Prop, 0),
		propNames:  make([]string, 0),
	}
}

// Property add/defines a property in a test.
func (p *Properties) Property(name string, prop Prop) {
	p.propNames = append(p.propNames, name)
	p.props[name] = prop
}

// Run checks all definied propertiesand reports the result
func (p *Properties) Run(reporter Reporter) bool {
	success := true
	for _, propName := range p.propNames {
		prop := p.props[propName]

		result := prop.Check(p.parameters)

		reporter.ReportTestResult(propName, result)
		if !result.Passed() {
			success = false
		}
	}
	return success
}

// TestingRun checks all definied properties with a testing.T context.
// This the preferred wait to run property tests as part of a go unit test.
func (p *Properties) TestingRun(t *testing.T, opts ...interface{}) {
	reporter := ConsoleReporter(true)
	for _, opt := range opts {
		if r, ok := opt.(Reporter); ok {
			reporter = r
		}
	}
	if !p.Run(reporter) {
		t.Errorf("failed with initial seed: %d", p.parameters.Seed)
	}
}
