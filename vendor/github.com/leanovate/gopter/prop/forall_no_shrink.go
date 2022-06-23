package prop

import (
	"reflect"

	"github.com/leanovate/gopter"
)

/*
ForAllNoShrink creates a property that requires the check condition to be true for all values.
As the name suggests the generated values will not be shrunk if the condition falsiies.

"condition" has to be a function with the same number of parameters as the provided
generators "gens". The function may return a simple bool (true means that the
condition has passed), a string (empty string means that condition has passed),
a *PropResult, or one of former combined with an error.
*/
func ForAllNoShrink(condition interface{}, gens ...gopter.Gen) gopter.Prop {
	callCheck, err := checkConditionFunc(condition, len(gens))
	if err != nil {
		return ErrorProp(err)
	}

	return gopter.SaveProp(func(genParams *gopter.GenParameters) *gopter.PropResult {
		genResults := make([]*gopter.GenResult, len(gens))
		values := make([]reflect.Value, len(gens))
		var ok bool
		for i, gen := range gens {
			result := gen(genParams)
			genResults[i] = result
			values[i], ok = result.RetrieveAsValue()
			if !ok {
				return &gopter.PropResult{
					Status: gopter.PropUndecided,
				}
			}
		}
		result := callCheck(values)
		for i, genResult := range genResults {
			result = result.AddArgs(gopter.NewPropArg(genResult, 0, values[i].Interface(), values[i].Interface()))
		}
		return result
	})
}

// ForAllNoShrink1 creates a property that requires the check condition to be true for all values
// As the name suggests the generated values will not be shrunk if the condition falsiies
func ForAllNoShrink1(gen gopter.Gen, check func(interface{}) (interface{}, error)) gopter.Prop {
	return gopter.SaveProp(func(genParams *gopter.GenParameters) *gopter.PropResult {
		genResult := gen(genParams)
		value, ok := genResult.Retrieve()
		if !ok {
			return &gopter.PropResult{
				Status: gopter.PropUndecided,
			}
		}
		return convertResult(check(value)).AddArgs(gopter.NewPropArg(genResult, 0, value, value))
	})
}
