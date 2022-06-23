package prop

import (
	"reflect"

	"github.com/leanovate/gopter"
)

var typeOfError = reflect.TypeOf((*error)(nil)).Elem()

/*
ForAll creates a property that requires the check condition to be true for all values, if the
condition falsiies the generated values will be shrunk.

"condition" has to be a function with the same number of parameters as the provided
generators "gens". The function may return a simple bool (true means that the
condition has passed), a string (empty string means that condition has passed),
a *PropResult, or one of former combined with an error.
*/
func ForAll(condition interface{}, gens ...gopter.Gen) gopter.Prop {
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
		if result.Success() {
			for i, genResult := range genResults {
				result = result.AddArgs(gopter.NewPropArg(genResult, 0, values[i].Interface(), values[i].Interface()))
			}
		} else {
			for i, genResult := range genResults {
				nextResult, nextValue := shrinkValue(genParams.MaxShrinkCount, genResult, values[i].Interface(), result,
					func(v interface{}) *gopter.PropResult {
						shrunkOne := make([]reflect.Value, len(values))
						copy(shrunkOne, values)
						if v == nil {
							shrunkOne[i] = reflect.Zero(values[i].Type())
						} else {
							shrunkOne[i] = reflect.ValueOf(v)
						}
						return callCheck(shrunkOne)
					})
				result = nextResult
				if nextValue == nil {
					values[i] = reflect.Zero(values[i].Type())
				} else {
					values[i] = reflect.ValueOf(nextValue)
				}
			}
		}
		return result
	})
}

// ForAll1 legacy interface to be removed in the future
func ForAll1(gen gopter.Gen, check func(v interface{}) (interface{}, error)) gopter.Prop {
	checkFunc := func(v interface{}) *gopter.PropResult {
		return convertResult(check(v))
	}
	return gopter.SaveProp(func(genParams *gopter.GenParameters) *gopter.PropResult {
		genResult := gen(genParams)
		value, ok := genResult.Retrieve()
		if !ok {
			return &gopter.PropResult{
				Status: gopter.PropUndecided,
			}
		}
		result := checkFunc(value)
		if result.Success() {
			return result.AddArgs(gopter.NewPropArg(genResult, 0, value, value))
		}

		result, _ = shrinkValue(genParams.MaxShrinkCount, genResult, value, result, checkFunc)
		return result
	})
}

func shrinkValue(maxShrinkCount int, genResult *gopter.GenResult, origValue interface{},
	firstFail *gopter.PropResult, check func(interface{}) *gopter.PropResult) (*gopter.PropResult, interface{}) {
	lastFail := firstFail
	lastValue := origValue

	shrinks := 0
	shrink := genResult.Shrinker(lastValue).Filter(genResult.Sieve)
	nextResult, nextValue := firstFailure(shrink, check)
	for nextResult != nil && shrinks < maxShrinkCount {
		shrinks++
		lastValue = nextValue
		lastFail = nextResult

		shrink = genResult.Shrinker(lastValue).Filter(genResult.Sieve)
		nextResult, nextValue = firstFailure(shrink, check)
	}

	return lastFail.WithArgs(firstFail.Args).AddArgs(gopter.NewPropArg(genResult, shrinks, lastValue, origValue)), lastValue
}

func firstFailure(shrink gopter.Shrink, check func(interface{}) *gopter.PropResult) (*gopter.PropResult, interface{}) {
	value, ok := shrink()
	for ok {
		result := check(value)
		if !result.Success() {
			return result, value
		}
		value, ok = shrink()
	}
	return nil, nil
}
