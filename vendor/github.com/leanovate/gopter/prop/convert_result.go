package prop

import (
	"fmt"

	"github.com/leanovate/gopter"
)

func convertResult(result interface{}, err error) *gopter.PropResult {
	if err != nil {
		return &gopter.PropResult{
			Status: gopter.PropError,
			Error:  err,
		}
	}
	switch result.(type) {
	case bool:
		if result.(bool) {
			return &gopter.PropResult{Status: gopter.PropTrue}
		}
		return &gopter.PropResult{Status: gopter.PropFalse}
	case string:
		if result.(string) == "" {
			return &gopter.PropResult{Status: gopter.PropTrue}
		}
		return &gopter.PropResult{
			Status: gopter.PropFalse,
			Labels: []string{result.(string)},
		}
	case *gopter.PropResult:
		return result.(*gopter.PropResult)
	}
	return &gopter.PropResult{
		Status: gopter.PropError,
		Error:  fmt.Errorf("Invalid check result: %#v", result),
	}
}
