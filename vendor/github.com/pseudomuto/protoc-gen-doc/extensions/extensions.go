// Package extensions implements a system for working with extended options.
package extensions

// Transformer functions for transforming payloads of an extension option into
// something that can be rendered by a template.
type Transformer func(payload interface{}) interface{}

var transformers = make(map[string]Transformer)

// SetTransformer sets the transformer function for the given extension name
func SetTransformer(extensionName string, f Transformer) {
	transformers[extensionName] = f
}

// Transform the extensions using the registered transformers.
func Transform(extensions map[string]interface{}) map[string]interface{} {
	if extensions == nil {
		return nil
	}
	out := make(map[string]interface{}, len(extensions))
	for name, payload := range extensions {
		transform, ok := transformers[name]
		if !ok {
			// No transformer registered, skip.
			continue
		}
		transformedPayload := transform(payload)
		if transformedPayload == nil {
			// Transformer returned nothing, skip.
			continue
		}
		out[name] = transformedPayload
	}
	return out
}
