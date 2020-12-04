package eventpb

import (
	"encoding/json"
	"testing"
)

func TestEvents(t *testing.T) {
	e := &EventCreateDatabase{CommonEventDetails{User: "hello"}}
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}

	t.Errorf("WOO %s", b)
}
