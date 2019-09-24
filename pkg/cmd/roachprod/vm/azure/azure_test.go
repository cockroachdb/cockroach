package azure

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test(t *testing.T) {
	a := assert.New(t)
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Minute)

	p := New()
	_, err := p.createVNets(ctx, []string{"eastus2", "westus", "centralus"})
	a.NoError(err)
}
