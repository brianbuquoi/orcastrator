package store_test

import (
	"testing"

	"github.com/brianbuquoi/orcastrator/internal/store"
	"github.com/brianbuquoi/orcastrator/internal/store/memory"
)

func TestMemoryStoreConformance(t *testing.T) {
	t.Parallel()
	RunConformanceTests(t, func() store.Store {
		return memory.New()
	})
}
