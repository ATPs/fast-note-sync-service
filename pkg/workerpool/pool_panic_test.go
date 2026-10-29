package workerpool

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

// A panicking task must be reported as an error and must not kill the pool or the process.
func TestPool_SubmitPanicTaskReturnsError(t *testing.T) {
	pool := New(&Config{MaxWorkers: 2, QueueSize: 10}, nil)
	defer pool.Shutdown(context.Background())

	err := pool.Submit(context.Background(), func(ctx context.Context) error {
		panic("boom")
	})
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "panic")
		assert.Contains(t, err.Error(), "boom")
	}

	// nil-pointer dereference panic (the real-world failure mode) is also recovered
	err = pool.Submit(context.Background(), func(ctx context.Context) error {
		var m map[string]int
		m["x"] = 1
		return nil
	})
	assert.Error(t, err)

	// Pool keeps processing subsequent tasks, on every worker (more tasks than panics * workers)
	var counter atomic.Int32
	for i := 0; i < 10; i++ {
		err := pool.Submit(context.Background(), func(ctx context.Context) error {
			counter.Add(1)
			return nil
		})
		assert.NoError(t, err)
	}
	assert.Equal(t, int32(10), counter.Load())
	assert.False(t, pool.IsClosed())
}

// Panics in SubmitAsync tasks must not crash the process and must not shrink the worker set.
func TestPool_SubmitAsyncPanicTaskDoesNotCrash(t *testing.T) {
	pool := New(&Config{MaxWorkers: 1, QueueSize: 10}, nil)
	defer pool.Shutdown(context.Background())

	for i := 0; i < 3; i++ {
		assert.NoError(t, pool.SubmitAsync(context.Background(), func(ctx context.Context) error {
			panic("async boom")
		}))
	}

	done := make(chan struct{})
	assert.NoError(t, pool.SubmitAsync(context.Background(), func(ctx context.Context) error {
		close(done)
		return nil
	}))
	<-done
}
