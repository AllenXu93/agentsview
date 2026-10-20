// Package lockctx takes a mutex but stops waiting when a context ends.
package lockctx

import (
	"context"
	"sync"
)

// Lock takes mu in the lock's normal queue, so the caller is not starved by
// back-to-back holders, and returns ctx.Err() if ctx ends first. On success
// the caller owns mu and must unlock it. A wait that gives up leaves a
// goroutine that unlocks mu once it is granted.
func Lock(ctx context.Context, mu sync.Locker) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	locked := make(chan struct{})
	go func() {
		mu.Lock()
		close(locked)
	}()
	select {
	case <-locked:
		return nil
	case <-ctx.Done():
		go func() {
			<-locked
			mu.Unlock()
		}()
		return ctx.Err()
	}
}
