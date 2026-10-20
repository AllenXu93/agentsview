package lockctx

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.kenn.io/agentsview/internal/testutil"
)

func TestLockTakesAFreeMutex(t *testing.T) {
	var mu sync.Mutex
	require.NoError(t, Lock(t.Context(), &mu))
	assert.False(t, mu.TryLock(), "Lock returned without holding the mutex")
	mu.Unlock()
}

func TestLockStopsWaitingAndReleasesTheMutexLater(t *testing.T) {
	var mu sync.Mutex
	mu.Lock()
	release := sync.OnceFunc(mu.Unlock)
	t.Cleanup(release)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	waited := make(chan error, 1)
	go func() { waited <- Lock(ctx, &mu) }()
	require.Eventually(t, func() bool {
		return testutil.GoroutineBlockedOnMutexIn("lockctx.Lock")
	}, 10*time.Second, 5*time.Millisecond)
	cancel()
	select {
	case err := <-waited:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		require.Fail(t, "the canceled caller kept waiting for the mutex")
	}

	release()
	// Let the abandoned wait take the mutex first, so the next Lock checks
	// that it gave the mutex back.
	require.Eventually(t, func() bool {
		return !testutil.GoroutineBlockedOnMutexIn("lockctx.Lock")
	}, 10*time.Second, 5*time.Millisecond)
	locked := make(chan error, 1)
	go func() { locked <- Lock(t.Context(), &mu) }()
	select {
	case err := <-locked:
		require.NoError(t, err)
		mu.Unlock()
	case <-time.After(5 * time.Second):
		require.Fail(t, "the abandoned wait kept the mutex")
	}
}

func TestLockRefusesAnEndedContext(t *testing.T) {
	var mu sync.Mutex
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, Lock(ctx, &mu), context.Canceled)
	assert.True(t, mu.TryLock(), "an ended context still took the mutex")
	mu.Unlock()
}
