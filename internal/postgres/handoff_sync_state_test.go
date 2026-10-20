package postgres

import (
	"path/filepath"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.kenn.io/agentsview/internal/db"
)

// A daemon push runs outside the sync lock, so a worker handoff can close the
// archive writer while the push saves its progress. The save must wait for
// the writer instead of failing the push.
func TestHandoffSyncStateWaitsForTheWriter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		local, err := db.Open(t.Context(), filepath.Join(t.TempDir(), "local.db"))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, local.Close()) })
		require.NoError(t, local.CloseWriter())

		done := make(chan error, 1)
		go func() {
			done <- handoffSyncState{local}.SetSyncState(t.Context(), "last_push_at", "cutoff")
		}()
		synctest.Wait()
		select {
		case err := <-done:
			require.Failf(t, "saved while the writer was closed", "err: %v", err)
		default:
		}
		require.NoError(t, local.ReopenWriter())
		require.NoError(t, <-done)

		got, err := local.GetSyncState(t.Context(), "last_push_at")
		require.NoError(t, err)
		assert.Equal(t, "cutoff", got)
	})
}

// The first push from an archive creates its push marker ID. A worker handoff
// at that moment must not fail the push.
func TestPushMarkerIDWaitsForTheWriter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		local, err := db.Open(t.Context(), filepath.Join(t.TempDir(), "local.db"))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, local.Close()) })
		require.NoError(t, local.CloseWriter())

		type created struct {
			id  string
			err error
		}
		done := make(chan created, 1)
		go func() {
			id, err := (&Sync{local: local}).pushMarkerID(t.Context())
			done <- created{id, err}
		}()
		synctest.Wait()
		select {
		case got := <-done:
			require.Failf(t, "created the ID while the writer was closed", "err: %v", got.err)
		default:
		}
		require.NoError(t, local.ReopenWriter())
		got := <-done
		require.NoError(t, got.err)

		stored, err := local.GetSyncState(t.Context(), pushMarkerIDStateKey)
		require.NoError(t, err)
		assert.Equal(t, stored, got.id)
		assert.NotEmpty(t, got.id)
	})
}
