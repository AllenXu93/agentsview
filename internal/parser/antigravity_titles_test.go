package parser

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/testjsonl"
)

func TestAntigravityTitleCacheExpiresMatchingMarkers(t *testing.T) {
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	priorNow := antigravityTitleNow
	antigravityTitleNow = func() time.Time { return now }
	t.Cleanup(func() { antigravityTitleNow = priorNow })
	store := filepath.Join(t.TempDir(), AntigravitySummariesDBName)
	conn, err := sql.Open("sqlite3", store)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	_, err = conn.ExecContext(t.Context(), testjsonl.AntigravityTitleSchema)
	require.NoError(t, err)
	_, err = conn.ExecContext(t.Context(), `PRAGMA journal_mode=WAL; INSERT INTO conversation_summaries VALUES ('conversation-a', 'First')`)
	require.NoError(t, err)
	checkpoint := func() {
		_, err := conn.ExecContext(t.Context(), "PRAGMA wal_checkpoint(TRUNCATE)")
		require.NoError(t, err)
		for _, path := range []string{store, store + "-wal"} {
			require.NoError(t, os.Chtimes(path, now, now))
		}
	}
	checkpoint()
	titles, _, err := AntigravityStoreTitles(t.Context(), store)
	require.NoError(t, err)
	require.Equal(t, "First", titles["conversation-a"])
	before, ok := StatSQLiteContainerState(store)
	require.True(t, ok)
	_, err = conn.ExecContext(t.Context(), "UPDATE conversation_summaries SET title = 'Later'")
	require.NoError(t, err)
	checkpoint()
	after, ok := StatSQLiteContainerState(store)
	require.True(t, ok)
	require.Equal(t, before, after)
	titles, _, err = AntigravityStoreTitles(t.Context(), store)
	require.NoError(t, err)
	assert.Equal(t, "First", titles["conversation-a"])
	now = now.Add(SQLiteContainerDigestVerifyInterval)
	titles, _, err = AntigravityStoreTitles(t.Context(), store)
	require.NoError(t, err)
	assert.Equal(t, "Later", titles["conversation-a"])
}
