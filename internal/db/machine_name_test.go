package db

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecordLocalMachineNameBackfillsLocalSessions(t *testing.T) {
	database := testDB(t)
	ctx := t.Context()
	insertSession(t, database, "local-row", "proj", func(s *Session) {
		s.Machine = "local"
	})
	insertSession(t, database, "owned-row", "proj", func(s *Session) {
		s.Machine = "install-a"
	})
	insertSession(t, database, "peer-row", "proj", func(s *Session) {
		s.Machine = "install-b"
		s.MachineName = "peer-laptop"
	})

	require.NoError(t, database.RecordLocalMachineName(ctx, "install-a", "desk"))

	local, err := database.GetSession(ctx, "local-row")
	require.NoError(t, err)
	require.NotNil(t, local)
	owned, err := database.GetSession(ctx, "owned-row")
	require.NoError(t, err)
	require.NotNil(t, owned)
	peer, err := database.GetSession(ctx, "peer-row")
	require.NoError(t, err)
	require.NotNil(t, peer)

	assert.Equal(t, "desk", local.MachineName)
	assert.Equal(t, "desk", owned.MachineName)
	assert.Equal(t, "peer-laptop", peer.MachineName)

	require.NoError(t, database.RecordLocalMachineName(ctx, "install-a", "renamed"))
	owned, err = database.GetSession(ctx, "owned-row")
	require.NoError(t, err)
	assert.Equal(t, "renamed", owned.MachineName)
	peer, err = database.GetSession(ctx, "peer-row")
	require.NoError(t, err)
	assert.Equal(t, "peer-laptop", peer.MachineName)
}

func TestUpsertSessionStampsLocalMachineName(t *testing.T) {
	database := testDB(t)
	ctx := t.Context()
	require.NoError(t, database.RecordLocalMachineName(ctx, "install-a", "desk"))

	require.NoError(t, database.UpsertSession(ctx, Session{
		ID: "fresh", Project: "proj", Machine: "install-a", Agent: "claude",
	}))
	require.NoError(t, database.UpsertSession(ctx, Session{
		ID: "peer", Project: "proj", Machine: "install-b", Agent: "claude",
		MachineName: "other-box",
	}))

	fresh, err := database.GetSession(ctx, "fresh")
	require.NoError(t, err)
	require.NotNil(t, fresh)
	peer, err := database.GetSession(ctx, "peer")
	require.NoError(t, err)
	require.NotNil(t, peer)
	assert.Equal(t, "desk", fresh.MachineName)
	assert.Equal(t, "other-box", peer.MachineName)
}

func TestMachineNameColumnMigratesOnOldArchive(t *testing.T) {
	path := filepath.Join(t.TempDir(), "old.db")
	database := testDBAtPath(t, path, "machine name migration")
	insertSession(t, database, "kept", "proj", func(s *Session) {
		s.Machine = "local"
	})
	require.NoError(t, database.Close())

	execRawSQLite(t, path, "ALTER TABLE sessions DROP COLUMN machine_name")
	reopened, err := Open(t.Context(), path)
	require.NoError(t, err)
	t.Cleanup(func() { reopened.Close() })

	var count int
	require.NoError(t, reopened.getReader().QueryRowContext(t.Context(),
		`SELECT count(*) FROM pragma_table_info('sessions') WHERE name = 'machine_name'`,
	).Scan(&count))
	assert.Equal(t, 1, count)

	require.NoError(t, reopened.RecordLocalMachineName(t.Context(), "install-a", "desk"))
	session, err := reopened.GetSession(t.Context(), "kept")
	require.NoError(t, err)
	require.NotNil(t, session)
	assert.Equal(t, "kept", session.ID)
	assert.Equal(t, "desk", session.MachineName)
}

func TestListSessionsFiltersByRecordedMachineName(t *testing.T) {
	database := testDB(t)
	ctx := t.Context()
	require.NoError(t, database.SetSyncState(ctx, MachineLabelKeyPrefix+"install-a", "desk"))
	require.NoError(t, database.RecordLocalMachineName(ctx, "install-a", "desk"))
	insertSession(t, database, "local-row", "proj", func(s *Session) {
		s.Machine = "install-a"
	})
	insertSession(t, database, "peer-row", "proj", func(s *Session) {
		s.Machine = "install-b"
		s.MachineName = "other-box"
	})

	resolved, err := ResolveMachineFilter(ctx, database, "desk")
	require.NoError(t, err)
	assert.Equal(t, "install-a", resolved)

	page, err := database.ListSessions(ctx, SessionFilter{Machine: resolved})
	require.NoError(t, err)
	require.Len(t, page.Sessions, 1)
	assert.Equal(t, "local-row", page.Sessions[0].ID)
	assert.Equal(t, "desk", page.Sessions[0].MachineName)
}
