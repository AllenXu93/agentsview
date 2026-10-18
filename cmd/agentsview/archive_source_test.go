package main

import (
	"bytes"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/config"
	"go.kenn.io/agentsview/internal/db"
	"go.kenn.io/agentsview/internal/dbtest"
	"go.kenn.io/agentsview/internal/rawarchive"
	"go.kenn.io/agentsview/internal/service"
	"go.kenn.io/agentsview/internal/sync"
	"go.kenn.io/agentsview/internal/testjsonl"
)

func TestArchiveSourceWorkerRebuildAfterStartupSchemaUpgrade(t *testing.T) {
	cfg := testConfigWithClaudeFixture(t)
	database, err := db.Open(t.Context(), cfg.DBPath)
	require.NoError(t, err)
	engine := sync.NewEngine(t.Context(), database, workerEngineConfig(cfg))
	require.Equal(t, 3, engine.SyncAll(t.Context(), nil).Synced)
	engine.Close()
	require.NoError(t, database.Update(t.Context(), func(tx *sql.Tx) error {
		_, err := tx.ExecContext(t.Context(), `DROP TABLE archive_session_provenance; DROP TABLE archive_attachment_sources; DROP TABLE archive_source_bindings; DROP TABLE archive_attachments`)
		return err
	}))
	require.NoError(t, database.Close())
	// The daemon upgrades schema before handing a read-only archive to its worker.
	database, lock, err := openWriteDB(t.Context(), cfg)
	require.NoError(t, err)
	closeWriteDB(database, lock)
	var output bytes.Buffer
	require.NoError(t, runSyncWorker(cfg, syncWorkerRequest{Mode: "resync-build"}, &output))
	assert.Equal(t, 3, decodeSingleResult(t, &output).Synced)
}

func archiveSourceCommandFixture(t *testing.T) config.ArchiveSource {
	t.Helper()
	inputs := t.TempDir()
	data := filepath.Join(inputs, "source")
	dbtest.WriteTestFile(t, filepath.Join(data, "telemetry-install-id"), []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	database, err := db.OpenIsolatedContext(t.Context(), filepath.Join(data, "sessions.db"))
	require.NoError(t, err)
	require.NoError(t, database.Close())
	root := filepath.Join(inputs, "claude")
	dbtest.WriteTestFile(t, filepath.Join(root, "project", "saved.jsonl"), []byte(testjsonl.NewSessionBuilder().AddClaudeUserWithSessionID("2026-01-01T00:00:00Z", "archived message", "saved").String()))
	capture := filepath.Join(inputs, "capture")
	settings := rawarchive.RecoverySettings{LocalMachineName: "retired-desktop"}
	_, err = rawarchive.Capture(t.Context(), rawarchive.CaptureOptions{DataDir: data, Destination: capture, Settings: settings, Roots: []rawarchive.RootSpec{{Provider: "claude", Path: root}}})
	require.NoError(t, err)
	seed := filepath.Join(inputs, "seed")
	_, err = rawarchive.Seed(t.Context(), filepath.Join(capture, "capture.json"), seed, nil)
	require.NoError(t, err)
	database, err = db.OpenIsolatedContext(t.Context(), filepath.Join(seed, "sessions.db"))
	require.NoError(t, err)
	archive, err := rawarchive.Open(t.Context(), database, seed, nil)
	require.NoError(t, err)
	repo := filepath.Join(t.TempDir(), "repository")
	report, err := archive.Backup(t.Context(), repo, settings, "test")
	require.NoError(t, err)
	require.NoError(t, archive.Close())
	require.NoError(t, database.Close())
	require.NoError(t, os.RemoveAll(inputs))
	return config.ArchiveSource{Name: "history", Repository: repo, Snapshot: report.SnapshotID}
}

func TestArchiveSourceDirectSyncAndWorkerRebuild(t *testing.T) {
	source := archiveSourceCommandFixture(t)
	cfg := testConfigWithClaudeFixture(t)
	cfg.ArchiveSources = []config.ArchiveSource{source}
	database, err := db.OpenIsolatedContext(t.Context(), cfg.DBPath)
	require.NoError(t, err)
	_, err = database.EnsureInstallationIdentity(t.Context(), cfg.InstallationID)
	require.NoError(t, err)
	_, stats, err := runLocalSyncResult(t.Context(), cfg, database, false)
	require.NoError(t, err)
	assert.Equal(t, 4, stats.Synced, "the command result includes attached history")
	const id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa~saved"
	before, err := database.GetAllMessages(t.Context(), id)
	require.NoError(t, err)
	require.Len(t, before, 1)
	require.NoError(t, database.Close())
	var output bytes.Buffer
	require.NoError(t, runSyncWorker(cfg, syncWorkerRequest{Mode: "resync-build"}, &output))
	result := decodeSingleResult(t, &output)
	assert.Equal(t, 4, result.Synced, "both local and attached sources are rebuilt")
	replacement, err := db.OpenReadOnly(t.Context(), cfg.DBPath+"-resync")
	require.NoError(t, err)
	defer replacement.Close()
	messages, err := replacement.GetAllMessages(t.Context(), id)
	require.NoError(t, err)
	require.Len(t, messages, 1)
	assert.Equal(t, "archived message", messages[0].Content)
	identity, err := replacement.GetSessionFull(t.Context(), id)
	require.NoError(t, err)
	require.NotNil(t, identity)
	assert.Equal(t, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", identity.Machine)
}

func TestArchiveSourceSessionRefreshReportsRequiredCommand(t *testing.T) {
	source := archiveSourceCommandFixture(t)
	cfg := testConfigWithClaudeFixture(t)
	cfg.ArchiveSources = []config.ArchiveSource{source}
	database, err := db.OpenIsolatedContext(t.Context(), cfg.DBPath)
	require.NoError(t, err)
	defer database.Close()
	_, _, err = runLocalSyncResult(t.Context(), cfg, database, false)
	require.NoError(t, err)
	engine := sync.NewEngine(t.Context(), database, localSyncEngineConfig(cfg))
	defer engine.Close()
	backend := service.NewDirectBackend(database, engine)
	const id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa~saved"
	before, err := backend.Get(t.Context(), id)
	require.NoError(t, err)
	for _, subagents := range []bool{false, true} {
		_, err := backend.Sync(t.Context(), service.SyncInput{ID: id, Subagents: subagents})
		require.ErrorContains(t, err, "sync --full")
	}
	after, err := backend.Get(t.Context(), id)
	require.NoError(t, err)
	assert.Equal(t, before, after)
}
