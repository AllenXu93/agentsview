package archivesource

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/config"
	"go.kenn.io/agentsview/internal/db"
	"go.kenn.io/agentsview/internal/dbtest"
	"go.kenn.io/agentsview/internal/parser"
	"go.kenn.io/agentsview/internal/rawarchive"
	syncer "go.kenn.io/agentsview/internal/sync"
	"go.kenn.io/agentsview/internal/testjsonl"
	"go.kenn.io/docbank/docbank"
	"go.kenn.io/kit/backup"
)

func TestArchiveDrainSingleFlight(t *testing.T) {
	source, inputs := archiveFixture(t)
	require.NoError(t, os.RemoveAll(inputs))
	receiver := dbtest.OpenTestDB(t)
	cfg := config.Config{DataDir: filepath.Dir(receiver.Path()), ArchiveSources: []config.ArchiveSource{source}, ArchiveContent: config.ArchiveContentFull}
	engine := syncer.NewEngine(t.Context(), receiver, syncer.EngineConfig{Ephemeral: true})
	defer engine.Close()
	boundary := make(chan struct{})
	release := make(chan struct{})
	var copies atomic.Int32
	manager, err := NewManager(t.Context(), cfg, receiver, engine, func(ctx context.Context) error {
		if copies.Add(1) == 1 {
			close(boundary)
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	require.NoError(t, err)
	defer manager.Close()
	finished := make(chan error, 1)
	go func() { finished <- manager.Drain(t.Context(), nil) }()
	<-boundary
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, manager.Drain(cancelled, nil), context.Canceled)
	close(release)
	require.NoError(t, <-finished)
	assert.Equal(t, int32(2), copies.Load())
	labels, err := receiver.GetMachineLabels(t.Context())
	require.NoError(t, err)
	assert.Equal(t, "retired-desktop", labels[sourceDevice])
	// A current pass must not copy or parse any source again.
	cacheFiles, err := filepath.Glob(filepath.Join(cfg.DataDir, "archive-sources", "*.db"))
	require.NoError(t, err)
	require.Len(t, cacheFiles, 1)
	index, err := OpenIndex(t.Context(), source, filepath.Dir(cacheFiles[0]), nil)
	require.NoError(t, err)
	key := db.ArchiveAttachmentKey{RepositoryID: index.Info.RepositoryID, SnapshotID: index.Info.SnapshotID}
	require.NoError(t, index.Close())
	require.NoError(t, manager.Drain(t.Context(), nil))
	assert.Equal(t, int32(2), copies.Load(), "unchanged pass performs no source copies")
	status, err := receiver.ArchiveAttachmentStatus(t.Context(), key)
	require.NoError(t, err)
	assert.Zero(t, status.Pending)
	offline := source.Repository + "-offline"
	require.NoError(t, os.Rename(source.Repository, offline))
	require.Error(t, manager.Drain(t.Context(), nil))
	session, err := receiver.GetSessionFull(t.Context(), sourceDevice+"~codex:019eb791-cf7d-75c1-8439-9ed74c122e02")
	require.NoError(t, err)
	require.NotNil(t, session)
}

func TestArchiveDrainRecordsFailuresAndContinues(t *testing.T) {
	source, _ := archiveFixture(t)
	receiver := dbtest.OpenTestDB(t)
	cfg := config.Config{DataDir: filepath.Dir(receiver.Path()), ArchiveSources: []config.ArchiveSource{source}}
	engine := syncer.NewEngine(t.Context(), receiver, syncer.EngineConfig{Ephemeral: true})
	defer engine.Close()
	localFailed := errors.New("local write failed")
	var handoffs atomic.Int32
	manager, err := NewManager(t.Context(), cfg, receiver, engine, func(context.Context) error {
		if handoffs.Add(1) == 1 {
			return localFailed
		}
		return nil
	})
	require.NoError(t, err)
	defer manager.Close()
	require.ErrorIs(t, manager.Drain(t.Context(), nil), localFailed)
	// Failed local reconciliation defers the archive rather than taking priority.
	require.NoError(t, manager.Drain(t.Context(), nil))
	assert.Equal(t, int32(3), handoffs.Load())
}

func TestArchiveDrainRefusesArchiveOnlyReceiver(t *testing.T) {
	receiver := dbtest.OpenTestDB(t)
	require.NoError(t, receiver.EnableArchiveOnly(t.Context()))
	engine := syncer.NewEngine(t.Context(), receiver, syncer.EngineConfig{Ephemeral: true})
	defer engine.Close()
	_, err := NewManager(t.Context(), config.Config{ArchiveSources: []config.ArchiveSource{{Name: "saved", Repository: "/missing", Snapshot: "pinned"}}}, receiver, engine, nil)
	require.ErrorIs(t, err, db.ErrArchiveOnly)
}

func TestArchiveDrainYieldsToPendingLocalWork(t *testing.T) {
	inputs := t.TempDir()
	root := filepath.Join(inputs, "claude")
	for _, id := range []string{"first", "second", "third"} {
		dbtest.WriteTestFile(t, filepath.Join(root, "project", id+".jsonl"), []byte(testjsonl.NewSessionBuilder().AddClaudeUserWithSessionID("2026-01-01T00:00:00Z", id, id).String()))
	}
	source := backupSourceRoots(t, inputs, []rawarchive.RootSpec{{Provider: "claude", Path: root}})
	require.NoError(t, os.RemoveAll(inputs))
	receiver := dbtest.OpenTestDB(t)
	local := filepath.Join(t.TempDir(), "project", "local.jsonl")
	cfg := config.Config{DataDir: filepath.Dir(receiver.Path()), ArchiveSources: []config.ArchiveSource{source}, ArchiveContent: config.ArchiveContentFull}
	engine := syncer.NewEngine(t.Context(), receiver, syncer.EngineConfig{Ephemeral: true, Machine: "receiver", AgentDirs: map[parser.AgentType][]string{parser.AgentClaude: {filepath.Dir(filepath.Dir(local))}}})
	defer engine.Close()
	dbtest.WriteTestFile(t, local, []byte(testjsonl.NewSessionBuilder().AddClaudeUserWithSessionID("2026-01-01T00:00:00Z", "new local message", "local").String()))
	var boundaries int
	manager, err := NewManager(t.Context(), cfg, receiver, engine, func(ctx context.Context) error {
		count := 0
		for _, id := range []string{sourceDevice + "~first", sourceDevice + "~second", sourceDevice + "~third", "local"} {
			messages, err := receiver.GetAllMessages(ctx, id)
			if err != nil {
				return err
			}
			count += len(messages)
		}
		if boundaries < 2 {
			assert.Equal(t, boundaries, count)
		} else {
			assert.Equal(t, boundaries+1, count)
		}
		if boundaries == 1 {
			if err := engine.ReparsePathsContext(ctx, []string{local}); err != nil {
				return err
			}
			session, err := receiver.GetSessionFull(ctx, "local")
			if err != nil {
				return err
			}
			if session == nil {
				return errors.New("local handoff did not publish")
			}
		}
		boundaries++
		return nil
	})
	require.NoError(t, err)
	defer manager.Close()
	require.NoError(t, manager.Drain(t.Context(), nil))
	assert.Equal(t, 3, boundaries)
}

func TestArchiveDrainDefersWhileBackupRuns(t *testing.T) {
	inputs := t.TempDir()
	root := filepath.Join(inputs, "claude")
	for _, id := range []string{"first", "second", "third"} {
		dbtest.WriteTestFile(t, filepath.Join(root, "project", id+".jsonl"), []byte(testjsonl.NewSessionBuilder().AddClaudeUserWithSessionID("2026-01-01T00:00:00Z", id, id).String()))
	}
	source := backupSourceRoots(t, inputs, []rawarchive.RootSpec{{Provider: "claude", Path: root}})
	repo, err := backup.Open(source.Repository)
	require.NoError(t, err)
	receiver := dbtest.OpenTestDB(t)
	engine := syncer.NewEngine(t.Context(), receiver, syncer.EngineConfig{Ephemeral: true})
	defer engine.Close()
	var lock *backup.RepoLock
	defer func() {
		if lock != nil {
			require.NoError(t, lock.Release())
		}
	}()
	handoffs := 0
	manager, err := NewManager(t.Context(), config.Config{DataDir: filepath.Dir(receiver.Path()), ArchiveSources: []config.ArchiveSource{source}}, receiver, engine, func(context.Context) error {
		handoffs++
		if handoffs == 2 {
			var err error
			lock, err = repo.AcquireExclusiveLock("backup", false)
			return err
		}
		return nil
	})
	require.NoError(t, err)
	defer manager.Close()
	require.ErrorIs(t, manager.Drain(t.Context(), nil), docbank.ErrBackupRepositoryLocked)
	assert.Equal(t, 2, handoffs, "defer the remaining sources while a backup owns the repository")
}
