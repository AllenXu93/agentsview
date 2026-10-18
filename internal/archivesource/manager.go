package archivesource

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"go.kenn.io/agentsview/internal/config"
	"go.kenn.io/agentsview/internal/db"
	syncer "go.kenn.io/agentsview/internal/sync"
	"go.kenn.io/docbank/docbank"
)

type drainRun struct {
	done chan struct{}
	err  error
}

// Manager owns one drain for a receiving database. A caller's cancellation ends
// its wait, while the owner's context controls the shared work.
type Manager struct {
	ctx          context.Context
	cancel       context.CancelFunc
	cfg          config.Config
	database     *db.DB
	engine       *syncer.Engine
	beforeSource func(context.Context) error
	mu           sync.Mutex
	active       *drainRun
}

func NewManager(ownerCtx context.Context, cfg config.Config, database *db.DB, engine *syncer.Engine, beforeSource func(context.Context) error) (*Manager, error) {
	if err := cfg.ValidateArchiveSources(); err != nil {
		return nil, err
	}
	if len(cfg.ArchiveSources) > 0 {
		only, err := database.IsArchiveOnly(ownerCtx)
		if err != nil {
			return nil, err
		}
		if only {
			return nil, db.ErrArchiveOnly
		}
	}
	ctx, cancel := context.WithCancel(ownerCtx)
	return &Manager{ctx: ctx, cancel: cancel, cfg: cfg, database: database, engine: engine, beforeSource: beforeSource}, nil
}

func (m *Manager) Close() {
	m.cancel()
	m.mu.Lock()
	run := m.active
	m.mu.Unlock()
	if run != nil {
		<-run.done
	}
}

func (m *Manager) Drain(ctx context.Context, progress syncer.ProgressFunc) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(m.cfg.ArchiveSources) == 0 {
		return nil
	}
	m.mu.Lock()
	run := m.active
	if run == nil {
		run = &drainRun{done: make(chan struct{})}
		m.active = run
		go func() {
			err := m.drain(progress)
			m.mu.Lock()
			run.err = err
			m.active = nil
			close(run.done)
			m.mu.Unlock()
		}()
	}
	m.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-run.done:
		return run.err
	}
}

func (m *Manager) drain(progress syncer.ProgressFunc) error {
	var result error
	for _, source := range m.cfg.ArchiveSources {
		if err := m.ctx.Err(); err != nil {
			return errors.Join(result, err)
		}
		if err := m.drainAttachment(source, progress); err != nil {
			result = errors.Join(result, fmt.Errorf("archive source %q unavailable; retry on the next sync: %w", source.Name, err))
		}
	}
	return result
}

func (m *Manager) drainAttachment(cfg config.ArchiveSource, progress syncer.ProgressFunc) (retErr error) {
	var index *Index
	err := m.engine.RunExclusive(func() error {
		var err error
		index, err = OpenIndex(m.ctx, cfg, filepath.Join(m.cfg.DataDir, "archive-sources"), indexProgress(progress))
		if err != nil {
			return err
		}
		return index.register(m.ctx, m.database, m.cfg)
	})
	if index != nil {
		defer func() { retErr = errors.Join(retErr, index.Close()) }()
	}
	if err != nil {
		return err
	}
	if index == nil {
		return errors.New("archive source index was not opened")
	}
	key := db.ArchiveAttachmentKey{RepositoryID: index.Info.RepositoryID, SnapshotID: index.Info.SnapshotID}
	status, err := m.database.ArchiveAttachmentStatus(m.ctx, key)
	if err != nil || status.Pending == 0 {
		return err
	}
	scratch := filepath.Join(m.cfg.DataDir, "archive-sources", "scratch")
	if err := os.MkdirAll(scratch, 0o700); err != nil {
		return err
	}
	var after db.ArchiveSourceIdentity
	for {
		pending, err := m.database.PendingArchiveSources(m.ctx, key, after, 128)
		if err != nil {
			return errors.Join(retErr, err)
		}
		if len(pending) == 0 {
			return retErr
		}
		for _, item := range pending {
			if err := m.ctx.Err(); err != nil {
				return errors.Join(retErr, err)
			}
			if m.beforeSource != nil {
				if err := m.beforeSource(m.ctx); err != nil {
					return errors.Join(retErr, err)
				}
			}
			source, err := index.source(m.ctx, item.Provenance.ManifestID)
			if err == nil {
				_, err = index.syncSource(m.ctx, cfg.Repository, source, m.cfg, m.engine, scratch, item.Target, progress)
			}
			if err != nil {
				recordErr := m.engine.RunExclusive(func() error { return m.database.RecordArchiveSourceError(m.ctx, item.Provenance, err.Error()) })
				retErr = errors.Join(retErr, err, recordErr)
				if errors.Is(err, docbank.ErrBackupRepositoryLocked) {
					return retErr
				}
			}
			after = item.Provenance.Source
		}
	}
}

func indexProgress(progress syncer.ProgressFunc) func(string) {
	if progress == nil {
		return nil
	}
	return func(detail string) { progress(syncer.Progress{Phase: syncer.PhaseDiscovering, Detail: detail}) }
}
