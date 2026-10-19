package archivesource

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"

	"go.kenn.io/agentsview/internal/config"
	"go.kenn.io/agentsview/internal/db"
)

func attachmentTarget(cfg config.Config) (string, error) {
	claude, err := completionTarget(Source{Root: db.RawArchiveRoot{Provider: "claude"}}, cfg)
	if err != nil {
		return "", err
	}
	codex, err := completionTarget(Source{Root: db.RawArchiveRoot{Provider: "codex"}}, cfg)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256([]byte(claude+codex))), nil
}

func (i *Index) register(ctx context.Context, database *db.DB, cfg config.Config) error {
	key := db.ArchiveAttachmentKey{RepositoryID: i.Info.RepositoryID, SnapshotID: i.Info.SnapshotID}
	if err := database.RegisterArchiveAttachment(ctx, key, i.ArchiveID); err != nil {
		return err
	}
	target, err := attachmentTarget(cfg)
	if err != nil {
		return err
	}
	status, err := database.ArchiveAttachmentStatus(ctx, key)
	if err != nil {
		return err
	}
	if status.Target == target {
		return i.configureMachineNames(ctx, database)
	}
	var targets []db.ArchiveSourceTarget
	after := ""
	for {
		sources, err := i.Sources(ctx, after, 128)
		if err != nil {
			return err
		}
		if len(sources) == 0 {
			break
		}
		for _, source := range sources {
			wanted, err := completionTarget(source, cfg)
			if err != nil {
				return err
			}
			targets = append(targets, db.ArchiveSourceTarget{Provenance: i.provenance(source), Target: wanted, MachineLabel: source.Root.Machine})
			after = source.ManifestID
		}
	}
	if err := database.SetArchiveAttachmentTargets(ctx, key, target, targets); err != nil {
		return err
	}
	return i.configureMachineNames(ctx, database)
}

func (i *Index) source(ctx context.Context, manifestID string) (Source, error) {
	var s Source
	err := i.database.QueryRowContext(ctx, `SELECT s.manifest_id,s.root_id,s.source_key,s.original_path,s.canonical_json,
  r.id,r.device_id,r.machine,r.provider,r.original_path,r.configured_root_id
  FROM sources s JOIN roots r ON r.id=s.root_id WHERE s.manifest_id=?`, manifestID).
		Scan(&s.ManifestID, &s.RootID, &s.SourceKey, &s.OriginalPath, &s.CanonicalJSON,
			&s.Root.ID, &s.Root.DeviceID, &s.Root.Machine, &s.Root.Provider, &s.Root.OriginalPath, &s.Root.ConfiguredRootID)
	return s, err
}

func (i *Index) configureMachineNames(ctx context.Context, database *db.DB) error {
	for id := range i.machineNames {
		var found bool
		if err := i.database.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM roots WHERE device_id=?)`, id).Scan(&found); err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("configured machine %s is absent from this archive", id)
		}
	}
	if len(i.machineNames) == 0 {
		return nil
	}
	return database.Update(ctx, func(tx *sql.Tx) error {
		for id, label := range i.machineNames {
			var attached bool
			if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM archive_source_bindings WHERE archive_id=? AND device_id=?)`, i.ArchiveID, id).Scan(&attached); err != nil {
				return err
			}
			if !attached {
				return errors.New("archive machine has not been registered")
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO pg_sync_state(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, db.MachineLabelKeyPrefix+id, label); err != nil {
				return err
			}
		}
		return nil
	})
}
