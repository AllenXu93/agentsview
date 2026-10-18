package sync

import (
	"context"

	"go.kenn.io/agentsview/internal/db"
	"go.kenn.io/agentsview/internal/ingest"
)

func (e *Engine) sessionArchiveProvenance(session *db.Session, native string) (*db.ArchiveSessionProvenance, error) {
	if e.archiveWriteProvenance == nil {
		return nil, nil
	}
	p, err := e.archiveWriteProvenance(session, native)
	if err != nil {
		return nil, err
	}
	return &p, nil
}

func (e *Engine) writeAttachedStaged(ctx context.Context, prepared ingest.PreparedSession, pw pendingWrite, provenance *db.ArchiveSessionProvenance) error {
	observation, hasObservation := e.projectIdentityObservationForWrite(pw, prepared.Session)
	snapshotProject := pw.sess.Project
	write := db.SessionBatchWrite{
		Session: prepared.Session, Messages: prepared.Messages, UsageEvents: prepared.UsageEvents, PlanUsage: prepared.PlanUsage,
		ToolResultImages: &e.toolResultImages, ArchiveProvenance: provenance,
		IdentityObservation: identityObservationOrZero(observation, hasObservation), IdentitySnapshotProject: &snapshotProject,
		ReplaceMessages: true, DataVersion: dataVersionForWrite(pw), SkipSignalUpdates: e.disableSignalRecompute,
	}
	return e.writeStagedFullParse(ctx, prepared.Session, prepared.Messages, pw, &write)
}
