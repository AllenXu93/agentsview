package db

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/agentsview/internal/export"
)

func archiveProvenanceWrite(t *testing.T, database *DB) SessionBatchWrite {
	t.Helper()
	const device = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	key := ArchiveAttachmentKey{RepositoryID: "repository", SnapshotID: "snapshot"}
	require.NoError(t, database.RegisterArchiveAttachment(t.Context(), key, "archive"))
	p := ArchiveSessionProvenance{
		Attachment: key, Source: ArchiveSourceIdentity{
			ArchiveID: "archive", DeviceID: device, RootID: "root", SourceKey: "source",
		}, ManifestID: "manifest", NativeSessionID: "codex:session",
	}
	require.NoError(t, database.SetArchiveAttachmentTargets(t.Context(), key, "target", []ArchiveSourceTarget{{Provenance: p.ArchiveSourceProvenance, Target: "target"}}))
	id := device + "~" + p.NativeSessionID
	return SessionBatchWrite{
		Session:         Session{ID: id, Machine: device, Agent: "codex", Project: "original", FilePath: new("archive://source/session")},
		Messages:        []Message{{SessionID: id, Ordinal: 0, Role: "user", Content: "original question"}},
		ReplaceMessages: true, DataVersion: 7, ArchiveProvenance: &p,
	}
}

func TestArchiveProvenanceCommitsWithContent(t *testing.T) {
	for _, mode := range []string{"batch", "staged"} {
		t.Run(mode, func(t *testing.T) {
			for _, fault := range []string{"success", "content failure", "existing unowned", "different source", "different manifest", "receiver alias", "publication authority", "cancelled"} {
				t.Run(fault, func(t *testing.T) {
					database := testDB(t)
					_, err := database.EnsureInstallationIdentity(t.Context(), "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
					require.NoError(t, err)
					write := archiveProvenanceWrite(t, database)
					publish := func(ctx context.Context, w SessionBatchWrite) error {
						if mode == "staged" {
							return database.WriteSessionBatchStaged(ctx, w, newScratchStagedResults(t), nil, nil)
						}
						result, err := database.WriteSessionBatchContext(ctx, []SessionBatchWrite{w})
						return errors.Join(append(result.Errors, err)...)
					}
					if fault == "existing unowned" {
						w := write
						w.ArchiveProvenance = nil
						require.NoError(t, publish(t.Context(), w))
					} else {
						require.NoError(t, publish(t.Context(), write))
					}
					before, err := database.GetSessionFull(t.Context(), write.Session.ID)
					require.NoError(t, err)
					beforeMessages, err := database.GetAllMessages(t.Context(), write.Session.ID)
					require.NoError(t, err)
					var beforeCount int
					require.NoError(t, database.getReader().QueryRow(t.Context(), `SELECT count(*) FROM archive_session_provenance`).Scan(&beforeCount))
					write.Session.Project = "replacement"
					write.Messages = []Message{{SessionID: write.Session.ID, Ordinal: 0, Role: "user", Content: "replacement question"}}
					ctx := t.Context()
					switch fault {
					case "content failure":
						_, err = database.getWriter().Exec(t.Context(), `CREATE TRIGGER fail_archive_message BEFORE INSERT ON messages BEGIN SELECT RAISE(ABORT,'injected content failure'); END`)
						require.NoError(t, err)
					case "different source":
						other := *write.ArchiveProvenance
						other.Source.SourceKey = "other-source"
						require.NoError(t, database.SetArchiveAttachmentTargets(ctx, other.Attachment, "target", []ArchiveSourceTarget{
							{Provenance: write.ArchiveProvenance.ArchiveSourceProvenance, Target: "target"},
							{Provenance: other.ArchiveSourceProvenance, Target: "target"},
						}))
						write.ArchiveProvenance = &other
					case "different manifest":
						write.ArchiveProvenance.ManifestID = "other-manifest"
					case "receiver alias":
						require.NoError(t, database.SetSyncState(ctx, MachineAliasKeyPrefix+write.Session.Machine, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
					case "publication authority":
						require.NoError(t, database.SetSyncState(ctx, "artifact_local_machine_name", write.Session.Machine))
					case "cancelled":
						var cancel context.CancelFunc
						ctx, cancel = context.WithCancel(ctx)
						cancel()
					}
					err = publish(ctx, write)
					if fault == "success" {
						require.NoError(t, err)
						// A repeat takes the staged no-content-change path while updating metadata.
						write.Session.Project = "metadata-only"
						require.NoError(t, publish(ctx, write))
						after, err := database.GetSessionFull(ctx, write.Session.ID)
						require.NoError(t, err)
						assert.Equal(t, "metadata-only", after.Project)
						assert.Equal(t, 7, after.DataVersion)
						var native, source string
						require.NoError(t, database.getReader().QueryRow(t.Context(), `SELECT native_session_id,source_key FROM archive_session_provenance WHERE session_id=?`, write.Session.ID).Scan(&native, &source))
						assert.Equal(t, "codex:session", native)
						assert.Equal(t, "source", source)
						return
					}
					require.Error(t, err)
					if fault != "content failure" && fault != "cancelled" {
						require.ErrorIs(t, err, ErrArchiveSourceConflict)
					}
					after, err := database.GetSessionFull(t.Context(), write.Session.ID)
					require.NoError(t, err)
					messages, err := database.GetAllMessages(t.Context(), write.Session.ID)
					require.NoError(t, err)
					assert.Equal(t, before, after)
					assert.Equal(t, beforeMessages, messages)
					var afterCount int
					require.NoError(t, database.getReader().QueryRow(t.Context(), `SELECT count(*) FROM archive_session_provenance`).Scan(&afterCount))
					assert.Equal(t, beforeCount, afterCount)
				})
			}
		})
	}
}

func TestArchiveProvenanceNewWriteRollsBack(t *testing.T) {
	for _, mode := range []string{"batch", "staged"} {
		t.Run(mode, func(t *testing.T) {
			database := testDB(t)
			write := archiveProvenanceWrite(t, database)
			require.NoError(t, database.Update(t.Context(), func(tx *sql.Tx) error {
				_, err := tx.ExecContext(t.Context(), `CREATE TRIGGER fail_archive_message BEFORE INSERT ON messages BEGIN SELECT RAISE(ABORT,'injected content failure'); END`)
				return err
			}))
			var err error
			if mode == "staged" {
				err = database.WriteSessionBatchStaged(t.Context(), write, newScratchStagedResults(t), nil, nil)
			} else {
				result, writeErr := database.WriteSessionBatchContext(t.Context(), []SessionBatchWrite{write})
				err = errors.Join(append(result.Errors, writeErr)...)
			}
			require.ErrorContains(t, err, "injected content failure")
			session, err := database.GetSessionFull(t.Context(), write.Session.ID)
			require.NoError(t, err)
			assert.Nil(t, session)
			var count int
			require.NoError(t, database.getReader().QueryRow(t.Context(), `SELECT count(*) FROM archive_session_provenance`).Scan(&count))
			assert.Zero(t, count)
		})
	}
}

func TestArchiveProvenanceRefusesOrdinaryWriters(t *testing.T) {
	for _, mode := range []string{"upsert", "project identity", "incremental"} {
		t.Run(mode, func(t *testing.T) {
			database := testDB(t)
			write := archiveProvenanceWrite(t, database)
			_, err := database.WriteSessionBatchAtomic(t.Context(), []SessionBatchWrite{write})
			require.NoError(t, err)
			before, err := database.GetSessionFull(t.Context(), write.Session.ID)
			require.NoError(t, err)
			messages, err := database.GetAllMessages(t.Context(), write.Session.ID)
			require.NoError(t, err)
			write.Session.Project = "replacement"
			switch mode {
			case "upsert":
				err = database.UpsertSession(t.Context(), write.Session)
			case "project identity":
				err = database.UpsertSessionWithProjectIdentity(t.Context(), write.Session, export.ProjectIdentityObservation{SessionID: write.Session.ID, Project: "replacement", Machine: write.Session.Machine}, "replacement")
			case "incremental":
				_, err = database.WriteSessionIncremental(t.Context(), write.Session.ID, []Message{{SessionID: write.Session.ID, Ordinal: 1, Role: "user", Content: "another route"}}, IncrementalSessionUpdate{MsgCount: 2, NextOrdinal: 2})
			}
			require.ErrorIs(t, err, ErrArchiveSourceConflict)
			after, err := database.GetSessionFull(t.Context(), write.Session.ID)
			require.NoError(t, err)
			assert.Equal(t, before, after)
			afterMessages, err := database.GetAllMessages(t.Context(), write.Session.ID)
			require.NoError(t, err)
			assert.Equal(t, messages, afterMessages)
		})
	}
}
