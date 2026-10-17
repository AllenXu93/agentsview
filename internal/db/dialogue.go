package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// refreshDialogueFTS upgrades the derived index once, preserving every message.
func refreshDialogueFTS(ctx context.Context, w *writerHandle, progress OpenProgressFunc) error {
	var ddl string
	err := w.QueryRowContext(ctx, "SELECT sql FROM sqlite_master WHERE name = 'messages_fts'").Scan(&ddl)
	if errors.Is(err, sql.ErrNoRows) || err == nil && strings.Contains(ddl, "dialogue,") {
		return nil
	}
	if err != nil {
		return fmt.Errorf("reading message FTS schema: %w", err)
	}
	tx, err := w.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	for _, query := range []string{
		"DROP TRIGGER IF EXISTS messages_ai", "DROP TRIGGER IF EXISTS messages_ad", "DROP TRIGGER IF EXISTS messages_au",
		"DROP TABLE messages_fts", "DROP VIEW IF EXISTS message_search_content", schemaFTS,
	} {
		if _, err := tx.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("refreshing dialogue FTS: %w", err)
		}
	}
	if err := populateDialogueFTS(ctx, tx, progress); err != nil {
		return fmt.Errorf("refreshing dialogue FTS: %w", err)
	}
	return tx.Commit()
}

func populateDialogueFTS(ctx context.Context, tx *sql.Tx, progress OpenProgressFunc) error {
	var total, done, lastID int64
	if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM messages").Scan(&total); err != nil {
		return err
	}
	progress.report(fmt.Sprintf("Updating full-text search: %d/%d messages", done, total))
	for done < total {
		var nextID int64
		if err := tx.QueryRowContext(ctx, `SELECT max(id) FROM
			(SELECT id FROM messages WHERE id > ? ORDER BY id LIMIT 10000)`, lastID).Scan(&nextID); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `INSERT INTO messages_fts(rowid, content, dialogue)
			SELECT id, content, dialogue FROM message_search_content WHERE id > ? AND id <= ?`, lastID, nextID)
		if err != nil {
			return err
		}
		count, err := result.RowsAffected()
		if err != nil {
			return err
		}
		done += count
		lastID = nextID
		progress.report(fmt.Sprintf("Updating full-text search: %d/%d messages", done, total))
	}
	return nil
}
