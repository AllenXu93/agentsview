package db

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// RecordLocalMachineName remembers the installation that owns this archive
// and the display name to store on its sessions. Writable archives also
// backfill that name onto rows already recorded here. Sessions that belong
// to another installation are left unchanged.
func (db *DB) RecordLocalMachineName(ctx context.Context, installationID, name string) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return errors.New("local machine name is required")
	}
	installationID = strings.TrimSpace(installationID)
	db.mu.Lock()
	db.localInstallationID = installationID
	db.localMachineName = name
	readOnly := db.readOnly
	db.mu.Unlock()
	if readOnly {
		return nil
	}
	return db.backfillLocalMachineName(ctx, installationID, name)
}

func (db *DB) backfillLocalMachineName(ctx context.Context, installationID, name string) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	_, err := db.getWriter().ExecContext(ctx, `
		UPDATE sessions
		SET machine_name = ?,
		    local_modified_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
		WHERE machine_name != ?
		  AND (
		    machine IN ('', 'local')
		    OR (? != '' AND machine = ?)
		  )`, name, name, installationID, installationID)
	if err != nil {
		return fmt.Errorf("backfilling session machine names: %w", err)
	}
	return nil
}

// applyLocalMachineName stamps sessions recorded by this archive with the
// configured display name. Other installations keep the name they arrived with.
func (db *DB) applyLocalMachineName(s *Session) {
	if db.localMachineName == "" || s == nil {
		return
	}
	if sessionBelongsToLocalMachine(s.Machine, db.localInstallationID) {
		s.MachineName = db.localMachineName
	}
}

func sessionBelongsToLocalMachine(machine, installationID string) bool {
	if machine == "" || machine == "local" {
		return true
	}
	return installationID != "" && machine == installationID
}
