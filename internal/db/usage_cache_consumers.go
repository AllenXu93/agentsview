package db

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"go.kenn.io/agentsview/internal/export"
	"go.kenn.io/agentsview/internal/money"
)

// usageRollupSlowRequestThreshold gates the privacy-safe phase-timing log so
// warm reads stay silent and slow requests explain where the time went.
const (
	usageRollupSlowRequestThreshold = 2 * time.Second
	usageRollupStaleServeWindow     = 10 * time.Second
)

type usageRollupRequestTimings struct {
	capture, sweep, fill, read time.Duration
	rollup                     usageRollupMetrics
	candidates                 int
}

func (timings usageRollupRequestTimings) logIfSlow(started time.Time) {
	total := time.Since(started)
	if total < usageRollupSlowRequestThreshold {
		return
	}
	round := func(value time.Duration) time.Duration {
		return value.Round(time.Millisecond)
	}
	log.Printf("usage rollup read took %s: capture %s, deletion sweep %s, "+
		"fact fill %s, rollup build %s, rollup install %s, cache read %s "+
		"(%d candidate sessions; built %d daily rows, %d exception rows in %d groups)",
		round(total), round(timings.capture), round(timings.sweep),
		round(timings.fill), round(timings.rollup.BuildDuration),
		round(timings.rollup.InstallDuration), round(timings.read),
		timings.candidates, timings.rollup.DailyRows,
		timings.rollup.ExceptionRows, timings.rollup.ExceptionGroups)
}

func (cache *usageCache) installedUsageRollups(ctx context.Context, snapshot usageQuerySnapshot, resolver *export.PricingResolver) (map[string]usageRollupInstall, []string, error) {
	now := cache.fill.now()
	recent := func(marker string) bool {
		changed, err := time.Parse(extractTimeLayout, marker)
		age := now.Sub(changed)
		return err == nil && age >= 0 && age <= usageRollupStaleServeWindow
	}
	anyRecent := false
	cache.fill.mu.Lock()
	for _, version := range snapshot.Versions {
		if state, ok := cache.fill.stale[version.SessionID]; ok {
			if !state.source.Equal(version) && now.Sub(state.since) >= usageRollupStaleServeWindow {
				cache.fill.mu.Unlock()
				return nil, nil, nil
			} else {
				anyRecent = true
			}
		}
		anyRecent = anyRecent || recent(version.SyncMarker)
	}
	cache.fill.mu.Unlock()
	if !anyRecent {
		return nil, nil, nil
	}
	conn, err := cache.db.Conn(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `BEGIN`); err != nil {
		return nil, nil, err
	}
	defer func() { _, _ = conn.ExecContext(context.Background(), `ROLLBACK`) }()
	fills := make(map[string]usageFillResult, len(snapshot.Versions))
	var behind []string
	for _, version := range snapshot.Versions {
		fill, err := readCurrentUsageFillResult(ctx, conn, version.SessionID)
		if err != nil {
			return nil, nil, err
		}
		fills[version.SessionID] = fill
		if fill.Deleted {
			return nil, nil, nil
		}
	}
	installs, stale, err := readUsageRollupInstalls(ctx, conn, usageTimezoneIdentityFor(snapshot.location, snapshot.Intervals), snapshot, fills, newUsagePricingIdentities(resolver))
	if err != nil || len(stale) > 0 || installs[usageRollupCursorSessionID].FactRevision < snapshot.CursorHighWater {
		return nil, nil, err
	}
	// Release the pinned connection before checking revisions against the live cache.
	if _, err := conn.ExecContext(ctx, `ROLLBACK`); err != nil {
		return nil, nil, err
	}
	if err := conn.Close(); err != nil {
		return nil, nil, err
	}
	for _, version := range snapshot.Versions {
		fill := fills[version.SessionID]
		eligible := cache.fill.staleSourceEligible(ctx, version, fill)
		if !fill.source.Equal(version) {
			if !eligible {
				return nil, nil, nil
			}
			behind = append(behind, version.SessionID)
		}
	}
	return installs, behind, nil
}

func (c *usageFillCoordinator) observeStaleFillLocked(id string, fill usageFillResult) {
	if state, ok := c.stale[id]; ok && (fill.InstallRevision > state.InstallRevision || fill.InstallRevision == state.InstallRevision && fill.Deleted) {
		delete(c.stale, id)
	}
}

func (c *usageFillCoordinator) reconcileStaleDeletions(ctx context.Context, revision int64) {
	c.mu.Lock()
	if revision <= c.staleDeletionRevision {
		c.mu.Unlock()
		return
	}
	ids := make([]string, 0, len(c.stale))
	fills := make(map[string]usageFillResult, len(c.stale))
	for id, state := range c.stale {
		ids = append(ids, id)
		fills[id] = usageFillResult{InstallRevision: state.InstallRevision, Deleted: true}
	}
	c.mu.Unlock()
	if c.observer.afterStaleSnapshot != nil {
		c.observer.afterStaleSnapshot()
	}
	if err := QueryChunkedSize(ids, usageFillInstallBatchSize, func(chunk []string) error {
		placeholders, args := InPlaceholders(chunk)
		rows, err := c.cache.db.QueryContext(ctx, `SELECT session_id, install_revision
			FROM usage_cached_sessions WHERE session_id IN `+placeholders, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id string
			var fill usageFillResult
			if err := rows.Scan(&id, &fill.InstallRevision); err != nil {
				return err
			}
			fills[id] = fill
		}
		return rows.Err()
	}); err != nil {
		log.Printf("reconciling stale usage windows: %v", err)
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, fill := range fills {
		c.observeStaleFillLocked(id, fill)
	}
	// Keep the pre-snapshot watermark so later deletions trigger another reconciliation.
	c.staleDeletionRevision = max(c.staleDeletionRevision, revision)
}

func (c *usageFillCoordinator) staleSourceEligible(ctx context.Context, version usageSourceVersion, fill usageFillResult) bool {
	if !c.archive.isUsageBackfillEnabled() {
		return false
	}
	conn, err := c.cache.db.Conn(ctx)
	if err != nil {
		return false
	}
	defer conn.Close()
	c.mu.Lock()
	defer c.mu.Unlock()
	// The installed revision fences pinned observations without retaining completed windows.
	current, err := readCurrentUsageFillResult(ctx, conn, version.SessionID)
	if err != nil || current.Deleted || fill.Deleted || current.InstallRevision != fill.InstallRevision {
		return false
	}
	c.observeStaleFillLocked(version.SessionID, fill)
	state, exists := c.stale[version.SessionID]
	if fill.source.SyncMarker == "" || fill.source.Equal(version) {
		return false
	}
	now := c.now()
	changed, err := time.Parse(extractTimeLayout, version.SyncMarker)
	installed, installedErr := time.Parse(extractTimeLayout, fill.source.SyncMarker)
	if err != nil || installedErr != nil || installed.After(now) || changed.After(now) {
		return false
	}
	if !exists {
		if now.Sub(changed) > usageRollupStaleServeWindow {
			return false
		}
		// Keep expired versions until refill so later appends cannot restart their window.
		if len(c.stale) >= usageFillNotificationCapacity {
			return false
		}
		state = usageStaleSource{usageFillResult: fill, since: now}
		c.stale[version.SessionID] = state
	}
	return now.Sub(state.since) < usageRollupStaleServeWindow
}

func (db *DB) queryUsageRollups(
	ctx context.Context, filter UsageFilter, kind usageQueryKind,
	includeCursor bool,
) (usageQuerySnapshot, usageFactsResult, *export.PricingResolver, error) {
	// Live archive writes no longer force a recapture: facts are filled from
	// their own read snapshot and rollups aggregate committed facts only.
	// What remains is cache-generation retirement, which replaces the whole
	// cache, and cache-side invalidation of an install between the build and
	// the read. Both are resolved by recapturing, and neither is driven by
	// the write rate.
	var lastErr error
	report := func(phase string) {
		if filter.Progress != nil {
			filter.Progress(phase)
		}
	}
	for attempt := 1; attempt <= usageFillMaxAttempts; attempt++ {
		started := time.Now()
		var timings usageRollupRequestTimings
		report("Reading archived sessions for this report")
		snapshot, captureErr := db.captureUsageQuery(ctx, filter, kind)
		if captureErr != nil {
			return usageQuerySnapshot{}, usageFactsResult{}, nil, captureErr
		}
		timings.capture = time.Since(started)
		timings.candidates = len(snapshot.Versions)
		report("Checking the usage cache for this report")
		if !includeCursor || !usageCursorIncluded(filter) {
			snapshot.CursorHighWater = 0
		}
		cache, release, generationErr := db.usageCache.acquireGeneration(
			ctx, snapshot.DatabaseID)
		if generationErr != nil {
			if errors.Is(generationErr, errUsageCacheSourceChanged) {
				lastErr = generationErr
				continue
			}
			return usageQuerySnapshot{}, usageFactsResult{}, nil, generationErr
		}
		sweepStarted := time.Now()
		if sweepErr := cache.sweepDeletionJournal(ctx, db); sweepErr != nil {
			release()
			return usageQuerySnapshot{}, usageFactsResult{}, nil, sweepErr
		}
		timings.sweep = time.Since(sweepStarted)
		resolver := export.NewPricingResolver(snapshot.PricingRows)
		var installs map[string]usageRollupInstall
		var behind []string
		if db.isUsageBackfillEnabled() && !filter.RequireFresh {
			var staleErr error
			installs, behind, staleErr = cache.installedUsageRollups(ctx, snapshot, resolver)
			if staleErr != nil {
				log.Printf("checking installed usage rollups: %v", staleErr)
			}
		}
		if installs == nil {
			fillStarted := time.Now()
			report(fmt.Sprintf("Preparing usage data for %d sessions in this report", timings.candidates))
			fills, fillErr := cache.fill.Ensure(
				ctx, snapshot.Versions, snapshot.CursorHighWater,
			)
			if fillErr != nil {
				release()
				if errors.Is(fillErr, errUsageCacheSourceChanged) {
					lastErr = fillErr
					continue
				}
				return usageQuerySnapshot{}, usageFactsResult{}, nil, fillErr
			}
			timings.fill = time.Since(fillStarted)
			snapshot.dropDeleted(fills)
			report(fmt.Sprintf("Calculating daily totals for %d sessions in this report", timings.candidates))
			var rollupMetrics usageRollupMetrics
			var rollupErr error
			installs, rollupMetrics, rollupErr = cache.rollup.Ensure(
				ctx, snapshot, fills, resolver)
			if rollupErr != nil {
				release()
				if errors.Is(rollupErr, errUsageCacheSourceChanged) {
					lastErr = rollupErr
					continue
				}
				return usageQuerySnapshot{}, usageFactsResult{}, nil, rollupErr
			}
			timings.rollup = rollupMetrics
		}
		readStarted := time.Now()
		report("Reading cached daily totals")
		var facts usageFactsResult
		var queryErr error
		if kind == usageQueryKindActivity {
			var matching map[string]UsageSessionInfo
			matching, queryErr = cache.usageRollupActivityQuery(
				ctx, snapshot, filter, installs)
			facts.MatchingSessions = matching
		} else {
			facts, queryErr = cache.usageRollupQuery(
				ctx, snapshot, filter, installs, resolver)
		}
		timings.read = time.Since(readStarted)
		if queryErr == nil {
			cache.fill.enqueueSessions(behind)
			release()
			timings.logIfSlow(started)
			return snapshot, facts, resolver, nil
		}
		release()
		if !usageCacheReadShouldRecapture(queryErr) {
			return usageQuerySnapshot{}, usageFactsResult{}, nil, queryErr
		}
		lastErr = queryErr
	}
	return usageQuerySnapshot{}, usageFactsResult{}, nil, lastErr
}

// GetTopSessionsByCost returns filtered sessions ranked by cost or tokens.
func (db *DB) GetTopSessionsByCost(
	ctx context.Context, filter UsageFilter, limit int,
) ([]TopSessionEntry, error) {
	snapshot, facts, _, err := db.queryUsageRollups(
		ctx, filter, usageQueryKindToken, false)
	if err != nil {
		return nil, err
	}
	type totals struct {
		input, output, cacheWrite, cacheRead int
		cost                                 money.Money
		authoritative                        *money.Money
	}
	bySession := make(map[string]*totals)
	for _, group := range facts.Groups {
		if group.SessionID == "" {
			continue
		}
		current := bySession[group.SessionID]
		if current == nil {
			current = &totals{}
			bySession[group.SessionID] = current
		}
		current.input += int(group.InputTokens)
		current.output += int(group.OutputTokens)
		current.cacheWrite += int(group.CacheCreationTokens)
		current.cacheRead += int(group.CacheReadTokens)
		current.cost, err = money.Add(current.cost,
			money.Money{Microdollars: group.CostMicrodollars})
		if err != nil {
			return nil, fmt.Errorf("summing top-session cost: %w", err)
		}
		if !filter.HasModelFilter() &&
			group.AuthoritativeCostMicrodollars != nil {
			value := money.Money{Microdollars: *group.AuthoritativeCostMicrodollars}
			current.authoritative = &value
		}
	}
	metadata := make(map[string]usageQuerySession, len(snapshot.Sessions))
	for _, session := range snapshot.Sessions {
		metadata[session.ID] = session
	}
	result := make([]TopSessionEntry, 0, len(bySession))
	for sessionID, value := range bySession {
		cost := value.cost
		if value.authoritative != nil {
			cost = *value.authoritative
		}
		entry := TopSessionEntry{
			SessionID: sessionID, DisplayName: sessionID,
			InputTokens: value.input, OutputTokens: value.output,
			CacheCreationTokens: value.cacheWrite,
			CacheReadTokens:     value.cacheRead,
			TotalTokens:         value.input + value.output + value.cacheWrite + value.cacheRead,
			Cost:                cost,
		}
		if session, ok := metadata[sessionID]; ok {
			entry.DisplayName = session.DisplayName
			entry.Agent = session.Agent
			entry.Project = session.Project
			entry.StartedAt = session.StartedAt
			entry.Machine = session.Machine
			entry.GroupKey = session.GroupKey
			entry.SessionName = session.SessionName
		}
		result = append(result, entry)
	}
	if filter.TopSessionsByGroup {
		return GroupTopSessions(result, limit, filter.TopSessionsSort, filter.TopSessionsTokenTypes)
	}
	return SortAndLimitTopSessions(
		result, limit, filter.TopSessionsSort, filter.TopSessionsTokenTypes,
	), nil
}

// GetUsageSessionCounts returns distinct billed survivor owners by metadata.
func (db *DB) GetUsageSessionCounts(
	ctx context.Context, filter UsageFilter,
) (UsageSessionCounts, error) {
	_, facts, _, err := db.queryUsageRollups(
		ctx, filter, usageQueryKindToken, false)
	if err != nil {
		return UsageSessionCounts{}, err
	}
	return NewUsageSessionCounts(facts.MatchingSessions), nil
}

// GetUsageMatchingSessionCount counts sessions with eligible usage activity.
func (db *DB) GetUsageMatchingSessionCount(
	ctx context.Context, filter UsageFilter,
) (int, error) {
	_, facts, _, err := db.queryUsageRollups(
		ctx, filter, usageQueryKindActivity, false)
	if err != nil {
		return 0, err
	}
	return len(facts.MatchingSessions), nil
}

// GetSessionUsage returns one session's exact, intra-session usage summary.
func (db *DB) GetSessionUsage(
	ctx context.Context, sessionID string, includeBreakdown bool,
) (*SessionUsage, error) {
	return db.getSessionUsageLegacy(ctx, sessionID, includeBreakdown)
}
