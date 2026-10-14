//go:build fts5

package db

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.kenn.io/agentsview/internal/export"
	"go.kenn.io/agentsview/internal/money"
)

func TestUsageStaleAppendRefreshesWithoutWaiting(t *testing.T) {
	database, cache := staleUsageFixture(t, true)
	started, release := pauseStaleUsageExtraction(t, cache, true)
	appendLiveWriteMessage(t, database, "session", "2026-08-10T10:00:00Z", 1, 7)
	waitUsageNotification(t, started)
	_, err := database.getWriter().Exec(t.Context(), `UPDATE sessions SET created_at = '2026-08-10T09:00:00Z', local_modified_at = ? WHERE id = 'session'`, cache.fill.now().Add(-time.Second).UTC().Format(time.RFC3339Nano))
	require.NoError(t, err)
	held := openRawUsageCacheTestDB(t, cache.path)
	defer held.Close()
	_, err = held.ExecContext(t.Context(), `BEGIN IMMEDIATE`)
	require.NoError(t, err)
	defer func() { _, _ = held.ExecContext(t.Context(), `ROLLBACK`) }()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	for range 5 {
		daily, err := database.GetDailyUsage(ctx, UsageFilter{Timezone: "UTC"})
		require.NoError(t, err)
		assert.Equal(t, 10, daily.Totals.InputTokens)
		assert.Equal(t, 1, daily.SessionCounts.Total)
		counts, err := database.GetUsageSessionCounts(ctx, UsageFilter{Timezone: "UTC"})
		require.NoError(t, err)
		assert.Equal(t, 1, counts.Total)
	}
	cache.fill.mu.Lock()
	queued := len(cache.fill.notify) + len(cache.fill.overflow)
	cache.fill.mu.Unlock()
	assert.Positive(t, queued, "reads must queue recovery while a batch is running")
	_, err = held.ExecContext(t.Context(), `COMMIT`)
	require.NoError(t, err)
	release()
}

func TestUsageStaleRejectedStatesStayExact(t *testing.T) {
	for _, state := range []string{"no facts", "old marker", "pending rollup", "pricing", "cursor", "standalone", "require fresh"} {
		t.Run(state, func(t *testing.T) {
			database, cache := staleUsageFixture(t, state != "standalone")
			changeStaleUsage(t, database, cache.fill.now().Add(-time.Second))
			want := 17
			switch state {
			case "no facts":
				_, err := cache.db.ExecContext(t.Context(), `DELETE FROM usage_cached_sessions WHERE session_id = 'session'`)
				require.NoError(t, err)
			case "old marker":
				changeStaleUsage(t, database, cache.fill.now().Add(-time.Minute))
			case "pending rollup":
				prepareUsageRollupTest(t, database)
			case "pricing":
				require.NoError(t, database.UpsertModelPricing([]ModelPricing{{ModelPattern: "model-a", InputPerMTok: money.Money{Microdollars: 1_000_000}}}))
			case "cursor":
				require.NoError(t, database.InsertCursorUsageEvents(t.Context(), []CursorUsageEvent{{OccurredAt: "2026-08-10T09:00:00Z", Model: "cursor-model", InputTokens: 1, DedupKey: "cursor-one"}}))
				want = 18
			case "standalone":
			}
			daily, err := database.GetDailyUsage(t.Context(), UsageFilter{Timezone: "UTC", RequireFresh: state == "require fresh"})
			require.NoError(t, err)
			assert.Equal(t, want, daily.Totals.InputTokens)
		})
	}
}

func TestUsageStaleWindowExpiresDuringSlowRefresh(t *testing.T) {
	for _, overlap := range []string{"none", "request", "fill", "backfill", "future marker"} {
		t.Run(overlap, func(t *testing.T) {
			database, cache := staleUsageFixture(t, true)
			snapshot, err := database.captureUsageQuery(t.Context(), UsageFilter{Timezone: "UTC"}, usageQueryKindToken)
			require.NoError(t, err)
			base := cache.fill.now()
			var elapsed atomic.Int64
			cache.fill.now = func() time.Time { return base.Add(time.Duration(elapsed.Load())) }
			started, release := pauseStaleUsageExtraction(t, cache, false)
			changeStaleUsage(t, database, base.Add(-2*time.Second))
			assert.Equal(t, 10, liveWriteDailyInput(t, database, "project"))
			waitUsageNotification(t, started)
			elapsed.Store(int64(9 * time.Second))
			assert.Equal(t, 10, liveWriteDailyInput(t, database, "project"))
			switch overlap {
			case "request":
				_, _, err := cache.installedUsageRollups(t.Context(), snapshot, export.NewPricingResolver(snapshot.PricingRows))
				require.NoError(t, err)
			case "fill":
				_, err := cache.fill.Ensure(t.Context(), snapshot.Versions, 0)
				require.NoError(t, err)
			case "backfill":
				_, err := cache.fill.FillBackground(t.Context(), snapshot.Versions, 0)
				require.NoError(t, err)
			}
			if overlap == "future marker" {
				changeStaleUsage(t, database, cache.fill.now().Add(time.Minute))
			} else {
				changeStaleUsage(t, database, cache.fill.now().Add(-time.Second))
				assert.Equal(t, 10, liveWriteDailyInput(t, database, "project"))
				elapsed.Store(int64(10 * time.Second))
			}
			result := make(chan DailyUsageResult, 1)
			errors := make(chan error, 1)
			go func() {
				daily, err := database.GetDailyUsage(t.Context(), UsageFilter{Timezone: "UTC"})
				result <- daily
				errors <- err
			}()
			waitUsageNotification(t, started)
			select {
			case <-result:
				require.FailNow(t, "ineligible stale totals returned while refresh was paused")
			default:
			}
			release()
			require.NoError(t, <-errors)
			assert.Equal(t, 17, (<-result).Totals.InputTokens)
		})
	}
}

func TestUsageStaleCorrectedFutureTimestampStaysExact(t *testing.T) {
	database, cache := staleUsageFixture(t, false)
	_, err := database.getWriter().Exec(t.Context(), `UPDATE sessions SET created_at = '2026-08-10T09:00:00Z', local_modified_at = ? WHERE id = 'session'`, cache.fill.now().Add(time.Minute).UTC().Format(time.RFC3339Nano))
	require.NoError(t, err)
	require.Equal(t, 10, liveWriteDailyInput(t, database, "project"))
	require.NoError(t, database.StartUsageCacheBackfill(t.Context()))
	require.NoError(t, database.WaitUsageCacheBackfill(t.Context()))
	changeStaleUsage(t, database, cache.fill.now().Add(-time.Second))
	assert.Equal(t, 17, liveWriteDailyInput(t, database, "project"))
}

func TestUsageStaleEligibilityCancellationWhileCacheConnectionsHeld(t *testing.T) {
	database, cache := staleUsageFixture(t, true)
	changeStaleUsage(t, database, cache.fill.now().Add(-time.Second))
	snapshot, err := database.captureUsageQuery(t.Context(), UsageFilter{Timezone: "UTC"}, usageQueryKindToken)
	require.NoError(t, err)
	conn, err := cache.db.Conn(t.Context())
	require.NoError(t, err)
	fill, err := readCurrentUsageFillResult(t.Context(), conn, "session")
	require.NoError(t, err)
	require.NoError(t, conn.Close())
	require.True(t, cache.fill.staleSourceEligible(t.Context(), snapshot.Versions[0], fill))
	for range cache.db.Stats().MaxOpenConnections {
		conn, err := cache.db.Conn(t.Context())
		require.NoError(t, err)
		t.Cleanup(func() { _ = conn.Close() })
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	waits := cache.db.Stats().WaitCount
	result := make(chan bool, 1)
	go func() {
		result <- cache.fill.staleSourceEligible(ctx, snapshot.Versions[0], fill)
	}()
	require.Eventually(t, func() bool { return cache.db.Stats().WaitCount > waits }, 10*time.Second, time.Millisecond)
	cancel()
	select {
	case eligible := <-result:
		assert.False(t, eligible)
	case <-time.After(10 * time.Second):
		require.FailNow(t, "cancelled stale eligibility check kept waiting for a cache connection")
	}
	assert.NoError(t, cache.fill.ctx.Err())
}

func TestUsageStaleOverlappingCacheSnapshotsAcrossRefill(t *testing.T) {
	for _, overlap := range []string{"matching snapshot", "stale snapshot", "missing snapshot", "install cleanup", "delete cleanup"} {
		for _, tracking := range []string{"cleared", "tracking"} {
			if tracking == "cleared" && overlap != "stale snapshot" {
				continue
			}
			t.Run(overlap+"/"+tracking, func(t *testing.T) {
				database, cache := staleUsageFixture(t, true)
				base := cache.fill.now()
				var elapsed atomic.Int64
				cache.fill.now = func() time.Time { return base.Add(time.Duration(elapsed.Load())) }
				filter := UsageFilter{Timezone: "UTC"}
				oldSnapshot, err := database.captureUsageQuery(t.Context(), filter, usageQueryKindToken)
				require.NoError(t, err)
				conn, err := cache.db.Conn(t.Context())
				require.NoError(t, err)
				defer conn.Close()
				_, err = conn.ExecContext(t.Context(), `BEGIN`)
				require.NoError(t, err)
				defer func() { _, _ = conn.ExecContext(t.Context(), `ROLLBACK`) }()
				oldFill, err := readCurrentUsageFillResult(t.Context(), conn, "session")
				require.NoError(t, err)
				changeStaleUsage(t, database, base.Add(-time.Second))
				snapshot, err := database.captureUsageQuery(t.Context(), filter, usageQueryKindToken)
				require.NoError(t, err)
				require.True(t, cache.fill.staleSourceEligible(t.Context(), snapshot.Versions[0], oldFill))
				var missingFill usageFillResult
				if overlap == "delete cleanup" || overlap == "missing snapshot" {
					require.NoError(t, cache.fill.deleteNotificationSessions([]string{"session"}))
					missingConn, err := cache.db.Conn(t.Context())
					require.NoError(t, err)
					missingFill, err = readCurrentUsageFillResult(t.Context(), missingConn, "session")
					require.NoError(t, err)
					require.NoError(t, missingConn.Close())
					require.True(t, missingFill.Deleted)
				}
				fills, err := cache.fill.FillBackground(t.Context(), snapshot.Versions, 0)
				require.NoError(t, err)
				_, _, err = cache.rollup.Ensure(t.Context(), snapshot, fills, export.NewPricingResolver(snapshot.PricingRows))
				require.NoError(t, err)
				changeStaleUsage(t, database, base.Add(-500*time.Millisecond))
				_, err = database.getWriter().Exec(t.Context(), `UPDATE messages SET token_usage = '{"input_tokens":24}' WHERE session_id = 'session'`)
				require.NoError(t, err)
				snapshot, err = database.captureUsageQuery(t.Context(), filter, usageQueryKindToken)
				require.NoError(t, err)
				newFill := fills["session"]
				require.Greater(t, newFill.InstallRevision, oldFill.InstallRevision)
				if tracking == "tracking" {
					require.True(t, cache.fill.staleSourceEligible(t.Context(), snapshot.Versions[0], newFill))
				}
				elapsed.Store(int64(9 * time.Second))
				switch overlap {
				case "matching snapshot":
					assert.False(t, cache.fill.staleSourceEligible(t.Context(), oldSnapshot.Versions[0], oldFill))
				case "stale snapshot":
					pinnedFill, err := readCurrentUsageFillResult(t.Context(), conn, "session")
					require.NoError(t, err)
					assert.False(t, cache.fill.staleSourceEligible(t.Context(), snapshot.Versions[0], pinnedFill))
				case "missing snapshot":
					assert.False(t, cache.fill.staleSourceEligible(t.Context(), snapshot.Versions[0], missingFill))
				case "install cleanup", "delete cleanup":
					oldFill.Deleted = overlap == "delete cleanup"
					cache.fill.mu.Lock()
					cache.fill.observeStaleFillLocked("session", oldFill)
					cache.fill.mu.Unlock()
				}
				deadline := usageRollupStaleServeWindow
				if tracking == "cleared" {
					require.True(t, cache.fill.staleSourceEligible(t.Context(), snapshot.Versions[0], newFill))
					deadline += 9 * time.Second
				}
				elapsed.Store(int64(deadline))
				installs, _, err := cache.installedUsageRollups(t.Context(), snapshot, export.NewPricingResolver(snapshot.PricingRows))
				require.NoError(t, err)
				assert.Nil(t, installs, "older observations must preserve the newer revision's original deadline")
				_, err = conn.ExecContext(t.Context(), `ROLLBACK`)
				require.NoError(t, err)
				daily, err := database.GetDailyUsage(t.Context(), filter)
				require.NoError(t, err)
				assert.Equal(t, 24, daily.Totals.InputTokens)
			})
		}
	}
}

func TestUsageStaleWindowsTurnOverAfterRefresh(t *testing.T) {
	for _, completion := range []string{"refill", "delete", "sweep before notification", "other process sweeps"} {
		t.Run(completion, func(t *testing.T) {
			database := testDB(t)
			for i := range 1280 {
				seedLiveWriteSession(t, database, fmt.Sprintf("turnover-%04d", i), "turnover", "2026-08-10T09:00:00Z", 0, 10)
			}
			require.NoError(t, database.StartUsageCacheBackfill(t.Context()))
			require.NoError(t, database.WaitUsageCacheBackfill(t.Context()))
			_, _, cache := prepareUsageRollupTest(t, database)
			sweeper := cache
			if completion == "other process sweeps" {
				var err error
				sweeper, err = openUsageCache(t.Context(), cache.path, cache.databaseID, false)
				require.NoError(t, err)
				sweeper.fill = newUsageFillCoordinator(t.Context(), database, sweeper)
				t.Cleanup(func() { sweeper.fill.Close(); _ = sweeper.db.Close() })
			}
			now := time.Now().Add(time.Hour)
			cache.fill.now = func() time.Time { return now }
			filter := UsageFilter{Timezone: "UTC", Project: "turnover"}
			snapshot, err := database.captureUsageQuery(t.Context(), filter, usageQueryKindToken)
			require.NoError(t, err)
			require.Len(t, snapshot.Versions, 1280)
			fills, err := cache.fill.FillBackground(t.Context(), snapshot.Versions, 0)
			require.NoError(t, err)
			_, err = database.getWriter().Exec(t.Context(), `UPDATE sessions SET transcript_revision = 'changed', created_at = '2026-08-10T09:00:00Z', local_modified_at = ? WHERE project = 'turnover'`, cache.fill.now().Add(-time.Second).UTC().Format(time.RFC3339Nano))
			require.NoError(t, err)
			snapshot, err = database.captureUsageQuery(t.Context(), filter, usageQueryKindToken)
			require.NoError(t, err)
			for start := 0; start < len(snapshot.Versions); start += 256 {
				batch := snapshot.Versions[start : start+256]
				ids := make([]string, 0, len(batch))
				for _, version := range batch {
					require.True(t, cache.fill.staleSourceEligible(t.Context(), version, fills[version.SessionID]), "session %s must get a window after earlier batches complete", version.SessionID)
					ids = append(ids, version.SessionID)
				}
				switch completion {
				case "refill":
					_, err = cache.fill.FillBackground(t.Context(), batch, 0)
				case "sweep before notification", "other process sweeps":
					_, err = database.getWriter().Exec(t.Context(), `DELETE FROM sessions WHERE id >= ? AND id <= ?`, ids[0], ids[len(ids)-1])
					require.NoError(t, err)
					require.NoError(t, sweeper.sweepDeletionJournal(t.Context(), database))
					require.NoError(t, cache.sweepDeletionJournal(t.Context(), database))
					err = cache.fill.deleteNotificationSessions(ids)
				default:
					err = cache.fill.deleteNotificationSessions(ids)
				}
				require.NoError(t, err)
				for _, version := range batch {
					assert.False(t, cache.fill.staleSourceEligible(t.Context(), version, fills[version.SessionID]), "completed windows must reject older observations")
				}
			}
		})
	}
}

func TestUsageStaleDeletionReconciliation(t *testing.T) {
	for _, timing := range []string{"expired same revision", "insert during reconciliation"} {
		t.Run(timing, func(t *testing.T) {
			database := testDB(t)
			seedLiveWriteSession(t, database, "session", "project", "2026-08-10T09:00:00Z", 0, 10)
			seedLiveWriteSession(t, database, "deleted", "removed", "2026-08-10T09:00:00Z", 0, 10)
			require.NoError(t, database.StartUsageCacheBackfill(t.Context()))
			require.NoError(t, database.WaitUsageCacheBackfill(t.Context()))
			_, _, cache := prepareUsageRollupTest(t, database)
			now := time.Now().Add(time.Hour)
			cache.fill.now = func() time.Time { return now }
			changeStaleUsage(t, database, cache.fill.now().Add(-time.Second))
			snapshot, err := database.captureUsageQuery(t.Context(), UsageFilter{Timezone: "UTC", Project: "project"}, usageQueryKindToken)
			require.NoError(t, err)
			var version usageSourceVersion
			for _, candidate := range snapshot.Versions {
				if candidate.SessionID == "session" {
					version = candidate
				}
			}
			require.Equal(t, "session", version.SessionID)
			conn, err := cache.db.Conn(t.Context())
			require.NoError(t, err)
			fill, err := readCurrentUsageFillResult(t.Context(), conn, "session")
			require.NoError(t, err)
			require.NoError(t, conn.Close())
			if timing == "expired same revision" {
				require.True(t, cache.fill.staleSourceEligible(t.Context(), version, fill))
				now = now.Add(usageRollupStaleServeWindow)
			} else {
				other, err := openUsageCache(t.Context(), cache.path, cache.databaseID, false)
				require.NoError(t, err)
				other.fill = newUsageFillCoordinator(t.Context(), database, other)
				t.Cleanup(func() { other.fill.Close(); _ = other.db.Close() })
				cache.fill.observer.afterStaleSnapshot = func() {
					require.True(t, cache.fill.staleSourceEligible(t.Context(), version, fill))
					require.NoError(t, database.DeleteSession(t.Context(), "session"))
					require.NoError(t, other.sweepDeletionJournal(t.Context(), database))
				}
			}
			require.NoError(t, database.DeleteSession(t.Context(), "deleted"))
			require.NoError(t, cache.sweepDeletionJournal(t.Context(), database))
			cache.fill.observer.afterStaleSnapshot = nil
			cache.fill.mu.Lock()
			_, tracked := cache.fill.stale["session"]
			cache.fill.mu.Unlock()
			require.True(t, tracked)
			require.NoError(t, cache.sweepDeletionJournal(t.Context(), database))
			if timing == "expired same revision" {
				changeStaleUsage(t, database, now.Add(-time.Second))
				snapshot, err = database.captureUsageQuery(t.Context(), UsageFilter{Timezone: "UTC", Project: "project"}, usageQueryKindToken)
				require.NoError(t, err)
				for _, candidate := range snapshot.Versions {
					if candidate.SessionID == "session" {
						version = candidate
					}
				}
				assert.False(t, cache.fill.staleSourceEligible(t.Context(), version, fill), "reconciliation must preserve the expired deadline")
			} else {
				cache.fill.mu.Lock()
				assert.NotContains(t, cache.fill.stale, "session", "the next sweep must reconcile the concurrent deletion")
				cache.fill.mu.Unlock()
			}
		})
	}
	for _, cancelRead := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancel reconciliation=%v", cancelRead), func(t *testing.T) {
			database, cache := staleUsageFixture(t, true)
			_, fills, _ := prepareUsageRollupTest(t, database)
			changeStaleUsage(t, database, cache.fill.now().Add(-time.Second))
			snapshot, err := database.captureUsageQuery(t.Context(), UsageFilter{Timezone: "UTC"}, usageQueryKindToken)
			require.NoError(t, err)
			require.True(t, cache.fill.staleSourceEligible(t.Context(), snapshot.Versions[0], fills["session"]))
			cache.db.SetMaxOpenConns(1)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			if cancelRead {
				cache.fill.observer.afterStaleSnapshot = cancel
			}
			require.NoError(t, database.DeleteSession(t.Context(), "session"))
			require.NoError(t, cache.sweepDeletionJournal(ctx, database))
			cache.fill.observer.afterStaleSnapshot = nil
			if cancelRead {
				cache.fill.mu.Lock()
				assert.Zero(t, cache.fill.staleDeletionRevision, "a failed reconciliation must be retried")
				assert.Contains(t, cache.fill.stale, "session")
				cache.fill.mu.Unlock()
				require.NoError(t, cache.sweepDeletionJournal(t.Context(), database))
			}
			cache.fill.mu.Lock()
			assert.NotContains(t, cache.fill.stale, "session", "reconciliation must acquire the pool's sole connection after the sweep releases it")
			cache.fill.mu.Unlock()
		})
	}
}

func TestUsageStaleDedupUsesCompatibleFacts(t *testing.T) {
	database := testDB(t)
	seedUsageSnapshotSession(t, database, "a", "a", "2026-08-10T09:00:00Z", 0, 20, "model-a")
	seedUsageSnapshotSession(t, database, "b", "b", "2026-08-10T09:00:00Z", 0, 10, "model-a")
	filter := UsageFilter{Timezone: "UTC"}
	daily, err := database.GetDailyUsage(t.Context(), filter)
	require.NoError(t, err)
	require.Equal(t, 20, daily.Totals.OutputTokens)
	require.NoError(t, database.StartUsageCacheBackfill(t.Context()))
	require.NoError(t, database.WaitUsageCacheBackfill(t.Context()))
	_, _, cache := prepareUsageRollupTest(t, database)
	now := time.Now().Add(time.Hour)
	cache.fill.now = func() time.Time { return now }
	_, err = database.getWriter().Exec(t.Context(), `UPDATE messages SET token_usage = '{}' WHERE session_id = 'a'; UPDATE sessions SET transcript_revision = 'changed', local_modified_at = ? WHERE id = 'a'`, now.Add(-time.Second).UTC().Format(time.RFC3339Nano))
	require.NoError(t, err)
	_, err = cache.db.ExecContext(t.Context(), `DELETE FROM usage_rollup_installs WHERE session_id = 'b'`)
	require.NoError(t, err)
	cache.fill.processNotificationBatch(map[string]bool{"b": true}, false)
	started, release := pauseStaleUsageExtraction(t, cache, false)
	batches := make(chan struct{}, 16)
	cache.fill.observer.afterNotificationBatch = func() { batches <- struct{}{} }
	var builds atomic.Int32
	cache.rollup.observer.beforeEnsure = func() { builds.Add(1) }
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	returned := make(chan struct{})
	go func() {
		daily, err = database.GetDailyUsage(ctx, filter)
		close(returned)
	}()
	select {
	case <-returned:
	case <-ctx.Done():
		release()
		<-returned
		require.FailNow(t, "stale read waited for blocked extraction")
	}
	require.NoError(t, err)
	waitUsageNotification(t, started)
	assert.Equal(t, 20, daily.Totals.OutputTokens)
	assert.Zero(t, builds.Load())
	release()
	waitUsageNotification(t, batches)
	daily, err = database.GetDailyUsage(t.Context(), filter)
	require.NoError(t, err)
	assert.Equal(t, 10, daily.Totals.OutputTokens)
}

func pauseStaleUsageExtraction(t *testing.T, cache *usageCache, firstOnly bool) (<-chan struct{}, func()) {
	t.Helper()
	started, released := make(chan struct{}, 16), make(chan struct{})
	var first atomic.Bool
	cache.fill.observer.beforeExtract = func([]usageSourceVersion) {
		if firstOnly && first.Swap(true) {
			return
		}
		started <- struct{}{}
		<-released
	}
	release := func() {
		select {
		case <-released:
		default:
			close(released)
		}
	}
	t.Cleanup(release)
	return started, release
}
