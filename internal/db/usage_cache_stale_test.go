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
	t.Run("idle session with held writer", func(t *testing.T) {
		database, cache := staleUsageFixture(t, true)
		started, release, batches := make(chan struct{}), make(chan struct{}), make(chan struct{}, 16)
		var first atomic.Bool
		cache.fill.observer.beforeExtract = func([]usageSourceVersion) {
			if !first.Swap(true) {
				close(started)
				<-release
			}
		}
		t.Cleanup(func() {
			select {
			case <-release:
			default:
				close(release)
			}
		})
		cache.fill.observer.afterNotificationBatch = func() { batches <- struct{}{} }
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
		close(release)
		waitUsageNotification(t, batches)
		assert.Equal(t, 17, liveWriteDailyInput(t, database, "project"))
	})
}

func TestUsageStaleRejectedStatesStayExact(t *testing.T) {
	for _, state := range []string{"no facts", "old marker", "future marker", "pending rollup", "pricing", "cursor", "standalone", "require fresh"} {
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
			case "future marker":
				changeStaleUsage(t, database, cache.fill.now().Add(time.Minute))
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
			started, release := make(chan struct{}, 4), make(chan struct{})
			cache.fill.observer.beforeExtract = func([]usageSourceVersion) {
				started <- struct{}{}
				<-release
			}
			t.Cleanup(func() {
				select {
				case <-release:
				default:
					close(release)
				}
			})
			changeStaleUsage(t, database, base.Add(-2*time.Second))
			assert.Equal(t, 10, liveWriteDailyInput(t, database, "project"))
			waitUsageNotification(t, started)
			elapsed.Store(int64(9 * time.Second))
			assert.Equal(t, 10, liveWriteDailyInput(t, database, "project"))
			switch overlap {
			case "request":
				installs, behind, err := cache.installedUsageRollups(t.Context(), snapshot, export.NewPricingResolver(snapshot.PricingRows))
				require.NoError(t, err)
				assert.NotNil(t, installs)
				assert.Empty(t, behind)
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
			close(release)
			require.NoError(t, <-errors)
			assert.Equal(t, 17, (<-result).Totals.InputTokens)
		})
	}
}

func TestUsageStaleCorrectedFutureTimestampStaysExact(t *testing.T) {
	for _, offset := range []time.Duration{time.Minute, 5 * time.Second} {
		t.Run(offset.String(), func(t *testing.T) {
			database, cache := staleUsageFixture(t, false)
			_, err := database.getWriter().Exec(t.Context(), `UPDATE sessions SET created_at = '2026-08-10T09:00:00Z', local_modified_at = ? WHERE id = 'session'`, cache.fill.now().Add(offset).UTC().Format(time.RFC3339Nano))
			require.NoError(t, err)
			require.Equal(t, 10, liveWriteDailyInput(t, database, "project"))
			require.NoError(t, database.StartUsageCacheBackfill(t.Context()))
			require.NoError(t, database.WaitUsageCacheBackfill(t.Context()))
			changeStaleUsage(t, database, cache.fill.now().Add(-time.Second))
			assert.Equal(t, 17, liveWriteDailyInput(t, database, "project"))
		})
	}
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
	for _, completion := range []string{"refill", "delete"} {
		t.Run(completion, func(t *testing.T) {
			database := testDB(t)
			for i := range 1280 {
				seedLiveWriteSession(t, database, fmt.Sprintf("turnover-%04d", i), "turnover", "2026-08-10T09:00:00Z", 0, 10)
			}
			require.NoError(t, database.StartUsageCacheBackfill(t.Context()))
			require.NoError(t, database.WaitUsageCacheBackfill(t.Context()))
			_, _, cache := prepareUsageRollupTest(t, database)
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
				if completion == "refill" {
					_, err = cache.fill.FillBackground(t.Context(), batch, 0)
				} else {
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

func TestUsageStaleDedupUsesCompatibleFacts(t *testing.T) {
	t.Run("facts pending with narrower read", func(t *testing.T) {
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
		batches := make(chan struct{}, 16)
		cache.fill.observer.afterNotificationBatch = func() { batches <- struct{}{} }
		var builds atomic.Int32
		cache.rollup.observer.beforeEnsure = func() { builds.Add(1) }
		daily, err = database.GetDailyUsage(t.Context(), filter)
		require.NoError(t, err)
		assert.Equal(t, 20, daily.Totals.OutputTokens)
		assert.Zero(t, builds.Load())
		waitUsageNotification(t, batches)
		daily, err = database.GetDailyUsage(t.Context(), filter)
		require.NoError(t, err)
		assert.Equal(t, 10, daily.Totals.OutputTokens)
	})
}
