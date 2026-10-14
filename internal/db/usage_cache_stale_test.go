//go:build fts5

package db

import (
	"context"
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
