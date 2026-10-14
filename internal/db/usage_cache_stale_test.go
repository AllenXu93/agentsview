//go:build fts5

package db

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.kenn.io/agentsview/internal/money"
)

func TestUsageStaleAppendRefreshesWithoutWaiting(t *testing.T) {
	t.Run("held writer", func(t *testing.T) {
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
		held := openRawUsageCacheTestDB(t, cache.path)
		defer held.Close()
		_, err := held.ExecContext(t.Context(), `BEGIN IMMEDIATE`)
		require.NoError(t, err)
		defer func() { _, _ = held.ExecContext(t.Context(), `ROLLBACK`) }()
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		for range 5 {
			daily, err := database.GetDailyUsage(ctx, UsageFilter{Timezone: "UTC"})
			require.NoError(t, err)
			assert.Equal(t, 10, daily.Totals.InputTokens)
			assert.Equal(t, 1, daily.SessionCounts.Total)
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
	for _, state := range []string{"no facts", "old marker", "old facts", "future marker", "pending rollup", "pricing", "cursor", "standalone"} {
		t.Run(state, func(t *testing.T) {
			database, cache := staleUsageFixture(t, state != "standalone")
			if state == "old facts" {
				_, err := database.getWriter().Exec(t.Context(), `UPDATE sessions SET created_at = '2026-08-10T09:00:00Z', local_modified_at = ? WHERE id = 'session'`, time.Now().Add(-time.Minute).UTC().Format(time.RFC3339Nano))
				require.NoError(t, err)
				require.Equal(t, 10, liveWriteDailyInput(t, database, "project"))
			}
			changeStaleUsage(t, database, time.Now().Add(-time.Second))
			want := 17
			switch state {
			case "no facts":
				_, err := cache.db.ExecContext(t.Context(), `DELETE FROM usage_cached_sessions WHERE session_id = 'session'`)
				require.NoError(t, err)
			case "old marker":
				changeStaleUsage(t, database, time.Now().Add(-time.Minute))
			case "future marker":
				changeStaleUsage(t, database, time.Now().Add(time.Minute))
			case "pending rollup":
				prepareUsageRollupTest(t, database)
			case "pricing":
				require.NoError(t, database.UpsertModelPricing([]ModelPricing{{ModelPattern: "model-a", InputPerMTok: money.Money{Microdollars: 1_000_000}}}))
			case "cursor":
				require.NoError(t, database.InsertCursorUsageEvents(t.Context(), []CursorUsageEvent{{OccurredAt: "2026-08-10T09:00:00Z", Model: "cursor-model", InputTokens: 1, DedupKey: "cursor-one"}}))
				want = 18
			case "standalone":
			}
			daily, err := database.GetDailyUsage(t.Context(), UsageFilter{Timezone: "UTC"})
			require.NoError(t, err)
			assert.Equal(t, want, daily.Totals.InputTokens)
		})
	}
}

func TestUsageStaleCorrectedFutureTimestampStaysExact(t *testing.T) {
	for _, offset := range []time.Duration{time.Minute, 5 * time.Second} {
		t.Run(offset.String(), func(t *testing.T) {
			database, _ := staleUsageFixture(t, false)
			_, err := database.getWriter().Exec(t.Context(), `UPDATE sessions SET created_at = '2026-08-10T09:00:00Z', local_modified_at = ? WHERE id = 'session'`, time.Now().Add(offset).UTC().Format(time.RFC3339Nano))
			require.NoError(t, err)
			require.Equal(t, 10, liveWriteDailyInput(t, database, "project"))
			require.NoError(t, database.StartUsageCacheBackfill(t.Context()))
			require.NoError(t, database.WaitUsageCacheBackfill(t.Context()))
			changeStaleUsage(t, database, time.Now().Add(-time.Second))
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
		_, err = database.getWriter().Exec(t.Context(), `UPDATE messages SET token_usage = '{}' WHERE session_id = 'a'; UPDATE sessions SET transcript_revision = 'changed', local_modified_at = ? WHERE id = 'a'`, time.Now().Add(-time.Second).UTC().Format(time.RFC3339Nano))
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
