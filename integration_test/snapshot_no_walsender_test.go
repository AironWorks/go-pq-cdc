package integration

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// walsenderDuringSnapshot runs an initial snapshot and samples, throughout the
// snapshot, how many walsender connections the slot holds. It returns the max
// observed and the number of samples taken during the snapshot.
//
// The handler updates atomics directly (no buffered channel) so it never
// backpressures the snapshot — that backpressure is what made the old 100-row
// version finish inside a single ticker interval and sample zero times. A large
// bulk-inserted dataset with tiny chunks keeps the snapshot long enough to be
// sampled many times, making the assertion deterministic.
func walsenderDuringSnapshot(t *testing.T, slotSuffix string, consistent bool) (maxWalsenders, samples, dataRows int, cdcWorks bool, jobLSN string) {
	t.Helper()
	ctx := context.Background()

	tableName := "snapshot_walsender_" + slotSuffix
	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_walsender_" + slotSuffix
	cdcCfg.Publication.Name = "pub_walsender_" + slotSuffix
	cdcCfg.Publication.Tables = publication.Tables{
		{Name: tableName, Schema: "public", ReplicaIdentity: publication.ReplicaIdentityFull},
	}
	cdcCfg.Snapshot.Enabled = true
	cdcCfg.Snapshot.Mode = "initial"
	cdcCfg.Snapshot.ConsistentSnapshot = consistent
	cdcCfg.Snapshot.ChunkSize = 5 // many small chunks => snapshot spans many samples

	const rowCount = 5000

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)
	require.NoError(t, createTestTable(ctx, postgresConn, tableName))
	require.NoError(t, pgExec(ctx, postgresConn, fmt.Sprintf(
		"INSERT INTO %s(id, name, age) SELECT g, 'User_'||g, 20+(g%%50) FROM generate_series(1,%d) g",
		tableName, rowCount)))

	var begin, end, cdcInsert atomic.Bool
	var data atomic.Int64
	handlerFunc := func(c *replication.ListenerContext) {
		switch m := c.Message.(type) {
		case *format.Snapshot:
			switch m.EventType {
			case format.SnapshotEventTypeBegin:
				begin.Store(true)
			case format.SnapshotEventTypeData:
				data.Add(1)
			case format.SnapshotEventTypeEnd:
				end.Store(true)
			}
		case *format.Insert:
			cdcInsert.Store(true)
		}
		_ = c.Ack()
	}

	connector, err := cdc.NewConnector(ctx, cdcCfg, handlerFunc)
	require.NoError(t, err)
	t.Cleanup(func() {
		connector.Close()
		postgresConn.Close(ctx)
		cleanupSnapshotTest(t, ctx, tableName, cdcCfg.Slot.Name, cdcCfg.Publication.Name)
	})

	checkConn, err := newPostgresConn()
	require.NoError(t, err)
	defer checkConn.Close(ctx)

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(25 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				if !begin.Load() || end.Load() {
					continue
				}
				count, err := countWalsendersForSlot(ctx, checkConn, cdcCfg.Slot.Name)
				if err != nil || end.Load() {
					continue
				}
				samples++
				if count > maxWalsenders {
					maxWalsenders = count
				}
			}
		}
	}()

	go connector.Start(ctx)

	deadline := time.After(60 * time.Second)
	for !end.Load() {
		select {
		case <-deadline:
			close(stop)
			<-done
			t.Fatalf("timeout waiting for snapshot end; begin=%v data=%d", begin.Load(), data.Load())
		case <-time.After(20 * time.Millisecond):
		}
	}
	close(stop)
	<-done

	// CDC after snapshot: insert and confirm it streams.
	for i := 1; i <= 3; i++ {
		require.NoError(t, pgExec(ctx, postgresConn, fmt.Sprintf(
			"INSERT INTO %s(id, name, age) VALUES(%d, 'CDC_%d', 30)", tableName, 100000+i, i)))
	}
	cdcDeadline := time.After(10 * time.Second)
	for !cdcInsert.Load() {
		select {
		case <-cdcDeadline:
			goto cdcDone
		case <-time.After(20 * time.Millisecond):
		}
	}
cdcDone:

	jobLSN = execScalar(t, ctx, postgresConn,
		fmt.Sprintf("SELECT snapshot_lsn FROM cdc_snapshot_job WHERE slot_name = '%s'", cdcCfg.Slot.Name))

	return maxWalsenders, samples, int(data.Load()), cdcInsert.Load(), jobLSN
}

// TestNoWalsenderDuringSnapshot validates Issue #56 for the default mode
// (ConsistentSnapshot off): the snapshot must hold ZERO walsenders, since it
// exports via pg_export_snapshot on a normal connection.
func TestNoWalsenderDuringSnapshot(t *testing.T) {
	const rowCount = 5000
	maxWal, samples, data, cdcWorks, jobLSN := walsenderDuringSnapshot(t, "default", false)

	require.Greater(t, samples, 0, "walsender count must be sampled at least once during the snapshot")
	assert.Equal(t, 0, maxWal, "default mode must hold zero walsenders during snapshot (Issue #56)")
	assert.Equal(t, rowCount, data, "all rows should be captured by the snapshot")
	assert.True(t, cdcWorks, "CDC streaming should work after snapshot")
	assert.NotEmpty(t, jobLSN, "snapshot LSN should be recorded")
	t.Logf("✅ default: maxWalsenders=%d over %d samples (want 0), rows=%d", maxWal, samples, data)
}

// TestConsistentSnapshotHoldsOneWalsender is the counterpart for the exact mode
// (ConsistentSnapshot on). The exact boundary requires keeping the slot's
// exported snapshot open, which holds exactly ONE walsender for the whole
// snapshot — the documented trade-off against Issue #56.
func TestConsistentSnapshotHoldsOneWalsender(t *testing.T) {
	const rowCount = 5000
	maxWal, samples, data, cdcWorks, jobLSN := walsenderDuringSnapshot(t, "consistent", true)

	require.Greater(t, samples, 0, "walsender count must be sampled at least once during the snapshot")
	assert.Equal(t, 1, maxWal, "exact mode holds exactly one walsender (the exported snapshot) during snapshot")
	assert.Equal(t, rowCount, data, "all rows should be captured by the snapshot")
	assert.True(t, cdcWorks, "CDC streaming should work after snapshot")
	assert.NotEmpty(t, jobLSN, "snapshot LSN (slot consistent_point) should be recorded")
	t.Logf("✅ exact: maxWalsenders=%d over %d samples (want 1), rows=%d, snapshotLSN=%s", maxWal, samples, data, jobLSN)
}

// TestConsistentSnapshotRefusesWhenSlotExists verifies exact mode never
// silently falls back to a non-exact snapshot. When the slot already exists
// there is no fresh exported snapshot, and falling back to pg_export_snapshot
// would double-count for additive consumers — so the connector must refuse
// (no snapshot completes) rather than proceed. The pre-existing slot here makes
// the old fall-back path complete a (wrong) snapshot; the fix makes it refuse.
func TestConsistentSnapshotRefusesWhenSlotExists(t *testing.T) {
	ctx := context.Background()

	tableName := "snapshot_refuse_test"
	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_refuse_exact"
	cdcCfg.Publication.Name = "pub_refuse_exact"
	cdcCfg.Publication.Tables = publication.Tables{
		{Name: tableName, Schema: "public", ReplicaIdentity: publication.ReplicaIdentityFull},
	}
	cdcCfg.Snapshot.Enabled = true
	cdcCfg.Snapshot.Mode = "initial"
	cdcCfg.Snapshot.ConsistentSnapshot = true

	conn, err := newPostgresConn()
	require.NoError(t, err)
	require.NoError(t, createTestTable(ctx, conn, tableName))
	require.NoError(t, pgExec(ctx, conn, fmt.Sprintf(
		"INSERT INTO %s(id, name, age) SELECT g, 'n'||g, 20 FROM generate_series(1,50) g", tableName)))

	// Pre-create the slot so the connector can't obtain a fresh exported snapshot.
	require.NoError(t, pgExec(ctx, conn, fmt.Sprintf(
		"SELECT pg_create_logical_replication_slot('%s', 'pgoutput')", cdcCfg.Slot.Name)))

	var snapshotEnded atomic.Bool
	handlerFunc := func(c *replication.ListenerContext) {
		if m, ok := c.Message.(*format.Snapshot); ok && m.EventType == format.SnapshotEventTypeEnd {
			snapshotEnded.Store(true)
		}
		_ = c.Ack()
	}
	connector, err := cdc.NewConnector(ctx, cdcCfg, handlerFunc)
	require.NoError(t, err)
	t.Cleanup(func() {
		connector.Close()
		conn.Close(ctx)
		cleanupSnapshotTest(t, ctx, tableName, cdcCfg.Slot.Name, cdcCfg.Publication.Name)
	})

	// Start returns once snapshot prepare gives up (it refuses on every retry
	// because the slot already exists). We wait for that return rather than a
	// fixed sleep, so teardown doesn't race the still-retrying goroutine.
	done := make(chan struct{})
	go func() { connector.Start(ctx); close(done) }()

	select {
	case <-done:
	case <-time.After(40 * time.Second):
		t.Fatal("connector did not stop; exact mode should refuse and return when the slot exists")
	}

	assert.False(t, snapshotEnded.Load(),
		"exact mode must refuse when the slot already exists, not silently complete a non-exact snapshot")
}

func execScalar(t *testing.T, ctx context.Context, conn pq.Connection, query string) string {
	t.Helper()
	results, err := execQuery(ctx, conn, query)
	require.NoError(t, err)
	if len(results) == 0 || len(results[0].Rows) == 0 || len(results[0].Rows[0]) == 0 {
		return ""
	}
	return string(results[0].Rows[0][0])
}
