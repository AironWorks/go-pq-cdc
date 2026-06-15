package slot

import (
	"context"
	goerrors "errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Trendyol/go-pq-cdc/internal/metric"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/go-playground/errors"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

var (
	ErrorSlotIsNotExists = goerrors.New("slot is not exists")
	ErrorNotConnected    = goerrors.New("slot is not connected")
	ErrorSlotClosed      = goerrors.New("slot is closed")
)

var typeMap = pgtype.NewMap()

type XLogUpdater interface {
	UpdateXLogPos(l pq.LSN)
}

type Slot struct {
	conn            pq.Connection
	replicationConn pq.Connection
	metric          metric.Metric
	logUpdater      XLogUpdater
	ticker          *time.Ticker
	statusSQL       string
	cfg             Config
	mu              sync.Mutex
	closed          atomic.Bool

	// Exact snapshot export. When holdExportedSnapshot is set (snapshot-enabled
	// pipelines), Create keeps the replication connection that ran
	// CREATE_REPLICATION_SLOT open and idle so the snapshot it exported stays
	// importable (SET TRANSACTION SNAPSHOT) by the snapshotter's workers. The
	// returned consistent_point is the exact boundary: the backfill reads state
	// at it and streaming starts from it, so neither overlaps nor drops events.
	// Releasing the connection (ReleaseExportedSnapshot) invalidates the export,
	// so it must stay open for the whole snapshot phase.
	heldReplicationConn  pq.Connection
	exportedSnapshotName string
	consistentPoint      pq.LSN
	holdExportedSnapshot bool
}

func NewSlot(replicationDSN, standardDSN string, cfg Config, m metric.Metric, updater XLogUpdater) *Slot {
	query := fmt.Sprintf("SELECT slot_name, slot_type, active, active_pid, restart_lsn, confirmed_flush_lsn, wal_status, PG_CURRENT_WAL_LSN() AS current_lsn FROM pg_replication_slots WHERE slot_name = '%s';", cfg.Name)

	return &Slot{
		cfg:             cfg,
		conn:            pq.NewConnectionTemplate(standardDSN),
		replicationConn: pq.NewConnectionTemplate(replicationDSN),
		statusSQL:       query,
		metric:          m,
		ticker:          time.NewTicker(time.Millisecond * cfg.SlotActivityCheckerInterval),
		logUpdater:      updater,
	}
}

func (s *Slot) Connect(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn.Connect(ctx)
}

func (s *Slot) Create(ctx context.Context) (*Info, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.conn.Connect(ctx); err != nil {
		return nil, errors.Wrap(err, "slot connect")
	}
	defer func() {
		_ = s.conn.Close(ctx)
	}()

	info, err := s.infoLocked(ctx)
	if err != nil {
		if !goerrors.Is(err, ErrorSlotIsNotExists) || !s.cfg.CreateIfNotExists {
			return nil, errors.Wrap(err, "replication slot info")
		}
	} else {
		logger.Warn("replication slot already exists")
		return info, nil
	}

	// Slot needs replication connection for CREATE_REPLICATION_SLOT command
	if err := s.createSlotWithReplicationConn(ctx); err != nil {
		return nil, err
	}

	logger.Info("replication slot created", "name", s.cfg.Name)

	return s.infoLocked(ctx)
}

func (s *Slot) createSlotWithReplicationConn(ctx context.Context) error {
	if err := s.replicationConn.Connect(ctx); err != nil {
		return errors.Wrap(err, "slot replication connect")
	}
	// When holding the exported snapshot we must NOT close (or run any further
	// command on) this connection — doing so invalidates the export. It's
	// released by ReleaseExportedSnapshot once the snapshot phase is done.
	closeConn := true
	defer func() {
		if closeConn {
			_ = s.replicationConn.Close(ctx)
		}
	}()

	sql := fmt.Sprintf("CREATE_REPLICATION_SLOT %s LOGICAL pgoutput", s.cfg.Name)
	resultReader := s.replicationConn.Exec(ctx, sql)
	results, err := resultReader.ReadAll()
	if err != nil {
		return errors.Wrap(err, "replication slot create result")
	}

	if err = resultReader.Close(); err != nil {
		return errors.Wrap(err, "replication slot create result reader close")
	}

	// CREATE_REPLICATION_SLOT returns one row: slot_name, consistent_point,
	// snapshot_name, output_plugin. consistent_point is the exact LSN the
	// exported snapshot reflects; the snapshot is importable while this
	// connection stays open.
	s.exportedSnapshotName, s.consistentPoint = parseCreateSlotResult(results)

	if s.holdExportedSnapshot && s.exportedSnapshotName != "" {
		closeConn = false
		s.heldReplicationConn = s.replicationConn
		logger.Info("replication slot snapshot exported",
			"name", s.cfg.Name,
			"snapshot", s.exportedSnapshotName,
			"consistentPoint", s.consistentPoint.String())
	}

	return nil
}

// SetHoldExportedSnapshot enables keeping the slot-creation replication
// connection open so the exported snapshot stays importable. Set by the
// connector for snapshot-enabled pipelines; the default CDC/non-snapshot path
// leaves it off and Create behaves exactly as before.
func (s *Slot) SetHoldExportedSnapshot(hold bool) {
	s.holdExportedSnapshot = hold
}

// ExportedSnapshotName returns the snapshot exported by the most recent slot
// creation, or "" if none (e.g. the slot already existed, or holding is off).
func (s *Slot) ExportedSnapshotName() string {
	return s.exportedSnapshotName
}

// ConsistentPoint is the LSN the exported snapshot reflects — the exact
// backfill/stream boundary.
func (s *Slot) ConsistentPoint() pq.LSN {
	return s.consistentPoint
}

// ReleaseExportedSnapshot closes the held replication connection, invalidating
// the exported snapshot. Call only after the snapshot phase has finished
// reading it. No-op if nothing is held.
func (s *Slot) ReleaseExportedSnapshot(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.heldReplicationConn == nil {
		return
	}
	_ = s.heldReplicationConn.Close(ctx)
	s.heldReplicationConn = nil
	s.exportedSnapshotName = ""
}

// Drop removes the replication slot. Used to recreate a fresh consistent
// snapshot when restarting an incomplete snapshot (the previous export died
// with its connection). Releases any held export first.
func (s *Slot) Drop(ctx context.Context) error {
	s.ReleaseExportedSnapshot(ctx)

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.conn.Connect(ctx); err != nil {
		return errors.Wrap(err, "slot connect for drop")
	}
	defer func() { _ = s.conn.Close(ctx) }()

	resultReader := s.conn.Exec(ctx, fmt.Sprintf("SELECT pg_drop_replication_slot('%s')", s.cfg.Name))
	if _, err := resultReader.ReadAll(); err != nil {
		return errors.Wrap(err, "drop replication slot")
	}
	return resultReader.Close()
}

func parseCreateSlotResult(results []*pgconn.Result) (snapshotName string, consistentPoint pq.LSN) {
	if len(results) == 0 || len(results[0].Rows) == 0 {
		return "", 0
	}
	result := results[0]
	for i, fd := range result.FieldDescriptions {
		if i >= len(result.Rows[0]) {
			break
		}
		switch fd.Name {
		case "consistent_point":
			if lsn, err := pq.ParseLSN(string(result.Rows[0][i])); err == nil {
				consistentPoint = lsn
			}
		case "snapshot_name":
			snapshotName = string(result.Rows[0][i])
		}
	}
	return snapshotName, consistentPoint
}

func (s *Slot) Info(ctx context.Context) (*Info, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed.Load() {
		return nil, ErrorSlotClosed
	}

	return s.infoLocked(ctx)
}

func (s *Slot) infoLocked(ctx context.Context) (*Info, error) {
	resultReader := s.conn.Exec(ctx, s.statusSQL)
	results, err := resultReader.ReadAll()
	if err != nil {
		return nil, errors.Wrap(err, "replication slot info result")
	}

	if len(results) == 0 || results[0].CommandTag.String() == "SELECT 0" {
		return nil, ErrorSlotIsNotExists
	}

	slotInfo, err := decodeSlotInfoResult(results[0])
	if err != nil {
		return nil, errors.Wrap(err, "replication slot info result decode")
	}

	if slotInfo.Type != Logical {
		return nil, errors.Newf("'%s' replication slot must be logical but it is %s", slotInfo.Name, slotInfo.Type)
	}

	return slotInfo, nil
}

func (s *Slot) Metrics(ctx context.Context) {
	for range s.ticker.C {
		if s.closed.Load() {
			return
		}

		slotInfo, err := s.Info(ctx)
		if err != nil {
			if goerrors.Is(err, ErrorSlotClosed) {
				return
			}
			logger.Error("slot metrics", "error", err)
			continue
		}

		s.metric.SetSlotActivity(slotInfo.Active)
		s.metric.SetSlotCurrentLSN(float64(slotInfo.CurrentLSN))
		s.metric.SetSlotConfirmedFlushLSN(float64(slotInfo.ConfirmedFlushLSN))
		s.metric.SetSlotRetainedWALSize(float64(slotInfo.RetainedWALSize))
		s.metric.SetSlotLag(float64(slotInfo.Lag))

		logger.Debug("slot metrics", "info", slotInfo)
	}
}

func (s *Slot) Close(ctx context.Context) {
	s.closed.Store(true)
	s.ticker.Stop()

	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.conn.IsClosed() {
		_ = s.conn.Close(ctx)
	}
}

func decodeSlotInfoResult(result *pgconn.Result) (*Info, error) {
	var slotInfo Info
	for i, fd := range result.FieldDescriptions {
		v, err := decodeTextColumnData(result.Rows[0][i], fd.DataTypeOID)
		if err != nil {
			return nil, err
		}

		if v == nil {
			continue
		}

		switch fd.Name {
		case "slot_name":
			slotInfo.Name = v.(string)
		case "slot_type":
			slotInfo.Type = Type(v.(string))
		case "active":
			slotInfo.Active = v.(bool)
		case "active_pid":
			slotInfo.ActivePID = v.(int32)
		case "restart_lsn":
			lsn, err := pq.ParseLSN(v.(string))
			if err != nil {
				return nil, errors.Wrap(err, "parse restart_lsn")
			}
			slotInfo.RestartLSN = lsn
		case "confirmed_flush_lsn":
			lsn, err := pq.ParseLSN(v.(string))
			if err != nil {
				return nil, errors.Wrap(err, "parse confirmed_flush_lsn")
			}
			slotInfo.ConfirmedFlushLSN = lsn
		case "wal_status":
			slotInfo.WalStatus = v.(string)
		case "current_lsn":
			lsn, err := pq.ParseLSN(v.(string))
			if err != nil {
				return nil, errors.Wrap(err, "parse current_lsn")
			}
			slotInfo.CurrentLSN = lsn
		}
	}

	slotInfo.RetainedWALSize = subtractLSN(slotInfo.CurrentLSN, slotInfo.RestartLSN)
	slotInfo.Lag = subtractLSN(slotInfo.CurrentLSN, slotInfo.ConfirmedFlushLSN)

	return &slotInfo, nil
}

func subtractLSN(current, previous pq.LSN) pq.LSN {
	if current <= previous {
		return 0
	}
	return current - previous
}

func decodeTextColumnData(data []byte, dataType uint32) (interface{}, error) {
	if dt, ok := typeMap.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(typeMap, dataType, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}
