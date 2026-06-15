package slot

import (
	"testing"

	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestParseCreateSlotResult(t *testing.T) {
	fields := []pgconn.FieldDescription{
		{Name: "slot_name"},
		{Name: "consistent_point"},
		{Name: "snapshot_name"},
		{Name: "output_plugin"},
	}

	tests := []struct {
		name         string
		results      []*pgconn.Result
		wantSnapshot string
		wantLSN      string // "" means zero
	}{
		{
			name: "full row",
			results: []*pgconn.Result{{
				FieldDescriptions: fields,
				Rows:              [][][]byte{{[]byte("aw_rollup_slot"), []byte("0/1A2B3C4"), []byte("00000003-00000002-1"), []byte("pgoutput")}},
			}},
			wantSnapshot: "00000003-00000002-1",
			wantLSN:      "0/1A2B3C4",
		},
		{
			name:         "no results",
			results:      nil,
			wantSnapshot: "",
			wantLSN:      "",
		},
		{
			name: "no rows",
			results: []*pgconn.Result{{
				FieldDescriptions: fields,
				Rows:              [][][]byte{},
			}},
			wantSnapshot: "",
			wantLSN:      "",
		},
		{
			name: "column order independent",
			results: []*pgconn.Result{{
				FieldDescriptions: []pgconn.FieldDescription{{Name: "snapshot_name"}, {Name: "consistent_point"}},
				Rows:              [][][]byte{{[]byte("snap-xyz"), []byte("0/5")}},
			}},
			wantSnapshot: "snap-xyz",
			wantLSN:      "0/5",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotSnapshot, gotLSN := parseCreateSlotResult(tt.results)
			if gotSnapshot != tt.wantSnapshot {
				t.Errorf("snapshot = %q, want %q", gotSnapshot, tt.wantSnapshot)
			}
			var want pq.LSN
			if tt.wantLSN != "" {
				parsed, err := pq.ParseLSN(tt.wantLSN)
				if err != nil {
					t.Fatalf("bad test LSN %q: %v", tt.wantLSN, err)
				}
				want = parsed
			}
			if gotLSN != want {
				t.Errorf("consistentPoint = %s, want %s", gotLSN.String(), want.String())
			}
		})
	}
}
