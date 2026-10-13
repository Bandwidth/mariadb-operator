package environment

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestExternalReplTables(t *testing.T) {
	tests := []struct {
		name         string
		env          PodEnvironment
		wantFiltered []string
		wantIgnored  []string
	}{
		{
			name:         "not set",
			env:          PodEnvironment{},
			wantFiltered: nil,
			wantIgnored:  nil,
		},
		{
			name:         "filtered tables",
			env:          PodEnvironment{MariaDBExternalReplFilteredTables: "db.t1,db.t2"},
			wantFiltered: []string{"db.t1", "db.t2"},
			wantIgnored:  nil,
		},
		{
			name:         "ignored tables",
			env:          PodEnvironment{MariaDBExternalReplIgnoredTables: "db.t1,db.t2"},
			wantFiltered: nil,
			wantIgnored:  []string{"db.t1", "db.t2"},
		},
		{
			name:         "single ignored table",
			env:          PodEnvironment{MariaDBExternalReplIgnoredTables: "db.t1"},
			wantFiltered: nil,
			wantIgnored:  []string{"db.t1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if diff := cmp.Diff(tt.wantFiltered, tt.env.ExternalReplFilteredTables()); diff != "" {
				t.Errorf("unexpected filtered tables (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tt.wantIgnored, tt.env.ExternalReplIgnoredTables()); diff != "" {
				t.Errorf("unexpected ignored tables (-want +got):\n%s", diff)
			}
		})
	}
}
