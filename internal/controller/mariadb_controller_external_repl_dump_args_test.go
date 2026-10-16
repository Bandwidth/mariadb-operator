package controller

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestExternalReplDumpArgs(t *testing.T) {
	baseArgs := []string{
		"--master-data=1",
		"--gtid",
		"--verbose",
		"--single-transaction",
		"--ignore-table=mysql.global_priv",
	}

	tests := []struct {
		name          string
		ignoredTables []string
		want          []string
	}{
		{
			name:          "no ignored tables",
			ignoredTables: nil,
			want:          baseArgs,
		},
		{
			name:          "one ignored table",
			ignoredTables: []string{"db.t1"},
			want:          append(append([]string{}, baseArgs...), "--ignore-table=db.t1"),
		},
		{
			name:          "multiple ignored tables across schemas",
			ignoredTables: []string{"db1.t1", "db2.t2"},
			want:          append(append([]string{}, baseArgs...), "--ignore-table=db1.t1", "--ignore-table=db2.t2"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if diff := cmp.Diff(tt.want, externalReplDumpArgs(tt.ignoredTables)); diff != "" {
				t.Errorf("unexpected dump args (-want +got):\n%s", diff)
			}
		})
	}
}
