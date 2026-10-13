package webhook_test

import (
	"testing"

	mariadbv1alpha1 "github.com/mariadb-operator/mariadb-operator/v26/api/v1alpha1"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/webhook"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestInmutableWebhookIgnoredReplicaTables(t *testing.T) {
	inmutableWebhook := webhook.NewInmutableWebhook(
		webhook.WithTagName("webhook"),
	)
	newMariaDB := func(ignored []string) *mariadbv1alpha1.MariaDB {
		return &mariadbv1alpha1.MariaDB{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test",
			},
			Spec: mariadbv1alpha1.MariaDBSpec{
				Replication: &mariadbv1alpha1.Replication{
					Enabled: true,
					ReplicationSpec: mariadbv1alpha1.ReplicationSpec{
						ReplicaFromExternal: &mariadbv1alpha1.ReplicaFromExternal{
							IgnoredReplicaTables: ignored,
						},
					},
				},
			},
		}
	}

	tests := []struct {
		name    string
		old     []string
		new     []string
		wantErr bool
	}{
		{
			name:    "unchanged",
			old:     []string{"db.t1"},
			new:     []string{"db.t1"},
			wantErr: false,
		},
		{
			name:    "not set",
			old:     nil,
			new:     nil,
			wantErr: false,
		},
		{
			name:    "table added",
			old:     []string{"db.t1"},
			new:     []string{"db.t1", "db.t2"},
			wantErr: true,
		},
		{
			name:    "table replaced",
			old:     []string{"db.t1"},
			new:     []string{"db.t2"},
			wantErr: true,
		},
		{
			name:    "set on an existing cluster",
			old:     nil,
			new:     []string{"db.t1"},
			wantErr: true,
		},
		{
			name:    "removed",
			old:     []string{"db.t1"},
			new:     nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := inmutableWebhook.ValidateUpdate(newMariaDB(tt.new), newMariaDB(tt.old))
			if tt.wantErr && err == nil {
				t.Error("expect error to have occurred, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("expect error to not have occurred, got: %v", err)
			}
		})
	}
}
