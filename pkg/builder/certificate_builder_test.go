package builder

import (
	"reflect"
	"testing"
	"time"

	cmmeta "github.com/cert-manager/cert-manager/pkg/apis/meta/v1"
	mariadbv1alpha1 "github.com/mariadb-operator/mariadb-operator/v26/api/v1alpha1"
	"k8s.io/apimachinery/pkg/types"
)

func TestBuildCertificate(t *testing.T) {
	builder := newDefaultTestBuilder(t)
	key := types.NamespacedName{
		Name:      "test-cert",
		Namespace: "test",
	}
	owner := &mariadbv1alpha1.MariaDB{}
	tests := []struct {
		name     string
		certOpts []CertOpt
		wantErr  bool
	}{
		{
			name: "missing key",
			certOpts: []CertOpt{
				WithOwner(owner),
				WithDNSnames([]string{"example.com"}),
				WithLifetime(24 * time.Hour),
				WithRenewBeforePercentage(50),
				WithIssuerRef(cmmeta.IssuerReference{Name: "test-issuer"}),
			},
			wantErr: true,
		},
		{
			name: "missing owner",
			certOpts: []CertOpt{
				WithKey(key),
				WithDNSnames([]string{"example.com"}),
				WithLifetime(24 * time.Hour),
				WithRenewBeforePercentage(50),
				WithIssuerRef(cmmeta.IssuerReference{Name: "test-issuer"}),
			},
			wantErr: true,
		},
		{
			name: "missing DNS names",
			certOpts: []CertOpt{
				WithKey(key),
				WithOwner(owner),
				WithLifetime(24 * time.Hour),
				WithRenewBeforePercentage(50),
				WithIssuerRef(cmmeta.IssuerReference{Name: "test-issuer"}),
			},
			wantErr: true,
		},
		{
			name: "missing lifetime",
			certOpts: []CertOpt{
				WithKey(key),
				WithOwner(owner),
				WithDNSnames([]string{"example.com"}),
				WithRenewBeforePercentage(50),
				WithIssuerRef(cmmeta.IssuerReference{Name: "test-issuer"}),
			},
			wantErr: false,
		},
		{
			name: "missing renew before percentage",
			certOpts: []CertOpt{
				WithKey(key),
				WithOwner(owner),
				WithDNSnames([]string{"example.com"}),
				WithLifetime(24 * time.Hour),
				WithIssuerRef(cmmeta.IssuerReference{Name: "test-issuer"}),
			},
			wantErr: false,
		},
		{
			name: "missing issuer ref",
			certOpts: []CertOpt{
				WithKey(key),
				WithOwner(owner),
				WithDNSnames([]string{"example.com"}),
				WithLifetime(24 * time.Hour),
				WithRenewBeforePercentage(50),
			},
			wantErr: true,
		},
		{
			name: "valid options",
			certOpts: []CertOpt{
				WithKey(key),
				WithOwner(owner),
				WithDNSnames([]string{"example.com"}),
				WithLifetime(24 * time.Hour),
				WithRenewBeforePercentage(50),
				WithIssuerRef(cmmeta.IssuerReference{Name: "test-issuer"}),
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := builder.BuildCertificate(tt.certOpts...)
			if (err != nil) != tt.wantErr {
				t.Errorf("BuildCertificate() error = %v, expectError %v", err, tt.wantErr)
			}
		})
	}
}

func TestBuildCertificateIPAddresses(t *testing.T) {
	builder := newDefaultTestBuilder(t)
	baseOpts := []CertOpt{
		WithKey(types.NamespacedName{Name: "test-cert", Namespace: "test"}),
		WithOwner(&mariadbv1alpha1.MariaDB{}),
		WithLifetime(24 * time.Hour),
		WithRenewBeforePercentage(50),
		WithIssuerRef(cmmeta.IssuerReference{Name: "test-issuer"}),
	}
	tests := []struct {
		name         string
		certOpts     []CertOpt
		wantDNSNames []string
		wantIPs      []string
	}{
		{
			name:         "without IP addresses",
			certOpts:     []CertOpt{WithDNSnames([]string{"example.com"})},
			wantDNSNames: []string{"example.com"},
			wantIPs:      nil,
		},
		{
			name: "only DNS names",
			certOpts: []CertOpt{
				WithDNSnames([]string{"example.com", "extra.example.com"}),
				WithIPAddresses(nil),
			},
			wantDNSNames: []string{"example.com", "extra.example.com"},
			wantIPs:      nil,
		},
		{
			name: "only IP addresses",
			certOpts: []CertOpt{
				WithDNSnames([]string{"example.com"}),
				WithIPAddresses([]string{"192.168.117.19"}),
			},
			wantDNSNames: []string{"example.com"},
			wantIPs:      []string{"192.168.117.19"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cert, err := builder.BuildCertificate(append(append([]CertOpt{}, baseOpts...), tt.certOpts...)...)
			if err != nil {
				t.Fatalf("BuildCertificate() unexpected error: %v", err)
			}
			if !reflect.DeepEqual(cert.Spec.DNSNames, tt.wantDNSNames) {
				t.Errorf("unexpected DNS names, got: %v, want: %v", cert.Spec.DNSNames, tt.wantDNSNames)
			}
			if !reflect.DeepEqual(cert.Spec.IPAddresses, tt.wantIPs) {
				t.Errorf("unexpected IP addresses, got: %v, want: %v", cert.Spec.IPAddresses, tt.wantIPs)
			}
			if cert.Spec.CommonName != tt.wantDNSNames[0] {
				t.Errorf("unexpected common name, got: %v, want: %v", cert.Spec.CommonName, tt.wantDNSNames[0])
			}
		})
	}
}
