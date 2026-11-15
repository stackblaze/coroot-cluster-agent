package metrics

import (
	"testing"
	"time"

	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/coroot/coroot-cluster-agent/k8s"
)

func TestTargetFromPodHTTPDatabases(t *testing.T) {
	pod := func(annotations map[string]string) *k8s.Pod {
		p := &k8s.Pod{IP: "10.0.0.5", Annotations: annotations}
		p.Id.Namespace = "ns"
		p.Id.Name = "db-0"
		return p
	}

	os := TargetFromPod(pod(map[string]string{
		"coroot.com/opensearch-scrape":                                 "true",
		"coroot.com/opensearch-scrape-credentials-secret-name":         "os-coroot",
		"coroot.com/opensearch-scrape-credentials-secret-username-key": "username",
		"coroot.com/opensearch-scrape-credentials-secret-password-key": "password",
		"coroot.com/opensearch-scrape-param-tls":                       "skip-verify",
	}))
	if os == nil || os.Type != TargetTypeOpensearch || os.Addr != "10.0.0.5:9200" {
		t.Fatalf("opensearch target: %+v", os)
	}
	if s := os.CredentialsSecret; s.Namespace != "ns" || s.Name != "os-coroot" || s.UsernameKey != "username" || s.PasswordKey != "password" {
		t.Errorf("opensearch credentials secret: %+v", s)
	}
	if url, client, err := os.httpEndpoint(common.TLSCredentials{}, time.Second); err != nil || url != "https://10.0.0.5:9200" || client == nil {
		t.Errorf("opensearch endpoint: %q, %v", url, err)
	}

	ch := TargetFromPod(pod(map[string]string{
		"coroot.com/clickhouse-scrape":      "true",
		"coroot.com/clickhouse-scrape-port": "8124",
	}))
	if ch == nil || ch.Type != TargetTypeClickhouse || ch.Addr != "10.0.0.5:8124" {
		t.Fatalf("clickhouse target: %+v", ch)
	}
	if url, _, err := ch.httpEndpoint(common.TLSCredentials{}, time.Second); err != nil || url != "http://10.0.0.5:8124" {
		t.Errorf("clickhouse endpoint: %q, %v", url, err)
	}

	if got := TargetFromPod(pod(map[string]string{"coroot.com/clickhouse-scrape": "false"})); got != nil {
		t.Errorf("not annotated for scraping, got %+v", got)
	}
}
