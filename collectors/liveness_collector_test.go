package collectors

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"gopkg.in/h2non/gock.v1"
)

const mockAkamaiBaseURL = "https://akaa-baseurl-xxxxxxxxxxx-xxxxxxxxxxxxx.luna.akamaiapis.net"

type livenessConstMetricCollector struct {
	collector *GTMLivenessTrafficExporter
}

// GTMLivenessTrafficExporter's real Describe() doesn't know about these const metrics (which might not exist in the current scrape),
// and CollectAndCompare() will fail if they are not described, so shim this into place.
func (c livenessConstMetricCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- prometheus.NewDesc(
		"akamai_property_liveness_errors_datacenter_failures",
		"Number of datacenter failures (per domain, property, datacenter)",
		[]string{"domain", "property", "datacenter"},
		nil,
	)
	ch <- prometheus.NewDesc(
		"akamai_property_liveness_errors_datacenter_failure_duration",
		"Datacenter failure duration (per domain, property, datacenter)",
		[]string{"domain", "property", "datacenter"},
		nil,
	)
}

func (c livenessConstMetricCollector) Collect(ch chan<- prometheus.Metric) {
	c.collector.Collect(ch)
}

// these maps are declared globally in liveness_collector.go, so reset them between tests
func resetLivenessCollectorTestState() {
	gtmLivenessTrafficExporter = GTMLivenessTrafficExporter{}
	livenessDurationHistogramMap = make(map[string]map[string]map[int]prometheus.Histogram)
	livenessErrorsSummaryMap = make(map[string]map[string]map[int]prometheus.Summary)
}

func newTestLivenessCollector() (*GTMLivenessTrafficExporter, *prometheus.Registry) {
	resetLivenessCollectorTestState()

	registry := prometheus.NewRegistry()
	useTimestamp := false
	collector := NewLivenessTrafficCollector(
		context.Background(),
		mockV12Session(),
		registry,
		GTMMetricsConfig{
			Domains: []*DomainTraffic{
				{
					Name: "example.akadns.net",
					Liveness: []*LivenessTestConfig{
						{
							PropertyName: "www",
						},
					},
				},
			},
			UseTimestamp: &useTimestamp,
		},
		"akamai_",
		time.Date(2026, time.August, 13, 12, 55, 0, 0, time.UTC), // tstart
		time.Hour, // lookbackDuration
	)

	return collector, registry
}

func mockLivenessCollectorResponses(t *testing.T, body string) {
	t.Helper()

	gock.New(mockAkamaiBaseURL).
		Get("/gtm-api/v1/reports/liveness-tests/window").
		Reply(200).
		JSON(map[string]string{
			"start": "2026-08-10T00:00:00Z",
			"end":   "2026-08-13T23:59:59Z",
		})

	gock.New(mockAkamaiBaseURL).
		Get("/gtm-api/v1/reports/liveness-tests/domains/example.akadns.net/properties/www").
		MatchParam("date", "2026-08-13").
		Reply(200).
		BodyString(body)
}

func TestLivenessCollectorCollectAndCompareUsesLatestNewRowForConstMetrics(t *testing.T) {
	defer gock.Off()
	collector, _ := newTestLivenessCollector()
	wrappedCollector := livenessConstMetricCollector{collector: collector}

	mockLivenessCollectorResponses(t, `{
		"metadata": {
			"date": "2026-08-13",
			"domain": "example.akadns.net",
			"property": "www",
			"uri": "https://example.invalid/gtm-api/v1/reports/liveness-tests/domains/example.akadns.net/properties/www?date=2026-08-14"
		},
		"dataRows": [
			{
				"timestamp": "2026-08-13T13:00:00Z",
				"datacenters": [
					{
						"datacenterId": 3201,
						"agentIp": "204.1.136.239",
						"testName": "Our defences",
						"errorCode": 3101,
						"duration": 15,
						"nickname": "Winterfell",
						"trafficTargetName": "Winterfell - 1.2.3.4",
						"targetIp": "1.2.3.4"
					}
				]
			},
			{
				"timestamp": "2026-08-13T13:01:00Z",
				"datacenters": [
					{
						"datacenterId": 3201,
						"agentIp": "204.1.136.239",
						"testName": "Our defences",
						"errorCode": 3101,
						"duration": 60,
						"nickname": "Winterfell",
						"trafficTargetName": "Winterfell - 1.2.3.4",
						"targetIp": "1.2.3.4"
					}
				]
			},
			{
				"timestamp": "2026-08-13T13:01:05Z",
				"datacenters": [
					{
						"datacenterId": 3201,
						"agentIp": "204.1.136.239",
						"testName": "Our defences",
						"errorCode": 3101,
						"duration": 120,
						"nickname": "Winterfell",
						"trafficTargetName": "Winterfell - 1.2.3.4",
						"targetIp": "1.2.3.4"
					}
				]
			}
		]
	}`)

	// const metrics should report 1 for the counter and 120 for the duration since only the latest row is considered.
	expected := strings.NewReader(`# HELP akamai_property_liveness_errors_datacenter_failures Number of datacenter failures (per domain, property, datacenter)
# TYPE akamai_property_liveness_errors_datacenter_failures counter
akamai_property_liveness_errors_datacenter_failures{datacenter="3201",domain="example.akadns.net",property="www"} 1
# HELP akamai_property_liveness_errors_datacenter_failure_duration Datacenter failure duration (per domain, property, datacenter)
# TYPE akamai_property_liveness_errors_datacenter_failure_duration gauge
akamai_property_liveness_errors_datacenter_failure_duration{datacenter="3201",domain="example.akadns.net",property="www"} 120
`)

	require.NoError(t, testutil.CollectAndCompare(
		wrappedCollector,
		expected,
		"akamai_property_liveness_errors_datacenter_failures",
		"akamai_property_liveness_errors_datacenter_failure_duration",
	))
	require.True(t, gock.IsDone(), "expected mocked liveness endpoints to be called")
	// LastTimestamp should be updated to the latest row's timestamp
	require.Equal(
		t,
		time.Date(2026, time.August, 13, 13, 1, 5, 0, time.UTC),
		collector.LastTimestamp["example.akadns.net"]["www"],
	)
}

func TestLivenessCollectorCollectUpdatesHistogramAndSummaryForAllNewRows(t *testing.T) {
	defer gock.Off()
	collector, registry := newTestLivenessCollector()

	mockLivenessCollectorResponses(t, `{
		"metadata": {
				"date": "2026-08-13",
			"domain": "example.akadns.net",
			"property": "www",
				"uri": "https://example.invalid/gtm-api/v1/reports/liveness-tests/domains/example.akadns.net/properties/www?date=2026-08-14"
		},
		"dataRows": [
			{
				"timestamp": "2026-08-13T13:01:00Z",
				"datacenters": [
					{
						"datacenterId": 3201,
						"agentIp": "204.1.136.239",
						"testName": "Our defences",
						"errorCode": 3101,
						"duration": 60,
						"nickname": "Winterfell",
						"trafficTargetName": "Winterfell - 1.2.3.4",
						"targetIp": "1.2.3.4"
					}
				]
			},
			{
				"timestamp": "2026-08-13T13:01:05Z",
				"datacenters": [
					{
						"datacenterId": 3201,
						"agentIp": "204.1.136.239",
						"testName": "Our defences",
						"errorCode": 3101,
						"duration": 1860,
						"nickname": "Winterfell",
						"trafficTargetName": "Winterfell - 1.2.3.4",
						"targetIp": "1.2.3.4"
					}
				]
			},
			{
				"timestamp": "2026-08-13T15:02:00Z",
				"datacenters": [
					{
						"datacenterId": 3201,
						"agentIp": "204.1.136.239",
						"testName": "Our defences",
						"errorCode": 3101,
						"duration": 120,
						"nickname": "Winterfell",
						"trafficTargetName": "Winterfell - 1.2.3.4",
						"targetIp": "1.2.3.4"
					}
				]
			}
		]
	}`)

	collector.Collect(make(chan prometheus.Metric, 50)) // hit Collect() so that the histogram and summary are actually populated

	expected := strings.NewReader(`# HELP akamai_property_liveness_errors_duration_per_datacenter_histogram Histogram of datacenter error duration (per domain and property)
# TYPE akamai_property_liveness_errors_duration_per_datacenter_histogram histogram
akamai_property_liveness_errors_duration_per_datacenter_histogram_bucket{datacenter="3201",domain="example.akadns.net",property="www",le="60"} 1
akamai_property_liveness_errors_duration_per_datacenter_histogram_bucket{datacenter="3201",domain="example.akadns.net",property="www",le="1800"} 2
akamai_property_liveness_errors_duration_per_datacenter_histogram_bucket{datacenter="3201",domain="example.akadns.net",property="www",le="3600"} 3
akamai_property_liveness_errors_duration_per_datacenter_histogram_bucket{datacenter="3201",domain="example.akadns.net",property="www",le="7200"} 3
akamai_property_liveness_errors_duration_per_datacenter_histogram_bucket{datacenter="3201",domain="example.akadns.net",property="www",le="14400"} 3
akamai_property_liveness_errors_duration_per_datacenter_histogram_bucket{datacenter="3201",domain="example.akadns.net",property="www",le="+Inf"} 3
akamai_property_liveness_errors_duration_per_datacenter_histogram_sum{datacenter="3201",domain="example.akadns.net",property="www"} 2040
akamai_property_liveness_errors_duration_per_datacenter_histogram_count{datacenter="3201",domain="example.akadns.net",property="www"} 3
# HELP akamai_property_liveness_errors_errors_per_datacenter_summary Summary of datacenter errors (per domain and property)
# TYPE akamai_property_liveness_errors_errors_per_datacenter_summary summary
akamai_property_liveness_errors_errors_per_datacenter_summary_sum{datacenter="3201",domain="example.akadns.net",property="www"} 3
akamai_property_liveness_errors_errors_per_datacenter_summary_count{datacenter="3201",domain="example.akadns.net",property="www"} 3
`)

	require.NoError(t, testutil.GatherAndCompare(
		registry,
		expected,
		"akamai_property_liveness_errors_duration_per_datacenter_histogram",
		"akamai_property_liveness_errors_errors_per_datacenter_summary",
	))
	require.True(t, gock.IsDone(), "expected mocked liveness endpoints to be called")
	// LastTimestamp should be updated to the latest row's timestamp
	require.Equal(
		t,
		time.Date(2026, time.August, 13, 15, 2, 0, 0, time.UTC),
		collector.LastTimestamp["example.akadns.net"]["www"],
	)
}

func TestLivenessCollectorCollectSkipsOldRows(t *testing.T) {
	defer gock.Off()
	collector, registry := newTestLivenessCollector()
	wrappedCollector := livenessConstMetricCollector{collector: collector}

	mockLivenessCollectorResponses(t, `{
		"metadata": {
				"date": "2026-08-13",
			"domain": "example.akadns.net",
			"property": "www",
				"uri": "https://example.invalid/gtm-api/v1/reports/liveness-tests/domains/example.akadns.net/properties/www?date=2026-08-14"
		},
		"dataRows": [
			{
				"timestamp": "2026-08-13T10:01:00Z",
				"datacenters": [
					{
						"datacenterId": 3201,
						"agentIp": "204.1.136.239",
						"testName": "Our defences",
						"errorCode": 3101,
						"duration": 3605,
						"nickname": "Winterfell",
						"trafficTargetName": "Winterfell - 1.2.3.4",
						"targetIp": "1.2.3.4"
					}
				]
			},
			{
				"timestamp": "2026-08-13T10:01:05Z",
				"datacenters": [
					{
						"datacenterId": 3201,
						"agentIp": "204.1.136.239",
						"testName": "Our defences",
						"errorCode": 3101,
						"duration": 1860,
						"nickname": "Winterfell",
						"trafficTargetName": "Winterfell - 1.2.3.4",
						"targetIp": "1.2.3.4"
					}
				]
			},
			{
				"timestamp": "2026-08-13T13:01:05Z",
				"datacenters": [
					{
						"datacenterId": 3201,
						"agentIp": "204.1.136.239",
						"testName": "Our defences",
						"errorCode": 3101,
						"duration": 30,
						"nickname": "Winterfell",
						"trafficTargetName": "Winterfell - 1.2.3.4",
						"targetIp": "1.2.3.4"
					}
				]
			},
			{
				"timestamp": "2026-08-13T13:12:00Z",
				"datacenters": [
					{
						"datacenterId": 3201,
						"agentIp": "204.1.136.239",
						"testName": "Our defences",
						"errorCode": 3101,
						"duration": 60,
						"nickname": "Winterfell",
						"trafficTargetName": "Winterfell - 1.2.3.4",
						"targetIp": "1.2.3.4"
					}
				]
			}
		]
	}`)

	// const metrics should report 1 for the counter and 60 for the duration since only the latest row is considered.
	expectedConst := strings.NewReader(`# HELP akamai_property_liveness_errors_datacenter_failures Number of datacenter failures (per domain, property, datacenter)
# TYPE akamai_property_liveness_errors_datacenter_failures counter
akamai_property_liveness_errors_datacenter_failures{datacenter="3201",domain="example.akadns.net",property="www"} 1
# HELP akamai_property_liveness_errors_datacenter_failure_duration Datacenter failure duration (per domain, property, datacenter)
# TYPE akamai_property_liveness_errors_datacenter_failure_duration gauge
akamai_property_liveness_errors_datacenter_failure_duration{datacenter="3201",domain="example.akadns.net",property="www"} 60
`)

	require.NoError(t, testutil.CollectAndCompare(
		wrappedCollector,
		expectedConst,
		"akamai_property_liveness_errors_datacenter_failures",
		"akamai_property_liveness_errors_datacenter_failure_duration",
	))

	// the first 2 rows are older, and should not be counted. only durations within 60s should be observed, as the longer failures are too old.
	expectedRegistered := strings.NewReader(`# HELP akamai_property_liveness_errors_duration_per_datacenter_histogram Histogram of datacenter error duration (per domain and property)
# TYPE akamai_property_liveness_errors_duration_per_datacenter_histogram histogram
akamai_property_liveness_errors_duration_per_datacenter_histogram_bucket{datacenter="3201",domain="example.akadns.net",property="www",le="60"} 2
akamai_property_liveness_errors_duration_per_datacenter_histogram_bucket{datacenter="3201",domain="example.akadns.net",property="www",le="1800"} 2
akamai_property_liveness_errors_duration_per_datacenter_histogram_bucket{datacenter="3201",domain="example.akadns.net",property="www",le="3600"} 2
akamai_property_liveness_errors_duration_per_datacenter_histogram_bucket{datacenter="3201",domain="example.akadns.net",property="www",le="7200"} 2
akamai_property_liveness_errors_duration_per_datacenter_histogram_bucket{datacenter="3201",domain="example.akadns.net",property="www",le="14400"} 2
akamai_property_liveness_errors_duration_per_datacenter_histogram_bucket{datacenter="3201",domain="example.akadns.net",property="www",le="+Inf"} 2
akamai_property_liveness_errors_duration_per_datacenter_histogram_sum{datacenter="3201",domain="example.akadns.net",property="www"} 90
akamai_property_liveness_errors_duration_per_datacenter_histogram_count{datacenter="3201",domain="example.akadns.net",property="www"} 2
# HELP akamai_property_liveness_errors_errors_per_datacenter_summary Summary of datacenter errors (per domain and property)
# TYPE akamai_property_liveness_errors_errors_per_datacenter_summary summary
akamai_property_liveness_errors_errors_per_datacenter_summary_sum{datacenter="3201",domain="example.akadns.net",property="www"} 2
akamai_property_liveness_errors_errors_per_datacenter_summary_count{datacenter="3201",domain="example.akadns.net",property="www"} 2
`)

	require.NoError(t, testutil.GatherAndCompare(
		registry,
		expectedRegistered,
		"akamai_property_liveness_errors_duration_per_datacenter_histogram",
		"akamai_property_liveness_errors_errors_per_datacenter_summary",
	))
	require.True(t, gock.IsDone(), "expected mocked liveness endpoints to be called")
	// LastTimestamp should be updated to the latest row's timestamp
	require.Equal(
		t,
		time.Date(2026, time.August, 13, 13, 12, 0, 0, time.UTC),
		collector.LastTimestamp["example.akadns.net"]["www"],
	)
}

func TestLivenessCollectorCollectMultipleSeriesHistogramSummary(t *testing.T) {
	defer gock.Off()
	collector, registry := newTestLivenessCollector()

	mockLivenessCollectorResponses(t, `{
		"metadata": {
				"date": "2026-08-13",
			"domain": "example.akadns.net",
			"property": "www",
				"uri": "https://example.invalid/gtm-api/v1/reports/liveness-tests/domains/example.akadns.net/properties/www?date=2026-08-14"
		},
		"dataRows": [
			{
				"timestamp": "2026-08-13T13:01:00Z",
				"datacenters": [
					{
						"datacenterId": 42,
						"agentIp": "204.1.136.239",
						"testName": "Our defences",
						"errorCode": 3101,
						"duration": 7200,
						"nickname": "Winterfell",
						"trafficTargetName": "Winterfell - 1.2.3.4",
						"targetIp": "1.2.3.4"
					}
				]
			},
			{
				"timestamp": "2026-08-13T13:01:05Z",
				"datacenters": [
					{
						"datacenterId": 42,
						"agentIp": "204.1.136.239",
						"testName": "Our defences",
						"errorCode": 3101,
						"duration": 1860,
						"nickname": "Winterfell",
						"trafficTargetName": "Winterfell - 1.2.3.4",
						"targetIp": "1.2.3.4"
					}
				]
			},
			{
				"timestamp": "2026-08-13T13:01:10Z",
				"datacenters": [
					{
						"datacenterId": 3201,
						"agentIp": "204.1.136.239",
						"testName": "Our defences",
						"errorCode": 3101,
						"duration": 65,
						"nickname": "Winterfell",
						"trafficTargetName": "Winterfell - 1.2.3.4",
						"targetIp": "1.2.3.4"
					}
				]
			},
			{
				"timestamp": "2026-08-13T13:15:05Z",
				"datacenters": [
					{
						"datacenterId": 3201,
						"agentIp": "204.1.136.239",
						"testName": "Our defences",
						"errorCode": 3101,
						"duration": 60,
						"nickname": "Winterfell",
						"trafficTargetName": "Winterfell - 1.2.3.4",
						"targetIp": "1.2.3.4"
					}
				]
			}
		]
	}`)

	collector.Collect(make(chan prometheus.Metric, 50)) // hit Collect() so that the histogram and summary are actually populated

	expected := strings.NewReader(`# HELP akamai_property_liveness_errors_duration_per_datacenter_histogram Histogram of datacenter error duration (per domain and property)
# TYPE akamai_property_liveness_errors_duration_per_datacenter_histogram histogram
akamai_property_liveness_errors_duration_per_datacenter_histogram_bucket{datacenter="3201",domain="example.akadns.net",property="www",le="60"} 1
akamai_property_liveness_errors_duration_per_datacenter_histogram_bucket{datacenter="3201",domain="example.akadns.net",property="www",le="1800"} 2
akamai_property_liveness_errors_duration_per_datacenter_histogram_bucket{datacenter="3201",domain="example.akadns.net",property="www",le="3600"} 2
akamai_property_liveness_errors_duration_per_datacenter_histogram_bucket{datacenter="3201",domain="example.akadns.net",property="www",le="7200"} 2
akamai_property_liveness_errors_duration_per_datacenter_histogram_bucket{datacenter="3201",domain="example.akadns.net",property="www",le="14400"} 2
akamai_property_liveness_errors_duration_per_datacenter_histogram_bucket{datacenter="3201",domain="example.akadns.net",property="www",le="+Inf"} 2
akamai_property_liveness_errors_duration_per_datacenter_histogram_bucket{datacenter="42",domain="example.akadns.net",property="www",le="60"} 0
akamai_property_liveness_errors_duration_per_datacenter_histogram_bucket{datacenter="42",domain="example.akadns.net",property="www",le="1800"} 0
akamai_property_liveness_errors_duration_per_datacenter_histogram_bucket{datacenter="42",domain="example.akadns.net",property="www",le="3600"} 1
akamai_property_liveness_errors_duration_per_datacenter_histogram_bucket{datacenter="42",domain="example.akadns.net",property="www",le="7200"} 2
akamai_property_liveness_errors_duration_per_datacenter_histogram_bucket{datacenter="42",domain="example.akadns.net",property="www",le="14400"} 2
akamai_property_liveness_errors_duration_per_datacenter_histogram_bucket{datacenter="42",domain="example.akadns.net",property="www",le="+Inf"} 2
akamai_property_liveness_errors_duration_per_datacenter_histogram_sum{datacenter="3201",domain="example.akadns.net",property="www"} 125
akamai_property_liveness_errors_duration_per_datacenter_histogram_count{datacenter="3201",domain="example.akadns.net",property="www"} 2
akamai_property_liveness_errors_duration_per_datacenter_histogram_sum{datacenter="42",domain="example.akadns.net",property="www"} 9060
akamai_property_liveness_errors_duration_per_datacenter_histogram_count{datacenter="42",domain="example.akadns.net",property="www"} 2
# HELP akamai_property_liveness_errors_errors_per_datacenter_summary Summary of datacenter errors (per domain and property)
# TYPE akamai_property_liveness_errors_errors_per_datacenter_summary summary
akamai_property_liveness_errors_errors_per_datacenter_summary_sum{datacenter="3201",domain="example.akadns.net",property="www"} 2
akamai_property_liveness_errors_errors_per_datacenter_summary_count{datacenter="3201",domain="example.akadns.net",property="www"} 2
akamai_property_liveness_errors_errors_per_datacenter_summary_sum{datacenter="42",domain="example.akadns.net",property="www"} 2
akamai_property_liveness_errors_errors_per_datacenter_summary_count{datacenter="42",domain="example.akadns.net",property="www"} 2
`)

	require.NoError(t, testutil.GatherAndCompare(
		registry,
		expected,
		"akamai_property_liveness_errors_duration_per_datacenter_histogram",
		"akamai_property_liveness_errors_errors_per_datacenter_summary",
	))
	require.True(t, gock.IsDone(), "expected mocked liveness endpoints to be called")
	// LastTimestamp should be updated to the latest row's timestamp
	require.Equal(
		t,
		time.Date(2026, time.August, 13, 13, 15, 5, 0, time.UTC),
		collector.LastTimestamp["example.akadns.net"]["www"],
	)
}
