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

// Creates a collector and registry for a test, with a configurable tstart (the first LastTimestamp) and a single property.
func newTestLivenessCollector(tstart time.Time) (*GTMLivenessTrafficExporter, *prometheus.Registry) {
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
		tstart,
		time.Hour, // lookbackDuration
	)

	return collector, registry
}

// mocks responses from the liveness-test reporting API for a single day based on the date passed in YYYY-MM-DD format
func mockLivenessCollectorResponsesSingleDay(t *testing.T, date string, body string) {
	t.Helper()

	gock.New(mockAkamaiBaseURL).
		Get("/gtm-api/v1/reports/liveness-tests/window").
		Reply(200).
		JSON(map[string]string{
			"start": date + "T00:00:00Z",
			"end":   date + "T23:59:59Z",
		})

	gock.New(mockAkamaiBaseURL).
		Get("/gtm-api/v1/reports/liveness-tests/domains/example.akadns.net/properties/www").
		MatchParam("date", date).
		Reply(200).
		BodyString(body)
}

func TestLivenessCollectorCollectAndCompareUsesLatestNewRowForConstMetrics(t *testing.T) {
	defer gock.Off()
	collector, _ := newTestLivenessCollector(time.Date(2026, time.August, 13, 12, 55, 0, 0, time.UTC))
	wrappedCollector := livenessConstMetricCollector{collector: collector}

	mockLivenessCollectorResponsesSingleDay(t, "2026-08-13", `{
		"metadata": {
			"date": "2026-08-13",
			"domain": "example.akadns.net",
			"property": "www",
			"uri": "https://example.invalid/gtm-api/v1/reports/liveness-tests/domains/example.akadns.net/properties/www?date=2026-08-13"
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
	collector, registry := newTestLivenessCollector(time.Date(2026, time.August, 13, 12, 55, 0, 0, time.UTC))

	mockLivenessCollectorResponsesSingleDay(t, "2026-08-13", `{
		"metadata": {
				"date": "2026-08-13",
			"domain": "example.akadns.net",
			"property": "www",
				"uri": "https://example.invalid/gtm-api/v1/reports/liveness-tests/domains/example.akadns.net/properties/www?date=2026-08-13"
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
	collector, registry := newTestLivenessCollector(time.Date(2026, time.August, 13, 12, 55, 0, 0, time.UTC))
	wrappedCollector := livenessConstMetricCollector{collector: collector}

	mockLivenessCollectorResponsesSingleDay(t, "2026-08-13", `{
		"metadata": {
				"date": "2026-08-13",
			"domain": "example.akadns.net",
			"property": "www",
				"uri": "https://example.invalid/gtm-api/v1/reports/liveness-tests/domains/example.akadns.net/properties/www?date=2026-08-13"
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
	collector, registry := newTestLivenessCollector(time.Date(2026, time.August, 13, 12, 55, 0, 0, time.UTC))

	mockLivenessCollectorResponsesSingleDay(t, "2026-08-13", `{
		"metadata": {
				"date": "2026-08-13",
			"domain": "example.akadns.net",
			"property": "www",
				"uri": "https://example.invalid/gtm-api/v1/reports/liveness-tests/domains/example.akadns.net/properties/www?date=2026-08-13"
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

func TestPickLivenessReportDate(t *testing.T) {
	testCases := []struct {
		name              string
		start             time.Time
		windowStart       time.Time
		windowEnd         time.Time
		expectedDate      string
		expectedReportEnd time.Time
	}{
		{
			name:              "StartWithinWindowDayBehindReportEndDayEnd",
			start:             time.Date(2026, time.August, 13, 12, 55, 0, 0, time.UTC),
			windowStart:       time.Date(2026, time.August, 12, 0, 0, 0, 0, time.UTC),
			windowEnd:         time.Date(2026, time.August, 14, 15, 25, 5, 0, time.UTC),
			expectedDate:      "2026-08-13",
			expectedReportEnd: time.Date(2026, time.August, 13, 23, 59, 59, 0, time.UTC),
		},
		{
			name:              "StartWithinWindowLatestDayReportEndWindowEnd",
			start:             time.Date(2026, time.August, 14, 14, 23, 0, 0, time.UTC),
			windowStart:       time.Date(2026, time.August, 12, 0, 0, 0, 0, time.UTC),
			windowEnd:         time.Date(2026, time.August, 14, 15, 25, 5, 0, time.UTC),
			expectedDate:      "2026-08-14",
			expectedReportEnd: time.Date(2026, time.August, 14, 15, 25, 5, 0, time.UTC),
		},
		{
			name:              "StartAfterWindowEndReportEndWindowEnd",
			start:             time.Date(2026, time.August, 16, 8, 0, 0, 0, time.UTC),
			windowStart:       time.Date(2026, time.August, 12, 0, 0, 0, 0, time.UTC),
			windowEnd:         time.Date(2026, time.August, 14, 15, 25, 5, 0, time.UTC),
			expectedDate:      "2026-08-14",
			expectedReportEnd: time.Date(2026, time.August, 14, 15, 25, 5, 0, time.UTC),
		},
		{
			name:              "StartBeforeWindowStartReportEndDayEnd",
			start:             time.Date(2026, time.August, 10, 8, 0, 0, 0, time.UTC),
			windowStart:       time.Date(2026, time.August, 12, 0, 0, 0, 0, time.UTC),
			windowEnd:         time.Date(2026, time.August, 14, 15, 25, 5, 0, time.UTC),
			expectedDate:      "2026-08-12",
			expectedReportEnd: time.Date(2026, time.August, 12, 23, 59, 59, 0, time.UTC),
		},
		{
			name:              "StartBeforeWindowStartReportEndWindowEnd",
			start:             time.Date(2026, time.August, 10, 8, 0, 0, 0, time.UTC),
			windowStart:       time.Date(2026, time.August, 12, 0, 0, 0, 0, time.UTC),
			windowEnd:         time.Date(2026, time.August, 12, 15, 25, 5, 0, time.UTC),
			expectedDate:      "2026-08-12",
			expectedReportEnd: time.Date(2026, time.August, 12, 15, 25, 5, 0, time.UTC),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			reportDate, reportEnd, err := pickLivenessReportDate(tc.start, tc.windowStart, tc.windowEnd)

			require.NoError(t, err)
			require.Equal(t, tc.expectedDate, reportDate)
			require.Equal(t, tc.expectedReportEnd, reportEnd)
		})
	}
}

func TestLivenessCollectorCollectAdvancesDayBoundary(t *testing.T) {
	defer gock.Off()
	// lastReportRequested falls within window, but is the day before windowEnd.
	collector, _ := newTestLivenessCollector(time.Date(2026, time.August, 11, 23, 30, 0, 0, time.UTC))
	wrappedCollector := livenessConstMetricCollector{collector: collector}

	// The first Collect() will request Aug 11 report (date of LastReportEndTime), which contains an already processed row
	// This will set LastReportEndTime to the end of Aug 11, and the next Collect() will request the next day.
	gock.New(mockAkamaiBaseURL).
		Get("/gtm-api/v1/reports/liveness-tests/window").
		Reply(200).
		JSON(map[string]string{
			"start": "2026-08-11T00:00:00Z",
			"end":   "2026-08-12T13:45:04Z",
		})

	gock.New(mockAkamaiBaseURL).
		Get("/gtm-api/v1/reports/liveness-tests/domains/example.akadns.net/properties/www").
		MatchParam("date", "2026-08-11").
		Reply(200).
		BodyString(`{ 
			"metadata": {
			"date": "2026-08-11",
			"domain": "example.akadns.net",
			"property": "www",
				"uri": "https://example.invalid/gtm-api/v1/reports/liveness-tests/domains/example.akadns.net/properties/www?date=2026-08-11"
		},
		"dataRows": [
			{
				"timestamp": "2026-08-11T23:30:00Z",
				"datacenters": [
					{
						"datacenterId": 3201,
						"agentIp": "204.1.136.239",
						"testName": "Our defences",
						"errorCode": 3101,
						"duration": 1297,
						"nickname": "Winterfell",
						"trafficTargetName": "Winterfell - 1.2.3.4",
						"targetIp": "1.2.3.4"
					}
				]
			}
		]
	}`)

	// The const metrics should not appear as the row is not older than lastTimestamp
	require.NoError(t, testutil.CollectAndCompare(
		wrappedCollector,
		strings.NewReader(``), // expect no output → metric must be absent
		"akamai_property_liveness_errors_datacenter_failures",
		"akamai_property_liveness_errors_datacenter_failure_duration",
	))

	// The second Collect() should be for Aug 12 (the next day)
	gock.New(mockAkamaiBaseURL).
		Get("/gtm-api/v1/reports/liveness-tests/window").
		Reply(200).
		JSON(map[string]string{
			"start": "2026-08-11T00:00:00Z",
			"end":   "2026-08-12T13:45:04Z",
		})

	gock.New(mockAkamaiBaseURL).
		Get("/gtm-api/v1/reports/liveness-tests/domains/example.akadns.net/properties/www").
		MatchParam("date", "2026-08-12").
		Reply(200).
		BodyString(`{ 
			"metadata": {
			"date": "2026-08-12",
			"domain": "example.akadns.net",
			"property": "www",
				"uri": "https://example.invalid/gtm-api/v1/reports/liveness-tests/domains/example.akadns.net/properties/www?date=2026-08-11"
		},
		"dataRows": [
			{
				"timestamp": "2026-08-12T09:16:05Z",
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
			}
		]
	}`)

	// The const metrics should reflect the new failure
	expectedConst := strings.NewReader(`# HELP akamai_property_liveness_errors_datacenter_failures Number of datacenter failures (per domain, property, datacenter)
# TYPE akamai_property_liveness_errors_datacenter_failures counter
akamai_property_liveness_errors_datacenter_failures{datacenter="3201",domain="example.akadns.net",property="www"} 1
# HELP akamai_property_liveness_errors_datacenter_failure_duration Datacenter failure duration (per domain, property, datacenter)
# TYPE akamai_property_liveness_errors_datacenter_failure_duration gauge
akamai_property_liveness_errors_datacenter_failure_duration{datacenter="3201",domain="example.akadns.net",property="www"} 30
`)

	require.NoError(t, testutil.CollectAndCompare(
		wrappedCollector,
		expectedConst,
		"akamai_property_liveness_errors_datacenter_failures",
		"akamai_property_liveness_errors_datacenter_failure_duration",
	))

	require.True(t, gock.IsDone(), "expected mocked liveness endpoints to be called")
	// LastTimestamp should be updated to the timestamp of the single row in day 2
	require.Equal(
		t,
		time.Date(2026, time.August, 12, 9, 16, 5, 0, time.UTC),
		collector.LastTimestamp["example.akadns.net"]["www"],
	)
	// LastReportEndTime should be updated to the windowEnd of the report.
	require.Equal(
		t,
		time.Date(2026, time.August, 12, 13, 45, 4, 0, time.UTC),
		collector.LastReportEndTime["example.akadns.net"]["www"],
	)
}
