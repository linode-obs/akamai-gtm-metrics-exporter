// Copyright 2021 Akamai Technologies, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package collectors

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	// Akamai v12 Package
	"github.com/akamai/AkamaiOPEN-edgegrid-golang/v12/pkg/session"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

var (
	gtmLivenessTrafficExporter GTMLivenessTrafficExporter
	durationBuckets            = []float64{60, 1800, 3600, 7200, 14400}
)

type GTMLivenessTrafficExporter struct {
	GTMConfig                GTMMetricsConfig
	LivenessMetricPrefix     string
	LivenessLookbackDuration time.Duration
	LastTimestamp            map[string]map[string]time.Time // index by domain, liveness
	LivenessRegistry         *prometheus.Registry
	AkamaiSession            session.Session // v12 Authenticated Session
	ctx                      context.Context
}

func NewLivenessTrafficCollector(ctx context.Context, sess session.Session, r *prometheus.Registry, gtmMetricsConfig GTMMetricsConfig, gtmMetricPrefix string, tstart time.Time, lookbackDuration time.Duration) *GTMLivenessTrafficExporter {

	gtmLivenessTrafficExporter = GTMLivenessTrafficExporter{
		GTMConfig:                gtmMetricsConfig,
		LivenessLookbackDuration: lookbackDuration,
		AkamaiSession:            sess,
		ctx:                      ctx, // Inject v12 Session
	}
	gtmLivenessTrafficExporter.LivenessMetricPrefix = gtmMetricPrefix + "property_liveness_errors"
	gtmLivenessTrafficExporter.LivenessRegistry = r

	// Populate LastTimestamp per domain, liveness. Start time applies to all.
	domainMap := make(map[string]map[string]time.Time)
	for _, domain := range gtmMetricsConfig.Domains {
		tStampMap := make(map[string]time.Time) // index by property name
		livenessDurationHistogramMap[domain.Name] = make(map[string]map[int]prometheus.Histogram)
		livenessErrorsSummaryMap[domain.Name] = make(map[string]map[int]prometheus.Summary)
		for _, prop := range domain.Liveness {
			livenessDurationHistogramMap[domain.Name][prop.PropertyName] = make(map[int]prometheus.Histogram)
			livenessErrorsSummaryMap[domain.Name][prop.PropertyName] = make(map[int]prometheus.Summary)
			tStampMap[prop.PropertyName] = tstart
		}
		domainMap[domain.Name] = tStampMap
	}
	gtmLivenessTrafficExporter.LastTimestamp = domainMap

	return &gtmLivenessTrafficExporter
}

// Summaries map by domain, property, datacenter
var livenessDurationHistogramMap = make(map[string]map[string]map[int]prometheus.Histogram)
var livenessErrorsSummaryMap = make(map[string]map[string]map[int]prometheus.Summary)

func (l *GTMLivenessTrafficExporter) getDatacenterHistogramMetrics(domain, property string, dcid int) map[string]interface{} {

	histMap := make(map[string]interface{})
	if histo, ok := livenessDurationHistogramMap[domain][property][dcid]; ok {
		histMap["duration"] = histo
	} else {
		// doesn't exist. need to create
		labels := prometheus.Labels{"domain": domain, "property": property, "datacenter": strconv.Itoa(dcid)}
		livenessDurationHistogramMap[domain][property][dcid] = prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Namespace:   gtmLivenessTrafficExporter.LivenessMetricPrefix,
				Name:        "duration_per_datacenter_histogram",
				Help:        "Histogram of datacenter error duration (per domain and property)",
				ConstLabels: labels,
				Buckets:     durationBuckets,
			})
		l.LivenessRegistry.MustRegister(livenessDurationHistogramMap[domain][property][dcid])
		histMap["duration"] = livenessDurationHistogramMap[domain][property][dcid]
	}

	if esum, ok := livenessErrorsSummaryMap[domain][property][dcid]; ok {
		histMap["errors"] = esum
	} else {
		// doesn't exist. need to create
		labels := prometheus.Labels{"domain": domain, "property": property, "datacenter": strconv.Itoa(dcid)}
		livenessErrorsSummaryMap[domain][property][dcid] = prometheus.NewSummary(
			prometheus.SummaryOpts{
				Namespace:   gtmLivenessTrafficExporter.LivenessMetricPrefix,
				Name:        "errors_per_datacenter_summary",
				Help:        "Summary of datacenter errors  (per domain and property)",
				ConstLabels: labels,
				MaxAge:      gtmLivenessTrafficExporter.LivenessLookbackDuration,
				BufCap:      prometheus.DefBufCap * 2,
			})
		l.LivenessRegistry.MustRegister(livenessErrorsSummaryMap[domain][property][dcid])
		histMap["errors"] = livenessErrorsSummaryMap[domain][property][dcid]
	}

	return histMap

}

// Describe function
func (l *GTMLivenessTrafficExporter) Describe(ch chan<- *prometheus.Desc) {

	ch <- prometheus.NewDesc(l.LivenessMetricPrefix, "Akamai GTM Property Liveness Errors", nil, nil)
}

func (l *GTMLivenessTrafficExporter) Collect(ch chan<- prometheus.Metric) {
	logrus.Debugf("Entering GTM Property Liveness Errors Collect")

	endtime := time.Now().UTC()

	for _, domain := range l.GTMConfig.Domains {
		logrus.Debugf("Processing domain %s", domain.Name)
		for _, prop := range domain.Liveness {
			lasttime := l.LastTimestamp[domain.Name][prop.PropertyName].Add(time.Minute)
			logrus.Debugf("Fetching liveness errors Report for property %s in domain %s.", prop.PropertyName, domain.Name)

			livenessTrafficReport, err := l.retrieveLivenessTraffic(domain.Name, prop.PropertyName, prop.AgentIP, prop.TargetIP, lasttime)
			if err != nil {
				// Handle v12 style error checking via string contains or status if available
				if strings.Contains(err.Error(), "status: 500") {
					logrus.Warnf("Unable to get liveness errors for %s: Internal Server Error. Skipping.", prop.PropertyName)
					continue
				}
				logrus.Errorf("Unable to get liveness report for property %s. Error: %s", prop.PropertyName, err.Error())
				continue
			}

			if len(livenessTrafficReport.DataRows) < 1 && endtime.Day() != lasttime.Day() {
				lasttime = lasttime.Add(23*time.Hour + 59*time.Minute + 59*time.Second)
				livenessTrafficReport, err = l.retrieveLivenessTraffic(domain.Name, prop.PropertyName, prop.AgentIP, prop.TargetIP, lasttime)
				if err != nil {
					logrus.Errorf("Unable to get liveness report for property %s ... Skipping. Error: %s", prop.PropertyName, err.Error())
					continue
				}
			}
			logrus.Debugf("Traffic Metadata: [%v]", livenessTrafficReport.Metadata)

			for _, reportInstance := range livenessTrafficReport.DataRows {
				instanceTimestamp, err := parseTimeString(reportInstance.Timestamp, GTMTrafficLongTimeFormat)
				if err != nil {
					logrus.Errorf("Instance timestamp invalid  ... Skipping. Error: %s", err.Error())
					continue
				}
				if !instanceTimestamp.After(l.LastTimestamp[domain.Name][prop.PropertyName]) {
					continue
				}

				logrus.Debugf("Instance timestamp: [%v]. Last timestamp: [%v]", instanceTimestamp, l.LastTimestamp[domain.Name][prop.PropertyName])
				var baseLabels = []string{"domain", "property", "datacenter"}
				for _, instanceDC := range reportInstance.Datacenters {
					var tsLabels = baseLabels
					labelVals := []string{domain.Name, prop.PropertyName, strconv.Itoa(instanceDC.DatacenterID)}

					if prop.AgentIP == instanceDC.AgentIP {
						tsLabels = append(tsLabels, "agentip")
						labelVals = append(labelVals, instanceDC.AgentIP)
					}
					if prop.TargetIP == instanceDC.TargetIP {
						tsLabels = append(tsLabels, "targetip")
						labelVals = append(labelVals, instanceDC.TargetIP)
					}
					if prop.ErrorCode {
						tsLabels = append(tsLabels, "errorcode")
						codestring := fmt.Sprintf("%v", instanceDC.ErrorCode)
						labelVals = append(labelVals, codestring)
					}
					ts := instanceTimestamp.Format(time.RFC3339)
					if l.GTMConfig.TSLabel {
						tsLabels = append(tsLabels, "interval_timestamp")
						labelVals = append(labelVals, ts)
					}

					// Build Failures Counter
					desc := prometheus.NewDesc(prometheus.BuildFQName(l.LivenessMetricPrefix, "", "datacenter_failures"), "Number of datacenter failures", tsLabels, nil)
					errorsmetric := prometheus.MustNewConstMetric(desc, prometheus.CounterValue, 1, labelVals...)

					// Build Duration Gauge
					descDur := prometheus.NewDesc(prometheus.BuildFQName(l.LivenessMetricPrefix, "", "datacenter_failure_duration"), "Datacenter failure duration", tsLabels, nil)
					durmetric := prometheus.MustNewConstMetric(descDur, prometheus.GaugeValue, float64(instanceDC.Duration), labelVals...)

					if l.GTMConfig.UseTimestamp != nil && !*l.GTMConfig.UseTimestamp {
						ch <- errorsmetric
						ch <- durmetric
					} else {
						ch <- prometheus.NewMetricWithTimestamp(instanceTimestamp, errorsmetric)
						ch <- prometheus.NewMetricWithTimestamp(instanceTimestamp, durmetric)
					}

					maps := l.getDatacenterHistogramMetrics(domain.Name, prop.PropertyName, instanceDC.DatacenterID)
					maps["duration"].(prometheus.Histogram).Observe(float64(instanceDC.Duration))
					maps["errors"].(prometheus.Summary).Observe(float64(1))
				}

				if instanceTimestamp.After(l.LastTimestamp[domain.Name][prop.PropertyName]) {
					l.LastTimestamp[domain.Name][prop.PropertyName] = instanceTimestamp
				}
				break
			}
		}
	}
}

func (l *GTMLivenessTrafficExporter) retrieveLivenessTraffic(domain, prop, agentID, targetID string, start time.Time) (*LivenessErrorsResponse, error) {
	// 1. Fetch Window
	windowPath := "/gtm-api/v1/reports/liveness-tests/window"
	windowReq, err := http.NewRequest(http.MethodGet, windowPath, nil)
	if err != nil {
		return nil, err
	}

	var livenessWindow WindowResponse
	_, err = l.AkamaiSession.Exec(windowReq, &livenessWindow)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch liveness window: %w", err)
	}

	// --- FIX: Window Parsing Guard ---
	// If Akamai returns an empty window, or it fails to unmarshal,
	// we use 'start' as our anchor so we don't send 0001-01-01.
	anchorTime := start
	if !livenessWindow.EndTime.IsZero() && livenessWindow.EndTime.Year() > 1 {
		anchorTime = livenessWindow.EndTime
	}

	// 2. Align Timestamps
	qargsStart := floorToGTMInterval(start)
	qargsEnd := floorToGTMInterval(anchorTime)

	// 3. Build the actual report request
	path := fmt.Sprintf("/gtm-api/v1/reports/liveness-tests/domains/%s/properties/%s", domain, prop)
	req, err := http.NewRequest(http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}

	q := req.URL.Query()

	// --- FIX: EXACT DATE FORMAT FROM DOCS ---
	// Format: YYYY-MM-DD
	q.Add("date", qargsStart.Format("2006-01-02"))

	// Add start/end as ISO 8601 strings (Truncated to seconds)
	q.Add("start", qargsStart.Truncate(time.Second).Format(time.RFC3339))
	q.Add("end", qargsEnd.Truncate(time.Second).Format(time.RFC3339))

	if len(targetID) > 0 {
		q.Add("targetId", targetID)
	} else if len(agentID) > 0 {
		q.Add("agentId", agentID)
	}
	req.URL.RawQuery = q.Encode()

	// LOG THE URL: Compare this to your tech docs example
	logrus.Infof("Liveness Request: %s?%s", path, req.URL.RawQuery)

	var result LivenessErrorsResponse
	resp, err := l.AkamaiSession.Exec(req, &result)
	if err != nil {
		return nil, err
	}

	// If still 400, print the body so we can see Akamai's specific complaint
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API returned non-200 status: %d", resp.StatusCode)
	}

	sortLivenessDataRowsByTimestamp(result.DataRows)
	return &result, nil
}
