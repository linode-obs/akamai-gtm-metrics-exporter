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

	"github.com/akamai/AkamaiOPEN-edgegrid-golang/v13/pkg/session"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

var (
	gtmLivenessTrafficExporter GTMLivenessTrafficExporter
	durationBuckets            = []float64{60, 1800, 3600, 7200, 14400}
)

type LivenessTMeta struct {
	URI      string `json:"uri"`
	Domain   string `json:"domain"`
	Property string `json:"property"`
	Date     string `json:"date"`
}

type LivenessDRow struct {
	Nickname          string `json:"nickname"`
	DatacenterID      int    `json:"datacenterId"`
	TrafficTargetName string `json:"trafficTargetName"`
	ErrorCode         int64  `json:"errorCode"`
	Duration          int64  `json:"duration"`
	TestName          string `json:"testName"`
	AgentIP           string `json:"agentIp"`
	TargetIP          string `json:"targetIp"`
	Status            string `json:"status"` // Added: Often present in GTM reports
}

type GTMLivenessTrafficExporter struct {
	GTMConfig                GTMMetricsConfig
	LivenessMetricPrefix     string
	LivenessLookbackDuration time.Duration
	LastTimestamp            map[string]map[string]time.Time // index by domain, liveness
	LastReportEndTime        map[string]map[string]time.Time // index by domain, liveness
	LivenessRegistry         *prometheus.Registry
	AkamaiSession            session.Session
	ctx                      context.Context
}

func NewLivenessTrafficCollector(ctx context.Context, sess session.Session, r *prometheus.Registry, gtmMetricsConfig GTMMetricsConfig, gtmMetricPrefix string, tstart time.Time, lookbackDuration time.Duration) *GTMLivenessTrafficExporter {

	gtmLivenessTrafficExporter = GTMLivenessTrafficExporter{
		GTMConfig:                gtmMetricsConfig,
		LivenessLookbackDuration: lookbackDuration,
		AkamaiSession:            sess,
		ctx:                      ctx,
	}
	gtmLivenessTrafficExporter.LivenessMetricPrefix = gtmMetricPrefix + "property_liveness_errors"
	gtmLivenessTrafficExporter.LivenessLookbackDuration = lookbackDuration
	gtmLivenessTrafficExporter.LivenessRegistry = r

	domainMap := make(map[string]map[string]time.Time)
	reportMap := make(map[string]map[string]time.Time)
	for _, domain := range gtmMetricsConfig.Domains {
		tStampMap := make(map[string]time.Time)
		rEndTimeMap := make(map[string]time.Time)
		livenessDurationHistogramMap[domain.Name] = make(map[string]map[int]prometheus.Histogram)
		livenessErrorsSummaryMap[domain.Name] = make(map[string]map[int]prometheus.Summary)
		for _, prop := range domain.Liveness {
			livenessDurationHistogramMap[domain.Name][prop.PropertyName] = make(map[int]prometheus.Histogram)
			livenessErrorsSummaryMap[domain.Name][prop.PropertyName] = make(map[int]prometheus.Summary)
			tStampMap[prop.PropertyName] = tstart
			rEndTimeMap[prop.PropertyName] = tstart
		}
		domainMap[domain.Name] = tStampMap
		reportMap[domain.Name] = rEndTimeMap
	}
	gtmLivenessTrafficExporter.LastTimestamp = domainMap
	gtmLivenessTrafficExporter.LastReportEndTime = reportMap

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
		labels := prometheus.Labels{"domain": domain, "property": property, "datacenter": strconv.Itoa(dcid)}
		livenessErrorsSummaryMap[domain][property][dcid] = prometheus.NewSummary(
			prometheus.SummaryOpts{
				Namespace:   gtmLivenessTrafficExporter.LivenessMetricPrefix,
				Name:        "errors_per_datacenter_summary",
				Help:        "Summary of datacenter errors (per domain and property)",
				ConstLabels: labels,
				MaxAge:      gtmLivenessTrafficExporter.LivenessLookbackDuration,
				BufCap:      prometheus.DefBufCap * 2,
			})
		l.LivenessRegistry.MustRegister(livenessErrorsSummaryMap[domain][property][dcid])
		histMap["errors"] = livenessErrorsSummaryMap[domain][property][dcid]
	}
	return histMap
}

func (l *GTMLivenessTrafficExporter) Describe(ch chan<- *prometheus.Desc) {
	ch <- prometheus.NewDesc(l.LivenessMetricPrefix, "Akamai GTM Property Liveness Errors", nil, nil)
}

func (l *GTMLivenessTrafficExporter) Collect(ch chan<- prometheus.Metric) {
	logrus.Debug("Entering GTM Property Liveness Errors Collect")

	// Collect metrics for each domain and liveness
	for _, domain := range l.GTMConfig.Domains {
		logrus.Debugf("Processing domain %s", domain.Name)
		for _, prop := range domain.Liveness {
			// Timestamp of the end of the last report requested for this property, plus a small buffer which will advance the day if needed.
			lastReportRequested := l.LastReportEndTime[domain.Name][prop.PropertyName].Add(time.Minute)
			logrus.Debugf("Fetching liveness errors Report for property %s in domain %s.", prop.PropertyName, domain.Name)
			livenessTrafficReport, reportEnd, err := l.retrieveLivenessTraffic(domain.Name, prop.PropertyName, prop.AgentIP, prop.TargetIP, lastReportRequested)

			if err != nil {
				errStr := err.Error()
				if strings.Contains(errStr, "500") {
					logrus.Warnf("Unable to get liveness errors report for property %s. Internal error ... Skipping.", prop.PropertyName)
					continue
				}
				if strings.Contains(errStr, "400") {
					logrus.Warnf("Unable to get liveness errors report for property %s. ... Skipping.", prop.PropertyName)
					logrus.Errorf("%s", err.Error())
					continue
				}
				logrus.Errorf("Unable to get liveness report for property %s ... Skipping. Error: %s", prop.PropertyName, err.Error())
				continue
			}

			logrus.Debugf("Traffic Metadata: [%v]", livenessTrafficReport.Metadata)

			// Only consider entries in dataRows with a timestamp later than the last processed timestamp for the property.
			type newRow struct {
				instance  *LivenessTData
				timestamp time.Time
			}
			var newRows []newRow
			for _, reportInstance := range livenessTrafficReport.DataRows {
				instanceTimestamp, err := parseTimeString(reportInstance.Timestamp, GTMTrafficLongTimeFormat)
				if err != nil {
					logrus.Errorf("Instance timestamp invalid ... Skipping. Error: %s", err.Error())
					continue
				}
				if !instanceTimestamp.After(l.LastTimestamp[domain.Name][prop.PropertyName]) {
					logrus.Debugf("Instance timestamp: [%v]. Last timestamp: [%v]", instanceTimestamp, l.LastTimestamp[domain.Name][prop.PropertyName])
					logrus.Infof("Skipping already processed report instance: [%v].", reportInstance)
					continue
				}
				newRows = append(newRows, newRow{reportInstance, instanceTimestamp})
			}

			// Regardless of whether the report included new rows, update the LastReportEndTime.
			l.LastReportEndTime[domain.Name][prop.PropertyName] = reportEnd

			// Observe() Histograms and Summaries for every new failure entry, but create new const metrics only on the final row to avoid duplicates.
			for i, row := range newRows {
				reportInstance, instanceTimestamp := row.instance, row.timestamp
				isLast := i == len(newRows)-1
				var baseLabels = []string{"domain", "property", "datacenter"}
				for _, instanceDC := range reportInstance.Datacenters {
					maps := l.getDatacenterHistogramMetrics(domain.Name, prop.PropertyName, instanceDC.DatacenterID)
					maps["duration"].(prometheus.Histogram).Observe(float64(instanceDC.Duration))
					maps["errors"].(prometheus.Summary).Observe(float64(1))

					if isLast {
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
							labelVals = append(labelVals, fmt.Sprintf("%v", instanceDC.ErrorCode))
						}

						ts := instanceTimestamp.Format(time.RFC3339)
						if l.GTMConfig.TSLabel {
							tsLabels = append(tsLabels, "interval_timestamp")
							labelVals = append(labelVals, ts)
						}

						desc := prometheus.NewDesc(prometheus.BuildFQName(l.LivenessMetricPrefix, "", "datacenter_failures"), "Number of datacenter failures (per domain, property, datacenter)", tsLabels, nil)
						logrus.Debugf("Creating error failures counter metric. Domain: %s, Property: %s, Datacenter: %d, Timestamp: %v", domain.Name, prop.PropertyName, instanceDC.DatacenterID, ts)
						var errorsmetric, durmetric prometheus.Metric
						errorsmetric = prometheus.MustNewConstMetric(
							desc, prometheus.CounterValue, 1, labelVals...)
						if l.GTMConfig.UseTimestamp != nil && !*l.GTMConfig.UseTimestamp {
							ch <- errorsmetric
						} else {
							ch <- prometheus.NewMetricWithTimestamp(instanceTimestamp, errorsmetric)
						}
						desc = prometheus.NewDesc(prometheus.BuildFQName(l.LivenessMetricPrefix, "", "datacenter_failure_duration"), "Datacenter failure duration (per domain, property, datacenter)", tsLabels, nil)
						logrus.Debugf("Creating failure duration gauge metric. Domain: %s, Property: %s, Datacenter: %d, Timestamp: %v", domain.Name, prop.PropertyName, instanceDC.DatacenterID, ts)
						durmetric = prometheus.MustNewConstMetric(
							desc, prometheus.GaugeValue, float64(instanceDC.Duration), labelVals...)
						if l.GTMConfig.UseTimestamp != nil && !*l.GTMConfig.UseTimestamp {
							ch <- durmetric
						} else {
							ch <- prometheus.NewMetricWithTimestamp(instanceTimestamp, durmetric)
						}
					}
				} // datacenter end

				// Update last timestamp processed
				if instanceTimestamp.After(l.LastTimestamp[domain.Name][prop.PropertyName]) {
					logrus.Debugf("Updating Last Timestamp from %v TO %v", l.LastTimestamp[domain.Name][prop.PropertyName], instanceTimestamp)
					l.LastTimestamp[domain.Name][prop.PropertyName] = instanceTimestamp
				}
			}
		}
	}
}

func (l *GTMLivenessTrafficExporter) retrieveLivenessTraffic(domain, prop, agentID, targetID string, start time.Time) (*LivenessErrorsResponse, time.Time, error) {
	qargs := make(map[string]string)

	if len(targetID) > 0 {
		qargs["targetIp"] = targetID // Takes priority
		logrus.Info("Target IP Set. Using Target IP.")
	}
	if len(agentID) > 0 {
		if len(targetID) > 0 {
			logrus.Warn("Both agentIp and targetIp filters set. Using targetIp ONLY")
		} else {
			qargs["agentIp"] = agentID
			logrus.Info("Agent IP Set. Using Agent IP.")
		}
	}

	var apiWindow struct {
		Start string `json:"start"`
		End   string `json:"end"`
	}

	windowPath := "/gtm-api/v1/reports/liveness-tests/window"
	windowReq, err := http.NewRequestWithContext(l.ctx, http.MethodGet, windowPath, nil)
	if err != nil {
		return nil, time.Time{}, err
	}

	_, err = l.AkamaiSession.Exec(windowReq, &apiWindow)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("failed to fetch liveness window: %w", err)
	}

	// Convert API strings to time.Time objects
	windowStart, err := time.Parse(time.RFC3339, apiWindow.Start)
	if err != nil {
		return nil, time.Time{}, err
	}
	windowEnd, err := time.Parse(time.RFC3339, apiWindow.End)
	if err != nil {
		return nil, time.Time{}, err
	}

	reportDate, reportEnd, err := pickLivenessReportDate(start, windowStart, windowEnd)
	if err != nil {
		return nil, time.Time{}, err
	}
	qargs["date"] = reportDate

	path := fmt.Sprintf("/gtm-api/v1/reports/liveness-tests/domains/%s/properties/%s", domain, prop)
	req, err := http.NewRequestWithContext(l.ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, time.Time{}, err
	}

	if _, ok := qargs["date"]; !ok {
		return nil, time.Time{}, fmt.Errorf("GetLivenessErrorsReport: date parameter is required")
	}

	q := req.URL.Query()
	for k, v := range qargs {
		switch k {
		case "date":
			q.Add(k, v)
		case "agentIp":
			q.Add(k, v)
		case "targetIp":
			q.Add(k, v)
		}
	}
	req.URL.RawQuery = q.Encode()

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	var result LivenessErrorsResponse
	resp, err := l.AkamaiSession.Exec(req, &result)
	if err != nil {
		if resp != nil && resp.StatusCode == http.StatusNotFound {
			return nil, time.Time{}, fmt.Errorf("property %s not found in domain %s for liveness report", prop, domain)
		}
		return nil, time.Time{}, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, time.Time{}, fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	sortLivenessDataRowsByTimestamp(result.DataRows)
	return &result, reportEnd, nil
}

func pickLivenessReportDate(start, windowStart, windowEnd time.Time) (string, time.Time, error) {
	var requestDate time.Time
	var reportEnd time.Time

	// start is the start of the time period the caller is interested in (generally the ending time of the last report processed
	// or the start time of the exporter minus an optional prefill period). We want to pick the report for
	// the day that includes start (if possible), and keep track of the end time of the report requested, which will be
	// either the end of the window (if that date is the most recent day available) or the end of that day
	// (if the date is for a day in the past).
	// Setting reportEnd to the end of the day cues the next Collect() to request the following day.

	if windowStart.Before(start) {
		if windowEnd.After(start) {
			// If the window includes the requested start time, request that date.
			requestDate = start
			// reportEnd is the end of the date requested, or the end of the window, whichever is earlier
			reportEnd = time.Date(start.Year(), start.Month(), start.Day(), 23, 59, 59, 0, time.UTC)
			if reportEnd.After(windowEnd) {
				reportEnd = windowEnd
			}
		} else {
			// If the window ends before the requested start time, request the latest available date
			requestDate = windowEnd
			reportEnd = windowEnd
		}
	} else {
		// If the window start is after the requested start time, request the earliest available date.
		requestDate = windowStart
		// reportEnd is the end of the date requested, or the end of the window, whichever is earlier
		reportEnd = time.Date(windowStart.Year(), windowStart.Month(), windowStart.Day(), 23, 59, 59, 0, time.UTC)
		if reportEnd.After(windowEnd) {
			reportEnd = windowEnd
		}
	}

	reportDate, err := convertTimeFormat(requestDate, GTMTrafficDateFormat)
	if err != nil {
		return "", time.Time{}, err
	}

	return reportDate, reportEnd, nil
}
