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

	"github.com/akamai/AkamaiOPEN-edgegrid-golang/v12/pkg/session"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

var (
	gtmDatacenterTrafficExporter GTMDatacenterTrafficExporter
)

type GTMDatacenterTrafficExporter struct {
	GTMConfig          GTMMetricsConfig
	DCMetricPrefix     string
	DCLookbackDuration time.Duration
	LastTimestamp      map[string]map[int]time.Time // [domain][datacenterID]
	DCRegistry         *prometheus.Registry
	AkamaiSession      session.Session
	ctx                context.Context
}

func NewDatacenterTrafficCollector(ctx context.Context, sess session.Session, r *prometheus.Registry, gtmMetricsConfig GTMMetricsConfig, gtmMetricPrefix string, tstart time.Time, lookbackDuration time.Duration) *GTMDatacenterTrafficExporter {

	gtmDatacenterTrafficExporter = GTMDatacenterTrafficExporter{GTMConfig: gtmMetricsConfig, DCLookbackDuration: lookbackDuration, AkamaiSession: sess, ctx: ctx}
	gtmDatacenterTrafficExporter.DCMetricPrefix = gtmMetricPrefix + "datacenter_traffic"
	gtmDatacenterTrafficExporter.DCLookbackDuration = lookbackDuration
	gtmDatacenterTrafficExporter.DCRegistry = r
	domainMap := make(map[string]map[int]time.Time)
	for _, domain := range gtmMetricsConfig.Domains {
		dcReqSummaryMap[domain.Name] = make(map[int]prometheus.Summary)
		tStampMap := make(map[int]time.Time)
		for _, dc := range domain.Datacenters {
			tStampMap[dc.DatacenterID] = tstart
			dcSumMap := createDatacenterMaps(domain.Name, dc.DatacenterID)
			r.MustRegister(dcSumMap)
		}
		domainMap[domain.Name] = tStampMap
	}
	gtmDatacenterTrafficExporter.LastTimestamp = domainMap

	return &gtmDatacenterTrafficExporter
}

// Summaries map by domain and datacenter
var dcReqSummaryMap = make(map[string]map[int]prometheus.Summary)

// Initialize locally maintained maps. Only use domain and datacenter.
func createDatacenterMaps(domain string, dc int) prometheus.Summary {

	dclabel := strconv.Itoa(dc)
	labels := prometheus.Labels{"domain": domain, "datacenter": dclabel}

	dcReqSummaryMap[domain][dc] = prometheus.NewSummary(
		prometheus.SummaryOpts{
			Namespace:   gtmDatacenterTrafficExporter.DCMetricPrefix,
			Name:        "requests_per_interval_summary",
			Help:        "Number of aggregate datacenter requests per 5 minute interval (per domain)",
			MaxAge:      gtmDatacenterTrafficExporter.DCLookbackDuration,
			BufCap:      prometheus.DefBufCap * 2,
			ConstLabels: labels,
		})

	return dcReqSummaryMap[domain][dc]
}

// Describe function
func (d *GTMDatacenterTrafficExporter) Describe(ch chan<- *prometheus.Desc) {

	ch <- prometheus.NewDesc(d.DCMetricPrefix, "Akamai GTM Datacenter Traffic", nil, nil)
}

// Collect function
func (d *GTMDatacenterTrafficExporter) Collect(ch chan<- prometheus.Metric) {
	logrus.Debug("Entering GTM DC Traffic Collect")

	//endtime := time.Now().UTC()

	for _, domain := range d.GTMConfig.Domains {
		logrus.Debugf("Processing domain %s", domain.Name)
		for _, dc := range domain.Datacenters {

			nextIntervalStart := d.LastTimestamp[domain.Name][dc.DatacenterID].Add(5 * time.Minute)

			safeNow := time.Now().UTC().Add(-15 * time.Minute)

			if nextIntervalStart.After(safeNow) {
				logrus.Debugf("Next interval %v is too recent (SafeNow: %v). Skipping DC %d.", nextIntervalStart, safeNow, dc.DatacenterID)
				continue
			}

			logrus.Debugf("Fetching datacenter Report for DC %d in domain %s.", dc.DatacenterID, domain.Name)

			dcTrafficReport, err := d.retrieveDatacenterTraffic(domain.Name, dc.DatacenterID, nextIntervalStart, safeNow)

			if err != nil {
				if strings.Contains(err.Error(), "status: 500") {
					logrus.Warnf("Unable to get traffic report for datacenter %d. Internal error ... Skipping.", dc.DatacenterID)
					continue
				}
				if strings.Contains(err.Error(), "status: 400") {
					logrus.Warnf("Unable to get traffic report for datacenter %d. Bad Request ... Skipping.", dc.DatacenterID)
					logrus.Errorf("API Error Detail: %s", err.Error())
					continue
				}

				logrus.Errorf("Unable to get traffic report for datacenter %d ... Skipping. Error: %s", dc.DatacenterID, err.Error())
				continue
			}

			logrus.Debugf("Traffic Metadata: [%v]", dcTrafficReport.Metadata)

			for _, reportInstance := range dcTrafficReport.DataRows {
				instanceTimestamp, err := parseTimeString(reportInstance.Timestamp, GTMTrafficLongTimeFormat)
				if err != nil {
					logrus.Errorf("Instance timestamp invalid ... Skipping. Error: %s", err.Error())
					continue
				}

				if !instanceTimestamp.After(d.LastTimestamp[domain.Name][dc.DatacenterID]) {
					logrus.Debugf("Instance timestamp: [%v] already processed. Last: [%v]", instanceTimestamp, d.LastTimestamp[domain.Name][dc.DatacenterID])
					continue
				}

				if instanceTimestamp.After(d.LastTimestamp[domain.Name][dc.DatacenterID].Add(time.Minute * (trafficReportInterval + 1))) {
					logrus.Warnf("Missing report interval. Current: %v, Last: %v", instanceTimestamp, d.LastTimestamp[domain.Name][dc.DatacenterID])
				}

				var aggReqs int64
				var baseLabels = []string{"domain", "datacenter"}

				for _, instanceProp := range reportInstance.Properties {
					aggReqs += instanceProp.Requests // aggregate all properties

					// If specific properties are filtered in config
					if len(dc.Properties) > 0 && stringSliceContains(dc.Properties, instanceProp.Name) {
						tsLabels := append(baseLabels, "property")
						if d.GTMConfig.TSLabel {
							tsLabels = append(tsLabels, "interval_timestamp")
						}

						ts := instanceTimestamp.Format(time.RFC3339)
						desc := prometheus.NewDesc(prometheus.BuildFQName(d.DCMetricPrefix, "", "requests_per_interval"), "Number of datacenter requests per 5 minute interval", tsLabels, nil)

						var reqsmetric prometheus.Metric
						if d.GTMConfig.TSLabel {
							reqsmetric = prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(instanceProp.Requests), domain.Name, strconv.Itoa(dc.DatacenterID), instanceProp.Name, ts)
						} else {
							reqsmetric = prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(instanceProp.Requests), domain.Name, strconv.Itoa(dc.DatacenterID), instanceProp.Name)
						}

						// Ship metric
						if d.GTMConfig.UseTimestamp != nil && !*d.GTMConfig.UseTimestamp {
							ch <- reqsmetric
						} else {
							ch <- prometheus.NewMetricWithTimestamp(instanceTimestamp, reqsmetric)
						}
					}
				}

				// If no specific properties were requested, export the aggregate for the DC
				if len(dc.Properties) < 1 {
					tsLabels := baseLabels
					if d.GTMConfig.TSLabel {
						tsLabels = append(tsLabels, "interval_timestamp")
					}

					ts := instanceTimestamp.Format(time.RFC3339)
					desc := prometheus.NewDesc(prometheus.BuildFQName(d.DCMetricPrefix, "", "requests_per_interval"), "Aggregate datacenter requests per 5 minute interval", tsLabels, nil)

					var reqsmetric prometheus.Metric
					if d.GTMConfig.TSLabel {
						reqsmetric = prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(aggReqs), domain.Name, strconv.Itoa(dc.DatacenterID), ts)
					} else {
						reqsmetric = prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(aggReqs), domain.Name, strconv.Itoa(dc.DatacenterID))
					}

					if d.GTMConfig.UseTimestamp != nil && !*d.GTMConfig.UseTimestamp {
						ch <- reqsmetric
					} else {
						ch <- prometheus.NewMetricWithTimestamp(instanceTimestamp, reqsmetric)
					}
				}

				// Update internal summary and tracking timestamp
				dcReqSummaryMap[domain.Name][dc.DatacenterID].Observe(float64(aggReqs))
				if instanceTimestamp.After(d.LastTimestamp[domain.Name][dc.DatacenterID]) {
					d.LastTimestamp[domain.Name][dc.DatacenterID] = instanceTimestamp
				}

				// Process only one interval per scrape to keep logic simple
				break
			}
		}
	}
}

func (d *GTMDatacenterTrafficExporter) retrieveDatacenterTraffic(domain string, dc int, start, end time.Time) (*DcTrafficResponse, error) {
	// Get the valid Traffic Window for Datacenters
	windowPath := "/gtm-api/v1/reports/traffic/datacenters-window"
	windowReq, err := http.NewRequestWithContext(d.ctx, http.MethodGet, windowPath, nil)
	if err != nil {
		return nil, err
	}

	var window WindowResponse
	_, err = d.AkamaiSession.Exec(windowReq, &window)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch traffic window: %w", err)
	}

	winStart := window.StartTime
	winEnd := window.EndTime

	if winEnd.IsZero() || winEnd.Year() < 2000 {
		logrus.Warn("Window API returned invalid timestamps, using 48h fallback from Now")
		winEnd = ceilToGTMInterval(time.Now().UTC())
		winStart = floorToGTMInterval(winEnd.Add(-48 * time.Hour))
	}

	// Validate and adjust requested start/end against the available window
	qargsStart := floorToGTMInterval(start)
	qargsEnd := ceilToGTMInterval(end)

	maxAllowed := floorToGTMInterval(time.Now().UTC().Add(-15 * time.Minute))
	if qargsEnd.After(maxAllowed) {
		qargsEnd = maxAllowed
	}

	if qargsStart.Before(winStart) {
		qargsStart = winStart
	}
	if qargsEnd.After(winEnd) {
		qargsEnd = winEnd
	}

	if qargsStart.After(qargsEnd) || qargsStart.Equal(qargsEnd) {
		logrus.Warnf("Start/End time outside valid report window for domain %s DC %d. Skipping.", domain, dc)
		return &DcTrafficResponse{DataRows: []*DatacenterTrafficData{}}, nil
	}

	// Request actual Traffic Data
	path := fmt.Sprintf("/gtm-api/v1/reports/traffic/domains/%s/datacenters/%d", domain, dc)
	req, err := http.NewRequestWithContext(d.ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}

	q := req.URL.Query()
	q.Add("start", qargsStart.Truncate(time.Second).Format(time.RFC3339))
	q.Add("end", qargsEnd.Truncate(time.Second).Format(time.RFC3339))
	req.URL.RawQuery = q.Encode()

	var result DcTrafficResponse
	resp, err := d.AkamaiSession.Exec(req, &result)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API returned non-200 status: %d", resp.StatusCode)
	}

	// Sort results by timestamp for Prometheus consistency
	sortDCDataRowsByTimestamp(result.DataRows)
	return &result, nil
}
