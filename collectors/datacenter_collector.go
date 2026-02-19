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
	LastTimestamp      map[string]map[int]time.Time // index by domain, datacenterid
	DCRegistry         *prometheus.Registry
	AkamaiSession      session.Session
	ctx                context.Context
}

type DcTrafficResponse struct {
	Metadata    *Metadata                `json:"metadata"`    // Restored pointer
	DataRows    []*DatacenterTrafficData `json:"dataRows"`    // Restored pointer slice
	DataSummary interface{}              `json:"dataSummary"` // Restored DataSummary
	Links       []interface{}            `json:"links"`       // Interface for parity without old library
}

type DatacenterTrafficData struct {
	Timestamp  string             `json:"timestamp"`
	Properties []*TrafficProperty `json:"properties"` // Restored pointer slice
}

func NewDatacenterTrafficCollector(ctx context.Context, sess session.Session, r *prometheus.Registry, gtmMetricsConfig GTMMetricsConfig, gtmMetricPrefix string, tstart time.Time, lookbackDuration time.Duration) *GTMDatacenterTrafficExporter {

	gtmDatacenterTrafficExporter = GTMDatacenterTrafficExporter{
		GTMConfig:          gtmMetricsConfig,
		DCLookbackDuration: lookbackDuration,
		AkamaiSession:      sess,
		ctx:                ctx,
	}
	gtmDatacenterTrafficExporter.DCMetricPrefix = gtmMetricPrefix + "datacenter_traffic"
	gtmDatacenterTrafficExporter.DCLookbackDuration = lookbackDuration
	gtmDatacenterTrafficExporter.DCRegistry = r

	domainMap := make(map[string]map[int]time.Time)
	for _, domain := range gtmMetricsConfig.Domains {
		dcReqSummaryMap[domain.Name] = make(map[int]prometheus.Summary)
		tStampMap := make(map[int]time.Time)
		for _, dc := range domain.Datacenters {
			tStampMap[dc.DatacenterID] = tstart

			// Create and register Summaries
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

	endtime := time.Now().UTC() // Use same current time for all zones

	// Collect metrics for each domain and datacenter
	for _, domain := range d.GTMConfig.Domains {
		logrus.Debugf("Processing domain %s", domain.Name)
		for _, dc := range domain.Datacenters {
			// get last timestamp recorded. make sure diff > 5 mins.
			lasttime := d.LastTimestamp[domain.Name][dc.DatacenterID].Add(time.Minute)
			if endtime.Before(lasttime.Add(time.Minute * 5)) {
				lasttime = lasttime.Add(time.Minute * 5)
			}

			logrus.Debugf("Fetching datacenter Report for datacenter %d in domain %s.", dc.DatacenterID, domain.Name)
			dcTrafficReport, err := d.retrieveDatacenterTraffic(domain.Name, dc.DatacenterID, lasttime, endtime)

			if err != nil {
				errStr := err.Error()
				if strings.Contains(errStr, "500") {
					logrus.Warnf("Unable to get traffic report for datacenter %d. Internal error ... Skipping.", dc.DatacenterID)
					continue
				}
				if strings.Contains(errStr, "400") {
					logrus.Warnf("Unable to get traffic report for datacenter %d. Skipping. Error: %s", dc.DatacenterID, errStr)
					logrus.Errorf("%s", errStr)
					continue
				}
				logrus.Errorf("Unable to get traffic report for datacenter %d ... Skipping. Error: %s", dc.DatacenterID, errStr)
				continue
			}
			logrus.Debugf("Traffic Metadata: [%v]", dcTrafficReport.Metadata)
			for _, reportInstance := range dcTrafficReport.DataRows {
				instanceTimestamp, err := parseTimeString(reportInstance.Timestamp, GTMTrafficLongTimeFormat)
				if err != nil {
					logrus.Errorf("Instance timestamp invalid ... Skipping. Error: %s", err.Error())
					continue
				}

				// Strict check against last processed timestamp
				if !instanceTimestamp.After(d.LastTimestamp[domain.Name][dc.DatacenterID]) {
					logrus.Debugf("Instance timestamp: [%v]. Last timestamp: [%v]", instanceTimestamp, d.LastTimestamp[domain.Name][dc.DatacenterID])
					logrus.Warnf("Attempting to re process report instance: [%v]. Skipping.", reportInstance)
					continue
				}

				// Missing interval warning
				logrus.Debugf("Instance timestamp: [%v]. Last timestamp: [%v]", instanceTimestamp, d.LastTimestamp[domain.Name][dc.DatacenterID])
				if instanceTimestamp.After(d.LastTimestamp[domain.Name][dc.DatacenterID].Add(time.Minute * (trafficReportInterval + 1))) {
					logrus.Warnf("Missing report interval. Current: %v, Last: %v", instanceTimestamp, d.LastTimestamp[domain.Name][dc.DatacenterID])
				}

				var aggReqs int64
				var baseLabels = []string{"domain", "datacenter"}

				for _, instanceProp := range reportInstance.Properties {
					aggReqs += instanceProp.Requests

					if len(dc.Properties) > 0 {
						if stringSliceContains(dc.Properties, instanceProp.Name) {
							tsLabels := append(baseLabels, "property")
							if d.GTMConfig.TSLabel {
								tsLabels = append(tsLabels, "interval_timestamp")
							}

							ts := instanceTimestamp.Format(time.RFC3339)
							desc := prometheus.NewDesc(prometheus.BuildFQName(d.DCMetricPrefix, "", "requests_per_interval"), "Number of datacenter requests per 5 minute interval (per domain)", tsLabels, nil)
							logrus.Debugf("Creating Requests metric. Domain: %s, Datacenter: %d, Property: %s, Requests: %v, Timestamp: %v", domain.Name, dc.DatacenterID, instanceProp.Name, float64(instanceProp.Requests), ts)
							var reqsmetric prometheus.Metric
							if d.GTMConfig.TSLabel {
								reqsmetric = prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(instanceProp.Requests), domain.Name, strconv.Itoa(dc.DatacenterID), instanceProp.Name, ts)
							} else {
								reqsmetric = prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(instanceProp.Requests), domain.Name, strconv.Itoa(dc.DatacenterID), instanceProp.Name)
							}

							if d.GTMConfig.UseTimestamp != nil && !*d.GTMConfig.UseTimestamp {
								ch <- reqsmetric
							} else {
								ch <- prometheus.NewMetricWithTimestamp(instanceTimestamp, reqsmetric)
							}
						}
					}
				}

				if len(dc.Properties) < 1 {
					tsLabels := baseLabels
					if d.GTMConfig.TSLabel {
						tsLabels = append(tsLabels, "interval_timestamp")
					}
					ts := instanceTimestamp.Format(time.RFC3339)
					desc := prometheus.NewDesc(prometheus.BuildFQName(d.DCMetricPrefix, "", "requests_per_interval"), "Number of datacenter requests per 5 minute interval (per domain)", tsLabels, nil)
					logrus.Debugf("Creating Requests metric. Domain: %s, Datacenter: %d, Requests: %v, Timestamp: %v", domain.Name, dc.DatacenterID, float64(aggReqs), ts)
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

				dcReqSummaryMap[domain.Name][dc.DatacenterID].Observe(float64(aggReqs))

				if instanceTimestamp.After(d.LastTimestamp[domain.Name][dc.DatacenterID]) {
					logrus.Debugf("Updating Last Timestamp from %v TO %v", d.LastTimestamp[domain.Name][dc.DatacenterID], instanceTimestamp)
					d.LastTimestamp[domain.Name][dc.DatacenterID] = instanceTimestamp
				}
				break // Only process one per interval
			}
		}
	}
}

func (d *GTMDatacenterTrafficExporter) retrieveDatacenterTraffic(domain string, dc int, start, end time.Time) (*DcTrafficResponse, error) {
	// 1. Get Traffic Window
	var apiWindow struct {
		Start string `json:"start"`
		End   string `json:"end"`
	}

	windowPath := "/gtm-api/v1/reports/traffic/datacenters-window"
	windowReq, _ := http.NewRequestWithContext(d.ctx, http.MethodGet, windowPath, nil)

	// Use d.AkamaiSession.Exec.
	// Note: If Exec returns the response, check for errors before parsing.
	_, err := d.AkamaiSession.Exec(windowReq, &apiWindow)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch traffic window: %w", err)
	}

	// Convert strings to time.Time to match old WindowResponse logic
	startTime, err := time.Parse(time.RFC3339, apiWindow.Start)
	if err != nil {
		return nil, fmt.Errorf("invalid start time format from API (%s): %w", apiWindow.Start, err)
	}
	endTime, err := time.Parse(time.RFC3339, apiWindow.End)
	if err != nil {
		return nil, fmt.Errorf("invalid end time format from API (%s): %w", apiWindow.End, err)
	}

	// 2. Original Boundary Logic
	qargs := make(map[string]string)

	if startTime.Before(start) {
		if endTime.After(start) {
			qargs["start"], err = convertTimeFormat(start, time.RFC3339)
		} else {
			qargs["start"], err = convertTimeFormat(endTime, time.RFC3339)
		}
	} else {
		qargs["start"], err = convertTimeFormat(startTime, time.RFC3339)
	}
	if err != nil {
		return nil, err
	}

	if endTime.Before(end) {
		qargs["end"], err = convertTimeFormat(endTime, time.RFC3339)
	} else {
		qargs["end"], err = convertTimeFormat(end, time.RFC3339)
	}
	if err != nil {
		return nil, err
	}

	// Window validation check
	if qargs["start"] >= qargs["end"] {
		logrus.Warnf("Start or End time outside valid report window")
		return &DcTrafficResponse{DataRows: []*DatacenterTrafficData{}}, nil
	}

	// 3. Fetch Traffic Data
	path := fmt.Sprintf("/gtm-api/v1/reports/traffic/domains/%s/datacenters/%d", domain, dc)
	req, _ := http.NewRequestWithContext(d.ctx, http.MethodGet, path, nil)

	q := req.URL.Query()
	q.Add("start", qargs["start"])
	q.Add("end", qargs["end"])
	req.URL.RawQuery = q.Encode()

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	var result DcTrafficResponse
	resp, err := d.AkamaiSession.Exec(req, &result)
	if err != nil {
		if resp != nil && resp.StatusCode == http.StatusNotFound {
			return nil, fmt.Errorf("datacenter %d not found in domain %s", dc, domain)
		}
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	sortDCDataRowsByTimestamp(result.DataRows)
	return &result, nil
}
