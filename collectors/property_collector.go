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
	gtmPropertyTrafficExporter GTMPropertyTrafficExporter
)

// --- Property Traffic Structs ---

type PropertyTrafficResponse struct {
	Metadata    PropertyTMeta          `json:"metadata"`
	DataRows    []*PropertyTrafficData `json:"dataRows"`
	DataSummary interface{}            `json:"dataSummary"` // Added for parity
	Links       []interface{}          `json:"links"`
}

type PropertyTrafficData struct {
	Timestamp   string            `json:"timestamp"`
	Datacenters []*PropertyDCData `json:"datacenters"` // Changed to pointer slice
}

type PropertyDCData struct {
	Nickname          string `json:"nickname"`
	DatacenterId      int    `json:"datacenterId"`
	TrafficTargetName string `json:"trafficTargetName"`
	Requests          int64  `json:"requests"`
	Status            string `json:"status"` // Added missing field
}

// --- Liveness/Errors Structs ---

type LivenessErrorsResponse struct {
	Metadata    *LivenessTMeta   `json:"metadata"`
	DataRows    []*LivenessTData `json:"dataRows"`
	DataSummary interface{}      `json:"dataSummary"`
	Links       []interface{}    `json:"links"`
}

type LivenessTData struct {
	Timestamp   string          `json:"timestamp"`
	Datacenters []*LivenessDRow `json:"datacenters"`
}

// Ensure your LivenessDRow and Metadata structs look like this:
type PropertyTMeta struct {
	URI      string `json:"uri"`
	Domain   string `json:"domain"`
	Interval string `json:"interval,omitempty"`
	Property string `json:"property"`
	Start    string `json:"start"`
	End      string `json:"end"`
}

type GTMPropertyTrafficExporter struct {
	GTMConfig                GTMMetricsConfig
	PropertyMetricPrefix     string
	PropertyLookbackDuration time.Duration
	LastTimestamp            map[string]map[string]time.Time // index by domain, property
	PropertyRegistry         *prometheus.Registry
	AkamaiSession            session.Session
	ctx                      context.Context
}

func NewPropertyTrafficCollector(ctx context.Context, sess session.Session, r *prometheus.Registry, gtmMetricsConfig GTMMetricsConfig, gtmMetricPrefix string, tstart time.Time, lookbackDuration time.Duration) *GTMPropertyTrafficExporter {

	gtmPropertyTrafficExporter = GTMPropertyTrafficExporter{
		GTMConfig:                gtmMetricsConfig,
		PropertyLookbackDuration: lookbackDuration,
		AkamaiSession:            sess,
		ctx:                      ctx,
	}
	gtmPropertyTrafficExporter.PropertyMetricPrefix = gtmMetricPrefix + "property_traffic"
	gtmPropertyTrafficExporter.PropertyLookbackDuration = lookbackDuration
	gtmPropertyTrafficExporter.PropertyRegistry = r

	domainMap := make(map[string]map[string]time.Time)
	for _, domain := range gtmMetricsConfig.Domains {
		propertyReqSummaryMap[domain.Name] = make(map[string]prometheus.Summary)
		tStampMap := make(map[string]time.Time)
		for _, prop := range domain.Properties {
			tStampMap[prop.Name] = tstart

			propertySumMap := createPropertyMaps(domain.Name, prop.Name)
			r.MustRegister(propertySumMap)
		}
		domainMap[domain.Name] = tStampMap
	}
	gtmPropertyTrafficExporter.LastTimestamp = domainMap

	return &gtmPropertyTrafficExporter
}

var propertyReqSummaryMap = make(map[string]map[string]prometheus.Summary)

func createPropertyMaps(domain, prop string) prometheus.Summary {
	labels := prometheus.Labels{"domain": domain, "property": prop}

	propertyReqSummaryMap[domain][prop] = prometheus.NewSummary(
		prometheus.SummaryOpts{
			Namespace:   gtmPropertyTrafficExporter.PropertyMetricPrefix,
			Name:        "requests_per_interval_summary",
			Help:        "Number of aggregate property requests per 5 minute interval (per domain)",
			MaxAge:      gtmPropertyTrafficExporter.PropertyLookbackDuration,
			BufCap:      prometheus.DefBufCap * 2,
			ConstLabels: labels,
		})

	return propertyReqSummaryMap[domain][prop]
}

func (p *GTMPropertyTrafficExporter) Describe(ch chan<- *prometheus.Desc) {
	ch <- prometheus.NewDesc(p.PropertyMetricPrefix, "Akamai GTM Property Traffic", nil, nil)
}

func (p *GTMPropertyTrafficExporter) Collect(ch chan<- prometheus.Metric) {
	logrus.Debug("Entering GTM Property Traffic Collect")

	endtime := time.Now().UTC()

	for _, domain := range p.GTMConfig.Domains {
		logrus.Debugf("Processing domain %s", domain.Name)
		for _, prop := range domain.Properties {
			// Restore Original Logic: lasttime + 1 minute, ensuring 5min buffer
			lasttime := p.LastTimestamp[domain.Name][prop.Name].Add(time.Minute)
			if endtime.Before(lasttime.Add(time.Minute * 5)) {
				lasttime = lasttime.Add(time.Minute * 5)
			}

			logrus.Debugf("Fetching property Report for property %s in domain %s.", prop.Name, domain.Name)
			propertyTrafficReport, err := p.retrievePropertyTraffic(domain.Name, prop.Name, lasttime, endtime)

			if err != nil {
				errStr := err.Error()
				if strings.Contains(errStr, "500") {
					logrus.Warnf("Unable to get traffic report for property %s. Internal error ... Skipping.", prop.Name)
					continue
				}
				if strings.Contains(errStr, "400") {
					logrus.Warnf("Unable to get traffic report for property %s. Skipping. Error: %s", prop.Name, errStr)
					logrus.Errorf("%s", err.Error())
					continue
				}
				logrus.Errorf("Unable to get traffic report for property %s ... Skipping. Error: %s", prop.Name, errStr)
				continue
			}
			logrus.Debugf("Traffic Metadata: [%v]", propertyTrafficReport.Metadata)
			for _, reportInstance := range propertyTrafficReport.DataRows {
				instanceTimestamp, err := parseTimeString(reportInstance.Timestamp, GTMTrafficLongTimeFormat)
				if err != nil {
					logrus.Errorf("Instance timestamp invalid ... Skipping. Error: %s", err.Error())
					continue
				}

				if !instanceTimestamp.After(p.LastTimestamp[domain.Name][prop.Name]) {
					logrus.Debugf("Instance timestamp: [%v]. Last timestamp: [%v]", instanceTimestamp, p.LastTimestamp[domain.Name][prop.Name])
					logrus.Debugf("Attempting to re process report instance: [%v]. Skipping.", reportInstance)
					continue
				}

				// Check for missing intervals
				logrus.Debugf("Instance timestamp: [%v]. Last timestamp: [%v]", instanceTimestamp, p.LastTimestamp[domain.Name][prop.Name])
				if instanceTimestamp.After(p.LastTimestamp[domain.Name][prop.Name].Add(time.Minute * (trafficReportInterval + 1))) {
					logrus.Warnf("Missing report interval. Current: %v, Last: %v", instanceTimestamp, p.LastTimestamp[domain.Name][prop.Name])
				}

				var aggReqs int64
				var baseLabels = []string{"domain", "property"}

				for _, instanceDC := range reportInstance.Datacenters {
					aggReqs += instanceDC.Requests

					if len(prop.DatacenterIDs) > 0 || len(prop.DCNicknames) > 0 || len(prop.Targets) > 0 {
						var tsLabels []string
						var filterVal string
						var filterLabel string

						if intSliceContains(prop.DatacenterIDs, instanceDC.DatacenterId) {
							filterVal = strconv.Itoa(instanceDC.DatacenterId)
							filterLabel = "datacenterid"
							tsLabels = append(baseLabels, filterLabel)
						} else if stringSliceContains(prop.DCNicknames, instanceDC.Nickname) {
							filterVal = instanceDC.Nickname
							filterLabel = "nickname"
							tsLabels = append(baseLabels, filterLabel)
						} else if stringSliceContains(prop.Targets, instanceDC.TrafficTargetName) {
							filterVal = instanceDC.TrafficTargetName
							filterLabel = "target"
							tsLabels = append(baseLabels, filterLabel)
						}

						if filterVal != "" {
							if p.GTMConfig.TSLabel {
								tsLabels = append(tsLabels, "interval_timestamp")
							}
							ts := instanceTimestamp.Format(time.RFC3339)
							desc := prometheus.NewDesc(prometheus.BuildFQName(p.PropertyMetricPrefix, "", "requests_per_interval"), "Number of property requests per 5 minute interval (per domain)", tsLabels, nil)

							var reqsmetric prometheus.Metric
							if p.GTMConfig.TSLabel {
								reqsmetric = prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(instanceDC.Requests), domain.Name, prop.Name, filterVal, ts)
							} else {
								reqsmetric = prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(instanceDC.Requests), domain.Name, prop.Name, filterVal)
							}

							if p.GTMConfig.UseTimestamp != nil && !*p.GTMConfig.UseTimestamp {
								ch <- reqsmetric
							} else {
								ch <- prometheus.NewMetricWithTimestamp(instanceTimestamp, reqsmetric)
							}
						}
					}
				}

				if len(prop.DatacenterIDs) < 1 && len(prop.DCNicknames) < 1 && len(prop.Targets) < 1 {
					tsLabels := baseLabels
					if p.GTMConfig.TSLabel {
						tsLabels = append(tsLabels, "interval_timestamp")
					}
					ts := instanceTimestamp.Format(time.RFC3339)
					desc := prometheus.NewDesc(prometheus.BuildFQName(p.PropertyMetricPrefix, "", "requests_per_interval"), "Number of property requests per 5 minute interval (per domain)", tsLabels, nil)
					logrus.Debugf("Creating Requests metric. Domain: %s, Property: %s, Requests: %v, Timestamp: %v", domain.Name, prop.Name, float64(aggReqs), ts)
					var reqsmetric prometheus.Metric
					if p.GTMConfig.TSLabel {
						reqsmetric = prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(aggReqs), domain.Name, prop.Name, ts)
					} else {
						reqsmetric = prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(aggReqs), domain.Name, prop.Name)
					}

					if p.GTMConfig.UseTimestamp != nil && !*p.GTMConfig.UseTimestamp {
						ch <- reqsmetric
					} else {
						ch <- prometheus.NewMetricWithTimestamp(instanceTimestamp, reqsmetric)
					}
				}

				propertyReqSummaryMap[domain.Name][prop.Name].Observe(float64(aggReqs))

				if instanceTimestamp.After(p.LastTimestamp[domain.Name][prop.Name]) {
					logrus.Debugf("Updating Last Timestamp from %v TO %v", p.LastTimestamp[domain.Name][prop.Name], instanceTimestamp)
					p.LastTimestamp[domain.Name][prop.Name] = instanceTimestamp
				}
				break
			}
		}
	}
}

func (p *GTMPropertyTrafficExporter) retrievePropertyTraffic(domain, prop string, start, end time.Time) (*PropertyTrafficResponse, error) {
	var apiWindow struct {
		Start string `json:"start"`
		End   string `json:"end"`
	}

	windowPath := "/gtm-api/v1/reports/traffic/properties-window"
	windowReq, _ := http.NewRequestWithContext(p.ctx, http.MethodGet, windowPath, nil)

	_, err := p.AkamaiSession.Exec(windowReq, &apiWindow)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch property traffic window: %w", err)
	}

	windowStart, err := time.Parse(time.RFC3339, apiWindow.Start)
	if err != nil {
		return nil, fmt.Errorf("invalid start time format from API (%s): %w", apiWindow.Start, err)
	}
	windowEnd, err := time.Parse(time.RFC3339, apiWindow.End)
	if err != nil {
		return nil, fmt.Errorf("invalid end time format from API (%s): %w", apiWindow.End, err)
	}

	qargs := make(map[string]string)

	if windowStart.Before(start) {
		if windowEnd.After(start) {
			qargs["start"], err = convertTimeFormat(start, time.RFC3339)
		} else {
			qargs["start"], err = convertTimeFormat(windowEnd, time.RFC3339)
		}
	} else {
		qargs["start"], err = convertTimeFormat(windowStart, time.RFC3339)
	}
	if err != nil {
		return nil, err
	}
	if windowEnd.Before(end) {
		qargs["end"], err = convertTimeFormat(windowEnd, time.RFC3339)
	} else {
		qargs["end"], err = convertTimeFormat(end, time.RFC3339)
	}
	if err != nil {
		return nil, err
	}

	if qargs["start"] >= qargs["end"] {
		logrus.Infof("Start or End time outside valid property window for %s. Skipping.", prop)
		return &PropertyTrafficResponse{DataRows: []*PropertyTrafficData{}}, nil
	}

	path := fmt.Sprintf("/gtm-api/v1/reports/traffic/domains/%s/properties/%s", domain, prop)
	req, _ := http.NewRequestWithContext(p.ctx, http.MethodGet, path, nil)

	q := req.URL.Query()
	q.Add("start", qargs["start"])
	q.Add("end", qargs["end"])
	req.URL.RawQuery = q.Encode()

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	var result PropertyTrafficResponse
	resp, err := p.AkamaiSession.Exec(req, &result)
	if err != nil {
		if resp != nil && resp.StatusCode == http.StatusNotFound {
			return nil, fmt.Errorf("property %s not found in domain %s", prop, domain)
		}
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	sortPropertyDataRowsByTimestamp(result.DataRows)
	return &result, nil
}
