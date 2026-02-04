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
	gtmPropertyTrafficExporter GTMPropertyTrafficExporter
)

type GTMPropertyTrafficExporter struct {
	GTMConfig                GTMMetricsConfig
	PropertyMetricPrefix     string
	PropertyLookbackDuration time.Duration
	LastTimestamp            map[string]map[string]time.Time
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
	gtmPropertyTrafficExporter.PropertyRegistry = r

	// Populate LastTimestamp per domain, property. Start time applies to all.
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

// Summaries map by domain and property
var propertyReqSummaryMap = make(map[string]map[string]prometheus.Summary)

// Initialize locally maintained maps. Only use domain and property.
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

// Describe function
func (p *GTMPropertyTrafficExporter) Describe(ch chan<- *prometheus.Desc) {

	ch <- prometheus.NewDesc(p.PropertyMetricPrefix, "Akamai GTM Property Traffic", nil, nil)
}

// Collect function
func (p *GTMPropertyTrafficExporter) Collect(ch chan<- prometheus.Metric) {
	logrus.Debugf("Entering GTM Property Traffic Collect")

	for _, domain := range p.GTMConfig.Domains {
		logrus.Debugf("Processing domain %s", domain.Name)
		for _, prop := range domain.Properties {

			nextIntervalStart := p.LastTimestamp[domain.Name][prop.Name].Add(5 * time.Minute)

			safeNow := time.Now().UTC().Add(-15 * time.Minute)

			if nextIntervalStart.After(safeNow) {
				logrus.Debugf("Property %s next interval %v is too recent (SafeNow: %v). Skipping.", prop.Name, nextIntervalStart, safeNow)
				continue
			}

			targetEnd := nextIntervalStart.Add(5 * time.Minute)

			logrus.Debugf("Fetching property Report for %s in domain %s (Window: %v to %v)", prop.Name, domain.Name, nextIntervalStart, targetEnd)

			propertyTrafficReport, err := p.retrievePropertyTraffic(domain.Name, prop.Name, nextIntervalStart, targetEnd)
			if err != nil {
				if strings.Contains(err.Error(), "status: 500") {
					logrus.Warnf("Internal error for property %s. Skipping.", prop.Name)
					continue
				}
				logrus.Errorf("Unable to get traffic report for property %s. Error: %s", prop.Name, err.Error())
				continue
			}

			logrus.Debugf("Traffic Metadata: [%v]", propertyTrafficReport.Metadata)

			if len(propertyTrafficReport.DataRows) == 0 {
				logrus.Debugf("No traffic found for %s in window. Advancing timestamp.", prop.Name)
				p.LastTimestamp[domain.Name][prop.Name] = nextIntervalStart
				continue
			}

			for _, reportInstance := range propertyTrafficReport.DataRows {
				instanceTimestamp, err := parseTimeString(reportInstance.Timestamp, GTMTrafficLongTimeFormat)
				if err != nil {
					logrus.Errorf("Instance timestamp invalid ... Skipping. Error: %s", err.Error())
					continue
				}

				if !instanceTimestamp.After(p.LastTimestamp[domain.Name][prop.Name]) {
					continue
				}

				var aggReqs int64
				var baseLabels = []string{"domain", "property"}

				for _, instanceDC := range reportInstance.Datacenters {
					aggReqs += instanceDC.Requests

					// Filter and emit per-datacenter/target metrics if configured
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
							desc := prometheus.NewDesc(prometheus.BuildFQName(p.PropertyMetricPrefix, "", "requests_per_interval"), "Number of property requests per 5 minute interval", tsLabels, nil)

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

				// Emit aggregate metric if no specific filters were applied
				if len(prop.DatacenterIDs) < 1 && len(prop.DCNicknames) < 1 && len(prop.Targets) < 1 {
					tsLabels := baseLabels
					if p.GTMConfig.TSLabel {
						tsLabels = append(tsLabels, "interval_timestamp")
					}
					ts := instanceTimestamp.Format(time.RFC3339)
					desc := prometheus.NewDesc(prometheus.BuildFQName(p.PropertyMetricPrefix, "", "requests_per_interval"), "Number of property requests per 5 minute interval", tsLabels, nil)

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

				// Update Summary and last timestamp
				propertyReqSummaryMap[domain.Name][prop.Name].Observe(float64(aggReqs))
				p.LastTimestamp[domain.Name][prop.Name] = instanceTimestamp

				// Break to process exactly one interval per scrape
				break
			}
		}
	}
}

func (p *GTMPropertyTrafficExporter) retrievePropertyTraffic(domain, prop string, start, end time.Time) (*PropertyTrafficResponse, error) {
	// Get the valid Traffic Window for Properties
	windowPath := "/gtm-api/v1/reports/traffic/properties-window"
	windowReq, err := http.NewRequest(http.MethodGet, windowPath, nil)
	if err != nil {
		return nil, err
	}

	var window WindowResponse
	_, err = p.AkamaiSession.Exec(windowReq, &window)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch property traffic window: %w", err)
	}

	// Ensure the window isn't zero/unparsed
	if window.EndTime.IsZero() || window.EndTime.Year() < 2000 {
		logrus.Warnf("Property window for %s returned invalid dates. Using 48h fallback.", prop)
		window.EndTime = ceilToGTMInterval(time.Now().UTC())
		window.StartTime = floorToGTMInterval(window.EndTime.Add(-48 * time.Hour))
	}

	qargsStart := floorToGTMInterval(start)
	qargsEnd := ceilToGTMInterval(end)

	maxAllowed := floorToGTMInterval(time.Now().UTC().Add(-15 * time.Minute))
	if qargsEnd.After(maxAllowed) {
		qargsEnd = maxAllowed
	}

	if qargsStart.Before(window.StartTime) {
		qargsStart = window.StartTime
	}
	if qargsEnd.After(window.EndTime) {
		qargsEnd = window.EndTime
	}

	if qargsStart.After(qargsEnd) || qargsStart.Equal(qargsEnd) {
		logrus.Warnf("Start/End time outside valid property window for %s. Skipping.", prop)
		return &PropertyTrafficResponse{DataRows: []*PropertyTrafficData{}}, nil
	}

	// Request actual Traffic Data
	path := fmt.Sprintf("/gtm-api/v1/reports/traffic/domains/%s/properties/%s", domain, prop)
	req, err := http.NewRequest(http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}

	q := req.URL.Query()
	q.Add("start", qargsStart.Truncate(time.Second).Format(time.RFC3339))
	q.Add("end", qargsEnd.Truncate(time.Second).Format(time.RFC3339))
	req.URL.RawQuery = q.Encode()

	var result PropertyTrafficResponse
	resp, err := p.AkamaiSession.Exec(req, &result)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API returned non-200 status: %d", resp.StatusCode)
	}

	// Sort results (using the helper for PropertyTrafficData)
	sortPropertyDataRowsByTimestamp(result.DataRows)
	return &result, nil
}
