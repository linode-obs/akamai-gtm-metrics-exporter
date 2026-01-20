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
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	// New Akamai v12 Packages
	"github.com/akamai/AkamaiOPEN-edgegrid-golang/v12/pkg/edgegrid"
	"github.com/akamai/AkamaiOPEN-edgegrid-golang/v12/pkg/session"
	// Keep configgtm for link structures if needed, or define locally
)

const (
	GTMTrafficLongTimeFormat string = "2006-01-02T15:04:05Z"
	GTMTrafficDateFormat     string = "2006-01-02"
)

var (
	// EdgegridConfig contains the Akamai OPEN Edgegrid API credentials for automatic signing of requests
	EdgegridConfig edgegrid.Config = edgegrid.Config{}
	// testflag is used for test automation only
)

type Metadata struct {
	Domain     string `json:"domain"`
	Datacenter int    `json:"datacenterId"`
	Property   string `json:"property"`
	StartTime  string `json:"start"`
	EndTime    string `json:"end"`
}

type WindowResponse struct {
	StartTime time.Time
	EndTime   time.Time
}

// GTM Reports Query args struct
type GTMReportQueryArgs struct {
	End      string `json:"end"`   // YYYY-MM-DDThh:mm:ssZ in UTC
	Start    string `json:"start"` // YYYY-MM-DDThh:mm:ssZ in UTC
	Date     string `json:"date"`  // YYYY-MM-DD format
	AgentIP  string `json:"agentIp"`
	TargetIP string `json:"targetIp"`
}

// Liveness Errors Report Structs
type LivenessTMeta struct {
	URI      string
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
}

type LivenessTData struct {
	Timestamp   string          `json:"timestamp"`
	Datacenters []*LivenessDRow `json:"datacenters"`
}

// The Liveness Errors Response structure returned by the Reports API
type LivenessErrorsResponse struct {
	Metadata    *LivenessTMeta   `json:"metadata"`
	DataRows    []*LivenessTData `json:"dataRows"`
	DataSummary interface{}      `json:"dataSummary"`
	//Links       []*configgtm.Link `json:"links"`
}

type DatacenterTrafficData struct {
	Timestamp  string            `json:"timestamp"`
	Properties []TrafficProperty `json:"properties"`
}

// DcTrafficResponse is the top-level response for Datacenter traffic
type DcTrafficResponse struct {
	Metadata Metadata                 `json:"metadata"`
	DataRows []*DatacenterTrafficData `json:"dataRows"`
}

// 1. Rename PropertyTData to PropertyTrafficData so it matches the sorter
type PropertyTrafficData struct {
	Timestamp   string           `json:"timestamp"`
	Datacenters []PropertyDCData `json:"datacenters"`
}

// 2. Ensure the Response uses the renamed type
type PropertyTrafficResponse struct {
	Metadata Metadata               `json:"metadata"`
	DataRows []*PropertyTrafficData `json:"dataRows"`
}

// 3. The Datacenter breakdown used inside the row
type PropertyDCData struct {
	Nickname          string `json:"nickname"`
	DatacenterId      int    `json:"datacenterId"`
	TrafficTargetName string `json:"trafficTargetName"`
	Requests          int64  `json:"requests"`
}

type TrafficProperty struct {
	Name     string `json:"name"`
	Requests int64  `json:"requests"`
}

// Rounds DOWN to the nearest 5-minute boundary
func floorToGTMInterval(t time.Time) time.Time {
	return t.Truncate(5 * time.Minute)
}

// Rounds UP to the next 5-minute boundary
func ceilToGTMInterval(t time.Time) time.Time {
	if t.Truncate(5 * time.Minute).Equal(t) {
		return t
	}
	return t.Truncate(5 * time.Minute).Add(5 * time.Minute)
}

// NewSession creates the authenticated Akamai session required for all calls
func NewSession(edgercpath, section string) (session.Session, error) {
	config, err := edgegrid.New(
		edgegrid.WithFile(edgercpath),
		edgegrid.WithSection(section),
	)
	if err != nil {
		return nil, fmt.Errorf("edgegrid initialization failed: %w", err)
	}

	sess, err := session.New(session.WithSigner(config))
	if err != nil {
		return nil, fmt.Errorf("session creation failed: %w", err)
	}

	return sess, nil
}

// GetLivenessErrorsReport refactored for v12 Session with original logic parity
func GetLivenessErrorsReport(sess session.Session, domainName, propertyName string, queryArgs map[string]string) (*LivenessErrorsResponse, error) {
	path := fmt.Sprintf("/gtm-api/v1/reports/liveness-tests/domains/%s/properties/%s", domainName, propertyName)

	// 1. Create the Request
	req, err := http.NewRequest(http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}

	// 2. Original Logic: Mandatory Date check
	if _, ok := queryArgs["date"]; !ok {
		return nil, fmt.Errorf("GetLivenessErrorsReport: date parameter is required")
	}

	// 3. Original Logic: Specific Query Parameter filtering
	q := req.URL.Query()
	for k, v := range queryArgs {
		switch k {
		case "date", "agentIp", "targetIp": // Only allow these specific keys
			q.Add(k, v)
		}
	}
	req.URL.RawQuery = q.Encode()

	// 4. Original Requirement: Content-Type header
	// Even for GETs, the original code explicitly required this for timestamp processing
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	// 5. Execute using v12 Session
	var result LivenessErrorsResponse
	resp, err := sess.Exec(req, &result)

	// 6. Detailed Error Handling mimicking the original client.Do logic
	if err != nil {
		// If it's a 404, we handle it specifically like the original code
		if resp != nil && resp.StatusCode == http.StatusNotFound {
			// Note: If you have configgtm imported, you can return cErr.
			// Otherwise, a formatted error is best for v12.
			return nil, fmt.Errorf("liveness report not found (404) for property: %s", propertyName)
		}
		return nil, fmt.Errorf("API request failed: %w", err)
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("Akamai API returned error status: %d", resp.StatusCode)
	}

	return &result, nil
}

// --- Utility Functions (Cleaned up) ---

func convertTimeFormat(src time.Time, format string) (string, error) {
	t := src.UTC().Format(time.RFC3339)
	if format == time.RFC3339 {
		return t, nil
	}
	// Simplified parsing logic
	tslice := strings.Split(t, "Z")
	if format == GTMTrafficLongTimeFormat {
		return tslice[0] + "Z", nil
	}
	if format == GTMTrafficDateFormat {
		return strings.Split(tslice[0], "T")[0], nil
	}
	return "", fmt.Errorf("invalid format")
}

// Create and return new GTMReportQueryArgs object
func NewGTMReportQueryArgs() *GTMReportQueryArgs {

	return &GTMReportQueryArgs{}
}

// Util function to convert string to time.Time object
func parseTimeString(srctime, format string) (time.Time, error) {

	ts, err := time.Parse(format, srctime)

	return ts, err
}

// Update the sorting helpers to use these local types
func sortDCDataRowsByTimestamp(drs []*DatacenterTrafficData) {
	sort.Slice(drs, func(i, j int) bool {
		return drs[i].Timestamp < drs[j].Timestamp
	})
}

func sortPropertyDataRowsByTimestamp(drs []*PropertyTrafficData) {
	sort.Slice(drs, func(i, j int) bool {
		return drs[i].Timestamp < drs[j].Timestamp
	})
}

func sortLivenessDataRowsByTimestamp(drs []*LivenessTData) {

	sort.Slice(drs, func(i, j int) bool {
		return drs[i].Timestamp < drs[j].Timestamp
	})
}

func stringSliceContains(sl []string, entry string) bool {

	for _, e := range sl {
		if e == entry {
			return true
		}
	}

	return false

}

func intSliceContains(sl []int, entry int) bool {

	for _, e := range sl {
		if e == entry {
			return true
		}
	}

	return false

}
