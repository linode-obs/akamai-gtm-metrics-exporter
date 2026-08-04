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

	"github.com/akamai/AkamaiOPEN-edgegrid-golang/v13/pkg/edgegrid"
	"github.com/akamai/AkamaiOPEN-edgegrid-golang/v13/pkg/session"
)

const (
	GTMTrafficLongTimeFormat string = "2006-01-02T15:04:05Z"
	GTMTrafficDateFormat     string = "2006-01-02"
)

// --- Structs & Types ---

type Metadata struct {
	Uri                string `json:"uri"`
	Domain             string `json:"domain"`
	Interval           string `json:"interval,omitempty"`
	DatacenterId       int    `json:"datacenterId"`
	DatacenterNickname string `json:"datacenterNickname"`
	Start              string `json:"start"`
	End                string `json:"end"`
}

type WindowResponse struct {
	StartTime time.Time `json:"start"`
	EndTime   time.Time `json:"end"`
}

type GTMReportQueryArgs struct {
	End      string `json:"end"`
	Start    string `json:"start"`
	Date     string `json:"date"`
	AgentIP  string `json:"agentIp"`
	TargetIP string `json:"targetIp"`
}

// Datacenter Traffic Structs
type TrafficProperty struct {
	Name     string `json:"name"`
	Requests int64  `json:"requests"`
	Status   string `json:"status"` // Restored status field
}

// --- Initialization & Session ---

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

// --- API Implementation ---

func GetLivenessErrorsReport(sess session.Session, domainName, propertyName string, queryArgs map[string]string) (*LivenessErrorsResponse, error) {
	path := fmt.Sprintf("/gtm-api/v1/reports/liveness-tests/domains/%s/properties/%s", domainName, propertyName)

	req, err := http.NewRequest(http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}

	if _, ok := queryArgs["date"]; !ok {
		return nil, fmt.Errorf("GetLivenessErrorsReport: date parameter is required")
	}

	q := req.URL.Query()
	for k, v := range queryArgs {
		switch k {
		case "date", "agentIp", "targetIp":
			q.Add(k, v)
		}
	}
	req.URL.RawQuery = q.Encode()
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	var result LivenessErrorsResponse
	resp, err := sess.Exec(req, &result)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("akamai API returned error status: %d", resp.StatusCode)
	}

	return &result, nil
}

// --- Utility Functions ---

func convertTimeFormat(src time.Time, format string) (string, error) {
	t := src.UTC().Format(time.RFC3339)
	if format == time.RFC3339 {
		return t, nil
	}

	if format == GTMTrafficLongTimeFormat {
		tslice := strings.Split(t, "Z")
		return tslice[0] + "Z", nil
	}

	if format == GTMTrafficDateFormat {
		tslice := strings.Split(t, "T")
		return tslice[0], nil
	}
	return "", fmt.Errorf("invalid time format")
}

func parseTimeString(srctime, format string) (time.Time, error) {
	return time.Parse(format, srctime)
}

// --- Sorters ---

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

// --- Helpers ---

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
