// Copyright 2020 Akamai Technologies, Inc.
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
	"testing"

	"github.com/akamai/AkamaiOPEN-edgegrid-golang/v12/pkg/edgegrid"
	"github.com/akamai/AkamaiOPEN-edgegrid-golang/v12/pkg/session"
	"github.com/stretchr/testify/assert"
	"gopkg.in/h2non/gock.v1"
)

func mockV12Session() session.Session {
	config := &edgegrid.Config{
		Host:         "akaa-baseurl-xxxxxxxxxxx-xxxxxxxxxxxxx.luna.akamaiapis.net",
		ClientToken:  "akab-client-token-xxx-xxxxxxxxxxxxxxxx",
		ClientSecret: "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx=",
		AccessToken:  "akab-access-token-xxx-xxxxxxxxxxxxxxxx",
	}

	// session.New takes the config and returns (Session, error)
	sess, err := session.New(session.WithSigner(config))
	if err != nil {
		panic(fmt.Sprintf("failed to create mock session: %v", err))
	}

	return sess
}
func TestGetTrafficReport(t *testing.T) {
	dnsTestDomain := "testdomain.com.akadns.net"
	dnsTestProperty := "testprop"
	queryargs := map[string]string{"date": "2016/11/23"}

	sess := mockV12Session()

	defer gock.Off()
	mock := gock.New("https://akaa-baseurl-xxxxxxxxxxx-xxxxxxxxxxxxx.luna.akamaiapis.net")
	mock.
		Get(fmt.Sprintf("/gtm-api/v1/reports/liveness-tests/domains/%s/properties/%s", dnsTestDomain, dnsTestProperty)).
		MatchParam("date", "2016/11/23").
		Reply(200).
		BodyString(`{
                "metadata": {
                    "date": "2016-11-23",
                    "domain": "example.akadns.net",
                    "property": "www",
                "uri": "https://akab-xxxxxxxxxxxxxxxx-xxxxxxxxxxxxxxxx.luna.akamaiapis.net/gtm-api/v1/reports/liveness-tests/domains/example.akadns.net/properties/www?date=2016-11-23"
                },
                "dataRows": [ {
                        "timestamp": "2016-11-23T00:13:23Z",
                        "datacenters": [ {
                                "datacenterId": 3201,
                                "agentIp": "204.1.136.239",
                                "testName": "Our defences",
                                "errorCode": 3101,
                                "duration": 0,
                                "nickname": "Winterfell",
                                "trafficTargetName": "Winterfell - 1.2.3.4",
                                "targetIp": "1.2.3.4"
                        } ]
                } ]
        }`)

	report, err := GetLivenessErrorsReport(sess, dnsTestDomain, dnsTestProperty, queryargs)

	assert.NoError(t, err)
	assert.NotNil(t, report)
	assert.Equal(t, report.Metadata.Date, "2016-11-23")
}

func TestGetTrafficReport_BadArg(t *testing.T) {
	dnsTestDomain := "testdomain.com.akadns.net"
	dnsTestProperty := "testprop"
	queryargs := map[string]string{"date": "2016/11/23"}

	sess := mockV12Session()

	defer gock.Off()
	mock := gock.New("https://akaa-baseurl-xxxxxxxxxxx-xxxxxxxxxxxxx.luna.akamaiapis.net")
	mock.
		Get(fmt.Sprintf("/gtm-api/v1/reports/liveness-tests/domains/%s/properties/%s", dnsTestDomain, dnsTestProperty)).
		Reply(500).
		BodyString(`Server Error`)

	_, err := GetLivenessErrorsReport(sess, dnsTestDomain, dnsTestProperty, queryargs)

	assert.Error(t, err)
}
