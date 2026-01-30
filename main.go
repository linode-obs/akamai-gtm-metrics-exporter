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

package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/akamai/AkamaiOPEN-edgegrid-golang/v12/pkg/edgegrid"
	"github.com/akamai/AkamaiOPEN-edgegrid-golang/v12/pkg/session"
	"github.com/akamai/akamai-gtm-metrics-exporter/collectors"
	kingpin "github.com/alecthomas/kingpin/v2"
	"github.com/prometheus/client_golang/prometheus"
	promcollectors "github.com/prometheus/client_golang/prometheus/collectors"
	buildversion "github.com/prometheus/client_golang/prometheus/collectors/version"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/version"
	"github.com/sirupsen/logrus"
	yaml "gopkg.in/yaml.v3"
)

const (
	defaultlistenaddress    = ":9800"
	namespace               = "akamai_gtm_"
	HoursInDay              = 24
	trafficReportInterval   = 5
	lookbackDefaultDuration = 2 * 24 * time.Hour
	prefillDefaultDuration  = 10 * time.Minute
)

var (
	configFile           = kingpin.Flag("config.file", "GTM Metrics exporter configuration file. Default: ./gtm_metrics_config.yml").Default("gtm_metrics_config.yml").String()
	listenAddress        = kingpin.Flag("web.listen-address", "The address to listen on for HTTP requests.").Default(defaultlistenaddress).String()
	edgegridHost         = kingpin.Flag("gtm.edgegrid-host", "The Akamai Edgegrid host auth credential.").String()
	edgegridClientSecret = kingpin.Flag("gtm.edgegrid-client-secret", "The Akamai Edgegrid client_secret credential.").String()
	edgegridClientToken  = kingpin.Flag("gtm.edgegrid-client-token", "The Akamai Edgegrid client_token credential.").String()
	edgegridAccessToken  = kingpin.Flag("gtm.edgegrid-access-token", "The Akamai Edgegrid access_token credential.").String()
	logLevel             = kingpin.Flag("log.level", "Set the logging level (debug, info, warn, error, fatal)").Default("info").String()
	timestampLabel       = kingpin.Flag("gtm.timestamp-label", "Creates time series with traffic timestamp as label.").Bool()
	trafficTimestamp     = kingpin.Flag("gtm.traffic-timestamp", "Create time series with traffic timestamp.").Bool()
	logFormat            = kingpin.Flag("log.format", "Set the log target and format. Example: logger:stderr or logger:stdout?json=true").Default("logger:stderr").String()

	// invalidMetricChars    = regexp.MustCompile("[^a-zA-Z0-9_:]")
	lookbackDuration = lookbackDefaultDuration
	prefillDuration  = prefillDefaultDuration
)

// Initialize Akamai Edgegrid Config. Priority order:
// 1. Command line
// 2. Edgerc path
// 3. Environment
// 4. Default
func initAkamaiSession(gtmMetricsConfig collectors.GTMMetricsConfig) (session.Session, error) {
	var config *edgegrid.Config
	var err error

	// 1. Try Flags first
	if *edgegridHost != "" && *edgegridClientSecret != "" && *edgegridClientToken != "" && *edgegridAccessToken != "" {
		config = &edgegrid.Config{
			Host:         *edgegridHost,
			ClientToken:  *edgegridClientToken,
			ClientSecret: *edgegridClientSecret,
			AccessToken:  *edgegridAccessToken,
		}
	}

	// 2. If flags weren't used/complete, use the EdgegridInit pattern
	if config == nil {
		options := []edgegrid.Option{edgegrid.WithEnv(true)}
		if gtmMetricsConfig.EdgercPath != "" {
			options = append(options, edgegrid.WithFile(gtmMetricsConfig.EdgercPath))
			options = append(options, edgegrid.WithSection(gtmMetricsConfig.EdgercSection))
		}

		config, err = edgegrid.New(options...)
		if err != nil {
			return nil, err
		}
	}

	if config.Host == "" {
		return nil, fmt.Errorf("akamai host is empty: check your environment variables (AKAMAI_HOST) or edgerc file")
	}

	return session.New(session.WithSigner(config), session.WithHTTPTracing(false))
}

// Calculate window duration based on config and save in lookbackDuration global variable
func calcWindowDuration(window string) (time.Duration, error) {

	var datawin int
	var err error
	var multiplier time.Duration = time.Hour * time.Duration(HoursInDay)

	logrus.Debugf("Window: %s", window)
	if window == "" {
		return time.Second * 0, fmt.Errorf("Summary window not set")
	}
	iunit := window[len(window)-1:]
	if !strings.Contains("mhd", strings.ToLower(iunit)) {
		// no units. default days
		datawin, err = strconv.Atoi(window)
	} else {
		len := window[0 : len(window)-1]
		datawin, err = strconv.Atoi(len)
		if strings.ToLower(iunit) == "m" {
			multiplier = time.Minute
			if err == nil && datawin < trafficReportInterval {
				datawin = trafficReportInterval
			}
		} else if strings.ToLower(iunit) == "h" {
			multiplier = time.Hour
		}
	}
	if err != nil {
		logrus.Warnf("ERROR: %s", err.Error())
		return time.Second * 0, err
	}
	logrus.Debugf("multiplier: [%v} units: [%v]", multiplier, datawin)
	return multiplier * time.Duration(datawin), nil

}

func setupLogging(level string, format string) {
	lvl, err := logrus.ParseLevel(level)
	if err != nil {
		logrus.Fatalf("Invalid log level: %v", err)
	}
	logrus.SetLevel(lvl)

	if strings.Contains(format, "json=true") {
		logrus.SetFormatter(&logrus.JSONFormatter{})
	} else {
		logrus.SetFormatter(&logrus.TextFormatter{
			FullTimestamp: true,
		})
	}

	if strings.Contains(format, "stdout") {
		logrus.SetOutput(os.Stdout)
	} else {
		logrus.SetOutput(os.Stderr)
	}
}

func boolPtr(b bool) *bool {
	return &b
}

func main() {
	kingpin.Version(version.Print(namespace + "metrics_exporter"))
	kingpin.HelpFlag.Short('h')
	kingpin.Parse()

	setupLogging(*logLevel, *logFormat)

	logrus.Infof("Config file: %s", *configFile)
	logrus.Infof("Starting GTM Metrics exporter %s", version.Info())
	logrus.Infof("Build context: %s", version.BuildContext())

	gtmMetricsConfig, err := loadConfig(*configFile)
	if err != nil {
		logrus.Fatalf("Error loading config: %v", err)
	}
	logrus.Debugf("Exporter configuration: [%v]", gtmMetricsConfig)

	if *timestampLabel {
		gtmMetricsConfig.TSLabel = true
	}
	if *trafficTimestamp {
		gtmMetricsConfig.UseTimestamp = boolPtr(true)
	}

	// Initialize Session
	akamaiSession, err := initAkamaiSession(gtmMetricsConfig)
	if err != nil {
		logrus.Fatalf("Error initializing Akamai session: %v", err)
	}

	ctx := context.Background()

	// Time window calculations
	tstart := time.Now().UTC().Add(-15 * time.Minute).Add(-1 * prefillDuration) // assume start time is Exporter launch less default prefill
	if gtmMetricsConfig.SummaryWindow != "" {
		lookbackDuration, err = calcWindowDuration(gtmMetricsConfig.SummaryWindow)
		if err != nil {
			logrus.Warnf("Summary Retention window is not valid. Using default")
		}
	} else {
		logrus.Warnf("Summary Retention window is not configured. Using default")
	}
	if gtmMetricsConfig.PreFillWindow != "" {
		prefillDuration, err = calcWindowDuration(gtmMetricsConfig.PreFillWindow)
		if err == nil {
			tstart = time.Now().UTC().Add(prefillDuration * -1)
		} else {
			logrus.Warnf("Prefill window is not valid. Using default")
		}
	} else {
		logrus.Warnf("Prefill window is not configured. Using default")
	}

	tstart = tstart.Truncate(5 * time.Minute)

	logrus.Infof("GTM Metrics exporter lookback: %v, start time: %v", lookbackDuration, tstart)

	// Create Prometheus Registry
	r := prometheus.NewRegistry()
	r.MustRegister(
		// Use 'promcollectors' alias
		promcollectors.NewProcessCollector(promcollectors.ProcessCollectorOpts{}),
		promcollectors.NewGoCollector(),

		// Use 'buildversion' alias
		buildversion.NewCollector(namespace+"metrics_exporter"),
	)

	logrus.Infof("Starting exporter %s", version.Info())

	// Register the various GTM collectors
	r.MustRegister(collectors.NewDatacenterTrafficCollector(ctx, akamaiSession, r, gtmMetricsConfig, namespace, tstart, lookbackDuration))
	r.MustRegister(collectors.NewPropertyTrafficCollector(ctx, akamaiSession, r, gtmMetricsConfig, namespace, tstart, lookbackDuration))
	r.MustRegister(collectors.NewLivenessTrafficCollector(ctx, akamaiSession, r, gtmMetricsConfig, namespace, tstart, lookbackDuration))

	// Define HTTP handlers
	http.Handle("/metrics", promhttp.HandlerFor(r, promhttp.HandlerOpts{Registry: r}))
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
            <head><title>akamai_gtm_metrics_exporter</title></head>
            <body>
            <h1>akamai_gtm_metrics_exporter</h1>
            <p><a href="/metrics">Metrics</a></p>
            </body>
            </html>`))
	})

	logrus.Infof("Beginning to serve on address %s", *listenAddress)
	if err := http.ListenAndServe(*listenAddress, nil); err != nil {
		logrus.Fatalf("Error starting HTTP server: %v", err)
	}
}

func loadConfig(configFile string) (collectors.GTMMetricsConfig, error) {
	if fileExists(configFile) {
		configData, err := os.ReadFile(configFile)
		if err != nil {
			return collectors.GTMMetricsConfig{}, err
		}
		logrus.Debugf("GTM metrics config file: %s", string(configData))
		return loadConfigContent(configData)
	}

	logrus.Infof("Config file %v does not exist, using default values", configFile)
	return collectors.GTMMetricsConfig{}, nil
}

func loadConfigContent(configData []byte) (collectors.GTMMetricsConfig, error) {
	domains := make([]*collectors.DomainTraffic, 0)
	domains = append(domains, &collectors.DefaultDomainTraffic)
	config := collectors.GTMMetricsConfig{Domains: domains}
	err := yaml.Unmarshal(configData, &config)
	if err != nil {
		return config, err
	}

	logrus.Info("akamai_gtm_metrics_exporter config loaded")
	return config, nil
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}
