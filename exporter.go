package main

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"sync"
	"time"

	"net/http"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/log"
	"github.com/voxelbrain/goptions"
)

//TODO remove the prometheus log output when running tests

const (
	namespace = "ingestor"
)

type ingestorResponse struct {
	Consume                *float64 `json:"consume"`
	ConsumePerSecond       *float64 `json:"consume_per_sec"`
	ConsumeFail            *float64 `json:"consume_fail"`
	ConsumeHttpStartStop   *float64 `json:"consume_http_start_stop"`
	ConsumeValueMetric     *float64 `json:"consume_value_metric"`
	ConsumeCounterEvent    *float64 `json:"consume_counter_event"`
	ConsumeLogMessage      *float64 `json:"consume_log_message"`
	ConsumeError           *float64 `json:"consume_error"`
	ConsumeContainerMetric *float64 `json:"consume_container_metric"`
	ConsumeUnknown         *float64 `json:"consume_unknown"`
	Ignored                *float64 `json:"ignored"`
	Forwarded              *float64 `json:"forwarded"`
	Publish                *float64 `json:"publish"`
	PublishPerSecond       *float64 `json:"publish_per_sec"`
	PublishFail            *float64 `json:"publish_fail"`
	SlowConsumerAlert      *float64 `json:"slow_consumer_alert"`
	SubinuptBuffer         *float64 `json:"subinupt_buffer"`
	InstanceID             *float64 `json:"instance_id"`
}

type Properties struct {
	IngestorProtocol                string
	IngestorHostname                string
	IngestorPort                    int
	IngestorPath                    string
	IngestorSkipSsl                 bool
	IngestorResponseTimeoutSeconds  int
	IngestorExporterMetricsEndpoint string
	IngestorExporterPort            int
	IngestorMetricsEnvironment      string
}

type Exporter struct {
	mutex sync.Mutex

	Properties Properties

	consume                *prometheus.Desc
	consumeFail            *prometheus.Desc
	consumeHttpStartStop   *prometheus.Desc
	consumeValueMetric     *prometheus.Desc
	consumeCounterEvent    *prometheus.Desc
	consumeLogMessage      *prometheus.Desc
	consumeError           *prometheus.Desc
	consumeContainerMetric *prometheus.Desc
	consumeUnknown         *prometheus.Desc
	ignored                *prometheus.Desc
	forwarded              *prometheus.Desc
	publish                *prometheus.Desc
	publishFail            *prometheus.Desc
	slowConsumerAlert      *prometheus.Desc
	subinuptBuffer         *prometheus.Desc
	instanceID             *prometheus.Desc
	up                     *prometheus.Desc
	healthy                *prometheus.Desc
}

type Options struct {
	Version bool          `goptions:"-v, --version, description='Show the version'"`
	Help    goptions.Help `goptions:"-h, --help, description='Show this help'"`
}

var Version = "(development)"

func usage() {
	goptions.PrintHelp()
	os.Exit(1)
}

func NewProperties() Properties {
	propertyStruct := Properties{
		IngestorProtocol:                "http",
		IngestorHostname:                "127.0.0.1",
		IngestorPort:                    8080,
		IngestorPath:                    "/stats/app",
		IngestorSkipSsl:                 false,
		IngestorResponseTimeoutSeconds:  10,
		IngestorExporterMetricsEndpoint: "/metrics",
		IngestorExporterPort:            9495,
		IngestorMetricsEnvironment:      "",
	}

	value, ok := os.LookupEnv("INGESTOR_PROTOCOL")
	if ok == true {
		propertyStruct.IngestorProtocol = value
	}

	value, ok = os.LookupEnv("INGESTOR_HOSTNAME")
	if ok == true {
		propertyStruct.IngestorHostname = value
	}

	value, ok = os.LookupEnv("INGESTOR_PORT")
	if ok == true {
		portInt, err := strconv.Atoi(value)
		if err != nil {
			panic(fmt.Sprintf("could not read INGESTOR_PORT %s", value))
		}
		propertyStruct.IngestorPort = portInt
	}

	value, ok = os.LookupEnv("INGESTOR_PATH")
	if ok == true {
		propertyStruct.IngestorPath = value
	}

	value, ok = os.LookupEnv("INGESTOR_SKIP_SSL")
	if ok == true {
		skipSsl, err := strconv.ParseBool(value)
		if err != nil {
			panic(fmt.Sprintf("could not read INGESTOR_SKIP_SSL %s", value))
		}
		propertyStruct.IngestorSkipSsl = skipSsl
	}

	value, ok = os.LookupEnv("INGESTOR_RESPONSE_TIMEOUT_SECONDS")
	if ok == true {
		timeoutSec, err := strconv.Atoi(value)
		if err != nil {
			panic(fmt.Sprintf("could not read INGESTOR_RESPONSE_TIMEOUT_SECONDS %s", value))
		}
		propertyStruct.IngestorResponseTimeoutSeconds = timeoutSec
	}

	value, ok = os.LookupEnv("INGESTOR_EXPORTER_METRICS_ENDPOINT")
	if ok == true {
		propertyStruct.IngestorExporterMetricsEndpoint = value
	}

	value, ok = os.LookupEnv("INGESTOR_EXPORTER_PORT")
	if ok == true {
		portInt, err := strconv.Atoi(value)
		if err != nil {
			panic(fmt.Sprintf("could not read INGESTOR_EXPORTER_PORT %s", value))
		}
		propertyStruct.IngestorExporterPort = portInt
	}

	value, ok = os.LookupEnv("INGESTOR_METRICS_ENVIRONMENT")
	if ok == true {
		propertyStruct.IngestorMetricsEnvironment = value
	}

	return propertyStruct

}

func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	ch <- e.up
	ch <- e.healthy
	ch <- e.consume
	ch <- e.consumeHttpStartStop
	ch <- e.consumeUnknown
	ch <- e.consumeContainerMetric
	ch <- e.consumeError
	ch <- e.consumeLogMessage
	ch <- e.consumeCounterEvent
	ch <- e.consumeValueMetric
	ch <- e.consumeFail
	ch <- e.instanceID
	ch <- e.subinuptBuffer
	ch <- e.slowConsumerAlert
	ch <- e.publishFail
	ch <- e.publish
	ch <- e.forwarded
	ch <- e.ignored
}

func (e *Exporter) collect(ch chan<- prometheus.Metric) error {
	timeout := time.Duration(e.Properties.IngestorResponseTimeoutSeconds) * time.Second
	client := http.Client{
		Timeout: timeout,
	}

	if e.Properties.IngestorSkipSsl == true {
		transCfg := &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // ignore expired SSL certificates
		}

		client.Transport = transCfg
	}

	customUrlString := e.Properties.IngestorProtocol + "://" + e.Properties.IngestorHostname + ":" + strconv.Itoa(e.Properties.IngestorPort) + e.Properties.IngestorPath

	resp, err := client.Get(customUrlString)

	if err != nil {
		ch <- prometheus.MustNewConstMetric(e.up, prometheus.GaugeValue, 0)
		return fmt.Errorf("Error scraping ingestor: %v", err)
	}
	ch <- prometheus.MustNewConstMetric(e.up, prometheus.GaugeValue, 1)

	if resp.StatusCode >= 400 {
		ch <- prometheus.MustNewConstMetric(e.healthy, prometheus.GaugeValue, 0)
		return errors.New(fmt.Sprintf("Got bad status code from ingestor %d", resp.StatusCode))
	}
	ch <- prometheus.MustNewConstMetric(e.healthy, prometheus.GaugeValue, 1)
	defer resp.Body.Close()

	ingestorResponse := &ingestorResponse{}
	bodyBytes, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		log.Fatal(err)
	}

	err = json.Unmarshal([]byte(bodyBytes), ingestorResponse)

	if err != nil {
		log.Fatal(err)
	}

	if ingestorResponse.Consume != nil {
		ch <- prometheus.MustNewConstMetric(e.consume, prometheus.CounterValue, *ingestorResponse.Consume)
	}
	if ingestorResponse.ConsumeHttpStartStop != nil {
		ch <- prometheus.MustNewConstMetric(e.consumeHttpStartStop, prometheus.CounterValue, *ingestorResponse.ConsumeHttpStartStop)
	}
	if ingestorResponse.ConsumeFail != nil {
		ch <- prometheus.MustNewConstMetric(e.consumeFail, prometheus.CounterValue, *ingestorResponse.ConsumeFail)
	}
	if ingestorResponse.ConsumeValueMetric != nil {
		ch <- prometheus.MustNewConstMetric(e.consumeValueMetric, prometheus.CounterValue, *ingestorResponse.ConsumeValueMetric)
	}
	if ingestorResponse.ConsumeCounterEvent != nil {
		ch <- prometheus.MustNewConstMetric(e.consumeCounterEvent, prometheus.CounterValue, *ingestorResponse.ConsumeCounterEvent)
	}
	if ingestorResponse.ConsumeLogMessage != nil {
		ch <- prometheus.MustNewConstMetric(e.consumeLogMessage, prometheus.CounterValue, *ingestorResponse.ConsumeLogMessage)
	}
	if ingestorResponse.ConsumeError != nil {
		ch <- prometheus.MustNewConstMetric(e.consumeError, prometheus.CounterValue, *ingestorResponse.ConsumeError)
	}
	if ingestorResponse.ConsumeContainerMetric != nil {
		ch <- prometheus.MustNewConstMetric(e.consumeContainerMetric, prometheus.CounterValue, *ingestorResponse.ConsumeContainerMetric)
	}
	if ingestorResponse.ConsumeUnknown != nil {
		ch <- prometheus.MustNewConstMetric(e.consumeUnknown, prometheus.CounterValue, *ingestorResponse.ConsumeUnknown)
	}
	if ingestorResponse.SlowConsumerAlert != nil {
		ch <- prometheus.MustNewConstMetric(e.slowConsumerAlert, prometheus.CounterValue, *ingestorResponse.SlowConsumerAlert)
	}
	if ingestorResponse.Publish != nil {
		ch <- prometheus.MustNewConstMetric(e.publish, prometheus.CounterValue, *ingestorResponse.Publish)
	}
	if ingestorResponse.PublishFail != nil {
		ch <- prometheus.MustNewConstMetric(e.publishFail, prometheus.CounterValue, *ingestorResponse.PublishFail)
	}
	if ingestorResponse.Ignored != nil {
		ch <- prometheus.MustNewConstMetric(e.ignored, prometheus.CounterValue, *ingestorResponse.Ignored)
	}
	if ingestorResponse.Forwarded != nil {
		ch <- prometheus.MustNewConstMetric(e.forwarded, prometheus.CounterValue, *ingestorResponse.Forwarded)
	}
	if ingestorResponse.SubinuptBuffer != nil {
		ch <- prometheus.MustNewConstMetric(e.subinuptBuffer, prometheus.GaugeValue, *ingestorResponse.SubinuptBuffer)
	}
	if ingestorResponse.InstanceID != nil {
		ch <- prometheus.MustNewConstMetric(e.instanceID, prometheus.GaugeValue, *ingestorResponse.InstanceID)
	}
	return nil
}

// Collect fetches the stats from configured ingestor location and delivers them
// as Prometheus metrics.
// It implements prometheus.Collector.
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	e.mutex.Lock() // To protect metrics from concurrent collects.
	defer e.mutex.Unlock()
	if err := e.collect(ch); err != nil {
		log.Errorf("Error scraping ingestor: %s", err)
	}
	return
}

func NewExporter(properties Properties) *Exporter {
	constLabels := map[string]string{
		"ingestor_protocol": properties.IngestorProtocol,
		"ingestor_hostname": properties.IngestorHostname,
		"ingestor_port":     strconv.Itoa(properties.IngestorPort),
		"ingestor_path":     properties.IngestorPath,
		"environment":       properties.IngestorMetricsEnvironment,
	}

	return &Exporter{
		Properties: properties,
		up: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "up"),
			"Could ingestor be reached",
			nil,
			constLabels),
		healthy: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "healthy"),
			"Is ingestor giving successful status codes",
			nil,
			constLabels),
		consume: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "consume_total"),
			"Messages received",
			nil,
			constLabels),
		consumeFail: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "consume_fail_total"),
			"Messages failed to be consumed",
			nil,
			constLabels),
		consumeHttpStartStop: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "consume_http_start_stop_total"),
			"HttpStartStop messages received",
			nil,
			constLabels),
		consumeValueMetric: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "consume_value_metric_total"),
			"ValueMetric messages received",
			nil,
			constLabels),
		consumeCounterEvent: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "consume_counter_event_total"),
			"CounterEvent messages received",
			nil,
			constLabels),
		consumeLogMessage: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "consume_log_message_total"),
			"Log messages received",
			nil,
			constLabels),
		consumeError: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "consume_error_total"),
			"Error messages received",
			nil,
			constLabels),
		consumeContainerMetric: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "consume_container_metric_total"),
			"ContainerMetric messages received",
			nil,
			constLabels),
		consumeUnknown: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "consume_unknown_total"),
			"Unknown type messages received",
			nil,
			constLabels),
		ignored: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "ignored_total"),
			"Messages dropped due to no forwarding rule",
			nil,
			constLabels),
		forwarded: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "forwarded_total"),
			"Messages enqueued to be sent to syslog endpoint",
			nil,
			constLabels),
		publish: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "publish_total"),
			"Messages succesfully sent to syslog endpoint",
			nil,
			constLabels),
		publishFail: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "publish_fail_total"),
			"Messages that couldn't be sent to syslog endpoint",
			nil,
			constLabels),
		slowConsumerAlert: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "slow_consumer_alert_total"),
			"Slow consumer alerts emitted by noaa",
			nil,
			constLabels),
		subinuptBuffer: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "subinupt_buffer"),
			"Messages in buffer",
			nil,
			constLabels),
		instanceID: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "instance_id"),
			"ID for nozzle instance. This is used to identify stats from different instances",
			nil,
			constLabels),
	}
}

func main() {
	options := Options{}
	err := goptions.Parse(&options)
	if err != nil {
		usage()
	}

	if options.Version {
		fmt.Printf("%s - Version %s\n", os.Args[0], Version)
		os.Exit(0)
	}

	properties := NewProperties()
	exporter := NewExporter(properties)

	prometheus.MustRegister(exporter)
	http.Handle(exporter.Properties.IngestorExporterMetricsEndpoint, promhttp.Handler())
	ingestorExporterPort := ":" + strconv.Itoa(exporter.Properties.IngestorExporterPort)
	err = http.ListenAndServe(ingestorExporterPort, nil)
	if err != nil {
		panic(err)
	}
}
