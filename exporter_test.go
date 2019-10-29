package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	sampleIngestorResponse = ingestorResponse{
		Consume:                Float64Pointer(0),
		ConsumeFail:            Float64Pointer(1),
		ConsumeHttpStartStop:   Float64Pointer(2),
		ConsumeValueMetric:     Float64Pointer(3),
		ConsumeCounterEvent:    Float64Pointer(4),
		ConsumeLogMessage:      Float64Pointer(5),
		ConsumeError:           Float64Pointer(6),
		ConsumeContainerMetric: Float64Pointer(7),
		ConsumeUnknown:         Float64Pointer(8),
		Ignored:                Float64Pointer(9),
		Forwarded:              Float64Pointer(10),
		Publish:                Float64Pointer(11),
		PublishFail:            Float64Pointer(12),
		SlowConsumerAlert:      Float64Pointer(13),
		SubinuptBuffer:         Float64Pointer(14),
		InstanceID:             Float64Pointer(15),
	}

	allZerosIngestorResponse = ingestorResponse{
		Consume:                Float64Pointer(0),
		ConsumeFail:            Float64Pointer(0),
		ConsumeHttpStartStop:   Float64Pointer(0),
		ConsumeValueMetric:     Float64Pointer(0),
		ConsumeCounterEvent:    Float64Pointer(0),
		ConsumeLogMessage:      Float64Pointer(0),
		ConsumeError:           Float64Pointer(0),
		ConsumeContainerMetric: Float64Pointer(0),
		ConsumeUnknown:         Float64Pointer(0),
		Ignored:                Float64Pointer(0),
		Forwarded:              Float64Pointer(0),
		Publish:                Float64Pointer(0),
		PublishFail:            Float64Pointer(0),
		SlowConsumerAlert:      Float64Pointer(0),
		SubinuptBuffer:         Float64Pointer(0),
		InstanceID:             Float64Pointer(0),
	}
)

func mapKeysInAlphabeticalOrder(m map[string]string) []string {
	keysSorted := make([]string, len(m))
	i := 0
	for k, _ := range m {
		keysSorted[i] = k
		i++
	}
	sort.Strings(keysSorted)
	return keysSorted
}

func prometheusTagsToString(tagsMap map[string]string) string {
	// first we get an array of tag keys in alphabetical order
	keysSorted := mapKeysInAlphabeticalOrder(tagsMap)

	// then we build a prometheus-tags-string- but with tags in alphabetical order
	tagsStr := "{"
	for _, sortedKey := range keysSorted {
		tagsStr += sortedKey + `="` + tagsMap[sortedKey] + `",`
	}
	tagsStr = strings.TrimSuffix(tagsStr, ",")
	tagsStr += "}"

	return tagsStr
}

func exporterPropertiesToTagsMap(props Properties) map[string]string {
	tagsMap := map[string]string{
		"environment": props.IngestorMetricsEnvironment,
		"ingestor_hostname": props.IngestorHostname,
		"ingestor_path": props.IngestorPath,
		"ingestor_port": strconv.Itoa(props.IngestorPort),
		"ingestor_protocol": props.IngestorProtocol,
	}

	return tagsMap
}

func responseCall(expectedSubString string) {

	req := httptest.NewRequest("GET", "http://myingestorexporter.com/metrics", nil) //URL of this request does not matter

	recorder := httptest.NewRecorder()
	promhttp.Handler().ServeHTTP(recorder, req)

	resp := recorder.Result()
	body, err := ioutil.ReadAll(resp.Body)
	Expect(err).To(BeNil())
	Expect(resp.StatusCode).To(Equal(http.StatusOK))
	Expect(resp.Header.Get("Content-Type")).To(Equal("text/plain; version=0.0.4; charset=utf-8"))
	Expect(string(body)).To(ContainSubstring(expectedSubString))
}

func NotExpectedReponse(expectedSubString string) {
	req := httptest.NewRequest("GET", "http://myingestorexporter.com/metrics", nil) //URL of this request does not matter

	recorder := httptest.NewRecorder()
	promhttp.Handler().ServeHTTP(recorder, req)

	resp := recorder.Result()
	body, err := ioutil.ReadAll(resp.Body)
	Expect(err).To(BeNil())
	Expect(resp.StatusCode).To(Equal(http.StatusOK))
	Expect(resp.Header.Get("Content-Type")).To(Equal("text/plain; version=0.0.4; charset=utf-8"))
	Expect(string(body)).To(Not(ContainSubstring(expectedSubString)))
}

func Float64Pointer(number float64) *float64 {
	return &number
}

var _ = Describe("ingestor exporter", func() {
	It("returns 200- single metrics call", func() {
		fakeIngestorServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			marshallIngestor, err := json.Marshal(sampleIngestorResponse)
			Expect(err).To(BeNil())
			_, err = fmt.Fprintln(w, string(marshallIngestor))
			Expect(err).To(BeNil())
		}))
		defer fakeIngestorServer.Close()

		s := strings.Split(fakeIngestorServer.URL, "://")
		protocolValue, portWithHostnameValue := s[0], s[1]

		s2 := strings.Split(portWithHostnameValue, ":")
		hostnameValue, portValue := s2[0], s2[1]

		portValue2, _ := strconv.Atoi(portValue)

		propertiesStruct := Properties{
			IngestorProtocol:                protocolValue,
			IngestorHostname:                hostnameValue,
			IngestorPort:                    portValue2,
			IngestorPath:                    "/stats/app",
			IngestorSkipSsl:                 true,
			IngestorResponseTimeoutSeconds:  1,
			IngestorExporterMetricsEndpoint: "/metrics",
			IngestorMetricsEnvironment:		 "dev",
		}

		req := httptest.NewRequest("GET", "http://myingestorexporter.com/metrics", nil) //URL of this request does not matter

		recorder := httptest.NewRecorder()
		exporter := NewExporter(propertiesStruct)
		prometheus.MustRegister(exporter)
		promhttp.Handler().ServeHTTP(recorder, req)

		resp := recorder.Result()
		body, err := ioutil.ReadAll(resp.Body)
		Expect(err).To(BeNil())
		Expect(resp.StatusCode).To(Equal(http.StatusOK))
		Expect(resp.Header.Get("Content-Type")).To(Equal("text/plain; version=0.0.4; charset=utf-8"))

		tagsMap := exporterPropertiesToTagsMap(propertiesStruct)
		tagsStr := prometheusTagsToString(tagsMap)

		Expect(string(body)).To(ContainSubstring("ingestor_consume_total" + tagsStr + " 0"))
		Expect(string(body)).To(ContainSubstring("ingestor_consume_fail_total" + tagsStr + " 1"))
		Expect(string(body)).To(ContainSubstring("ingestor_consume_http_start_stop_total" + tagsStr + " 2"))
		Expect(string(body)).To(ContainSubstring("ingestor_consume_value_metric_total" + tagsStr + " 3"))
		Expect(string(body)).To(ContainSubstring("ingestor_consume_counter_event_total" + tagsStr + " 4"))
		Expect(string(body)).To(ContainSubstring("ingestor_consume_log_message_total" + tagsStr + " 5"))
		Expect(string(body)).To(ContainSubstring("ingestor_consume_error_total" + tagsStr + " 6"))
		Expect(string(body)).To(ContainSubstring("ingestor_consume_container_metric_total" + tagsStr + " 7"))
		Expect(string(body)).To(ContainSubstring("ingestor_consume_unknown_total" + tagsStr + " 8"))
		Expect(string(body)).To(ContainSubstring("ingestor_ignored_total" + tagsStr + " 9"))
		Expect(string(body)).To(ContainSubstring("ingestor_forwarded_total" + tagsStr + " 10"))
		Expect(string(body)).To(ContainSubstring("ingestor_publish_total" + tagsStr + " 11"))
		Expect(string(body)).To(ContainSubstring("ingestor_publish_fail_total" + tagsStr + " 12"))
		Expect(string(body)).To(ContainSubstring("ingestor_slow_consumer_alert_total" + tagsStr + " 13"))
		Expect(string(body)).To(ContainSubstring("ingestor_subinupt_buffer" + tagsStr + " 14"))
		Expect(string(body)).To(ContainSubstring("ingestor_instance_id" + tagsStr + " 15"))
		Expect(string(body)).To(ContainSubstring("ingestor_up" + tagsStr + " 1"))
	})

	It("returns 200 - multiple metrics calls", func() {
		ingestorResponse0 := sampleIngestorResponse
		ingestorResponse1 := allZerosIngestorResponse

		numTimesFakeIngestorServerCalled := 0
		fakeIngestorServerResponses := []ingestorResponse{
			ingestorResponse0,
			ingestorResponse1,
		}

		fakeIngestorServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fakeResponse := fakeIngestorServerResponses[numTimesFakeIngestorServerCalled]
			w.WriteHeader(http.StatusOK)
			marshallIngestor, err := json.Marshal(fakeResponse)
			Expect(err).To(BeNil())
			_, err = fmt.Fprintln(w, string(marshallIngestor))
			Expect(err).To(BeNil())
			numTimesFakeIngestorServerCalled++
		}))
		defer fakeIngestorServer.Close()

		s := strings.Split(fakeIngestorServer.URL, "://")
		protocolValue, portWithHostnameValue := s[0], s[1]

		s2 := strings.Split(portWithHostnameValue, ":")
		hostnameValue, portValue := s2[0], s2[1]

		portValue2, _ := strconv.Atoi(portValue)

		propertiesStruct := Properties{
			IngestorProtocol:                protocolValue,
			IngestorHostname:                hostnameValue,
			IngestorPort:                    portValue2,
			IngestorPath:                    "/stats/app",
			IngestorSkipSsl:                 true,
			IngestorResponseTimeoutSeconds:  1,
			IngestorExporterMetricsEndpoint: "/metrics",
		}

		exporter := NewExporter(propertiesStruct)
		prometheus.MustRegister(exporter)

		tagsMap := exporterPropertiesToTagsMap(propertiesStruct)
		tagsStr := prometheusTagsToString(tagsMap)

		responseCall("ingestor_instance_id" + tagsStr + " 15")
		responseCall("ingestor_instance_id" + tagsStr + " 0")
	})

	It("ingestor exporter returns 200 and says up metric is 0 when the ingestor is down", func() {
		propertiesStruct := Properties{
			IngestorProtocol:                "https",
			IngestorHostname:                "idontexistatallasdf.com",
			IngestorPort:                    8080,
			IngestorPath:                    "/stats/app",
			IngestorSkipSsl:                 true,
			IngestorResponseTimeoutSeconds:  1,
			IngestorExporterMetricsEndpoint: "/metrics",
		}

		tagsMap := exporterPropertiesToTagsMap(propertiesStruct)
		tagsStr := prometheusTagsToString(tagsMap)

		req := httptest.NewRequest("GET", "http://myingestorexporter.com/metrics", nil) //URL of this request does not matter

		recorder := httptest.NewRecorder()
		exporter := NewExporter(propertiesStruct)
		prometheus.MustRegister(exporter)
		promhttp.Handler().ServeHTTP(recorder, req)

		resp := recorder.Result()
		body, err := ioutil.ReadAll(resp.Body)
		Expect(err).To(BeNil())
		Expect(resp.StatusCode).To(Equal(http.StatusOK))
		Expect(resp.Header.Get("Content-Type")).To(Equal("text/plain; version=0.0.4; charset=utf-8"))

		Expect(string(body)).To(ContainSubstring("ingestor_up" + tagsStr + " 0"))
	})

	//we care if the response code from the ingestor is not 200 (>=400)
	It("ingestor exporter returns 200 but healthy metric is 0 when we get a bad response code from ingestor", func() {
		ingestorResponse := sampleIngestorResponse
		fakeIngestorServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			marshallIngestor, err := json.Marshal(ingestorResponse)
			Expect(err).To(BeNil())
			_, err = fmt.Fprintln(w, string(marshallIngestor))
			Expect(err).To(BeNil())
		}))
		defer fakeIngestorServer.Close()

		s := strings.Split(fakeIngestorServer.URL, "://")
		protocolValue, portWithHostnameValue := s[0], s[1]

		s2 := strings.Split(portWithHostnameValue, ":")
		hostnameValue, portValue := s2[0], s2[1]

		portValue2, _ := strconv.Atoi(portValue)

		propertiesStruct := Properties{
			IngestorProtocol:                protocolValue,
			IngestorHostname:                hostnameValue,
			IngestorPort:                    portValue2,
			IngestorPath:                    "/stats/app",
			IngestorSkipSsl:                 true,
			IngestorResponseTimeoutSeconds:  1,
			IngestorExporterMetricsEndpoint: "/metrics",
		}

		tagsMap := exporterPropertiesToTagsMap(propertiesStruct)
		tagsStr := prometheusTagsToString(tagsMap)

		req := httptest.NewRequest("GET", "http://myingestorexporter.com/metrics", nil) //URL of this request does not matter

		recorder := httptest.NewRecorder()
		exporter := NewExporter(propertiesStruct)
		prometheus.MustRegister(exporter)
		promhttp.Handler().ServeHTTP(recorder, req)

		resp := recorder.Result()
		body, err := ioutil.ReadAll(resp.Body)
		Expect(err).To(BeNil())
		Expect(string(body)).To(ContainSubstring("ingestor_healthy" + tagsStr + " 0"))
	})

	It("returns 200 and omits metrics which are missing from the ingestor", func() {
		ingestorResponse := ingestorResponse{
			Consume: Float64Pointer(0),
		}

		fakeIngestorServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			marshallIngestor, err := json.Marshal(ingestorResponse)
			Expect(err).To(BeNil())
			_, err = fmt.Fprintln(w, string(marshallIngestor))
			Expect(err).To(BeNil())
		}))
		defer fakeIngestorServer.Close()

		s := strings.Split(fakeIngestorServer.URL, "://")
		protocolValue, portWithHostnameValue := s[0], s[1]

		s2 := strings.Split(portWithHostnameValue, ":")
		hostnameValue, portValue := s2[0], s2[1]

		portValue2, _ := strconv.Atoi(portValue)

		propertiesStruct := Properties{
			IngestorProtocol:                protocolValue,
			IngestorHostname:                hostnameValue,
			IngestorPort:                    portValue2,
			IngestorPath:                    "/stats/app",
			IngestorSkipSsl:                 true,
			IngestorResponseTimeoutSeconds:  1,
			IngestorExporterMetricsEndpoint: "/metrics",
		}

		req := httptest.NewRequest("GET", "http://myingestorexporter.com/metrics", nil) //URL of this request does not matter
		recorder := httptest.NewRecorder()
		exporter := NewExporter(propertiesStruct)
		prometheus.MustRegister(exporter)
		promhttp.Handler().ServeHTTP(recorder, req)
		resp := recorder.Result()
		body, err := ioutil.ReadAll(resp.Body)
		Expect(err).To(BeNil())
		Expect(resp.StatusCode).To(Equal(http.StatusOK))
		Expect(resp.Header.Get("Content-Type")).To(Equal("text/plain; version=0.0.4; charset=utf-8"))

		tagsMap := exporterPropertiesToTagsMap(propertiesStruct)
		tagsStr := prometheusTagsToString(tagsMap)

		Expect(string(body)).To(ContainSubstring("ingestor_consume_total" + tagsStr + " 0"))
		Expect(string(body)).To(Not(ContainSubstring("ingestor_consume_fail_total{")))
		Expect(string(body)).To(Not(ContainSubstring("ingestor_consume_http_start_stop_total{")))
		Expect(string(body)).To(Not(ContainSubstring("ingestor_consume_value_metric_total{")))
		Expect(string(body)).To(Not(ContainSubstring("ingestor_consume_counter_event_total{")))
		Expect(string(body)).To(Not(ContainSubstring("ingestor_consume_log_message_total{")))
		Expect(string(body)).To(Not(ContainSubstring("ingestor_consume_error_total{")))
		Expect(string(body)).To(Not(ContainSubstring("ingestor_consume_container_metric_total{")))
		Expect(string(body)).To(Not(ContainSubstring("ingestor_consume_unknown_total{")))
		Expect(string(body)).To(Not(ContainSubstring("ingestor_ignored_total{")))
		Expect(string(body)).To(Not(ContainSubstring("ingestor_forwarded_total{")))
		Expect(string(body)).To(Not(ContainSubstring("ingestor_publish_total{")))
		Expect(string(body)).To(Not(ContainSubstring("ingestor_publish_fail_total{")))
		Expect(string(body)).To(Not(ContainSubstring("ingestor_slow_consumer_alert_total{")))
		Expect(string(body)).To(Not(ContainSubstring("ingestor_subinupt_buffer{")))
		Expect(string(body)).To(Not(ContainSubstring("ingestor_instance_id{")))
	})

	It("returns 200 when multiple metric calls are made and at least one ingestor metric is missing ", func() {
		ingestorResponse0 := sampleIngestorResponse
		ingestorResponse1 := ingestorResponse{
			Consume: Float64Pointer(0),
		}

		numTimesFakeIngestorServerCalled := 0
		fakeIngestorServerResponses := []ingestorResponse{
			ingestorResponse0,
			ingestorResponse1,
		}

		fakeIngestorServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fakeResponse := fakeIngestorServerResponses[numTimesFakeIngestorServerCalled]
			w.WriteHeader(http.StatusOK)
			marshallIngestor, err := json.Marshal(fakeResponse)
			Expect(err).To(BeNil())
			_, err = fmt.Fprintln(w, string(marshallIngestor))
			Expect(err).To(BeNil())
			numTimesFakeIngestorServerCalled++
		}))
		defer fakeIngestorServer.Close()

		u, err := url.Parse(fakeIngestorServer.URL)
		Expect(err).To(BeNil())

		scheme := u.Scheme
		host := u.Host

		s1 := strings.Split(host, ":")
		hostnameString, portString := s1[0], s1[1]

		portStringConv, _ := strconv.Atoi(portString)

		propertiesStruct := Properties{
			IngestorProtocol:                scheme,
			IngestorHostname:                hostnameString,
			IngestorPort:                    portStringConv,
			IngestorPath:                    "/stats/app",
			IngestorSkipSsl:                 true,
			IngestorResponseTimeoutSeconds:  1,
			IngestorExporterMetricsEndpoint: "/metrics",
		}
		exporter := NewExporter(propertiesStruct)
		prometheus.MustRegister(exporter)

		tagsMap := exporterPropertiesToTagsMap(propertiesStruct)
		tagsStr := prometheusTagsToString(tagsMap)

		responseCall("ingestor_instance_id" + tagsStr + " 15")
		NotExpectedReponse("ingestor_instance_id{")

	})
})

var _ = Describe("properties", func() {
	AfterEach(func() {
		os.Clearenv()
	})

	It("gets default values", func() {
		props := NewProperties()
		Expect(props.IngestorHostname).To(Equal("127.0.0.1"))
		Expect(props.IngestorPort).To(Equal(8080))
		Expect(props.IngestorPath).To(Equal("/stats/app"))
		Expect(props.IngestorSkipSsl).To(Equal(false))
		Expect(props.IngestorResponseTimeoutSeconds).To(Equal(10))
		Expect(props.IngestorExporterMetricsEndpoint).To(Equal("/metrics"))
		Expect(props.IngestorExporterPort).To(Equal(9184))
	})

	It("gets values from environment variables", func() {
		Expect(os.Setenv("INGESTOR_PROTOCOL", "http")).To(Succeed())
		Expect(os.Setenv("INGESTOR_HOSTNAME", "myingestor.com")).To(Succeed())
		Expect(os.Setenv("INGESTOR_PORT", "1234")).To(Succeed())
		Expect(os.Setenv("INGESTOR_PATH", "/my/ingestor/metrics/endpoint")).To(Succeed())
		Expect(os.Setenv("INGESTOR_SKIP_SSL", "true")).To(Succeed())
		Expect(os.Setenv("INGESTOR_RESPONSE_TIMEOUT_SECONDS", "60")).To(Succeed())
		Expect(os.Setenv("INGESTOR_EXPORTER_METRICS_ENDPOINT", "/my/ingestor/exporter/metrics/endpoint")).To(Succeed())
		Expect(os.Setenv("INGESTOR_EXPORTER_PORT", "5678")).To(Succeed())
		Expect(os.Setenv("INGESTOR_METRICS_ENVIRONMENT", "dev")).To(Succeed())

		props := NewProperties()

		Expect(props.IngestorProtocol).To(Equal("http"))
		Expect(props.IngestorHostname).To(Equal("myingestor.com"))
		Expect(props.IngestorPort).To(Equal(1234))
		Expect(props.IngestorPath).To(Equal("/my/ingestor/metrics/endpoint"))
		Expect(props.IngestorSkipSsl).To(Equal(true))
		Expect(props.IngestorResponseTimeoutSeconds).To(Equal(60))
		Expect(props.IngestorExporterMetricsEndpoint).To(Equal("/my/ingestor/exporter/metrics/endpoint"))
		Expect(props.IngestorExporterPort).To(Equal(5678))
		Expect(props.IngestorMetricsEnvironment).To(Equal("dev"))
	})
})
