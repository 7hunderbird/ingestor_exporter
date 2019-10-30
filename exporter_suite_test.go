package main

import (
. "github.com/onsi/gomega"
. "github.com/onsi/ginkgo"
"testing"
)

func TestTests(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "The ingestor exporter Suite")
}
