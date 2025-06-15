package integration

import (
	"encoding/json"
	"fmt"
	. "gopkg.in/check.v1"
	"net/http"
	"os"
	"testing"
	"time"
)

func Test(t *testing.T) { TestingT(t) }

type IntegrationSuite struct{}

var _ = Suite(&IntegrationSuite{})

const baseAddress = "http://balancer:8090"
const team = "MysteriousGophers"

type Response struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

var client = http.Client{
	Timeout: 3 * time.Second,
}

func (s *IntegrationSuite) TestBalancer(c *C) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		c.Skip("Integration test is not enabled")
	}

	server1, err := client.Get(fmt.Sprintf("%s/some", baseAddress))
	if err != nil {
		c.Error(err)
	}

	server1Header := server1.Header.Get("lb-from")
	c.Check(server1Header, Equals, "server1:8080")

	server2, err := client.Get(fmt.Sprintf("%s/some1", baseAddress))
	if err != nil {
		c.Error(err)
	}

	server2Header := server2.Header.Get("lb-from")
	c.Check(server2Header, Equals, "server2:8080")

	server3, err := client.Get(fmt.Sprintf("%s/some2", baseAddress))
	if err != nil {
		c.Error(err)
	}

	server3Header := server3.Header.Get("lb-from")
	c.Check(server3Header, Equals, "server3:8080")

	server1Repeat, err := client.Get(fmt.Sprintf("%s/some1", baseAddress))
	if err != nil {
		c.Error(err)
	}

	server1RepeatHeader := server1Repeat.Header.Get("lb-from")
	c.Check(server1RepeatHeader, Equals, server1Header)

	// check database
	db, err := client.Get(fmt.Sprintf("%s/api/v1/some-data?key=MysteriousGophers", baseAddress))

	if err != nil {
		c.Error(err)
	}

	var body Response
	err = json.NewDecoder(db.Body).Decode(&body)
	if err != nil {
		c.Error(err)
	}

	c.Check(body.Key, Equals, team)
	if body.Value == "" {
		c.Error(err)
	}
}

func (s *IntegrationSuite) BenchmarkBalancer(c *C) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		c.Skip("Integration test is not enabled")
	}

	for i := 0; i < c.N; i++ {
		_, err := client.Get(fmt.Sprintf("%s/api/v1/some-data", baseAddress))
		if err != nil {
			c.Error(err)
		}
	}
}
