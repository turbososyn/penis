package main

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
)

type MockHealthChecker struct {
	healthStatus map[string]bool
}

func (m *MockHealthChecker) Check(server string, useHttps bool) bool {
	status, exists := m.healthStatus[server]
	if !exists {
		return true
	}
	return status
}

type MockRequestSender struct {
	Response *http.Response
	Err      error
}

func (m *MockRequestSender) Send(req *http.Request) (*http.Response, error) {
	if m.Err != nil {
		return nil, m.Err
	}
	m.Response.Request = req
	return m.Response, nil
}

func TestBalancer_ChooseServer_And_HealthCheck(t *testing.T) {
	servers := []string{"server1", "server2", "server3"}

	mockChecker := &MockHealthChecker{
		healthStatus: map[string]bool{
			"server1": true,
			"server2": false,
			"server3": true,
		},
	}

	balancer := NewBalancer(servers, mockChecker, &MockRequestSender{}, 1*time.Second, false)

	newHealthyPool := make([]string, 0)
	for _, server := range balancer.pool {
		if balancer.healthChecker.Check(server, false) {
			newHealthyPool = append(newHealthyPool, server)
		}
	}
	balancer.healthyPool = newHealthyPool

	expectedHealthy := []string{"server1", "server3"}
	if !reflect.DeepEqual(balancer.healthyPool, expectedHealthy) {
		t.Fatalf("Health check logic failed. Expected healthy pool %v, got %v", expectedHealthy, balancer.healthyPool)
	}

	balancer.serverTraffic = map[string]int64{
		"server1": 100,
		"server2": 10,
		"server3": 200,
	}

	chosen := balancer.chooseServer()
	expectedServer := "server1"
	if chosen != expectedServer {
		t.Errorf("Expected to choose server %q, but got %q", expectedServer, chosen)
	}
}

func TestBalancer_Forward(t *testing.T) {
	destinationServer := "backend:8080"

	t.Run("Successful forward", func(t *testing.T) {
		responseBody := "OK"
		mockSender := &MockRequestSender{
			Response: &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(responseBody)),
			},
		}
		balancer := NewBalancer([]string{destinationServer}, &MockHealthChecker{}, mockSender, 1*time.Second, false)
		req := httptest.NewRequest("GET", "/", nil)
		rr := httptest.NewRecorder()

		err := balancer.forward(destinationServer, rr, req)

		if err != nil {
			t.Fatalf("forward() returned an unexpected error: %v", err)
		}
		if rr.Code != http.StatusOK {
			t.Errorf("Expected status OK, got %d", rr.Code)
		}
	})

	t.Run("Backend error", func(t *testing.T) {
		mockSender := &MockRequestSender{Err: errors.New("connection failed")}
		balancer := NewBalancer([]string{destinationServer}, &MockHealthChecker{}, mockSender, 1*time.Second, false)
		req := httptest.NewRequest("GET", "/", nil)
		rr := httptest.NewRecorder()

		err := balancer.forward(destinationServer, rr, req)

		if err == nil {
			t.Fatal("forward() was expected to return an error, but didn't")
		}
		if rr.Code != http.StatusServiceUnavailable {
			t.Errorf("Expected status Service Unavailable, got %d", rr.Code)
		}
	})
}
