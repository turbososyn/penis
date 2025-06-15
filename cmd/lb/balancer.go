// File: cmd/lb/balancer.go
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/mysteriousgophers/architecture-lab-4/httptools"
	"github.com/mysteriousgophers/architecture-lab-4/signal"
)

var (
	port         = flag.Int("port", 8090, "load balancer port")
	timeoutSec   = flag.Int("timeout-sec", 3, "request timeout time in seconds")
	https        = flag.Bool("https", false, "whether backends support HTTPs")
	traceEnabled = flag.Bool("trace", false, "whether to include tracing information into responses")
)

var serversPool = []string{
	"server1:8080",
	"server2:8080",
	"server3:8080",
}

type Balancer struct {
	pool          []string
	healthyPool   []string
	serverTraffic map[string]int64
	lock          sync.RWMutex
	healthChecker HealthChecker
	requestSender RequestSender
	timeout       time.Duration
	useHttps      bool
}

func NewBalancer(pool []string, hc HealthChecker, rs RequestSender, timeout time.Duration, useHttps bool) *Balancer {
	b := &Balancer{
		pool:          pool,
		healthyPool:   make([]string, len(pool)),
		serverTraffic: make(map[string]int64),
		healthChecker: hc,
		requestSender: rs,
		timeout:       timeout,
		useHttps:      useHttps,
	}
	copy(b.healthyPool, b.pool)
	return b
}

type HealthChecker interface {
	Check(dst string, useHttps bool) bool
}

type DefaultHealthChecker struct {
	Timeout time.Duration
}

func (hc *DefaultHealthChecker) scheme(useHttps bool) string {
	if useHttps {
		return "https"
	}
	return "http"
}

func (hc *DefaultHealthChecker) Check(dst string, useHttps bool) bool {
	ctx, cancel := context.WithTimeout(context.Background(), hc.Timeout)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, "GET",
		fmt.Sprintf("%s://%s/health", hc.scheme(useHttps), dst), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

type RequestSender interface {
	Send(*http.Request) (*http.Response, error)
}

type DefaultRequestSender struct{}

func (rs *DefaultRequestSender) Send(fwdRequest *http.Request) (*http.Response, error) {
	return http.DefaultClient.Do(fwdRequest)
}

func (b *Balancer) scheme() string {
	if b.useHttps {
		return "https"
	}
	return "http"
}

func (b *Balancer) forward(dst string, rw http.ResponseWriter, r *http.Request) error {
	ctx, cancel := context.WithTimeout(r.Context(), b.timeout)
	defer cancel()
	fwdRequest := r.Clone(ctx)
	fwdRequest.RequestURI = ""
	fwdRequest.URL.Host = dst
	fwdRequest.URL.Scheme = b.scheme()
	fwdRequest.Host = dst

	resp, err := b.requestSender.Send(fwdRequest)
	if err != nil {
		log.Printf("Failed to get response from %s: %s", dst, err)
		rw.WriteHeader(http.StatusServiceUnavailable)
		return err
	}
	defer resp.Body.Close()

	for k, values := range resp.Header {
		for _, value := range values {
			rw.Header().Add(k, value)
		}
	}

	if *traceEnabled {
		rw.Header().Set("lb-from", dst)
	}

	log.Println("fwd", resp.StatusCode, resp.Request.URL)
	rw.WriteHeader(resp.StatusCode)

	n, err := io.Copy(rw, resp.Body)
	if err != nil {
		log.Printf("Failed to write response: %s", err)
		return err
	}

	b.lock.Lock()
	b.serverTraffic[dst] += n
	b.lock.Unlock()

	return nil
}

func (b *Balancer) chooseServer() string {
	b.lock.RLock()
	defer b.lock.RUnlock()

	if len(b.healthyPool) == 0 {
		return ""
	}

	var minTrafficServer string
	var minTraffic int64 = -1

	for _, server := range b.healthyPool {
		traffic := b.serverTraffic[server]
		if minTraffic == -1 || traffic < minTraffic {
			minTraffic = traffic
			minTrafficServer = server
		}
	}
	return minTrafficServer
}

func (b *Balancer) healthCheck() {
	for {
		time.Sleep(10 * time.Second)
		log.Println("Starting health check...")
		newHealthyPool := make([]string, 0, len(b.pool))
		for _, server := range b.pool {
			isHealthy := b.healthChecker.Check(server, b.useHttps)
			if isHealthy {
				newHealthyPool = append(newHealthyPool, server)
			}
			log.Printf("Server %s is %s", server, map[bool]string{true: "healthy", false: "unhealthy"}[isHealthy])
		}

		b.lock.Lock()
		b.healthyPool = newHealthyPool
		b.lock.Unlock()
		log.Println("Health check finished.")
	}
}

func main() {
	flag.Parse()
	timeout := time.Duration(*timeoutSec) * time.Second

	balancer := NewBalancer(
		serversPool,
		&DefaultHealthChecker{Timeout: timeout},
		&DefaultRequestSender{},
		timeout,
		*https,
	)

	go balancer.healthCheck()

	frontend := httptools.CreateServer(*port, http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		server := balancer.chooseServer()
		if server == "" {
			http.Error(rw, "No healthy servers available", http.StatusServiceUnavailable)
			return
		}
		if err := balancer.forward(server, rw, r); err != nil {
			return
		}
	}))

	log.Println("Starting load balancer...")
	log.Printf("Tracing support enabled: %t", *traceEnabled)
	frontend.Start()
	signal.WaitForTerminationSignal()
}
