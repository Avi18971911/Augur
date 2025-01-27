package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"golang.org/x/sync/errgroup"
	"io"
	"net/http"
	"time"
)

type APIConfig struct {
	URL         string                 // API Endpoint
	Method      string                 // HTTP Method (GET, POST, etc.)
	Headers     map[string]string      // Custom headers
	Body        map[string]interface{} // Request body (for POST)
	Users       int                    // Number of virtual users
	DurationMin int                    // Test duration in minutes
}

// worker sends HTTP requests and records response times
func worker(ctx context.Context, apiConfig APIConfig, results chan<- time.Duration) error {
	client := &http.Client{}
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			var reqBody io.Reader
			if apiConfig.Body != nil {
				jsonBody, err := json.Marshal(apiConfig.Body)
				if err != nil {
					return err
				}
				reqBody = bytes.NewBuffer(jsonBody)
			}

			req, err := http.NewRequest(apiConfig.Method, apiConfig.URL, reqBody)
			if err != nil {
				return err
			}

			for key, value := range apiConfig.Headers {
				req.Header.Set(key, value)
			}

			start := time.Now()
			resp, _ := client.Do(req)
			duration := time.Since(start)

			results <- duration
			if resp != nil {
				resp.Body.Close()
			}
		}
	}
}

func runLoadTest(apiConfig APIConfig) {
	results := make(chan time.Duration, 1000)

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(apiConfig.DurationMin)*time.Minute)
	defer cancel()

	var g errgroup.Group

	fmt.Printf("Starting load test: %d users, %d min",
		apiConfig.Users, apiConfig.DurationMin)

	for i := 0; i < apiConfig.Users; i++ {
		g.Go(func() error {
			return worker(ctx, apiConfig, results)
		})
	}

	go func() {
		g.Wait()
		close(results)
	}()

	// Process results
	var totalRequests int
	var totalTime time.Duration

	for r := range results {
		totalRequests++
		totalTime += r
	}

	avgTime := time.Duration(0)
	if totalRequests > 0 {
		avgTime = totalTime / time.Duration(totalRequests)
	}

	fmt.Printf("\nLoad Test Results:\n")
	fmt.Printf("Total Requests: %d\n", totalRequests)
	fmt.Printf("Average Response Time: %s\n", avgTime)

	if err := g.Wait(); err != nil {
		fmt.Println("Load test encountered errors:", err)
	}

	fmt.Println("Load test completed.")
}

func main() {
	apiConfig := APIConfig{
		URL:         "http://localhost:8080/accounts/login",
		Method:      "POST",
		Headers:     map[string]string{"Content-Type": "application/json"},
		Body:        map[string]interface{}{"username": "Bob", "password": "Barker"},
		Users:       5, // Number of concurrent virtual users
		DurationMin: 1,
	}

	runLoadTest(apiConfig)
}
