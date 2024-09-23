package main

import (
    "encoding/json"
    "fmt"
    "io"
    "math"
    "net/http"
    "os"
    "strings"
)

const (
    baseDir = "telegraf.vsphere_metrics.oob.qa.dell"
    metricsDir = "snmp"
)

type DataPoint struct {
    Target     string      `json:"target"`
    Tags       interface{} `json:"tags"`
    DataPoints [][]float64 `json:"datapoints"`
}

type MetricStatistics struct {
    Count             int     `json:"count"`
    Average           float64 `json:"average"`
    Sum               float64 `json:"sum"`
    Maximum           float64 `json:"maximum"`
    Minimum           float64 `json:"minimum"`
    StandardDeviation float64 `json:"standard_deviation"`
}

type ServerStatistics map[string]MetricStatistics

type OutputFormat []map[string]ServerStatistics

func fetchServerList(graphiteURL string) ([]string, error) {
    url := fmt.Sprintf("%s/metrics/find?query=%s.*&format=json", graphiteURL, baseDir)

    resp, err := http.Get(url)
    if err != nil {
        return nil, fmt.Errorf("failed to fetch server list: %v", err)
    }
    defer resp.Body.Close()

    if resp.StatusCode != http.StatusOK {
        return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
    }

    body, err := io.ReadAll(resp.Body)
    if err != nil {
        return nil, fmt.Errorf("failed to read response body: %v", err)
    }

    var servers []struct {
        Path string `json:"path"`
    }
    err = json.Unmarshal(body, &servers)
    if err != nil {
        return nil, fmt.Errorf("failed to parse JSON: %v", err)
    }

    var serverNames []string
    for _, server := range servers {
        parts := strings.Split(server.Path, ".")
        serverNames = append(serverNames, parts[len(parts)-1])
    }

    return serverNames, nil
}

func fetchMetricsList(graphiteURL, server string) ([]string, error) {
    url := fmt.Sprintf("%s/metrics/find?query=%s.%s.%s.*&format=json", graphiteURL, baseDir, server, metricsDir)

    resp, err := http.Get(url)
    if err != nil {
        return nil, fmt.Errorf("failed to fetch metrics list: %v", err)
    }
    defer resp.Body.Close()

    if resp.StatusCode != http.StatusOK {
        return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
    }

    body, err := io.ReadAll(resp.Body)
    if err != nil {
        return nil, fmt.Errorf("failed to read response body: %v", err)
    }

    var metrics []struct {
        Path string `json:"path"`
    }
    err = json.Unmarshal(body, &metrics)
    if err != nil {
        return nil, fmt.Errorf("failed to parse JSON: %v", err)
    }

    var metricNames []string
    for _, metric := range metrics {
        metricNames = append(metricNames, metric.Path)
    }

    return metricNames, nil
}

func fetchData(graphiteURL, metric string) (string, error) {
    url := fmt.Sprintf("%s/render?target=%s&from=-7d&format=json", graphiteURL, metric)

    resp, err := http.Get(url)
    if err != nil {
        return "", fmt.Errorf("failed to fetch data: %v", err)
    }
    defer resp.Body.Close()

    if resp.StatusCode != http.StatusOK {
        return "", fmt.Errorf("unexpected status code: %d", resp.StatusCode)
    }

    body, err := io.ReadAll(resp.Body)
    if err != nil {
        return "", fmt.Errorf("failed to read response body: %v", err)
    }

    return string(body), nil
}

func calculateStatistics(data string) (MetricStatistics, error) {
    var dataPoints []DataPoint
    err := json.Unmarshal([]byte(data), &dataPoints)
    if err != nil {
        return MetricStatistics{}, fmt.Errorf("failed to parse JSON: %v", err)
    }

    var sum, max, min, sumOfSquares float64
    var count int

    for _, dp := range dataPoints {
        for _, point := range dp.DataPoints {
            value := point[0]
            sum += value
            sumOfSquares += value * value
            if count == 0 || value > max {
                max = value
            }
            if count == 0 || value < min {
                min = value
            }
            count++
        }
    }

    if count == 0 {
        return MetricStatistics{}, fmt.Errorf("no data points found")
    }

    average := sum / float64(count)
    variance := (sumOfSquares / float64(count)) - (average * average)
    stddev := math.Sqrt(variance)

    return MetricStatistics{
        Count:             count,
        Average:           average,
        Sum:               sum,
        Maximum:           max,
        Minimum:           min,
        StandardDeviation: stddev,
    }, nil
}

func main() {
    graphiteURL := os.Getenv("GRAPHITE_URL")
    if graphiteURL == "" {
        fmt.Fprintf(os.Stderr, "Error: GRAPHITE_URL environment variable is not set\n")
        os.Exit(1)
    }

    servers, err := fetchServerList(graphiteURL)
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error: %v\n", err)
        os.Exit(1)
    }

    var output OutputFormat

    for _, server := range servers {
        metrics, err := fetchMetricsList(graphiteURL, server)
        if err != nil {
            fmt.Fprintf(os.Stderr, "Error: %v\n", err)
            continue
        }

        serverStats := ServerStatistics{}

        for _, metric := range metrics {
            data, err := fetchData(graphiteURL, metric)
            if err != nil {
                fmt.Fprintf(os.Stderr, "Error: %v\n", err)
                continue
            }

            stats, err := calculateStatistics(data)
            if err != nil {
                fmt.Fprintf(os.Stderr, "Error: %v\n", err)
                continue
            }

            parts := strings.Split(metric, ".")
            metricName := parts[len(parts)-1]

            serverStats[metricName] = stats
        }

        output = append(output, map[string]ServerStatistics{server: serverStats})
    }

    jsonOutput, err := json.MarshalIndent(output, "", "  ")
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error: %v\n", err)
        os.Exit(1)
    }

    fmt.Println(string(jsonOutput))
}
