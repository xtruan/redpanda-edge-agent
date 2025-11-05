package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

type StatusServer struct {
	server *http.Server
	mu     sync.RWMutex
	stats  AgentStats
}

type AgentStats struct {
	StartTime          time.Time              `json:"start_time"`
	Uptime             string                 `json:"uptime"`
	SourceName         string                 `json:"source_name"`
	DestinationName    string                 `json:"destination_name"`
	IsMqttSource       bool                   `json:"is_mqtt_source"`
	RecordsConsumed    int64                  `json:"records_consumed"`
	RecordsProduced    int64                  `json:"records_produced"`
	RecordsFailed      int64                  `json:"records_failed"`
	LastRecordTime     *time.Time             `json:"last_record_time,omitempty"`
	ConsumerGroupID    string                 `json:"consumer_group_id,omitempty"`
	Topics             []string               `json:"topics"`
	ErrorCount         int                    `json:"error_count"`
	LastErrorTime      *time.Time             `json:"last_error_time,omitempty"`
	LastError          string                 `json:"last_error,omitempty"`
	Status             string                 `json:"status"`
}

type HealthResponse struct {
	Status    string     `json:"status"`
	Timestamp time.Time  `json:"timestamp"`
	Uptime    string     `json:"uptime"`
}

var (
	statusServer *StatusServer
	startTime    time.Time
)

func initStatusServer(addr string) *StatusServer {
	startTime = time.Now()
	
	ss := &StatusServer{
		stats: AgentStats{
			StartTime: startTime,
			Status:    "starting",
		},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", ss.handleHealth)
	mux.HandleFunc("/status", ss.handleStatus)
	mux.HandleFunc("/metrics", ss.handleMetrics)

	ss.server = &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	return ss
}

func (ss *StatusServer) Start() error {
	log.Infof("Starting HTTP status server on %s", ss.server.Addr)
	return ss.server.ListenAndServe()
}

func (ss *StatusServer) Shutdown() error {
	log.Info("Shutting down HTTP status server")
	return ss.server.Close()
}

func (ss *StatusServer) UpdateStats(update func(*AgentStats)) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	update(&ss.stats)
}

func (ss *StatusServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	ss.mu.RLock()
	defer ss.mu.RUnlock()

	health := HealthResponse{
		Status:    ss.stats.Status,
		Timestamp: time.Now(),
		Uptime:    time.Since(startTime).Round(time.Second).String(),
	}

	w.Header().Set("Content-Type", "application/json")
	
	// Return 503 if status is not "running"
	if ss.stats.Status != "running" {
		w.WriteHeader(http.StatusServiceUnavailable)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	
	json.NewEncoder(w).Encode(health)
}

func (ss *StatusServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	ss.mu.RLock()
	defer ss.mu.RUnlock()

	stats := ss.stats
	stats.Uptime = time.Since(startTime).Round(time.Second).String()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(stats)
}

func (ss *StatusServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	ss.mu.RLock()
	defer ss.mu.RUnlock()

	// Prometheus-style text format
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)

	uptime := time.Since(startTime).Seconds()
	
	fmt.Fprintf(w, "# HELP agent_uptime_seconds Time since agent started\n")
	fmt.Fprintf(w, "# TYPE agent_uptime_seconds gauge\n")
	fmt.Fprintf(w, "agent_uptime_seconds %.2f\n", uptime)
	
	fmt.Fprintf(w, "# HELP agent_records_consumed_total Total records consumed\n")
	fmt.Fprintf(w, "# TYPE agent_records_consumed_total counter\n")
	fmt.Fprintf(w, "agent_records_consumed_total %d\n", ss.stats.RecordsConsumed)
	
	fmt.Fprintf(w, "# HELP agent_records_produced_total Total records produced\n")
	fmt.Fprintf(w, "# TYPE agent_records_produced_total counter\n")
	fmt.Fprintf(w, "agent_records_produced_total %d\n", ss.stats.RecordsProduced)
	
	fmt.Fprintf(w, "# HELP agent_records_failed_total Total records failed\n")
	fmt.Fprintf(w, "# TYPE agent_records_failed_total counter\n")
	fmt.Fprintf(w, "agent_records_failed_total %d\n", ss.stats.RecordsFailed)
	
	fmt.Fprintf(w, "# HELP agent_error_count Current error count\n")
	fmt.Fprintf(w, "# TYPE agent_error_count gauge\n")
	fmt.Fprintf(w, "agent_error_count %d\n", ss.stats.ErrorCount)
	
	statusValue := 0
	if ss.stats.Status == "running" {
		statusValue = 1
	}
	fmt.Fprintf(w, "# HELP agent_status Agent status (1=running, 0=not running)\n")
	fmt.Fprintf(w, "# TYPE agent_status gauge\n")
	fmt.Fprintf(w, "agent_status %d\n", statusValue)
}

// Helper functions to update stats from main code
func incrementRecordsConsumed(count int64) {
	if statusServer != nil {
		statusServer.UpdateStats(func(s *AgentStats) {
			s.RecordsConsumed += count
			now := time.Now()
			s.LastRecordTime = &now
		})
	}
}

func incrementRecordsProduced(count int64) {
	if statusServer != nil {
		statusServer.UpdateStats(func(s *AgentStats) {
			s.RecordsProduced += count
		})
	}
}

func incrementRecordsFailed(count int64) {
	if statusServer != nil {
		statusServer.UpdateStats(func(s *AgentStats) {
			s.RecordsFailed += count
		})
	}
}

func updateErrorCount(count int, errMsg string) {
	if statusServer != nil {
		statusServer.UpdateStats(func(s *AgentStats) {
			s.ErrorCount = count
			if errMsg != "" {
				s.LastError = errMsg
				now := time.Now()
				s.LastErrorTime = &now
			}
		})
	}
}

func setStatus(status string) {
	if statusServer != nil {
		statusServer.UpdateStats(func(s *AgentStats) {
			s.Status = status
		})
	}
}

func setAgentInfo(sourceName, destName string, isMqtt bool, topics []string, consumerGroup string) {
	if statusServer != nil {
		statusServer.UpdateStats(func(s *AgentStats) {
			s.SourceName = sourceName
			s.DestinationName = destName
			s.IsMqttSource = isMqtt
			s.Topics = topics
			s.ConsumerGroupID = consumerGroup
		})
	}
}
