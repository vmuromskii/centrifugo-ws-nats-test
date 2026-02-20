package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	natsConn *nats.Conn

	messagesReceived = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "backend_messages_received_total",
	})
	messagesEchoed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "backend_messages_echoed_total",
	})
	echoLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "backend_echo_latency_seconds",
		Buckets: prometheus.DefBuckets,
	})

	totalReceived int64
)

func init() {
	prometheus.MustRegister(messagesReceived, messagesEchoed, echoLatency)
}

func main() {
	natsURL := getEnv("NATS_URL", "nats://localhost:4222")

	var err error
	for i := 0; i < 30; i++ {
		natsConn, err = nats.Connect(natsURL,
			nats.MaxReconnects(-1),
			nats.ReconnectWait(time.Second),
		)
		if err == nil {
			break
		}
		log.Printf("Waiting for NATS... attempt %d: %v", i+1, err)
		time.Sleep(time.Second)
	}
	if err != nil {
		log.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer natsConn.Close()
	log.Println("Connected to NATS")

	// Слушаем сообщения от Centrifugo (raw mode)
	// Клиенты публикуют в каналы room.N → NATS subject centrifugo.room.N
	natsConn.Subscribe("centrifugo.room.>", func(msg *nats.Msg) {
		start := time.Now()
		messagesReceived.Inc()
		atomic.AddInt64(&totalReceived, 1)

		parts := strings.Split(msg.Subject, ".")
		if len(parts) < 3 {
			return
		}
		roomID := parts[2]

		var incoming map[string]interface{}
		if err := json.Unmarshal(msg.Data, &incoming); err != nil {
			return
		}

		echo := map[string]interface{}{
			"type":      "echo",
			"room":      roomID,
			"original":  incoming,
			"echoed_at": time.Now().UnixMilli(),
			"server":    "backend-1",
		}

		data, err := json.Marshal(echo)
		if err != nil {
			return
		}

		// Отвечаем в тот же канал — все подписчики получат
		replySubject := fmt.Sprintf("centrifugo.room.%s", roomID)
		if err := natsConn.Publish(replySubject, data); err != nil {
			log.Printf("publish error: %v", err)
			return
		}
		messagesEchoed.Inc()
		echoLatency.Observe(time.Since(start).Seconds())
	})

	log.Println("Subscribed to centrifugo.room.>")

	go func() {
		for {
			time.Sleep(5 * time.Second)
			log.Printf("[backend] total received=%d", atomic.LoadInt64(&totalReceived))
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/centrifugo/connect", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"result": map[string]interface{}{}})
	})
	mux.HandleFunc("/centrifugo/subscribe", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"result": map[string]interface{}{}})
	})
	mux.HandleFunc("/centrifugo/publish", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"result": map[string]interface{}{}})
	})
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte("OK"))
	})

	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())
	go func() {
		log.Println("Metrics on :8081")
		log.Fatal(http.ListenAndServe(":8081", metricsMux))
	}()

	log.Println("Backend on :8080")
	log.Fatal(http.ListenAndServe(":8080", mux))
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}