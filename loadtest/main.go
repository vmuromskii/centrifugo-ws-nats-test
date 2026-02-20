package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/centrifugal/centrifuge-go"
	"github.com/golang-jwt/jwt/v5"
)

var (
	connected    int64
	disconnected int64
	connectErrs  int64
	msgSent      int64
	msgRecv      int64
	echoRecv     int64
	sendErrors   int64
)

func main() {
	wsURL := env("CENTRIFUGO_WS_URL", "ws://localhost:8000/connection/websocket")
	tokenSecret := env("CENTRIFUGO_TOKEN_SECRET", "token-secret-key")
	numClients, _ := strconv.Atoi(env("NUM_CLIENTS", "100"))
	msgInterval, _ := time.ParseDuration(env("MESSAGE_INTERVAL", "3s"))
	msgSize, _ := strconv.Atoi(env("MESSAGE_SIZE", "300"))
	rampUp, _ := time.ParseDuration(env("RAMP_UP_DELAY", "10ms"))
	offset, _ := strconv.Atoi(env("USER_ID_OFFSET", "0"))
	usersPerRoom, _ := strconv.Atoi(env("USERS_PER_ROOM", "10"))

	numRooms := numClients / usersPerRoom
	if numRooms < 1 {
		numRooms = 1
	}

	log.Printf("=== Centrifugo Load Test (Rooms) ===")
	log.Printf("URL:            %s", wsURL)
	log.Printf("Clients:        %d (offset %d)", numClients, offset)
	log.Printf("Users per room: %d", usersPerRoom)
	log.Printf("Rooms:          %d", numRooms)
	log.Printf("Interval:       %s  Size: %d bytes", msgInterval, msgSize)
	log.Printf("Ramp-up:        %s", rampUp)
	log.Printf("====================================")

	time.Sleep(5 * time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	var wg sync.WaitGroup
	clients := make([]*centrifuge.Client, 0, numClients)
	var mu sync.Mutex

	go printStats(ctx)

	payload := genPayload(msgSize)

	for i := 0; i < numClients; i++ {
		userID := fmt.Sprintf("user_%d", offset+i)
		roomID := fmt.Sprintf("room.%d", (offset+i)/usersPerRoom)
		token := generateJWT(userID, tokenSecret)

		wg.Add(1)
		go func(userID, roomID, token string) {
			defer wg.Done()

			client := centrifuge.NewJsonClient(wsURL, centrifuge.Config{
				Token: token,
			})

			client.OnConnected(func(e centrifuge.ConnectedEvent) {
				atomic.AddInt64(&connected, 1)
			})

			client.OnDisconnected(func(e centrifuge.DisconnectedEvent) {
				atomic.AddInt64(&connected, -1)
				atomic.AddInt64(&disconnected, 1)
			})

			client.OnError(func(e centrifuge.ErrorEvent) {
				atomic.AddInt64(&sendErrors, 1)
			})

			sub, err := client.NewSubscription(roomID, centrifuge.SubscriptionConfig{})
			if err != nil {
				log.Printf("[%s] subscription error: %v", userID, err)
				atomic.AddInt64(&connectErrs, 1)
				return
			}

			sub.OnPublication(func(e centrifuge.PublicationEvent) {
				atomic.AddInt64(&msgRecv, 1)
				var msg map[string]interface{}
				if json.Unmarshal(e.Data, &msg) == nil {
					if t, _ := msg["type"].(string); t == "echo" {
						atomic.AddInt64(&echoRecv, 1)
					}
				}
			})

			sub.OnError(func(e centrifuge.SubscriptionErrorEvent) {
				atomic.AddInt64(&sendErrors, 1)
			})

			err = sub.Subscribe()
			if err != nil {
				log.Printf("[%s] subscribe error: %v", userID, err)
				atomic.AddInt64(&connectErrs, 1)
				return
			}

			err = client.Connect()
			if err != nil {
				log.Printf("[%s] connect error: %v", userID, err)
				atomic.AddInt64(&connectErrs, 1)
				return
			}

			mu.Lock()
			clients = append(clients, client)
			mu.Unlock()

			// Writer
			jitter := time.Duration(rand.Intn(int(msgInterval.Milliseconds()))) * time.Millisecond
			time.Sleep(2*time.Second + jitter)

			ticker := time.NewTicker(msgInterval)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					msg := fmt.Sprintf(`{"user":"%s","room":"%s","data":"%s","ts":%d}`,
						userID, roomID, payload, time.Now().UnixMilli())

					_, err := sub.Publish(ctx, []byte(msg))
					if err != nil {
						atomic.AddInt64(&sendErrors, 1)
						continue
					}
					atomic.AddInt64(&msgSent, 1)
				}
			}
		}(userID, roomID, token)

		time.Sleep(rampUp)

		if (i+1)%500 == 0 {
			log.Printf("started %d/%d  connected=%d  errs=%d",
				i+1, numClients,
				atomic.LoadInt64(&connected),
				atomic.LoadInt64(&connectErrs))
		}
	}

	log.Printf("all %d started, connected=%d", numClients, atomic.LoadInt64(&connected))

	<-sigCh
	log.Println("shutting down...")
	cancel()

	mu.Lock()
	for _, c := range clients {
		c.Close()
	}
	mu.Unlock()

	wg.Wait()

	log.Printf("========== FINAL ==========")
	log.Printf("Connected (last):  %d", atomic.LoadInt64(&connected))
	log.Printf("Disconnected:      %d", atomic.LoadInt64(&disconnected))
	log.Printf("Connect errors:    %d", atomic.LoadInt64(&connectErrs))
	log.Printf("Messages sent:     %d", atomic.LoadInt64(&msgSent))
	log.Printf("Messages received: %d", atomic.LoadInt64(&msgRecv))
	log.Printf("Echo received:     %d", atomic.LoadInt64(&echoRecv))
	log.Printf("Send errors:       %d", atomic.LoadInt64(&sendErrors))
	log.Printf("============================")
}

func printStats(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	var lastSent, lastRecv, lastEcho int64

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sent := atomic.LoadInt64(&msgSent)
			recv := atomic.LoadInt64(&msgRecv)
			echo := atomic.LoadInt64(&echoRecv)
			log.Printf("[STATS] conn=%d disc=%d connErr=%d | sent=%d(+%d/5s) recv=%d(+%d/5s) echo=%d(+%d/5s) | sendErr=%d",
				atomic.LoadInt64(&connected),
				atomic.LoadInt64(&disconnected),
				atomic.LoadInt64(&connectErrs),
				sent, sent-lastSent,
				recv, recv-lastRecv,
				echo, echo-lastEcho,
				atomic.LoadInt64(&sendErrors),
			)
			lastSent = sent
			lastRecv = recv
			lastEcho = echo
		}
	}
}

func generateJWT(userID, secret string) string {
	claims := jwt.MapClaims{
		"sub": userID,
		"exp": time.Now().Add(24 * time.Hour).Unix(),
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, err := token.SignedString([]byte(secret))
	if err != nil {
		log.Fatalf("Failed to generate token: %v", err)
	}
	return tokenString
}

func genPayload(size int) string {
	n := size - 60
	if n < 10 {
		n = 10
	}
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = chars[rand.Intn(len(chars))]
	}
	return string(b)
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}