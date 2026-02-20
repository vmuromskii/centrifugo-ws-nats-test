package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
)

var (
	connected    int64
	disconnected int64
	connectErrs  int64
	msgSent      int64
	msgRecv      int64
	sendErrors   int64
)

func main() {
	wsURL := env("WS_URL", "ws://localhost:8010/connection/websocket")
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

	log.Printf("=== Raw WS Load Test (Rooms) ===")
	log.Printf("URL:            %s", wsURL)
	log.Printf("Clients:        %d (offset %d)", numClients, offset)
	log.Printf("Users per room: %d", usersPerRoom)
	log.Printf("Rooms:          %d", numRooms)
	log.Printf("Interval:       %s  Size: %d bytes", msgInterval, msgSize)
	log.Printf("Ramp-up:        %s", rampUp)
	log.Printf("================================")

	time.Sleep(3 * time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	var wg sync.WaitGroup
	var connsMu sync.Mutex
	conns := make([]net.Conn, 0, numClients)

	go printStats(ctx)

	payload := genPayload(msgSize)

	for i := 0; i < numClients; i++ {
		userID := fmt.Sprintf("user_%d", offset+i)
		roomID := (offset + i) / usersPerRoom

		wg.Add(1)
		go func(userID string, roomID int) {
			defer wg.Done()

			url := fmt.Sprintf("%s?user=%s&room=%d", wsURL, userID, roomID)

			conn, _, _, err := ws.Dial(ctx, url)
			if err != nil {
				log.Printf("[%s] connect error: %v", userID, err)
				atomic.AddInt64(&connectErrs, 1)
				return
			}
			atomic.AddInt64(&connected, 1)

			connsMu.Lock()
			conns = append(conns, conn)
			connsMu.Unlock()

			// Reader
			go func() {
				for {
					_, _, err := wsutil.ReadServerData(conn)
					if err != nil {
						atomic.AddInt64(&connected, -1)
						atomic.AddInt64(&disconnected, 1)
						return
					}
					atomic.AddInt64(&msgRecv, 1)
				}
			}()

			// Writer
			jitter := time.Duration(rand.Intn(int(msgInterval.Milliseconds()))) * time.Millisecond
			time.Sleep(jitter)

			ticker := time.NewTicker(msgInterval)
			defer ticker.Stop()

			msg := []byte(fmt.Sprintf(`{"user":"%s","room":"%d","data":"%s","ts":%d}`,
				userID, roomID, payload, time.Now().UnixMilli()))

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					err := wsutil.WriteClientText(conn, msg)
					if err != nil {
						atomic.AddInt64(&sendErrors, 1)
						return
					}
					atomic.AddInt64(&msgSent, 1)
				}
			}
		}(userID, roomID)

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

	connsMu.Lock()
	for _, c := range conns {
		c.Close()
	}
	connsMu.Unlock()

	wg.Wait()

	log.Printf("========== FINAL ==========")
	log.Printf("Connected (last):  %d", atomic.LoadInt64(&connected))
	log.Printf("Disconnected:      %d", atomic.LoadInt64(&disconnected))
	log.Printf("Connect errors:    %d", atomic.LoadInt64(&connectErrs))
	log.Printf("Messages sent:     %d", atomic.LoadInt64(&msgSent))
	log.Printf("Messages received: %d", atomic.LoadInt64(&msgRecv))
	log.Printf("Send errors:       %d", atomic.LoadInt64(&sendErrors))
	log.Printf("============================")
}

func printStats(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	var lastSent, lastRecv int64

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sent := atomic.LoadInt64(&msgSent)
			recv := atomic.LoadInt64(&msgRecv)
			log.Printf("[STATS] conn=%d disc=%d connErr=%d | sent=%d(+%d/5s) recv=%d(+%d/5s) | sendErr=%d",
				atomic.LoadInt64(&connected),
				atomic.LoadInt64(&disconnected),
				atomic.LoadInt64(&connectErrs),
				sent, sent-lastSent,
				recv, recv-lastRecv,
				atomic.LoadInt64(&sendErrors),
			)
			lastSent = sent
			lastRecv = recv
		}
	}
}

func genPayload(size int) string {
	n := size - 40
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