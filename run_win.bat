cd loadtest

go mod tidy

set CENTRIFUGO_WS_URL=ws://192.168.1.80:8021/connection/websocket
set CENTRIFUGO_TOKEN_SECRET=token-secret-key
set NUM_CLIENTS=10000
set USERS_PER_ROOM=10
set MESSAGE_INTERVAL=5s
set MESSAGE_SIZE=100
set RAMP_UP_DELAY=1ms

go run main.go
