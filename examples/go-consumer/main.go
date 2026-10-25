// Go consumer example: pull updates through the gateway, echo each
// message back with an automatic Idempotency-Key (a crash between
// send and ack can never double-send).
//
// Env:
//
//	TGGW_BASE       gateway base URL     (default http://127.0.0.1:8080)
//	TGGW_APP_SECRET GW_APP_SECRET value  (required)
//	BOT_TOKEN       the token registered in the gateway (required)
package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	tggw "github.com/DovBerKaplan/tg-session-gateway/pkg/client"
)

func main() {
	base := env("TGGW_BASE", "http://127.0.0.1:8080")
	secret := os.Getenv("TGGW_APP_SECRET")
	token := os.Getenv("BOT_TOKEN")
	if secret == "" || token == "" {
		log.Fatal("TGGW_APP_SECRET and BOT_TOKEN are required")
	}

	c := tggw.NewBotAPI(base, secret, token)
	ctx := context.Background()

	for {
		updates, err := c.PullUpdates(ctx, 25*time.Second, 100)
		if err != nil {
			log.Printf("pull: %v", err)
			time.Sleep(time.Second)
			continue
		}
		for _, u := range updates {
			var msg struct {
				Message struct {
					Chat struct {
						ID int64 `json:"id"`
					} `json:"chat"`
					Text string `json:"text"`
				} `json:"message"`
			}
			if json.Unmarshal(u.Raw, &msg) != nil || msg.Message.Chat.ID == 0 {
				continue // not a text-message update
			}
			_, err := c.SendMessage(ctx, msg.Message.Chat.ID,
				"echo: "+msg.Message.Text, nil)
			if err != nil {
				log.Printf("send: %v", err) // includes retry_after on 429
			}
		}
		// persist c.Offset() if you want exactly-once across restarts
	}
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
