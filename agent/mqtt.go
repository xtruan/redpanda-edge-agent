package main

import (
	"context"
	"fmt"
	"strings"
	"sync"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/twmb/franz-go/pkg/kgo"

	log "github.com/sirupsen/logrus"
)

type Mqtt struct {
	name   string
	prefix Prefix
	topics []Topic
	server *mqtt.Server
}

func initMqtt(mq *Mqtt, mutex *sync.Once, prefix Prefix) {
	mutex.Do(func() {
		// MQTT server configuration
		// TODO: allow specifying these in the config yaml
		tcpAddr := ":1883"
		wsAddr := ":8083"
		infoAddr := ":8080"
		tlsTcpAddr := ":8883"
		tlsWsAddr := ":8084"
		tlsInfoAddr := ":8443"

		name := config.String(
			fmt.Sprintf("%s.name", prefix))
		topics := GetTopics(prefix)

		mq.name = name
		mq.prefix = prefix
		mq.topics = topics

		mq.server = mqtt.New(&mqtt.Options{
			InlineClient: true, // must enable inline client to use direct publishing and subscribing.
		})
		_ = mq.server.AddHook(new(auth.AllowHook), nil)

		tcp := listeners.NewTCP(listeners.Config{
			ID:      "tcp",
			Address: tcpAddr,
		})
		err := mq.server.AddListener(tcp)
		if err != nil {
			log.Fatal(err)
		}

		ws := listeners.NewWebsocket(listeners.Config{
			ID:      "ws",
			Address: wsAddr,
		})
		err = mq.server.AddListener(ws)
		if err != nil {
			log.Fatal(err)
		}

		http := listeners.NewHTTPStats(
			listeners.Config{
				ID:      "http",
				Address: infoAddr,
			},
			mq.server.Info,
		)
		err = mq.server.AddListener(http)
		if err != nil {
			log.Fatal(err)
		}

		tlsPath := fmt.Sprintf("%s.tls", prefix)
		if config.Exists(tlsPath) {
			tlsConfig := TLSConfig{}
			config.Unmarshal(tlsPath, &tlsConfig)
			tc := TLSConf(&tlsConfig)

			tcps := listeners.NewTCP(listeners.Config{
				ID:        "tcps",
				Address:   tlsTcpAddr,
				TLSConfig: tc,
			})
			err = mq.server.AddListener(tcps)
			if err != nil {
				log.Fatal(err)
			}

			wss := listeners.NewWebsocket(listeners.Config{
				ID:        "wss",
				Address:   tlsWsAddr,
				TLSConfig: tc,
			})
			err = mq.server.AddListener(wss)
			if err != nil {
				log.Fatal(err)
			}

			https := listeners.NewHTTPStats(
				listeners.Config{
					ID:        "https",
					Address:   tlsInfoAddr,
					TLSConfig: tc,
				}, mq.server.Info,
			)
			err = mq.server.AddListener(https)
			if err != nil {
				log.Fatal(err)
			}
		}
	})
}

func serveMqtt(mq *Mqtt) {
	err := mq.server.Serve()
	if err != nil {
		log.Fatal(err)
	}
}

func forwardMqttRecords(src *Mqtt, dst *Redpanda, ctx context.Context) {
	defer wg.Done()
	// var errCount int = 0

	// Subscribe to a filter and handle any received messages via a callback function.
	recordHandler := func(cl *mqtt.Client, sub packets.Subscription, pk packets.Packet) {
		// log.Debug("Inline MQTT client receive - ",
		// 	"client: ", cl.ID,
		// 	", subscriptionId: ", sub.Identifier,
		// 	", topic: ", pk.TopicName,
		// 	", payload: ", string(pk.Payload))

		// Convert topic from MQTT to Redpanda style
		// TODO: handle user topic mapping, i.e. source:destination
		topic := strings.Replace(pk.TopicName, "/", ".", -1)

		// Check the local cache for the topic
		// TODO: allow resetting the cache/syncing with Redpanda
		if !contains(createTopicCache, topic) {
			createTopicCache = append(createTopicCache, topic)
			// Need to check/create topics here because MQTT allows wildcards
			checkSpecifiedTopics(dst, createTopicCache)
		}

		// Send records to destination
		record := createRecord(topic, "", string(pk.Payload))
		dst.client.Produce(ctx, record, func(_ *kgo.Record, err error) {
			if err != nil {
				if err == context.Canceled {
					logWithId("info", src.name,
						fmt.Sprintf("Received interrupt: %s", err.Error()))
					return
				}
				logWithId("error", src.name,
					fmt.Sprintf("Unable to send %d record(s) to %s: %s",
						1, dst.name, err.Error()))
				// backoff(&errCount)
			} else {
				logWithId("debug", src.name,
					fmt.Sprintf("Sent %d records to %s",
						1, dst.name))
			}
		})
	}

	log.Info("Inline MQTT client subscribing: ", src.topics)
	i := 0
	for _, topic := range src.topics {
		i += 1
		_ = src.server.Subscribe(topic.consumeFrom(), i, recordHandler)
	}

	// Wait for the context to be cancelled
	<-ctx.Done()
}
