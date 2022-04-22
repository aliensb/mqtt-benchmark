package main

import (
	"crypto/tls"
	"fmt"
	"log"
	"time"

	"github.com/GaryBoone/GoStats/stats"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// Client implements an MQTT client running benchmark test
type Client struct {
	ID              int
	ClientID        string
	BrokerURL       string
	BrokerUser      string
	BrokerPass      string
	MsgTopic        string
	MsgPayload      string
	MsgSize         int
	MsgCount        int
	MsgQoS          byte
	Quiet           bool
	WaitTimeout     time.Duration
	TLSConfig       *tls.Config
	MessageInterval int
	Mod             string
	SubTopic        string
}

// Run runs benchmark tests and writes results in the provided channel
func (c *Client) Run(res chan *RunResults, clientStarted chan bool) {
	newMsgs := make(chan *Message)
	pubMsgs := make(chan *Message)
	receivedMsgs := make(chan *Message)
	doneGen := make(chan bool)
	donePub := make(chan bool)
	doneSub := make(chan bool)
	loseConection := make(chan bool)
	subscribeFailed := make(chan bool)
	runResults := new(RunResults)
	started := time.Now()
	// start generator,
	if c.Mod == "pub" {
		go c.genMessages(newMsgs, doneGen)
	}

	// start mqtt client
	go c.start(newMsgs, pubMsgs, receivedMsgs, doneGen, donePub, doneSub, loseConection, subscribeFailed, clientStarted)

	runResults.ID = c.ID
	times := []float64{}
	for {
		select {
		case m := <-pubMsgs:
			if m.Error {
				log.Printf("CLIENT %v ERROR publishing message: %v: at %v\n", c.ID, m.Topic, m.Sent.Unix())
				runResults.Failures++
			} else {
				// log.Printf("Message published: %v: sent: %v delivered: %v flight time: %v\n", m.Topic, m.Sent, m.Delivered, m.Delivered.Sub(m.Sent))
				runResults.Successes++
				times = append(times, m.Delivered.Sub(m.Sent).Seconds()*1000) // in milliseconds
			}
		case rm := <-receivedMsgs:
			if rm.Error {
				log.Printf("CLIENT %v ERROR receive message: %v: at %v\n", c.ID, rm.Topic, rm.Sent.Unix())
			}
			runResults.MsgsReceived++
		case <-donePub:
			// calculate results
			duration := time.Since(started)
			runResults.MsgTimeMin = stats.StatsMin(times)
			runResults.MsgTimeMax = stats.StatsMax(times)
			runResults.MsgTimeMean = stats.StatsMean(times)
			runResults.RunTime = duration.Seconds()
			runResults.MsgsPerSec = float64(runResults.Successes) / duration.Seconds()
			// calculate std if sample is > 1, otherwise leave as 0 (convention)
			if c.MsgCount > 1 {
				runResults.MsgTimeStd = stats.StatsSampleStandardDeviation(times)
			}

			// report results and exit
			res <- runResults
			return

		case <-doneSub:
			res <- runResults

		case <-loseConection:
			runResults.LostConnectin = true
			res <- runResults
			return

		case <-subscribeFailed:
			runResults.SubscribeFailed = true
			res <- runResults
			return

		}
	}
}

func (c *Client) genMessages(ch chan *Message, done chan bool) {
	var payload interface{}
	// set payload if specified
	if c.MsgPayload != "" {
		payload = c.MsgPayload
	} else {
		payload = make([]byte, c.MsgSize)
	}

	for i := 0; i < c.MsgCount; i++ {
		ch <- &Message{
			Topic:   c.MsgTopic,
			QoS:     c.MsgQoS,
			Payload: payload,
		}
		time.Sleep(time.Duration(c.MessageInterval) * time.Second)
	}

	done <- true
	// log.Printf("CLIENT %v is done generating messages\n", c.ID)
}

func (c *Client) start(in, out, receivedMsgs chan *Message, doneGen, donePub, doneSub, loseConection, subscribeFailed, clientStarted chan bool) {
	//client连接上后的回调
	onConnected := func(client mqtt.Client) {
		if !c.Quiet {
			log.Printf("CLIENT %v is connected to the broker %v\n", c.ID, c.BrokerURL)
			loseConection <- true
		}
		ctr := 0
		if c.Mod == "pub" {
			clientStarted <- true
			for {
				select {
				case m := <-in:
					m.Sent = time.Now()
					token := client.Publish(m.Topic, m.QoS, false, m.Payload)
					res := token.WaitTimeout(c.WaitTimeout)
					if !res {
						log.Printf("CLIENT %v Timeout sending message: %v\n", c.ID, token.Error())
						m.Error = true
					} else if token.Error() != nil {
						log.Printf("CLIENT %v Error sending message: %v\n", c.ID, token.Error())
						m.Error = true
					} else {
						m.Delivered = time.Now()
						m.Error = false
					}
					out <- m

					if ctr > 0 && ctr%100 == 0 {
						if !c.Quiet {
							log.Printf("CLIENT %v published %v messages and keeps publishing...\n", c.ID, ctr)
						}
					}
					ctr++
				case <-doneGen:
					donePub <- true
					if !c.Quiet {
						log.Printf("CLIENT %v is done publishing\n", c.ID)
					}
					return
				}
			}
		} else {
			t := client.Subscribe(c.SubTopic, 0, func(client mqtt.Client, msg mqtt.Message) {
				if string(msg.Payload()) == "finish" {
					// log.Printf("CLIENT %v had done sub \n", c.ID)
					donePub <- true
				}
				// if !c.Quiet {
				// 	log.Printf("CLIENT %v is receive message %v\n", c.ID, string(msg.Payload()))
				// }
				receivedMsgs <- &Message{
					Error: false,
				}
			})
			t.Wait()
			if t.Error() != nil {
				log.Printf("CLIENT %v had error sub to the broker: %v\n", c.ID, t.Error())
				subscribeFailed <- true
			}
			clientStarted <- true
		}
	}
	log.Printf("CLIENT id is  %v", fmt.Sprintf("%s-%v", c.ClientID, c.ID))
	opts := mqtt.NewClientOptions().
		AddBroker(c.BrokerURL).
		SetClientID(fmt.Sprintf("%s-%v", c.ClientID, c.ID)).
		SetCleanSession(true).
		SetAutoReconnect(false).
		SetOnConnectHandler(onConnected).
		SetPingTimeout(25 * time.Second).
		SetConnectionLostHandler(func(client mqtt.Client, reason error) {
			log.Printf("CLIENT %v lost connection to the broker: %v. Will reconnect...\n", c.ID, reason.Error())
			loseConection <- true
		})
	if c.BrokerUser != "" && c.BrokerPass != "" {
		opts.SetUsername(c.BrokerUser)
		opts.SetPassword(c.BrokerPass)
	}
	if c.TLSConfig != nil {
		opts.SetTLSConfig(c.TLSConfig)
	}

	client := mqtt.NewClient(opts)
	token := client.Connect()
	token.Wait()
	if token.Error() != nil {
		log.Printf("CLIENT %v had error connecting to the broker: %v\n", c.ID, token.Error())
		loseConection <- true
	}
}
