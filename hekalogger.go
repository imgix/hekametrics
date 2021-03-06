/***** BEGIN LICENSE BLOCK *****

# Author: David Birdsong (david@imgix.com)
# Copyright (c) 2014, Zebrafish Labs Inc.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# 	Redistributions of source code must retain the above copyright notice,
# 	this list of conditions and the following disclaimer.
#
# 	Redistributions in binary form must reproduce the above copyright notice,
# 	this list of conditions and the following disclaimer in the documentation
# 	and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
# ***** END LICENSE BLOCK *****/

/*
Package hekametrics adds an output for https://github.com/rcrowley/go-metrics/

hekametrics encodes all metrics from a registry into a Heka protobuf message
and sends to a Heka server on it's native listener port.
*/
package hekametrics

import (
	"code.google.com/p/go-uuid/uuid"
	"fmt"
	"github.com/mozilla-services/heka/client"
	"github.com/mozilla-services/heka/message"
	"github.com/rcrowley/go-metrics"
	"log"
	"net/url"
	"os"
	"time"
)

var logger = log.New(os.Stderr, "[hekametrics]", log.LstdFlags)

type HekaClient struct {
	pid               int32
	hostname, msgtype string

	client    client.Client
	encoder   client.StreamEncoder
	sender    client.Sender
	connect_s *url.URL
	stop      chan struct{}
}

//NewHekaClient creates and returns a HekaClient
//
//connect string like 'tcp://127.0.0.1:5564' and 'udp://127.0.0.1:5564'
//
//msgtype sets the 'Type' field on a Heka message
func NewHekaClient(connect, msgtype string) (hc *HekaClient, err error) {
	hc = &HekaClient{}
	hc.connect_s, err = url.ParseRequestURI(connect)
	if err != nil {
		return nil, err
	}
	switch hc.connect_s.Scheme {
	case "tcp", "udp":
	default:
		return nil, fmt.Errorf("scheme: '%s' not supported, try 'tcp://<host>:<port>' or 'udp://<host>:<port>'", hc.connect_s.Scheme)
	}
	hc.msgtype = msgtype
	hc.encoder = client.NewProtobufEncoder(nil)
	hc.pid = int32(os.Getpid())
	hc.hostname, err = os.Hostname()
	if err != nil {
		hc.hostname = "<no hostname>"
	}
	hc.stop = make(chan struct{})
	return
}

func (hc *HekaClient) write(b []byte) error {
	var err error
	reconnect := func() (e error) {
		if hc.sender != nil {
			hc.sender.Close()
			hc.sender = nil
		}

		logger.Printf("Connecting: %s\n", hc.connect_s)
		hc.sender, e = client.NewNetworkSender(hc.connect_s.Scheme, hc.connect_s.Host)
		if e != nil {
			hc.sender = nil
			logger.Printf("Err Connecting: %s %v\n", hc.connect_s, e)
		}
		return e
	}

	if hc.sender == nil {
		err = reconnect()
		if err != nil {
			return err
		}

	}

	err = hc.sender.SendMessage(b)
	if err != nil {
		logger.Printf("Inject: [error] send message: %s\n", err)
		err = reconnect()
		if err != nil {
			return err
		}
		err = hc.sender.SendMessage(b)
		if err != nil {
			return err
		}

	}
	return err
}

// Stops LogHeka from another goroutine
func (hc *HekaClient) Stop() {
	close(hc.stop)
}

// LogHeka is a blocking exporter function which encodes and sends metrics to a Heka server
//
// all metrics in metrics.Registry r are stored on message.Message.Fields
//
// flushing them every Duration d
func (hc *HekaClient) LogHeka(r metrics.Registry, d time.Duration) {

	var (
		stream  []byte
		err     error
		running bool = true
	)

	for running {
		select {
		case _, running = <-hc.stop:
		case <-time.After(d):
		}
		msg := make_message(r)
		msg.SetTimestamp(time.Now().UnixNano())
		msg.SetUuid(uuid.NewRandom())
		msg.SetLogger("go-metrics")
		msg.SetType(hc.msgtype)
		msg.SetPid(hc.pid)
		msg.SetSeverity(100)
		msg.SetHostname(hc.hostname)
		msg.SetPayload("")

		err = hc.encoder.EncodeMessageStream(msg, &stream)
		if err != nil {
			logger.Printf("Inject: [error] encode message: %s\n", err)
		}
		err = hc.write(stream)
		if err != nil {
			logger.Printf("Inject: [error] send message: %s\n", err)
		}

	}

}

func make_message(r metrics.Registry) *message.Message {

	msg := &message.Message{}
	add_float_mapping := func(pref string, names []string, vals []float64) {
		for i, n := range names {

			n = fmt.Sprintf("%s.%s", pref, n)

			if i+1 > len(vals) {
				logger.Println("skipping: %s no value\n", n)
				continue
			}
			f, e := message.NewField(n, vals[i], "")
			if e == nil {
				msg.AddField(f)
			} else {
				logger.Println("skipping: %s %v: %v\n", n, vals[i], e)
			}

		}

	}

	r.Each(func(name string, i interface{}) {

		switch metric := i.(type) {
		case metrics.Counter:
			message.NewInt64Field(msg, name, metric.Count(), "")
		case metrics.Gauge:
			message.NewInt64Field(msg, name, metric.Value(), "")

		case metrics.GaugeFloat64:
			f, e := message.NewField(name, metric.Value(), "")
			if e == nil {
				msg.AddField(f)
			} else {
				logger.Println("skipping: %s %v: %v\n", name, metric.Value(), e)
			}

		case metrics.Histogram:
			h := metric.Snapshot()
			vals_fl := h.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999})
			vals_fl = append(vals_fl, h.Mean(), h.StdDev())
			names := []string{"50-percentile", "75-percentile", "95-percentile",
				"99-percentile", "999-percentile", "mean", "std-dev"}
			add_float_mapping(fmt.Sprintf("%s.histogram", name), names, vals_fl)

			names = []string{"count", "min", "max"}
			vals_i := []int64{h.Count(), h.Min(), h.Max()}

			for i, n := range names {
				n = fmt.Sprintf("%s.histogram.%s", name, n)
				message.NewInt64Field(msg, n, vals_i[i], n)
			}

		case metrics.Meter:
			m := metric.Snapshot()
			message.NewInt64Field(msg, fmt.Sprintf("%s.count", name),
				m.Count(), "")
			names := []string{"one-minute", "five-minute", "fifteen-minute", "mean"}
			vals_fl := []float64{m.Rate1(), m.Rate5(), m.Rate15(), m.RateMean()}

			add_float_mapping(name, names, vals_fl)
		case metrics.Timer:
			h := metric.Snapshot()
			vals_fl := h.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999})
			vals_fl = append(vals_fl, h.Mean(), h.StdDev(), h.Rate1(),
				h.Rate5(), h.Rate15(), h.RateMean())
			names := []string{"50-percentile", "75-percentile", "95-percentile",
				"99-percentile", "999-percentile", "mean", "std-dev", "one-minute",
				"five-minute", "fifteen-minute", "mean-rate"}

			add_float_mapping(fmt.Sprintf("%s.timer", name), names, vals_fl)
			names = []string{"count", "min", "max"}
			vals_i := []int64{h.Count(), h.Min(), h.Max()}
			for i, n := range names {
				n = fmt.Sprintf("%s.timer.%s", name, n)
				message.NewInt64Field(msg, n, vals_i[i], "")
			}

		}
	})
	return msg

}
