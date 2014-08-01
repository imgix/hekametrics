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

package hekametrics

import (
	"errors"
	"github.com/rcrowley/go-metrics"
	"math/rand"
	"time"
)

const fanout = 10

func main() {

	r := metrics.NewRegistry()

	c := metrics.NewCounter()
	r.Register("foo", c)
	for i := 0; i < fanout; i++ {
		go func() {
			for {
				c.Dec(19)
				time.Sleep(300e6)
			}
		}()
		go func() {
			for {
				c.Inc(47)
				time.Sleep(400e6)
			}
		}()
	}

	g := metrics.NewGauge()
	r.Register("bar", g)
	for i := 0; i < fanout; i++ {
		go func() {
			for {
				g.Update(19)
				time.Sleep(300e6)
			}
		}()
		go func() {
			for {
				g.Update(47)
				time.Sleep(400e6)
			}
		}()
	}

	gf := metrics.NewGaugeFloat64()
	r.Register("barfloat64", gf)
	for i := 0; i < fanout; i++ {
		go func() {
			for {
				g.Update(19.0)
				time.Sleep(300e6)
			}
		}()
		go func() {
			for {
				g.Update(47.0)
				time.Sleep(400e6)
			}
		}()
	}

	hc := metrics.NewHealthcheck(func(h metrics.Healthcheck) {
		if 0 < rand.Intn(2) {
			h.Healthy()
		} else {
			h.Unhealthy(errors.New("baz"))
		}
	})
	r.Register("baz", hc)

	s := metrics.NewExpDecaySample(1028, 0.015)
	//s := metrics.NewUniformSample(1028)
	h := metrics.NewHistogram(s)
	r.Register("bang", h)
	for i := 0; i < fanout; i++ {
		go func() {
			for {
				h.Update(19)
				time.Sleep(300e6)
			}
		}()
		go func() {
			for {
				h.Update(47)
				time.Sleep(400e6)
			}
		}()
	}

	m := metrics.NewMeter()
	r.Register("quux", m)
	for i := 0; i < fanout; i++ {
		go func() {
			for {
				m.Mark(19)
				time.Sleep(300e6)
			}
		}()
		go func() {
			for {
				m.Mark(47)
				time.Sleep(400e6)
			}
		}()
	}

	t := metrics.NewTimer()
	r.Register("hooah", t)
	for i := 0; i < fanout; i++ {
		go func() {
			for {
				t.Time(func() { time.Sleep(300e6) })
			}
		}()
		go func() {
			for {
				t.Time(func() { time.Sleep(400e6) })
			}
		}()
	}

	metrics.RegisterDebugGCStats(r)
	go metrics.CaptureDebugGCStats(r, 5e9)

	metrics.RegisterRuntimeMemStats(r)
	go metrics.CaptureRuntimeMemStats(r, 5e9)

	client, err := NewHekaClient("udp://localhost:5565", "teststats")
	if err != nil {
		panic(err)
	}
	client.LogHeka(r, time.Second*4)
}
