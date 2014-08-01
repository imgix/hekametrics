# hekametrics
go metrics -> heka

- go metrics: [rcrowley's go-metric](https://github.com/rcrowley/go-metrics) library (inspired by coda hale)
- message router: [mozilla's message router: heka](https://github.com/mozilla-services/heka)

Send a native Heka protobuf with metrics stuffed into its [fields](http://hekad.readthedocs.org/en/v0.6.0/message/index.html) every 4 seconds over UDP or TCP.

hekametrics sets ```Logger``` to ```go-metrics```, caller controls the ```Type``` field. Payload is left empty.
```golang
client, err := NewHekaClient("tcp://localhost:5565", "teststats")
if err != nil {
	panic(err)
}
go client.LogHeka(metrics.DefaultRegistry, time.Second*4)
```
