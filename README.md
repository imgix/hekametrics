# hekametrics
[go-metrics](https://github.com/rcrowley/go-metrics) -> [mozilla heka](https://github.com/mozilla-services/heka)
Documenation: [godoc](http://godoc.org/github.com/imgix/hekametrics)

Send a native Heka protobuf with metrics stuffed into its [fields](http://hekad.readthedocs.org/en/v0.6.0/message/index.html) every 4 seconds over UDP or TCP.

## Usage
hekametrics sets ```Logger``` to ```go-metrics```, caller controls the ```Type``` field. Payload is left empty.
```golang
client, err := NewHekaClient("tcp://localhost:5565", "teststats")
if err != nil {
	panic(err)
}
go client.LogHeka(metrics.DefaultRegistry, time.Second*4)
```
