# GoMeshcoreLib

Golang library for control meshcore-companion 






| Task | Call |
| --- | --- |
| Direct message | `SendDirectMessage(ctx, "Name", "text")` |
| Set region scope and send to hast-tag channel | `EnsureHashtagChannel` → `SendChannelMessage(ctx, ch, text, ChannelOptions{Scope:"PNW"})` |
| Recive DM + SNR + Path | `Subscribe(EventDirectMessage)` → `ev.Direct.SNR`, `.Path` |
| Recive channel message + Path | `Subscribe(EventChannelMessage)` → `ev.Channel` |
| Full path + RSSI | `Subscribe(EventRxLog)` → `ev.RxLog.Pkt.Path.Hops` |
| Get telemetry from contact list | `RequestTelemetry(ctx, "Name")` |
| Self telemetry | `RequestSelfTelemetry(ctx)` |
| Node info | `NodeName()`, `RadioPrefs()` |
| Node options | `Battery`, `StatsCore/Radio/Packets`, `CLI`, `Login` |




-----------------------------------------

## Build

```
gofmt -w . && go mod tidy && go build ./... && go vet ./...
```
