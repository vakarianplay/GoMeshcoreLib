# GoMeshcoreLib <img src="https://cdn.jsdelivr.net/gh/selfhst/icons/webp/meshcore.webp" alt="Meshtastic Logo" width="80"/>

[English](README.md) | [Русский](README_ru.md)



![alt text](https://img.shields.io/badge/Golang-%20>1.21-blue?style=flat-square&logo=go)

![alt text](https://img.shields.io/badge/Status-progress-orange?style=for-the-badge&logo=Buddy)
<!-- ![alt text](https://img.shields.io/badge/Status-in%20complete-2E8B57?style=for-the-badge&logo=Buddy) -->

A dependency-light Go client library for MeshCore LoRa mesh networks. It speaks
the companion protocol directly, so it works with any MeshCore companion radio
over WiFi/TCP or USB/serial — no phone app and no Python runtime required.

The library implements direct messaging with a direct-then-flood retry policy,
hashtag channel messaging with region (flood scope) selection, inbound message
streaming with SNR and path data, telemetry requests, and local node
introspection.

## Features

- Direct messages to a contact with configurable retries: first over the last
  known path, then over flood after an automatic path reset.
- Hashtag channel messaging with region selection, plus confirmation that at
  least one repeater echoed the packet.
- Inbound direct and channel messages with SNR, hop count and route type.
- Raw packet capture via `rx_log`, the only source of RSSI, hop hashes and
  region transport codes.
- Telemetry from any node, and from the local companion without using the radio.
- Cayenne LPP decoding into typed fields.
- Local node info: name, radio parameters, battery, storage, core/radio/packet
  statistics, device clock, and arbitrary CLI commands.
- Remote CLI commands to other nodes.
- Event subscription API with per-subscription buffered channels.
- Transport agnostic: the same protocol code over TCP and serial.

## Requirements

- Go 1.21 or newer (the library uses the builtin `min` and `max`).
- One external dependency: `go.bug.st/serial` for USB/COM access.
- A MeshCore companion radio with a TCP (WiFi) or USB serial interface.
- `CMD_RUN_CLI_COMMAND` (the `CLI` method) requires companion firmware v14 or
  newer.

## Installation

```bash
go get github.com/vakarianplay/GoMeshcoreLib
```

Manual install

```bash
git clone https://github.com/vakarianplay/GoMeshcoreLib
cd GoMeshcoreLib
go mod init GoMeshcoreLib
go mod tidy
go build ./...
```

## Quick start

```go
package main

import (
    "context"
    "fmt"
    "log"

    meshcore "GoMeshcoreLib"
)

func main() {
    ctx := context.Background()

    cfg := meshcore.DefaultConfig()
    cfg.Verbose = true
    cfg.Logf = func(f string, a ...any) { fmt.Printf(f+"\n", a...) }

    // WiFi companion
    c, err := meshcore.DialTCPClient("mesh.lan", 5000, cfg)
    if err != nil {
        log.Fatal(err)
    }
    defer c.Close()

    // USB companion instead:
    // c, err := meshcore.DialSerialClient("/dev/ttyACM0", 115200, cfg)

    fmt.Println("node:", c.NodeName())
    fmt.Println("radio:", c.RadioPrefs())

    t, err := c.RequestSelfTelemetry(ctx)
    if err != nil {
        log.Fatal(err)
    }
    for _, f := range t.LPP.Fields {
        fmt.Printf("  ch%d %s = %.2f %s\n", f.Channel, f.Name, f.Value, f.Unit)
    }
}
```

## Configuration

`DefaultConfig()` returns the values below. Any zero field falls back to these
defaults through `Config.withDefaults`.

| Field | Default | Meaning |
|---|---|---|
| `DirectAttempts` | 5 | Tries over the last known path before switching to flood |
| `FloodAttempts` | 5 | Flood tries after `ResetPath` |
| `ChannelAttempts` | 3 | Channel send tries until a repeater echoes the packet |
| `RetryDelay` | 0 | Pause between tries |
| `EchoWindow` | 8s | How long to wait for repeater echoes |
| `RequireRepeats` | 1 | Echoes of one packet counted as proof |
| `CommandTimeout` | 15s | Timeout for short command/response exchanges |
| `AckSlack` | 1.25 | Multiplier applied to the firmware `suggested_timeout` |
| `MinWait` | 5s | Lower bound for waiting on a mesh response |
| `MaxWait` | 5m | Upper bound for waiting on a mesh response |
| `AppName` | `go-meshcore` | Name reported to the firmware in `APP_START` |
| `Verbose` | false | Dump every frame in hex through `Logf` |
| `Logf` | no-op | Logging sink |
| `EventBuffer` | 256 | Default subscription buffer size |

Waiting on a mesh response uses `suggested_timeout` from the firmware `SENT`
frame, scaled by `AckSlack` and clamped to `[MinWait, MaxWait]`. The firmware
computes that value from airtime, so flood routes naturally get a much longer
window than direct ones.

## Direct messages

```go
res, err := c.SendDirectMessage(ctx, "MO EST 13", "hello")
if err != nil {
    log.Println("send failed:", err)
}
fmt.Printf("acked=%v attempts=%d direct=%d flood=%d route=%s trip=%s\n",
    res.Acked, res.Attempts, res.DirectAttempts, res.FloodAttempts,
    res.Route, res.TripTime)
```

`SendDirectMessageTo(ctx, ct, text)` takes an already resolved `*Contact` and
avoids a second contact lookup.

Retry sequence:

1. Up to `DirectAttempts` tries over the path stored for the contact.
2. `ResetPath` marks the outbound path unknown.
3. Up to `FloodAttempts` tries over flood.

Each try is confirmed by an ACK push carrying the same 4-byte tag as the `SENT`
frame. The loop returns as soon as one matches. `ErrNotFound` aborts
immediately, since retrying an unknown contact cannot help.

Text longer than `MaxTextLen` (133 bytes) is rejected. `SplitText` splits it
into tagged `[i/n]` chunks.

## Channel messages and regions

```go
ch, err := c.EnsureHashtagChannel(ctx, "test")
if err != nil {
    log.Fatal(err)
}

res, err := c.SendChannelMessage(ctx, ch, "hello #test", meshcore.ChannelOptions{
    Scope: "vostok",
})
fmt.Printf("sent=%v heard=%v attempts=%d echoes=%d\n",
    res.Sent, res.Heard, res.Attempts, len(res.Echoes))
for _, e := range res.Echoes {
    fmt.Printf("  echo %s snr=%.2f rssi=%.0f\n", e.Path, e.SNR, e.RSSI)
}
```

`ChannelOptions`:

| Field | Meaning |
|---|---|
| `Scope` | Region name, e.g. `vostok`. Empty keeps the node default scope |
| `Unscoped` | Classic unscoped flood, ignores `Scope` |
| `Attempts` | Overrides `ChannelAttempts` |
| `EchoWindow` | Overrides `EchoWindow` |
| `RequireRepeats` | Overrides `RequireRepeats` |
| `RestoreScope` | Restore the previous scope afterwards, default true |

`EnsureHashtagChannel` finds an existing slot or creates one. The channel key is
the first 16 bytes of `SHA-256("#name")`, which is the standard hashtag channel
derivation, so other MeshCore clients see the same channel. Slot 0 is reserved
for the built-in Public channel and is never taken. If no free slot exists the
call returns `ErrNoChannelSlots`.

Region handling uses the same derivation: `SetFloodScope(ctx, "vostok")` hashes
`#vostok` into a 16-byte transport key. `ClearFloodScope` removes the override,
`SetUnscoped` enables classic flooding, `DefaultScope` reads the region
configured on the node.

### How "heard" is determined

For group messages the firmware answers `RESP_CODE_OK` only — no tag and no
expected ACK, unlike direct messages. There is therefore no reliable way to tie
a reply to a specific send.

`Heard` is derived from `rx_log` echoes instead: a repeater that forwards the
packet appends its own hop hash, and that copy is pushed to the client with SNR
and RSSI. Matching is heuristic and uses three conditions:

- payload type `GRP_TXT` (0x05),
- a non-empty path, meaning at least one repeater appended a hash,
- clustering by byte-identical payload, since repeaters never alter it.

`Sent` is true whenever the firmware accepted the packet, even if no echo was
confirmed. In a busy mesh raise `RequireRepeats` to 2 to reduce false
attributions.

## Receiving messages

```go
sub := c.Subscribe(meshcore.EventDirectMessage, meshcore.EventChannelMessage)
defer sub.Cancel()

for ev := range sub.Events() {
    switch ev.Kind {
    case meshcore.EventDirectMessage:
        m := ev.Direct
        fmt.Printf("[DM] %s: %s (snr=%.2f, %s)\n",
            m.FromName, m.Text, m.SNR, m.Path)
    case meshcore.EventChannelMessage:
        m := ev.Channel
        fmt.Printf("[CH %s] %s (snr=%.2f, %s)\n",
            m.ChannelName, m.Text, m.SNR, m.Path)
    }
}
```

`Subscribe()` with no arguments receives every event kind. `SubscribeBuffered`
sets an explicit buffer size — use a large one for `EventRxLog`, since that is a
stream of all received packets.

When a subscription buffer fills, events are dropped and counted. `Dropped()`
returns the cumulative counter.

The client runs an internal pump: on every `MSG_WAITING` push it drains the
firmware message queue with `CMD_SYNC_NEXT_MESSAGE` and broadcasts the frames to
subscribers. `DrainMessages()` triggers the same cycle manually.

### Event kinds

| Kind | Payload field |
|---|---|
| `EventDirectMessage` | `ev.Direct` |
| `EventChannelMessage` | `ev.Channel` |
| `EventChannelData` | `ev.Frame` |
| `EventRxLog` | `ev.RxLog` |
| `EventACK` | `ev.Frame` |
| `EventTelemetry` | `ev.Telem` |
| `EventStatus` | `ev.Frame` |
| `EventBinaryResponse` | `ev.Frame` |
| `EventAdvert`, `EventNewAdvert` | `ev.PubKey`, `ev.Frame` |
| `EventPathUpdate` | `ev.Frame` |
| `EventMsgWaiting` | none |
| `EventLoginSuccess`, `EventLoginFail` | `ev.Frame` |
| `EventTraceData`, `EventControlData`, `EventRawData` | `ev.Frame` |

## Route, SNR and RSSI

Message frames carry SNR and the `path_len` byte only. `PathInfo` decodes that
byte into route type, hash size and hop count:

```go
fmt.Println(m.Path.Flood)      // false means a direct route (marker 0xFF)
fmt.Println(m.Path.HashCount)  // number of hops
fmt.Println(m.Path.HashSize)   // bytes per hop hash: 1..3
fmt.Println(m.Path)            // "direct", "flood/0hop" or "flood[a3→5f]"
```

The hop hashes themselves and the RSSI are not present in message frames. They
come from `rx_log` (push 0x88), which carries the raw packet:

```go
sub := c.Subscribe(meshcore.EventRxLog)
defer sub.Cancel()

for ev := range sub.Events() {
    e := ev.RxLog
    fmt.Printf("snr=%.2f rssi=%.0f type=%d path=%s hops=%v\n",
        e.SNR, e.RSSI, e.Pkt.PayloadType, e.Pkt.Path, e.Pkt.Path.Hops)
    if e.Pkt.HasTransport {
        fmt.Printf("  region code=0x%04x reply=0x%04x\n",
            e.Pkt.TransportCodes[0], e.Pkt.TransportCodes[1])
    }
}
```

For a contact, the route of its most recent advert is available directly:

```go
path, when, err := c.ContactAdvertPath(ctx, ct)
```

Note that a region name cannot be recovered from a transport code. It is a
2-byte truncated HMAC-SHA256 over the payload type and body, keyed by the region
key. Only the raw code is exposed.

## Telemetry

```go
t, err := c.RequestTelemetry(ctx, "MO EST 08")
if err != nil {
    log.Fatal(err)
}
fmt.Printf("%s: %d attempts, route=%s\n", t.FromName, t.Attempts, t.Route)
for _, f := range t.LPP.Fields {
    fmt.Printf("  ch%d %-20s %8.2f %s\n", f.Channel, f.Name, f.Value, f.Unit)
}
if v, ok := t.LPP.Get("voltage"); ok {
    fmt.Println("battery:", v, "V")
}
```

No login or password is required. The firmware telemetry handler checks only the
node telemetry modes (`telemetry_mode_base`, `_loc`, `_env`) and the contact
flags. If the node is set to deny telemetry, no response ever arrives and the
call exhausts its retries.

`RequestTelemetryFrom(ctx, ct)` skips the name lookup. `RequestSelfTelemetry(ctx)`
asks the companion for its own data; the request is 4 bytes with no public key
and the firmware answers immediately without using the radio.

Responses are matched by the 6-byte public key prefix, because the firmware
strips the tag from the telemetry push frame.

Retries follow the same policy as direct messages: `DirectAttempts` over the
known path, then `ResetPath` and `FloodAttempts` over flood.

### LPP fields

| Type id | Name | Unit | Width | Scale |
|---|---|---|---|---|
| 0 | `digital_input` | — | 1 | 1 |
| 1 | `digital_output` | — | 1 | 1 |
| 2 | `analog_input` | — | 2 | 0.01 signed |
| 3 | `analog_output` | — | 2 | 0.01 signed |
| 100 | `generic_sensor` | — | 4 | 1 |
| 101 | `luminosity` | lx | 2 | 1 |
| 102 | `presence` | — | 1 | 1 |
| 103 | `temperature` | °C | 2 | 0.1 signed |
| 104 | `relative_humidity` | % | 1 | 0.5 |
| 113 | `accelerometer` | G | 6 | 0.001 per axis |
| 115 | `pressure` | hPa | 2 | 0.1 |
| 116 | `voltage` | V | 2 | 0.01 |
| 117 | `current` | A | 2 | 0.001 signed |
| 118 | `frequency` | Hz | 4 | 1 |
| 120 | `percentage` | % | 1 | 1 |
| 121 | `altitude` | m | 2 | 1 signed |
| 125 | `concentration` | ppm | 2 | 1 |
| 128 | `power` | W | 2 | 1 |
| 130 | `distance` | m | 4 | 0.001 |
| 131 | `energy` | kWh | 4 | 0.001 |
| 132 | `direction` | ° | 2 | 1 |
| 133 | `unixtime` | s | 4 | 1 |
| 134 | `gyrometer` | °/s | 6 | 0.01 per axis |
| 135 | `colour` | RGB | 3 | per channel |
| 136 | `gps` | ° | 9 | 0.0001 lat/lon, 0.01 alt |
| 142 | `switch` | — | 1 | 1 |

Channel 1 conventionally carries battery voltage and MCU temperature. Channel 0
in the stream marks the end of data.

Unknown type ids stop parsing and land in `LPPResult.Tail` as hex. This is
deliberate: the firmware `getDataSize()` returns 1 for unknown types, which
would silently corrupt every following field.

If humidity reads double and later fields look shifted, the firmware disagreed
with itself on that field width. Set:

```go
meshcore.HumidityFieldSize = 2
```

## Local node information

```go
c.NodeName()        // node name, no radio traffic
c.Self()            // SelfInfo: key, radio, telemetry modes, position
c.Device()          // DeviceInfo: firmware version, model, limits
c.RadioPrefs()      // freq, bandwidth, SF, CR, TX power, path hash mode

bat, _ := c.Battery(ctx)
fmt.Printf("%d mV, %d/%d KB used\n", bat.Millivolts, bat.UsedKB, bat.TotalKB)

core, _ := c.StatsCore(ctx)     // battery, uptime, error flags, queue length
radio, _ := c.StatsRadio(ctx)   // noise floor, last RSSI/SNR, TX/RX airtime
pkts, _ := c.StatsPackets(ctx)  // sent/received counters, flood vs direct

secs, _ := c.CurrentTime(ctx)
_ = c.SyncClock(ctx)            // set the device clock to host time
```

`RadioPrefs()` reads from the cached handshake and costs no radio time.

### CLI

```go
out, err := c.CLI(ctx, "get radio")   // runs on the local companion (v14+)
fmt.Println(meshcore.ParseCLIValue(out))
```

```go
err := c.RemoteCLI(ctx, ct, "get name") // sends a command to another node
```

A remote node executes the command only if the contact was granted Remote CLI
permission. Otherwise the text reaches its owner as a plain message. Grant that
permission only to nodes you control: it allows changing radio parameters,
renaming the node, rebooting it and reading the saved WiFi password.

`ParseCLIValue` splits a reply such as `> 910.5250244,62.5,7,5` into fields,
`ParseCLIFloat` converts the first one.

## Contacts and channels

```go
_ = c.RefreshContacts(ctx)   // re-read the contact book
for _, ct := range c.Contacts() {
    fmt.Println(ct.Name, ct.Type, ct.IsRepeater())
}

ct, err := c.ResolveContact(ctx, "MO EST 13")  // exact, then case-insensitive
_ = c.ResetPath(ctx, ct)                       // force the next send over flood
path, when, _ := c.ContactAdvertPath(ctx, ct)
```

```go
_ = c.RefreshChannels()
for _, ch := range c.Channels() {
    fmt.Println(ch.Index, ch.Name)
}
ch, ok := c.FindChannel("test")
_ = c.SetChannel(ctx, ch.Index, "#test", meshcore.DeriveKey16("#test"))
```

Contact frames are parsed from the end of the frame backwards, because the
variable-length path field precedes the name and its maximum size is not exposed
by the firmware. The 16-byte tail after the name is fixed, so the name always
starts at `len - 48`.

The contact book is streamed one frame per firmware loop iteration, so a large
book takes seconds. `RefreshContacts` bounds each frame by a scaled
`CommandTimeout` and the whole transfer by the passed context.

## Protocol notes

Framing is identical over TCP and serial:

```text
app  -> radio: '<' + uint16 LE length + payload
radio -> app : '>' + uint16 LE length + payload
```

On resynchronisation the reader scans for the next `'>'`. Lengths of 0 or above
512 are treated as garbage.

Session setup sends `APP_START`, `DEVICE_QUERY` with protocol version 3, and
`SET_DEVICE_TIME`. Protocol version 3 is what makes the firmware emit V3 message
frames carrying SNR. `APP_START` also resets the firmware contact iterator, so
`GET_CONTACTS` cannot fail with `BAD_STATE` left over from another client.

One short command/response transaction is serialised by a mutex. Writes are
serialised separately. Waiters are armed before the command is written, so a
push that arrives immediately after `SENT` is never missed.

Telemetry requests must be issued sequentially. The firmware keeps a single
`pending_telemetry` slot and clears pending requests on every new one, so a
second concurrent request would orphan the first response.

## Limitations

- Region names are not recoverable from transport codes. Those are truncated
  HMAC values, not identifiers. Only the raw 16-bit code is exposed.
- Channel send attribution is heuristic, see the `Heard` section above.
- Message frames carry neither hop hashes nor RSSI. Use `EventRxLog` or
  `ContactAdvertPath`.
- Telemetry push frames carry no tag, so responses are matched by public key
  prefix. Two simultaneous telemetry requests to the same node cannot be told
  apart.
- `HumidityFieldSize` is ambiguous in the firmware sources themselves. The
  default of 1 matches the CayenneLPP encoder and `readRelativeHumidity`, while
  `getDataSize` says 2.
- The maximum path size is not exposed by the firmware. It does not affect
  parsing, but it does mean unknown-type LPP fields cannot be skipped safely.
- The library targets the companion protocol as of firmware v1.16 to v1.18 era
  builds. Older builds may lack `CMD_RUN_CLI_COMMAND`, scoped floods or V3
  frames.
- IPv6 hosts are supported through `net.JoinHostPort`.

## Troubleshooting

| Symptom | Cause and action |
|---|---|
| `dial ... connection refused` | The companion is a BLE-only build. The TCP server exists only in WiFi-enabled builds. Check `get wifi.status` and `get wifi.ip` over the serial console |
| `NOT_FOUND` on a send or telemetry request | The node is not in the contact book. Send an advert, wait for the node to appear, then retry |
| `SENT` arrives but no telemetry push | The node telemetry mode is deny, or the contact lacks the telemetry flags |
| `ErrNotHeard` although `Sent` is true | No repeater echoed the packet within `EchoWindow`. The packet did go out. Widen the window or raise `ChannelAttempts` |
| `ErrNoAck` after all retries | The peer is out of range, powered off, or on different radio parameters |
| No channel messages at all | The hashtag channel slot is missing. Call `EnsureHashtagChannel` |
| `Dropped()` keeps growing | Subscription buffer overflow. Use `SubscribeBuffered` with a larger size, especially for `EventRxLog` |
| Humidity looks doubled | Set `HumidityFieldSize = 2` |
| Everything looks desynchronised | Enable `Verbose` and inspect the hex dump; the reader resynchronises on `'>'` automatically |

## Testing

Nothing in the protocol path requires hardware:

```bash
go test ./...
```

The pure functions worth covering with table tests are `DecodeLPP`,
`decodePathBytes`, `parseRawPacket`, `parseContact` and `messageBody`.

A hardware-free smoke test on a real radio is `RequestSelfTelemetry`: it
exercises framing, the handshake and the LPP decoder in one call, without any
transmission.


## References

Protocol details were taken from the firmware and the official client libraries:

- [meshcore-dev/MeshCore](https://github.com/meshcore-dev/MeshCore) —
  `examples/companion_radio/MyMesh.cpp`, `src/helpers/wifi/SerialWifiInterface.cpp`,
  `src/helpers/ArduinoSerialInterface.cpp`, `src/Packet.h`, `src/Packet.cpp`,
  `src/helpers/TransportKeyStore.cpp`, `src/helpers/sensors/LPPDataHelpers.h`,
  `src/helpers/BaseChatMesh.cpp`, `src/helpers/CommonCLI.cpp`
- [meshcore-dev/meshcore_py](https://github.com/meshcore-dev/meshcore_py) —
  `packets.py`, `messaging.py`, `binary.py`, `tcp_cx.py`
- [meshcore-dev/meshcore-cli](https://github.com/meshcore-dev/meshcore-cli)
- [Companion Protocol — MeshCore Docs](https://docs.meshcore.io/companion_protocol/)
- [CLI Commands — MeshCore Docs](https://docs.meshcore.io/cli_commands/)
- [Repeater commands — MeshCore Ninja](https://meshcore.ninja/repeater-commands/)
