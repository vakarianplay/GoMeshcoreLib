// Command telemetry requests telemetry from a MeshCore node through a
// companion radio and keeps retrying until the data arrives.
//
// Build:  go build -o telemetry ./examples/telemetry
// Run:    ./telemetry "MO EST 08"
//         ./telemetry -self
//
// Byte layouts, framing and LPP decoding all live in the library now; this file
// only wires flags, the retry policy and the output.
package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"

	// The alias keeps the code working whether the library declares
	// "package meshcore" or "package GoMeshcoreLib". Only the import PATH
	// needs to match your go.mod.
	meshcore "GoMeshcoreLib"
)

// ---------------------------------------------------------------------------
// Flags
// ---------------------------------------------------------------------------

type options struct {
	host       string
	port       int
	serialPort string
	baud       int

	self bool

	timeout   float64 // seconds; 0 = use the firmware suggested_timeout
	maxRounds int     // outer retry rounds; 0 = unlimited
	retry     float64 // initial backoff, seconds
	maxRetry  float64 // backoff ceiling, seconds
	humidity  int     // LPP humidity field width: 1 or 2

	direct int // inner tries over the known path
	flood  int // inner tries over flood, after ResetPath

	asJSON  bool
	verbose bool
}

func parseFlags() (options, string) {
	var o options
	flag.StringVar(&o.host, "host", "mesh.lan", "companion host (WiFi/TCP)")
	flag.IntVar(&o.port, "port", 5000, "companion TCP port")
	flag.StringVar(&o.serialPort, "serial", "", "COM/serial port (used instead of TCP)")
	flag.IntVar(&o.baud, "baud", 115200, "serial baud rate")

	flag.BoolVar(&o.self, "self", false,
		     "request the companion's own telemetry (no radio traffic)")

	flag.Float64Var(&o.timeout, "timeout", 0,
			"per-attempt wait, seconds (0 = firmware suggested_timeout)")
	flag.IntVar(&o.maxRounds, "attempts", 0, "max outer rounds (0 = unlimited)")
	flag.Float64Var(&o.retry, "retry", 3, "initial backoff between rounds, seconds")
	flag.Float64Var(&o.maxRetry, "max-retry", 60, "backoff ceiling, seconds")
	flag.IntVar(&o.humidity, "humidity-size", 1,
		    "LPP humidity field width: 1 (0.5% step) or 2 (0.1% step)")

	flag.IntVar(&o.direct, "direct", 5,
		    "library tries over the last known path, per round (must be >= 1)")
	flag.IntVar(&o.flood, "flood", 5,
		    "library tries over flood after a path reset, per round")

	flag.BoolVar(&o.asJSON, "json", false, "print the result as JSON")
	flag.BoolVar(&o.verbose, "v", false, "dump every frame to stderr")
	flag.Parse()

	return o, flag.Arg(0)
}

func usage(msg string) {
	if msg != "" {
		fmt.Fprintln(os.Stderr, "ERROR:", msg)
	}
	fmt.Fprintln(os.Stderr, "Usage: telemetry [flags] <node-name>")
	fmt.Fprintln(os.Stderr, "   or: telemetry -self")
	flag.PrintDefaults()
	os.Exit(2)
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

func main() {
	o, target := parseFlags()

	if !o.self && target == "" {
		usage("a node name is required unless -self is given")
	}
	if o.humidity != 1 && o.humidity != 2 {
		usage("-humidity-size must be 1 or 2")
	}
	if o.direct < 1 {
		// The library replaces 0 with its own default of 5, so 0 is not
		// "disable" here. Fail loudly instead of silently retrying 5 times.
		usage("-direct must be >= 1")
	}
	if o.flood < 0 {
		usage("-flood must be >= 0")
	}

	// Applies to the LPP decoder library-wide.
	meshcore.HumidityFieldSize = o.humidity

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	// In JSON mode nothing may pollute stdout, so library logging is silenced
	// the same way the standalone version silenced its own logf.
	logf := func(string, ...any) {}
	if !o.asJSON {
		logf = func(format string, a ...any) {
			fmt.Fprintf(os.Stderr, format+"\n", a...)
		}
	}

	cfg := meshcore.DefaultConfig()
	cfg.AppName = "telemetry-example"
	cfg.Verbose = o.verbose
	cfg.Logf = logf
	cfg.DirectAttempts = o.direct
	cfg.FloodAttempts = o.flood
	cfg.RetryDelay = 500 * time.Millisecond

	// A fixed -timeout pins both clamp bounds so clampWait returns exactly it,
	// ignoring the firmware suggested_timeout.
	if o.timeout > 0 {
		w := time.Duration(o.timeout * float64(time.Second))
		cfg.MinWait = w
		cfg.MaxWait = w
	}

	c, err := dial(cfg, o)
	if err != nil {
		fmt.Fprintln(os.Stderr, "ERROR:", err)
		os.Exit(1)
	}
	defer c.Close()

	self := c.Self()
	dev := c.Device()
	logf("Connected: node=%q model=%q firmware=%s", self.Name, dev.Model, dev.Version)

	// pubKey-less request in -self mode returns the companion's own telemetry
	// immediately, without going over the air.
	var ct *meshcore.Contact
	name := "self"
	if !o.self {
		ct, err = c.ResolveContact(ctx, target)
		if err != nil {
			reportContactError(c, err)
			os.Exit(1)
		}
		if ct.Type != meshcore.AdvTypeRepeater {
			logf("WARNING: %q has type=%d (2=repeater, 3=room, 4=sensor)",
			     ct.Name, ct.Type)
		}
		name = ct.Name
		logf("Found node: %s pubkey=%s", name, hex.EncodeToString(ct.PublicKey[:]))
	}

	backoff := time.Duration(o.retry * float64(time.Second))
	maxBackoff := time.Duration(o.maxRetry * float64(time.Second))

	for round := 1; o.maxRounds == 0 || round <= o.maxRounds; round++ {
		if interrupted(ctx) {
			exitInterrupted()
		}
		logf("Round %d: requesting telemetry…", round)

		t, err := request(ctx, c, ct, o.self)
		if err != nil {
			logf("  not received: %v", err)
		} else {
			printResult(t, name, round, o.asJSON)
			return
		}

		if !sleepCtx(ctx, backoff) {
			exitInterrupted()
		}
		backoff = min(backoff*2, maxBackoff)
	}

	fmt.Fprintln(os.Stderr, "ERROR: telemetry not received within the allowed attempts")
	os.Exit(1)
}

func dial(cfg meshcore.Config, o options) (*meshcore.Client, error) {
	if o.serialPort != "" {
		return meshcore.DialSerialClient(o.serialPort, o.baud, cfg)
	}
	return meshcore.DialTCPClient(o.host, o.port, cfg)
}

// request performs one full round: DirectAttempts over the known path, then
// FloodAttempts over flood. The retry policy inside the library is what makes
// this a round rather than a single send.
func request(ctx context.Context, c *meshcore.Client, ct *meshcore.Contact,
	     self bool) (*meshcore.Telemetry, error) {
		     if self {
			     return c.RequestSelfTelemetry(ctx)
		     }
		     return c.RequestTelemetryFrom(ctx, ct)
	     }

	     // ---------------------------------------------------------------------------
	     // Output
	     // ---------------------------------------------------------------------------

	     func printResult(t *meshcore.Telemetry, name string, round int, asJSON bool) {
		     key := hex.EncodeToString(t.From[:])

		     if asJSON {
			     out := struct {
				     Node          string              `json:"node"`
				     PubKey        string              `json:"pubkey_prefix"`
				     Attempts      int                 `json:"attempts"`
				     InnerAttempts int                 `json:"inner_attempts"`
				     Route         string              `json:"route,omitempty"`
				     At            time.Time           `json:"at"`
				     Fields        []meshcore.LPPField `json:"fields"`
				     RawHex        string              `json:"raw_hex"`
				     Unparsed      string              `json:"unparsed_hex,omitempty"`
			     }{
				     Node:          name,
				     PubKey:        key,
				     Attempts:      round,
				     InnerAttempts: t.Attempts,
				     Route:         t.Route,
				     At:            time.Now(),
				     Fields:        t.LPP.Fields,
				     RawHex:        t.LPP.Raw,
				     Unparsed:      t.LPP.Tail,
			     }
			     enc := json.NewEncoder(os.Stdout)
			     enc.SetIndent("", "  ")
			     _ = enc.Encode(out)
			     return
		     }

		     fmt.Printf("Telemetry %s (key %s), round %d\n", name, key, round)
		     if t.Route != "" {
			     fmt.Printf("  route: %s, send attempts: %d\n", t.Route, t.Attempts)
		     }
		     if len(t.LPP.Fields) == 0 {
			     fmt.Println("  no fields, raw:", t.LPP.Raw)
		     }
		     for _, f := range t.LPP.Fields {
			     if f.Unit != "" {
				     fmt.Printf("  ch%-3d %-26s %10.2f %s\n",
						f.Channel, f.Name, f.Value, f.Unit)
				     continue
			     }
			     fmt.Printf("  ch%-3d %-26s %10.0f\n", f.Channel, f.Name, f.Value)
		     }
		     if t.LPP.Tail != "" {
			     fmt.Println("  unparsed:", t.LPP.Tail)
		     }
		     fmt.Println("  raw:", t.LPP.Raw)
	     }

	     // reportContactError prints the error and, for a missing contact, the list of
	     // plausible node names to choose from.
	     func reportContactError(c *meshcore.Client, err error) {
		     fmt.Fprintln(os.Stderr, "ERROR:", err)
		     if !errors.Is(err, meshcore.ErrNotFound) {
			     return
		     }
		     var names []string
		     for _, ct := range c.Contacts() {
			     if ct.IsRepeater() || ct.IsRoom() || ct.IsSensor() {
				     names = append(names, ct.Name)
			     }
		     }
		     if len(names) == 0 {
			     fmt.Fprintln(os.Stderr, "The contact book is empty: send an advert first.")
			     return
		     }
		     fmt.Fprintln(os.Stderr, "Available nodes:")
		     for _, n := range names {
			     fmt.Fprintln(os.Stderr, "  ", n)
		     }
	     }

	     // ---------------------------------------------------------------------------
	     // Helpers
	     // ---------------------------------------------------------------------------

	     func sleepCtx(ctx context.Context, d time.Duration) bool {
		     if d <= 0 {
			     return true
		     }
		     t := time.NewTimer(d)
		     defer t.Stop()
		     select {
			     case <-ctx.Done():
				     return false
			     case <-t.C:
				     return true
		     }
	     }

	     func interrupted(ctx context.Context) bool { return ctx.Err() != nil }

	     func exitInterrupted() {
		     fmt.Fprintln(os.Stderr, "interrupted by user")
		     os.Exit(130)
	     }
