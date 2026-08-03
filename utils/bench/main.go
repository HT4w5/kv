// LLM usage: the bench utility is generated with deepseek-v4-pro.
package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"math"
	"math/rand"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	units "github.com/docker/go-units"
	flag "github.com/spf13/pflag"
	"golang.org/x/time/rate"

	kv "github.com/HT4w5/kv"
)

// Config holds all benchmark parameters, parsed from CLI flags.
type Config struct {
	Duration       time.Duration
	Concurrency    int
	CacheSize      string
	KeySpaceSize   int
	ValueSize      int
	SampleInterval time.Duration
	Output         string
	CPUProfile     string
	MemProfile     string
	HotKeySkew     float64
	LoadPattern    string
	MixAReadPct    int
	MixADeletePct  int
	MixARate       int
	MixBReadPct    int
	MixBDeletePct  int
	MixBRate       int
	SinePeriod     time.Duration
	BurstDuration  time.Duration
	SilentDuration time.Duration
}

func parseFlags() Config {
	var cfg Config

	flag.DurationVarP(&cfg.Duration, "duration", "d", 10*time.Minute, "test run duration")
	flag.IntVarP(&cfg.Concurrency, "concurrency", "c", 16, "number of worker goroutines")
	flag.StringVarP(&cfg.CacheSize, "cache-size", "s", "32MiB", "total cache capacity (e.g. 64KiB, 32MiB, 1GiB)")
	flag.IntVarP(&cfg.KeySpaceSize, "key-space", "k", 500_000, "number of distinct keys in the pool")
	flag.IntVarP(&cfg.ValueSize, "value-size", "v", 1024, "byte size of values")
	flag.DurationVarP(&cfg.SampleInterval, "sample-interval", "i", 5*time.Second, "metrics recording interval")
	flag.StringVarP(&cfg.Output, "output", "o", "cache_perf_log.csv", "CSV output file path")
	flag.StringVar(&cfg.CPUProfile, "cpu-profile", "", "write CPU profile to file")
	flag.StringVar(&cfg.MemProfile, "mem-profile", "", "write heap profile to file")
	flag.Float64VarP(&cfg.HotKeySkew, "hot-key-skew", "z", 10.0, "Zipfian skew factor (higher = more concentrated on hot keys)")
	flag.StringVar(&cfg.LoadPattern, "load-pattern", "full", "load pattern: full, stable, sine, burst")
	flag.IntVar(&cfg.MixAReadPct, "mix-a-read-pct", 85, "Mix A: read percentage")
	flag.IntVar(&cfg.MixADeletePct, "mix-a-delete-pct", 5, "Mix A: delete percentage (set% = 100 - read - delete)")
	flag.IntVar(&cfg.MixARate, "mix-a-rate", 1_000_000, "Mix A: target ops/sec")
	flag.IntVar(&cfg.MixBReadPct, "mix-b-read-pct", 0, "Mix B: read percentage")
	flag.IntVar(&cfg.MixBDeletePct, "mix-b-delete-pct", 0, "Mix B: delete percentage (set% = 100 - read - delete)")
	flag.IntVar(&cfg.MixBRate, "mix-b-rate", 1_000_000, "Mix B: target ops/sec (0 = truly silent in burst)")
	flag.DurationVar(&cfg.SinePeriod, "sine-period", 60*time.Second, "period of sine wave")
	flag.DurationVar(&cfg.BurstDuration, "burst-duration", 10*time.Second, "length of burst phase (uses Mix A)")
	flag.DurationVar(&cfg.SilentDuration, "silent-duration", 10*time.Second, "length of silent phase (uses Mix B)")

	flag.Parse()
	return cfg
}

func (c Config) String() string {
	mixAWrite := 100 - c.MixAReadPct - c.MixADeletePct
	mixBWrite := 100 - c.MixBReadPct - c.MixBDeletePct
	return fmt.Sprintf(
		"Duration:       %v\n"+
			"Concurrency:    %d\n"+
			"Cache Size:     %s\n"+
			"Key Space:      %d\n"+
			"Value Size:     %d bytes\n"+
			"Mix A:          %d%% read / %d%% delete / %d%% set @ %d ops/s\n"+
			"Mix B:          %d%% read / %d%% delete / %d%% set @ %d ops/s\n"+
			"Sample Interval:%v\n"+
			"Hot-Key Skew:   %.1f\n"+
			"Load Pattern:   %s\n"+
			"Output:         %s",
		c.Duration,
		c.Concurrency,
		c.CacheSize,
		c.KeySpaceSize,
		c.ValueSize,
		c.MixAReadPct, c.MixADeletePct, mixAWrite, c.MixARate,
		c.MixBReadPct, c.MixBDeletePct, mixBWrite, c.MixBRate,
		c.SampleInterval,
		c.HotKeySkew,
		c.LoadPattern,
		c.Output,
	)
}

func main() {
	cfg := parseFlags()

	fmt.Println("=== kv.Cache Long-Term Benchmark ===")
	fmt.Println(cfg)
	fmt.Println()

	cacheSizeBytes, err := units.RAMInBytes(cfg.CacheSize)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid cache-size %q: %v\n", cfg.CacheSize, err)
		os.Exit(1)
	}

	cache := kv.New(int(cacheSizeBytes))

	// Start CPU profiling if requested.
	if cfg.CPUProfile != "" {
		f, err := os.Create(cfg.CPUProfile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "cannot create CPU profile: %v\n", err)
			os.Exit(1)
		}
		defer f.Close()
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
		fmt.Printf("CPU profile: %s\n", cfg.CPUProfile)
	}

	runBenchmark(cache, cfg)

	// Write heap profile if requested.
	if cfg.MemProfile != "" {
		f, err := os.Create(cfg.MemProfile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "cannot create memory profile: %v\n", err)
			os.Exit(1)
		}
		defer f.Close()
		runtime.GC()
		if err := pprof.WriteHeapProfile(f); err != nil {
			fmt.Fprintf(os.Stderr, "cannot write memory profile: %v\n", err)
		}
		fmt.Printf("Heap profile:  %s\n", cfg.MemProfile)
	}

	cache.Reset()
	fmt.Println("Benchmark complete.")
}

// --- Load Controllers ---

// Mix defines an operation distribution and rate.
type Mix struct {
	ReadPct   float64
	DeletePct float64
	Rate      float64
}

// OpType is the operation the worker should perform.
type OpType int

const (
	OpRead OpType = iota
	OpDelete
	OpSet
)

// lerp linearly interpolates between a and b by t (0=a, 1=b).
func lerp(a, b, t float64) float64 {
	return a + (b-a)*t
}

// LoadController throttles worker goroutines and selects operations.
type LoadController interface {
	Acquire(ctx context.Context)
	OpType(elapsed time.Duration, rc int) OpType
}

// fullController applies no throttling — workers run at full speed.
type fullController struct {
	mix Mix
}

func (c *fullController) Acquire(ctx context.Context) {}

func (c *fullController) OpType(_ time.Duration, rc int) OpType {
	if rc < int(c.mix.ReadPct) {
		return OpRead
	}
	if rc < int(c.mix.ReadPct+c.mix.DeletePct) {
		return OpDelete
	}
	return OpSet
}

// stableController maintains a constant rate via rate.Limiter.
type stableController struct {
	limiter *rate.Limiter
	mix     Mix
}

func (c *stableController) Acquire(ctx context.Context) {
	c.limiter.Wait(ctx) //nolint:errcheck
}

func (c *stableController) OpType(_ time.Duration, rc int) OpType {
	if rc < int(c.mix.ReadPct) {
		return OpRead
	}
	if rc < int(c.mix.ReadPct+c.mix.DeletePct) {
		return OpDelete
	}
	return OpSet
}

// sineController oscillates rate and mix blend sinusoidally between MixA and MixB.
type sineController struct {
	limiter *rate.Limiter
	mixA    Mix
	mixB    Mix
	period  time.Duration
	start   time.Time
}

func (c *sineController) Acquire(ctx context.Context) {
	c.limiter.Wait(ctx) //nolint:errcheck
}

func (c *sineController) OpType(elapsed time.Duration, rc int) OpType {
	// blend = (sin(2π·t/T) + 1) / 2, maps [0,1,0] over one period
	t := elapsed.Seconds()
	blend := (math.Sin(2*math.Pi*t/c.period.Seconds()) + 1) / 2
	rp := lerp(c.mixB.ReadPct, c.mixA.ReadPct, blend)
	dp := lerp(c.mixB.DeletePct, c.mixA.DeletePct, blend)
	if rc < int(rp) {
		return OpRead
	}
	if rc < int(rp+dp) {
		return OpDelete
	}
	return OpSet
}

func (c *sineController) run(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			elapsed := time.Since(c.start).Seconds()
			blend := (math.Sin(2*math.Pi*elapsed/c.period.Seconds()) + 1) / 2
			r := lerp(c.mixB.Rate, c.mixA.Rate, blend)
			c.limiter.SetLimit(rate.Limit(r))
			c.limiter.SetBurst(int(r))
		}
	}
}

// burstController alternates between MixA (burst) and MixB (silent) phases.
// If MixB.Rate is 0, the gate closes and workers spin-wait; otherwise the
// gate stays open and the limiter throttles to MixB.Rate.
type burstController struct {
	gate      atomic.Bool
	limiter   *rate.Limiter
	mixA      Mix
	mixB      Mix
	burstDur  time.Duration
	silentDur time.Duration
	start     time.Time
}

func (c *burstController) Acquire(ctx context.Context) {
	for !c.gate.Load() {
		select {
		case <-ctx.Done():
			return
		case <-time.After(10 * time.Millisecond):
		}
	}
	c.limiter.Wait(ctx) //nolint:errcheck
}

func (c *burstController) OpType(elapsed time.Duration, rc int) OpType {
	cycleDur := c.burstDur + c.silentDur
	mix := &c.mixB
	if elapsed%cycleDur < c.burstDur {
		mix = &c.mixA
	}
	if rc < int(mix.ReadPct) {
		return OpRead
	}
	if rc < int(mix.ReadPct+mix.DeletePct) {
		return OpDelete
	}
	return OpSet
}

func (c *burstController) run(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	cycleDur := c.burstDur + c.silentDur
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			phase := time.Since(c.start) % cycleDur
			if phase < c.burstDur {
				c.gate.Store(true)
				c.limiter.SetLimit(rate.Limit(c.mixA.Rate))
				c.limiter.SetBurst(int(c.mixA.Rate))
			} else if c.mixB.Rate > 0 {
				c.gate.Store(true)
				c.limiter.SetLimit(rate.Limit(c.mixB.Rate))
				c.limiter.SetBurst(int(c.mixB.Rate))
			} else {
				c.gate.Store(false)
			}
		}
	}
}

func newLoadController(cfg Config, startTime time.Time, ctx context.Context) LoadController {
	mixA := Mix{
		ReadPct:   float64(cfg.MixAReadPct),
		DeletePct: float64(cfg.MixADeletePct),
		Rate:      float64(cfg.MixARate),
	}
	mixB := Mix{
		ReadPct:   float64(cfg.MixBReadPct),
		DeletePct: float64(cfg.MixBDeletePct),
		Rate:      float64(cfg.MixBRate),
	}
	switch cfg.LoadPattern {
	case "stable":
		limiter := rate.NewLimiter(rate.Limit(mixA.Rate), int(mixA.Rate))
		return &stableController{limiter: limiter, mix: mixA}
	case "sine":
		limiter := rate.NewLimiter(rate.Limit(mixA.Rate), int(mixA.Rate))
		sc := &sineController{
			limiter: limiter,
			mixA:    mixA,
			mixB:    mixB,
			period:  cfg.SinePeriod,
			start:   startTime,
		}
		go sc.run(ctx)
		return sc
	case "burst":
		limiter := rate.NewLimiter(rate.Limit(mixA.Rate), int(mixA.Rate))
		bc := &burstController{
			limiter:   limiter,
			mixA:      mixA,
			mixB:      mixB,
			burstDur:  cfg.BurstDuration,
			silentDur: cfg.SilentDuration,
			start:     startTime,
		}
		bc.gate.Store(true) // start in burst phase
		go bc.run(ctx)
		return bc
	default:
		if cfg.LoadPattern != "full" {
			fmt.Fprintf(os.Stderr, "warning: unknown load-pattern %q, using full\n", cfg.LoadPattern)
		}
		return &fullController{mix: mixA}
	}
}

func runBenchmark(cache *kv.Cache, cfg Config) {
	fmt.Printf("Starting run: %v duration across %d goroutines...\n\n", cfg.Duration, cfg.Concurrency)

	// Open CSV output file.
	file, err := os.Create(cfg.Output)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	header := []string{
		"Elapsed_Sec", "Ops_Per_Sec",
		"Heap", "Sys", "NumGC", "PauseTotal_Ns",
		"Cache_Gets", "Cache_Sets", "Cache_Misses",
		"Cache_Collisions", "Cache_Vacuums", "Cache_Dels", "Cache_Deallocs",
		"Cache_Allocs", "Cache_Allocated",
	}
	if err := writer.Write(header); err != nil {
		panic(err)
	}
	writer.Flush()

	// Pre-generate key pool and value template.
	keys := make([][]byte, cfg.KeySpaceSize)
	for i := 0; i < cfg.KeySpaceSize; i++ {
		keys[i] = []byte(fmt.Sprintf("key_%d", i))
	}
	payload := make([]byte, cfg.ValueSize)

	var totalOps uint64
	var totalDels uint64
	stopCh := make(chan struct{})
	var wg sync.WaitGroup

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// --- Workers ---
	startTime := time.Now()
	controller := newLoadController(cfg, startTime, ctx)

	for i := 0; i < cfg.Concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))
			buf := make([]byte, 0, cfg.ValueSize)

			for {
				select {
				case <-stopCh:
					return
				default:
					// Zipfian key distribution: ExpFloat64/divisor generates
					// small indices much more frequently than large ones.
					keyIdx := int(r.ExpFloat64()*float64(cfg.KeySpaceSize)/cfg.HotKeySkew) % cfg.KeySpaceSize
					if keyIdx < 0 {
						keyIdx = 0
					}
					key := keys[keyIdx]
					controller.Acquire(ctx)
					switch controller.OpType(time.Since(startTime), r.Intn(100)) {
					case OpRead:
						cache.Get(buf, key)
					case OpDelete:
						cache.Del(key)
						atomic.AddUint64(&totalDels, 1)
					case OpSet:
						cache.Set(key, payload)
					}
					atomic.AddUint64(&totalOps, 1)
				}
			}
		}(i)
	}

	// --- Telemetry ---
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(cfg.SampleInterval)
		defer ticker.Stop()

		var lastOps uint64
		var lastDels uint64
		var prevStats kv.Stats
		var prevPause time.Duration

		for {
			select {
			case <-stopCh:
				return
			case t := <-ticker.C:
				currentOps := atomic.LoadUint64(&totalOps)
				currentDels := atomic.LoadUint64(&totalDels)
				opsDelta := currentOps - lastOps
				delsDelta := currentDels - lastDels
				lastOps = currentOps
				lastDels = currentDels

				opsPerSec := float64(opsDelta) / cfg.SampleInterval.Seconds()

				// Go runtime memory.
				var m runtime.MemStats
				runtime.ReadMemStats(&m)

				elapsed := int(t.Sub(startTime).Seconds())

				// Cache-level stats (deltas since last sample).
				var curStats kv.Stats
				cache.LoadStats(&curStats)
				getsDelta := curStats.Gets - prevStats.Gets
				pauseDelta := time.Duration(m.PauseTotalNs) - prevPause
				setsDelta := curStats.Sets - prevStats.Sets
				missesDelta := curStats.Misses - prevStats.Misses
				collisionsDelta := curStats.Collisions - prevStats.Collisions
				vacuumsDelta := curStats.Vacuums - prevStats.Vacuums
				deallocsDelta := curStats.Deallocations - prevStats.Deallocations
				allocsDelta := curStats.Allocations - prevStats.Allocations
				prevStats = curStats
				prevPause = time.Duration(m.PauseTotalNs)

				// Console readout.
				fmt.Printf("[%4ds] Ops/s: %8.0f | Heap: %s | Sys: %s | GC: %4d | Pause: %s | "+
					"Gets: %8d | Sets: %8d | Miss: %8d | Coll: %8d | Vacuums: %8d | Dels: %8d | Deallocs: %6d | Allocs: %6d | Allocated: %s\n",
					elapsed, opsPerSec, units.BytesSize(float64(m.HeapAlloc)), units.BytesSize(float64(m.Sys)), m.NumGC, pauseDelta.String(),
					getsDelta, setsDelta, missesDelta, collisionsDelta, vacuumsDelta,
					delsDelta, deallocsDelta, allocsDelta, units.BytesSize(float64(curStats.Allocated)))
				// Write CSV row.
				row := []string{
					strconv.Itoa(elapsed),
					fmt.Sprintf("%.0f", opsPerSec),
					fmt.Sprintf("%d", m.HeapAlloc),
					fmt.Sprintf("%d", m.Sys),
					strconv.FormatUint(uint64(m.NumGC), 10),
					fmt.Sprintf("%d", m.PauseTotalNs),
					strconv.FormatInt(getsDelta, 10),
					strconv.FormatInt(setsDelta, 10),
					strconv.FormatInt(missesDelta, 10),
					strconv.FormatInt(collisionsDelta, 10),
					strconv.FormatInt(vacuumsDelta, 10),
					strconv.FormatUint(delsDelta, 10),
					strconv.FormatInt(deallocsDelta, 10),
					strconv.FormatInt(allocsDelta, 10),
					fmt.Sprintf("%d", curStats.Allocated),
				}
				if err := writer.Write(row); err != nil {
					panic(err)
				}
				writer.Flush()
			}
		}
	}()

	// Run for the configured duration.
	time.Sleep(cfg.Duration)
	cancel()
	close(stopCh)
	wg.Wait()

	// Final summary.
	elapsed := time.Since(startTime)
	finalOps := atomic.LoadUint64(&totalOps)
	finalDels := atomic.LoadUint64(&totalDels)
	var finalStats kv.Stats
	cache.LoadStats(&finalStats)

	fmt.Println()
	fmt.Println("=== Final Summary ===")
	fmt.Printf("Elapsed:        %v\n", elapsed.Round(time.Second))
	fmt.Printf("Total ops:      %d\n", finalOps)
	fmt.Printf("Avg ops/s:      %.0f\n", float64(finalOps)/elapsed.Seconds())
	fmt.Printf("Cache Gets:     %d\n", finalStats.Gets)
	fmt.Printf("Cache Sets:     %d\n", finalStats.Sets)
	fmt.Printf("Cache Dels:     %d\n", finalDels)
	fmt.Printf("Cache Misses:   %d\n", finalStats.Misses)
	fmt.Printf("Collisions:     %d\n", finalStats.Collisions)
	fmt.Printf("Vacuums:        %d\n", finalStats.Vacuums)
	fmt.Printf("Allocations:    %d\n", finalStats.Allocations)
	fmt.Printf("Deallocations:  %d\n", finalStats.Deallocations)
	fmt.Printf("Allocated:      %s\n", units.BytesSize(float64(finalStats.Allocated)))
	fmt.Printf("CSV saved to:   %s\n", cfg.Output)
}
