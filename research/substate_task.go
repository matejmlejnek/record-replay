package research

import (
	"fmt"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shirou/gopsutil/cpu"
	cli "github.com/urfave/cli/v2"
)

var (
	WorkersFlag = &cli.IntFlag{
		Name:  "workers",
		Usage: "Number of worker threads (goroutines), 0 for current CPU physical cores",
		Value: 4,
	}
	SkipTransferTxsFlag = &cli.BoolFlag{
		Name:  "skip-transfer-txs",
		Usage: "Skip executing transactions that only transfer ETH",
	}
	SkipCallTxsFlag = &cli.BoolFlag{
		Name:  "skip-call-txs",
		Usage: "Skip executing CALL transactions to accounts with contract bytecode",
	}
	SkipCreateTxsFlag = &cli.BoolFlag{
		Name:  "skip-create-txs",
		Usage: "Skip executing CREATE transactions",
	}
	BlockSegmentFlag = &cli.StringFlag{
		Name:     "block-segment",
		Usage:    "Single block segment (e.g. 1001, 1_001, 1_001-2_000, 1-2k, 1-2M)",
		Required: true,
	}
	BlockSegmentListFlag = &cli.StringFlag{
		Name:     "block-segment-list",
		Usage:    "One or more block segments, e.g. '0-1M,1000-1100k,1100001,1_100_002-1_101_000'",
		Required: true,
	}
)

type BlockSegment struct {
	First, Last uint64
}

func NewBlockSegment(first, last uint64) *BlockSegment {
	return &BlockSegment{First: first, Last: last}
}

func ParseBlockSegment(s string) (*BlockSegment, error) {
	var err error
	// <first>: first block number
	// <last>: optional, last block number
	// <siunit>: optinal, k for 1000, M for 1000000
	re := regexp.MustCompile(`^(?P<first>[0-9][0-9_]*)((-|~)(?P<last>[0-9][0-9_]*)(?P<siunit>[kM]?))?$`)
	seg := &BlockSegment{}
	if !re.MatchString(s) {
		return nil, fmt.Errorf("invalid block segment string: %q", s)
	}
	matches := re.FindStringSubmatch(s)
	first := strings.ReplaceAll(matches[re.SubexpIndex("first")], "_", "")
	seg.First, err = strconv.ParseUint(first, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid block segment first: %s", err)
	}
	last := strings.ReplaceAll(matches[re.SubexpIndex("last")], "_", "")
	if len(last) == 0 {
		seg.Last = seg.First
	} else {
		seg.Last, err = strconv.ParseUint(last, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid block segment last: %s", err)
		}
	}
	siunit := matches[re.SubexpIndex("siunit")]
	switch siunit {
	case "k":
		seg.First = seg.First*1_000 + 1
		seg.Last = seg.Last * 1_000
	case "M":
		seg.First = seg.First*1_000_000 + 1
		seg.Last = seg.Last * 1_000_000
	}
	if seg.First > seg.Last {
		return nil, fmt.Errorf("block segment first is larger than last: %v-%v", seg.First, seg.Last)
	}
	return seg, nil
}

type BlockSegmentList = []*BlockSegment

func ParseBlockSegmentList(s string) (BlockSegmentList, error) {
	var err error

	lxs := strings.Split(s, ",")
	br := make(BlockSegmentList, len(lxs))
	for i, lx := range lxs {
		br[i], err = ParseBlockSegment(lx)
		if err != nil {
			return nil, err
		}
	}

	return br, nil
}

type SubstateTaskFunc func(block uint64, tx int, substate *Substate, taskPool *SubstateTaskPool) error

type SubstateTaskConfig struct {
	Workers int

	SkipTransferTxs bool
	SkipCallTxs     bool
	SkipCreateTxs   bool
}

func NewSubstateTaskConfigCli(ctx *cli.Context) *SubstateTaskConfig {
	return &SubstateTaskConfig{
		Workers: ctx.Int(WorkersFlag.Name),

		SkipTransferTxs: ctx.Bool(SkipTransferTxsFlag.Name),
		SkipCallTxs:     ctx.Bool(SkipCallTxsFlag.Name),
		SkipCreateTxs:   ctx.Bool(SkipCreateTxsFlag.Name),
	}
}

type SubstateTaskPool struct {
	Name     string
	TaskFunc SubstateTaskFunc
	Config   *SubstateTaskConfig

	DB *SubstateDB
}

func NewSubstateTaskPool(name string, taskFunc SubstateTaskFunc, config *SubstateTaskConfig) *SubstateTaskPool {
	return &SubstateTaskPool{
		Name:     name,
		TaskFunc: taskFunc,
		Config:   config,

		DB: staticSubstateDB,
	}
}

func NewSubstateTaskPoolCli(name string, taskFunc SubstateTaskFunc, ctx *cli.Context) *SubstateTaskPool {
	return &SubstateTaskPool{
		Name:     name,
		TaskFunc: taskFunc,
		Config:   NewSubstateTaskConfigCli(ctx),

		DB: staticSubstateDB,
	}
}

// NumWorkers calculates number of workers especially when --workers=0
func (pool *SubstateTaskPool) NumWorkers() int {
	// return pool.Workers if it is positive integer
	if pool.Config.Workers > 0 {
		return pool.Config.Workers
	}

	// try to return number of physical cores
	cores, err := cpu.Counts(false)
	if err == nil {
		return cores
	}

	// return number of logical cores
	return runtime.NumCPU()
}

// ExecuteBlock function iterates on substates of a given block call TaskFunc
func (pool *SubstateTaskPool) ExecuteBlock(block uint64) (numTx int64, err error) {
	for tx, substate := range pool.DB.GetBlockSubstates(block) {
		alloc := substate.InputAlloc
		msg := substate.Message

		to := msg.To
		if pool.Config.SkipTransferTxs && to != nil {
			// skip regular transactions (ETH transfer)
			if account, exist := alloc[*to]; !exist || len(account.Code) == 0 {
				continue
			}
		}
		if pool.Config.SkipCallTxs && to != nil {
			// skip CALL trasnactions with contract bytecode
			if account, exist := alloc[*to]; exist && len(account.Code) > 0 {
				continue
			}
		}
		if pool.Config.SkipCreateTxs && to == nil {
			// skip CREATE transactions
			continue
		}

		err = pool.TaskFunc(block, tx, substate, pool)
		if err != nil {
			return numTx, fmt.Errorf("%s: %v_%v: %v", pool.Name, block, tx, err)
		}

		numTx++
	}

	return numTx, nil
}

// Execute function spawns worker goroutines and schedule tasks.
func (pool *SubstateTaskPool) ExecuteSegment(segment *BlockSegment) error {
	start := time.Now()

	var totalNumBlock, totalNumTx int64
	defer func() {
		duration := time.Since(start) + 1*time.Nanosecond
		sec := duration.Seconds()

		nb, nt := atomic.LoadInt64(&totalNumBlock), atomic.LoadInt64(&totalNumTx)
		blkPerSec := float64(nb) / sec
		txPerSec := float64(nt) / sec
		fmt.Printf("%s: block segment = %v %v\n", pool.Name, segment.First, segment.Last)
		fmt.Printf("%s: total #block = %v\n", pool.Name, nb)
		fmt.Printf("%s: total #tx    = %v\n", pool.Name, nt)
		fmt.Printf("%s: %.2f blk/s, %.2f tx/s\n", pool.Name, blkPerSec, txPerSec)
		fmt.Printf("%s done in %v\n", pool.Name, duration.Round(1*time.Millisecond))
	}()

	numWorkers := pool.NumWorkers()
	// numProcs = numWorkers + work producer (1) + main thread (1)
	numProcs := numWorkers + 2
	if goMaxProcs := runtime.GOMAXPROCS(0); goMaxProcs < numProcs {
		runtime.GOMAXPROCS(numProcs)
	}

	fmt.Printf("%s: block segment = %v-%v\n", pool.Name, segment.First, segment.Last)
	fmt.Printf("%s: workers = %v\n", pool.Name, numWorkers)

	workChan := make(chan uint64, numWorkers*1000)
	doneChan := make(chan interface{}, numWorkers*1000)
	stopChan := make(chan struct{}, numWorkers)
	wg := sync.WaitGroup{}
	defer func() {
		// stop all workers
		for i := 0; i < numWorkers; i++ {
			stopChan <- struct{}{}
		}
		// stop work producer (1)
		stopChan <- struct{}{}

		wg.Wait()
		close(workChan)
		close(doneChan)
	}()
	// dynamically schedule one block per worker
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		// worker goroutine
		go func() {
			defer wg.Done()

			for {
				select {

				case block := <-workChan:
					nt, err := pool.ExecuteBlock(block)
					atomic.AddInt64(&totalNumTx, nt)
					atomic.AddInt64(&totalNumBlock, 1)
					if err != nil {
						doneChan <- err
					} else {
						doneChan <- block
					}

				case <-stopChan:
					return

				}
			}
		}()
	}

	// wait until all workers finish all tasks
	wg.Add(1)
	go func() {
		defer wg.Done()

		for block := segment.First; block <= segment.Last; block++ {
			select {

			case workChan <- block:
				continue

			case <-stopChan:
				return

			}
		}
	}()

	// Count finished blocks in order and report execution speed
	var lastSec float64
	var lastNumBlock, lastNumTx int64
	waitMap := make(map[uint64]struct{})
	for block := segment.First; block <= segment.Last; {

		// Count finshed blocks from waitMap in order
		if _, ok := waitMap[block]; ok {
			delete(waitMap, block)

			block++
			continue
		}

		duration := time.Since(start) + 1*time.Nanosecond
		sec := duration.Seconds()
		if block == segment.Last ||
			(block%10000 == 0 && sec > lastSec+5) ||
			(block%1000 == 0 && sec > lastSec+10) ||
			(block%100 == 0 && sec > lastSec+20) ||
			(block%10 == 0 && sec > lastSec+40) ||
			(sec > lastSec+60) {
			nb, nt := atomic.LoadInt64(&totalNumBlock), atomic.LoadInt64(&totalNumTx)
			blkPerSec := float64(nb-lastNumBlock) / (sec - lastSec)
			txPerSec := float64(nt-lastNumTx) / (sec - lastSec)
			fmt.Printf("%s: elapsed time: %v, number = %v\n", pool.Name, duration.Round(1*time.Millisecond), block)
			fmt.Printf("%s: %.2f blk/s, %.2f tx/s\n", pool.Name, blkPerSec, txPerSec)

			lastSec, lastNumBlock, lastNumTx = sec, nb, nt
		}

		data := <-doneChan
		switch t := data.(type) {

		case uint64:
			waitMap[data.(uint64)] = struct{}{}

		case error:
			err := data.(error)
			return err

		default:
			panic(fmt.Errorf("%s: unknown type %T value from doneChan", pool.Name, t))

		}
	}

	return nil
}
