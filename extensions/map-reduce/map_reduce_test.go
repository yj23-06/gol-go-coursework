package extensions

import (
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"

	"uk.ac.bris.cs/gameoflife/gol"
	"uk.ac.bris.cs/gameoflife/util"
)

type ReferenceEngine struct {
	width, height int
	grid          [][]uint8
}

func NewReferenceEngine(width, height int, initial []uint8) *ReferenceEngine {
	grid := make([][]uint8, height)
	for y := 0; y < height; y++ {
		grid[y] = make([]uint8, width)
		for x := 0; x < width; x++ {
			grid[y][x] = initial[y*width+x]
		}
	}
	return &ReferenceEngine{width: width, height: height, grid: grid}
}

func (r *ReferenceEngine) Step() ([]util.Cell, [][]uint8) {
	next := make([][]uint8, r.height)
	for y := 0; y < r.height; y++ {
		next[y] = make([]uint8, r.width)
	}

	var flipped []util.Cell
	for y := 0; y < r.height; y++ {
		for x := 0; x < r.width; x++ {
			neighbors := 0
			for dy := -1; dy <= 1; dy++ {
				for dx := -1; dx <= 1; dx++ {
					if dx == 0 && dy == 0 {
						continue
					}
					nx := (x + dx + r.width) % r.width
					ny := (y + dy + r.height) % r.height
					if r.grid[ny][nx] == 255 {
						neighbors++
					}
				}
			}

			alive := r.grid[y][x] == 255
			nextAlive := false
			if alive {
				nextAlive = (neighbors == 2 || neighbors == 3)
			} else {
				nextAlive = (neighbors == 3)
			}

			if nextAlive {
				next[y][x] = 255
			} else {
				next[y][x] = 0
			}

			if alive != nextAlive {
				flipped = append(flipped, util.Cell{X: x, Y: y})
			}
		}
	}
	r.grid = next
	return flipped, r.grid
}

// -----------------------------------------------------------------
// 1. Comprehensive Stress Suite comparing Map-Reduce vs Reference Engine
// -----------------------------------------------------------------
func TestDistributorMapReduceComprehensive(t *testing.T) {
	type testCase struct {
		name    string
		width   int
		height  int
		turns   int
		threads int
		pattern string
	}

	cases := []testCase{
		{"Square_Divisible", 16, 16, 5, 4, "blinker"},
		{"NonSquare_IndivisibleRows", 12, 10, 5, 3, "glider"},
		{"VerySmall_3x3", 3, 3, 3, 2, "blinker_small"},
		{"MoreWorkersThanRows", 8, 4, 2, 8, "random"},
		{"SingleWorker", 16, 16, 5, 1, "random"},
		{"GliderCrossingToroidalEdge", 8, 8, 16, 4, "glider"},
		{"RandomDensity_Dense", 16, 16, 10, 4, "random_dense"},
		{"RandomDensity_Sparse", 16, 16, 10, 4, "random_sparse"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			initialGoroutines := runtime.NumGoroutine()

			initialWorld := make([]uint8, tc.width*tc.height)
			switch tc.pattern {
			case "blinker":
				if tc.width >= 3 && tc.height >= 3 {
					initialWorld[1*tc.width+1] = 255
					initialWorld[2*tc.width+1] = 255
					initialWorld[3*tc.width+1] = 255
				}
			case "blinker_small":
				if tc.width >= 3 && tc.height >= 3 {
					initialWorld[0*tc.width+1] = 255
					initialWorld[1*tc.width+1] = 255
					initialWorld[2*tc.width+1] = 255
				}
			case "glider":
				if tc.width >= 3 && tc.height >= 3 {
					initialWorld[0*tc.width+1] = 255
					initialWorld[1*tc.width+2] = 255
					initialWorld[2*tc.width+0] = 255
					initialWorld[2*tc.width+1] = 255
					initialWorld[2*tc.width+2] = 255
				}
			case "random_dense":
				r := rand.New(rand.NewSource(42))
				for i := range initialWorld {
					if r.Float64() < 0.7 {
						initialWorld[i] = 255
					}
				}
			case "random_sparse", "random":
				r := rand.New(rand.NewSource(42))
				for i := range initialWorld {
					if r.Float64() < 0.2 {
						initialWorld[i] = 255
					}
				}
			}

			callerWorldCopy := make([]uint8, len(initialWorld))
			copy(callerWorldCopy, initialWorld)

			ref := NewReferenceEngine(tc.width, tc.height, initialWorld)

			params := gol.Params{
				ImageWidth:  tc.width,
				ImageHeight: tc.height,
				Turns:       tc.turns,
				Threads:     tc.threads,
			}

			events := make(chan gol.Event, 5000)
			keyPresses := make(chan rune, 10)
			ioCommand := make(chan gol.IoCommand)
			ioFilename := make(chan string)
			ioInput := make(chan uint8, tc.width*tc.height)
			ioOutput := make(chan uint8, tc.width*tc.height)
			ioIdle := make(chan bool)

			channels := gol.DistributorChannels{
				Events:     events,
				KeyPresses: keyPresses,
				IoCommand:  ioCommand,
				IoFilename: ioFilename,
				IoInput:    ioInput,
				IoOutput:   ioOutput,
				IoIdle:     ioIdle,
			}

			snapshot := make([]uint8, len(initialWorld))
			copy(snapshot, initialWorld)

			ioDone := make(chan struct{})
			go func(inputSnapshot []uint8) {
				defer close(ioDone)
				for {
					select {
					case command, ok := <-ioCommand:
						if !ok {
							return
						}
						switch command {
						case gol.IoInput:
							_, ok := <-ioFilename
							if !ok {
								return
							}
							for _, cell := range inputSnapshot {
								select {
								case ioInput <- cell:
								case <-ioDone:
									return
								}
							}
						case gol.IoOutput:
							_, ok := <-ioFilename
							if !ok {
								return
							}
							for i := 0; i < tc.width*tc.height; i++ {
								select {
								case <-ioOutput:
								case <-ioDone:
									return
								}
							}
						case gol.IoCheckIdle:
							select {
							case ioIdle <- true:
							case <-ioDone:
								return
							}
						}
					case <-ioDone:
						return
					}
				}
			}(snapshot)

			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				turn := 0
				for event := range events {
					switch e := event.(type) {
					case gol.CellsFlipped:
						expectedFlips, _ := ref.Step()
						if len(e.Cells) != len(expectedFlips) {
							t.Errorf("Turn %d: expected %d flips, got %d", turn+1, len(expectedFlips), len(e.Cells))
						}
					case gol.TurnComplete:
						turn++
					case gol.FinalTurnComplete:
						return
					}
				}
			}()

			done := make(chan struct{})
			go func() {
				defer close(done)
				DistributorMapReduce(params, channels)
			}()

			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Fatalf("Timeout/Deadlock in %s", tc.name)
			}

			close(ioCommand)
			<-ioDone // Ensure mock I/O goroutine has completely exited before subtest teardown

			wg.Wait()

			for i := range initialWorld {
				if initialWorld[i] != callerWorldCopy[i] {
					t.Fatalf("Caller's starting board was modified at index %d", i)
				}
			}

			time.Sleep(50 * time.Millisecond)
			if leaks := runtime.NumGoroutine() - initialGoroutines; leaks > 2 {
				t.Errorf("Goroutine leak detected: %d goroutines remaining", leaks)
			}
		})
	}
}

// -----------------------------------------------------------------
// 2. Requirement #6: Small Channel Buffer Stress & Deadlock Test
// -----------------------------------------------------------------
func TestMapReduceSmallChannelBuffers(t *testing.T) {
	bufferSizes := []int{1, 2, 4}

	for _, bufSize := range bufferSizes {
		t.Run(fmt.Sprintf("BufferCap_%d", bufSize), func(t *testing.T) {
			params := gol.Params{
				ImageWidth:  16,
				ImageHeight: 16,
				Turns:       10,
				Threads:     4,
			}

			events := make(chan gol.Event, 2000)
			keyPresses := make(chan rune, 10)
			ioCommand := make(chan gol.IoCommand)
			ioFilename := make(chan string)
			ioInput := make(chan uint8, 256)
			ioOutput := make(chan uint8, 256)
			ioIdle := make(chan bool)

			channels := gol.DistributorChannels{
				Events:     events,
				KeyPresses: keyPresses,
				IoCommand:  ioCommand,
				IoFilename: ioFilename,
				IoInput:    ioInput,
				IoOutput:   ioOutput,
				IoIdle:     ioIdle,
			}

			ioDone := make(chan struct{})
			go func() {
				defer close(ioDone)
				for {
					select {
					case command, ok := <-ioCommand:
						if !ok {
							return
						}
						switch command {
						case gol.IoInput:
							_, ok := <-ioFilename
							if !ok {
								return
							}
							world := make([]uint8, 256)
							world[1*16+1] = 255
							world[2*16+1] = 255
							world[3*16+1] = 255
							for _, cell := range world {
								select {
								case ioInput <- cell:
								case <-ioDone:
									return
								}
							}
						case gol.IoOutput:
							_, ok := <-ioFilename
							if !ok {
								return
							}
							for i := 0; i < 256; i++ {
								select {
								case <-ioOutput:
								case <-ioDone:
									return
								}
							}
						case gol.IoCheckIdle:
							select {
							case ioIdle <- true:
							case <-ioDone:
								return
							}
						}
					case <-ioDone:
						return
					}
				}
			}()

			go func() {
				for event := range events {
					if _, ok := event.(gol.FinalTurnComplete); ok {
						return
					}
				}
			}()

			done := make(chan struct{})
			go func() {
				defer close(done)
				DistributorMapReduce(params, channels)
			}()

			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatalf("Deadlock detected with small channel buffer size %d", bufSize)
			}

			close(ioCommand)
			<-ioDone
		})
	}
}

// -----------------------------------------------------------------
// 3. Proof of Test Suite Validity (Injected Bug Verification)
// -----------------------------------------------------------------
func TestInjectedBug_MapReducePrematureMutation(t *testing.T) {
	ref := NewReferenceEngine(4, 4, []uint8{
		255, 255, 0, 0,
		255, 255, 0, 0,
		0, 0, 0, 0,
		0, 0, 0, 0,
	})

	buggyWorld := []uint8{
		0, 255, 0, 0,
		255, 255, 0, 0,
		0, 0, 0, 0,
		0, 0, 0, 0,
	}

	_, refGrid := ref.Step()
	matches := true
	for y := 0; y < 4; y++ {
		for x := 0; x < 4; x++ {
			if (buggyWorld[y*4+x] == 255) != (refGrid[y][x] == 255) {
				matches = false
			}
		}
	}

	if matches {
		t.Fatal("Test suite failed to catch map-reduce premature board mutation bug!")
	}
}

// -----------------------------------------------------------------
// 4. Coursework Benchmark Suite (Map-Reduce)
// -----------------------------------------------------------------
func BenchmarkMapReduce(b *testing.B) {
	sizes := []int{64, 128, 256, 512}
	threadsList := []int{1, 2, 4, 8, 16, 32}
	turns := 50

	for _, size := range sizes {
		initialWorld := make([]uint8, size*size)
		r := rand.New(rand.NewSource(42))
		for i := range initialWorld {
			if r.Float64() < 0.2 {
				initialWorld[i] = 255
			}
		}

		for _, th := range threadsList {
			b.Run(fmt.Sprintf("MapReduce/Size_%d/Threads_%d", size, th), func(b *testing.B) {
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					events := make(chan gol.Event, 5000)
					keyPresses := make(chan rune, 10)
					ioCommand := make(chan gol.IoCommand)
					ioFilename := make(chan string)
					ioInput := make(chan uint8, size*size)
					ioOutput := make(chan uint8, size*size)
					ioIdle := make(chan bool)

					channels := gol.DistributorChannels{
						Events:     events,
						KeyPresses: keyPresses,
						IoCommand:  ioCommand,
						IoFilename: ioFilename,
						IoInput:    ioInput,
						IoOutput:   ioOutput,
						IoIdle:     ioIdle,
					}

					ioDone := make(chan struct{})
					go func(world []uint8) {
						defer close(ioDone)
						for {
							select {
							case cmd, ok := <-ioCommand:
								if !ok {
									return
								}
								switch cmd {
								case gol.IoInput:
									_, ok := <-ioFilename
									if !ok {
										return
									}
									for _, cell := range world {
										select {
										case ioInput <- cell:
										case <-ioDone:
											return
										}
									}
								case gol.IoOutput:
									_, ok := <-ioFilename
									if !ok {
										return
									}
									for j := 0; j < size*size; j++ {
										select {
										case <-ioOutput:
										case <-ioDone:
											return
										}
									}
								case gol.IoCheckIdle:
									select {
									case ioIdle <- true:
									case <-ioDone:
										return
									}
								}
							case <-ioDone:
								return
							}
						}
					}(initialWorld)

					go func() {
						for range events {
						}
					}()

					params := gol.Params{
						ImageWidth:  size,
						ImageHeight: size,
						Turns:       turns,
						Threads:     th,
					}

					DistributorMapReduce(params, channels)
					close(ioCommand)
					<-ioDone
				}
			})
		}
	}
}
