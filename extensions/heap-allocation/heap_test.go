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

// A simple Game of Life implementation used to check the heap allocation version.
type ReferenceEngine struct {
	width, height int
	grid          [][]uint8
}

// Creates a reference engine using the starting board.
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

// Calculates one generation and returns the cells that changed along with the updated board.
func (r *ReferenceEngine) Step() ([]util.Cell, [][]uint8) {
	// Create a new board so that the current generation is not overwritten.
	next := make([][]uint8, r.height)
	// Count the live neighbours, including cells across the board edges.
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
			// Apply the Game of Life rules to decide whether this cell survives.

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
			// Record any cells that changed between generations.
			if alive != nextAlive {
				flipped = append(flipped, util.Cell{X: x, Y: y})
			}
		}
	}
	r.grid = next
	return flipped, r.grid
}

// Tests the heap implementation against the reference engine using different board sizes, patterns and numbers of workers.
func TestDistributorHeapComprehensive(t *testing.T) {

	// Define the board settings for each test case.
	type testCase struct {
		name    string
		width   int
		height  int
		turns   int
		threads int
		pattern string
	}
	// These cases cover different worker counts, board sizes and starting patterns.
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

	// Run each case separately to make failures easier to identify.
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Record the number of goroutines before starting the test.
			initialGoroutines := runtime.NumGoroutine()

			// Set up the starting board.
			initialWorld := make([]uint8, tc.width*tc.height)

			// Use a fixed seed so random boards are the same every time.
			switch tc.pattern {
			case "blinker":
				if tc.width >= 3 && tc.height >= 4 {
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

			// Keep a copy of the original board to check that it is not modified.
			callerWorldCopy := make([]uint8, len(initialWorld))
			copy(callerWorldCopy, initialWorld)

			// Calculate the expected results using the reference engine.
			ref := NewReferenceEngine(tc.width, tc.height, initialWorld)

			params := gol.Params{
				ImageWidth:  tc.width,
				ImageHeight: tc.height,
				Turns:       tc.turns,
				Threads:     tc.threads,
			}

			// Set up the channels used by the distributor and mock I/O.
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

			// Mock the I/O goroutine by providing the starting board and handling output requests.
			go func() {
				for command := range ioCommand {
					switch command {
					case gol.IoInput:
						_ = <-ioFilename
						for _, cell := range initialWorld {
							ioInput <- cell
						}
					case gol.IoOutput:
						_ = <-ioFilename
						for i := 0; i < tc.width*tc.height; i++ {
							<-ioOutput
						}
					case gol.IoCheckIdle:
						ioIdle <- true
					}
				}
			}()

			// Listen for events and compare the reported cell changes with the reference engine.
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
					}
				}
			}()

			// Run the distributor in a separate goroutine.
			done := make(chan struct{})
			go func() {
				defer close(done)
				DistributorHeap(params, channels)
			}()

			// Stop the test if the distributor gets stuck.
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Fatalf("Timeout/Deadlock in %s", tc.name)
			}

			wg.Wait()
			// Check that the original input board has not been changed.
			for i := range initialWorld {
				if initialWorld[i] != callerWorldCopy[i] {
					t.Fatalf("Caller's starting board was modified at index %d", i)
				}
			}
			// Give any remaining goroutines time to finish before checking for leaks.
			time.Sleep(50 * time.Millisecond)
			if leaks := runtime.NumGoroutine() - initialGoroutines; leaks > 2 {
				t.Errorf("Goroutine leak detected: %d goroutines remaining", leaks)
			}
		})
	}
}

// Proof of Test Suite Validity (Injected Bug Verification)
// Uses an intentionally incorrect board to check that the reference comparison can detect a difference.
func TestInjectedBug_HeapPrematureMutation(t *testing.T) {
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

	// Calculate what the board should look like after one generation.
	_, refGrid := ref.Step()
	matches := true
	for y := 0; y < 4; y++ {
		for x := 0; x < 4; x++ {
			if (buggyWorld[y*4+x] == 255) != (refGrid[y][x] == 255) {
				matches = false
			}
		}
	}
	// The incorrect board should not match the reference result.
	if matches {
		t.Fatal("Test suite failed to catch injected memory mutation bug!")
	}
}

// Benchmark Suite
// Measures performance with different board sizes and worker counts
func BenchmarkHeap(b *testing.B) {
	sizes := []int{64, 128, 256, 512}
	threadsList := []int{1, 2, 4, 8, 16, 32}
	turns := 50
	// Generate a repeatable starting board for the benchmark.
	for _, size := range sizes {
		initialWorld := make([]uint8, size*size)
		r := rand.New(rand.NewSource(42))
		for i := range initialWorld {
			if r.Float64() < 0.2 {
				initialWorld[i] = 255
			}
		}

		for _, th := range threadsList {
			// Run the benchmark with each combination of board size and worker count.
			b.Run(fmt.Sprintf("Heap/Size_%d/Threads_%d", size, th), func(b *testing.B) {
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					// Set up the channels for the distributor.
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
					// Mock the I/O goroutine so the benchmark does not need actual image files.
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
					// Run the simulation and wait for the mock I/O goroutine to finish.
					DistributorHeap(params, channels)
					close(ioCommand)
					<-ioDone
				}
			})
		}
	}
}
