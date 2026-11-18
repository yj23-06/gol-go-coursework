package extensions

import (
	"fmt"
	"math/rand"
	"testing"

	"uk.ac.bris.cs/gameoflife/gol"
)

// BenchmarkDoubleBufferConcurrent measures the concurrent flip-collection variant across board and worker sizes.
func BenchmarkDoubleBufferConcurrent(b *testing.B) {
	sizes := []int{64, 128, 256, 512}
	threadsList := []int{1, 2, 4, 8, 16, 32}
	const turns = 50

	for _, size := range sizes {
		// Reuse the same seeded starting board for comparable benchmark iterations.
		initialWorld := make([]uint8, size*size)
		r := rand.New(rand.NewSource(42))
		for i := range initialWorld {
			if r.Float64() < 0.2 {
				initialWorld[i] = 255
			}
		}

		for _, threads := range threadsList {
			b.Run(fmt.Sprintf("DoubleBufferConcurrent/Size_%d/Threads_%d", size, threads), func(b *testing.B) {
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					// Set up buffered channels to mimic the distributor's I/O and event consumers.
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
					// Handle input, output, and idle-check requests from the distributor.
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
									if _, ok := <-ioFilename; !ok {
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
									if _, ok := <-ioFilename; !ok {
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

					// Drain events so the benchmark does not block on event delivery.
					go func() {
						for range events {
						}
					}()

					params := gol.Params{
						ImageWidth:  size,
						ImageHeight: size,
						Turns:       turns,
						Threads:     threads,
					}

					// Include the full simulation and wait for the mock I/O handler to finish.
					DistributorDoubleBufferConcurrent(params, channels)
					close(ioCommand)
					<-ioDone
				}
			})
		}
	}
}
