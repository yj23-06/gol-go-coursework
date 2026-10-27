package extensions

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"uk.ac.bris.cs/gameoflife/gol"
)

func TestDistributorHeapCorrectness(t *testing.T) {

	params := gol.Params{
		ImageWidth:  16,
		ImageHeight: 16,
		Turns:       5,
		Threads:     2,
	}

	events := make(chan gol.Event, 1000)
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

	//-----------------------------------------------------------------
	// Mock IO Goroutine
	//-----------------------------------------------------------------
	go func() {
		for command := range ioCommand {
			switch command {
			case gol.IoInput:
				_ = <-ioFilename

				// 16x16 world with a simple blinker
				world := make([]uint8, 16*16)
				world[7*16+7] = 255
				world[8*16+7] = 255
				world[9*16+7] = 255

				for _, cell := range world {
					ioInput <- cell
				}

			case gol.IoOutput:
				_ = <-ioFilename

				// consume all output pixels
				for i := 0; i < 16*16; i++ {
					<-ioOutput
				}
			case gol.IoCheckIdle:
				ioIdle <- true
			}
		}
	}()

	//-----------------------------------------------------------------
	// Event Consumer
	//-----------------------------------------------------------------
	var wg sync.WaitGroup
	wg.Add(1)
	finalReceived := make(chan gol.FinalTurnComplete, 1)
	go func() {
		defer wg.Done()
		for event := range events {
			switch e := event.(type) {
			case gol.FinalTurnComplete:
				finalReceived <- e
			}
		}
	}()

	//-----------------------------------------------------------------
	// Run DistributorHeap
	//-----------------------------------------------------------------
	done := make(chan struct{})
	go func() {
		defer close(done)
		DistributorHeap(params, channels)
	}()

	//-----------------------------------------------------------------
	// Assertions
	//-----------------------------------------------------------------
	select {
	case final := <-finalReceived:
		if final.CompletedTurns != params.Turns {
			t.Fatalf(
				"expected %d completed turns, got %d",
				params.Turns,
				final.CompletedTurns,
			)
		}
	case <-time.After(10 * time.Second):
		t.Fatal(
			"timeout waiting for FinalTurnComplete event (possible deadlock)",
		)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal(
			"DistributorHeap did not return after completion",
		)
	}
	wg.Wait()
}

// -----------------------------------------------------------------
// Additional Test: Multi-Threaded Scaling Test
// -----------------------------------------------------------------
func TestDistributorHeapDifferentThreadCounts(t *testing.T) {
	threadCounts := []int{1, 2, 4}

	for _, threads := range threadCounts {
		t.Run(fmt.Sprintf("threads-%d", threads), func(t *testing.T) {
			params := gol.Params{
				ImageWidth:  16,
				ImageHeight: 16,
				Turns:       3,
				Threads:     threads,
			}

			events := make(chan gol.Event, 500)
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

			// Mock IO handler with explicit shutdown channel
			doneIO := make(chan struct{})
			defer close(doneIO)

			go func() {
				for {
					select {
					case command, ok := <-ioCommand:
						if !ok {
							return
						}
						switch command {
						case gol.IoInput:
							_ = <-ioFilename
							world := make([]uint8, 16*16)
							world[7*16+7] = 255
							world[7*16+8] = 255
							world[8*16+7] = 255
							world[8*16+8] = 255
							for _, cell := range world {
								ioInput <- cell
							}
						case gol.IoOutput:
							_ = <-ioFilename
							for i := 0; i < 16*16; i++ {
								<-ioOutput
							}
						case gol.IoCheckIdle:
							ioIdle <- true
						}
					case <-doneIO:
						return
					}
				}
			}()

			go func() {
				for range events {
				}
			}()

			done := make(chan struct{})
			go func() {
				defer close(done)
				DistributorHeap(params, channels)
			}()

			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatalf("timeout with %d threads", threads)
			}
		})
	}
}

// -----------------------------------------------------------------
// Additional Test: Event Monitoring Verification
// -----------------------------------------------------------------
func TestDistributorHeapEmitsEvents(t *testing.T) {
	params := gol.Params{
		ImageWidth:  8,
		ImageHeight: 8,
		Turns:       2,
		Threads:     1,
	}

	events := make(chan gol.Event, 200)
	keyPresses := make(chan rune)
	ioCommand := make(chan gol.IoCommand)
	ioFilename := make(chan string)
	ioInput := make(chan uint8, 64)
	ioOutput := make(chan uint8, 64)
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

	go func() {
		for command := range ioCommand {
			switch command {
			case gol.IoInput:
				_ = <-ioFilename
				world := make([]uint8, 8*8)
				for _, cell := range world {
					ioInput <- cell
				}
			case gol.IoOutput:
				_ = <-ioFilename
				for i := 0; i < 64; i++ {
					<-ioOutput
				}
			case gol.IoCheckIdle:
				ioIdle <- true
			}
		}
	}()

	receivedStateChange := false
	receivedFinalTurn := false

	// Consume events concurrently until channel closes
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for event := range events {
			switch event.(type) {
			case gol.StateChange:
				receivedStateChange = true
			case gol.FinalTurnComplete:
				receivedFinalTurn = true
			}
		}
	}()

	DistributorHeap(params, channels)
	wg.Wait()

	if !receivedStateChange {
		t.Error("expected StateChange event to be emitted")
	}
	if !receivedFinalTurn {
		t.Error("expected FinalTurnComplete event to be emitted")
	}
}

// -----------------------------------------------------------------
// Benchmark: Heap Allocation Extension
// -----------------------------------------------------------------

func BenchmarkHeapAllocation(b *testing.B) {
	params := gol.Params{
		ImageWidth:  256,
		ImageHeight: 256,
		Turns:       20,
		Threads:     4,
	}

	for i := 0; i < b.N; i++ {
		events := make(chan gol.Event, 5000)
		keyPresses := make(chan rune)
		ioCommand := make(chan gol.IoCommand)
		ioFilename := make(chan string)
		ioInput := make(chan uint8, 256*256)
		ioOutput := make(chan uint8, 256*256)
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

		go func() {
			for command := range ioCommand {
				switch command {
				case gol.IoInput:
					_ = <-ioFilename
					world := make([]uint8, 256*256)
					for _, cell := range world {
						ioInput <- cell
					}
				case gol.IoOutput:
					_ = <-ioFilename
					for j := 0; j < 256*256; j++ {
						<-ioOutput
					}
				case gol.IoCheckIdle:
					ioIdle <- true
				}
			}
		}()

		go func() {
			for range events {
			}
		}()

		DistributorHeap(params, channels)
	}
}
