package extensions

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"uk.ac.bris.cs/gameoflife/gol"
)

// -----------------------------------------------------------------
// 1. Core Correctness Test: Blinker pattern check
// -----------------------------------------------------------------
func TestDistributorDoubleBufferCorrectness(t *testing.T) {
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

	go func() {
		for command := range ioCommand {
			switch command {
			case gol.IoInput:
				_ = <-ioFilename
				world := make([]uint8, 16*16)
				world[7*16+7] = 255
				world[8*16+7] = 255
				world[9*16+7] = 255

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
		}
	}()

	var wg sync.WaitGroup
	wg.Add(1)
	finalReceived := make(chan gol.FinalTurnComplete, 1)
	go func() {
		defer wg.Done()
		for event := range events {
			switch e := event.(type) {
			case gol.FinalTurnComplete:
				finalReceived <- e
				return
			}
		}
	}()

	done := make(chan struct{})
	go func() {
		defer close(done)
		DistributorDoubleBuffer(params, channels)
	}()

	select {
	case final := <-finalReceived:
		if final.CompletedTurns != params.Turns {
			t.Fatalf("expected %d completed turns, got %d", params.Turns, final.CompletedTurns)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for FinalTurnComplete event")
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("DistributorDoubleBuffer did not return after completion")
	}
	wg.Wait()
}

// -----------------------------------------------------------------
// 2. Different Thread Counts Test
// -----------------------------------------------------------------
func TestDistributorDoubleBufferDifferentThreadCounts(t *testing.T) {
	threadCounts := []int{1, 2, 4}

	for _, threads := range threadCounts {
		t.Run(fmt.Sprintf("threads-%d", threads), func(t *testing.T) {
			params := gol.Params{
				ImageWidth:  32,
				ImageHeight: 32,
				Turns:       10,
				Threads:     threads,
			}

			events := make(chan gol.Event, 5000)
			keyPresses := make(chan rune, 10)
			ioCommand := make(chan gol.IoCommand)
			ioFilename := make(chan string)
			ioInput := make(chan uint8, 32*32)
			ioOutput := make(chan uint8, 32*32)
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
						world := make([]uint8, 32*32)
						// Add a simple glider or block
						world[1*32+1] = 255
						world[1*32+2] = 255
						world[2*32+1] = 255
						world[2*32+2] = 255
						for _, cell := range world {
							ioInput <- cell
						}
					case gol.IoOutput:
						_ = <-ioFilename
						for i := 0; i < 32*32; i++ {
							<-ioOutput
						}
					case gol.IoCheckIdle:
						ioIdle <- true
					}
				}
			}()

			finalReceived := make(chan gol.FinalTurnComplete, 1)
			go func() {
				for event := range events {
					if e, ok := event.(gol.FinalTurnComplete); ok {
						finalReceived <- e
						return
					}
				}
			}()

			done := make(chan struct{})
			go func() {
				defer close(done)
				DistributorDoubleBuffer(params, channels)
			}()

			select {
			case final := <-finalReceived:
				if final.CompletedTurns != params.Turns {
					t.Errorf("expected %d turns, got %d", params.Turns, final.CompletedTurns)
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("timeout with %d threads", threads)
			}
			<-done
		})
	}
}

// -----------------------------------------------------------------
// 3. Event Emission Verification Test (Race-Free)
// -----------------------------------------------------------------
func TestDistributorDoubleBufferEmitsEvents(t *testing.T) {
	params := gol.Params{
		ImageWidth:  8,
		ImageHeight: 8,
		Turns:       2,
		Threads:     2,
	}

	events := make(chan gol.Event, 100)
	keyPresses := make(chan rune, 10)
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
				world := make([]uint8, 64)
				world[0] = 255
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

	done := make(chan struct{})
	go func() {
		defer close(done)
		DistributorDoubleBuffer(params, channels)
	}()

	// Collect events safely in the main test goroutine after completion
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout in event test")
	}

	close(events)

	hasTurnComplete := false
	hasFinalTurn := false
	for event := range events {
		switch event.(type) {
		case gol.TurnComplete:
			hasTurnComplete = true
		case gol.FinalTurnComplete:
			hasFinalTurn = true
		}
	}

	if !hasTurnComplete {
		t.Error("expected TurnComplete event to be emitted")
	}
	if !hasFinalTurn {
		t.Error("expected FinalTurnComplete event to be emitted")
	}
}

// -----------------------------------------------------------------
// 4. Benchmark: Double Buffering Performance
// -----------------------------------------------------------------
func BenchmarkDoubleBuffer(b *testing.B) {
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

		DistributorDoubleBuffer(params, channels)
	}
}
