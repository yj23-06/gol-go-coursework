package gol

import (
	"fmt"
	"sync"
	"time"

	"uk.ac.bris.cs/gameoflife/util"
)

type DistributorChannels struct {
	Events     chan<- Event
	IoCommand  chan<- IoCommand
	IoIdle     <-chan bool
	IoFilename chan<- string
	IoOutput   chan<- uint8
	IoInput    <-chan uint8
	KeyPresses <-chan rune
}

type WorkerResult struct {
	flipped []util.Cell
}

// distributor divides the work between workers and interacts with other goroutines.
func distributor(p Params, c DistributorChannels) {
	// Create a 2D slice to store the world.
	world := make([][]uint8, p.ImageHeight)
	for i := range world {
		world[i] = make([]uint8, p.ImageWidth)
	}

	turn := 0

	// Read the starting image from the PGM file.
	c.IoCommand <- IoInput
	c.IoFilename <- fmt.Sprintf("%dx%d", p.ImageWidth, p.ImageHeight)

	// Load the image into the world one cell at a time.
	for y := 0; y < p.ImageHeight; y++ {
		for x := 0; x < p.ImageWidth; x++ {
			world[y][x] = <-c.IoInput

			// Report any cells that are already alive.
			if world[y][x] == 255 {
				c.Events <- CellFlipped{
					CompletedTurns: 0,
					Cell:           util.Cell{X: x, Y: y},
				}
			}
		}
	}
	// Let the application know that the simulation has started.
	c.Events <- StateChange{turn, Executing}

	// Ticker setup for reporting the number of alive cell counts every 2 seconds
	var completedTurns int
	var mutex sync.RWMutex

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	// This channel tells the ticker goroutine when to stop.
	done := make(chan struct{})
	// Run the ticker in a separate goroutine so it does not interruptthe main simulation while counting alive cells.
	go func() {
		for {
			select {
			case <-ticker.C:
				// Lock the world while reading it to avoid accessingit at the same time as it is being updated.
				mutex.RLock()
				alive := 0
				for y := 0; y < p.ImageHeight; y++ {
					for x := 0; x < p.ImageWidth; x++ {
						if world[y][x] == 255 {
							alive++
						}
					}
				}
				turns := completedTurns
				mutex.RUnlock()

				// Send the current alive cell count to the application.
				c.Events <- AliveCellsCount{
					CompletedTurns: turns,
					CellsCount:     alive,
				}
			case <-done:
				return
			}
		}
	}()

	// Divide the image into roughly equal sections for the workers.
	rowsPerWorker := p.ImageHeight / p.Threads

	// Run the simulation until all turns are completed.
	for turn < p.Turns {
		select {

		// Check for key presses before starting the next turn.
		case key := <-c.KeyPresses:
			switch key {

			case 'p':
				// Pause the simulation until p is pressed again.
				c.Events <- StateChange{CompletedTurns: turn, NewState: Paused}
				paused := true

				// Wait for another key press while the simulation is paused.
				for paused {
					k := <-c.KeyPresses
					switch k {
					case 'p':
						c.Events <- StateChange{CompletedTurns: turn, NewState: Executing}
						paused = false

					case 's':
						// Save the current world without stopping the simulation.
						mutex.RLock()
						SaveWorld(world, turn, p, c)
						mutex.RUnlock()

					case 'q':
						// Save the current world and quit.
						mutex.RLock()
						SaveWorld(world, turn, p, c)

						// Collect the positions of all alive cells.
						var aliveCells []util.Cell
						for y := 0; y < p.ImageHeight; y++ {
							for x := 0; x < p.ImageWidth; x++ {
								if world[y][x] == 255 {
									aliveCells = append(aliveCells, util.Cell{X: x, Y: y})
								}
							}
						}
						mutex.RUnlock()

						// Stop the ticker and report the final state.
						close(done)

						c.Events <- FinalTurnComplete{CompletedTurns: turn, Alive: aliveCells}
						c.Events <- StateChange{CompletedTurns: turn, NewState: Quitting}
						close(c.Events)
						return

					case 'k':
						// Quit without saving another image.
						mutex.RLock()

						var aliveCells []util.Cell
						for y := 0; y < p.ImageHeight; y++ {
							for x := 0; x < p.ImageWidth; x++ {
								if world[y][x] == 255 {
									aliveCells = append(aliveCells, util.Cell{X: x, Y: y})
								}
							}
						}
						mutex.RUnlock()
						close(done)

						c.Events <- FinalTurnComplete{CompletedTurns: turn, Alive: aliveCells}
						c.Events <- StateChange{CompletedTurns: turn, NewState: Quitting}
						close(c.Events)
						return
					}
				}

			case 's':
				// Save the current state.
				mutex.RLock()
				SaveWorld(world, turn, p, c)
				mutex.RUnlock()
			case 'q':
				// Save the current state and quit.
				mutex.RLock()
				SaveWorld(world, turn, p, c)

				var aliveCells []util.Cell
				for y := 0; y < p.ImageHeight; y++ {
					for x := 0; x < p.ImageWidth; x++ {
						if world[y][x] == 255 {
							aliveCells = append(aliveCells, util.Cell{X: x, Y: y})
						}
					}
				}

				mutex.RUnlock()
				close(done)
				c.Events <- FinalTurnComplete{CompletedTurns: turn, Alive: aliveCells}
				c.Events <- StateChange{CompletedTurns: turn, NewState: Quitting}
				close(c.Events)
				return

			case 'k':
				// Quit without saving another image.
				mutex.RLock()
				var aliveCells []util.Cell
				for y := 0; y < p.ImageHeight; y++ {
					for x := 0; x < p.ImageWidth; x++ {
						if world[y][x] == 255 {
							aliveCells = append(aliveCells, util.Cell{X: x, Y: y})
						}
					}
				}
				mutex.RUnlock()
				close(done)

				c.Events <- FinalTurnComplete{CompletedTurns: turn, Alive: aliveCells}
				c.Events <- StateChange{CompletedTurns: turn, NewState: Quitting}
				close(c.Events)
				return
			}

		default:
			// Create a new world for the next turn.
			nextWorld := make([][]uint8, p.ImageHeight)
			for y := range nextWorld {
				nextWorld[y] = make([]uint8, p.ImageWidth)
			}

			resultChan := make(chan WorkerResult, p.Threads)

			// Start one worker for each section of rows.
			for i := 0; i < p.Threads; i++ {
				startY := i * rowsPerWorker
				endY := startY + rowsPerWorker
				// Give the last worker any remaining rows.
				if i == p.Threads-1 {
					endY = p.ImageHeight
				}
				go worker(
					startY,
					endY,
					world,
					nextWorld,
					p.ImageWidth,
					p.ImageHeight,
					resultChan,
				)
			}

			// Wait for every worker to finish.
			var allFlipped []util.Cell
			for i := 0; i < p.Threads; i++ {
				result := <-resultChan
				allFlipped = append(allFlipped, result.flipped...)
			}

			// Report cells that changed state during this turn.
			if len(allFlipped) > 0 {
				c.Events <- CellsFlipped{
					CompletedTurns: turn,
					Cells:          allFlipped,
				}
			}

			// Replace the old world with the updated one.
			mutex.Lock()
			world = nextWorld
			completedTurns = turn + 1
			mutex.Unlock()

			turn++
			c.Events <- TurnComplete{
				CompletedTurns: turn,
			}
		}
	}

	// Stop the ticker goroutine once the simulation is finished.
	close(done)

	// Save the final state as a PGM image before finishing
	mutex.RLock()
	SaveWorld(world, p.Turns, p, c)

	// Collect the positions of all remaining alive cells.
	var aliveCells []util.Cell

	for y := 0; y < p.ImageHeight; y++ {
		for x := 0; x < p.ImageWidth; x++ {
			if world[y][x] == 255 {
				aliveCells = append(aliveCells, util.Cell{X: x, Y: y})
			}
		}
	}
	mutex.RUnlock()

	// Report that the final turn has finished.
	c.Events <- FinalTurnComplete{
		CompletedTurns: p.Turns,
		Alive:          aliveCells,
	}

	// Make sure that the IO has finished any output before exiting.
	c.IoCommand <- IoCheckIdle
	<-c.IoIdle

	c.Events <- StateChange{p.Turns, Quitting}

	// Close the events channel when everything is finished.
	close(c.Events)
}

// SaveWorld writes the current world to a PGM file.
func SaveWorld(
	world [][]uint8,
	turn int,
	p Params,
	c DistributorChannels,
) {
	filename := fmt.Sprintf("%dx%dx%d",
		p.ImageWidth,
		p.ImageHeight,
		turn,
	)
	c.IoCommand <- IoOutput
	c.IoFilename <- filename

	// Send each cell to the IO goroutine.
	for y := 0; y < p.ImageHeight; y++ {
		for x := 0; x < p.ImageWidth; x++ {
			c.IoOutput <- world[y][x]
		}
	}
	// Wait until the file has been written.
	c.IoCommand <- IoCheckIdle
	<-c.IoIdle

	c.Events <- ImageOutputComplete{
		CompletedTurns: turn,
		Filename:       filename,
	}
}

// worker calculates the next state of a section of the world.

func worker(
	startY, endY int,
	world [][]uint8,
	nextWorld [][]uint8,
	width, height int,
	resultChan chan<- WorkerResult,
) {

	var flipped []util.Cell
	// Process every cell in the rows assigned to this worker.
	for y := startY; y < endY; y++ {
		for x := 0; x < width; x++ {

			neighbours := CountNeighbours(
				world,
				x,
				y,
				width,
				height,
			)
			alive := world[y][x] == 255

			// Apply the Game of Life rules.
			if alive && (neighbours == 2 || neighbours == 3) {
				nextWorld[y][x] = 255
			} else if !alive && neighbours == 3 {
				nextWorld[y][x] = 255
			}

			// Keep track of cells that changed state.
			if nextWorld[y][x] != world[y][x] {
				flipped = append(flipped, util.Cell{
					X: x,
					Y: y,
				})
			}
		}
	}

	// Send the changed cells back to the distributor.
	resultChan <- WorkerResult{
		flipped: flipped,
	}
}

// CountNeighbours returns the number of alive neighbours around a cell.
func CountNeighbours(world [][]uint8, x, y, width, height int) int {
	count := 0
	// Check the eight surrounding cells
	for dy := -1; dy <= 1; dy++ {
		for dx := -1; dx <= 1; dx++ {
			// Skip the cell itself.
			if dx == 0 && dy == 0 {
				continue
			}
			// Wrap around the edges of the world.
			nx := (x + dx + width) % width
			ny := (y + dy + height) % height
			if world[ny][nx] == 255 {
				count++
			}
		}
	}
	return count
}
