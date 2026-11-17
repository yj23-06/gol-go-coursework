package extensions

import (
	"fmt"
	"sync"

	"uk.ac.bris.cs/gameoflife/gol"
	"uk.ac.bris.cs/gameoflife/util"
)

// countNeighbours computes live neighbors with toroidal wrapping using the immutable source board.
func countNeighbours(world [][]uint8, x, y, width, height int) int {
	count := 0
	// Check the eight positions surrounding the current cell.
	for dy := -1; dy <= 1; dy++ {
		for dx := -1; dx <= 1; dx++ {
			if dx == 0 && dy == 0 {
				continue
			}
			// Use modulo to wrap around the edges of the grid.
			nx := (x + dx + width) % width
			ny := (y + dy + height) % height
			if world[ny][nx] == 255 {
				count++
			}
		}
	}
	return count
}

// Each worker processes a section of rows and calculatesthe next state of those cells using the source board.
func doubleBufferWorker(
	source [][]uint8,
	dest [][]uint8,
	width int,
	height int,
	startY int,
	endY int,
	flippedChan chan<- []util.Cell,
	wg *sync.WaitGroup,
) {
	// Tell the WaitGroup when this worker has finished.
	defer wg.Done()
	var flipped []util.Cell

	// Only process the rows assigned to this worker.
	for y := startY; y < endY; y++ {
		for x := 0; x < width; x++ {

			// Count the alive neighbours and check the current cell.
			neighbours := countNeighbours(source, x, y, width, height)
			alive := source[y][x] == 255
			// Start with the cell dead and update it based on the Game of Life rules.
			newValue := uint8(0)
			if alive {
				// An alive cell survives with 2 or 3 neighbours.
				if neighbours == 2 || neighbours == 3 {
					newValue = 255
				}
			} else {
				// A dead cell becomes alive with exactly 3 neighbours.
				if neighbours == 3 {
					newValue = 255
				}
			}
			// Store the result in the destination board (the source board is not changed during this calculation)
			dest[y][x] = newValue
			// Keep track of cells whose state has changed.
			if newValue != source[y][x] {
				flipped = append(
					flipped,
					util.Cell{
						X: x,
						Y: y,
					},
				)
			}
		}
	}
	// Send the changed cells back to the distributor.
	flippedChan <- flipped
}

// DistributorDoubleBuffer manages the double-buffering lifecycle, worker coordination, and pointer swapping.
func DistributorDoubleBuffer(p gol.Params, c gol.DistributorChannels) {
	// 1. Create two boards before starting the simulation.
	// One stores the current state and the other stores the next state.
	source := make([][]uint8, p.ImageHeight)
	dest := make([][]uint8, p.ImageHeight)
	for y := 0; y < p.ImageHeight; y++ {
		source[y] = make([]uint8, p.ImageWidth)
		dest[y] = make([]uint8, p.ImageWidth)
	}

	// 2. Load Initial State
	c.IoCommand <- gol.IoInput
	c.IoFilename <- fmt.Sprintf("%dx%d", p.ImageWidth, p.ImageHeight)
	// Read each cell from the PGM file and store it in the source board
	for y := 0; y < p.ImageHeight; y++ {
		for x := 0; x < p.ImageWidth; x++ {
			source[y][x] = <-c.IoInput
			// Report any cells that are initially alive.
			if source[y][x] == 255 {
				c.Events <- gol.CellFlipped{
					CompletedTurns: 0,
					Cell: util.Cell{
						X: x,
						Y: y,
					},
				}
			}
		}
	}

	// 3. Main Simulation Loop
	for turn := 0; turn < p.Turns; turn++ {
		// The WaitGroup makes sure all workers finish before the boards are swapped.
		var wg sync.WaitGroup

		// Each worker sends its list of changed cells through this channel.
		// The buffer allows all workers to send their results.
		flippedChan := make(chan []util.Cell, p.Threads)

		rowsPerWorker := p.ImageHeight / p.Threads

		// Spawn Workers
		for i := 0; i < p.Threads; i++ {
			startY := i * rowsPerWorker
			endY := startY + rowsPerWorker
			if i == p.Threads-1 {
				endY = p.ImageHeight
			}
			// Add this worker to the WaitGroup before starting it.
			wg.Add(1)

			go doubleBufferWorker(
				source,
				dest,
				p.ImageWidth,
				p.ImageHeight,
				startY,
				endY,
				flippedChan,
				&wg,
			)
		}

		// Synchronise Workers
		wg.Wait()
		close(flippedChan)

		// Collect Flipped Cells
		var allFlipped []util.Cell
		for flipped := range flippedChan {
			allFlipped = append(allFlipped, flipped...)
		}

		// Report the cells that changed during this turn.
		if len(allFlipped) > 0 {
			c.Events <- gol.CellsFlipped{
				CompletedTurns: turn + 1,
				Cells:          allFlipped,
			}
		}
		// Notify the application that the current turn is complete.
		c.Events <- gol.TurnComplete{
			CompletedTurns: turn + 1,
		}

		// Swap the two boards so the newly calculated board becomes the source for the next turn.
		source, dest = dest, source
	}

	// 4. Final Turn Completion
	var alive []util.Cell
	for y := 0; y < p.ImageHeight; y++ {
		for x := 0; x < p.ImageWidth; x++ {
			if source[y][x] == 255 {
				alive = append(
					alive,
					util.Cell{
						X: x,
						Y: y,
					},
				)
			}
		}
	}

	// Send the final state of the simulation to the application.
	c.Events <- gol.FinalTurnComplete{
		CompletedTurns: p.Turns,
		Alive:          alive,
	}

	// 5. Request I/O Output for the final board state
	c.IoCommand <- gol.IoOutput
	c.IoFilename <- fmt.Sprintf("%dx%dx%d", p.ImageWidth, p.ImageHeight, p.Turns)
	for y := 0; y < p.ImageHeight; y++ {
		for x := 0; x < p.ImageWidth; x++ {
			c.IoOutput <- source[y][x]
		}
	}

	// 6. Check Idle State to ensure clean shutdown
	c.IoCommand <- gol.IoCheckIdle
	<-c.IoIdle
}
