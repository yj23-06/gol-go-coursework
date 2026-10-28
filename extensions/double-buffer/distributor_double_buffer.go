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
	for dy := -1; dy <= 1; dy++ {
		for dx := -1; dx <= 1; dx++ {
			if dx == 0 && dy == 0 {
				continue
			}
			nx := (x + dx + width) % width
			ny := (y + dy + height) % height
			if world[ny][nx] == 255 {
				count++
			}
		}
	}
	return count
}

// doubleBufferWorker computes a horizontal strip of the destination board from the source board.
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
	defer wg.Done()
	var flipped []util.Cell
	for y := startY; y < endY; y++ {
		for x := 0; x < width; x++ {
			neighbours := countNeighbours(source, x, y, width, height)
			alive := source[y][x] == 255
			newValue := uint8(0)
			if alive {
				if neighbours == 2 || neighbours == 3 {
					newValue = 255
				}
			} else {
				if neighbours == 3 {
					newValue = 255
				}
			}
			dest[y][x] = newValue
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
	flippedChan <- flipped
}

// DistributorDoubleBuffer manages the double-buffering lifecycle, worker coordination, and pointer swapping.
func DistributorDoubleBuffer(p gol.Params, c gol.DistributorChannels) {
	// 1. Allocate both boards upfront (only allocation during execution)
	source := make([][]uint8, p.ImageHeight)
	dest := make([][]uint8, p.ImageHeight)
	for y := 0; y < p.ImageHeight; y++ {
		source[y] = make([]uint8, p.ImageWidth)
		dest[y] = make([]uint8, p.ImageWidth)
	}

	// 2. Load Initial State
	c.IoCommand <- gol.IoInput
	c.IoFilename <- fmt.Sprintf("%dx%d", p.ImageWidth, p.ImageHeight)

	for y := 0; y < p.ImageHeight; y++ {
		for x := 0; x < p.ImageWidth; x++ {
			source[y][x] = <-c.IoInput
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
		var wg sync.WaitGroup
		flippedChan := make(chan []util.Cell, p.Threads)
		rowsPerWorker := p.ImageHeight / p.Threads

		// Spawn Workers
		for i := 0; i < p.Threads; i++ {
			startY := i * rowsPerWorker
			endY := startY + rowsPerWorker
			if i == p.Threads-1 {
				endY = p.ImageHeight
			}
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

		if len(allFlipped) > 0 {
			c.Events <- gol.CellsFlipped{
				CompletedTurns: turn + 1,
				Cells:          allFlipped,
			}
		}

		c.Events <- gol.TurnComplete{
			CompletedTurns: turn + 1,
		}

		// O(1) Pointer Swap
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
