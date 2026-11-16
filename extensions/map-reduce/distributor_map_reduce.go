package extensions

import (
	"fmt"
	"sync"

	"uk.ac.bris.cs/gameoflife/gol"
	"uk.ac.bris.cs/gameoflife/util"
)

// MapReduceChanBuffer defines the capacity of the flip channel.
// Default is 1024, but can be overridden in tests to verify small channel buffer behavior.
var MapReduceChanBuffer = 1024

// countNeighbours computes live neighbors with toroidal wrapping using the read-only world.
func countNeighboursMapReduce(world [][]uint8, x, y, width, height int) int {
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

// mapWorker examines its assigned horizontal strip and sends cell flips to the channel.
func mapWorker(
	startY, endY int,
	world [][]uint8,
	width, height int,
	flips chan<- util.Cell,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	for y := startY; y < endY; y++ {
		for x := 0; x < width; x++ {
			neighbours := countNeighboursMapReduce(
				world,
				x,
				y,
				width,
				height,
			)
			alive := world[y][x] == 255
			nextAlive := false
			if alive {
				nextAlive = neighbours == 2 || neighbours == 3
			} else {
				nextAlive = neighbours == 3
			}
			if nextAlive != alive {
				flips <- util.Cell{
					X: x,
					Y: y,
				}
			}
		}
	}
}

// startReducer runs concurrently to drain the flip channel and accumulate changes.
func startReducer(
	flipChan <-chan util.Cell,
	result chan<- []util.Cell,
) {
	go func() {
		var flipped []util.Cell
		for cell := range flipChan {
			flipped = append(flipped, cell)
		}
		result <- flipped
	}()
}

// processTurn manages the map-reduce phase for a single generation.
func processTurn(
	world [][]uint8,
	p gol.Params,
) []util.Cell {
	flipChan := make(chan util.Cell, MapReduceChanBuffer)
	resultChan := make(chan []util.Cell, 1)

	// Start reducer first to avoid blocking workers on small buffer sizes
	startReducer(flipChan, resultChan)

	var wg sync.WaitGroup
	rowsPerWorker := p.ImageHeight / p.Threads
	for i := 0; i < p.Threads; i++ {
		startY := i * rowsPerWorker
		endY := startY + rowsPerWorker
		if i == p.Threads-1 {
			endY = p.ImageHeight
		}
		wg.Add(1)
		go mapWorker(
			startY,
			endY,
			world,
			p.ImageWidth,
			p.ImageHeight,
			flipChan,
			&wg,
		)
	}

	wg.Wait()
	close(flipChan)

	flipped := <-resultChan
	return flipped
}

// applyFlips mutates the world board using the collected list of flipped cells.
func applyFlips(world [][]uint8, flipped []util.Cell) {
	for _, cell := range flipped {
		if world[cell.Y][cell.X] == 255 {
			world[cell.Y][cell.X] = 0
		} else {
			world[cell.Y][cell.X] = 255
		}
	}
}

// DistributorMapReduce manages the map-reduce lifecycle, worker coordination, and board updates.
func DistributorMapReduce(
	p gol.Params,
	c gol.DistributorChannels,
) {
	// 1. Allocate a single board upfront (no second board allocation required)
	world := make([][]uint8, p.ImageHeight)
	for y := 0; y < p.ImageHeight; y++ {
		world[y] = make([]uint8, p.ImageWidth)
	}

	// 2. Load Initial State via IO
	c.IoCommand <- gol.IoInput
	c.IoFilename <- fmt.Sprintf("%dx%d", p.ImageWidth, p.ImageHeight)

	for y := 0; y < p.ImageHeight; y++ {
		for x := 0; x < p.ImageWidth; x++ {
			world[y][x] = <-c.IoInput
			if world[y][x] == 255 {
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
	turn := 0
	for turn < p.Turns {
		select {
		case <-c.KeyPresses:
			// Handle pause/quit or key events if needed by skeleton runners
		default:
			flipped := processTurn(world, p)
			applyFlips(world, flipped)

			if len(flipped) > 0 {
				c.Events <- gol.CellsFlipped{
					CompletedTurns: turn + 1,
					Cells:          flipped,
				}
			}

			c.Events <- gol.TurnComplete{
				CompletedTurns: turn + 1,
			}

			turn++
		}
	}

	// 4. Final Turn Completion & I/O Output handshake
	var alive []util.Cell
	for y := 0; y < p.ImageHeight; y++ {
		for x := 0; x < p.ImageWidth; x++ {
			if world[y][x] == 255 {
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

	c.IoCommand <- gol.IoOutput
	c.IoFilename <- fmt.Sprintf("%dx%dx%d", p.ImageWidth, p.ImageHeight, p.Turns)
	for y := 0; y < p.ImageHeight; y++ {
		for x := 0; x < p.ImageWidth; x++ {
			c.IoOutput <- world[y][x]
		}
	}

	c.IoCommand <- gol.IoCheckIdle
	<-c.IoIdle
}
