package extensions

import (
	"fmt"
	"sync"
	"time"

	"uk.ac.bris.cs/gameoflife/gol"
	"uk.ac.bris.cs/gameoflife/util"
)

type heapStripResult struct {
	startY  int
	rows    [][]uint8
	flipped []util.Cell
}

func DistributorHeap(p gol.Params, c gol.DistributorChannels) {
	// ---------- Load initial world ----------
	world := make([][]uint8, p.ImageHeight)
	for y := range world {
		world[y] = make([]uint8, p.ImageWidth)
	}

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

	turn := 0
	c.Events <- gol.StateChange{
		CompletedTurns: turn,
		NewState:       gol.Executing,
	}

	// ---------- Shared state for ticker ----------
	var mutex sync.RWMutex
	completedTurns := 0
	ticker := time.NewTicker(2 * time.Second)
	done := make(chan struct{})

	go func() {
		for {
			select {
			case <-ticker.C:
				mutex.RLock()
				alive := 0
				for y := 0; y < p.ImageHeight; y++ {
					for x := 0; x < p.ImageWidth; x++ {
						if world[y][x] == 255 {
							alive++
						}
					}
				}
				currentTurn := completedTurns
				mutex.RUnlock()
				c.Events <- gol.AliveCellsCount{
					CompletedTurns: currentTurn,
					CellsCount:     alive,
				}
			case <-done:
				return
			}
		}
	}()

	rowsPerWorker := p.ImageHeight / p.Threads

	// ---------- Main simulation ----------
	for turn < p.Turns {
		select {
		case key := <-c.KeyPresses:
			switch key {
			case 'p':
				handlePauseHeap(world, turn, p, c, &mutex)
			case 's':
				mutex.RLock()
				gol.SaveWorld(world, turn, p, c)
				mutex.RUnlock()
			case 'q':
				mutex.RLock()
				gol.SaveWorld(world, turn, p, c)
				mutex.RUnlock()
				close(done)
				ticker.Stop()
				c.Events <- gol.FinalTurnComplete{
					CompletedTurns: turn,
					Alive:          getAliveCells(world),
				}
				c.Events <- gol.StateChange{
					CompletedTurns: turn,
					NewState:       gol.Quitting,
				}
				close(c.Events)
				return
			case 'k':
				close(done)
				ticker.Stop()
				c.Events <- gol.FinalTurnComplete{
					CompletedTurns: turn,
					Alive:          getAliveCells(world),
				}
				c.Events <- gol.StateChange{
					CompletedTurns: turn,
					NewState:       gol.Quitting,
				}
				close(c.Events)
				return
			}
		default:
			resultChan := make(chan heapStripResult, p.Threads)

			// Launch workers
			for workerID := 0; workerID < p.Threads; workerID++ {
				startY := workerID * rowsPerWorker
				endY := startY + rowsPerWorker
				if workerID == p.Threads-1 {
					endY = p.ImageHeight
				}
				go heapWorker(
					startY,
					endY,
					world,
					p.ImageWidth,
					p.ImageHeight,
					resultChan,
				)
			}

			// Assemble next generation
			nextWorld := make([][]uint8, p.ImageHeight)
			var flipped []util.Cell
			for i := 0; i < p.Threads; i++ {
				result := <-resultChan
				for localRow, row := range result.rows {
					nextWorld[result.startY+localRow] = row
				}
				flipped = append(flipped, result.flipped...)
			}

			if len(flipped) > 0 {
				c.Events <- gol.CellsFlipped{
					CompletedTurns: turn,
					Cells:          flipped,
				}
			}

			mutex.Lock()
			world = nextWorld
			completedTurns = turn + 1
			mutex.Unlock()

			turn++
			c.Events <- gol.TurnComplete{
				CompletedTurns: turn,
			}
		}
	}

	// ---------- Finalise ----------
	close(done)
	ticker.Stop()
	c.Events <- gol.FinalTurnComplete{
		CompletedTurns: turn,
		Alive:          getAliveCells(world),
	}

	c.IoCommand <- gol.IoCheckIdle
	<-c.IoIdle

	c.Events <- gol.StateChange{
		CompletedTurns: turn,
		NewState:       gol.Quitting,
	}

	close(c.Events)
}

// ---------------------------------------------------------
// Heap worker
// ---------------------------------------------------------
func heapWorker(
	startY int,
	endY int,
	world [][]uint8,
	width int,
	height int,
	resultChan chan<- heapStripResult,
) {
	// Fresh allocation every iteration
	rows := make([][]uint8, endY-startY)
	var flipped []util.Cell
	for localY := range rows {
		rows[localY] = make([]uint8, width)
		globalY := startY + localY
		for x := 0; x < width; x++ {
			neighbours := gol.CountNeighbours(
				world,
				x,
				globalY,
				width,
				height,
			)
			alive := world[globalY][x] == 255
			if alive && (neighbours == 2 || neighbours == 3) {
				rows[localY][x] = 255
			} else if !alive && neighbours == 3 {
				rows[localY][x] = 255
			}

			if rows[localY][x] != world[globalY][x] {
				flipped = append(flipped, util.Cell{
					X: x,
					Y: globalY,
				})
			}
		}
	}

	resultChan <- heapStripResult{
		startY:  startY,
		rows:    rows,
		flipped: flipped,
	}
}

// ---------------------------------------------------------
// Pause handler
// ---------------------------------------------------------
func handlePauseHeap(
	world [][]uint8,
	turn int,
	p gol.Params,
	c gol.DistributorChannels,
	mutex *sync.RWMutex,
) {
	c.Events <- gol.StateChange{
		CompletedTurns: turn,
		NewState:       gol.Paused,
	}
	for {
		key := <-c.KeyPresses
		switch key {
		case 'p':
			c.Events <- gol.StateChange{
				CompletedTurns: turn,
				NewState:       gol.Executing,
			}
			return
		case 's':
			mutex.RLock()
			gol.SaveWorld(world, turn, p, c)
			mutex.RUnlock()
		}
	}
}

// ---------------------------------------------------------
// Utility
// ---------------------------------------------------------
func getAliveCells(world [][]uint8) []util.Cell {
	var alive []util.Cell
	for y := range world {
		for x := range world[y] {
			if world[y][x] == 255 {
				alive = append(alive, util.Cell{
					X: x,
					Y: y,
				})
			}
		}
	}
	return alive
}
