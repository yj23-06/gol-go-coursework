package extensions

import (
	"fmt"
	"sync"
	"time"

	"uk.ac.bris.cs/gameoflife/gol"
	"uk.ac.bris.cs/gameoflife/util"
)

// Stores the results returned by each worker after processing its rows.
type heapStripResult struct {
	startY  int
	rows    [][]uint8
	flipped []util.Cell
}

func DistributorHeap(p gol.Params, c gol.DistributorChannels) {
	// Create a 2D slice to store the world.
	// Create the board and read the starting cell values from the I/O channel.
	world := make([][]uint8, p.ImageHeight)
	for y := range world {
		world[y] = make([]uint8, p.ImageWidth)
	}

	c.IoCommand <- gol.IoInput
	c.IoFilename <- fmt.Sprintf("%dx%d", p.ImageWidth, p.ImageHeight)

	for y := 0; y < p.ImageHeight; y++ {
		for x := 0; x < p.ImageWidth; x++ {
			world[y][x] = <-c.IoInput
			// Send an event for every cell that is initially alive.
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
	// Tell the event handler that the simulation has started.
	c.Events <- gol.StateChange{
		CompletedTurns: turn,
		NewState:       gol.Executing,
	}
	//The mutex protects the board and turn counter when they are accessed by the ticker and the main simulation at the same time.
	var mutex sync.RWMutex
	completedTurns := 0
	ticker := time.NewTicker(2 * time.Second)
	done := make(chan struct{})

	// Count the live cells every two seconds without interrupting the simulation.
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
	// Divide the rows between the workers. The last worker handles any remaining rows.
	rowsPerWorker := p.ImageHeight / p.Threads

	// Keep running until all requested turns have been completed.
	for turn < p.Turns {
		// Check for keyboard input before starting the next generation.
		select {
		case key := <-c.KeyPresses:

			// Handle pause, save and quit commands.
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
			// Create a channel to collect the results from all workers.
			resultChan := make(chan heapStripResult, p.Threads)

			// Start one worker for each assigned section of the board.
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

			// Collect the rows returned by the workers and combine them into the next board.
			nextWorld := make([][]uint8, p.ImageHeight)
			var flipped []util.Cell
			for i := 0; i < p.Threads; i++ {
				result := <-resultChan
				for localRow, row := range result.rows {
					nextWorld[result.startY+localRow] = row
				}
				flipped = append(flipped, result.flipped...)
			}
			// Report any cells that changed state during this turn.
			if len(flipped) > 0 {
				c.Events <- gol.CellsFlipped{
					CompletedTurns: turn,
					Cells:          flipped,
				}
			}
			// Replace the old board with the new one once all workers have finished.
			mutex.Lock()
			world = nextWorld
			completedTurns = turn + 1
			mutex.Unlock()
			// Move on to the next turn and notify the event handler.
			turn++
			c.Events <- gol.TurnComplete{
				CompletedTurns: turn,
			}
		}
	}

	// Stop the ticker and send the final board information.
	close(done)
	ticker.Stop()
	c.Events <- gol.FinalTurnComplete{
		CompletedTurns: turn,
		Alive:          getAliveCells(world),
	}
	// Wait for the I/O goroutine to finish any pending operations.
	c.IoCommand <- gol.IoCheckIdle
	<-c.IoIdle

	c.Events <- gol.StateChange{
		CompletedTurns: turn,
		NewState:       gol.Quitting,
	}

	close(c.Events)
}

// Each worker calculates the next state of its assigned rows.
// A new set of rows is allocated for every generation.
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
		// Check each cell and apply the Game of Life rules.
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
			// Record cells whose states have changed.
			if rows[localY][x] != world[globalY][x] {
				flipped = append(flipped, util.Cell{
					X: x,
					Y: globalY,
				})
			}
		}
	}

	// Send the completed rows back to the distributor.
	resultChan <- heapStripResult{
		startY:  startY,
		rows:    rows,
		flipped: flipped,
	}
}

// Handles keyboard input while the simulation is paused.
func handlePauseHeap(
	world [][]uint8,
	turn int,
	p gol.Params,
	c gol.DistributorChannels,
	mutex *sync.RWMutex,
) {
	// Notify the event handler that the simulation has paused.
	c.Events <- gol.StateChange{
		CompletedTurns: turn,
		NewState:       gol.Paused,
	}
	// Wait until the user chooses to resume or save the current board.
	for {
		key := <-c.KeyPresses
		switch key {
		// Resume the simulation when P is pressed.
		case 'p':
			c.Events <- gol.StateChange{
				CompletedTurns: turn,
				NewState:       gol.Executing,
			}
			return
		// Allow the board to be saved without resuming the simulation.
		case 's':
			mutex.RLock()
			gol.SaveWorld(world, turn, p, c)
			mutex.RUnlock()
		}
	}
}

// Returns the coordinates of all currently living cells.
func getAliveCells(world [][]uint8) []util.Cell {
	var alive []util.Cell
	for y := range world {
		for x := range world[y] {
			// Add the coordinates of each living cell to the result.
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
