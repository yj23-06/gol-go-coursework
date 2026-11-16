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

	// Load the initial PGM image
	c.IoCommand <- IoInput
	c.IoFilename <- fmt.Sprintf("%dx%d", p.ImageWidth, p.ImageHeight)

	for y := 0; y < p.ImageHeight; y++ {
		for x := 0; x < p.ImageWidth; x++ {
			world[y][x] = <-c.IoInput
			if world[y][x] == 255 {
				c.Events <- CellFlipped{
					CompletedTurns: 0,
					Cell:           util.Cell{X: x, Y: y},
				}
			}
		}
	}

	c.Events <- StateChange{turn, Executing}

	// Ticker setup for reporting alive cell counts every 2 seconds
	var completedTurns int
	var mutex sync.RWMutex
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
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
				turns := completedTurns
				mutex.RUnlock()
				c.Events <- AliveCellsCount{
					CompletedTurns: turns,
					CellsCount:     alive,
				}
			case <-done:
				return
			}
		}
	}()

	rowsPerWorker := p.ImageHeight / p.Threads

	// Execute turns with interactive keypress handling via select
	for turn < p.Turns {
		select {
		case key := <-c.KeyPresses:
			switch key {
			case 'p':
				c.Events <- StateChange{CompletedTurns: turn, NewState: Paused}
				paused := true
				for paused {
					k := <-c.KeyPresses
					switch k {
					case 'p':
						c.Events <- StateChange{CompletedTurns: turn, NewState: Executing}
						paused = false
					case 's':
						mutex.RLock()
						SaveWorld(world, turn, p, c)
						mutex.RUnlock()
					case 'q':
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
				mutex.RLock()
				SaveWorld(world, turn, p, c)
				mutex.RUnlock()
			case 'q':
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
			nextWorld := make([][]uint8, p.ImageHeight)
			for y := range nextWorld {
				nextWorld[y] = make([]uint8, p.ImageWidth)
			}

			resultChan := make(chan WorkerResult, p.Threads)

			for i := 0; i < p.Threads; i++ {
				startY := i * rowsPerWorker
				endY := startY + rowsPerWorker
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

			var allFlipped []util.Cell
			for i := 0; i < p.Threads; i++ {
				result := <-resultChan
				allFlipped = append(allFlipped, result.flipped...)
			}

			if len(allFlipped) > 0 {
				c.Events <- CellsFlipped{
					CompletedTurns: turn,
					Cells:          allFlipped,
				}
			}

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

	// Stop the ticker goroutine safely
	close(done)

	// Save the final state as a PGM image before finishing
	mutex.RLock()
	SaveWorld(world, p.Turns, p, c)

	// Report the final state using FinalTurnComplete Event.
	var aliveCells []util.Cell
	for y := 0; y < p.ImageHeight; y++ {
		for x := 0; x < p.ImageWidth; x++ {
			if world[y][x] == 255 {
				aliveCells = append(aliveCells, util.Cell{X: x, Y: y})
			}
		}
	}
	mutex.RUnlock()

	c.Events <- FinalTurnComplete{
		CompletedTurns: p.Turns,
		Alive:          aliveCells,
	}

	// Make sure that the IO has finished any output before exiting.
	c.IoCommand <- IoCheckIdle
	<-c.IoIdle

	c.Events <- StateChange{p.Turns, Quitting}

	// Close the channel to stop the SDL goroutine gracefully.
	close(c.Events)
}

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
	for y := 0; y < p.ImageHeight; y++ {
		for x := 0; x < p.ImageWidth; x++ {
			c.IoOutput <- world[y][x]
		}
	}
	// Ensure IO has finished writing before reporting completion
	c.IoCommand <- IoCheckIdle
	<-c.IoIdle

	c.Events <- ImageOutputComplete{
		CompletedTurns: turn,
		Filename:       filename,
	}
}

func worker(
	startY, endY int,
	world [][]uint8,
	nextWorld [][]uint8,
	width, height int,
	resultChan chan<- WorkerResult,
) {
	var flipped []util.Cell
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
			if alive && (neighbours == 2 || neighbours == 3) {
				nextWorld[y][x] = 255
			} else if !alive && neighbours == 3 {
				nextWorld[y][x] = 255
			}

			if nextWorld[y][x] != world[y][x] {
				flipped = append(flipped, util.Cell{
					X: x,
					Y: y,
				})
			}
		}
	}
	resultChan <- WorkerResult{
		flipped: flipped,
	}
}

func CountNeighbours(world [][]uint8, x, y, width, height int) int {
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
