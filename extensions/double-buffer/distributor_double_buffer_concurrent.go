package extensions

import (
	"fmt"
	"sync"

	"uk.ac.bris.cs/gameoflife/gol"
	"uk.ac.bris.cs/gameoflife/util"
)

// DistributorDoubleBufferConcurrent runs the double-buffer simulation while
// collecting worker flip lists concurrently with the workers.
func DistributorDoubleBufferConcurrent(p gol.Params, c gol.DistributorChannels) {
	source := make([][]uint8, p.ImageHeight)
	dest := make([][]uint8, p.ImageHeight)
	for y := 0; y < p.ImageHeight; y++ {
		source[y] = make([]uint8, p.ImageWidth)
		dest[y] = make([]uint8, p.ImageWidth)
	}

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

	for turn := 0; turn < p.Turns; turn++ {
		var wg sync.WaitGroup
		flippedChan := make(chan []util.Cell, p.Threads)
		allFlippedChan := make(chan []util.Cell, 1)

		go func() {
			var allFlipped []util.Cell
			for flipped := range flippedChan {
				allFlipped = append(allFlipped, flipped...)
			}
			allFlippedChan <- allFlipped
		}()

		rowsPerWorker := p.ImageHeight / p.Threads
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

		wg.Wait()
		close(flippedChan)
		allFlipped := <-allFlippedChan

		if len(allFlipped) > 0 {
			c.Events <- gol.CellsFlipped{
				CompletedTurns: turn + 1,
				Cells:          allFlipped,
			}
		}

		c.Events <- gol.TurnComplete{
			CompletedTurns: turn + 1,
		}

		source, dest = dest, source
	}

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

	c.IoCommand <- gol.IoOutput
	c.IoFilename <- fmt.Sprintf("%dx%dx%d", p.ImageWidth, p.ImageHeight, p.Turns)
	for y := 0; y < p.ImageHeight; y++ {
		for x := 0; x < p.ImageWidth; x++ {
			c.IoOutput <- source[y][x]
		}
	}

	c.IoCommand <- gol.IoCheckIdle
	<-c.IoIdle
}
