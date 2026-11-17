package gol

// Params provides the details of how to run the Game of Life and which image to load.
type Params struct {
	Turns       int
	Threads     int
	ImageWidth  int
	ImageHeight int
}

// Run starts the processing of Game of Life. It should initialise channels and goroutines.
func Run(p Params, events chan<- Event, keyPresses <-chan rune) {
	// Create channels for communication with the IO goroutine.
	ioCommand := make(chan IoCommand)
	ioIdle := make(chan bool)
	ioFilename := make(chan string)
	ioOutput := make(chan uint8)
	ioInput := make(chan uint8)

	// Group the IO channels together.
	ioChannels := ioChannels{
		command:  ioCommand,
		idle:     ioIdle,
		filename: ioFilename,
		output:   ioOutput,
		input:    ioInput,
	}
	// Start the IO goroutine.
	go startIo(p, ioChannels)

	// Pass the channels to the distributor.
	distributorChannels := DistributorChannels{
		Events:     events,
		IoCommand:  ioCommand,
		IoIdle:     ioIdle,
		IoFilename: ioFilename,
		IoOutput:   ioOutput,
		IoInput:    ioInput,
		KeyPresses: keyPresses,
	}
	// Start the simulation.
	distributor(p, distributorChannels)
}
