package main

import "C"
import (
	"pg-compare/cmd"
)

//export Run
func Run(filePath *C.char) {
	fileLocation := C.GoString(filePath)
	cmd.Execute(fileLocation)
}
func main() {
	// cmd.Execute("") // Call Execute with an empty string to run the default behavior and for local testing
}
