package main

/*
#include <stdlib.h>
*/
import "C"
import (
	"pg-compare/cmd"
)

//export Run
func Run(filePath *C.char) *C.char {
	fileLocation := C.GoString(filePath)
	output := cmd.ConfigurePgbelt(fileLocation)
	Str := C.CString(output)
	return Str
}
func main() {
	// cmd.Execute("") // Call Execute with an empty string to run the default behavior and for local testing
}
