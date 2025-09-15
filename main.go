package main

import (
	"log"

	"github.com/nimaaskarian/harsh/cmd"
)

func main() {
	if err := cmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
