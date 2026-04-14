package main

import (
	"flag"
	"log"
	"os"

	"github.com/FerroO2000/goccia/cmd/metrics-maker/generator"
	"github.com/FerroO2000/goccia/cmd/metrics-maker/loader"
)

func main() {
	inputFile := flag.String("input", "", "path to the YAML input file (required)")
	outputFile := flag.String("output", "", "path to the generated .go file (required)")
	flag.Parse()

	if *inputFile == "" || *outputFile == "" {
		flag.Usage()
		os.Exit(1)
	}

	input, err := loader.Load(*inputFile)
	if err != nil {
		panic(err)
	}

	out, err := os.Create(*outputFile)
	if err != nil {
		log.Fatalf("creating output file: %v", err)
	}
	defer out.Close()

	if err := generator.Generate(out, input); err != nil {
		log.Fatalf("generating code: %v", err)
	}
}
