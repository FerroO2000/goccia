package main

import (
	"flag"
	"log"
	"os"

	"github.com/FerroO2000/goccia/cmd/metrics-gen/pkg"
)

func main() {
	inputFile := flag.String("in", "", "path to the YAML spec file (required)")
	outputFile := flag.String("out", "", "directory path where to write the generated .go file (required)")
	flag.Parse()

	if *inputFile == "" || *outputFile == "" {
		flag.Usage()
		os.Exit(1)
	}

	spec, err := pkg.LoadSpec(*inputFile)
	if err != nil {
		log.Fatalf("loading spec: %v", err)
	}

	generator := pkg.NewGenerator(*outputFile)
	if err := generator.Generate(spec); err != nil {
		log.Fatalf("generating code: %v", err)
	}
}
