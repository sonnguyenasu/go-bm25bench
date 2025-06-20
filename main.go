package main

import (
	"elastic/base"
	"elastic/data"
	"flag"
	"fmt"
	"log"
)

func main() {
	dataset := flag.String("dataset", "scifact", "Dataset to be used")
	topK := flag.Int("top_k", 10, "Number of top result to return")
	initialize := flag.Bool("init", true, "Wheter to re-index the dataset to elasticsearch")
	batchsize := flag.Int("batch_size", 128, "Batchsize of elasticsearch indexing")
	flag.Parse()

	opts := base.BM25SearchOptions{
		Indexname:  *dataset,
		Initialize: *initialize,
		BatchSize:  *batchsize,
	}
	bm25Search := base.NewBM25Search(&opts)

	corpus, query, _ := data.Load(
		fmt.Sprintf("dataset/%s", *dataset), // folder path
		"corpus.jsonl",                      // corpus file
		"queries.jsonl",                     // query file
		"qrels",                             // qrels folder name
		"test",                              // split
	)

	log.Println("Begin search")
	result := bm25Search.Search(corpus, query, *topK, nil)
	log.Println("End search")
	base.SaveResultsAsJSON(result, fmt.Sprintf("results/%s.json", *dataset))
}
