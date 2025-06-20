package data

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
)

// type Corpus map[string]map[string]string

func Load(
	folderpath string,
	corpusfp string,
	queryfp string,
	qrelsfp string,
	split string,
) (
	map[string]map[string]string, // corpus
	map[string]string, //query
	map[string]map[string]int, //qrels
) {
	qrelsPath := filepath.Join(folderpath, qrelsfp, split+".tsv")
	corpusPath := filepath.Join(folderpath, corpusfp)
	queryPath := filepath.Join(folderpath, queryfp)

	// sanity check the files
	Check(qrelsPath, "tsv")
	Check(corpusPath, "jsonl")
	Check(queryPath, "jsonl")

	// load the data
	corpus := LoadCorpus(corpusPath)
	queries := LoadQueries(queryPath)
	qrels := LoadQrels(qrelsPath)
	return corpus, queries, qrels
}

// Check ensures file exists and ends with expected extension
func Check(path string, ext string) {
	if filepath.Ext(path) != "."+ext {
		log.Fatalf("File %s must have .%s extension", path, ext)
	}
	if _, err := os.Stat(path); os.IsNotExist(err) {
		log.Fatalf("File does not exist: %s", path)
	}
}

func LoadCorpus(filepath string) map[string]map[string]string {
	corpus := make(map[string]map[string]string)

	file, err := os.Open(filepath)

	if err != nil {
		log.Fatalf("Failed to open corpus file: %v", err)
	}

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		var line map[string]interface{}
		if err := json.Unmarshal(scanner.Bytes(), &line); err != nil {
			log.Printf("[CORPUS] Skipping line (invalid JSON): %v", err)
			continue
		}
		id, ok := line["_id"].(string)
		if !ok {
			log.Println("[CORPUS] Skipping line: missing _id field")
			continue
		}
		corpus[id] = map[string]string{
			"text":  line["text"].(string),
			"title": line["title"].(string),
		}
	}
	return corpus
}

func LoadQueries(filepath string) map[string]string {
	queries := make(map[string]string)
	file, err := os.Open(filepath)
	if err != nil {
		log.Fatalf("Failed to load query file: %v", err)
	}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var line map[string]interface{}
		if err := json.Unmarshal(scanner.Bytes(), &line); err != nil {
			log.Printf("[QUERY] Skipping line (invalid JSON): %v", err)
			continue
		}
		id, ok := line["_id"].(string)
		if !ok {
			log.Println("[QUERY] Skipping line: missing _id field")
			continue
		}
		queries[id] = line["text"].(string)
	}
	return queries
}

func LoadQrels(filepath string) map[string]map[string]int {
	qrels := make(map[string]map[string]int)

	file, err := os.Open(filepath)
	if err != nil {
		log.Fatalf("Error opening qrels file: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = '\t' // tab delimiter
	//skip header
	if _, err := reader.Read(); err != nil {
		log.Fatalf("Failed to read header: %v", err)
	}
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Skipping malformed line: %v", err)
			continue
		}
		if len(record) < 3 {
			log.Println("Skipping short line: ", record)
			continue
		}
		queryID := record[0]
		corpusID := record[1]
		score, err := strconv.Atoi(record[2])
		if err != nil {
			log.Printf("Invalid score %q on line: %v", record[2], err)
			continue
		}
		if _, exists := qrels[queryID]; !exists {
			qrels[queryID] = make(map[string]int)
		}
		qrels[queryID][corpusID] = score
	}
	return qrels
}
