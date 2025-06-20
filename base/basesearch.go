package base

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"os"
	"time"
	"path/filepath"
	"github.com/elastic/go-elasticsearch/v9"
	"github.com/elastic/go-elasticsearch/v9/esapi"
	"github.com/schollz/progressbar/v3"
)

type ElasticSearch struct {
	IndexName      string
	Languages      []string
	Language       string
	TextKey        string
	TitleKey       string
	BatchSize      int
	NumberOfShards int
	ES             *elasticsearch.Client
}

func NewElasticSearch(es_credentials map[string]interface{}) ElasticSearch {
	IndexName := es_credentials["IndexName"].(string)
	Languages := []string{
		"arabic",
		"armenian",
		"basque",
		"bengali",
		"brazilian",
		"bulgarian",
		"catalan",
		"cjk",
		"czech",
		"danish",
		"dutch",
		"english",
		"estonian",
		"finnish",
		"french",
		"galician",
		"german",
		"greek",
		"hindi",
		"hungarian",
		"indonesian",
		"irish",
		"italian",
		"latvian",
		"lithuanian",
		"norwegian",
		"persian",
		"portuguese",
		"romanian",
		"russian",
		"sorani",
		"spanish",
		"swedish",
		"turkish",
		"thai",
	}
	Language := es_credentials["Language"].(string)
	TextKey := es_credentials["keys"].(map[string]string)["body"]
	TitleKey := es_credentials["keys"].(map[string]string)["title"]
	NumberOfShards := es_credentials["NumberOfShards"].(int)
	BatchSize := es_credentials["BatchSize"].(int)
	config := elasticsearch.Config{
		Addresses: []string{es_credentials["Hostname"].(string)},
	}
	es, err := elasticsearch.NewClient(config)
	if err != nil {
		log.Fatalf("Error creating client: %s", err)
	}
	return ElasticSearch{
		IndexName,
		Languages,
		Language,
		TextKey,
		TitleKey,
		BatchSize,
		NumberOfShards,
		es,
	}
}

func (es *ElasticSearch) delete_index() {
	// check if exist
	res, err := es.ES.Indices.Exists([]string{es.IndexName})
	if err != nil{
		log.Fatalf("Error checking index existence: %s", err)
	}
	defer res.Body.Close()
	if res.StatusCode == 200{
		// Delete
		res_delete, err := es.ES.Indices.Delete([]string{es.IndexName})
		if err != nil {
			log.Fatalf("Error deleting index: %s", err)
		}
		defer res_delete.Body.Close()
		if res_delete.IsError() {
			log.Printf("Error deleting index: %s", res_delete.String())
		} else {
			log.Println("Index deleted successfully")
		}
	}else{
		log.Println("Index not exists. Skipping delete.")
	}
}

func (es *ElasticSearch) create_index() {
	// Create
	// Check if index exists

	res_exist, err := es.ES.Indices.Exists([]string{es.IndexName})
	if err != nil {
		log.Fatalf("Error checking if index exists: %s", err)
	}
	defer res_exist.Body.Close()

	if res_exist.StatusCode == 404 {
		// index not exists, create it
		settings := map[string]interface{}{
			"settings": map[string]interface{}{
				"analysis": map[string]interface{}{
					"analyzer": map[string]interface{}{
						"default": map[string]interface{}{
							"type": es.Language,
						},
					},
				},
			},
			"mappings": map[string]interface{}{
				"properties": map[string]interface{}{
					"title": map[string]interface{}{
						"type":     "text",
						"analyzer": es.Language,
					},
					"text": map[string]interface{}{
						"type":     "text",
						"analyzer": es.Language,
					},
				},
			},
		}
		var buf bytes.Buffer
		if err := json.NewEncoder(&buf).Encode(settings); err != nil {
			log.Fatalf("Error encoding index settings: %s", err)
		}
		esidx, err := es.ES.Indices.Create(es.IndexName, es.ES.Indices.Create.WithBody(&buf))
		if err != nil {
			log.Fatalf("Error creating index: %s", err)
		}
		defer esidx.Body.Close()
		log.Println(esidx)
	}
}

func (es *ElasticSearch) BulkAddToIndex(docs map[string]interface{}) {
	var buf bytes.Buffer
	count := 0
	for_count := 0
	bar := progressbar.Default(int64(len(docs)))
	for idx, doc := range docs {
		bar.Add(1)
		meta := map[string]interface{}{
			"index": map[string]interface{}{
				"_index": es.IndexName,
				"_id":    idx,
			},
		}
		json.NewEncoder(&buf).Encode(meta)
		json.NewEncoder(&buf).Encode(doc)
		count++
		for_count++

		// Send every BatchSize document
		if count >= es.BatchSize || for_count == len(docs) {
			res, err := es.ES.Bulk(&buf, es.ES.Bulk.WithRefresh("true"))
			// time.Sleep(500 * time.Millisecond)
			if err != nil {
				log.Fatalf("Bulk error: %s", err)
			}
			res.Body.Close()
			buf.Reset()
			count = 0
		}
	}
}

func (es *ElasticSearch) LexicalMSearch(
	queries []string,
	topHits int,
	skip int,
) []interface{} {
	if skip+topHits > 10000 {
		log.Fatalf("Elasticsearch window too large (max 10000), got %d", skip+topHits)
	}
	var buf bytes.Buffer
	for _, query := range queries {
		json.NewEncoder(&buf).Encode(map[string]interface{}{"index": es.IndexName, "search_type": "dfs_query_then_fetch"}) // metadata

		//body
		json.NewEncoder(&buf).Encode(map[string]interface{}{
			"_source": false,
			"query": map[string]interface{}{
				"multi_match": map[string]interface{}{
					"query":       query,
					"type":        "best_fields",
					"fields":      []string{es.TitleKey, es.TextKey},
					"tie_breaker": 0.5,
				},
			},
			"size": skip + topHits,
		})
	}
	req := esapi.MsearchRequest{
		Body: bytes.NewReader(buf.Bytes()),
	}
	res, err := req.Do(context.Background(), es.ES)
	if err != nil {
		log.Fatalf("Error performing msearch: %s", err)
	}
	defer res.Body.Close()
	var r map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Fatalf("Error parsing the response Body: %s", err)
	}
	result := []interface{}{}
	for _, resp := range r["responses"].([]interface{}) {
		responses := resp.(map[string](interface{}))["hits"].(map[string](interface{}))["hits"].([]interface{})[skip:]
		hits := []interface{}{}
		hits = append(hits, responses...)
		result = append(result, es.HitTemplate(resp.(map[string](interface{})), hits))
		// fmt.Println("Msearch response status:")
	}
	return result
}

func (es *ElasticSearch) HitTemplate(
	es_res map[string]interface{},
	hits []interface{},
) map[string]interface{} {
	result := map[string]interface{}{
		"meta": map[string]interface{}{
			"total":    es_res["hits"].(map[string]interface{})["total"].(map[string]interface{})["value"],
			"took":     es_res["took"],
			"num_hits": len(hits),
		},
		"hits": hits,
	}
	return result
}

type BaseSearch interface {
	Search(
		corpus map[string]map[string]string,
		queries map[string]string,
		top_k int,
		options map[string]interface{}, // other optional param
	) map[string]map[string]float64
}

type BM25Search struct {
	Results    map[string]map[string]float64
	BatchSize  int
	Initialize bool
	SleepFor   int
	ES         ElasticSearch
}

type BM25SearchOptions struct {
	Indexname        string
	Hostname         string
	Keys             map[string]string
	Language         string
	BatchSize        int
	Number_of_shards int
	Initialize       bool
	SleepFor         int
}

func NewBM25Search(
	opts *BM25SearchOptions,
) BM25Search {
	IndexName := opts.Indexname
	Hostname := opts.Hostname
	keys := opts.Keys
	Language := opts.Language
	BatchSize := opts.BatchSize
	NumberOfShards := opts.Number_of_shards
	Initialize := opts.Initialize
	SleepFor := opts.SleepFor

	if Hostname == "" {
		Hostname = "http://localhost:9200"
	}
	if keys == nil {
		keys = map[string]string{"title": "title", "body": "txt"}
	}
	if Language == "" {
		Language = "english"
	}
	if BatchSize == 0 {
		BatchSize = 128
	}
	if NumberOfShards == 0 {
		NumberOfShards = 1
	}
	if SleepFor == 0 {
		SleepFor = 2
	}
	es := NewElasticSearch(map[string]interface{}{
		"IndexName":      IndexName,
		"Hostname":       Hostname,
		"keys":           keys,
		"Language":       Language,
		"BatchSize":      BatchSize,
		"NumberOfShards": NumberOfShards,
		"Initialize":     Initialize,
		"SleepFor":       SleepFor,
	})
	bm25 := BM25Search{
		map[string]map[string]float64{},
		BatchSize,
		Initialize,
		SleepFor,
		es,
	}
	if Initialize {
		bm25.DoInitialize()
	}
	return bm25
}

func (bm25 *BM25Search) DoInitialize() {
	bm25.ES.delete_index()
	time.Sleep(time.Duration(bm25.SleepFor) * time.Second)
	bm25.ES.create_index()
}

func (bm25 *BM25Search) Index(corpus map[string]map[string]string) {
	dictionary := make(map[string]interface{})
	len_corpus := int64(len(corpus))
	bar := progressbar.Default(len_corpus)
	for idx := range corpus {
		bar.Add(1)
		var title, text string
		if v, ok := corpus[idx]["title"]; ok {
			title = v
		}
		if v, ok := corpus[idx]["text"]; ok {
			text = v
		}

		dictionary[idx] = map[string]string{
			bm25.ES.TitleKey: title,
			bm25.ES.TextKey:  text,
		}
	}
	bm25.ES.BulkAddToIndex(dictionary)
}

//TODO: bm25search function using MSearch of elasticsearch

func (bm25 *BM25Search) Search(
	corpus map[string]map[string]string,
	queries map[string]string,
	top_k int,
	options map[string]interface{}, // other optional param
) map[string]map[string]float64 {
	if bm25.Initialize {
		bm25.Index(corpus)
		// sleep for few seconds so es indexes the docs properly
		time.Sleep(time.Duration(bm25.SleepFor) * time.Second)
	}
	// Extract query IDs (keys)
	queryIDs := make([]string, 0, len(queries))
	for qid := range queries {
		queryIDs = append(queryIDs, qid)
	}

	// Get queries in that order
	queryTexts := make([]string, 0, len(queryIDs))
	for _, qid := range queryIDs {
		queryTexts = append(queryTexts, queries[qid])
	}
	for start := 0; start < len(queries); start += bm25.BatchSize {
		end := start + bm25.BatchSize
		if end > len(queries) {
			end = len(queries)
		}
		queryBatch := queryTexts[start:end]
		queryIDBatch := queryIDs[start:end]
		//call batch search
		hits := bm25.ES.LexicalMSearch(queryBatch, top_k+1, 0)

		for i, result := range hits {
			queryID := queryIDBatch[i]
			scores := make(map[string]float64)
			for _, scoreAndCorpusID := range result.(map[string]interface{})["hits"].([]interface{}) {
				scoreAndCorpusIDMap := scoreAndCorpusID.(map[string]interface{})
				corpusID := scoreAndCorpusIDMap["_id"].(string)
				score := scoreAndCorpusIDMap["_score"].(float64)
				if corpusID != queryID {
					scores[corpusID] = score
				}
			}
			bm25.Results[queryID] = scores
		}
	}
	return bm25.Results
}

func SaveResultsAsJSON(results map[string]map[string]float64, file_path string) {
	dir := filepath.Dir(file_path)
	err := os.MkdirAll(dir, os.ModePerm)
	if err!=nil{
		log.Fatalf("Error creating path: %v", err)
	}

	file, err := os.Create(file_path)
	
	if err != nil {
		log.Fatalf("Error creating file: %v", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ") // optional: pretty print

	if err := encoder.Encode(results); err != nil {
		log.Fatalf("Error encoding JSON: %v", err)
	}
}
