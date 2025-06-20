# Benchmark BM25, in GO

## How to build
```
go mod tidy
go build .
```


## How to run BM25
### Step 1: Download dataset from BEIR:
```
python download_data.py [dataset_name]
```
For example: 
```
python download_data.py scifact
```

### Step 2: Run BM25
```
./bm25 --dataset [dataset_name] --top_k [topk] --init [true/false] --batch_size [batch_size to index]
```

For example:
```
./bm25 --dataset scifact --top_k 100 --init true --batch_size 1024
```
would run bm25 benchmark on the scifact dataset with top-100 document retrieved

### Step 3: Benchmark
```
python benchmark_result.py --dataset [dataset_name] --topk_list [list of topk to run benchmark]
```

For example:
```
python benchmark_result.py --dataset scifact --topk_list 1 3 5 10
```
would run the benchmark on scifact dataset, with top-1, top-3, top-5, top-10 result





