import json
import pytrec_eval
import csv
import argparse

def evaluate(result_path, qrels_path, k_values):
    result = json.load(open(result_path))
    # for k,v in result.items():
    #     result[k] = dict(sorted(v.items(), key=lambda item: item[1], reverse=True))
    reader = csv.reader(
        open(qrels_path, encoding="utf-8"),
        delimiter="\t",
        quoting=csv.QUOTE_MINIMAL,
    )
    next(reader) # skip first row
    qrels = {}
    for id, row in enumerate(reader):
        query_id, corpus_id, score = row[0], row[1], int(row[2])

        if query_id not in qrels:
            qrels[query_id] = {corpus_id: score}
        else:
            qrels[query_id][corpus_id] = score

    # remove value not in qrels
   
    ndcg = {}
    _map = {}
    recall = {}
    precision = {}

    for k in k_values:
        ndcg[f"NDCG@{k}"] = 0.0
        _map[f"MAP@{k}"] = 0.0
        recall[f"Recall@{k}"] = 0.0
        precision[f"P@{k}"] = 0.0

    map_string = "map_cut." + ",".join([str(k) for k in k_values])
    ndcg_string = "ndcg_cut." + ",".join([str(k) for k in k_values])
    recall_string = "recall." + ",".join([str(k) for k in k_values])
    precision_string = "P." + ",".join([str(k) for k in k_values])
    evaluator = pytrec_eval.RelevanceEvaluator(qrels, {map_string, ndcg_string, recall_string, precision_string})
    scores = evaluator.evaluate(result)
    for query_id in scores.keys():
        for k in k_values:
            ndcg[f"NDCG@{k}"] += scores[query_id]["ndcg_cut_" + str(k)]
            _map[f"MAP@{k}"] += scores[query_id]["map_cut_" + str(k)]
            recall[f"Recall@{k}"] += scores[query_id]["recall_" + str(k)]
            precision[f"P@{k}"] += scores[query_id]["P_" + str(k)]

    for k in k_values:
        ndcg[f"NDCG@{k}"] = round(ndcg[f"NDCG@{k}"] / len(scores), 5)
        _map[f"MAP@{k}"] = round(_map[f"MAP@{k}"] / len(scores), 5)
        recall[f"Recall@{k}"] = round(recall[f"Recall@{k}"] / len(scores), 5)
        precision[f"P@{k}"] = round(precision[f"P@{k}"] / len(scores), 5)

    return ndcg, _map, recall, precision


if __name__ =="__main__":
    parser = argparse.ArgumentParser("Benchmark")
    parser.add_argument("--dataset", type=str, default="scifact", help="Name of dataset to benchmark")
    parser.add_argument("--topk_list", nargs='+', default=[10,100], help="List of topk value")
    args = parser.parse_args()
    ndcg, _map, recall, precision = evaluate(f"results/{args.dataset}.json", f"dataset/{args.dataset}/qrels/test.tsv", args.topk_list)
    for eval in [ndcg, _map, recall, precision]:
        # logger.info("\n")
        for k in eval.keys():
            print(f"{k}: {eval[k]:.4f}")
    # print(scores)