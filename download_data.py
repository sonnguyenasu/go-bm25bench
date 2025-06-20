from util import *


if __name__ == "__main__":
    import sys
    dataset_name = sys.argv[1]
    url = "https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/{}.zip".format(dataset_name)
    data_path = download_and_unzip(url, "dataset")