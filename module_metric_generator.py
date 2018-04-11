import pandas as pd
import logging

from maintainer_file_parser import *
from module_metric_calculator import *
from pymongo import MongoClient

def insert_metric(module_name, metric_name, metric, n_maitainers, db):
    x = list(metric.index.map(lambda idx: str(idx[0]) + '.' + str(idx[1])))
    y = map(lambda n: float(n), list(metric))
    data = {
        'x' : x,
        'y' : y,
    }
    db[module_name].insert_one({
        'name': metric_name,
        'n'   : n_maitainers,
        'data': data
        })

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG, filename='module_metric.log')

    client = MongoClient()
    db = client.research

    logging.info('Reading file commits.pkl')
    df = pd.read_pickle("../linux_research/commits.pkl")
    logging.info("File commits.pkl loaded successfully")

    logging.info("Reading and parsing MAINTAINER")
    with open("MAINTAINERS") as f:
        maintainer_text = f.read()
    modules = parse(maintainer_text)
    logging.info("MAINTAINER info loaded")

    for module_name, info in modules.items():
        try:
            if 'M' not in info:
                continue
            if 'F' not in info:
                continue
            if ('S' not in info) or (info['S'][0] not in ['Supported', 'Maintained']):
                continue
                
            logging.info("Processing collection " + module_name)
            db[module_name].drop()

            n_maitainers = len(info['M'])
            calculators = {
                'total_commit_count': total_commit_count,
                'top_n_commit_count': lambda df: top_n_commit_count(df, n_maitainers),
                'top_n_commit_ratio': lambda df: top_n_commit_ratio(df, n_maitainers),
                'latency_median': latency_median,
                'latency_mean': latency_mean,
                'non_contiguous_pressure': non_contiguous_pressure,
                'contiguous_pressure': contiguous_pressure,
                'max_file_complexity': max_file_complexity,
                'max_contact_complexity': max_contact_complexity,
            }
            commits = commit_filter(df, info)
            for metric_name, calculator in calculators.items():
                #logging.info("Module: " + module_name + " Metric: " + metric_name)
                metric = calculator(commits)
                insert_metric(module_name, metric_name, metric, n_maitainers, db)
        except Exception, e:
            logging.warning("In module " + module_name)
            logging.warning(info)
            logging.warning(e)
            continue


