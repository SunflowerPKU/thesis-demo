import pandas as pd
import logging

from overall_metric_calculator import *
from stat_analyzer import *
from pymongo import MongoClient

def insert_metric(metric, summary, db):
    def pack(s):
        slope, pvalue = linear_regression(s.index, s)

        data = {
            'data' : {
                'x' : map(lambda n: int(n), list(s.index)),
                'y' : list(s.astype(float))
            },
            'stat' : {
                'slope' : slope,
                'p-value' : pvalue
            }
        }
        return data

    mean_data = pack(summary.loc['mean'])
    median_data = pack(summary.loc['50%'])

    db.overall.insert_one({
        "name": metric,
        "mean": mean_data,
        "median": median_data
        })



if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)

    client = MongoClient()
    db = client.research

    logging.info('Reading file commits.pkl')
    df = pd.read_pickle("../linux_research/commits.pkl")
    logging.info("File commits.pkl loaded successfully")

    logging.info("Dropping all documents inside overall collection")
    db.overall.drop()

    metric_calculators = {
        'commit_count' : top_n_commit_count_stat,
        'non_contiguous_pressure' : top_n_non_contiguous_pressure_stat,
        'contiguous_pressure' : top_n_contiguous_pressure_stat,
        'file_complexity' : top_n_file_complexity_stat,
        'contact_complexity' : top_n_contact_complexity_stat,
        'latency' : top_n_latency_stat,
    }

    for metric, calculator in metric_calculators.items():
        logging.info("Calculating metric " + metric)
        summary = calculator(df)
        logging.info("Inserting metric " + metric)
        insert_metric(metric, summary, db)