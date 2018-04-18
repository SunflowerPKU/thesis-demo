import pandas as pd
import logging

from maintainer_file_parser import *
from module_metric_calculator import *
from pymongo import MongoClient
from stat_analyzer import *

def insert_metric(db, module_name, result, terms):
    keys = sorted(result.keys())
    data = {
        'name': 'topic',
        'keys': keys,
        'topics': {
            '0' : map(lambda k: result[k][0], keys),
            '1' : map(lambda k: result[k][1], keys),
            '2' : map(lambda k: result[k][2], keys),
            '3' : map(lambda k: result[k][3], keys),
            '4' : map(lambda k: result[k][4], keys),
        },
        'terms' : {
            '0' : terms[0],
            '1' : terms[1],
            '2' : terms[2],
            '3' : terms[3],
            '4' : terms[4],
        }
    }
    db[module_name].insert_one(data)

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG, filename='module_topic.log', filemode='w')

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
        # if module_name != 'INTEL DRM DRIVERS (excluding Poulsbo, Moorestown and derivative chipsets)':
        #     continue
        try:
            db[module_name].delete_many({'name' : 'topic'})
            if 'M' not in info:
                continue
            if 'F' not in info:
                continue
            if ('S' not in info) or (info['S'][0] not in ['Supported', 'Maintained']):
                continue
                
            logging.info("Processing collection " + module_name)
            commits = commit_filter(df, info)
            by_month = commits.committed_datetime.map(lambda t: t.year*  100 + t.month).value_counts()
            if len(by_month) != 54 or by_month.min() < 10:
                continue
            logging.info(module_name + " meet the topic modeling requirements")
            topic_distribution, terms = lda(commits.message)

            combined =  pd.concat([topic_distribution, commits[['committed_datetime']]], axis=1)
            result = {}
            for year in range(2013, 2018):
                for month in range(1,13):
                    if year == 2017 and month == 7:
                        break
                    selected = combined[combined.committed_datetime.map(lambda t: t.year == year and t.month == month)]
                    topics = selected[[i for i in range(5)]].mean()
                    result[(year, month)] = topics
            insert_metric(db, module_name, result, terms)

        except Exception, e:
            logging.warning("In module " + module_name)
            logging.warning(info)
            logging.warning(e)
            continue


