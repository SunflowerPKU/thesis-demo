# -*- coding: utf-8 -*-

from flask import *
from pymongo import MongoClient
from collections import defaultdict
import scipy.stats as stats

import pandas as pd

app = Flask(__name__)

app.config.update(dict(
    SECRET_KEY='development key',
))

@app.route('/')
def index():
    client = MongoClient()
    db = client.research
    cursor = db.overall.find()
    metrics = {}
    for m in cursor:
        metrics[m['name']] = m
    metric_names = ['commit_count', 'non_contiguous_pressure', 'contiguous_pressure', \
                    'file_complexity', 'contact_complexity', 'latency']
    metric_labels = {
        'commit_count': u'提交数量',
        'non_contiguous_pressure': u'压力（非连续）',
        'contiguous_pressure': u'压力（连续）',
        'file_complexity': u'文件复杂度',
        'contact_complexity': u'人员复杂度',
        'latency': u'延迟',
    }
    type_labels = {
        'mean' : u'均值',
        'median' : u'中值',
    }

    collections = db.collection_names()
    if 'overall' in collections:
        collections.remove('overall')
    return render_template('index.html', 
                        metrics=metrics, 
                        metric_names=metric_names,
                        metric_labels=metric_labels,
                        type_labels=type_labels,
                        modules=collections)

@app.route('/module/<path:module_name>')
def module(module_name):
    client = MongoClient()
    db = client.research
    module_names = db.collection_names()
    if module_name not in module_names:
        flash('Module [%s] not found' % module_name, 'danger')
        return redirect(url_for('list_modules'))
    cursor = db[module_name].find()
    metrics = {}
    for m in cursor:
        metrics[m['name']] = m
        mann_whitney_u_test(metrics[m['name']], request.args)
    metric_names = [
        ('top_n_commit_count', 'top_n_commit_ratio'),
        ('latency_median', 'latency_mean'),
        ('non_contiguous_pressure', 'contiguous_pressure'),
        ('max_file_complexity', 'max_contact_complexity'),
    ]

    keys = metrics['top_n_commit_count']['data']['x']



    metric_labels = {
        'top_n_commit_count': u'前n提交者提交数量',
        'top_n_commit_ratio': u'前n提交者提交比例',
        'latency_median': u'延迟中值',
        'latency_mean': u'延迟均值',
        'non_contiguous_pressure': u'压力（非连续）',
        'contiguous_pressure': u'压力（连续）',
        'max_file_complexity': u'最大文件复杂度',
        'max_contact_complexity': u'最大人员复杂度',
    }
    
    collections = db.collection_names()
    if 'overall' in collections:
        collections.remove('overall')
    return render_template('module.html',
                            module_name=module_name,
                            metrics=metrics,
                            metric_names=metric_names,
                            metric_labels=metric_labels,
                            modules=collections,
                            keys=keys)

def mann_whitney_u_test(metric, args):
    if  'astart' not in args or\
        'aend' not in args or\
        'bstart' not in args or\
        'bend' not in args or\
        'data' not in metric:
        return

    astart = metric['data']['x'].index(args['astart'])
    aend = metric['data']['x'].index(args['aend'])
    bstart = metric['data']['x'].index(args['bstart'])
    bend = metric['data']['x'].index(args['bend'])

    data_a = metric['data']['y'][astart:aend + 1]
    data_b = metric['data']['y'][bstart:bend + 1]
    
    if len(data_a) == 0 or len(data_b) == 0:
        return
    try:
        greater_p = stats.mannwhitneyu(data_a, data_b, alternative='greater')[1]
        less_p = stats.mannwhitneyu(data_a, data_b, alternative='less')[1]

        if greater_p < 0.05:
            sign = '>'
            p_value = "{0:.3f}".format(greater_p)
            stars = get_stars(greater_p)
        elif less_p < 0.05:
            sign = '<'
            p_value = "{0:.3f}".format(less_p)
            stars = get_stars(less_p)
        else:
            sign = '-'
            p_value = "NA"
            stars = "-"
    except Exception, e:
        sign = '-'
        p_value = "NA"
        stars = "-"


    comparison = {
        'data_a' : {
            'from'   : args['astart'],
            'to'     : args['aend'],
            'mean'   : pd.Series(data_a).mean(),
            'median' : pd.Series(data_a).median(),
        },
        'data_b' : {
            'from'   : args['bstart'],
            'to'     : args['bend'],
            'mean'   : pd.Series(data_b).mean(),
            'median' : pd.Series(data_b).median(),
        },
        'sign' : sign,
        'p_value' : p_value,
        'stars' : stars,
    }
    metric['comparison'] = comparison


def get_stars(p):
    if p <= 0.001:
        return "***"
    elif p <= 0.01:
        return "**"
    elif p <= 0.05:
        return "*"
    else:
        return "-"

@app.route('/modules')
def list_modules():
    def group_by_init(collections):
        alpha = defaultdict(list)
        other = []
        for c in collections:
            if c[0].isalpha():
                alpha[c[0]].append(c)
            else:
                other.append(c)
        return alpha, other

    client = MongoClient()
    db = client.research
    collections = db.collection_names()
    if 'overall' in collections:
        collections.remove('overall')
    alpha, other = group_by_init(collections)
    return render_template('all_modules.html', alpha=alpha, other=other, modules=collections)