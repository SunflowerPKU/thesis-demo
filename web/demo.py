# -*- coding: utf-8 -*-

from flask import *
from pymongo import MongoClient
from collections import defaultdict

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
    metric_names = [
        ('top_n_commit_count', 'top_n_commit_ratio'),
        ('latency_median', 'latency_mean'),
        ('non_contiguous_pressure', 'contiguous_pressure'),
        ('max_file_complexity', 'max_contact_complexity'),
    ]

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
                            modules=collections)

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