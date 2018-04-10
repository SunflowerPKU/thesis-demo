# -*- coding: utf-8 -*-

from flask import Flask
from flask import render_template
from pymongo import MongoClient

app = Flask(__name__)

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
    return render_template('index.html', 
                        metrics=metrics, 
                        metric_names=metric_names,
                        metric_labels=metric_labels,
                        type_labels=type_labels)

@app.route('/<module_name>')
def show(module_name):
    return render_template('module.html')