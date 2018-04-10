import pandas as pd
import re
import numpy as np
import statsmodels.api as sm
import logging

from scipy import stats
from collections import Counter
from collections import defaultdict


pattern = re.compile("Signed-off-by: (.*) <.*>")
review_pattern = re.compile("Reviewed-by: (.*) <.*>")

#----------------------------------

def total_commit_count(df):
    result = {}
    for year in range(2013, 2018):
        for month in range(1, 13):
            if year == 2017 and month == 7:
                break
            if not df.empty:
                selected = df[df.committed_datetime.map(lambda t: t.year == year and t.month == month)]
            else:
                selected = df.copy()
            result[(year, month)] = selected.shape[0]
    return pd.Series(result)

def top_n_commit_count(df, n):
    result = {}
    for year in range(2013, 2018):
        for month in range(1, 13):
            if year == 2017 and month == 7:
                break
            if not df.empty:
                selected = df[df.committed_datetime.map(lambda t: t.year == year and t.month == month)]
            else:
                selected = df.copy()
            counter = Counter()
            if (year,month) < (2015,10):
                for msg in selected.message:
                    signers = pattern.findall(msg)
                    if len(signers) > 1:
                        for signer in signers[1:]:
                            counter[signer] += 1
            else:
                for index in selected.index:
                    row = selected.loc[index]
                    author = row.author_name
                    committer = row.committer_name
                    msg = row.message
                    if author == committer:
                        reviewers = review_pattern.findall(msg)
                        for reviewer in reviewers:
                            counter[reviewer] += 1
                    else:
                        signers = pattern.findall(msg)
                        if len(signers) > 1:
                            for signer in signers[1:]:
                                counter[signer] += 1

            sorted_count = pd.Series(map(lambda t: t[1], counter.most_common()))
            if len(sorted_count) == 0:
                result[(year, month)] = 0.0
            else:
                result[(year, month)] = sorted_count[:n].sum()
    return pd.Series(result)

def top_n_commit_ratio(df, n):
    result = {}
    for year in range(2013, 2018):
        for month in range(1, 13):
            if year == 2017 and month == 7:
                break
            if not df.empty:
                selected = df[df.committed_datetime.map(lambda t: t.year == year and t.month == month)]
            else:
                selected = df.copy()
            counter = Counter()
            if (year,month) < (2015,10):
                for msg in selected.message:
                    signers = pattern.findall(msg)
                    if len(signers) > 1:
                        for signer in signers[1:]:
                            counter[signer] += 1
            else:
                for index in selected.index:
                    row = selected.loc[index]
                    author = row.author_name
                    committer = row.committer_name
                    msg = row.message
                    if author == committer:
                        reviewers = review_pattern.findall(msg)
                        for reviewer in reviewers:
                            counter[reviewer] += 1
                    else:
                        signers = pattern.findall(msg)
                        if len(signers) > 1:
                            for signer in signers[1:]:
                                counter[signer] += 1

            sorted_count = pd.Series(map(lambda t: t[1], counter.most_common()))
            if len(sorted_count) == 0:
                result[(year, month)] = 0.0
            else:
                result[(year, month)] = sorted_count[:n].sum() * 1.0 / sorted_count.sum()
    return pd.Series(result)

def latency_median(df):
    result = {}
    for year in range(2013,2018):
        for month in range(1,13):
            if year == 2017 and month > 6:
                break
            if not df.empty:
                subset = df[df.committed_datetime.map(lambda t: t.year == year and t.month == month)]
            else:
                subset = df.copy()
            if subset.shape[0] == 0:
                result[(year,month)] = 0
                continue
            diff = (subset.committed_datetime - subset.authored_datetime).map(lambda t: t.delta / (86400 * 1e9))
            desc = diff.describe()['50%']
            result[(year,month)] = desc
    return pd.Series(result)

def latency_mean(df):
    result = {}
    for year in range(2013,2018):
        for month in range(1,13):
            if year == 2017 and month > 6:
                break
            if not df.empty:
                subset = df[df.committed_datetime.map(lambda t: t.year == year and t.month == month)]
            else:
                subset = df.copy()
            if subset.shape[0] == 0:
                result[(year,month)] = 0
                continue
            diff = (subset.committed_datetime - subset.authored_datetime).map(lambda t: t.delta / (86400 * 1e9))
            desc = diff.describe()['mean']
            result[(year,month)] = desc
    return pd.Series(result)

def non_contiguous_pressure(commits):
    result = {}
    for year in range(2013,2018):
        for month in range(1,13):
            if year == 2017 and month > 6:
                break
            if not commits.empty:
                subset = commits[commits.committed_datetime.map(lambda t: t.year == year and t.month == month)]
            else:
                subset = commits.copy()
            committers = subset.committer_name.value_counts(normalize=True)
            acc = 0.0
            for committer in committers.index:
                df = subset[subset.committer_name == committer]
                s = df.committed_datetime.map(lambda t: t.hour).value_counts(normalize=True)
                if len(s) > 8:
                    acc += s[8:].sum() * committers[committer]
            result[(year,month)] = acc
    return pd.Series(result)

def contiguous_pressure(commits):
    def overwork_ratio(s):
        temp = {}
        for i in range(24):
            if i in s:
                temp[i] = s[i]
            else:
                temp[i] = 0
        for i in range(24, 48):
            temp[i] = temp[i-24]
        s = pd.Series(temp).sort_index()
        return 1 - s.rolling(window=8).sum().max()
    result = {}
    for year in range(2013,2018):
        for month in range(1,13):
            if not commits.empty:
                subset = commits[commits.committed_datetime.map(lambda t: t.year == year and t.month == month)]
            else:
                subset = commits.copy()
            if year == 2017 and month > 6:
                break
            committers = subset.committer_name.value_counts(normalize=True)
            acc = 0.0
            for committer in committers.index:
                df = subset[subset.committer_name == committer]
                s = df.committed_datetime.map(lambda t: t.hour).value_counts(normalize=True)
                ratio = overwork_ratio(s)
                acc += ratio * committers[committer]
            result[(year,month)] = acc
    return pd.Series(result)

def max_file_complexity(df):
    result = {}
    for year in range(2013, 2018):
        for month in range(1, 13):
            if year == 2017 and month == 7:
                break
            if not df.empty:
                selected = df[df.committed_datetime.map(lambda t: t.year == year and t.month == month)]
            else:
                selected = df.copy()
            dev_to_file = defaultdict(lambda: set())
            if (year,month) < (2015,10):
                for index in selected.index:
                    row = selected.loc[index]
                    msg = row.message
                    files = row.files.split(',')
                    if len(files) > 65:
                        continue
                    signers = pattern.findall(msg)
                    if len(signers) > 1:
                        for signer in signers[1:]:
                            for f in files:
                                dev_to_file[signer].add(f)
            else:
                for index in selected.index:
                    row = selected.loc[index]
                    author = row.author_name
                    committer = row.committer_name
                    msg = row.message
                    files = row.files.split(',')
                    if len(files) > 65:
                        continue
                    if author == committer:
                        reviewers = review_pattern.findall(msg)
                        for reviewer in reviewers:
                            for f in files:
                                dev_to_file[reviewer].add(f)
                    else:
                        signers = pattern.findall(msg)
                        if len(signers) > 1:
                            for signer in signers[1:]:
                                for f in files:
                                    dev_to_file[signer].add(f)
            
            each_file_count = map(lambda s: len(s), dev_to_file.values())
            if len(each_file_count) == 0:
                result[(year, month)] = 0.0
            else:
                result[(year, month)] = max(each_file_count)
    return pd.Series(result)
            
def max_contact_complexity(df):
    result = {}
    for year in range(2013, 2018):
        for month in range(1, 13):
            if year == 2017 and month == 7:
                break
            if not df.empty:
                selected = df[df.committed_datetime.map(lambda t: t.year == year and t.month == month)]
            else:
                selected = df.copy()
            dev_to_prev = defaultdict(lambda: set())
            if (year,month) < (2015,10):
                for index in selected.index:
                    row = selected.loc[index]
                    msg = row.message
                    files = row.files
                    if len(files) > 65:
                        continue
                    signers = pattern.findall(msg)
                    for i in range(1, len(signers)):
                        dev_to_prev[signers[i]].add(signers[i-1])

            else:
                for index in selected.index:
                    row = selected.loc[index]
                    author = row.author_name
                    committer = row.committer_name
                    msg = row.message
                    files = row.files
                    if len(files) > 65:
                        continue
                    if author == committer:
                        reviewers = review_pattern.findall(msg)
                        for reviewer in reviewers:
                            dev_to_prev[reviewer].add(author)
                    else:
                        signers = pattern.findall(msg)
                        for i in range(1, len(signers)):
                            dev_to_prev[signers[i]].add(signers[i-1])
            
            counts = map(lambda s: len(s), dev_to_prev.values())
            if len(counts) == 0:
                result[(year, month)] = 0
            else:
                result[(year, month)] = max(counts)
    return pd.Series(result)            

#-----------------------------------------

def commit_filter(df, info, start_date = "20130101", end_date = "20170701"):
    df = df[(df.committed_datetime >= start_date) & (df.committed_datetime < end_date)]
    df = df[df.files.map(lambda files: match_files_patterns(files, info['F']))]
    return df

def match_files_patterns(files, patterns):
    if len(files) == 0:
        return False
    files = files.split(',')
    for file in files:
        for pattern in patterns:
            if match_file_pattern(file, pattern):
                return True
    return False

def match_file_pattern(file, pattern):
    try:
        pattern = pattern.replace('*', '.*')
        if pattern.endswith('/'):
            pattern = pattern + '.*'
        elif pattern.endswith('.*'):
            pattern = pattern[:-2] + "[^/]*"
        pattern = "^" + pattern
        #print pattern
        pattern = re.compile(pattern)
        match = pattern.search(file)
        if match != None and match.end() == len(file):
            return True
        else:
            return False
    except Exception, e:
        return False

