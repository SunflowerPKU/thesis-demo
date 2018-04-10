import pandas as pd
import re
import numpy as np
import statsmodels.api as sm

from scipy import stats
from collections import Counter
from collections import defaultdict

__pattern = re.compile("Signed-off-by: (.*) <.*>")
__review_pattern = re.compile("Reviewed-by: (.*) <.*>")

def top_n_commit_ratio_stat(commits, n = 60):
    result = {}
    for year in range(2007, 2017):
        selected = commits[commits.committed_datetime.map(lambda t: t.year == year)]
        result[year] = selected.committer_name.value_counts(normalize=True)[:n].sum()
    return pd.Series(result)

def top_n_commit_count_stat(commits, n = 60):
    result = {}
    for year in range(2007, 2017):
        selected = commits[commits.committed_datetime.map(lambda t: t.year == year)]
        counter = Counter()
        for msg in selected.message:
            signers = __pattern.findall(msg)
            if len(signers) > 1:
                for signer in signers[1:]:
                    counter[signer] += 1
        result[year] = pd.Series(map(lambda t: t[1], counter.most_common(n)))
    stat = pd.DataFrame(result).describe()
    return stat

def top_n_non_contiguous_pressure_stat(commits, n = 60):
    result = {}
    for year in range(2007, 2017):
        subset = commits[commits.committed_datetime.map(lambda t: t.year) == year]
        committers = subset.committer_name.value_counts()[:n]
        arr = []
        for committer in committers.index:
            df = subset[subset.committer_name == committer]
            s = df.committed_datetime.map(lambda t: t.hour).value_counts(normalize=True)
            if len(s) > 8:
                arr.append(s[8:].sum())
            else:
                arr.append(0.0)
        result[year] = arr
    stat = pd.DataFrame(result).describe()
    return stat

def top_n_contiguous_pressure_stat(commits, n = 60):
    def overwork_ratio(s):
        temp = {}
        for i in range(24):
            if i in s:
                temp[i] = s[i]
            else:
                temp[i] = 0
        for i in range(24, 48):
            temp[i] = temp[i-24]
        s = pd.Series(temp)
        s = s.sort_index()
        return 1 - s.rolling(window=8).sum().max()
    result = {}
    for year in range(2007, 2017):
        subset = commits[commits.committed_datetime.map(lambda t: t.year) == year]
        committers = subset.committer_name.value_counts()[:n]
        arr = []
        for committer in committers.index:
            df = subset[subset.committer_name == committer]
            s = df.committed_datetime.map(lambda t: t.hour).value_counts(normalize=True)
            arr.append(overwork_ratio(s))
        result[year] = arr
    stat = pd.DataFrame(result).describe()
    return stat

def top_n_file_complexity_stat(commits, n = 60):
    result = {}
    for year in range(2007, 2017):
        selected = commits[commits.committed_datetime.map(lambda t: t.year == year)]
        counter = Counter()
        maintainer_to_file = defaultdict(lambda: set())
        for idx in selected.index:
            row = selected.loc[idx]
            msg = row['message']
            filelist = row['files'].split(',')
            if len(filelist) > 26:
                continue
            signers = __pattern.findall(msg)
            if len(signers) > 1:
                for signer in signers[1:]:
                    counter[signer] += 1
                    for f in filelist:
                        maintainer_to_file[signer].add(f)
        result[year] = pd.Series(map(lambda t: len(maintainer_to_file[t[0]]), counter.most_common(n)))
    stat = pd.DataFrame(result).describe()
    return stat

def top_n_contact_complexity_stat(commits, n = 60):
    result = {}
    for year in range(2007, 2017):
        selected = commits[commits.committed_datetime.map(lambda t: t.year == year)]
        counter = Counter()
        maintainer_to_contact = defaultdict(lambda: set())
        for idx in selected.index:
            row = selected.loc[idx]
            msg = row['message']
            signers = __pattern.findall(msg)
            if len(signers) > 1:
                for i in range(1, len(signers)):
                    signer = signers[i]
                    prev = signers[i - 1]
                    counter[signer] += 1
                    maintainer_to_contact[signer].add(prev)
        result[year] = pd.Series(map(lambda t: len(maintainer_to_contact[t[0]]), counter.most_common(n)))
    stat = pd.DataFrame(result).describe()
    return stat

def top_n_latency_stat(commits, n = 60):
    result = {}
    for year in range(2007, 2017):
        subset = commits[commits.committed_datetime.map(lambda t: t.year) == year]
        committers = subset.committer_name.value_counts()[:n]
        arr = []
        #for committer in committers.index:
            #df = subset[subset.committer_name == committer]
            #arr.append((df.committed_datetime - df.authored_datetime).mean())
        df = subset[subset.committer_name.isin(committers.index)]
        df = df[df.committed_datetime != df.authored_datetime]
        diff = df.committed_datetime - df.authored_datetime
        q1, q2 = [0.05, 0.95]
        quantiles = diff.quantile([q1, q2])
        q_01 = quantiles.loc[q1]
        q_99 = quantiles.loc[q2]
        diff = diff[(diff > q_01) & (diff < q_99)]
        diff = diff.map(lambda t: t.delta / (86400 * 1e9))
        #result[year] = arr
        result[year] = diff.describe()
    stat = pd.DataFrame(result)
    return stat
