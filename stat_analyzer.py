import pandas as pd

import statsmodels.api as sm

def linear_regression(x, y):
    X = pd.DataFrame({'ts':x, 'const': [1.0] * len(x)}, index=x)
    mod = sm.OLS(y, X)
    res = mod.fit()
    slope = res.params['ts']
    pvalue = res.pvalues['ts']
    return slope, pvalue