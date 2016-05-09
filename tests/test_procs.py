from timeseries import TimeSeries
from procs import *
import numpy as np
from scipy.stats import norm


def tsmaker(m, s, j):
    '''
    Helper function #1: randomly generates a time series for testing.
    '''

    # generate metadata
    meta = {}
    meta['order'] = int(np.random.choice([-5, -4, -3, -2, -1, 0,
                                          1, 2, 3, 4, 5]))
    meta['blarg'] = int(np.random.choice([1, 2]))
    meta['vp'] = False  # initialize vantage point indicator as negative

    # generate time series data
    t = np.arange(0.0, 1.0, 0.01)
    v = norm.pdf(t, m, s) + j * np.random.randn(100)

    # return time series and metadata
    return meta, TimeSeries(t, v)


def random_ts(a):
    '''
    Helper function #2: randomly generates a time series for testing.
    '''
    t = np.arange(0.0, 1.0, 0.01)
    v = (a * np.random.random(100))
    return TimeSeries(t, v)


# adapted from tests in _corr.py
def test_procs():

    # check that standardization is successful
    _, t1 = tsmaker(0.5, 0.1, 0.01)  # ignore meta-data returned
    t2 = random_ts(2)
    standts1 = stand(t1, t1.mean(), t1.std())
    standts2 = stand(t2, t2.mean(), t2.std())
    assert np.round(standts1.mean(), 10) == 0.0
    assert np.round(standts1.std(), 10) == 1.0
    assert np.round(standts2.mean(), 10) == 0.0
    assert np.round(standts2.std(), 10) == 1.0

    # once more, with hard-coded data so we know what answers to expect
    v1 = [2.00984793, 3.94985729, 0.51427819, 4.16184495, 2.73640138,
          0.07386398, 1.32847121, 0.3811719, 4.34006452, 1.86213488]
    v2 = [6.43496991, 10.34439479, 11.71829468, 3.92319708, 7.07694841,
          6.7165553, 5.72293448, 4.79283759, 11.74512723, 11.74048488]
    t1 = TimeSeries(np.arange(0.0, 1.0, 0.1), v1)
    t2 = TimeSeries(np.arange(0.0, 1.0, 0.1), v2)
    standts1 = stand(t1, t1.mean(), t1.std())
    standts2 = stand(t2, t2.mean(), t2.std())
    assert np.round(standts1.mean(), 10) == 0.0
    assert np.round(standts1.std(), 10) == 1.0
    assert np.round(standts2.mean(), 10) == 0.0
    assert np.round(standts2.std(), 10) == 1.0

    idx, mcorr = max_corr_at_phase(standts1, standts2)
    assert idx == 2
    assert np.round(mcorr, 4) == 0.5207

    sumcorr = kernel_corr(standts1, standts2, mult=10)
    assert np.round(sumcorr, 4) == 0.0125
