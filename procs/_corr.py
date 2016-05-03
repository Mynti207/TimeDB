import numpy.fft as nfft
import numpy as np
import timeseries as ts
from scipy.stats import norm
from collections import deque

# for debugging purposes
# import sys
# sys.path.append('/Users/nicolasdrizard/Documents/Course CSE/CS207/TimeDB/procs')


def tsmaker(m, s, j):
    '''
    Creates a random time series with some metadata.
    '''
    meta = {}
    meta['order'] = int(np.random.choice([-5, -4, -3, -2, -1, 0,
                                          1, 2, 3, 4, 5]))
    meta['blarg'] = int(np.random.choice([1, 2]))
    t = np.arange(0.0, 1.0, 0.01)
    v = norm.pdf(t, m, s) + j * np.random.randn(100)
    return meta, ts.TimeSeries(t, v)


def random_ts(a):
    '''
    Creates a random time series, where the times are equally spaced between
    0.0 and 1.0, and the values are random variables uniformly distributed
    between 0 and the input a.
    '''
    t = np.arange(0.0, 1.0, 0.01)
    v = (a * np.random.random(100))
    return ts.TimeSeries(t, v)


def stand(x, m, s):
    '''
    Standardizes a variable x, using its mean m and its standard deviation s.
    '''
    return (x - m) / s


def ccor(ts1, ts2):
    '''
    Given two standardized time series, computes their cross-correlation using
    fast fourier transform.
    '''
    fft_ts1 = nfft.fft(ts1.valuesseq)
    fft_ts2 = nfft.fft(ts2.valuesseq)
    return (1/(1.*len(ts1))) * nfft.ifft(fft_ts1 * np.conjugate(fft_ts2)).real


def max_corr_at_phase(ts1, ts2):
    '''
    Given two standardized time series, determines the time at which their
    cross-correlation is maximized, as well as the cross-correlation itself.
    '''
    ccorts = ccor(ts1, ts2)
    idx = np.argmax(ccorts)
    maxcorr = ccorts[idx]
    return idx, maxcorr


def kernel_corr(ts1, ts2, mult=1):
    '''
    Computes a kernelized correlation, so that we can get a real distance.
    We normalize the kernel by np.sqrt(K(x,x)K(y,y)), so that the correlation
    of a time series with itself is 1.
    Reference: http://www.cs.tufts.edu/~roni/PUB/ecml09-tskernels.pdf
    '''
    cross_correlation = ccor(ts1, ts2)
    num = np.sum(np.exp(mult * cross_correlation))
    denom = np.sqrt(np.sum(np.exp(mult * ccor(ts1, ts1))) *
                    np.sum(np.exp(mult * ccor(ts2, ts2))))
    return num/denom


# adapted and move to test suite
# if __name__ == "__main__":
#     '''
#     Used to test the above functions.
#     '''
#     print("HI")
#     _, t1 = tsmaker(0.5, 0.1, 0.01)
#     _, t2 = tsmaker(0.5, 0.1, 0.01)
#     print(t1.mean(), t1.std(), t2.mean(), t2.std())
#     import matplotlib.pyplot as plt
#     plt.plot(t1)
#     plt.plot(t2)
#     plt.show()
#     standts1 = stand(t1, t1.mean(), t1.std())
#     standts2 = stand(t2, t2.mean(), t2.std())
#
#     idx, mcorr = max_corr_at_phase(standts1, standts2)
#     print(idx, mcorr)
#     sumcorr = kernel_corr(standts1, standts2, mult=10)
#     print(sumcorr)
#     t3 = random_ts(2)
#     t4 = random_ts(3)
#     plt.plot(t3)
#     plt.plot(t4)
#     plt.show()
#     standts3 = stand(t3, t3.mean(), t3.std())
#     standts4 = stand(t4, t4.mean(), t4.std())
#     idx, mcorr = max_corr_at_phase(standts3, standts4)
#     print(idx, mcorr)
#     sumcorr = kernel_corr(standts3, standts4, mult=10)
#     print(sumcorr)
