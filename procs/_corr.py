import numpy.fft as nfft
import numpy as np
from timeseries import TimeSeries


def stand(x, m, s):
    '''
    Standardizes a variable x, using its mean m and its standard deviation s.

    Parameters
    ----------
    x : float
        The variable to standardize
    m : float
        The variable's mean
    s : float
        The variable's standard deviation

    Returns
    -------
    The standardized variable.
    '''
    return (x - m) / s


def ccor(ts1, ts2):
    '''
    Given two standardized time series, computes their cross-correlation using
    fast fourier transform. Assume that the two time series are of the same
    length.

    Parameters
    ----------
    ts1 : TimeSeries
        A standardized time series
    ts2 : TimeSeries
        Another standardized time series

    Returns
    -------
    The two time series' cross-correlation.
    '''
    # calculate fast fourier transform of the two time series
    fft_ts1 = nfft.fft(ts1.valuesseq)
    fft_ts2 = nfft.fft(ts2.valuesseq)

    # print(len(ts1))
    # print(len(ts2))
    # assert len(ts1) == len(ts2)

    # return cross-correlation, i.e. the convolution of the first fft
    # and the conjugate of the second
    return ((1 / (1. * len(ts1))) *
            nfft.ifft(fft_ts1 * np.conjugate(fft_ts2)).real)


def max_corr_at_phase(ts1, ts2):
    '''
    Given two standardized time series, determines the time at which their
    cross-correlation is maximized, as well as the cross-correlation itself
    at that point.

    Parameters
    ----------
    ts1 : TimeSeries
        A standardized time series
    ts2 : TimeSeries
        Another standardized time series

    Returns
    -------
    idx, maxcorr : int, float
        Tuple of the time at which cross-correlation is maximized, and the
        cross-correlation at that point.
    '''

    # calculate cross-correlation between the two time series
    ccorts = ccor(ts1, ts2)

    # determine the time at which cross-correlation is maximized
    idx = np.argmax(ccorts)

    # determine the value of cross-correlation at that time
    maxcorr = ccorts[idx]

    # return the time, cross-correlation tuple
    return idx, maxcorr


def kernel_corr(ts1, ts2, mult=1):
    '''
    Given two standardized time series, calculates the distance between them
    based on the kernelized cross-correlation. The kernel is normalized so that
    the cross-correlation of a time series with itself equals one.

    Reference: http://www.cs.tufts.edu/~roni/PUB/ecml09-tskernels.pdf

    Parameters
    ----------
    ts1 : TimeSeries
        A standardized time series
    ts2 : TimeSeries
        Another standardized time series
    mult : int
        Multiplicative constant in kernel function (gamma)

    Returns
    -------
    float
        Distance between two time series.
    '''

    # calculate cross-correlation
    cross_correlation = ccor(ts1, ts2)

    # calculate kernel
    num = np.sum(np.exp(mult * cross_correlation))

    # calculate kernel normalization
    denom = np.sqrt(np.sum(np.exp(mult * ccor(ts1, ts1))) *
                    np.sum(np.exp(mult * ccor(ts2, ts2))))

    # return normalized kernel
    if denom == 0:
        return 0
    else:
        return num/denom
