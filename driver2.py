import sys
from pype.pipeline import *
from timeseries.TimeSeries import TimeSeries

for fname in sys.argv[1:]:
  pipeline = Pipeline(source=fname)

  times = []
  values = []

  for time in range(100):
    times.append(time)
    values.append(time-50)
  
  ts = TimeSeries(times, values)

  standardized_ts = pipeline['standardize'].run(ts)

  print("Output should be 0, 1")
  print("Output:", standardized_ts.mean(), standardized_ts.std())
  assert(standardized_ts.mean() == 0)
  assert(standardized_ts.std() == -1)