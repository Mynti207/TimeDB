import sys
from pipeline import *
from timeseries.TimeSeries import TimeSeries

for fname in sys.argv[1:]:
  pipeline = Pipeline(source=fname)

  time = []
  values = []

  for x in range(100):
    time.append(x)
    values.append(x-50)
  a = TimeSeries(time, values)
  standardized_TS = pipeline['standardize'].run(a)

  print("Output should be 0, 1")
  print("Output:", standardized_TS.mean(), standardized_TS.std())
  assert(round(standardized_TS.mean(), 7) == 0)
  assert(round(standardized_TS.std()-1, 7) == 0)