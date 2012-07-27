Benchmarks
==========

.. TODO Benchmark each method of side-data for a few common scenarios
TypedBytes
----------

The majority of the time spent by Hadoopy (and Dumbo) is in the TypedBytes conversion code.  This is a simple binary serialization format that covers standard types with the ability to use Pickle for types not natively supported.  We generate a large set of test vectors (using the tb_make_test.py_ script), that have primatives, containers, and a uniform mix (GrabBag).  The idea is that by factoring out the types, we can easily see where optimization is needed.  Each element is read from stdin, then written to stdout.  Outside of the timing all of the values are compared to ensure that the final written values are the same.  Four methods are compared:  Hadoopy TypedBytes (speed_hadoopy.py_), Hadoopy TypedBytes file interface (speed_hadoopyfp.py_), TypedBytes_ (speed_tb.py_), and cTypedBytes_ (speed_tbc.py_).  All columns are in seconds except for ratio.  The ratio is min(TB, cTB) / HadoopyFP (e.g., 7 means HadoopyFP is 7 times faster).

.. _tb_make_test.py: https://github.com/bwhite/hadoopy/blob/master/play/tb_make_test.py
.. _speed_hadoopy.py: https://github.com/bwhite/hadoopy/blob/master/play/speed_hadoopy.py
.. _speed_hadoopyfp.py: https://github.com/bwhite/hadoopy/blob/master/play/speed_hadoopyfp.py
.. _speed_tbc.py: https://github.com/bwhite/hadoopy/blob/master/play/speed_tbc.py
.. _speed_tb.py: https://github.com/bwhite/hadoopy/blob/master/play/speed_tb.py
.. _TypedBytes:  https://github.com/klbostee/typedbytes
.. _cTypedBytes: https://github.com/klbostee/ctypedbytes

+-----------------+---------+---------+---------+---------+---------+
|Filename         |Hadoopy  |HadoopyFP|TB       |cTB      |Ratio    |
+=================+=========+=========+=========+=========+=========+
|   double_100k.tb| 0.148790| 0.119961| 0.904720| 0.993845| 7.541784|
+-----------------+---------+---------+---------+---------+---------+
|    float_100k.tb| 0.145637| 0.118920| 0.883124| 0.992447| 7.426198|
+-----------------+---------+---------+---------+---------+---------+
|       gb_100k.tb| 4.638573| 4.011934|25.577765|16.515563| 4.116609|
+-----------------+---------+---------+---------+---------+---------+
|     bool_100k.tb| 0.171327| 0.150975| 0.942188| 0.542741| 3.594907|
+-----------------+---------+---------+---------+---------+---------+
|       dict_50.tb| 0.394323| 0.364878| 1.649921| 1.225979| 3.359970|
+-----------------+---------+---------+---------+---------+---------+
|      tuple_50.tb| 0.370037| 0.413579| 1.546317| 1.241491| 3.001823|
+-----------------+---------+---------+---------+---------+---------+
|     byte_100k.tb| 0.183307| 0.164549| 0.894184| 0.487520| 2.962767|
+-----------------+---------+---------+---------+---------+---------+
|       list_50.tb| 0.355870| 0.370738| 1.529233| 1.092422| 2.946614|
+-----------------+---------+---------+---------+---------+---------+
|      int_100k.tb| 0.234842| 0.193235| 0.922423| 0.526160| 2.722903|
+-----------------+---------+---------+---------+---------+---------+
|     long_100k.tb| 0.761289| 0.640638| 1.727951| 1.957162| 2.697234|
+-----------------+---------+---------+---------+---------+---------+
| bytes_100_10k.tb| 0.069889| 0.069375| 0.147470| 0.096838| 1.395862|
+-----------------+---------+---------+---------+---------+---------+
|string_100_10k.tb| 0.106642| 0.104784| 0.157907| 0.106571| 1.017054|
+-----------------+---------+---------+---------+---------+---------+
|string_10k_10k.tb| 6.392013| 6.527343| 6.494607| 6.949912| 0.994985|
+-----------------+---------+---------+---------+---------+---------+
| bytes_10k_10k.tb| 3.073718| 3.123196| 3.039668| 3.100858| 0.973256|
+-----------------+---------+---------+---------+---------+---------+
| string_1k_10k.tb| 0.742198| 0.719119| 0.686382| 0.676537| 0.940786|
+-----------------+---------+---------+---------+---------+---------+
|  bytes_1k_10k.tb| 0.379785| 0.370314| 0.329728| 0.339387| 0.890401|
+-----------------+---------+---------+---------+---------+---------+
|     gb_single.tb| 0.045760| 0.042701| 0.038656| 0.034925| 0.817896|
+-----------------+---------+---------+---------+---------+---------+
