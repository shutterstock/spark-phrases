Spark-phrases
====
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)

Spark phrases using count min sketch, based on word2phrase ([https://arxiv.org/pdf/1310.4546.pdf](https://arxiv.org/pdf/1310.4546.pdf)).


## Build
```
sbt clean assembly
```

## Usage
```python
from pyspark.sql import DataFrame


rdd = spark.sparkContext.parallelize([Row(foo=['greater', 'new', 'york', 'city']),
                                      Row(foo=['southern', 'new', 'york', 'state']),
                                      Row(foo=['northern', 'new', 'york', 'state']),
                                      Row(foo=['quick', 'fox']),
                                      Row(foo=['slow', 'fox']),
                                      Row(foo=['lazy', 'dog']),
                                      Row(foo=['the', 'quick', 'fox']),
                                      Row(foo=['the', 'brown', 'lazy', 'dog'])])
df = spark.createDataFrame(rdd)

phraser = spark._jvm.com.shutterstock.Phraser()

# First pass
phraser.setInputCol('foo')
phraser.setOutputCol('first_pass')

phrases_df = DataFrame(phraser.transform(df._jdf), df.sql_ctx)

# Second pass
phraser.setInputCol('first_pass')
phraser.setOutputCol('second_pass')

phrases_df = DataFrame(phraser.transform(sdf._jdf), sdf.sql_ctx)


phrases_df.show(truncate=False)

```
```
+----------------------------+---------------------------+--------------------------+
|foo                         |first_pass                 |second_pass               |
+----------------------------+---------------------------+--------------------------+
|[greater, new, york, city]  |[greater, new_york, city]  |[greater, new_york, city] |
|[southern, new, york, state]|[southern, new_york, state]|[southern, new_york_state]|
|[northern, new, york, state]|[northern, new_york, state]|[northern, new_york_state]|
|[quick, fox]                |[quick_fox]                |[quick_fox]               |
|[slow, fox]                 |[slow, fox]                |[slow, fox]               |
|[lazy, dog]                 |[lazy_dog]                 |[lazy_dog]                |
|[the, quick, fox]           |[the, quick_fox]           |[the, quick_fox]          |
|[the, brown, lazy, dog]     |[the, brown, lazy_dog]     |[the, brown, lazy_dog]    |
+----------------------------+---------------------------+--------------------------+
```
