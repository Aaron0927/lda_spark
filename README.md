lda_spark
=========

**3 versions of LDA implementations now**

* A version copy from this [gist](https://gist.github.com/waleking/5477002) with my modificatoin

* A version implemented with mapPartitions

* A version implemented with Accumulater

Please select from according branch.

TODO:

* A version with log-based update, sharding parameters, broadcast instead of serialization and even optimized dropOneSampler.

* Streaming LDA cooperate with current incremental LDA.
