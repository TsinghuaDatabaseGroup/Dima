---
layout: global
title: Basic Statistics - spark.mllib
displayTitle: Basic Statistics - spark.mllib
---

* Table of contents
{:toc}


`\[
\newcommand{\R}{\mathbb{R}}
\newcommand{\E}{\mathbb{E}} 
\newcommand{\x}{\mathbf{x}}
\newcommand{\y}{\mathbf{y}}
\newcommand{\wv}{\mathbf{w}}
\newcommand{\av}{\mathbf{\alpha}}
\newcommand{\bv}{\mathbf{b}}
\newcommand{\N}{\mathbb{N}}
\newcommand{\id}{\mathbf{I}} 
\newcommand{\ind}{\mathbf{1}} 
\newcommand{\0}{\mathbf{0}} 
\newcommand{\unit}{\mathbf{e}} 
\newcommand{\one}{\mathbf{1}} 
\newcommand{\zero}{\mathbf{0}}
\]`

## Summary statistics 

We provide column summary statistics for `RDD[Vector]` through the function `colStats` 
available in `Statistics`.

<div class="codetabs">
<div data-lang="scala" markdown="1">

[`colStats()`](api/scala/index.html#org.apache.spark.mllib.stat.Statistics$) returns an instance of
[`MultivariateStatisticalSummary`](api/scala/index.html#org.apache.spark.mllib.stat.MultivariateStatisticalSummary),
which contains the column-wise max, min, mean, variance, and number of nonzeros, as well as the
total count.

Refer to the [`MultivariateStatisticalSummary` Scala docs](api/scala/index.html#org.apache.spark.mllib.stat.MultivariateStatisticalSummary) for details on the API.

{% highlight scala %}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}

val observations: RDD[Vector] = ... // an RDD of Vectors

// Compute column summary statistics.
val summary: MultivariateStatisticalSummary = Statistics.colStats(observations)
println(summary.mean) // a dense vector containing the mean value for each column
println(summary.variance) // column-wise variance
println(summary.numNonzeros) // number of nonzeros in each column

{% endhighlight %}
</div>

<div data-lang="java" markdown="1">

[`colStats()`](api/java/org/apache/spark/mllib/stat/Statistics.html) returns an instance of
[`MultivariateStatisticalSummary`](api/java/org/apache/spark/mllib/stat/MultivariateStatisticalSummary.html),
which contains the column-wise max, min, mean, variance, and number of nonzeros, as well as the
total count.

Refer to the [`MultivariateStatisticalSummary` Java docs](api/java/org/apache/spark/mllib/stat/MultivariateStatisticalSummary.html) for details on the API.

{% highlight java %}
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.stat.Statistics;

JavaSparkContext jsc = ...

JavaRDD<Vector> mat = ... // an RDD of Vectors

// Compute column summary statistics.
MultivariateStatisticalSummary summary = Statistics.colStats(mat.rdd());
System.out.println(summary.mean()); // a dense vector containing the mean value for each column
System.out.println(summary.variance()); // column-wise variance
System.out.println(summary.numNonzeros()); // number of nonzeros in each column

{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
[`colStats()`](api/python/pyspark.mllib.html#pyspark.mllib.stat.Statistics.colStats) returns an instance of
[`MultivariateStatisticalSummary`](api/python/pyspark.mllib.html#pyspark.mllib.stat.MultivariateStatisticalSummary),
which contains the column-wise max, min, mean, variance, and number of nonzeros, as well as the
total count.

Refer to the [`MultivariateStatisticalSummary` Python docs](api/python/pyspark.mllib.html#pyspark.mllib.stat.MultivariateStatisticalSummary) for more details on the API.

{% highlight python %}
from pyspark.mllib.stat import Statistics

sc = ... # SparkContext

mat = ... # an RDD of Vectors

# Compute column summary statistics.
summary = Statistics.colStats(mat)
print(summary.mean())
print(summary.variance())
print(summary.numNonzeros())

{% endhighlight %}
</div>

</div>

## Correlations

Calculating the correlation between two series of data is a common operation in Statistics. In `spark.mllib`
we provide the flexibility to calculate pairwise correlations among many series. The supported 
correlation methods are currently Pearson's and Spearman's correlation.
 
<div class="codetabs">
<div data-lang="scala" markdown="1">
[`Statistics`](api/scala/index.html#org.apache.spark.mllib.stat.Statistics$) provides methods to 
calculate correlations between series. Depending on the type of input, two `RDD[Double]`s or 
an `RDD[Vector]`, the output will be a `Double` or the correlation `Matrix` respectively.

Refer to the [`Statistics` Scala docs](api/scala/index.html#org.apache.spark.mllib.stat.Statistics) for details on the API.

{% highlight scala %}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.stat.Statistics

val sc: SparkContext = ...

val seriesX: RDD[Double] = ... // a series
val seriesY: RDD[Double] = ... // must have the same number of partitions and cardinality as seriesX

// compute the correlation using Pearson's method. Enter "spearman" for Spearman's method. If a 
// method is not specified, Pearson's method will be used by default. 
val correlation: Double = Statistics.corr(seriesX, seriesY, "pearson")

val data: RDD[Vector] = ... // note that each Vector is a row and not a column

// calculate the correlation matrix using Pearson's method. Use "spearman" for Spearman's method.
// If a method is not specified, Pearson's method will be used by default. 
val correlMatrix: Matrix = Statistics.corr(data, "pearson")

{% endhighlight %}
</div>

<div data-lang="java" markdown="1">
[`Statistics`](api/java/org/apache/spark/mllib/stat/Statistics.html) provides methods to 
calculate correlations between series. Depending on the type of input, two `JavaDoubleRDD`s or 
a `JavaRDD<Vector>`, the output will be a `Double` or the correlation `Matrix` respectively.

Refer to the [`Statistics` Java docs](api/java/org/apache/spark/mllib/stat/Statistics.html) for details on the API.

{% highlight java %}
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.*;
import org.apache.spark.mllib.stat.Statistics;

JavaSparkContext jsc = ...

JavaDoubleRDD seriesX = ... // a series
JavaDoubleRDD seriesY = ... // must have the same number of partitions and cardinality as seriesX

// compute the correlation using Pearson's method. Enter "spearman" for Spearman's method. If a 
// method is not specified, Pearson's method will be used by default. 
Double correlation = Statistics.corr(seriesX.srdd(), seriesY.srdd(), "pearson");

JavaRDD<Vector> data = ... // note that each Vector is a row and not a column

// calculate the correlation matrix using Pearson's method. Use "spearman" for Spearman's method.
// If a method is not specified, Pearson's method will be used by default. 
Matrix correlMatrix = Statistics.corr(data.rdd(), "pearson");

{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
[`Statistics`](api/python/pyspark.mllib.html#pyspark.mllib.stat.Statistics) provides methods to 
calculate correlations between series. Depending on the type of input, two `RDD[Double]`s or 
an `RDD[Vector]`, the output will be a `Double` or the correlation `Matrix` respectively.

Refer to the [`Statistics` Python docs](api/python/pyspark.mllib.html#pyspark.mllib.stat.Statistics) for more details on the API.

{% highlight python %}
from pyspark.mllib.stat import Statistics

sc = ... # SparkContext

seriesX = ... # a series
seriesY = ... # must have the same number of partitions and cardinality as seriesX

# Compute the correlation using Pearson's method. Enter "spearman" for Spearman's method. If a 
# method is not specified, Pearson's method will be used by default. 
print(Statistics.corr(seriesX, seriesY, method="pearson"))

data = ... # an RDD of Vectors
# calculate the correlation matrix using Pearson's method. Use "spearman" for Spearman's method.
# If a method is not specified, Pearson's method will be used by default. 
print(Statistics.corr(data, method="pearson"))

{% endhighlight %}
</div>

</div>

## Stratified sampling

Unlike the other statistics functions, which reside in `spark.mllib`, stratified sampling methods,
`sampleByKey` and `sampleByKeyExact`, can be performed on RDD's of key-value pairs. For stratified
sampling, the keys can be thought of as a label and the value as a specific attribute. For example 
the key can be man or woman, or document ids, and the respective values can be the list of ages 
of the people in the population or the list of words in the documents. The `sampleByKey` method 
will flip a coin to decide whether an observation will be sampled or not, therefore requires one 
pass over the data, and provides an *expected* sample size. `sampleByKeyExact` requires significant 
more resources than the per-stratum simple random sampling used in `sampleByKey`, but will provide
the exact sampling size with 99.99% confidence. `sampleByKeyExact` is currently not supported in 
python.

<div class="codetabs">
<div data-lang="scala" markdown="1">
[`sampleByKeyExact()`](api/scala/index.html#org.apache.spark.rdd.PairRDDFunctions) allows users to
sample exactly $\lceil f_k \cdot n_k \rceil \, \forall k \in K$ items, where $f_k$ is the desired 
fraction for key $k$, $n_k$ is the number of key-value pairs for key $k$, and $K$ is the set of
keys. Sampling without replacement requires one additional pass over the RDD to guarantee sample 
size, whereas sampling with replacement requires two additional passes.

{% highlight scala %}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.PairRDDFunctions

val sc: SparkContext = ...

val data = ... // an RDD[(K, V)] of any key value pairs
val fractions: Map[K, Double] = ... // specify the exact fraction desired from each key

// Get an exact sample from each stratum
val approxSample = data.sampleByKey(withReplacement = false, fractions)
val exactSample = data.sampleByKeyExact(withReplacement = false, fractions)

{% endhighlight %}
</div>

<div data-lang="java" markdown="1">
[`sampleByKeyExact()`](api/java/org/apache/spark/api/java/JavaPairRDD.html) allows users to
sample exactly $\lceil f_k \cdot n_k \rceil \, \forall k \in K$ items, where $f_k$ is the desired 
fraction for key $k$, $n_k$ is the number of key-value pairs for key $k$, and $K$ is the set of
keys. Sampling without replacement requires one additional pass over the RDD to guarantee sample 
size, whereas sampling with replacement requires two additional passes.

{% highlight java %}
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

JavaSparkContext jsc = ...

JavaPairRDD<K, V> data = ... // an RDD of any key value pairs
Map<K, Object> fractions = ... // specify the exact fraction desired from each key

// Get an exact sample from each stratum
JavaPairRDD<K, V> approxSample = data.sampleByKey(false, fractions);
JavaPairRDD<K, V> exactSample = data.sampleByKeyExact(false, fractions);

{% endhighlight %}
</div>
<div data-lang="python" markdown="1">
[`sampleByKey()`](api/python/pyspark.html#pyspark.RDD.sampleByKey) allows users to
sample approximately $\lceil f_k \cdot n_k \rceil \, \forall k \in K$ items, where $f_k$ is the 
desired fraction for key $k$, $n_k$ is the number of key-value pairs for key $k$, and $K$ is the 
set of keys.

*Note:* `sampleByKeyExact()` is currently not supported in Python.

{% highlight python %}

sc = ... # SparkContext

data = ... # an RDD of any key value pairs
fractions = ... # specify the exact fraction desired from each key as a dictionary

approxSample = data.sampleByKey(False, fractions);

{% endhighlight %}
</div>

</div>

## Hypothesis testing

Hypothesis testing is a powerful tool in statistics to determine whether a result is statistically 
significant, whether this result occurred by chance or not. `spark.mllib` currently supports Pearson's 
chi-squared ( $\chi^2$) tests for goodness of fit and independence. The input data types determine
whether the goodness of fit or the independence test is conducted. The goodness of fit test requires 
an input type of `Vector`, whereas the independence test requires a `Matrix` as input.

`spark.mllib` also supports the input type `RDD[LabeledPoint]` to enable feature selection via chi-squared 
independence tests.

<div class="codetabs">
<div data-lang="scala" markdown="1">
[`Statistics`](api/scala/index.html#org.apache.spark.mllib.stat.Statistics$) provides methods to 
run Pearson's chi-squared tests. The following example demonstrates how to run and interpret 
hypothesis tests.

{% highlight scala %}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.stat.Statistics._

val sc: SparkContext = ...

val vec: Vector = ... // a vector composed of the frequencies of events

// compute the goodness of fit. If a second vector to test against is not supplied as a parameter, 
// the test runs against a uniform distribution.  
val goodnessOfFitTestResult = Statistics.chiSqTest(vec)
println(goodnessOfFitTestResult) // summary of the test including the p-value, degrees of freedom, 
                                 // test statistic, the method used, and the null hypothesis.

val mat: Matrix = ... // a contingency matrix

// conduct Pearson's independence test on the input contingency matrix
val independenceTestResult = Statistics.chiSqTest(mat) 
println(independenceTestResult) // summary of the test including the p-value, degrees of freedom...

val obs: RDD[LabeledPoint] = ... // (feature, label) pairs.

// The contingency table is constructed from the raw (feature, label) pairs and used to conduct
// the independence test. Returns an array containing the ChiSquaredTestResult for every feature 
// against the label.
val featureTestResults: Array[ChiSqTestResult] = Statistics.chiSqTest(obs)
var i = 1
featureTestResults.foreach { result =>
    println(s"Column $i:\n$result")
    i += 1
} // summary of the test 

{% endhighlight %}
</div>

<div data-lang="java" markdown="1">
[`Statistics`](api/java/org/apache/spark/mllib/stat/Statistics.html) provides methods to 
run Pearson's chi-squared tests. The following example demonstrates how to run and interpret 
hypothesis tests.

Refer to the [`ChiSqTestResult` Java docs](api/java/org/apache/spark/mllib/stat/test/ChiSqTestResult.html) for details on the API.

{% highlight java %}
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.*;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.mllib.stat.test.ChiSqTestResult;

JavaSparkContext jsc = ...

Vector vec = ... // a vector composed of the frequencies of events

// compute the goodness of fit. If a second vector to test against is not supplied as a parameter, 
// the test runs against a uniform distribution.  
ChiSqTestResult goodnessOfFitTestResult = Statistics.chiSqTest(vec);
// summary of the test including the p-value, degrees of freedom, test statistic, the method used, 
// and the null hypothesis.
System.out.println(goodnessOfFitTestResult);

Matrix mat = ... // a contingency matrix

// conduct Pearson's independence test on the input contingency matrix
ChiSqTestResult independenceTestResult = Statistics.chiSqTest(mat);
// summary of the test including the p-value, degrees of freedom...
System.out.println(independenceTestResult);

JavaRDD<LabeledPoint> obs = ... // an RDD of labeled points

// The contingency table is constructed from the raw (feature, label) pairs and used to conduct
// the independence test. Returns an array containing the ChiSquaredTestResult for every feature 
// against the label.
ChiSqTestResult[] featureTestResults = Statistics.chiSqTest(obs.rdd());
int i = 1;
for (ChiSqTestResult result : featureTestResults) {
    System.out.println("Column " + i + ":");
    System.out.println(result); // summary of the test
    i++;
}

{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
[`Statistics`](api/python/index.html#pyspark.mllib.stat.Statistics$) provides methods to
run Pearson's chi-squared tests. The following example demonstrates how to run and interpret
hypothesis tests.

Refer to the [`Statistics` Python docs](api/python/pyspark.mllib.html#pyspark.mllib.stat.Statistics) for more details on the API.

{% highlight python %}
from pyspark import SparkContext
from pyspark.mllib.linalg import Vectors, Matrices
from pyspark.mllib.regresssion import LabeledPoint
from pyspark.mllib.stat import Statistics

sc = SparkContext()

vec = Vectors.dense(...) # a vector composed of the frequencies of events

# compute the goodness of fit. If a second vector to test against is not supplied as a parameter,
# the test runs against a uniform distribution.
goodnessOfFitTestResult = Statistics.chiSqTest(vec)
print(goodnessOfFitTestResult) # summary of the test including the p-value, degrees of freedom,
                               # test statistic, the method used, and the null hypothesis.

mat = Matrices.dense(...) # a contingency matrix

# conduct Pearson's independence test on the input contingency matrix
independenceTestResult = Statistics.chiSqTest(mat)
print(independenceTestResult)  # summary of the test including the p-value, degrees of freedom...

obs = sc.parallelize(...)  # LabeledPoint(feature, label) .

# The contingency table is constructed from an RDD of LabeledPoint and used to conduct
# the independence test. Returns an array containing the ChiSquaredTestResult for every feature
# against the label.
featureTestResults = Statistics.chiSqTest(obs)

for i, result in enumerate(featureTestResults):
    print("Column $d:" % (i + 1))
    print(result)
{% endhighlight %}
</div>

</div>

Additionally, `spark.mllib` provides a 1-sample, 2-sided implementation of the Kolmogorov-Smirnov (KS) test
for equality of probability distributions. By providing the name of a theoretical distribution
(currently solely supported for the normal distribution) and its parameters, or a function to 
calculate the cumulative distribution according to a given theoretical distribution, the user can
test the null hypothesis that their sample is drawn from that distribution. In the case that the
user tests against the normal distribution (`distName="norm"`), but does not provide distribution
parameters, the test initializes to the standard normal distribution and logs an appropriate 
message.

<div class="codetabs">
<div data-lang="scala" markdown="1">
[`Statistics`](api/scala/index.html#org.apache.spark.mllib.stat.Statistics$) provides methods to
run a 1-sample, 2-sided Kolmogorov-Smirnov test. The following example demonstrates how to run
and interpret the hypothesis tests.

Refer to the [`Statistics` Scala docs](api/scala/index.html#org.apache.spark.mllib.stat.Statistics) for details on the API.

{% highlight scala %}
import org.apache.spark.mllib.stat.Statistics

val data: RDD[Double] = ... // an RDD of sample data

// run a KS test for the sample versus a standard normal distribution
val testResult = Statistics.kolmogorovSmirnovTest(data, "norm", 0, 1)
println(testResult) // summary of the test including the p-value, test statistic,
                    // and null hypothesis
                    // if our p-value indicates significance, we can reject the null hypothesis

// perform a KS test using a cumulative distribution function of our making
val myCDF: Double => Double = ...
val testResult2 = Statistics.kolmogorovSmirnovTest(data, myCDF)
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">
[`Statistics`](api/java/org/apache/spark/mllib/stat/Statistics.html) provides methods to
run a 1-sample, 2-sided Kolmogorov-Smirnov test. The following example demonstrates how to run
and interpret the hypothesis tests.

Refer to the [`Statistics` Java docs](api/java/org/apache/spark/mllib/stat/Statistics.html) for details on the API.

{% highlight java %}
import java.util.Arrays;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.mllib.stat.test.KolmogorovSmirnovTestResult;

JavaSparkContext jsc = ...
JavaDoubleRDD data = jsc.parallelizeDoubles(Arrays.asList(0.2, 1.0, ...));
KolmogorovSmirnovTestResult testResult = Statistics.kolmogorovSmirnovTest(data, "norm", 0.0, 1.0);
// summary of the test including the p-value, test statistic,
// and null hypothesis
// if our p-value indicates significance, we can reject the null hypothesis
System.out.println(testResult);
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
[`Statistics`](api/python/pyspark.mllib.html#pyspark.mllib.stat.Statistics) provides methods to
run a 1-sample, 2-sided Kolmogorov-Smirnov test. The following example demonstrates how to run
and interpret the hypothesis tests.

Refer to the [`Statistics` Python docs](api/python/pyspark.mllib.html#pyspark.mllib.stat.Statistics) for more details on the API.

{% highlight python %}
from pyspark.mllib.stat import Statistics

parallelData = sc.parallelize([1.0, 2.0, ... ])

# run a KS test for the sample versus a standard normal distribution
testResult = Statistics.kolmogorovSmirnovTest(parallelData, "norm", 0, 1)
print(testResult) # summary of the test including the p-value, test statistic,
                  # and null hypothesis
                  # if our p-value indicates significance, we can reject the null hypothesis
# Note that the Scala functionality of calling Statistics.kolmogorovSmirnovTest with
# a lambda to calculate the CDF is not made available in the Python API
{% endhighlight %}
</div>
</div>

### Streaming Significance Testing
`spark.mllib` provides online implementations of some tests to support use cases
like A/B testing. These tests may be performed on a Spark Streaming
`DStream[(Boolean,Double)]` where the first element of each tuple
indicates control group (`false`) or treatment group (`true`) and the
second element is the value of an observation.

Streaming significance testing supports the following parameters:

* `peacePeriod` - The number of initial data points from the stream to
ignore, used to mitigate novelty effects.
* `windowSize` - The number of past batches to perform hypothesis
testing over. Setting to `0` will perform cumulative processing using
all prior batches.


<div class="codetabs">
<div data-lang="scala" markdown="1">
[`StreamingTest`](api/scala/index.html#org.apache.spark.mllib.stat.test.StreamingTest)
provides streaming hypothesis testing.

{% include_example scala/org/apache/spark/examples/mllib/StreamingTestExample.scala %}
</div>
</div>


## Random data generation

Random data generation is useful for randomized algorithms, prototyping, and performance testing.
`spark.mllib` supports generating random RDDs with i.i.d. values drawn from a given distribution:
uniform, standard normal, or Poisson.

<div class="codetabs">
<div data-lang="scala" markdown="1">
[`RandomRDDs`](api/scala/index.html#org.apache.spark.mllib.random.RandomRDDs) provides factory
methods to generate random double RDDs or vector RDDs.
The following example generates a random double RDD, whose values follows the standard normal
distribution `N(0, 1)`, and then map it to `N(1, 4)`.

Refer to the [`RandomRDDs` Scala docs](api/scala/index.html#org.apache.spark.mllib.random.RandomRDDs) for details on the API.

{% highlight scala %}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.random.RandomRDDs._

val sc: SparkContext = ...

// Generate a random double RDD that contains 1 million i.i.d. values drawn from the
// standard normal distribution `N(0, 1)`, evenly distributed in 10 partitions.
val u = normalRDD(sc, 1000000L, 10)
// Apply a transform to get a random double RDD following `N(1, 4)`.
val v = u.map(x => 1.0 + 2.0 * x)
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">
[`RandomRDDs`](api/java/index.html#org.apache.spark.mllib.random.RandomRDDs) provides factory
methods to generate random double RDDs or vector RDDs.
The following example generates a random double RDD, whose values follows the standard normal
distribution `N(0, 1)`, and then map it to `N(1, 4)`.

Refer to the [`RandomRDDs` Java docs](api/java/org/apache/spark/mllib/random/RandomRDDs) for details on the API.

{% highlight java %}
import org.apache.spark.SparkContext;
import org.apache.spark.api.JavaDoubleRDD;
import static org.apache.spark.mllib.random.RandomRDDs.*;

JavaSparkContext jsc = ...

// Generate a random double RDD that contains 1 million i.i.d. values drawn from the
// standard normal distribution `N(0, 1)`, evenly distributed in 10 partitions.
JavaDoubleRDD u = normalJavaRDD(jsc, 1000000L, 10);
// Apply a transform to get a random double RDD following `N(1, 4)`.
JavaDoubleRDD v = u.map(
  new Function<Double, Double>() {
    public Double call(Double x) {
      return 1.0 + 2.0 * x;
    }
  });
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
[`RandomRDDs`](api/python/pyspark.mllib.html#pyspark.mllib.random.RandomRDDs) provides factory
methods to generate random double RDDs or vector RDDs.
The following example generates a random double RDD, whose values follows the standard normal
distribution `N(0, 1)`, and then map it to `N(1, 4)`.

Refer to the [`RandomRDDs` Python docs](api/python/pyspark.mllib.html#pyspark.mllib.random.RandomRDDs) for more details on the API.

{% highlight python %}
from pyspark.mllib.random import RandomRDDs

sc = ... # SparkContext

# Generate a random double RDD that contains 1 million i.i.d. values drawn from the
# standard normal distribution `N(0, 1)`, evenly distributed in 10 partitions.
u = RandomRDDs.normalRDD(sc, 1000000L, 10)
# Apply a transform to get a random double RDD following `N(1, 4)`.
v = u.map(lambda x: 1.0 + 2.0 * x)
{% endhighlight %}
</div>
</div>

## Kernel density estimation

[Kernel density estimation](https://en.wikipedia.org/wiki/Kernel_density_estimation) is a technique
useful for visualizing empirical probability distributions without requiring assumptions about the
particular distribution that the observed samples are drawn from. It computes an estimate of the
probability density function of a random variables, evaluated at a given set of points. It achieves
this estimate by expressing the PDF of the empirical distribution at a particular point as the the
mean of PDFs of normal distributions centered around each of the samples.

<div class="codetabs">

<div data-lang="scala" markdown="1">
[`KernelDensity`](api/scala/index.html#org.apache.spark.mllib.stat.KernelDensity) provides methods
to compute kernel density estimates from an RDD of samples. The following example demonstrates how
to do so.

Refer to the [`KernelDensity` Scala docs](api/scala/index.html#org.apache.spark.mllib.stat.KernelDensity) for details on the API.

{% highlight scala %}
import org.apache.spark.mllib.stat.KernelDensity
import org.apache.spark.rdd.RDD

val data: RDD[Double] = ... // an RDD of sample data

// Construct the density estimator with the sample data and a standard deviation for the Gaussian
// kernels
val kd = new KernelDensity()
  .setSample(data)
  .setBandwidth(3.0)

// Find density estimates for the given values
val densities = kd.estimate(Array(-1.0, 2.0, 5.0))
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">
[`KernelDensity`](api/java/index.html#org.apache.spark.mllib.stat.KernelDensity) provides methods
to compute kernel density estimates from an RDD of samples. The following example demonstrates how
to do so.

Refer to the [`KernelDensity` Java docs](api/java/org/apache/spark/mllib/stat/KernelDensity.html) for details on the API.

{% highlight java %}
import org.apache.spark.mllib.stat.KernelDensity;
import org.apache.spark.rdd.RDD;

RDD<Double> data = ... // an RDD of sample data

// Construct the density estimator with the sample data and a standard deviation for the Gaussian
// kernels
KernelDensity kd = new KernelDensity()
  .setSample(data)
  .setBandwidth(3.0);

// Find density estimates for the given values
double[] densities = kd.estimate(new double[] {-1.0, 2.0, 5.0});
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
[`KernelDensity`](api/python/pyspark.mllib.html#pyspark.mllib.stat.KernelDensity) provides methods
to compute kernel density estimates from an RDD of samples. The following example demonstrates how
to do so.

Refer to the [`KernelDensity` Python docs](api/python/pyspark.mllib.html#pyspark.mllib.stat.KernelDensity) for more details on the API.

{% highlight python %}
from pyspark.mllib.stat import KernelDensity

data = ... # an RDD of sample data

# Construct the density estimator with the sample data and a standard deviation for the Gaussian
# kernels
kd = KernelDensity()
kd.setSample(data)
kd.setBandwidth(3.0)

# Find density estimates for the given values
densities = kd.estimate([-1.0, 2.0, 5.0])
{% endhighlight %}
</div>

</div>
