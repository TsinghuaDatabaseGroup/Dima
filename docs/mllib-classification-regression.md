---
layout: global
title: Classification and Regression - spark.mllib
displayTitle: Classification and Regression - spark.mllib
---

The `spark.mllib` package supports various methods for 
[binary classification](http://en.wikipedia.org/wiki/Binary_classification),
[multiclass
classification](http://en.wikipedia.org/wiki/Multiclass_classification), and
[regression analysis](http://en.wikipedia.org/wiki/Regression_analysis). The table below outlines
the supported algorithms for each type of problem.

<table class="table">
  <thead>
    <tr><th>Problem Type</th><th>Supported Methods</th></tr>
  </thead>
  <tbody>
    <tr>
      <td>Binary Classification</td><td>linear SVMs, logistic regression, decision trees, random forests, gradient-boosted trees, naive Bayes</td>
    </tr>
    <tr>
      <td>Multiclass Classification</td><td>logistic regression, decision trees, random forests, naive Bayes</td>
    </tr>
    <tr>
      <td>Regression</td><td>linear least squares, Lasso, ridge regression, decision trees, random forests, gradient-boosted trees, isotonic regression</td>
    </tr>
  </tbody>
</table>

More details for these methods can be found here:

* [Linear models](mllib-linear-methods.html)
  * [classification (SVMs, logistic regression)](mllib-linear-methods.html#classification)
  * [linear regression (least squares, Lasso, ridge)](mllib-linear-methods.html#linear-least-squares-lasso-and-ridge-regression)
* [Decision trees](mllib-decision-tree.html)
* [Ensembles of decision trees](mllib-ensembles.html)
  * [random forests](mllib-ensembles.html#random-forests)
  * [gradient-boosted trees](mllib-ensembles.html#gradient-boosted-trees-gbts)
* [Naive Bayes](mllib-naive-bayes.html)
* [Isotonic regression](mllib-isotonic-regression.html)
