# AWS Deeque Demo

https://ajithshetty28.medium.com/deequ-i-mean-data-quality-a0e6c048469d


**Introduction**
Deequ is a library built on top of Apache Spark for defining “unit tests for data”, which measure data quality in large datasets.
Python users may also be interested in PyDeequ, a Python interface for Deequ. You can find PyDeequ on GitHub, readthedocs, and PyPI.
source: https://github.com/awslabs/deequ

**What does it do**

Amazon Deequ would help you in:

Metrics Computation: You can use Deequ to get the quality metrics like maximum, minimum, correlation, completeness etc. Once the metrics are calculated you can store the data in S3 to analyse at later point.

Constraint Verification: You may define the constraint verification and the Deequ will generates the data quality report.

Constraint Suggestion: Well Deequ is smart enough to generate automated constraints based on the data you define.
