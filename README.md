# Price Transparency Data

This repository contains source code for the blog post series titled <b>A Practical Take On Processing Price Transparency Data</b>

- [Part A](https://medium.com/@CCH0/a-practical-take-on-processing-price-transparency-data-part-a-870619c8d48d)
- [Part B](https://medium.com/@CCH0/a-practical-take-on-processing-price-transparency-data-part-b-2d8707ab1522)
- [Part C](https://medium.com/@CCH0/a-practical-take-on-processing-price-transparency-data-part-c-a326e7d99704)

<br>

In Part A, we talk about downloading source machine readable data file using AWS Lambda running Python script.

In Part B, we talk about using Polars script to pre-process the source data and store data in Parquet format in S3.

In Part C, we talk about using both PySpark and Polars scripts to produce denormalized data partitioned by billing_code and store final data in Parquet format in S3.
