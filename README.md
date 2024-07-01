# Employee Join Example with PySpark

## Table of Contents
1. [Introduction](#introduction)
2. [Setup and Installation](#setup-and-installation)
3. [Employee Join Analysis](#employee-join-analysis)
   - [Inner Join](#inner-join)
   - [Left Outer Join](#left-outer-join)
   - [Total Salary Calculation](#total-salary-calculation)
   - [Broadcast Join](#broadcast-join)
   - [Accumulator Example](#accumulator-example)
4. [Dependencies](#dependencies)
5. [Usage](#usage)
6. [Results](#results)

## Introduction
This project demonstrates how to use PySpark for performing various join operations on employee data. It includes examples of inner join, left outer join, total salary calculation, broadcast join, and accumulator usage.

## Setup and Installation
1. **Install PySpark:**
    ```sh
    pip install pyspark
    ```

2. **Prepare Input Data:**
    - Place your input text files (`emp1.txt` and `emp2.txt`) in the same directory as your scripts.

## Employee Join Analysis

### Inner Join
- Performs an inner join on two employee datasets to find common entries based on employee IDs.

### Left Outer Join
- Performs a left outer join on two employee datasets to include all entries from the left dataset and matching entries from the right dataset.

### Total Salary Calculation
- Calculates the total salary of employees by joining employee details with their respective salary and hours worked.

### Broadcast Join
- Demonstrates the use of broadcast variables to efficiently join small datasets with large datasets.

### Accumulator Example
- Uses an accumulator to sum values in an RDD.

## Dependencies
- `pyspark`

## Usage
1. **Start a PySpark Session and Load the Data:**
    ```python
    from pyspark import SparkContext, SparkConf

    conf = SparkConf().setAppName("Employee Join Example").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    ```

2. **Load and Process the Data:**
    - Load the employee datasets using `textFile`.
    - Perform various join operations and transformations as described in the analysis sections.

3. **Show Results:**
    - Display the results of each join operation using the `collect()` method.

## Results
The output of each join operation will display the respective results, such as lists of joined employee records and calculated total salaries.
