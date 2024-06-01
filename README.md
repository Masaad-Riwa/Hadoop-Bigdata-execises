# Hadoop Big Data Exercises Project

This project contains a set of exercises designed to practice processing and analyzing large datasets using Hadoop. Each exercise focuses on different types of data processing tasks, ranging from word counts in textual files to PM10 pollution analysis. The exercises help users get hands-on experience with Hadoop's MapReduce functionality.


## Exercises

### Exercise 1: Word Count Problem (Single File)
**Input:** An unstructured textual file.  
**Output:** The number of occurrences of each word appearing at least once in the input file.

**Example:**

**Input File:**
Toy example file for Hadoop. Hadoop running example.

**Output Pairs:**
(toy, 1)
(example, 2)
(file, 1)
(for, 1)
(hadoop, 2)
(running, 1)


### Exercise 2: Word Count Problem (Multiple Files)
**Input:** A HDFS folder containing textual files.  
**Output:** The number of occurrences of each word appearing in at least one file of the collection.

**Example:**

**Input Files:**

*File 1:*
Toy example
file for Hadoop.
Hadoop running
example.


*File 2:*
Another file for Hadoop.


**Output Pairs:**
(another, 1)
(example, 2)
(file, 2)
(for, 2)
(hadoop, 3)
(running, 1)
(toy, 1)


### Exercise 3: PM10 Pollution Analysis (Single File)
**Input:** A structured textual file containing the daily value of PM10 for a set of sensors.  
Each line of the file has the format: `sensorId,date\tPM10 value (μg/m3)\n`  
**Output:** Report for each sensor the number of days with PM10 above a specific threshold (50 μg/m3).

**Example:**

**Input File:**
s1,2016-01-01 20.5
s2,2016-01-01 30.1
s1,2016-01-02 60.2
s2,2016-01-02 20.4
s1,2016-01-03 55.5
s2,2016-01-03 52.5


**Output Pairs:**
(s1, 2)
(s2, 1)


### Exercise 4: PM10 Pollution Analysis per City Zone
**Input:** A structured textual file containing the daily value of PM10 for a set of city zones.  
Each line of the file has the format: `zoneId,date\tPM10 value (μg/m3)\n`  
**Output:** Report for each zone the list of dates associated with a PM10 value above a specific threshold (50 μg/m3).

**Example:**

**Input File:**
zone1,2016-01-01 20.5
zone2,2016-01-01 30.1
zone1,2016-01-02 60.2
zone2,2016-01-02 20.4
zone1,2016-01-03 55.5
zone2,2016-01-03 52.5

**Output Pairs:**
(zone1, [2016-01-03, 2016-01-02])
(zone2, [2016-01-03])



### Exercise 5: Average PM10 Value
**Input:** A collection of structured textual CSV files containing the daily value of PM10 for a set of sensors.  
Each line of the files has the format: `sensorId,date,PM10 value (μg/m3)\n`  
**Output:** Report for each sensor the average value of PM10.

**Example:**

**Input File:**
s1,2016-01-01,20.5
s2,2016-01-01,30.1
s1,2016-01-02,60.2
s2,2016-01-02,20.4
s1,2016-01-03,55.5
s2,2016-01-03,52.5

**Output Pairs:**
(s1, 45.4)
(s2, 34.3)


### Exercise 6: Max and Min PM10 Value
**Input:** A collection of structured textual CSV files containing the daily value of PM10 for a set of sensors.  
Each line of the files has the format: `sensorId,date,PM10 value (μg/m3)\n`  
**Output:** Report for each sensor the maximum and minimum value of PM10.

**Example:**

**Input File:**
s1,2016-01-01,20.5
s2,2016-01-01,30.1
s1,2016-01-02,60.2
s2,2016-01-02,20.4
s1,2016-01-03,55.5
s2,2016-01-03,52.5

**Output Pairs:**
(s1, max=60.2_min=20.5)
(s2, max=52.5_min=20.4)


### Exercise 7: Inverted Index
**Input:** A textual file containing a set of sentences.  
Each line of the file has the format: `sentenceId\tsentence\n`  
**Output:** Report for each word `w` the list of `sentenceIds` of the sentences containing `w`. Do not consider the words “and”, “or”, “not”.

**Example:**

**Input File:**
Sentence#1 Hadoop or Spark
Sentence#2 Hadoop or Spark and Java
Sentence#3 Hadoop and Big Data

**Output Pairs:**
(hadoop, [Sentence#1, Sentence#2, Sentence#3])
(spark, [Sentence#1, Sentence#2])
(java, [Sentence#2])
(big, [Sentence#3])
(data, [Sentence#3])

### Exercise 8: Total and Average Monthly Income
**Input:** A structured textual CSV file containing the daily income of a company.  
Each line of the files has the format: `date\tdaily income\n`  
**Output:** 
1. Total income for each month of the year.
2. Average monthly income for each year considering only the months with a total income greater than 0.

**Example:**

**Input File:**
2015-11-01 1000
2015-11-02 1305
2015-12-01 500
2015-12-02 750
2016-01-01 345
2016-01-02 1145
2016-02-03 200
2016-02-04 500

**Output Pairs:**
(2015-11, 2305) (2015, 1777.5)
(2015-12, 1250)
(2016-01, 1490) (2016, 1095.0)
(2016-02, 700)


## How to Run the Exercises

1. Set up a Hadoop environment (Hadoop and HDFS).
2. Place the input files in the specified HDFS directories.
3. Run the provided MapReduce jobs for each exercise.
4. Retrieve and verify the output files from HDFS.

For detailed instructions on setting up Hadoop and running MapReduce jobs, refer to the official Hadoop documentation: Apache Hadoop Documentation.

Happy Data Processing!