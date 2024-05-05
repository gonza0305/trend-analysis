
# tech-assignment

Spark based trending


Trending is something that usually is updated in real-time or (more realistically) near real-time. While this provides obvious benefits, we will focus for this assignment on determining trending topics in batch based on an available dataset, which was captured over a period of time.
We will be to create one or more Spark jobs that take a set of Twitter data and two user input topic as input and produces a comparison of trend history  for last week.

### Trending
We will loosely define a trending topic for a period in time as: words or phrases of which the [slope](https://en.wikipedia.org/wiki/Slope) of the frequency of occurrence is largest during said period in time.

What constitutes a word or phrase and whether the distinction between the two should be made is open for interpretation. So, you can get all fancy and do stop word removal, determine that certain combinations of words should be counted as one, do spell checking to classify duplicates, etc., or just split strings on whitespace. It is advisable to start out with the simplest possible thing.

Spark job which produces historical trend comparison for last week. 
The output is written in a flat file with two line result, each line should start with the topic and their historical trend for last 7 days.

