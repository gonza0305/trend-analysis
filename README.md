
# tech-assignment

Spark based trending

Note that sharing this assessment is disallowed.

For this assessment you will build a spark based solution to compare historical trend for any two given trending topic in Twitter data. The [Twitter trending topics](https://en.wikipedia.org/wiki/Twitter#Trending_topics) feature is bound to geographical areas, such as countries or cities, whereas for different purposes you may want to determine trending topics amongst arbitrary groups of Twitter users. For example all the followers of your corporate account or networks of people that are somehow of special interest to you.

## Assignment
Trending is something that usually is updated in real-time or (more realistically) near real-time. While this provides obvious benefits, we will focus for this assignment on determining trending topics in batch based on an available dataset, which was captured over a period of time.
The assignment will be to create one or more Spark jobs that take a set of Twitter data and two user input topic as input and produces a comparison of trend history  for last week.

### Trending
We will loosely define a trending topic for a period in time as: words or phrases of which the [slope](https://en.wikipedia.org/wiki/Slope) of the frequency of occurrence is largest during said period in time.

What constitutes a word or phrase and whether the distinction between the two should be made is open for interpretation. So, you can get all fancy and do stop word removal, determine that certain combinations of words should be counted as one, do spell checking to classify duplicates, etc., or just split strings on whitespace. It is advisable to start out with the simplest possible thing.

### Required solution

#### 1. A Spark job which produces historical trend comparison (80%)
Implement a Spark job which produces historical trend comparison for last week. See below for the requirements on languages, frameworks, and tools.
The output must be written in a flat file with two line result, each line should start with the topic and their historical trend for last 7 days.

#### 2. Presentation (20%)
See Evaluation below for the expectations of this presentation.

### Bonuspoints
If you have some time to spare, or want to show off your skills you could extend the assignment with one of the following and earn some bonus points:

- If the user provides one topic as input the program chooses the most trending topic from dataset and compares the trends with provided topic.
- Build a REST api to serve the trending topics. Eg, implement a `GET /api/trending_topics?param1=value&param2=value...&paramN=value` (determine your own parameters). You can choose any language/library to implement your API, and also run it either locally or in a container.
- Persist trending topics results into a NoSQL database as backend to our REST api.
- Deploy to a public cloud such as Ms Azure, AWS or GCloud and serve it from there.

### Languages / frameworks / tools
- Scala or Python
- Apache Spark
- Cassandra, HBase, MongoDB, Redis, ElasticSearch or another NoSQL database


### Data
Provided separately.



### Evaluation


While the given problem is seemingly simple, don't treat it as just a programming exercise for the sake of validating that you know how to write code (in that case, we'd have asked you to write Quicksort or some other very well defined problem). Instead, treat it as if there will be a real user depending on this solution for detecting trends in data. Obviously, you'd like to start with a simple solution that works, but we value evidence that you have thought about the problem and perhaps expose ideas for improvement upon this.

The goal of this assignment and the presentation is to assess candidates' skills in the following areas:

- Computer science
- Software development and craftmanship
- Distributed systems
- Coding productivity


Apart from the problem interpretation, we value evidence of solid software engineering principles, such as testability, separation of concerns, fit-for-production code, etc.

Also, do make sure that your solution runs against both the small and larger sample data set.

## Note
If you have any questions or want clarification on the requirements, please email sbhuinya@datapebbles.com
