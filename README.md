
# tech-assignment

Spark-based trending

Trending is something that usually is updated in real-time or (more realistically) near real-time. While this provides obvious benefits, we will focus for this assignment on determining trending topics in batch based on an available dataset, which was captured over a period of time.
We will be to create one or more Spark jobs that take a set of Twitter data and two user input topic as input and produces a comparison of trend history  for last week.

### Trending
We will loosely define a trending topic for a period in time as: words or phrases of which the [slope](https://en.wikipedia.org/wiki/Slope) of the frequency of occurrence is largest during said period in time.


This project is based on a Spark job which produces historical trend comparison for last week. 
The output is written in a flat file with two line result, each line should start with the topic and their historical trend for last 7 days.


### AWS integration
Pushing the branch into the project deploys the project to AWS. CodeBuild is activated, which creates the DockerImage of the project, which is stored in ECR and we can then run it in ECS.
