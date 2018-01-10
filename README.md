# From Hadoop to Pig

## Slides

[Slides](pig.pdf) are available.

## Introduction

[Apache Pig](https://pig.apache.org/) is a platform for analysing large data sets that consists of:

* a high-level language for expressing data analysis programs
* coupled with infrastructure for evaluating these programs.

The salient property of Pig programs is that their structure is amenable to substantial parallelization, which in turns enables them to handle very large data sets.

Pig makes it easier to write significant map-reduce programs compared to writing the code directly e.g. in Java.

As an example, we'll demonstrate a simple program to count the occurrences of words in a text file, first via map-reduce code in java, and then in pig.

## Word Count, Using Map Reduce

Two useful references:

* [Map Reduce Tutorial](https://hadoop.apache.org/docs/r2.8.0/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html)
* [Hadoop on Mac OSx, part 2](https://amodernstory.com/2014/09/23/hadoop-on-mac-osx-yosemite-part-2/)

### Install Hadoop

Assuming you're working on a mac, follow the steps in [Hadoop on Mac OSx](https://amodernstory.com/2014/09/23/installing-hadoop-on-mac-osx-yosemite/).

Add the following to ~/.bashrc:

```
alias hstart="/usr/local/Cellar/hadoop/2.8.1/sbin/start-dfs.sh;/usr/local/Cellar/hadoop/2.8.1/sbin/start-yarn.sh"

alias hstop="/usr/local/Cellar/hadoop/2.8.1/sbin/stop-yarn.sh;/usr/local/Cellar/hadoop/2.8.1/sbin/stop-dfs.sh"

export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar
```

Start hadoop from a new terminal:

```
$ hstart
...
localhost: starting nodemanager, logging to /usr/local/Cellar/hadoop/2.8.1/libexec/logs/yarn-ludwiggj-nodemanager-graemes-mbp.out
```

### Create Required Directories and Input Data File

```
$ hdfs dfs -mkdir -p /user/ludwiggj/wordcount/input

$ hadoop fs -copyFromLocal ../input/example.txt wordcount/input

$ hdfs dfs -ls -R

drwxr-xr-x   - ludwiggj supergroup          0 2017-11-15 15:46 wordcount
drwxr-xr-x   - ludwiggj supergroup          0 2017-11-15 15:48 wordcount/input
-rw-r--r--   1 ludwiggj supergroup        100 2017-11-15 15:48 wordcount/input/example.txt
```

Check contents of input data file:

```
$ hdfs dfs -cat wordcount/input/example.txt

This is a hadoop post
Hadoop is a bigdata technology this is a known fact
A database stores records
```

### Compile Word Count Program

To compile the [WordCount.java](wordCount/map-reduce/WordCount.java) file:

```
$ cd wordCount/map-reduce
$ hadoop com.sun.tools.javac.Main WordCount.java
```

Hadoop requires the class file to be in a jar:

```
$ jar cf wc.jar WordCount*.class
```

### Run Word Count Program

```
$ hadoop jar wc.jar WordCount wordcount/input wordcount/output

$ hdfs dfs -ls -R

drwxr-xr-x   - ludwiggj supergroup          0 2017-11-15 16:21 wordcount
drwxr-xr-x   - ludwiggj supergroup          0 2017-11-15 15:48 wordcount/input
-rw-r--r--   1 ludwiggj supergroup        100 2017-11-15 15:48 wordcount/input/example.txt
drwxr-xr-x   - ludwiggj supergroup          0 2017-11-15 16:21 wordcount/output
-rw-r--r--   1 ludwiggj supergroup          0 2017-11-15 16:21 wordcount/output/_SUCCESS
-rw-r--r--   1 ludwiggj supergroup        120 2017-11-15 16:21 wordcount/output/part-r-00000
```

NOTE:

You might need to make some changes to allow yarn to manage map-reduce, as per these links:

* [Hadoop is not showing my job in the job tracker even though it is running](https://stackoverflow.com/questions/21345022/hadoop-is-not-showing-my-job-in-the-job-tracker-even-though-it-is-running)
* [Hadoop MapReduce Job stuck because auxService:mapreduce_shuffle does not exist](https://stackoverflow.com/questions/42262335/hadoop-mapreduce-job-stuck-because-auxservicemapreduce-shuffle-does-not-exist)


### View Program Output

```
$ hdfs dfs -cat wordcount/output/part-r-00000

A	1
Hadoop	1
This	1
a	3
bigdata	1
database	1
fact	1
hadoop	1
is	3
known	1
post	1
records	1
stores	1
technology	1
this	1
```

Alternatively, you can [view the files via a browser](http://localhost:50070/explorer.html#/user/ludwiggj).

NOTE: You must delete the output directory before running the program again:

```
$ hdfs dfs -rm -r -f wordcount/output
```

## Word Count, Using Pig

Based on [Word Count in Pig Latin](http://www.hadooplessons.info/2015/01/word-count-in-pig-latin.html)

### Installing Pig

Assuming you are running on a Mac:

```
$ brew install pig

$ pig -version
Apache Pig version 0.16.0 (r1746530)
compiled Jun 01 2016, 23:10:49
```

### Starting Pig

Start pig in interactive [local mode](https://pig.apache.org/docs/r0.16.0/start.html#execution-modes):

```
pig -x local

grunt>
```

### Loading Data

Pig can load data from a hadoop cluster or a local file. See [Difference between PIG local and mapreduce mode](https://stackoverflow.com/questions/11669394/difference-between-pig-local-and-mapreduce-mode) for futher details. Therefore Pig could load the data from the HDFS instance set up previously. However, in this case we'll run pig in local mode and load the same data from a local file.

In local mode, the [LOAD operator](https://pig.apache.org/docs/r0.16.0/basic.html#load) can load data from a [text file](wordCount/input/example.txt).

```
data = LOAD '../input/example.txt' AS (line:Chararray);

dump data;

(This is a hadoop post)
(hadoop is a bigdata technology)
(A database stores records)
```

The data also has a schema:

```
describe data;

data: {line: chararray}
```

### Tokenizing the Data

The data can be split into words by combining the [FOREACH...GENERATE](https://pig.apache.org/docs/r0.16.0/basic.html#foreach) operators and [TOKENIZE](https://pig.apache.org/docs/r0.16.0/func.html#tokenize) function:

```
words = FOREACH data GENERATE(TOKENIZE(line, ' '));

dump words;

({(This),(is),(a),(hadoop),(post)})
({(hadoop),(is),(a),(bigdata),(technology)})
```

The use of punctuation symbols e.g. (){} can be confusing when looking at the data. Key symbols:

| Symbol | Meaning                                                                                           |
| ------ | ------------------------------------------------------------------------------------------------- |
| ()     | Parentheses enclose one or more items. Parentheses are also used to indicate the tuple data type. |
| {}     | Curly brackets enclose two or more items, one of which is required.                               |
|        | Curly brackets also used to indicate the bag data type.                                           |
|        | In this case <> is used to indicate required items.                                               |

See [conventions](https://pig.apache.org/docs/r0.16.0/basic.html#Conventions) for more details.

The data schema:

```
describe words;

words: {bag_of_tokenTuples_from_line: {tuple_of_tokens: (token: chararray)}}
```

### Flattening the Data

The data can be simplified via the [FLATTEN](https://pig.apache.org/docs/r0.16.0/basic.html#flatten) operator:

```
words = FOREACH data GENERATE FLATTEN(TOKENIZE(line, ' '));

dump words;

(This)
(is)
(a)
(hadoop)
(post)
(hadoop)
(is)
(a)
(bigdata)
(technology)
```

The data schema:

```
describe words;

words: {bag_of_tokenTuples_from_line::token: chararray}
```

### Renaming A Relation for Use in a Subsequent Query

This time, note the addition of 'AS word':

```
words = FOREACH data GENERATE FLATTEN(TOKENIZE(line, ' ')) AS word;

dump words;

(This)
(is)
(a)
(hadoop)
(post)
(hadoop)
(is)
(a)
(bigdata)
(technology)
```

'word' now appears in the data schema, and can be used in subsequent queries:

```
describe words;

words: {word: chararray}
```

### Grouping Data

The [GROUP operator](https://pig.apache.org/docs/r0.16.0/basic.html#group) groups the data in one or more relations.

To group the data by each unique word:

```
grouped = GROUP words by word;

dump grouped

(a,{(a),(a)})
(is,{(is),(is)})
(This,{(This)})
(post,{(post)})
(hadoop,{(hadoop),(hadoop)})
(bigdata,{(bigdata)})
(technology,{(technology)})
```

The data schema:

```
describe grouped

grouped: {group: chararray,words: {(word: chararray)}}
```

### Counting the Words

Pig provides a standard [COUNT function](https://pig.apache.org/docs/r0.16.0/func.html#count):

```
wordcount = FOREACH grouped GENERATE group, COUNT(words);

dump wordcount

(a,2)
(is,2)
(This,1)
(post,1)
(hadoop,2)
(bigdata,1)
(technology,1)
```

The data schema:

```
describe wordcount

wordcount: {group: chararray,long}
```

### Running Pig from a File

The above commands can be combined into a single pig script, [wordcount.pig](wordCount/pig/wordcount.pig):

```
data = LOAD '../input/example.txt' AS (line:Chararray);
words = FOREACH data GENERATE FLATTEN(TOKENIZE(line, ' ')) as word;
grouped = GROUP words by word;
wordcount = FOREACH grouped GENERATE group, COUNT(words);
dump wordcount;
```

This can then be run from the command-line:

```
$ cd wordCount/pig
$ pig -x local wordcount.pig

(A,1)
(a,3)
(is,3)
(This,1)
(fact,1)
(post,1)
(this,1)
(known,1)
(Hadoop,1)
(hadoop,1)
(stores,1)
(bigdata,1)
(records,1)
(database,1)
(technology,1)
```

### UDFs

[UDFs](https://pig.apache.org/docs/r0.16.0/udf.html) (User Defined Functions) are a way to extend Pig's functionality. The basic pig commands are used to load, combine, shape and output data. UDFs allow you to process data in more specific ways. Pig supplies a number of [built-in functions](https://pig.apache.org/docs/r0.16.0/func.html) (UDFs) out of the box. COUNT, which we've already used, is an example of a built-in function.

[Upper.java](wordCount/pig/myudfs/Upper.java) is a UDF to convert a string into upper case. Most of the work is to unpack the string from the tuple supplied to the exec function.

```
package myudfs;

import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class Upper extends EvalFunc<String> {
  public String exec(Tuple input) throws IOException {
    if (input == null || input.size() == 0 || input.get(0) == null) {
      return null;
    }
    try {
      String str = (String)input.get(0);
      return str.toUpperCase();
    } catch(Exception e){
      throw new IOException("Caught exception processing input row ", e);
    }
  }
}
```

The UDF must be compiled against a pig jar file (see section "How to Write a Simple Eval Function" in [Writing Java UDFs](https://pig.apache.org/docs/r0.16.0/udf.html#udf-java) for further details.

Given that the pig.jar file is in the wordCount/pig/pig-jar directory, the UDF is compiled as follows:

```
$ cd wordCount/pig/myudfs
$ javac -cp ../pig-jar/pig.jar:/usr/local/Cellar/hadoop/2.8.1/libexec/share/hadoop/common/hadoop-common-2.8.1.jar Upper.java
```

The UDF is then packaged as a jar.

```
cd ..
jar -cf myudfs.jar myudfs/*.class
```

The UDF can then be used in a pig script. Here is the [wordcountUpper.pig](wordCount/pig/wordcountUpper.pig) script: 

```
// Include the jar file
REGISTER myudfs.jar;

data = LOAD '../input/example.txt' AS (line:Chararray);

words = FOREACH data GENERATE FLATTEN(TOKENIZE(line, ' ')) as word;

// Upper function is used here to change each word into upper case
upper_words = FOREACH words GENERATE myudfs.Upper(word) as upper_word;

grouped = GROUP upper_words by upper_word;

wordcount = FOREACH grouped GENERATE group, COUNT(upper_words);

dump wordcount;
```

The result shows that the same words in different cases are counted together: 

```
$ pig -x local wordcountUpper.pig

(A,4)
(IS,3)
(FACT,1)
(POST,1)
(THIS,2)
(KNOWN,1)
(HADOOP,2)
(STORES,1)
(BIGDATA,1)
(RECORDS,1)
(DATABASE,1)
(TECHNOLOGY,1)
```

UDFs can be tested using standard unit testing techniques.

### Pig Unit

[Pig unit](https://pig.apache.org/docs/r0.8.1/pigunit.html) can be used to test pig scripts without the need for an HDFS installation. See [pig-tested](wordCount/pig-tested) files for details.

Key files:

| File                                                                                                                | Description                         |
| ------------------------------------------------------------------------------------------------------------------- | ----------------------------------- |
| [Upper.java](wordCount/pig-tested/UDFs/src/main/java/com/examples/pig/udf/Upper.java)                               | UDF to convert string to upper case |
| [UpperTest.groovy](wordCount/pig-tested/UDFs/src/test/groovy/com/examples/pig/udf/UpperTest.groovy)                 | UDF unit test                       |
| [wordcountUpper.pig](wordCount/pig-tested/core/src/main/resources/wordcountUpper.pig)                               | Pig script                          | 
| [WordCountTest.groovy](wordCount/pig-tested/core/src/integration-test/groovy/com/examples/pig/WordCountTest.groovy) | Pig integration test using PigUnit  |

The code can be viewed in a text-editor or IDE. [IntelliJ Community Edition](https://www.jetbrains.com/idea/download) is free, and can be used to import the code as a gradle project. The tests can be run within the IDE.

Alternatively the tests can be run directly from gradle as follows:

```
$ cd wordCount/pig-tested/
$ ./gradlew test integrationTest
```

END
