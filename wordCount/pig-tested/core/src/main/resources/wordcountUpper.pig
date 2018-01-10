REGISTER $REPORT_HOME/libs/UDFs-1.0-SNAPSHOT.jar;

data = LOAD '../input/example.txt' AS (line:Chararray);
words = FOREACH data GENERATE FLATTEN(TOKENIZE(line, ' ')) as word;
upper_words = FOREACH words GENERATE com.examples.pig.udf.Upper(word) as upper_word;
grouped = GROUP upper_words by upper_word;
wordcount = FOREACH grouped GENERATE group, COUNT(upper_words);