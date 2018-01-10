data = LOAD '../input/example.txt' AS (line:Chararray);
words = FOREACH data GENERATE FLATTEN(TOKENIZE(line, ' ')) as word;
grouped = GROUP words by word;
wordcount = FOREACH grouped GENERATE group, COUNT(words);
dump wordcount;
