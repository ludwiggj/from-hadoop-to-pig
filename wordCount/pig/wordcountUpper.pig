REGISTER myudfs.jar;

data = LOAD '../input/example.txt' AS (line:Chararray);
words = FOREACH data GENERATE FLATTEN(TOKENIZE(line, ' ')) as word;
upper_words = FOREACH words GENERATE myudfs.Upper(word) as upper_word;
grouped = GROUP upper_words by upper_word;
wordcount = FOREACH grouped GENERATE group, COUNT(upper_words);
dump wordcount;
