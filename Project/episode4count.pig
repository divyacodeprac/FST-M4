-- Load input file from HDFS
inputFile = LOAD 'hdfs:///user/root/divya/episode4.txt' USING PigStorage('\t') AS (line);
ranked = RANK inputFile;
lines = FILTER ranked BY (ranked_inputFile >2);
-- Tokeize each word in the file (Map)
words = FOREACH inputFile GENERATE FLATTEN(TOKENIZE(line)) AS word;
-- Combine the words from the above stage
grpd = GROUP words BY word;
-- Count the occurence of each word (Reduce)
cntd = FOREACH grpd GENERATE group, COUNT(words) AS cnt;
--Sort the result
counts_ordered = ORDER cntd BY cnt DESC;
-- Store the result in HDFS
STORE counts_ordered INTO 'hdfs:///user/root/resultPig1';
