data = LOAD 'testTrain.txt' AS (movieID:int, userID:int, rating:float);

averages = LOAD 'userAverages.txt' USING PigStorage(',') AS (userID:int, totalRatings:float, numRatings:int);

joined = JOIN data BY userID LEFT OUTER, averages BY userID;

formatted = FOREACH joined GENERATE $0, $1, $2, $4, $5;

outputData = FOREACH formatted GENERATE $0, $1, ($2 - $3/$4);

DUMP outputData;
