data = LOAD 'testTest.txt' USING PigStorage(',') AS (movieID:int, userID:int, rating:float); 

singleUser = FILTER data BY userID == 2591126;
singleMovies = FOREACH singleUser GENERATE movieID AS userMovie:int;

trainData = LOAD 'testTrain.txt' AS (movieID:int, userID:int, rating:float);

otherUsers = FILTER trainData BY userID != 2591126;
groupMovies = GROUP otherUsers BY movieID;

totalMovies = FOREACH groupMovies GENERATE group AS movieID:int, COUNT($1) AS numViews:int;

joinedMovies = JOIN singleMovies BY userMovie LEFT OUTER, totalMovies BY movieID;

prettyMovies = FOREACH joinedMovies GENERATE $1 AS movieID:int, $2 AS numViews:int;

DUMP prettyMovies;
