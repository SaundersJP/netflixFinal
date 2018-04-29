data = LOAD 'testTest.txt' USING PigStorage(',') AS (movieID:int, userID:int, rating:float); 

singleMovie = FILTER data BY movieID == 8;
singleUsers = FOREACH singleMovie GENERATE userID AS movieUser:int;

trainData = LOAD 'testTrain.txt' AS (movieID:int, userID:int, rating:float);

otherMovies = FILTER trainData BY movieID != 8;
groupMovies = GROUP otherMovies BY userID;

totalUsers = FOREACH groupMovies GENERATE group AS userID:int, COUNT($1) AS numUsers:int;

joinedUsers = JOIN singleUsers BY movieUser LEFT OUTER, totalUsers BY userID;

prettyUsers = FOREACH joinedUsers GENERATE $1 AS userID:int, $2 AS numUsers:int;

outputUsers = FILTER prettyUsers BY ($1) is not null;

DUMP outputUsers;
