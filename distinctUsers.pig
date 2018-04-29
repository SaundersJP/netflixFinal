data = LOAD 'TestingRatings.txt' USING PigStorage(',') AS (movieID:int, userID:int, rating:float);

groupedUsers = GROUP data BY userID;

outputUsers = FOREACH groupedUsers GENERATE group AS userID:int;

DUMP outputUsers;
