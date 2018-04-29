data = LOAD 'TrainingRatings.txt' USING PigStorage(',') AS (movieID:int, userID:int, rating:float);

groupedUsers = GROUP data BY userID;

stats = FOREACH groupedUsers GENERATE group AS userID:int, SUM(data.rating) AS totalRatings:float, COUNT(data) AS numRatings:int;

STORE stats INTO 'userStats' USING PigStorage(',');
