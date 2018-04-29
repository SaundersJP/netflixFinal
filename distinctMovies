data = LOAD 'TestingRatings.txt' USING PigStorage(',') AS (movieID:int, userID:int, rating:float);

groupedMovies = GROUP data BY movieID;

outputMovies = FOREACH groupedMovies GENERATE group AS movieID:int;


DUMP outputMovies;
