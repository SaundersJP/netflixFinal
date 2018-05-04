ratings = LOAD '/netflix/unchangedData/TestingRatings.txt' USING PigStorage(',') AS (movieID:int, userID:int, rating:double);

similarity = LOAD '/netflix/output/part-r-00000' AS (itemPair:chararray, coefficient:double);

firstPass = FOREACH similarity GENERATE (int)REGEX_EXTRACT(itemPair, '(\\d+)', 1) as itemSELECT:int, (int)REGEX_EXTRACT(itemPair, ',([0-9]+)', 1) as item:int, coefficient;

grouped = GROUP firstPass BY itemSELECT;

topN = FOREACH grouped{
	sorted = ORDER firstPass BY coefficient DESC;
	limited = LIMIT sorted 5;
	GENERATE group, flatten(limited);
};

topNclean = FOREACH topN GENERATE $0 as movieSELECT:int, $2 as movieOther:int, $3 as coefficient:double, ($3 * $3) as coefficient2:double;

joined = JOIN topNclean BY movieOther, ratings BY movieID;
joinedClean = FOREACH joined GENERATE topNclean::movieSELECT AS movieSELECT, ratings::userID AS userID:int, (ratings::rating * topNclean::coefficient) AS rating:double, topNclean::coefficient2 AS coefficient2:double;

grouped2 = GROUP joinedClean BY (movieSELECT, userID);

ourRatings = FOREACH grouped2 GENERATE flatten(group) AS (movieID, userID), (SUM(joinedClean.rating)/SUM(joinedClean.coefficient2)) AS rating;

STORE ourRatings INTO '/netflix/newRatings/';
