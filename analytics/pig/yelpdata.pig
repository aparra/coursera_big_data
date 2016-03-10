
register /usr/lib/pig/piggybank.jar;
define CSVLoader org.apache.pig.piggybank.storage.CSVLoader();

yelp_data = LOAD '/usr/lib/hue/apps/search/examples/collections/solr_configs_yelp_demo/index_data.csv' USING CSVLoader() AS (business_id:chararray, cool, date, funny, id, stars:int, text:chararray, type, useful:int, user_id, name, full_address, latitude, longitude, neighborhoods, open, review_count, state);

good_data = FILTER yelp_data BY (useful is not null and stars is not null);

group_data = GROUP good_data ALL;
max_useful = FOREACH group_data GENERATE MAX(good_data.useful);
DUMP max_useful

limit_rate = FOREACH good_data 
             GENERATE business_id, stars, (useful > 5 ? 5 : useful) as useful_clipped;

rate = FOREACH limit_rate 
       GENERATE $0.., (double) stars * (useful_clipped / 5) as wtd_stars;

business_group = GROUP rate BY business_id;
averages = FOREACH business_group 
           GENERATE group as business_idgroup, COUNT(rate.stars) as num_ratings,
             AVG(rate.stars) as avg_stars,
             AVG(rate.useful_clipped) as avg_useful,
             AVG(rate.wtd_stars) as avg_wtdstars;

ranking = RANK averages BY avg_wtdstars;

business_without_worst = FILTER averages BY (num_ratings > 1);
business_without_worst_group = GROUP business ALL;

avg = FOREACH business_without_worst_group GENERATE AVG(business_without_worst.avg_wtdstars);
DUMP avg;

