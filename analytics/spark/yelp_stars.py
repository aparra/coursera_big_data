#!/usr/bin/env python

yelp_data = sqlCtx.load(
        source='com.databricks.spark.csv',
        header = 'true',
        inferSchema = 'true',
        path = 'file:///usr/lib/hue/apps/search/examples/collections/solr_configs_yelp_demo/index_data.csv')

top_reviews = yelp_data.filter('review_count >= 10')
print top_reviews.groupBy('stars').avg('cool').orderBy('stars').show()

open_reviews = yelp_data.filter("review_count >= 10 and open = 'True'")
print open_reviews.groupBy('stars').avg('cool').orderBy('stars').show()

