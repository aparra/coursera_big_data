#!/usr/bin/env python

yelp_data = sqlCtx.load(
        source='com.databricks.spark.csv',
        header = 'true',
        inferSchema = 'true',
        path = 'file:///usr/lib/hue/apps/search/examples/collections/solr_configs_yelp_demo/index_data.csv')

def cool_mean(data):
    data.agg({'cool': 'mean'})

def cool_mean_top_reviews(data):
    top_reviews = data.filter('review_count >= 10')
    top_reviews.groupBy('stars').avg('cool')

def cool_mean_top_open_reviews(data):
    open_reviews = data.filter('review_count >= 10 and open is not null')
    open_reviews.groupBy('stars').avg('cool')

def count_by_states_top_open_reviews(data)
    open_reviews = data.filter('review_count >= 10 and open is not null')
    open_reviews.groupBy('state').count()

def count_by_business_id(data):
    data.groupBy('business_id').count()

print 'Q1:', cool_mean(yelp_data).show()

print 'Q2:', cool_mean_top_reviews(yelp_data).show()

print 'Q3:', cool_mean_top_open_reviews(yelp_data).show()

print 'Q4:', count_by_states_top_open_reviews(yelp_data).show()

print 'Q5:', count_by_business_id(yelp_data).show()

