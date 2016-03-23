#!/usr/bin/env python

yelp_data = sqlCtx.load(
        source='com.databricks.spark.csv',
        header = 'true',
        inferSchema = 'true',
        path = 'file:///usr/lib/hue/apps/search/examples/collections/solr_configs_yelp_demo/index_data.csv')

print yelp_data.groupBy('business_id').count().agg({'count': 'max'}).show()

