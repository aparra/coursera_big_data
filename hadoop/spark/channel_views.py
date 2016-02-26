
def split_show_views(line):
    show, views = map(lambda e: e.strip(), line.strip().split(','))
    return (show, int(views))

def split_show_channel(line):
    show, channel = map(lambda e: e.strip(), line.strip().split(','))
    return (show, channel)

def extract_channel_views(show_views_channel): 
    _, channel_views = show_views_channel
    channel, views = channel_views
    return (channel, views)

def some_function(a, b):
    some_result = a + b
    return some_result

show_views_file = sc.textFile("input/join2_gennum?.txt")
show_views = show_views_file.map(split_show_views)

show_channel_file = sc.textFile("input/join2_genchan?.txt")
show_channel = show_channel_file.map(split_show_channel)

joined_dataset = show_channel.join(show_views)
channel_views = joined_dataset.map(extract_channel_views)

print channel_views.reduceByKey(some_function).collect()

