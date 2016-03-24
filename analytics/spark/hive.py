
sqlCtx.sql("SELECT count(1)
              FROM orders 
             WHERE oder_status = 'SUSPECTED_FRAUD'").show()

sqlCtx.sql("SELECT order_item_order_id, sum(order_item_subtotal) as total 
              FROM order_items 
          GROUP BY order_item_order_id 
          ORDER BY total desc").show()

order_items = sqlCtx.sql("SELECT * FROM order_items")
order_items = sqlCtx.createDataFrame(order_items.rdd, order_items.schema)

orders = sqlCtx.sql("SELECT * FROM orders")
orders = sqlCtx.createDataFrame(orders.rdd, orders.schema)

status_price = orders.join(
        order_items, 
        order_items.order_item_order_id == orders.order_id, 
        "inner").select('order_item_product_price', 'order_status')
status_price.filter("order_status = 'COMPLETED'").agg({'order_item_product_price': 'mean'}).show()

order_price = orders.join(
        order_items, 
        order_items.order_item_order_id == orders.order_id, 
        "inner").select("order_id", "order_item_subtotal", "order_status")
order_price.filter("order_status != 'COMPLETE'").groupBy("order_id").sum("order_item_subtotal").collect()

customer_price = orders.join(
        order_items, 
        order_items.order_item_order_id == orders.order_id, 
        "inner").select("order_customer_id", "order_item_subtotal", "order_status")
customer_price.filter("order_status = 'COMPLETE'").groupBy("order_customer_id").sum("order_item_subtotal").collect()
