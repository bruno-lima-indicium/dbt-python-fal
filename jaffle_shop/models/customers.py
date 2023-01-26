def model(dbt, fal):

    stg_customers_df = dbt.ref('stg_customers')
    stg_orders_df = dbt.ref('stg_orders')
    stg_payments_df = dbt.ref('stg_payments')

    customer_orders_df = stg_orders_df.groupby('customer_id').agg({
        'order_date': ['min', 'max'],
        'order_id': 'count'
    }).reset_index()
    customer_orders_df.columns = ['customer_id', 'first_order', 'most_recent_order', 'number_of_orders']

    customer_payments_df = stg_payments_df.merge(stg_orders_df[['order_id', 'customer_id']], on='order_id')
    customer_payments_df = customer_payments_df.groupby('customer_id').agg({'amount': 'sum'}).reset_index()
    customer_payments_df.columns = ['customer_id', 'total_amount']

    final_df = stg_customers_df.merge(customer_orders_df, on='customer_id').merge(customer_payments_df, on='customer_id')

    return final_df