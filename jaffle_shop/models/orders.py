def model(dbt, fal):

    stg_orders_df = dbt.ref('stg_orders')
    stg_payments_df = dbt.ref('stg_payments')

    payment_methods = ['credit_card', 'coupon', 'bank_transfer', 'gift_card']

    order_payments_df = stg_payments_df.groupby('order_id').agg({
        'payment_method': 'first',
        'amount': 'sum'
    }).reset_index()

    for payment_method in payment_methods:
        order_payments_df[f"{payment_method}_amount"] = order_payments_df.apply(lambda x: x['amount'] if x['payment_method'] == payment_method else 0, axis=1)

    final_df = stg_orders_df.merge(order_payments_df, on='order_id')

    return final_df