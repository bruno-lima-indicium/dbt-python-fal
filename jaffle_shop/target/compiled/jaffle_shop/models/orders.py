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


# This part is user provided model code
# you will need to copy the next section to run the code
# COMMAND ----------
# this part is dbt logic for get ref work, do not modify

def ref(*args,dbt_load_df_function):
    refs = {"stg_orders": "jaffle-shop-375704.bruno.stg_orders", "stg_payments": "jaffle-shop-375704.bruno.stg_payments"}
    key = ".".join(args)
    return dbt_load_df_function(refs[key])


def source(*args, dbt_load_df_function):
    sources = {}
    key = ".".join(args)
    return dbt_load_df_function(sources[key])


config_dict = {}


class config:
    def __init__(self, *args, **kwargs):
        pass

    @staticmethod
    def get(key, default=None):
        return config_dict.get(key, default)

class this:
    """dbt.this() or dbt.this.identifier"""
    database = 'jaffle-shop-375704'
    schema = 'bruno'
    identifier = 'orders'
    def __repr__(self):
        return 'jaffle-shop-375704.bruno.orders'


class dbtObj:
    def __init__(self, load_df_function) -> None:
        self.source = lambda *args: source(*args, dbt_load_df_function=load_df_function)
        self.ref = lambda *args: ref(*args, dbt_load_df_function=load_df_function)
        self.config = config
        self.this = this()
        self.is_incremental = False

# COMMAND ----------


