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


# This part is user provided model code
# you will need to copy the next section to run the code
# COMMAND ----------
# this part is dbt logic for get ref work, do not modify

def ref(*args,dbt_load_df_function):
    refs = {"stg_customers": "jaffle-shop-375704.bruno.stg_customers", "stg_orders": "jaffle-shop-375704.bruno.stg_orders", "stg_payments": "jaffle-shop-375704.bruno.stg_payments"}
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
    identifier = 'customers'
    def __repr__(self):
        return 'jaffle-shop-375704.bruno.customers'


class dbtObj:
    def __init__(self, load_df_function) -> None:
        self.source = lambda *args: source(*args, dbt_load_df_function=load_df_function)
        self.ref = lambda *args: ref(*args, dbt_load_df_function=load_df_function)
        self.config = config
        self.this = this()
        self.is_incremental = False

# COMMAND ----------


# Generated by dbt-fal

def main(read_df, write_df, fal_context=None):
  dbt_context = dbtObj(read_df)
  df = model(dbt_context, fal_context)
  return write_df(
      'jaffle-shop-375704.bruno.customers',
      df
  )