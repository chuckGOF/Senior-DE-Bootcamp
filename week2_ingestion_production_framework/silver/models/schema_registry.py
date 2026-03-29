schemas = {
    "orders": {
        "order_id": "int64",
        "customer_id": "int64",
        "item_type": "string",
        "unit": "int64",
        "price": "float",
        "order_date": "datetime64[ns]",
        "updated_at": "datetime64[ns]",
    },
    "customers": {
        "customer_id": "int64",
        "firstname": "string",
        "lastname": "string",
        "country": "string",
        "signup_date": "datetime64[ns]",
        "updated_at": "datetime64[ns]",
    },
}
