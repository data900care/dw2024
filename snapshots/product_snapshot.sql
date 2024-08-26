{% snapshot product_snapshot_check %}

    {{
        config(
          target_schema='dbt_snapshots',
          strategy='check',
          unique_key='sku',
          check_cols=['name', 'Category','Type','Quantity_Refill','basket_Size_Quantity'],
          invalidate_hard_deletes=True
        )
    }}

    select * from {{ source('BIContent900', 'content900_Product_Variant') }}

{% endsnapshot %}