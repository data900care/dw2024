{% snapshot customer_snapshot_check %}

    {{
        config(
          target_schema='dbt_snapshots',
          strategy='timestamp',
          unique_key='id',
          updated_at='updated_at'
        )
    }}

    select * from {{ source('shopify', 'customer') }}

{% endsnapshot %}