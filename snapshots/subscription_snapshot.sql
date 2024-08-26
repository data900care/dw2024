{% snapshot subscription_snapshot_check %}

    {{
        config(
          target_schema='dbt_snapshots',
          strategy='timestamp',
          unique_key='id',
          updated_at='updated_at', 
          invalidate_hard_deletes=True
        )
    }}

    select * from {{ source('recharge_airbyte', 'recharge_subscriptions') }}

{% endsnapshot %}