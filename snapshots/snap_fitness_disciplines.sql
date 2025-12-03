{% snapshot snap_fitness_disciplines %}
 {{
      config(
        schema = 'snapshots',          
        unique_key    = 'fitness_discipline', 
        strategy      = 'check',              
        check_cols    = ['is_active'],
        invalidate_hard_deletes=true           
      )
}}

    SELECT fitness_discipline,
           is_active
    FROM {{ ref('fitness_disciplines') }}
    WHERE is_active = true -- if logic is needed then make an ephemeral model best practice
    
{% endsnapshot %}