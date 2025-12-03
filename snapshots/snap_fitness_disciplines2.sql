{% snapshot snap_fitness_disciplines2 %}
 {{
      config(
        schema = 'snapshots',          
        unique_key    = 'fitness_discipline', 
        strategy      = 'check',              
        check_cols    = ['is_active', 'fitness_discipline']          
      )
}}

    SELECT fitness_discipline,
           is_active
    FROM {{ ref('fitness_disciplines') }}
    
{% endsnapshot %}