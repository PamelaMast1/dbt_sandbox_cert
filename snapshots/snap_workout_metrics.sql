{% snapshot snap_workout_metrics %}

  {{
    config(
      schema = 'snapshots',     
      unique_key    = 'workout_id',    
      strategy      = 'timestamp',
      updated_at    = 'workout_timestamp'
    )
  }}

  SELECT workout_id,
         workout_timestamp,
         workout_date,
         fitness_discipline,
         instructor,
         length_minutes,
         total_output,
         output_per_min,
         distance_km,
         calories_burned,
         avg_heartrate_bpm,
         avg_speed_kmh,
         intensity_band,
         avg_watts
  FROM {{ ref('workout_metrics') }}

{% endsnapshot %}