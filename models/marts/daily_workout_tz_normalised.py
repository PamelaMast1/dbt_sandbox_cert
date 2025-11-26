from dateutil import tz
import pandas as pd

# This model will not run on Bigquery without additional
# config that is detailed here - https://docs.getdbt.com/guides/dbt-python-bigframes?step=1
def model(dbt, session):
    """
    Example dbt Python model that:
      - reads from the stg_workout SQL model
      - converts local timestamps in many time zones to UTC
      - returns a DataFrame dbt will materialise as a table/view
    """

    dbt.config(
        materialized="table",
        tags=["python", "timezone"]
    )

    workouts_df = dbt.ref("stg_workout")

    # Ensure we’re working with datetimes
    workouts_df["workout_timestamp"] = pd.to_datetime(
        workouts_df["workout_timestamp"]
    )

    def convert_to_utc(row):
        """
        Convert a naive local timestamp + timezone string
        into a timezone-aware UTC timestamp.
        """
        local_zone = tz.gettz(row["workout_timezone"])
        utc_zone = tz.UTC

        # Attach local timezone
        local_dt = row["workout_timestamp"].replace(tzinfo=local_zone)

        # Convert to UTC
        utc_dt = local_dt.astimezone(utc_zone)
        return utc_dt

    # Apply our conversion using dateutil
    workouts_df["workout_timestamp_utc"] = workouts_df.apply(
        convert_to_utc, axis=1
    )

    # You can add further aggregations here if you want a true fact table
    return workouts_df