# Import required packages
import hsfs
from pyspark.sql.functions import unix_timestamp

def model(dbt, session):
    # Setup cluster usage
    dbt.config(
        submission_method="cluster",
        dataproc_cluster_name="hops-dbt",
    )

    # Read read_bigquery_data SQL model
    my_sql_model_df = dbt.ref("read_bigquery_data")

    # Convert timestamp column to long type
    my_sql_model_df = my_sql_model_df.withColumn("base_time", unix_timestamp(my_sql_model_df["base_time"]).cast("long"))

    # Pring a type of the model(Pyspark DataFrame)
    print(type(my_sql_model_df))

    # Connect to the Hopsworks feature store
    hsfs_connection = hsfs.connection(
        host="8a4602e0-3766-11ee-8156-f95d5f25a5b2.cloud.hopsworks.ai",
        project="dataproc_test",
        hostname_verification=False,
        api_key_value="z9GWZD6jkv1Druar.QyaXBE0fHLoyL4DvoqbiFzpVCatxhJhYM7lDholuxeoR3fOcFQETBbkVZzIG8iO0",
        engine='spark',
    )

    # Retrieve the metadata handle
    feature_store = hsfs_connection.get_feature_store()

    # Get or create Feature Group
    feature_group = feature_store.get_or_create_feature_group(
        name = 'weather_fg',
        description = 'Feature Group description',
        version = 1,
        primary_key = ['city_name', 'hour'],
        event_time = 'base_time',
        online_enabled = True,
    )

    # Insert data into Feature Group
    feature_group.insert(my_sql_model_df)   

    return my_sql_model_df