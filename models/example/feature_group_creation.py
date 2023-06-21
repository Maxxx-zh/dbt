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

    # Returns Pyspark DataFrame
    print(type(my_sql_model_df))

    # Connect to the Hopsworks feature store
    hsfs_connection = hsfs.connection(
        host="35419860-0e83-11ee-bfef-b195b95727f3.cloud.hopsworks.ai",                                # DNS of your Feature Store instance
        project="tutorials",                      # Name of your Hopsworks Feature Store project
        hostname_verification=False,                     # Disable for self-signed certificates
        api_key_value="UJ6YQAnDdB2eDKiz.loF2EQC12lOMs8N8D6piYuk5OqvWWkjLI33ixVg0zFbhEGLsejaBgKaawxDLiyfd"  
    )

    # Retrieve the metadata handle
    feature_store = hsfs_connection.get_feature_store()

    # Retrieve storage connector
    connector = feature_store.get_storage_connector('dbt_bq_connector')  

    # Get or create Feature Group
    feature_group = feature_store.get_or_create_feature_group(
        name = 'weather_fg',
        description = 'Feature Group description',
        version = 1,
        primary_key = ['index_column'],
        online_enabled = True,
    )    

    # Insert data into Feature Group
    feature_group.insert(my_sql_model_df)   

    return my_sql_model_df