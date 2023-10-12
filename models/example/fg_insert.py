# Import required packages
import hsfs
from pyspark.sql.functions import unix_timestamp

def model(dbt, session):
    # Setup cluster usage
    dbt.config(
        submission_method="cluster",
        dataproc_cluster_name="hops-dbt",
    )

    # Read data_pipeline Python model
    data_pipeline = dbt.ref("data_pipeline")

    # Connect to the Hopsworks feature store
    hsfs_connection = hsfs.connection(
        host="be60e8e0-68dc-11ee-bc84-514626b986ca.cloud.hopsworks.ai",
        project="dbt",
        hostname_verification=False,
        api_key_value="ObMMR17zLKYl3NA3.vBgRcJsxJDubjNEQIo7SqgoCyaMzWVRJexSreOONg9pG6TQl0SpL0d1iEVqoYdZZ",
        engine='spark',
    )

    # Retrieve the metadata handle
    feature_store = hsfs_connection.get_feature_store()

    # Get or create Feature Group
    feature_group = feature_store.get_or_create_feature_group(
        name = 'weather',
        version = 1,
    )

    # Insert data into Feature Group
    feature_group.insert(data_pipeline)   

    return data_pipeline