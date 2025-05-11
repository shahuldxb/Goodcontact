import os
import logging
import json
import sys
import platform
import datetime
import traceback
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError, HttpResponseError
import pymssql

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Azure Storage settings
STORAGE_CONNECTION_STRING = os.environ.get(
    "AZURE_STORAGE_CONNECTION_STRING",
    "DefaultEndpointsProtocol=https;AccountName=infolder;AccountKey=NN3vJ8jLMvleobtI+l0ImQtilzSN5KPlC+JAmYHJi7iWKqZjkKg1sjW274/wDNSoPwqwIgQvVy5m+ASt+S+Mjw==;EndpointSuffix=core.windows.net"
)
SOURCE_CONTAINER = os.environ.get("AZURE_SOURCE_CONTAINER", "shahulin")
DESTINATION_CONTAINER = os.environ.get("AZURE_DESTINATION_CONTAINER", "shahulout")

# Azure SQL settings
SQL_SERVER = os.environ.get("AZURE_SQL_SERVER", "callcenter1.database.windows.net")
SQL_DATABASE = os.environ.get("AZURE_SQL_DATABASE", "call")
SQL_USER = os.environ.get("AZURE_SQL_USER", "shahul")
SQL_PASSWORD = os.environ.get("AZURE_SQL_PASSWORD", "apple123!@#")
SQL_PORT = int(os.environ.get("AZURE_SQL_PORT", "1433"))

def check_azure_storage():
    """
    Check Azure Storage connectivity and container existence
    """
    results = {
        "status": False,
        "connection": False,
        "source_container": False,
        "destination_container": False,
        "blob_count": {"source": 0, "destination": 0},
        "source_blobs": [],
        "destination_blobs": [],
        "errors": [],
        "account_details": {}
    }
    
    try:
        # Connect to Azure Storage
        blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
        
        # Get account properties
        account_info = blob_service_client.get_account_information()
        results["connection"] = True
        results["account_details"] = {
            "name": STORAGE_CONNECTION_STRING.split(";")[1].split("=")[1] if "AccountName=" in STORAGE_CONNECTION_STRING else "Unknown",
            "sku": account_info["sku_name"],
            "account_kind": account_info["account_kind"]
        }
        
        # Test source container
        try:
            container_client = blob_service_client.get_container_client(SOURCE_CONTAINER)
            # List blobs without parameters to avoid API version conflicts
            all_blobs = list(container_client.list_blobs())
            # Take just the first 5 blobs for verification
            blobs = all_blobs[:5] if all_blobs else []
            
            results["source_container"] = True
            results["blob_count"]["source"] = len(blobs)
            results["source_blobs"] = [blob.name for blob in blobs]
        except (ResourceNotFoundError, HttpResponseError) as e:
            results["errors"].append(f"Source container error: {str(e)}")
        
        # Test destination container
        try:
            container_client = blob_service_client.get_container_client(DESTINATION_CONTAINER)
            # List blobs without parameters to avoid API version conflicts
            all_blobs = list(container_client.list_blobs())
            # Take just the first 5 blobs for verification
            blobs = all_blobs[:5] if all_blobs else []
                
            results["destination_container"] = True
            results["blob_count"]["destination"] = len(blobs)
            results["destination_blobs"] = [blob.name for blob in blobs]
        except (ResourceNotFoundError, HttpResponseError) as e:
            results["errors"].append(f"Destination container error: {str(e)}")
            
        # Test writing a test blob
        try:
            test_container = blob_service_client.get_container_client(DESTINATION_CONTAINER)
            test_blob_name = f"test_health_check_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.txt"
            test_blob_client = test_container.get_blob_client(test_blob_name)
            test_blob_client.upload_blob("Health check test blob", overwrite=True)
            test_blob_client.delete_blob()
            results["write_test"] = True
        except Exception as e:
            results["errors"].append(f"Write test error: {str(e)}")
            results["write_test"] = False
            
        # Set overall status
        results["status"] = results["source_container"] and results["destination_container"]
    except Exception as e:
        results["errors"].append(f"Storage connection error: {str(e)}")
    
    return results

def check_azure_sql():
    """
    Check Azure SQL Database connectivity and basic queries
    """
    results = {
        "status": False,
        "connection": False,
        "schema": {
            "tables": [],
            "stored_procedures": []
        },
        "errors": [],
        "row_counts": {}
    }
    
    try:
        # Connect to SQL Database
        conn = pymssql.connect(
            server=SQL_SERVER,
            port=SQL_PORT,
            database=SQL_DATABASE,
            user=SQL_USER,
            password=SQL_PASSWORD,
            tds_version='7.4',
            as_dict=True
        )
        results["connection"] = True
        
        # Get tables
        cursor = conn.cursor()
        cursor.execute("""
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE'
        """)
        tables = cursor.fetchall()
        
        # Store table names and row counts
        results["schema"]["tables"] = [t["TABLE_NAME"] for t in tables]
        
        for table in results["schema"]["tables"]:
            try:
                cursor.execute(f"SELECT COUNT(*) AS count FROM [{table}]")
                count = cursor.fetchone()
                results["row_counts"][table] = count["count"] if count else 0
            except Exception as e:
                results["errors"].append(f"Error counting rows in {table}: {str(e)}")
        
        # Get stored procedures
        cursor.execute("""
            SELECT ROUTINE_NAME
            FROM INFORMATION_SCHEMA.ROUTINES
            WHERE ROUTINE_TYPE = 'PROCEDURE'
        """)
        procs = cursor.fetchall()
        results["schema"]["stored_procedures"] = [p["ROUTINE_NAME"] for p in procs]
        
        # Test specific tables that should exist
        expected_tables = [
            "rdt_assets", "rdt_sentiment", "rdt_language", 
            "rdt_summarization", "rdt_forbidden_phrases", 
            "rdt_topic_modeling", "rdt_speaker_diarization"
        ]
        
        results["missing_tables"] = [t for t in expected_tables if t not in results["schema"]["tables"]]
        
        # Set overall status
        results["status"] = results["connection"] and len(results["missing_tables"]) == 0
        
        cursor.close()
        conn.close()
    except Exception as e:
        error_str = str(e)
        results["errors"].append(f"SQL connection error: {error_str}")
        
        # Add more detailed error analysis for common errors
        if "18456" in error_str:
            results["errors"].append("SQL Error 18456: Login failed for user. This typically means either:")
            results["errors"].append("1. The username or password is incorrect")
            results["errors"].append("2. The user doesn't have permission to access this database")
            results["errors"].append("3. The user account is disabled or locked")
            results["errors"].append("4. The database server may be rejecting connections from this IP address")
    
    return results

def run_health_check():
    """
    Run health checks for all Azure services
    """
    # Get system info
    system_info = {
        "timestamp": datetime.datetime.now().isoformat(),
        "os": platform.system(),
        "python_version": platform.python_version(),
        "libraries": {
            "azure-storage-blob": "12.x",  # Get actual version if needed
            "pymssql": "2.x"              # Get actual version if needed
        }
    }
    
    # Run health checks
    storage_results = check_azure_storage()
    sql_results = check_azure_sql()
    
    # Compile overall results
    overall_results = {
        "system_info": system_info,
        "azure_storage": storage_results,
        "azure_sql": sql_results,
        "overall_status": storage_results["status"] and sql_results["status"]
    }
    
    return overall_results

def main():
    """
    Main function to run health checks and display results
    """
    try:
        results = run_health_check()
        
        # Save detailed results to file
        filename = f"azure_health_check_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(filename, 'w') as f:
            json.dump(results, f, indent=2)
        
        # Display summary results
        print("Azure Services Health Check")
        print("==========================")
        
        # Overall status
        print(f"\nOverall Status: {'Success' if results['overall_status'] else 'Failed'}")
        print(f"Check Time: {results['system_info']['timestamp']}")
        
        print("\nService Status Summary:")
        print("--------------------------------------------------")
        storage_status = "✅ Success" if results["azure_storage"]["status"] else "❌ Failed"
        sql_status = "✅ Success" if results["azure_sql"]["status"] else "❌ Failed"
        print(f"{storage_status} Azure Storage")
        print(f"{sql_status} Azure SQL Database")
        
        print("\nDetailed Results:")
        print("--------------------------------------------------")
        
        # Azure Storage details
        print("\nAzure Storage ({}):" .format("Success" if results["azure_storage"]["status"] else "Failed"))
        storage_connection = "✅ Connected" if results["azure_storage"]["connection"] else "❌ Failed"
        print(f"  - Connection: {storage_connection}")
        
        source_container = "✅ Found and Accessible" if results["azure_storage"]["source_container"] else "❌ Not Found/Access Denied"
        print(f"  - Source Container '{SOURCE_CONTAINER}': {source_container}")
        
        dest_container = "✅ Found and Accessible" if results["azure_storage"]["destination_container"] else "❌ Not Found/Access Denied"
        print(f"  - Destination Container '{DESTINATION_CONTAINER}': {dest_container}")
        
        if results["azure_storage"]["errors"]:
            print("  - Storage Errors:")
            for error in results["azure_storage"]["errors"]:
                print(f"    * {error}")
        
        # Azure SQL details
        print("\nAzure SQL Database ({}):" .format("Success" if results["azure_sql"]["status"] else "Failed"))
        sql_connection = "✅ Connected" if results["azure_sql"]["connection"] else "❌ Failed"
        print(f"  - Connection: {sql_connection}")
        
        if results["azure_sql"]["connection"]:
            print(f"  - Tables Found: {len(results['azure_sql']['schema']['tables'])}")
            print(f"  - Stored Procedures: {len(results['azure_sql']['schema']['stored_procedures'])}")
            
            if results["azure_sql"]["missing_tables"]:
                print("  - Missing Required Tables:")
                for table in results["azure_sql"]["missing_tables"]:
                    print(f"    * {table}")
        
        if results["azure_sql"]["errors"]:
            print("  - SQL Errors:")
            for error in results["azure_sql"]["errors"]:
                print(f"    * {error}")
        
        print(f"\nDetailed results saved to {filename}")
        
    except Exception as e:
        print(f"Error running health check: {str(e)}")
        traceback.print_exc()

if __name__ == "__main__":
    main()