#!/usr/bin/env python3
"""
Azure Services Health Check

This script tests connectivity to Azure Storage and Azure SQL Database
and provides a detailed status report.
"""

import os
import sys
import json
import logging
import pymssql
from datetime import datetime
from azure.storage.blob import BlobServiceClient, BlobClient
from azure.core.exceptions import ResourceNotFoundError, ServiceRequestError, HttpResponseError

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Azure Storage configuration
AZURE_STORAGE_CONNECTION_STRING = os.environ.get(
    "AZURE_STORAGE_CONNECTION_STRING", 
    "DefaultEndpointsProtocol=https;AccountName=infolder;AccountKey=NN3vJ8jLMvleobtI+l0ImQtilzSN5KPlC+JAmYHJi7iWKqZjkKg1sjW274/wDNSoPwqwIgQvVy5m+ASt+S+Mjw==;EndpointSuffix=core.windows.net"
)
SOURCE_CONTAINER = "shahulin"
DESTINATION_CONTAINER = "shahulout"

# Azure SQL Database configuration
DB_SERVER = os.environ.get("DB_SERVER", "callcenter1.database.windows.net")
DB_NAME = os.environ.get("DB_NAME", "callcenter")
DB_USER = os.environ.get("DB_USER", "shahul")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "apple123!@#")

def check_azure_storage():
    """
    Check Azure Storage connectivity and container existence
    """
    results = {
        "service": "Azure Storage",
        "status": "Unknown",
        "connection": False,
        "source_container": False,
        "destination_container": False,
        "blob_count": {"source": 0, "destination": 0},
        "timestamp": datetime.now().isoformat(),
        "errors": []
    }
    
    try:
        # Test connection to Azure Storage
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        
        # Simple connection test
        account_info = blob_service_client.get_account_information()
        results["connection"] = True
        results["account_info"] = {
            "sku_name": account_info["sku_name"], 
            "account_kind": account_info["account_kind"]
        }
        
        # Test source container
        try:
            container_client = blob_service_client.get_container_client(SOURCE_CONTAINER)
            # List a few blobs to verify access
            # Note: Some Azure SDK versions use max_results, others use num_results
            try:
                blobs = list(container_client.list_blobs(max_results=5))
            except TypeError:
                # Try with the alternate parameter name
                blobs = list(container_client.list_blobs(num_results=5))
            
            results["source_container"] = True
            results["blob_count"]["source"] = len(blobs)
            results["source_blobs"] = [blob.name for blob in blobs]
        except (ResourceNotFoundError, HttpResponseError) as e:
            results["errors"].append(f"Source container error: {str(e)}")
        
        # Test destination container
        try:
            container_client = blob_service_client.get_container_client(DESTINATION_CONTAINER)
            # List a few blobs to verify access
            # Note: Some Azure SDK versions use max_results, others use num_results
            try:
                blobs = list(container_client.list_blobs(max_results=5))
            except TypeError:
                # Try with the alternate parameter name
                blobs = list(container_client.list_blobs(num_results=5))
                
            results["destination_container"] = True
            results["blob_count"]["destination"] = len(blobs)
            results["destination_blobs"] = [blob.name for blob in blobs]
        except (ResourceNotFoundError, HttpResponseError) as e:
            results["errors"].append(f"Destination container error: {str(e)}")
        
        # Overall status
        if results["connection"] and results["source_container"] and results["destination_container"]:
            results["status"] = "Healthy"
        elif results["connection"] and (results["source_container"] or results["destination_container"]):
            results["status"] = "Degraded"
        else:
            results["status"] = "Failed"
            
    except Exception as e:
        results["status"] = "Failed"
        error_message = str(e)
        results["errors"].append(f"Storage connection error: {error_message}")
        
        # Provide more detailed information for common storage errors
        if "AuthenticationFailed" in error_message:
            results["errors"].append("Authentication Failed. This typically means:")
            results["errors"].append("1. The storage account key is incorrect or expired")
            results["errors"].append("2. The connection string format is invalid")
            
        if "ResourceNotFound" in error_message:
            results["errors"].append("Resource Not Found. This typically means:")
            results["errors"].append("1. The storage account name doesn't exist")
            results["errors"].append("2. The container name is incorrect")
            
        if "timeout" in error_message.lower():
            results["errors"].append("Connection timeout. This typically means:")
            results["errors"].append("1. Network connectivity issues to Azure services")
            results["errors"].append("2. Firewall rules blocking outbound connections")
    
    return results

def check_azure_sql():
    """
    Check Azure SQL Database connectivity and basic queries
    """
    results = {
        "service": "Azure SQL Database",
        "status": "Unknown",
        "connection": False,
        "tables": [],
        "row_counts": {},
        "timestamp": datetime.now().isoformat(),
        "errors": []
    }
    
    try:
        # Test connection to SQL Server
        conn = pymssql.connect(
            server=DB_SERVER,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        
        results["connection"] = True
        cursor = conn.cursor()
        
        # Get list of tables
        try:
            cursor.execute("""
                SELECT TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_TYPE = 'BASE TABLE'
                AND TABLE_NAME LIKE 'rdt%'
                ORDER BY TABLE_NAME
            """)
            
            tables = [row[0] for row in cursor.fetchall()]
            results["tables"] = tables
            
            # Get row counts for key tables
            for table in tables:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM [{table}]")
                    count = cursor.fetchone()[0]
                    results["row_counts"][table] = count
                except Exception as e:
                    results["errors"].append(f"Error counting rows in {table}: {str(e)}")
        
        except Exception as e:
            results["errors"].append(f"Error listing tables: {str(e)}")
        
        # Check for specific tables we need
        required_tables = ["rdt_assets", "rdt_paragraphs", "rdt_sentences"]
        results["missing_tables"] = [table for table in required_tables if table not in results["tables"]]
        
        # Overall status
        if results["connection"] and not results["missing_tables"] and not results["errors"]:
            results["status"] = "Healthy"
        elif results["connection"] and (results["tables"] or not results["missing_tables"]):
            results["status"] = "Degraded"
        else:
            results["status"] = "Failed"
        
        # Close connection
        conn.close()
        
    except Exception as e:
        results["status"] = "Failed"
        error_message = str(e)
        results["errors"].append(f"SQL connection error: {error_message}")
        
        # Provide more detailed information for common SQL errors
        if "18456" in error_message:  # Login failed error
            results["errors"].append("SQL Error 18456: Login failed for user. This typically means either:")
            results["errors"].append("1. The username or password is incorrect")
            results["errors"].append("2. The user doesn't have permission to access this database")
            results["errors"].append("3. The user account is disabled or locked")
            results["errors"].append("4. The database server may be rejecting connections from this IP address")
            
        if "timeout" in error_message.lower():
            results["errors"].append("Connection timeout. This typically means either:")
            results["errors"].append("1. The database server is unreachable (network issues)")
            results["errors"].append("2. The server is rejecting connections due to firewall settings")
            results["errors"].append("3. The server may be under heavy load or experiencing issues")
    
    return results

def run_health_check():
    """
    Run health checks for all Azure services
    """
    # Overall health status
    health_results = {
        "overall_status": "Unknown",
        "timestamp": datetime.now().isoformat(),
        "environment": {
            "DB_SERVER": DB_SERVER,
            "DB_NAME": DB_NAME,
            "DB_USER": DB_USER,
            "SOURCE_CONTAINER": SOURCE_CONTAINER,
            "DESTINATION_CONTAINER": DESTINATION_CONTAINER
        },
        "services": {}
    }
    
    # Check Azure Storage
    storage_results = check_azure_storage()
    health_results["services"]["azure_storage"] = storage_results
    
    # Check Azure SQL
    sql_results = check_azure_sql()
    health_results["services"]["azure_sql"] = sql_results
    
    # Determine overall status
    if all(service["status"] == "Healthy" for service in health_results["services"].values()):
        health_results["overall_status"] = "Healthy"
    elif any(service["status"] == "Failed" for service in health_results["services"].values()):
        health_results["overall_status"] = "Failed"
    else:
        health_results["overall_status"] = "Degraded"
    
    return health_results

def main():
    """
    Main function to run health checks and display results
    """
    print("\nAzure Services Health Check")
    print("==========================")\
    
    health_results = run_health_check()
    
    # Print status summary
    print(f"\nOverall Status: {health_results['overall_status']}")
    print(f"Check Time: {health_results['timestamp']}")
    print("\nService Status Summary:")
    print("-" * 50)
    
    for service_name, service_results in health_results["services"].items():
        status_symbol = "✅" if service_results["status"] == "Healthy" else "⚠️" if service_results["status"] == "Degraded" else "❌"
        print(f"{status_symbol} {service_results['service']}: {service_results['status']}")
    
    print("\nDetailed Results:")
    print("-" * 50)
    
    # Azure Storage details
    storage = health_results["services"]["azure_storage"]
    print(f"\nAzure Storage ({storage['status']}):")
    print(f"  - Connection: {'✅ Connected' if storage['connection'] else '❌ Failed'}")
    print(f"  - Source Container '{SOURCE_CONTAINER}': {'✅ Found' if storage['source_container'] else '❌ Not Found/Access Denied'}")
    print(f"  - Destination Container '{DESTINATION_CONTAINER}': {'✅ Found' if storage['destination_container'] else '❌ Not Found/Access Denied'}")
    
    if storage["connection"] and storage["source_container"]:
        print(f"  - Source Container Contents: {storage['blob_count']['source']} blobs found in sample")
        if storage['blob_count']['source'] > 0:
            print(f"    Sample files: {', '.join(storage['source_blobs'][:3])}" + ("..." if len(storage['source_blobs']) > 3 else ""))
    
    if storage["errors"]:
        print(f"  - Storage Errors:")
        for error in storage["errors"]:
            print(f"    * {error}")
    
    # Azure SQL details
    sql = health_results["services"]["azure_sql"]
    print(f"\nAzure SQL Database ({sql['status']}):")
    print(f"  - Connection: {'✅ Connected' if sql['connection'] else '❌ Failed'}")
    
    if sql["connection"]:
        print(f"  - Tables: {len(sql['tables'])} tables found")
        if sql['tables']:
            print(f"    Found tables: {', '.join(sql['tables'][:5])}" + ("..." if len(sql['tables']) > 5 else ""))
        
        if sql["missing_tables"]:
            print(f"  - Missing Required Tables: {', '.join(sql['missing_tables'])}")
        
        if sql["row_counts"]:
            print("  - Table Row Counts:")
            for table, count in sql["row_counts"].items():
                print(f"    * {table}: {count} rows")
    
    if sql["errors"]:
        print(f"  - SQL Errors:")
        for error in sql["errors"]:
            print(f"    * {error}")
    
    # Save results to JSON file
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"azure_health_check_{timestamp}.json"
        
        with open(filename, "w") as f:
            json.dump(health_results, f, indent=2)
        
        print(f"\nDetailed results saved to {filename}")
    except Exception as e:
        print(f"\nError saving results: {str(e)}")
    
    # Return exit code based on overall status
    if health_results["overall_status"] == "Failed":
        return 1
    return 0

if __name__ == "__main__":
    sys.exit(main())