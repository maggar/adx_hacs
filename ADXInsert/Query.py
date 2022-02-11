import json



    
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
import io


## For Insert
from azure.kusto.data import KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoServiceError, KustoAuthenticationError, KustoError

from azure.kusto.ingest import (
    QueuedIngestClient,
    IngestionProperties,
    FileDescriptor,
    BlobDescriptor,
    StreamDescriptor,
    KustoStreamingIngestClient,
    #ManagedStreamingIngestClient,
    #IngestionStatus,
)

## Query
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.data.helpers import dataframe_from_result_table

unsingfreecluster = False

if unsingfreecluster:

    #URIs
    clusterIngestURI = "https://ingest-kvc89a814059a4d45e7b16.kusto.windows.net"

    #Auth
    # In case you want to authenticate with AAD application.
    client_id = "b5253d02-c8f4-4a79-a0f0-818491ba2a1f"
    client_secret = "AZP7Q~zqKuMvJ.NU__f22mQi8RH3QEa9BgVYK"

    # read more at https://docs.microsoft.com/en-us/onedrive/find-your-office-365-tenant-id
    authority_id = "72f988bf-86f1-41af-91ab-2d7cd011db47"

    #Database
    database="HomeAssistant"
    table="file"

else:
    #URIs
    clusterIngestURI = "https://homeasistantcluster.westeurope.kusto.windows.net"

    #Auth
    # In case you want to authenticate with AAD application.
    client_id = "b5253d02-c8f4-4a79-a0f0-818491ba2a1f"
    client_secret = "AZP7Q~zqKuMvJ.NU__f22mQi8RH3QEa9BgVYK"

    # read more at https://docs.microsoft.com/en-us/onedrive/find-your-office-365-tenant-id
    authority_id = "72f988bf-86f1-41af-91ab-2d7cd011db47"

    #Database
    database="HomeAssistant"
    table="test"

#################################################################
##                              AUTH                            ##
##################################################################

kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(clusterIngestURI, client_id, client_secret, authority_id)

# The authentication method will be taken from the chosen KustoConnectionStringBuilder.
#client = QueuedIngestClient(kcsb)
client = KustoClient(kcsb)

# there are more options for authenticating - see azure-kusto-data samples



##################################################################
##                        Query                                 ##
##################################################################

query = table + " | take 1"

try:
    response = client.execute(database, query)
except KustoServiceError as exp:
    print (exp)
except KustoAuthenticationError as exp:
    print (exp)
except KustoError as exp:
    print (exp)


tx = "Done"