from cmath import exp
from typing import List
import logging
import json
import io
import azure.functions as func
from azure.kusto.data import KustoConnectionStringBuilder
from azure.kusto.data.data_format import DataFormat
from azure.kusto.ingest import (
    IngestionProperties,
    StreamDescriptor,
    ManagedStreamingIngestClient,
    IngestionResult
)

class ADXevent:
    """
    Class for the dataschema to be inserted int ADX.
    """

    def __init__(self, Bsid, value, ts):
        self.Bsid = Bsid
        self.value = value
        self.ts = ts

class Credential:
    """
    Class for the connection details for ADX.
    """
    def __init__(self, cluster_ingest_uri,database, table, client_id, client_secret, tenant_id, ingestion_mapping_reference ):

        self.cluster_ingest_uri = cluster_ingest_uri
        self.database=database
        self.table=table
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id
        self.ingestion_mapping_reference = ingestion_mapping_reference

def main(events: List[func.EventHubEvent]):
    """
    Function to be triggered when ever new data is avaiable from Eventhub.
    Data wil be comming in batches of 1 to n events, and each event will have a list of all propperties from all events in the batch
    Code can be expanded to get credentials from runtime enviroment and Azure Keyvault
    As JSON is injected, both table schema and JSON mapping in ADX need to be alligned with JSON Schema.
    """

    _adx_credetials = Credential(
        "https://ingest-homeasistantcluster.westeurope.kusto.windows.net",
        "HomeAssistant",
        "sensor",
        "b5253d02-c8f4-4a79-a0f0-818491ba2a1f",
        "AZP7Q~zqKuMvJ.NU__f22mQi8RH3QEa9BgVYK",
        "72f988bf-86f1-41af-91ab-2d7cd011db47",
        "sensor_json_mapping"
    )

    _adx_events = []
    _adx_json_str = ""

    #Loop through all events in batch
    for event_in_batch_count, event in enumerate(events):
        logging.info('Python EventHub trigger processed an event: %s',
                        event.get_body().decode('utf-8'))
        body = json.loads(event.get_body().decode('utf-8'))
        
        #Extract needed values from the eventbody
        value = body['value']
        ts = body['TS']
           
        #Extract and convert the propperties array into an object
        properties_array = str(event.metadata["PropertiesArray"])
        properties_array = properties_array.replace("'",'"')
        obj_properties_array = json.loads(properties_array)

        #Extract needed custom properties from properties list
        bsid = obj_properties_array[event_in_batch_count]['bsID']

        #Build the event to injected int ADX
        _adx_event = ADXevent(bsid, value, ts)
        _adx_events.append(_adx_event)
    
    #Write out all events to a single JSON string
    for _adx_event in _adx_events:
        _adx_json_str += json.dumps(_adx_event.__dict__)
    
    result = writeToAdx(_adx_json_str)

    try: 
        result = writeToAdx(_adx_json_str, _adx_credetials)   
        logging.info(result)
    except Exception as exp:
        logging.error(exp)



def writeToAdx(json_string: str, _adx_credetials: Credential ) -> IngestionResult: 
    """
    Function to write JSON string with a list of events to Azure Data Explorer.
    """

    #Setup injection client
    _ingestion_properties  = IngestionProperties(
        database=_adx_credetials.database,
        table=_adx_credetials.table,
        data_format=DataFormat.MULTIJSON,
        ingestion_mapping_reference=_adx_credetials.ingestion_mapping_reference
    )

    kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(
        _adx_credetials.cluster_ingest_uri, 
        _adx_credetials.client_id, 
        _adx_credetials.client_secret, 
        _adx_credetials.tenant_id)

    _client = ManagedStreamingIngestClient.from_dm_kcsb(kcsb)

    #Inject data
    _bytes_stream = io.StringIO(json_string)
    _stream_descriptor = StreamDescriptor(_bytes_stream)
    result = _client.ingest_from_stream(_stream_descriptor, ingestion_properties=_ingestion_properties)

    return result




   




