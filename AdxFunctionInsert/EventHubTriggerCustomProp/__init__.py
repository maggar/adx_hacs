from cmath import exp
from typing import List
import logging
import json
import io
import os
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

class ADX_client:
    """
    Injection client to inject data into ADX.
    """
    def __init__(self):
        """
        Init for setting up the client.
        """

        self.cluster_ingest_uri = os.environ["cluster_ingest_uri"],
        self.database = os.environ["database"],
        self.table = os.environ["table"],
        self.client_id = os.environ["client_id"],
        self.client_secret = os.environ["client_secret"],
        self.tenant_id = os.environ["tenant_id"],
        self.ingestion_mapping_reference = os.environ["ingestion_mapping_reference"]

        self._ingestion_properties  = IngestionProperties(
            database = self.database,
            table = self.table,
            data_format=DataFormat.MULTIJSON,
            ingestion_mapping_reference = self.ingestion_mapping_reference
        )

        kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(
            connection_string = str(self.cluster_ingest_uri), 
            aad_app_id = str(self.client_id), 
            app_key = str(self.client_secret), 
            authority_id = str(self.tenant_id)
        )

        self.client = ManagedStreamingIngestClient.from_dm_kcsb(kcsb)

    def inject_data(self, json_string: str) -> IngestionResult:
        """
        Function for injecting the corectly formated JSON.
        """
        _bytes_stream = io.StringIO(json_string)
        _stream_descriptor = StreamDescriptor(_bytes_stream)
        result = self.client.ingest_from_stream(_stream_descriptor, ingestion_properties=self.adx_client.ingestion_properties)
        return (result)


def main(events: List[func.EventHubEvent]):
    """
    Function to be triggered when ever new data is avaiable from Eventhub.
    Data wil be comming in batches of 1 to n events, and each event will have a list of all propperties from all events in the batch.
    Code can be expanded to get credentials from runtime enviroment and Azure Keyvault.
    As JSON is injected, JSON mapping in ADX need to be alligned with JSON schema and Table schema.
    """

    #Create a client for injecting data to ADX
    _adx_client = ADX_client()

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

    try: 
        #Inject data to ADX using the client
        result = _adx_client.inject_data(_adx_json_str)   
        logging.info(result)
    except Exception as exp:
        logging.error(exp)






   




