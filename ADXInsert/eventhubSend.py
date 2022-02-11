import asyncio
import math
import uuid
from calendar import c
from email.policy import default
from msilib.schema import Class
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData
from time import time, sleep
from datetime import datetime

from numpy import mat

class  EventProducer:

       
    def __init__(self, mathFunction):
        self.id = str(uuid.uuid4())
        self.mathFunction = mathFunction,
        self.seed = 0
        self.dir = 1


    def calcNextVal(self):
        # Step up and down
        if self.mathFunction[0] == 'Step':  
            if self.dir == 1:
                self.seed = self.seed + 1
            else :
                self.seed = self.seed - 1
            if self.seed == 59:
                self.dir = 0
            if self.seed == 0:
                self.dir = 1
            return self.seed
        # triangle
        elif self.mathFunction[0] == "Triangle":
            self.seed = self.seed + 1
            if self.seed == 60:
                self.seed = 0
            return self.seed
        # Cosinus
        elif self.mathFunction[0] == "Cosin":  
            self.seed = self.seed + 1
            angle = 30 + (30*math.cos(self.seed*2*math.pi/60)) 
            if self.seed == 60:
                self.seed = 0    
            return  angle 
        # Sinus
        else: 
            self.seed = self.seed + 1
            angle = 30 + (30*math.sin(self.seed*2*math.pi/60)) 
            if self.seed == 60:
                self.seed = 0
            return  angle   

    def printNextVal(self):
        print("ID:  %s   Value: %s" %(str(self.id),str(self.calcNextVal())))

    def genEvent(self):
        strJson = '{"value":%s, "TS": "%s"}' %(str(self.calcNextVal()), str(datetime.utcnow()))
        event_data = EventData(strJson)
        event_data.properties = {"event-type": "com.microsoft.samples.hello-event", "priority": 1, "score": 9.0, "bsID":self.id}
        return event_data


async def run():
    # Create a producer client to send messages to the event hub.
    # Specify a connection string to your event hubs namespace and
    # the event hub name.
    producer = EventHubProducerClient.from_connection_string(conn_str="Endpoint=sb://kaareseraseventhubcustomprops.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=oSAQhR6MWcTWRjkDo3Jlb+o0wNQXBRqrFGFPFQ6xbW8=", eventhub_name="testconn")
    async with producer:
        # Create a batch.
        event_data_batch = await producer.create_batch()

        # Add events to the batch.
        event_data_batch.add(sensor1.genEvent())
        event_data_batch.add(sensor2.genEvent())
        event_data_batch.add(sensor3.genEvent())
        event_data_batch.add(sensor4.genEvent())

        # Send the batch of events to the event hub.
        await producer.send_batch(event_data_batch)



sensor1 = EventProducer("Step")
sensor2 = EventProducer("Triangle")
sensor3 = EventProducer("Sin")
sensor4 = EventProducer("Cosin")

while True:
    sleep(1 - time() % 1) # run every 1 second... you can change that
    print("**************")
    sensor1.printNextVal()
    sensor2.printNextVal()
    sensor3.printNextVal()
    sensor4.printNextVal()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())