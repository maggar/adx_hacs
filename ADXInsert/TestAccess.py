
import traceback


## For Insert
from azure.kusto.data import KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoServiceError, KustoAuthenticationError, KustoError


## Query
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoServiceError


class adx_cluster:

    def __init__(self, cluster_query_uri, name) -> None:
        self.name = name
        self.cluster_query_uri = cluster_query_uri
        self.database = "HomeAssistant"
        self.table = "testConn"

    def test_connection(self, sp):
        print ("")
        print ("*****************************************************************************************")
        print ("************   Atempting connection to a %s with a %s" % (self.name,  sp.name))
        print ("*****************************************************************************************")
        
        # Create cLient for quereing data
        kcsbq = KustoConnectionStringBuilder.with_aad_application_key_authentication(
            self.cluster_query_uri, sp.client_id, sp.client_secret, sp.authority_id
        )

        self.query_client = KustoClient(kcsbq)

        query = "%s | count" %(self.table)

        try:
            self.query_client.execute(self.database, query)
        except Exception as exp:
            print("Failed with eception:")
            print(exp)
            #print(traceback.format_exc())
        else:
            print("Connection OK")

        return None

class SP:

    def __init__(self, client_id, client_secret, authority_id, name):
        self.client_id = client_id
        self.client_secret = client_secret
        self.authority_id = authority_id
        self.name = name


MS_Paid_cluster = adx_cluster("https://homeasistantcluster.westeurope.kusto.windows.net","Microsoft.com account Paid cluster")
MS_FREE_cluster = adx_cluster("https://kvc155560eb74764710a05.kusto.windows.net","Microsoft.com account FREE cluster")
WS_FREE_cluster = adx_cluster("https://kvc87e708ecd6604e8c8df.northeurope.kusto.windows.net","Work or school acount FREE cluster")
MSA_FREE_cluster = adx_cluster("https://kvc790617bafc0a4f23b2c.northeurope.kusto.windows.net","MSA_FREE_cluster")


MS_SP1 = SP("b5253d02-c8f4-4a79-a0f0-818491ba2a1f",
        "AZP7Q~zqKuMvJ.NU__f22mQi8RH3QEa9BgVYK",
        "72f988bf-86f1-41af-91ab-2d7cd011db47",
        "Microsoft.com account SP1"
)

WS_SP1 = SP("b280bbf8-ae2a-4534-88f7-4888d2d0d79a",
        "udi7Q~A0P6MhEoZpStveaF65eNtMzxLR5ym_E",
        "e95d5bfb-3ffc-4c3e-a70d-ddffb338062b",
        "Work or school SP1"
)

WS_SP2 = SP("23c02100-1f77-4606-8413-5f2619c76889",
        "raZ7Q~EoLcPqs7nlTzzC2dL~z.MmMXHTeIMFQ",
        "e95d5bfb-3ffc-4c3e-a70d-ddffb338062b",
        "Work or school SP2"
)

MSA_SP1 = SP("a226de0a-ec30-48f9-8a06-e79378b92236",
        "yid7Q~Fa1ZXvgWrt0t8WprUjFLU2bt73XfkPe",
        "5b9f306e-06f7-4af8-9065-37144fe2a536",
        "MSA account SP1"
)

MSA_SP2 = SP("0f1f066b-52d2-4431-9266-f271f3d31e1b",
        "gXC7Q~jVRHMTigtIO5fzYegTWo8jsRN~1wLq9",
        "5b9f306e-06f7-4af8-9065-37144fe2a536",
        "MSA account SP2"
)



MS_Paid_cluster.test_connection(MS_SP1)

MS_FREE_cluster.test_connection(MS_SP1)

WS_FREE_cluster.test_connection(MS_SP1)
WS_FREE_cluster.test_connection(WS_SP1)
#WS_FREE_cluster.test_connection(WS_SP2)
WS_FREE_cluster.test_connection(MSA_SP1)
#WS_FREE_cluster.test_connection(MSA_SP2)



MSA_FREE_cluster.test_connection(MS_SP1)
MSA_FREE_cluster.test_connection(WS_SP1)
#MSA_FREE_cluster.test_connection(WS_SP2)
MSA_FREE_cluster.test_connection(MSA_SP1)
#MSA_FREE_cluster.test_connection(MSA_SP2)




