
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
        
        self.query_client.execute(self.database, query)

        return None

class SP:

    def __init__(self, client_id, client_secret, authority_id, name):
        self.client_id = client_id
        self.client_secret = client_secret
        self.authority_id = authority_id
        self.name = name

adx_clusters = []
SPs = []

adx_clusters.append(adx_cluster("https://kvc5520da122edb407099a.northeurope.kusto.windows.net","Microsoft.com account FREE cluster"))
adx_clusters.append(adx_cluster("https://kvc87e708ecd6604e8c8df.northeurope.kusto.windows.net","Work or school acount FREE cluster"))
adx_clusters.append(adx_cluster("https://kvc790617bafc0a4f23b2c.northeurope.kusto.windows.net","MSA_FREE_cluster"))

SPs.append(SP("b280bbf8-ae2a-4534-88f7-4888d2d0d79a",
        "udi7Q~A0P6MhEoZpStveaF65eNtMzxLR5ym_E",
        "e95d5bfb-3ffc-4c3e-a70d-ddffb338062b",
        "Work or school SP1"
))

SPs.append(SP("a226de0a-ec30-48f9-8a06-e79378b92236",
        "yid7Q~Fa1ZXvgWrt0t8WprUjFLU2bt73XfkPe",
        "5b9f306e-06f7-4af8-9065-37144fe2a536",
        "MSA account SP1"
))

hasError = False

for adx_cluster in adx_clusters:
    for SP in SPs:
        try:
            adx_cluster.test_connection(SP)
        except Exception as exp:
            print("Failed with eception:")
            #print(exp)
            hasError = True
        else:
            print("Connection OK")









