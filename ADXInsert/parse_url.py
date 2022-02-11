from posixpath import split
from typing import Literal


url = "https://homeasistantcluster.westeurope.kusto.windows.net"

title =  url.split("//")[1]
title2 = title.split(".")[0]

print(title2)