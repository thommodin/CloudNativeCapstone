import pystac_client

client = pystac_client.Client.open("catalog/catalog.json")
print(client)

search = client.search(
    bbox=[94.921875,-31.728167,111.005859,-18.646245],
)