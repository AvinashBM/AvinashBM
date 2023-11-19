import asyncio
import pandas as pd
import time

from azure.eventhub.aio import EventHubProducerClient  # The package name suffixed with ".aio" for async
from azure.eventhub import EventData

connection_str = 'Endpoint=sb://evh-sm-wheeler-battery-dev.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=K22TvOc5hfva/zwNcROhyalgJeZ61wxf9gmsnXaqeFw='
consumer_group = 'blobstorage_capture-cg'
eventhub_name = 'blivestream'

df = pd.read_parquet(
    'D:\Avinash.BM\OneDrive - SUN Mobility\Desktop\Main\PycharmProjects\Output\INSMOMAH0106G230A3122023-05-01.parquet')
first = df.head(1)
json_first = str(first.to_json())


async def create_batch(client):
    event_data_batch = await client.create_batch()

    can_add = True

    while can_add:

        try:

            # event_data_batch.add(EventData(json_first))
            event_data_batch.add('/dev/2023/08/INSCD/INSCD2023-08-07.parquet')
            # limitation

        except ValueError:

            can_add = False  # EventDataBatch object reaches max_size.

            print("here")

    return event_data_batch


async def send():
    client = EventHubProducerClient.from_connection_string(connection_str, eventhub_name=eventhub_name)

    batch_data = await create_batch(client)

    while True:
        time.sleep(60)
        async with client:
            await client.send_batch(batch_data)

            print("data send")


if __name__ == '__main__':
    loop = asyncio.get_event_loop()

    loop.run_until_complete(send())