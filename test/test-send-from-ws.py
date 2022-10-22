import asyncio
import json
import time

import websockets


async def main():
    async with websockets.connect('ws://127.0.0.1:8765') as websocket:
        while 1:
            try:
                data = {
                    'headers': "[('1', b'test1'), ('2', b'test2')]",
                    'body': 'this is a test',
                    'id': 'nttai'
                }
                await websocket.send(json.dumps(data))
                print('Sent')
                time.sleep(1)
            except Exception as e:
                print(e)
                exit()
            # print('Waiting for response')
            # result = await websocket.recv()
            # print(result)


asyncio.get_event_loop().run_until_complete(main())
