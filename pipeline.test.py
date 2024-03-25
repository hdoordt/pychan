import asyncio
import aiofiles
import pychan


async def pipe_bytes(writer):
    async with aiofiles.open('Cargo.lock', mode='rb', buffering=1000) as file:
        while True:
            bytes = await file.read(1000)
            if len(bytes) == 0:
                break
            await pychan.chan_send(writer, bytes)
        await pychan.sender_close(writer)


async def poll_reader(reader):
    while True:
        res = await pychan.chan_read(reader, 120)
        print('Read result (' + str(len(res))  + ') :\t' + str(res))
        if len(res) == 0:
            break
    print('Done reading!')
    


async def main():
    writer, reader = pychan.bytes_chan(16)
    test1 = asyncio.create_task(pipe_bytes(writer))
    test2 = asyncio.create_task(poll_reader(reader))

    await asyncio.gather(test1, test2)
    await test1

asyncio.run(main())

