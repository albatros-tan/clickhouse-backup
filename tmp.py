#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import asyncio
from settings.clickhouse import ClHouseConfig


async def additional_coro(num_process):
    print(f"addition coro {num_process} is run")
    await asyncio.sleep(0.09)
    print(f"addition coro {num_process} is stop")
    

async def simple_request(num_process, sleep_time):
    print(f"{num_process} is run")
    await asyncio.sleep(sleep_time)
    print(f"{num_process} is stop")
    

async def run_big_task(num_process, sleep_time, sem: asyncio.Semaphore):
    await sem.acquire()
    for item in range(5):
        await simple_request(num_process, sleep_time)
    sem.release()        


async def main_():
    loop = asyncio.get_event_loop()
    sem = asyncio.Semaphore(value=2)
    await asyncio.wait(
        [
            asyncio.create_task(run_big_task(1, 0.2, sem)),
            asyncio.create_task(run_big_task(2, 0.4, sem)),
            asyncio.create_task(run_big_task(3, 0.1, sem)),
        ]
    )
    
    
async def _main_():
    coro = simple_request
    print("wait")
    await coro(15, 0.2)
    

asyncio.run(_main_())

from settings.clickhouse import ClHouseConfig
from asynch import connect
from asynch.proto.connection import Connection
from asynch.cursors import DictCursor


async def connect_database():
    return await connect(**ClHouseConfig.get_connection_data())

async def test_func(conn: Connection):
    async with conn.cursor(cursor=DictCursor) as cursor:
        #await cursor.execute("describe table fake_income_tax")
        await cursor.execute("select load_guid, count() as count from fake_income_tax group by load_guid")
        res = await cursor.fetchall()
        #await cursor.execute("select count() as cnt from fake_income_tax")
        #res = await cursor.fetchone()
        print(res)
        
async def main():
    conn = await connect_database()
    await test_func(conn)
    await conn.close()
    

    
#asyncio.run(main())

    