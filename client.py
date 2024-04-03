import aiohttp
import json


def request_parser(request):
    
    task = (json.loads(request))["task"]
    task_id = (json.loads(request))["task_id"]

    return (task, task_id)


async def node_post(node_url, request):
    
    async with aiohttp.ClientSession() as session:
        response = await session.post(url = node_url, json=request)
        print(response.status)

        return response


async def node_get(node_url):

    async with aiohttp.ClientSession() as session:
        async with session.get(node_url) as response:
            status = response.status
            data = await response.json()
            return (status, data)


async def master(master_url, task, task_id):

    data = { 
                "task" : task, 
                "task_id" : task_id, 
            }

    async with aiohttp.ClientSession() as session:
        response = await session.post(url = master_url, json=data)
        return response.status


async def post_to_master(request, master_url):

    task, task_id = request_parser(request)
    status = await master(master_url, task, task_id)
    print(status)


async def post_to_nodes(request, node_urls):
    
    for node_url in node_urls:
        status  = await node_post(node_url, request)
        print(node_url, status)


async def get_from_nodes(node_urls):

    for node_url in node_urls:
        status, response = await node_get(node_url)
        print(status)
        if(status == 200):
            return response

         
