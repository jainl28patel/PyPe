import asyncio
import aiohttp
import json
import threading

class Node_Health:
    def __init__ (self, node_urls):
        self.node_urls = {}
        for url in node_urls:
            self.node_urls[url] = "down"
        self.lock = threading.Lock()

    def __str__(self):
        with self.lock:
            return json.dumps(self.node_urls, indent=4)
        
    def update_health(self, url, status):
        with self.lock:
            self.node_urls[url] = status

    def get_data(self):
        with self.lock:
            return self.node_urls.copy()

# Dictionary to store the health of each node
nodes_health = None

# Function to check the health of a node
async def check_node_health(node_url, session):
    try:
        async with session.get(node_url) as response:
            if response.status == 200:
                return "up"
            else:
                return "down"
    except aiohttp.ClientError:
        return "down"

# Main function to orchestrate the heartbeat checks
async def main(node_urls):
    global nodes_health
    nodes_health = Node_Health(node_urls)
    async with aiohttp.ClientSession() as session:
        while True:
            # Asynchronously check the health of each node
            tasks = [check_node_health(url, session) for url in node_urls]
            health_results = await asyncio.gather(*tasks)

            # Update the nodes_health dictionary with the new health results
            new_nodes_health = dict(zip(node_urls, health_results))

            # Check if there's a change in the health status
            previous_nodes_health = nodes_health.get_data()
            if new_nodes_health != previous_nodes_health:
                # update the nodes_health dictionary
                for url, status in new_nodes_health.items():
                    if status != previous_nodes_health[url]:
                        nodes_health.update_health(url, status)

            # Wait for some time before the next check
            await asyncio.sleep(10)  # Check every 10 seconds

'''
# List of node URLs to check
node_urls = [
    "https://github.com/iiteen",
    "https://github.com/wadetb/heartbeat",
    "http://127.0.0.1:80/",
    # Add more node URLs as needed
]

# URL of the responsible node
responsible_node_url = "http://127.0.0.1:5000/report"

# Run the main function
asyncio.run(main(node_urls, responsible_node_url))
'''