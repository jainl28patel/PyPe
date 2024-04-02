import asyncio
import aiohttp
import json

# Dictionary to store the health of each node
nodes_health = {}


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


# Function to send the health data to the responsible node
async def send_health_data(responsible_node_url, health_data, session):
    try:
        # Convert the health data to JSON
        health_data_json = json.dumps(health_data)
        # Send the health data via POST request
        async with session.post(
            responsible_node_url, json=health_data_json
        ) as response:
            if response.status == 200:
                print("Health data sent successfully.")
            else:
                print("Failed to send health data.")
    except aiohttp.ClientError as e:
        print(f"An error occurred: {e}")


# Main function to orchestrate the heartbeat checks
async def main(node_urls, responsible_node_url):
    global nodes_health
    async with aiohttp.ClientSession() as session:
        while True:
            # Store the previous health data to detect changes
            previous_health = nodes_health.copy()

            # Asynchronously check the health of each node
            tasks = [check_node_health(url, session) for url in node_urls]
            health_results = await asyncio.gather(*tasks)

            # Update the nodes_health dictionary with the new health results
            nodes_health = dict(zip(node_urls, health_results))

            # Check if there's a change in the health status
            if nodes_health != previous_health:
                # Send the updated health data to the responsible node
                await send_health_data(responsible_node_url, nodes_health,session)

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