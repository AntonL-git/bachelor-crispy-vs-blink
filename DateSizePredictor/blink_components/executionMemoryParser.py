import requests

base_api_url = "http://localhost:18080/api/v1/applications/{}/stages?details=false"
    
#Pagerank
# app_map={
#     1: "app-20240905172330-0002",
#     2: "app-20240905172437-0003",
#     3: "app-20240905172910-0000",
#     4: "app-20240905173036-0001",
#     5: "app-20240905172145-0001"
# }

#KMeans
app_map={
    1: "app-20240911113543-0000",
    2: "app-20240911113727-0001",
    3: "app-20240911113838-0002",
    4: "app-20240911114012-0003",
    5: "app-20240911114319-0004"
}

def fetch_data(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from {url}: {e}")
        return None

def get_max_peak_execution_memory(data):
    if data:
        peak_memory_values = [item['peakExecutionMemory'] for item in data]
        return max(peak_memory_values)
    else:
        print("No data available.")
        return None

def getExecutionMemories():
    max_execution_memories = {}
    for app_id, app_name in app_map.items():
        url = base_api_url.format(app_name)
        print(f"Fetching data for {app_name}...")        
        data = fetch_data(url)
        
        max_memory = get_max_peak_execution_memory(data)
        
        # Print the results
        if max_memory is not None:
            print(f"Maximum peakExecutionMemory for {app_name}: {max_memory}\n")
            max_execution_memories[app_id] = max_memory
        else:
            print(f"No data retrieved for {app_name}\n")
        
    print(max_execution_memories)
    return max_execution_memories


