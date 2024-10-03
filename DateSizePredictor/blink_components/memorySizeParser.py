import requests
from bs4 import BeautifulSoup

def sum_memory_sizes(sizes):
    total_memory = sum(convert_to_num(size) for size in sizes)
    return total_memory

def convert_to_num(size):
    value, unit = size.split()
    print( value + " " + unit)
    value = float(value)
    if unit.lower() == 'mib':
        return value * 1024**2
    if unit.lower() == 'kib':
        return value * 1024
    
    return value

def getSizesFromHTML():
    
    #Map of application ID's
    # Pagerank
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

    
    # Base URL for the storage page
    base_url = "http://localhost:18080/history/{}/storage/"

    # Dictionary to hold the memory sizes for each application
    memory_sizes = {}

    # Iterate through the app_map
    for key, app_id in app_map.items():
        url = base_url.format(app_id)
        
        try:
            response = requests.get(url)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                table = soup.find(id='storage-by-rdd-table')

                if table:
                    rows = table.find_all('tr')[0:]
                    memory_sizes[key] = []

                    for row in rows:
                        size_in_memory = row.find_all('td')[5].text.strip()  # Index of the 5th column for "Size in Memory"
                        memory_sizes[key].append(size_in_memory)

                else:
                    print(f"Table not found for {key}!")
            else:
                print(f"Failed to retrieve data for {key}. Status code: {response.status_code}")

        except Exception as e:
            print(f"An error occurred while processing {key}: {e}")
        
    return memory_sizes

def getSizes():
    retrieved_sizes = {}
    for key, sizes in getSizesFromHTML().items():
        size_sum = sum_memory_sizes(sizes)
        retrieved_sizes[key] = size_sum
        print(f"Application Scale: {key}, Sizes in Memory: {size_sum}")
    print(retrieved_sizes)
    return retrieved_sizes

