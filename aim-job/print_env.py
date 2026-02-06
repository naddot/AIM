import json
with open('job_desc.json', 'rb') as f:
    data = json.loads(f.read().decode('utf-16le'))
env = data['spec']['template']['spec']['template']['spec']['containers'][0]['env']
for e in env:
    print(f"{e['name']} = {e.get('value', 'N/A')}")
