import json
with open('latest_job_logs.json', 'rb') as f:
    data = json.loads(f.read().decode('utf-16le'))
for entry in data:
    payload = entry.get('textPayload')
    if payload:
        print(payload)
