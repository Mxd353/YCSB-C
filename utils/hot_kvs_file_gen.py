def get_dev_id(ip):
    if ip.startswith('192.168.7.'):
        return 0
    elif ip.startswith('192.168.8.'):
        return 1
    elif ip.startswith('192.168.9.'):
        return 2
    else:
        return -1

with open("hot_statistics.output", "r") as infile, open("hot_keys.output", "w") as outfile:
    for line in infile:
        parts = line.strip().split()
        if len(parts) < 7:
            continue
        ip = parts[0]
        key = parts[1]
        dev_id = get_dev_id(ip)
        outfile.write(f"{dev_id} {key}\n")
