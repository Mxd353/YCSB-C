LEAF_SIZE = 32
LEAVES = [i for i in range(LEAF_SIZE)]

def get_rack(ip_str):
    parts = ip_str.strip().split(".")
    if len(parts) != 4:
        return -1
    try:
        third_octet = int(parts[2])
    except ValueError:
        return -1

    rack_index = third_octet - 7
    if 0 <= rack_index < LEAF_SIZE:
        return LEAVES[rack_index]
    return -1

with open("hot_statistics.output", "r") as infile, open("hot_keys.output", "w") as outfile:
    for line in infile:
        parts = line.strip().split()
        if len(parts) < 7:
            continue
        ip = parts[0]
        key = parts[1]
        dev_id = get_rack(ip)
        outfile.write(f"{dev_id} {key}\n")
