def generate_ips():
    lines = []
    for i in range(0, 32):
        for j in range(1, 33):
            lines.append("10.0.{}.{}".format(i, j))
    return lines

def main():
    ips = generate_ips()
    with open("server_ips.conf", "w") as f:
        for ip in ips:
            f.write(ip + "\n")
    print("Generated server_ips.conf with {} addresses.".format(len(ips)))


if __name__ == "__main__":
    main()
