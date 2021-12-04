import re

if __name__ == '__main__':
    symbol = "RKT"
    escape_string = "He is RKT RKT"
    pattern = '\\b'+symbol+'\\b'
    print(pattern)
    if re.search(pattern, escape_string):
        print("Found")
    else:
        print("NOT FOUND")
