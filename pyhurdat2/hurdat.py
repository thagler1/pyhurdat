# base class for reading pyhurdat data
import csv
import pprint
import requests

hurdat2_data_structure = [
    "date",
    "hours",
    "record_identifier",
    "status_of_system",
    "latitude",
    "longitude",
    "maximum_sustained_wind",
    "maximum_pressure",
    "34_mw_ne",
    "34_mW_se",
    "34_mW_nw",
    "34_mW_sw",
    "50_mw_ne",
    "50_mW_se",
    "50_mW_nw",
    "50_mW_sw",
    "50_mW_sw",
    "64_mw_ne",
    "64_mW_se",
    "64_mW_nw",
    "64_mW_sw",
]

storm_code= [
    "uid",
    'name',
    "number_rows"
]


def create_hurdat_dict(hurdat_list):
    # this allows key access to lookup withour reiterating the list

    d = {}
    for i , item in enumerate(hurdat_list):
        d[hurdat_list[i]] = i
    return d


def pluck(col, line, header):

    # Return value from column. This uses the index of the column in the header to get the value for the line
    try:
        d = line[header.index(col)]
    except:
        d = ""
    return d

def get_description(desc):
    if desc == "null" or desc == "":
        return "No description provided"
    else:
        return desc

def isHeader(line):
    if len(line) < 18:
        return True
    else:
        return False



def process_uid(uid):
    item={
    "basin": uid[:2],
    "atcf": uid[2:4],
    "year": uid[4:9]
    }
    return item

def strip_white_space(string):
    return string.lstrip()

def process_name(name):
    whitespace = len(name.split()[0])
    totat_chars = 0
    for part in name.split():
        totat_chars += len(part)
        totat_chars +=1
    r = name[len(name)- totat_chars:]
    return r

def add_storm(line, header, data):
    header_data = process_uid(line[0])

    item = {
        'name':  strip_white_space(process_name(line[1])),
        'atcs_name': line[0],
        "basin": header_data["basin"],
        "year": header_data['year'],
        "atcf_num": header_data['atcf'],
        "data": []
    }
    #print(line[0])
    return item

def add_line(line, header, data, current_storm):
    uid = current_storm['atcs_name']

    if uid == "":
        # if no name is provided we shouldnt push anything.
        return
    if uid not in data:
        # these elements are repeated for each column, if it has already been added to data we dont need to add it aga

        data.setdefault(uid, current_storm)

    # Get column info
    columnInfo = {item: strip_white_space(line[i]) for i, item in enumerate(hurdat2_data_structure)}

    data[uid]['data'].append(columnInfo)

def get_longest_storm(data):
    short = float("inf")
    max = 0
    biggest_storm = {}
    for storm in data:
        if len(data[storm]["data"]) > max:

            max = len(data[storm]['data'])
            biggest_storm = data[storm]["atcs_name"]
    return biggest_storm

def get_longest_name(data):
    max = 0
    biggest_storm = {}
    for storm in data:
        if len(data[storm]['name']) > max:

            max = len(data[storm]['name'])
            biggest_storm = data[storm]["atcs_name"]
    return biggest_storm


def parse_csv(filepath):
    f = open(filepath, 'r', newline='',
             encoding='utf-8-sig')
    reader = csv.reader(f)
    data = {}
    header = create_hurdat_dict(hurdat2_data_structure)
    storm_header = create_hurdat_dict(storm_code)
    current_storm = {}
    print("starting")
    count = 0
    for i, line in enumerate(reader):
        if isHeader(line):
            current_storm = add_storm(line, storm_header, data)
        else:
            if current_storm:
                add_line(line, header, data, current_storm)
        count = i
    return data, count

def post_item(item):
    requests.post("http://localhost:8000/hurdat", json=item)
    print("posted")

file = r"C:\Users\tdhag\Documents\github\pyhurdat\sampleData\hurdat2.csv"

data,count = parse_csv(file)

for item in data:
    post_item(data[item])

print("Scanned %s rows"% str(count))
print("Processed %s Records"%str(len(data)))

pprint.pprint(data[get_longest_name(data)])

