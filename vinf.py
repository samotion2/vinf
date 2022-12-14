import csv
import re
import os
import multiprocessing
import time
from difflib import get_close_matches
from pyspark.sql import SparkSession
import sys

FILE = 'file'
INDEXES = 'indexes'
INDEX_F_NAME = 'index.csv'
# SEARCHED_PHONE = 's5'
# DATA_DIR = '/home/vinfdata'
PARTITIONS = 10
data_dir = None

class Phone:
    def __init__(self, name='?', soc='?', released='?'):
        self.name = self.clean(name)
        self.soc = self.clean(soc)
        self.released = self.year_clean(released)

    def clean(self, word):
        """Removing unnecessary characters from word

        :param word: word itself
        :returns: cleaned word
        """
        cln = ['name', 'soc', 'released']
        spec = ['Start date and age', 'start date and age', 'df=y', 'ubl', ':']
        reg = r'[\|\[\]\;\&\(\)\{\}\n\#]'
        w = re.sub(reg, '', word)
        
        # REMOVE STARTING TAG
        for i in cln:
            w = re.sub(fr'{i} += ', '', w)
        
        # REMOVE STARTING SPECIAL CHARACTERS
        for i in spec:
            # print(i)
            w = re.sub(i, '', w)
        
        w = re.sub(r'\'\'\'.*?\'\'\'', '', w)
        w = w.strip()
        return w
    
    def year_clean(self, year):
        """Removing unnecessary characters from year

        :param year: year itself
        :returns: cleaned year
        """
        year = re.findall(r'\d+', year)
        if int(year[2]) < 10 and year[2][0] != '0':
            year[2] = '0' + year[2]
        if int(year[1]) < 10 and year[1][0] != '0':
            year[1] = '0' + year[1]
        return f'{year[2]}.{year[1]}.{year[0]}'

    def __str__(self) -> str:
        return f'name: {self.name}, soc: {self.soc}, released: {self.released}'
    
    def __eq__(self, other):
        if (isinstance(other, Phone)):
            return self.name == other.name and self.soc == other.soc and self.released == other.released
        return False

def chunky_index(file_name):
    """Processing file to create index

    :param file_name: name of the processing file
    """
    print(f'processing: {file_name}')
    index = {FILE: file_name, INDEXES: []}
    with open(file_name, 'r', encoding="utf8") as f:
        index_raw = spark.sparkContext.parallelize(enumerate(f), PARTITIONS).map(mapper_page).reduce(reduce_index)
        #SORTING
        # print(index_raw)
        index_raw.sort(key=lambda a: a[0])
        indexes = process_index(index_raw)
        for ind in indexes:
            index[INDEXES].append(f'{ind[0]}-{ind[1]}')
                    
    print(f'finished: {file_name}')
    # WRITING INDEXES TO FILE
    if index[INDEXES]:
        write_index_pages(index)
    return ''

def process_index(arr):
    """Filtering indeches into correct form

    :param arr: array of unprocessed indexes
    :returns: correct index array
    """
    indexes = []
    s = None
    t = None
    e = None

    for tup in arr:
        if s == None:
            if tup[1] == 's':
                s = tup[0]
        elif s != None:
            if tup[1] == 't':
                t = tup[0]
            if tup[1] == 'e':
                e = tup[0]
                if t == None:
                    s = None
                else:
                    indexes.append((s,e))
                    s = t = e = None
    return indexes


def reduce_file_names(a,b):
    """ File_name reducer

    :param a: first element
    :param b: second element
    :returns: list of filenames
    """
    if a is None:
        return b
    if b is None:
        return a

    if type(a) is not list:
        return [a,b]
    a.append(b)
    return a

def reduce_index(a,b):
    """ Index reducer

    :param a: first element
    :param b: second element
    :returns: list of tuples
    """
    if a is None:
        return b
    if b is None:
        return a

    if type(a) is not list and type(b) is not list:
        return [a,b]
    elif type(a) is not list:
        tmp = []
        tmp.append(a)
        tmp.extend(b)
        a = tmp
    elif type(b) is list:
        a.extend(b)
    else:
        a.append(b)
    return a

def write_index_header():
    """ Writes header into index file
    """
    index = {FILE: 'tmp', INDEXES: []}
    with open(INDEX_F_NAME, 'w', newline='') as f:
        w = csv.DictWriter(f, index.keys())
        w.writeheader()

def write_index_pages(index_dict):
    """Writes dictionary with indexes to file

    :param index_dict: dictionary with file name and its indexes
    """
    with open(INDEX_F_NAME, 'a', newline='') as f:
        w = csv.DictWriter(f, index_dict.keys())
        w.writerow(index_dict)

def custom_mapper(file_name):
    """Mapper for file_names

    :param file: file_name
    :returns: absolute filepath
    """
    if file_name.endswith('.xml'):
        return os.path.join(data_dir, file_name)

def mapper_page(tupple):
    """Mapper for lines
    's': start page tage
    'e': end page tag
    't': line containing smartphone category

    :param tupple: containing line number and its text
    :returns: line number with corresponding letter
    """
    line_num = tupple[0]
    text = tupple[1]
    if re.search(r'<page>',text):
        return (line_num, 's')
    if re.search(r'<\/page>', text):
        return (line_num, 'e')
    if re.search(r"\[\[Category:.{0,50}?smart.{0,50}?\]\]", text):
        return (line_num, 't')
    return None

##############################################################################

def chunky_get(record):
    """Extracting phones using index

    :param record: record(one line) in index file
    :returns: Phone object
    """
    file_name = record[FILE]
    indexes = record[INDEXES].strip('][').split(', ')
    print(f'processing: {file_name}')
    phones = []

    with open(file_name, 'r', encoding="utf8") as f:
        chunk = ''
        counter = 0
        index = [int(x) for x in indexes[counter].replace('\'', '').split('-')]
        for i, line in enumerate(f):
            if i > index[0] and i < index[1]:
                chunk += line.strip()+'\n'
            elif i == index[1]:
                ph = process_get(chunk)
                if (ph):
                    phones.append(ph)
                chunk = ''
                if counter < len(indexes)-1:
                    counter += 1
                    index = [int(x) for x in indexes[counter].replace('\'', '').split('-')]
    return phones

def process_get(page):
    """Check if page is correct and containing all necessary data

    :param page: text itself
    :returns: Phone object if can be built
    """
    # print(page)
    if re.search(r"\[\[Category:.{0,50}?smart.{0,50}?\]\]", page):
        name, soc, released = None, None, None
        reg_title = r'<title>(.*?)<\/title>'
        reg_soc = r' soc += .*?\[\[(.*?)\]\]'
        reg_released = r' released += .*?[\}\&]'
        if re.search(reg_title, page):
            name = re.search(reg_title, page, re.DOTALL).group(1)
        if re.search(reg_soc, page):
            soc = re.search(reg_soc, page, re.DOTALL).group(0)
        if re.search(reg_released, page):
            released = re.search(reg_released, page, re.DOTALL).group(0)
        # print(name, soc, released)
        if (name and soc and released):
            return Phone(name=name,soc=soc, released=released)
        return False

def read_index():
    """Reading index file
    """
    d = {'records': []}
    with open(INDEX_F_NAME, 'r') as f:
        reader = csv.reader(f)
        # TO GET RID OF HEADER
        next(reader)
        for l in reader:
            d['records'].append({FILE: l[0], INDEXES: l[1]})
    return d

def get_phones(searched_phone):
    """Getting all available phones

    :param searched_phone: Name of the phone we are loooking for
    """
    dictionary = read_index()
    # DELETES NON EXISTING FILES FROM INDEX
    dictionary['records'] = [x for x in dictionary['records'] if os.path.exists(x[FILE])]

    with multiprocessing.Pool() as pool:
        ph = pool.map(chunky_get, [rec for rec in dictionary['records']])
        ph = [ent for sublist in ph for ent in sublist]

    printed = []
    print('========List of all phones=========')
    for p in ph:
        if p not in printed:
            print(p)
            printed.append(p)
    print('===================================')

    if searched_phone in [x.name for x in printed]:
        closest_match = searched_phone
    else:
        closest_match = get_close_matches(searched_phone, [x.name for x in printed], n=1, cutoff=0.5)
        if closest_match:
            closest_match = closest_match[0]
    
    if closest_match:
        for p in printed:
            # print(p.name, closest_match)
            if p.name == closest_match:
                if closest_match == searched_phone:
                    print(f'Searched phone: {searched_phone}')
                else:
                    print(f'Closes match to searched phone: {searched_phone}')
                print(p)
    else:
        print(f'The phone named {searched_phone} not found!')

if __name__ == '__main__':
    # CHECK IF INPUT VARIABLES ARE CORRECT
    if len(sys.argv) > 2:
        data_dir = str(sys.argv[1])
        if not os.path.exists(data_dir):
            sys.exit(f'Entered dir path {data_dir} doesn\'t exist!')
        searched_phone = str(sys.argv[2])
    else:
        sys.exit('Missing path or phone argument')
    
    start = time.time()
    # SPARK INITIALIZATION
    spark = SparkSession\
        .builder\
        .appName("PythonPhones")\
        .getOrCreate()
    # changing logger level
    spark.sparkContext.setLogLevel("ERROR")    
    write_index_header()
    # GETTING RELEVANT FILES 
    files_names = spark.sparkContext.parallelize(os.listdir(data_dir), PARTITIONS).map(custom_mapper).reduce(reduce_file_names)
    # INDEXING
    for file_name in files_names:
        chunky_index(file_name)
    # SEARCH IN INDEXES
    get_phones(searched_phone)
    end = time.time()
    print('parsing duration:', end - start)
