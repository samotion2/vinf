import csv
import re
import os
import multiprocessing
import time

FILE = 'file'
INDEXES = 'indexes'
INDEX_F_NAME = 'index.csv'

class Phone:
    def __init__(self, name='?', soc='?', released='?'):
        self.name = self.clean(name)
        self.soc = self.clean(soc)
        self.released = self.year_clean(released)

    def clean(self, word):
        cln = ['name', 'soc', 'released']
        spec = ['Start date and age', 'start date and age', 'df=y']
        reg = r'[\|\[\]\;\&\(\)\{\}\n\#]'
        w = re.sub(reg, '', word)
        
        # remove starting tag
        for i in cln:
            w = re.sub(fr'{i} += ', '', w)
        
        for i in spec:
            # print(i)
            w = re.sub(i, '', w)
        
        return w
    
    def year_clean(self, year):
        year = re.findall(r'\d+', year)
        return f'{year[0]}.{year[1]}.{year[2]}'

    def __str__(self) -> str:
        return f'name: {self.name}, soc: {self.soc}, released: {self.released}'
    
    def __eq__(self, other):
        if (isinstance(other, Phone)):
            return self.name == other.name and self.soc == other.soc and self.released == other.released
        return False

def chunky_index(file_name):
    print(f'processing: {file_name}')
    index = {FILE: file_name, INDEXES: []}
    counter = 0
    cc = 0
    cc1 = 0
    with open(file_name, 'r', encoding="utf8") as f:
            chunk = ''
            appender = 0
            for line in f:
                if re.search(r'<page>', line):
                    appender = 1
                    cc1 = cc
                if (appender == 1):
                    chunk += line
                if re.search(r'<\/page>', line):
                    appender = 0
                    ph = process_index(chunk)
                    if (ph):
                        index[INDEXES].append(f'{cc1}-{cc}')
                        # print(cc1)
                    counter += 1
                    chunk = ''
                cc +=1 
    print(f'finished: {file_name}')
    if index[INDEXES]:
        write_index_pages(index)

def process_index(page):
    if re.search(r"\[\[Category:.{0,50}?smart.{0,50}?\]\]", page):
        return True
    return False

def write_index_header():
    index = {FILE: 'tmp', INDEXES: []}
    with open(INDEX_F_NAME, 'w', newline='') as f:
        w = csv.DictWriter(f, index.keys())
        w.writeheader()

def write_index_pages(index_dict):
    with open(INDEX_F_NAME, 'a', newline='') as f:
        w = csv.DictWriter(f, index_dict.keys())
        w.writerow(index_dict)

def create_index():
    write_index_header()
    folder_dir = 'C:\\Users\\PC\\Desktop\\vinf\\vinfdata'
    files_names = []
    for file in os.listdir(folder_dir):
        if file.endswith('.xml'):
            files_names.append(os.path.join(folder_dir, file))
    
    with multiprocessing.Pool() as pool:
        pool.map(chunky_index, files_names)

##############################################################################

def chunky_get(record):
    file_name = record[FILE]
    indexes = record[INDEXES].strip('][').split(', ')
    # print(file_name)
    # print(indexes)
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
                # print(chunk)
                # print(index[0], index[1])
                ph = process_get(chunk)
                if (ph):
                    phones.append(ph)
                chunk = ''
                if counter < len(indexes)-1:
                    counter += 1
                    index = [int(x) for x in indexes[counter].replace('\'', '').split('-')]
    return phones

def process_get(page):
    # print(page)
    if re.search(r"\[\[Category:.{0,50}?smart.{0,50}?\]\]", page):
        name, soc, released = None, None, None
        reg_name = r' name += .*?[\|\&\n]'
        reg_soc = r' soc += .*?[\(\&\]\|\#]'
        reg_released = r' released += .*?[\}\&]'

        if re.search(reg_name, page):
            name = re.search(reg_name, page, re.DOTALL).group(0)
        if re.search(reg_soc, page):
            soc = re.search(reg_soc, page, re.DOTALL).group(0)
        if re.search(reg_released, page):
            released = re.search(reg_released, page, re.DOTALL).group(0)
        # print(name, soc, released)
        if (name and soc and released):
            return Phone(name=name,soc=soc, released=released)
        return False

def read_index():
    d = {'records': []}
    with open(INDEX_F_NAME, 'r') as f:
        reader = csv.reader(f)
        next(reader) # to get rid of header
        for l in reader:
            d['records'].append({FILE: l[0], INDEXES: l[1]})
    return d

def get_phones():
    dictionary = read_index()
    # deletes non existing files from index
    dictionary['records'] = [x for x in dictionary['records'] if os.path.exists(x[FILE])]

    with multiprocessing.Pool() as pool:
        ph = pool.map(chunky_get, [rec for rec in dictionary['records']])
        ph = [ent for sublist in ph for ent in sublist]

    printed = []
    for p in ph:
        if p not in printed:
            print(p)
            printed.append(p)

if __name__ == '__main__':
    start = time.time()
    create_index()
    end = time.time()
    print('indexing: ', end - start)
    
    start = time.time()
    get_phones()
    end = time.time()
    print('parsing:', end - start)

