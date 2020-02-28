# import requests
import pdb
import csv
import tarfile
import urllib.request, urllib.parse, urllib.error
import xml.etree.ElementTree as ET
import tempfile
import psycopg2
import os
import shutil
import time
# from sqlalchemy import create_engine
# db = create_engine("postgres://geraldding@localhost:5432/capstone")

connection = psycopg2.connect(user = "geraldding", host = "localhost", port = "5432", database = "capstone")
cur = connection.cursor()

with open('oa_file_list.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    for i, row in enumerate(csv_reader):
        print("processing file " + str(i))
        start = time.time()
        url = "ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/" + row[0]
        response = urllib.request.urlopen(url)
        print("obtained response at " + str(time.time() - start))
        tar = tarfile.open(fileobj=response, mode="r:gz")
        print("opened tar at " + str(time.time() - start))
        tar.extractall("tmp")
        print("extracted tar at " + str(time.time() - start))
        name = [n for n in tar.getnames() if n[-3:] == "xml"][0]
        file = open("tmp/" + name, "r")
        content = file.read()
        print("read file at " + str(time.time() - start))
        tree = ET.fromstring(str(content))
        abstract = tree[0][1].find("abstract")
        if abstract:
            abstract = str(ET.tostring(abstract))
            print("found abstract at " + str(time.time() - start))
            file.close()
            shutil.rmtree("tmp/" + tar.getmembers()[0].name)
            tar.close()
            sql = """INSERT INTO articles(abstract) VALUES(%s);"""
            cur.execute(sql, (abstract,))
            print("processed sql at " + str(time.time() - start))
            if i % 20 == 0:
                connection.commit()

        # for abstract in tree.iter("abstract"):
        # # lst = tree.findall('.//count')
        # # xml = tar.extractfile(tar[0])
        #     # pdb.set_trace()
        #     if abstract.text:
        #         pdb.set_trace()
        #         print(abstract.text)
        # content = xml.read()

        #
        # except urllib.error.URLError as err:
        #     print(err)

    #     pdb.set_trace()
    #
    #     pdb.set_trace()
    #     #     thetarfile = tarfile.open(fileobj=ftpstream, mode="r|gz")
    #     #     thetarfile.extractall()
    #
    #     pdb.set_trace()
    # # print(f'Processed {line_count} lines.')

# for i in range(0, 1000000):
#     xml = urllib.request.urlopen("https://www.ncbi.nlm.nih.gov/pmc/utils/oa/oa.fcgi?id=PMC" + str(i)).read()
#     print(i)
#     if xml.find(b"\x7265636f7264730d0a") >= 0:
#         pdb.set_trace()
#         tree = ET.fromstring(xml)
#         print(i)
#         for record in tree.iter("record"):
#
#             pdb.set_trace()
#             for child in tree:
#                 pdb.set_trace()
#             pdb.set_trace()
#             url = f'Hello {name}! This is {program}'
#             file = url + i
#             pdb.set_trace()
#             ftpstream = urllib.request.urlopen(thetarfile)
#             thetarfile = tarfile.open(fileobj=ftpstream, mode="r|gz")
#             thetarfile.extractall()

# xml = 'http://py4e-data.dr-chuck.net/comments_42.xml'
# url = urllib.request.urlopen(xml)
# data = url.read()
# #print(data.decode())
# tree = ET.fromstring(data)
# lst = tree.findall('.//count')
# for item in lst:
#     print(item.text)
# import urllib.request
# url = "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC"
#
# for i in range(15000, 1000000):
#     # user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'
#     # request = urllib.request.Request(url + str(i),headers={'User-Agent': user_agent})
#     # response = urllib.request.urlopen(request)
#     # html = response.read()
#     URL = ' https://www.ncbi.nlm.nih.gov/pmc/utils/oa/oa.fcgi'
#     page = requests.get(URL)
#     pdb.set_trace()

# import urllib.request
# import tarfile
#
# def url(i):
#     return "ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_package/8e/71/PMC5334499.tar.gz"
#
# for i in range(15000, 1000000):
#     urllib.requesthttps://www.ncbi.nlm.nih.gov/pmc/tools/oa-service/
#     url = f'Hello {name}! This is {program}'
#     file = url + i
#     pdb.set_trace()
#     ftpstream = urllib.request.urlopen(thetarfile)
#     thetarfile = tarfile.open(fileobj=ftpstream, mode="r|gz")
#     thetarfile.extractall()
