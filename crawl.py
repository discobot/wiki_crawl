# -*- coding: utf-8 -*-

import sys, re
import requests
import queue
import pickle
import string
import os

import requests
import networkx as nx
import multiprocessing as mp

from bs4 import BeautifulSoup
from threading import Thread
from urllib.parse import urljoin

import config


valid_chars = "-_.() %s%s" % (string.ascii_letters, string.digits)
visited = set()
link_queue    = mp.Queue()
article_queue = mp.Queue()
utility_queue = mp.Queue()
edge_queue    = mp.Queue()
data_queue    = mp.Queue()


def get_article_name(url):
    if url == config.domain or url == config.domain + "/":
        return "Main_Page"
    m = re.search(config.article_pattern, url)
    if m:
        return m.group(1)

def get_request(url):
    try:
        req = requests.get(url)
        return req
    except KeyboardInterrupt :
            raise
    except:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print("url open problem " + str(exc_type) + str(exc_obj) + str(exc_tb))
        print("in URL", url)
        pass

def get_true_url(url):
    try:
        req = requests.get(url, stream = True)
        return req.url
    except KeyboardInterrupt :
            raise
    except:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print("url open problem " + str(exc_type) + str(exc_obj) + str(exc_tb))
        print("in URL", url)
        pass


def extract_links(soup, url):
    links = []
    for link in soup.findAll('a'):
        try:
            href = link['href']
        except KeyError:
            continue
        if sum(href.startswith(prefix) for prefix in config.ban_prefix) or sum(href.endswith(sufix) for sufix in config.ban_sufix):
            continue

        if href.startswith("/wiki/"):
            href = config.domain + href
        if (not href.startswith('http://')) and (not href.startswith('https://')):
            href = urljoin(url, href)

        if href.startswith(config.domain):
            if href == config.domain or href == config.domain + "/":
                href = config.start_page
            links.append(href)
    return links    

def process_links(links, name):
    for link in links:
        link_queue.put((link, name))


def ArticleCrawler() :
    print("article crawler spawned, pid is ", os.getpid())
    try:
        while True:
            url, parent_name  = article_queue.get(timeout=150)
            req   = get_request(url)
            try:
                req_url = req.url
                req_text = req.text
            except:
                pass

            name  = get_article_name(req_url) 
            #print(parent_name, " => ", name)
            soup  = BeautifulSoup(req_text, "html.parser")
            links = extract_links(soup, url)
            data_queue.put((name, str(soup.find("div", {"id": "bodyContent"}))))
            process_links(links, name)
    except queue.Empty:
        pass
    print("article queue stoping")

def UtilityCrawler():
    print("utility crawler spawned, pid is ", os.getpid())
    try:
        while True:
            url   = utility_queue.get(timeout=150)
            req   = get_request(url)
            soup  = BeautifulSoup(req.text, "html.parser") 
            links = extract_links(soup, url)
            process_links(links, "")
    except queue.Empty:
        pass
    print("utility crawler stopping ");


def UrlMaster():
    print("url master spawned, pid is ", os.getpid())
    processed = set([config.start_page])
    processed_articles = set(["Main_Page"])
    redirect_table = {}
    try:
        while True:
            url, parent_name = link_queue.get(timeout=150)
            #print(url, parent_name)
            url = url.strip().split("#")[0]
            if not config.domain in url or sum(page in url for page in config.wiki_ignored_pages):
                continue
            elif sum(page in url for page in config.wiki_utility_pages):
                if url in processed:
                    continue
                processed.add(url)
                utility_queue.put(url)
            elif "/wiki/" in url:
                if url in redirect_table:
                    url = redirect_table[url]
                else:
                    new_url = get_true_url(url)
                    redirect_table[url] = new_url
                    url = new_url
                #print(url)
                name = get_article_name(url)
                if parent_name != "":
                    edge_queue.put((parent_name, name))
                if not name in processed_articles:
                    processed_articles.add(name)
                    #print(len(processed_articles))
                    article_queue.put((url, parent_name))

            else:
                print(url)  #debug
    except queue.Empty:
        pass
    print("Url master stopping")
    print("Acknowledged articles", len(processed_articles))

def GraphMaster():
    print("graph master spawned, pid is ", os.getpid())
    try:
        G = nx.DiGraph()
        prev_amount = 0
        while True:
            parent, child = edge_queue.get(timeout=150)
            G.add_edge(parent, child)
            if len(G.nodes()) % 1000 == 0 and len(G.nodes()) != prev_amount:
                prev_amount = len(G.nodes()) 
                print(len(G.nodes()), "nodes in graph; ", len(G.edges()), "edges in graph; ", article_queue.qsize(), "articles in queue; ", utility_queue.qsize(), "utility queue size", edge_queue.qsize(), "graph queue size")
    except queue.Empty:
        pass
    pickle.dump(G, open("Graph", "wb"))
    print("graph master finishing...")

def DataKeeper():
    print("data keeper spawned, pid is ", os.getpid())
    articles = {}
    try:
        counter = 0
        while True:
            name, article = data_queue.get(timeout=150)
            f = open("data/" + str(name) + ".dump", "w")
            f.write(article)
            counter += 1
            if counter % 100 == 0:
                print(counter, "events saved")
    except queue.Empty:
        pass
    print("data keeper finalizing...")
    print(str(len(articles)) + " articles collected")

if __name__ == '__main__':
    article_workers = []
    for i in range(config.article_crawlers):
        task = mp.Process(target = ArticleCrawler)
        task.start()
        article_workers.append(task)

    utility_workers = []
    for i in range(config.utility_crawlers):
        task = mp.Process(target = UtilityCrawler)
        task.start()
        utility_workers.append(task)

    data_master = mp.Process(target = DataKeeper)
    data_master.start()

    graph_master = mp.Process(target = GraphMaster)
    graph_master.start()

    url_master = mp.Process(target = UrlMaster)
    url_master.start()
    article_queue.put((config.start_page, "#Main"))


