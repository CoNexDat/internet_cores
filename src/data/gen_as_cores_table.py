import pandas as pd
import random
import networkx as nx
import numpy as np
import matplotlib.pyplot as plt
from os import listdir
from os.path import isfile, join
import csv
import re
import datetime
import subprocess
import os
from urllib.request import Request, urlopen, urlretrieve
from bs4 import BeautifulSoup
import logging
from config import Config

#Load configuration parameters
with open('caida.cfg', 'rt') as f:
    cfg = Config(f)

# Initialize logging feature    
log_file=cfg.log_dir+cfg.log_prefix+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")+".log"
log_level=[logging.DEBUG,logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL]
logging.basicConfig(filename=log_file,level=log_level[cfg.log_level],format='%(asctime)s %(message)s')
logging.debug('Debug: Initializing...')
logging.info('Info: Initializing...')
logging.warning('Warning: Initializing...')
logging.error('Error: Initializing...')
logging.critical('Critical: Initializing...')

BGP_URLs=["http://data.caida.org/datasets/as-relationships/serial-1/","http://data.caida.org/datasets/as-relationships/serial-2/"]
ARK_URLs=["http://data.caida.org/datasets/topology/ark/ipv4/as-links/team-1/","http://data.caida.org/datasets/topology/ark/ipv4/as-links/team-2/","http://data.caida.org/datasets/topology/ark/ipv4/as-links/team-3/"]
SKITTER_URLs=["http://data.caida.org/datasets/topology/skitter-aslinks/"]
AS_CLASSIFICATION_URL='http://data.caida.org/datasets/as-classification/'
ASNames_URL='https://www.cidr-report.org/as2.0/autnums.html'

def addARKFileToGraph(file,Graph):
    df=pd.read_csv(file,names=["Tipo","campo0","campo1","campo2","campo3","campo4","campo5","campo6","campo7","campo8","campo9","campo10","campo11","campo12","campo13","campo14","campo15","campo16","campo17","campo18","campo19","campo20","campo21","campo22","campo23","campo24","campo25","campo26","campo27","campo28","campo29","campo30","campo31","campo32","campo33","campo34","campo35","campo36","campo37","campo38","campo39","campo40","campo41","campo42","campo43","campo44","campo45","campo46","campo47","campo48","campo49","campo50","campo51","campo52","campo53","campo54","campo55","campo56","campo57","campo58","campo59"],sep="\t",comment="#", error_bad_lines=False, encoding='utf8', engine='python',usecols=["Tipo","campo0","campo1"],index_col=False)
    a=df[df['campo0'].str.contains(",|_.",na=False)]["campo0"].str.split("_|,.")
    for i in a.index:
        df.loc[i,"campo0"]=random.choice(a[i])
    a=df[df['campo1'].str.contains(",|_.",na=False)]["campo1"].str.split("_|,.")
    for i in a.index:
        df.loc[i,"campo1"]=random.choice(a[i])
    df_direct=df[df.Tipo=="D"]
    Graph.add_edges_from(df_direct[["campo0","campo1"]].values)

def addBGPFileToGraph(file,Graph):
    grafo=pd.read_csv(file,error_bad_lines=False, encoding='utf8', engine='python',sep="|",comment="#", names=["AS1" , "AS2", "type"],dtype={'AS1':object,'AS2':object},index_col=False)
    grafo=grafo.drop("type",1)
    grafo_proc=grafo[grafo["AS1"].str.isdigit() & grafo["AS2"].str.isdigit()]
    Graph.add_edges_from(grafo_proc[["AS1","AS2"]].values)
def parse_asn_page(url):
    req = Request(url)
    a = urlopen(req).read()
    soup = BeautifulSoup(a, 'html.parser')
    return soup
def read_url(url):
    files=list()
    url = url.replace(" ","%20")
    req = Request(url)
    a = urlopen(req).read()
    soup = BeautifulSoup(a, 'html.parser')
    x = (soup.find_all('a'))
    for i in x:
        file_name = i.extract().get_text()
        url_new = url + file_name
        url_new = url_new.replace(" ","%20")
        if(file_name[-1]=='/' and file_name[0]!='.'):
            read_url(url_new)
        files.append(url_new)
    return files

def generate_ark_graph(year, month):
    logging.info('generate_ark_graph: Function called with parameters year={}, month={}'.format(year,month))
    G=nx.Graph()
    for url in ARK_URLs:
        logging.info("generate_ark_graph: Getting info from: {}".format(url))
        try:
            files=read_url(url+str(year)+"/")
            selected_files=[files[i] for i in range(5, len(files)) if (files[i][98:104]=='{}{:0>2d}'.format(year,month))]
            logging.debug(selected_files)
            for file in selected_files:
                logging.info("generate_ark_graph: Downloading file from: {}".format(file))
                urlretrieve (file,cfg.tmp_dir+"ark.txt.gz")
                subprocess.call(['gzip','-d','-f', cfg.tmp_dir+"ark.txt.gz"])
                addARKFileToGraph(cfg.tmp_dir+"ark.txt",G)
        except:
            logging.warning("generate_ark_graph: Can't get info from: {}".format(url+str(year)+"/"))
    logging.info("generate_ark_graph: Removing non connected components of the graph.")
    sub_graphs=sorted(nx.connected_component_subgraphs(G), key = len, reverse=True)
    if(sub_graphs):
        for graph in sub_graphs:
            logging.info("generate_ark_graph: Subgraph component found. Size: " + str(len(graph)))
        G=sub_graphs[0]
        logging.info("generate_ark_graph: Removing self loops.")
        G.remove_edges_from(G.selfloop_edges()) #Agregué esta línea para eliminar los self loops
        nx.write_adjlist(G, cfg.path_ark+'{}{}00.net'.format(year,month), comments='#', delimiter=' ', encoding='utf-8')
    
def generate_skitter_graph(year, month):
    logging.info('generate_skitter_graph: Function called with parameters year={}, month={}'.format(year,month))
    G=nx.Graph()
    for url in SKITTER_URLs:
        logging.info("generate_skitter_graph: Getting info from: {}".format(url))
        try:
            files=read_url(url+str(year)+"/"+str(month)+"/")
            selected_files=[files[i] for i in range(5, len(files)) if (files[i][81:87]=='{}{:0>2d}'.format(year,month))]
            logging.debug(selected_files)
            for file in selected_files:
                logging.info("generate_skitter_graph: Downloading file from: {}".format(file))
                urlretrieve (file,cfg.tmp_dir+"skitter.txt.gz")
                subprocess.call(['gzip','-d','-f', cfg.tmp_dir+"skitter.txt.gz"])
                addARKFileToGraph(cfg.tmp_dir+"skitter.txt",G)
        except:
            logging.warning("generate_skitter_graph: Can't get info from: {}".format(url+str(year)+"/"))
    logging.info("generate_skitter_graph: Removing non connected components of the graph.")
    sub_graphs=sorted(nx.connected_component_subgraphs(G), key = len, reverse=True)
    if(sub_graphs):
        for graph in sub_graphs:
            logging.info("generate_skitter_graph: Subgraph component found. Size: " + str(len(graph)))
        G=sub_graphs[0]
        logging.info("generate_skitter_graph: Removing self loops.")
        G.remove_edges_from(G.selfloop_edges()) #Agregué esta línea para eliminar los self loops
        nx.write_adjlist(G, cfg.path_skitter+'{}{}00.net'.format(year,month), comments='#', delimiter=' ', encoding='utf-8')
    
def generate_BGP_graph(year, month):
    logging.info('generate_BGP_graph: Function called with parameters year={}, month={}'.format(year,month))
    G=nx.Graph()
    for url in BGP_URLs:
        logging.info("generate_BGP_graph: Getting info from: {}".format(url))
        try:
            files=read_url(url)
            selected_files=[files[i] for i in range(5, len(files)) if (files[i][57:63]=='{}{:0>2d}'.format(year,month) 
                                                                   and (files[i][66:72]=='as-rel'))]
            logging.debug(selected_files)
            for file in selected_files:
                logging.info("generate_BGP_graph: Downloading file from: {}".format(file))
                urlretrieve (file,cfg.tmp_dir+"bgp.txt.bz2")
                subprocess.call(['bzip2','-d','-f', cfg.tmp_dir+"bgp.txt.bz2"])
                addBGPFileToGraph(cfg.tmp_dir+"bgp.txt",G)
        except:
            logging.warning("generate_BGP_graph: Can't get info from: {}".format(url))
    logging.info("generate_BGP_graph: Removing non connected components of the graph.")
    sub_graphs=sorted(nx.connected_component_subgraphs(G), key = len, reverse=True)
    if(sub_graphs):
        for graph in sub_graphs:
            logging.info("generate_BGP_graph: Subgraph component found. Size: " + str(len(graph)))
        G=sub_graphs[0]
        logging.info("generate_BGP_graph: Removing self loops.")
        G.remove_edges_from(G.selfloop_edges()) #Agregué esta línea para eliminar los self loops
        nx.write_adjlist(G, cfg.path_bgp+'{}{}00.net'.format(year,month), comments='#', delimiter=' ', encoding='utf-8')
    
def get_merged_graph(year, month, skitter=cfg.try_skitter, ark=cfg.try_ark, bgp=cfg.try_bgp):
    logging.info('get_merged_graph: Function called with parameters year={}, month={}'.format(year,month))
    if not (skitter or ark or bgp):
        logging.error("get_merged_graph: Nothing to do!")
        return None
    dirs=list()
    sources=list()
    download=list()
    if skitter:
        sources.append('skitter')
        download.append(generate_skitter_graph)
        dirs.append(cfg.path_skitter)
    if ark:
        sources.append('ark')
        download.append(generate_ark_graph)
        dirs.append(cfg.path_ark)
    if bgp:
        sources.append('bgp')
        download.append(generate_BGP_graph)
        dirs.append(cfg.path_bgp)
    G=nx.Graph()
    for idx,directory in enumerate(dirs):
        try:
            aux_graph=nx.read_adjlist(directory+'{}{}00.net'.format(year,month))
            G = nx.compose(G,aux_graph)
            logging.info("get_merged_graph: graph from {} successfully added".format(directory+'{}{}00.net'.format(year,month)))
        except:
            logging.error("get_merged_graph: No info available for {}. Trying to download".format(sources[idx]))
            download[idx](year,month)
            try:
                aux_graph=nx.read_adjlist(directory+'{}{}00.net'.format(year,month))
                G = nx.compose(G,aux_graph)
                logging.info("get_merged_graph: graph from {} successfully added".format(directory+'{}{}00.net'.format(year,month)))
            except:
                logging.error("get_merged_graph: No info available for {}. Giving up.".format(sources[idx]))
    return G

def get_kcores(year, month,skitter=cfg.try_skitter, ark=cfg.try_ark, bgp=cfg.try_bgp,normalize=True):
    logging.info('get_kcores: Function called with parameters year={}, month={}'.format(year,month))
    G=get_merged_graph(year,month, skitter=skitter, ark=ark, bgp=bgp)
    k_cores=nx.core_number(G)
    if k_cores:
        if normalize:
            top_core=max(k_cores.values())
            k_cores={key:value/top_core for key,value in k_cores.items()}
    return k_cores

def add_month_to_df(df, year, month, normalize=True):
    logging.info('add_month_to_df: Function called with parameters year={}, month={}, normalize={}'.format(year,month,normalize))
    kcores_dict=get_kcores(year, month, skitter=cfg.try_skitter, ark=cfg.try_ark, bgp=cfg.try_bgp,normalize=normalize)
    if kcores_dict:
        index=(year-cfg.zero_year)*12+month
        aux_df=pd.DataFrame(kcores_dict, index=[float(index)])
        df=df.append(aux_df,sort=True).fillna(0)
    return df

def add_year_kcores(year, from_month, to_month,normalize=True,df=None):
    if type(df)==type(None):
        df=pd.DataFrame()
        df.index.name='month'
    logging.info('get_year_kcores: Function called with parameters year={}, from_month={}, to_month={}, normalize={}'.format(year,from_month,to_month,normalize))
    for month in range(from_month, to_month+1):
        df=add_month_to_df(df, year, month, normalize=True)
    return df

def add_kcores(from_year, from_month, to_year, to_month, normalize=True,input_file=None,output_prefix='output'):
    if not input_file:
        df=pd.DataFrame()
        df.index.name='month'
    elif input_file.split('.')[1]=='csv':
        df=pd.read_csv(cfg.output_prefix+input_file,index_col='month')
    elif input_file.split('.')[1]=='hdf':
        df=pd.read_hdf(cfg.output_prefix+input_file,index_col='month')
    else:
        exit(1)
    if to_year<from_year:
        return None
    elif to_year==from_year:
        add_year_kcores(from_year, from_month, to_month,normalize=normalize,df=df)
    elif to_year>from_year:
        df=add_year_kcores(from_year, from_month, 12,normalize=normalize,df=df)
        for year in range(from_year+1,to_year):
            df=add_year_kcores(year, 1, 12,normalize=normalize,df=df)
        df=add_year_kcores(to_year, 1, to_month,normalize=normalize,df=df)
    df.to_csv(cfg.output_prefix+output_prefix+".csv")
    df.to_hdf(cfg.output_prefix+output_prefix+".csv",key='df', mode='w')
    return df

def generate_asn_table(output_prefix='asn',kcores_file='cores_norm8.hdf'):
    logging.info('generate_asn_table: Function called with parameters input={}, output={}'.format(input_file, output_file))
    try:
            files=read_url(AS_CLASSIFICATION_URL)
            last=0
            for i in range(5, len(files)):
                if files[i][49:57].isdigit() and int(files[i][49:57])>last:
                    last=int(files[i][49:57])
                    url=files[i]
            logging.debug("The last as classification file is: {}".format(url))
            logging.info("generate_asn_table: Downloading file from: {}".format(url))
            urlretrieve (url,cfg.tmp_dir+"as-classification.txt.gz")
            subprocess.call(['gzip','-d','-f', cfg.tmp_dir+"as-classification.txt.gz"])
            asn_class_df=pd.read_csv(cfg.tmp_dir+"as-classification.txt", sep='|',names=['ASNumber','Source','Class'], comment="#",index_col='ASNumber')
            return asn_class_df
    except:
        logging.warning("generate_asn_table: Can't get info from: {}".format(url))
    try:
        soup=parse_asn_page(ASNames_URL)
        kcores_df=pd.read_hdf(cfg.output_prefix+kcores_file)
        asn_df=pd.DataFrame(columns=['ShortName','ASType','Country','StartGrow','StopGrow','MonthGrow'])
        asn_df.index.name='ASNumber'
        for line in soup.find_all('a'):
            index=line.string.replace(' ','')
            short_name=line.next_sibling.string.split(' - ')[0][1:]
            country=line.next_sibling.string.split(',')[-1][1:-1]
            if int(index[2:]) in as_class.index:
                as_type=as_class.loc[int(index[2:])]["Class"]
            else:
                as_type='UNK'
            asn_df.loc[index]=[short_name, as_type,country,'','','']
            if len(asn_df) % 10000 ==0:
                logging.info("generate_asn_table: Added {} entries to table.".format(len(asn_df)))
        for col_name in kcores_df.columns:
            if 'AS'+col_name not in asn_df.index:
                asn_df.loc['AS'+col_name]=['UNK', 'UNK','UNK','','','']
        logging.info("generate_asn_table: Finished. Added {} total entries to table.".format(len(asn_df)))
    except:
        logging.warning("generate_asn_table: Can't get info from: {}".format(ASNames_URL))
    df.to_csv(cfg.output_prefix+output_prefix+".csv")
    df.to_hdf(cfg.output_prefix+output_prefix+".hdf",key='df', mode='w')

def main():
    now = datetime.datetime.now()
    if now.month==1:
        month=12
        year=now.year-1
    else:
        month=now.month-1
        year=now.year  
    add_kcores(1998, 1, 
               year,month, 
               normalize=True,
               input_file=None,
               output_prefix='core_norm_{}_{}_{}'.format(now.year,now.month,now.day))
    generate_asn_table(output_prefix='asn',kcores_file='cores_norm8.hdf')
    
if __name__ == '__main__':
    main()