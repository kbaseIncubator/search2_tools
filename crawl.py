#!/opt/anaconda3/bin/python


from kbase_workspace_client import WorkspaceClient
from kbase_workspace_client.exceptions import WorkspaceResponseError
import os
import sys
from elasticsearch.helpers import scan
from elasticsearch import Elasticsearch
from yaml import load
from yaml import CLoader as Loader

# TODO: Make this configurable
kbase_endpoint = 'https://kbase.us/services/ws'
ws_token = os.environ['KB_ADMIN_TOKEN']
# TODO: Make this configurable
es_url = 'es01:9200'
# TODO: Make this configurable
es_ns = 'search2prod.default_search'

if (len(sys.argv) < 3):
    print("Usage: ./crawl.py <log file> <start> [<stop>]")
    sys.exit(1)

fn = sys.argv[1]


relog = os.environ.get('RELOG', False)
if relog:
    reindexlog = open(fn, 'a')

def query_elastic(es, wsid):
    q={"query": {"match": {"access_group": wsid}}}
    obj_list = []
    for d in scan(es, query=q, index=es_ns):
        obj_list.append(d['_source']['obj_id'])
    return obj_list


def compare_ws(ws_client, es, wsid, excl_lst):
    ela = query_elastic(es, wsid) 
    try:
       wsi = ws_client.admin_req( 'getWorkspaceInfo', { 'id': wsid})
       infos = ws_client.generate_obj_infos(wsid, admin=True)
    except WorkspaceResponseError as ex:
       return

    # [70002,
    #  'jmcgonigle:narrative_1597768019342',
    #  'jmcgonigle',
    #  '2020-08-18T16:26:59+0000',
    #  1,
    #  'n',
    #  'n',
    #  'unlocked',
    #  {'cell_count': '1', 'searchtags': 'narrative', 'is_temporary': 'true', 'narrative': '1'}
    # ]
    ws_meta = wsi[8]
    if ('is_temporary' in ws_meta ) and ws_meta['is_temporary']=='true':
        return
    if 'narrative' not in ws_meta:
        sys.stderr.write("# Skipping %d since it isn't a narrative\n" % (wsid))
        return

    ct = 0
    bad = False
    try:
        for i in infos:
            ws_type = i[2].split('-')[0]
            if ws_type in excl_lst:
                continue
            obj_id = i[0]
            if obj_id not in ela:
                print("Missing %s/%d %s" % (wsid, obj_id, ws_type))
                bad = True
                if relog:
                    reindexlog.write('%d/%d\t%s\n' % (wsid, obj_id, ws_type))
            ct += 1
    except WorkspaceResponseError:
       return
    if bad:
        print("# %d versus %d (%d)" % (ct, len(ela), ct-len(ela)))

if __name__ == '__main__':
    with open('config.yaml') as f:
        config = load(f, Loader=Loader)
    excl_lst = config['ws_type_blacklist']
    es = Elasticsearch(es_url)
    start = int(sys.argv[2])
    stop = start + 1
    if len(sys.argv) > 3:
       stop = int(sys.argv[3])
    ws_client = WorkspaceClient(url=kbase_endpoint, token=ws_token)
    for wsid in range(start, stop):
         if (wsid % 100) == 0:
             sys.stderr.write("# Checking %d\n" % (wsid))
         compare_ws(ws_client, es, wsid, excl_lst)
