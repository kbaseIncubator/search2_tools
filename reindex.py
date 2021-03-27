#!/opt/anaconda3/bin/python

from confluent_kafka import Producer
import json
import sys
import os

fn = sys.argv[-1]


def _delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed:', err)
#    else:
#        print(f"Message delivered to topic '{msg.topic()}': {msg.value()}")

def _read_ckpt(fn):
   if not os.path.exists(fn):
       return 0
   with open(fn) as f:
       data = f.read().rstrip()
   return int(data)

def _write_ckpt(ct, fn):
   with open(fn, 'w') as f:
       f.write('%d\n' % (ct))

admin_topic='prodbackadmin'

producer = Producer({'bootstrap.servers': 'kafka01'})

ct = 0
#evtype = 'REINDEX'
evtype = 'INDEX_NONEXISTENT'


acton = True
force = False
for arg in sys.argv:
    if arg=='-f':
        evtype = 'REINDEX'
    elif arg=='-n':
        acton = False

ckpt_fn = fn + '.ckpt'
ckpt = _read_ckpt(ckpt_fn)

with open(fn) as f:
     for line in f:
         # Scan pass previous lines
         if ct < ckpt:
             ct += 1
             continue
         (wsid, objid) = line.rstrip().split('\t')[0].split('/')
         data = {'evtype': evtype, 'wsid': wsid, 'objid': objid}
         print(data)
         if acton:
             producer.produce(admin_topic, json.dumps(data), callback=_delivery_report)
             if ct%10 == 0:
                 producer.flush()
         ct += 1
#         if ct> 11:
#             break

producer.flush()
print(ct)

if acton:
    _write_ckpt(ct, ckpt_fn)
