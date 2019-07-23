#!/usr/bin/env python
# coding=utf-8
import json
import redis
import time
import uuid
def gen_req_id():
    return str(uuid.uuid1()).replace('-', '')

class ASYNCResult(object):
    def __init__(self,conn,req_id,hash_table='result'):
        self.conn = conn
        self.req_id = req_id
        self.hash_table = hash_table
        self.resp={}

    def _get(self):

        self.resp = self.conn.hgetall("result_%s"%self.req_id)
    @property
    def isDone(self):
        # 0: processing 1:done 2 error
        self._get()
        return self.resp['state'] !='0'

    @property
    def result(self):
        self._get()
        return json.loads(self.resp['data'])

    @property 
    def info(self):
        self._get()
        return self.resp['info']

    @property 
    def state(self):
        self._get()
        return self.resp['state']
    
    def __str__(self):
        self._get()
        return '(ASYNCResult: state:%s, info:%s, data:%s)' % (self.resp.get("state"), self.resp.get("info"),self.resp.get("data"))
    __repr__ = __str__


class RPCClient(object):
    def __init__(self,host,password,port=6379,db=0,list='job',hash_table='result'):
        self.queue = list
        self.retry = 0
        self.hash_table = hash_table
        while True:
          try:
             self.redis_conn = redis.Redis(host=host,password=password, port=port, db=db)
             break
          except Exception as e:
              print e
              self.retry += 1
              time.sleep(4)
              if self.retry == 10:
                  break
        self.pubsub = self.redis_conn.pubsub()

    def sync_call(self,function_name,*args, **kwargs):
        self.req_id = gen_req_id()
        task = {"req_id":self.req_id,
                "function":function_name,
                "args":args,
                "kwargs":kwargs,
                "type":"sync",
                "req_time":int(time.time())
               }
        #print json.dumps(task)
        self.redis_conn.lpush(self.queue,json.dumps(task))
        self.pubsub.subscribe(self.req_id)
        result=""
        for resp in self.pubsub.listen():
            #print resp
            if resp['type'] == 'message':
                 result = json.loads(resp['data'])
                 print type(result)
                 self.pubsub.unsubscribe()
                 break
        return result

    def async_call(self,function_name,*args,**kwargs):
        self.req_id = gen_req_id()

        task = {
                "req_id":self.req_id,
                "function":function_name,
                "args":args,
                "kwargs":kwargs,
                "type":"async",
               }
        result = {
             "req_id":self.req_id,
             "req_time":int(time.time()),
             "state":0,
             "info":"",
             "data":""
         }
        for k,v in result.items():
            self.redis_conn.hset("result_%s"%self.req_id, k, v)
        self.redis_conn.lpush(self.queue,json.dumps(task))
        return ASYNCResult(self.redis_conn,self.req_id)


if __name__ == '__main__':
    rpc  = RPCClient("127.0.0.1","antiy_redis")
    result = rpc.async_call('add',1,12)
    print result
    print result
    time.sleep(4)
    print result
