#!/usr/bin/env python
# coding=utf-8
import json
import redis
import time
from tools import test, add


class RPCServer(object):
    def __init__(self, host, password, port=6379, db=0, list='job'):
        self.queue = list
        self.retry = 0
        self.servicemap = {}
        while True:
            try:
                self.redis_conn = redis.Redis(
                    host=host, password=password, port=port, db=db)
                break
            except Exception as e:
                print e
                self.retry += 1
                time.sleep(4)
                if self.retry == 10:
                    break

    def register(self, func):
        self.servicemap[func.func_name] = func
    
    def async_handler(self,req):
        req_id = req['req_id']
        func = self.servicemap.get(req['function'], '')
        result = self.redis_conn.hgetall("result_%s"%req_id)
        result['finish_time'] = int(time.time())
        result['info'] = 'success'
        result['data'] = ""
        self.redis_conn.expire("result_%s"%req_id,100)
        if not func:            
            result['info'] = "no this function"          
            result['state'] = 2
            self.redis_conn.hmset("result_%s"%req_id,result)
            return
        try:
            res = func(*req['args'], **req['kwargs'])
        except Exception as e:
            result['info'] = str(e)
            result['state'] = 2
            self.redis_conn.hmset("result_%s"%req_id,result)
            return
        result['data'] = res
        result['state'] = 1 
        self.redis_conn.hmset("result_%s"%req_id,result)
        return         

    def sync_handler(self, req):
        result = {"state": 1, "info": "success", "req_time": req["req_time"]}
        req_id = req['req_id']
        func = self.servicemap.get(req['function'], '')
        if not func:
            result['finish_time'] = int(time.time())
            result['info'] = "no this function"
            result['data'] = ""
            result['state'] = 0
            self.send_message(req_id, json.dumps(result))
            return
        try:
            res = func(*req['args'], **req['kwargs'])
        except Exception as e:
            result['finish_time'] = int(time.time())
            result['info'] = str(e)
            result['data'] = ""
            result['state'] = 0
            self.send_message(req_id, json.dumps(result))
            return
        result['finish_time'] = int(time.time())
        result['data'] = res
        self.send_message(req_id, json.dumps(result))
        print 'publish success'
        return

    def send_message(self, channel, result):
        count = self.redis_conn.publish(channel, result)
        while count == 0:
            count = self.redis_conn.publish(channel, result)

    def handler(self):
        while True:
            # 同步阻塞的取消息
            channel,req = self.redis_conn.brpop(self.queue)
            req = json.loads(req)
            if req['type'] == 'sync':
                self.sync_handler(req)
            elif req['type'] == 'async':
                self.async_handler(req)


if __name__ == '__main__':
    rpc = RPCServer("192.168.48.100", "antiy_redis")
    rpc.register(test)
    rpc.register(add)
    rpc.handler()
