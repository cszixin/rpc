#!/usr/bin/env python
# coding=utf-8

import time

def test(a, b, c, d="123", e="456"):
    return (a, b, c, d, e)


def add(a, b):
    time.sleep(3)
    return {"result":a+b}
