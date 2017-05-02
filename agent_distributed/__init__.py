#!/usr/bin/env python
# -*- coding: utf-8 -*-

from functools import partial
from distributed import Client
from agent import sender
from agent.agent.agent import DefaultParser
from agent.agent.event import Event

__author__ = 'tong'
__version__ = '1.0.0'


class Parser(object):
    parser = None

    @classmethod
    def map(cls, x):
        return [Event(cls.parser.parse(i).result()) for i in x]

    @classmethod
    def reduce(cls, x):
        return sum(x, [])


class Sender(sender.BatchSender):
    def __init__(self, *args, **kwargs):
        super(Sender, self).__init__(*args, **kwargs)
        self.client = None
        self.parser = None
        self.sections = kwargs.get('sections', 10)

    def send(self, event):
        self._queue.put(event.raw_data)
        if self._queue.qsize() >= self._flush_size:
            self.need_flush.set()

    def catch(self, agent):
        super(Sender, self).catch(agent)
        args = agent.client.args
        kwargs = agent.client.kwargs
        self.client = Client(*args, **kwargs)
        self.parser = agent.real_parser

    def push(self):
        ret = False
        if not self._buffers:
            self._buffers.append([])
            for i in range(self._max_batch_size):
                if not self._queue.empty():
                    self._buffers[-1].append(self._queue.get())
                    if len(self._buffers[-1]) >= int(self._max_batch_size/self.sections):
                        self._buffers.append([])
                else:
                    break

        if self._buffers:
            buffers = self.client.map(Parser.map, self._buffers)
            buffers = self.client.submit(Parser.reduce, buffers).result()
            if hasattr(self._output, 'sendmany'):
                self._output.sendmany(buffers)
            else:
                for event in buffers:
                    self._output.send(event)
            self._buffers = []

        if self._queue.qsize() < self._flush_size:
            ret = True
        return ret


class AgentClient(object):
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs


class Distribute(object):
    def __init__(self, client, *args, **kwargs):
        self.client = client
        self.args = args
        self.kwargs = kwargs

    def __call__(self, agent):
        agent.sender = Sender(agent.sender, *self.args, **self.kwargs)
        agent.client = self.client
        agent.real_parser = agent.parser
        agent.parser = DefaultParser()
        return agent
