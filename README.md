# Distributed Agent

The framework to extend `python-agent` to a distributed architecture

#### install:

```shell
pip install git+https://github.com/sandabuliu/distributed-agent.git
```
or      
```shell
git clone https://github.com/sandabuliu/distributed-agent.git
cd distributed-agent
python setup.py install
```

#### example:

```python
from agent import rule, source, output, Agent
from agent_distributed import Distribute, AgentClient

s = source.File('/var/log/nginx/access.log')
o = output.Screen()
a = Agent(s, o, rule=rule('nginx'))
a = Distribute(AgentClient('192.168.1.151:8786'), flush_max_time=10, max_size=5000, queue_size=20000)(a)
a.start()
```