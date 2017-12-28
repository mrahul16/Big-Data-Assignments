
from kazoo.client import KazooClient
from kazoo.client import KazooState
from kazoo.protocol.states import EventType
import logging
import os

num_children = 0
logging.basicConfig()

zk = KazooClient(hosts='localhost:2181')
zk.start()


def con_state_listener(state):
    if state == KazooState.LOST:
        print("Lost")
    elif state == KazooState.SUSPENDED:
        print("Suspended")
    else:
    	print("Being Connected/Reconnected")


def set_master(path):
	zk.set("/nodes", ("%s" % path).encode("utf-8"))
	master_str = "I (%s) am the master" % path
	print(master_str)

def get_master():
	data, _ = zk.get("/nodes")
	return data.decode("utf-8")

def master_set():
	return get_master() != ""

def master_exists():
	return zk.exists(get_master())

def print_is_master(path):
	data, _ = zk.get("/nodes")
	if(path == get_master()):
		print("I (%s) am the master" % path)


zk.add_listener(con_state_listener)

zk.ensure_path("/nodes")
path = zk.create("/nodes/node-", 
	acl=None, 
	ephemeral=True,
	sequence=True,
	makepath=True)
print("Path:", path)
print(get_master())
if not master_set():
	set_master(path)

def election(children):
	if len(children) > 0:
		if master_set() and master_exists():
			return

		children = [int(child.split('-')[1]) for child in children]
		leader = max(children)
		node_num = int(path.split('-')[1])
		if node_num == leader:
			set_master(path)
			os.system('python3 /home/mrahul16/zkpr.py')


@zk.ChildrenWatch("/nodes")
def watch_children(children):
	global num_children
	l = len(children)
	if l > num_children:
		print("Node added")
		election(children)
	elif l < num_children:
		print("Node deleted")
		election(children)
	num_children = l
	if num_children == 1:
		pass
	print("Children nodes:", children)

print(zk.get_children("/nodes",watch=watch_children))
while True:
	pass
