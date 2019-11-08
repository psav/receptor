import yaml
import socket
import random
import subprocess
import click
from collections import namedtuple
from time import sleep
DEBUG=False
def random_port(tcp=True):
    """Get a random port number for making a socket

    Args:
        tcp: Return a TCP port number if True, UDP if False

    This may not be reliable at all due to an inherent race condition. This works
    by creating a socket on an ephemeral port, inspecting it to see what port was used,
    closing it, and returning that port number. In the time between closing the socket
    and opening a new one, it's possible for the OS to reopen that port for another purpose.

    In practical testing, this race condition did not result in a failure to (re)open the
    returned port number, making this solution squarely "good enough for now".
    """
    # Port 0 will allocate an ephemeral port
    socktype = socket.SOCK_STREAM if tcp else socket.SOCK_DGRAM
    s = socket.socket(socket.AF_INET, socktype)
    s.bind(("", 0))
    addr, port = s.getsockname()
    s.close()
    return port


procs = []


Node = namedtuple('Node', ['name', 'controller', 'listen_port', 'connections'])


def generate_random_mesh(controller_port, node_count, conn_method):
    a = {'controller': Node('controller', True, controller_port, [])}

    for i in range(node_count):
        a[f'node{i}'] = Node(f'node{i}', False, random_port(), [])

    for k, node in a.items():
        if node.controller == True:
            continue
        else:
            node.connections.extend(conn_method(a, node))
    return a



def do_it(topology):
    for k, node in topology.items():
        if node.controller == True:
            if not DEBUG:
                op = subprocess.Popen(" ".join(["receptor", "--debug", "-d", "/tmp/receptor", "--node-id", "controller", "controller", "--socket-path=/tmp/receptor/receptor.sock", f"--listen-port={node.listen_port}"]), shell=True)
                procs.append(op)
            sleep(2)
        else:
            peer_string = " ".join([f"--peer=localhost:{topology[pnode].listen_port}" for pnode in node.connections])
            if not DEBUG:
                op = subprocess.Popen(" ".join(
                    ["receptor", "-d", "/tmp/receptor", "--node-id", node.name, "node", f"--listen-port={node.listen_port}",
                     peer_string]), shell=True)
                procs.append(op)
            print(f" receptor --node-id {node.name} node --listen-port={node.listen_port} {peer_string}")

    try:
        while True:
            sleep(1)
    except KeyboardInterrupt:
        for proc in procs:
            proc.kill()


@click.group(help="Helper commands for application")
def main():
    pass


@main.command("random")
@click.option("--debug", is_flag=True, default=False)
@click.option("--controller-port", help="Chooses Controller port", default=8888)
@click.option("--node-count", help="Choose number of nodes", default=10)
@click.option("--max-conn-count", help="Choose max number of connections per node", default=2)
def randomize(controller_port, node_count, max_conn_count, debug):
    if debug:
        global DEBUG
        DEBUG=True

    def peer_function(nodes, cur_node):
        return random.choices(list(set(nodes.keys()) - {cur_node.name}), k=int(random.random() * max_conn_count + 1))

    node_topology = generate_random_mesh(controller_port, node_count, peer_function)
    print(node_topology)
    do_it(node_topology)

@main.command("flat")
@click.option("--debug", is_flag=True, default=False)
@click.option("--controller-port", help="Chooses Controller port", default=8888)
@click.option("--node-count", help="Choose number of nodes", default=10)
def flat(controller_port, node_count, debug):
    if debug:
        global DEBUG
        DEBUG=True

    def peer_function(nodes, cur_node):
        return ['controller']

    node_topology = generate_random_mesh(controller_port, node_count, peer_function)
    print(node_topology)
    do_it(node_topology)

@main.command("file")
@click.option("--debug", is_flag=True, default=False)
@click.argument("filename", type=click.File('r'))
def file(filename, debug):
    data = yaml.safe_load(filename)
    topology = {}
    for node, definition in data['nodes'].items():
        topology[node] = Node(definition['name'], definition['controller'], definition.get('listen_port', random_port()), definition['connections'])
    do_it(topology)
if __name__ == "__main__":
    main()
