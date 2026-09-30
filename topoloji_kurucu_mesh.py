from mininet.net import Mininet
from mininet.node import RemoteController, OVSKernelSwitch
from mininet.link import TCLink
from mininet.cli import CLI
from mininet.log import setLogLevel
import os

# ---------------------------------------------------------------------------
# MESH TOPOLOGY - 4 Alternative Paths
#
# The "edge switch -> core switch -> edge switch" logic from the diamond
# topology is preserved; only the number of core (backbone) switches is
# increased from 2 to 4. pe1 and pe2 are each connected to 4 separate core
# switches, each with a different delay/bandwidth/loss profile. This creates
# 4 distinct end-to-end paths between h1 and h2, and Ryu can select among
# these paths based on QoS class and anomaly state.
#
# Path definitions (use these path_label values when collecting the dataset):
#   Path A ("PathA_20ms")  : Wide backbone       - bw=1000Mbps, delay=20ms
#   Path B ("PathB_2ms")   : Bottleneck          - bw=100Mbps,  delay=2ms,  intentional loss
#   Path C ("PathC_10ms")  : Balanced path       - bw=500Mbps,  delay=10ms
#   Path D ("PathD_50ms")  : High delay          - bw=200Mbps,  delay=50ms, low loss
# ---------------------------------------------------------------------------

# Each core path's properties are defined as a dict so they are managed from
# a single place. To add a new alternative path, simply add a new entry here.
PATHS = [
    {
        "name": "p1",
        "path_label": "PathA_20ms",
        "description": "Path A - Wide Backbone",
        "bw": 1000,
        "delay": "20ms",
        "loss": 0,
    },
    {
        "name": "p2",
        "path_label": "PathB_2ms",
        "description": "Path B - Bottleneck",
        "bw": 100,
        "delay": "2ms",
        "loss": 2,          # intentional light loss -> for anomaly scenarios
        "max_queue_size": 1000,
    },
    {
        "name": "p3",
        "path_label": "PathC_10ms",
        "description": "Path C - Balanced Path",
        "bw": 500,
        "delay": "10ms",
        "loss": 0,
    },
    {
        "name": "p4",
        "path_label": "PathD_50ms",
        "description": "Path D - High Delay / Low Loss",
        "bw": 200,
        "delay": "50ms",
        "loss": 0.5,
    },
]


def build_mesh_topology():
    net = Mininet(controller=RemoteController, switch=OVSKernelSwitch, link=TCLink)

    print("*** Adding Controller (Ryu SDN Controller) ***")
    net.addController('c0', controller=RemoteController, ip='127.0.0.1', port=6633)

    print("*** Adding Edge Switches ***")
    pe1 = net.addSwitch('s1')  # Edge 1 (h1 side)
    pe2 = net.addSwitch('s2')  # Edge 2 (h2 side)

    print("*** Adding Core Switches - %d Alternative Paths ***" % len(PATHS))
    core_switches = {}
    for i, path in enumerate(PATHS, start=3):  # s3, s4, s5, s6 ...
        sw = net.addSwitch('s%d' % i)
        core_switches[path["name"]] = sw

    print("*** Adding End Hosts ***")
    h1 = net.addHost('h1', ip='10.0.0.1')
    h2 = net.addHost('h2', ip='10.0.0.2')

    print("*** Connecting Edge-Host Links ***")
    net.addLink(h1, pe1, bw=1000, delay='1ms')
    net.addLink(pe2, h2, bw=1000, delay='1ms')

    print("*** Connecting Mesh Links (pe1 <-> core <-> pe2 for each path) ***")
    # We keep each path's pe1-side and pe2-side interface objects so we can
    # print the port numbers below (needed for the in_port/out_port mapping
    # on the Ryu side).
    link_info = []
    for path in PATHS:
        core = core_switches[path["name"]]
        link_kwargs = {"bw": path["bw"], "delay": path["delay"]}
        if path.get("loss"):
            link_kwargs["loss"] = path["loss"]
        if path.get("max_queue_size"):
            link_kwargs["max_queue_size"] = path["max_queue_size"]

        link1 = net.addLink(pe1, core, **link_kwargs)
        link2 = net.addLink(core, pe2, **link_kwargs)

        link_info.append({
            "path_label": path["path_label"],
            "description": path["description"],
            "pe1_port": link1.intf1.name,
            "pe2_port": link2.intf2.name,
            "bw": path["bw"],
            "delay": path["delay"],
            "loss": path.get("loss", 0),
        })

    print("*** Starting Network ***")
    net.start()

    print("*** Applying QoS Queues (VIP, MEDIUM, LOW) to Each Core Port ***")
    for info in link_info:
        port = info["pe1_port"]
        max_rate_bps = int(info["bw"] * 1_000_000)          # Mbps -> bps
        q0_max = max_rate_bps
        q0_min = int(max_rate_bps * 0.7)
        q1_max = int(max_rate_bps * 0.3)
        q1_min = int(max_rate_bps * 0.2)
        q2_max = int(max_rate_bps * 0.1)

        cmd = (
            "ovs-vsctl -- set Port {port} qos=@newqos_{port} -- "
            "--id=@newqos_{port} create QoS type=linux-htb "
            "other-config:max-rate={max_rate} queues=0=@q0_{port},1=@q1_{port},2=@q2_{port} -- "
            "--id=@q0_{port} create Queue other-config:max-rate={q0_max} other-config:min-rate={q0_min} -- "
            "--id=@q1_{port} create Queue other-config:max-rate={q1_max} other-config:min-rate={q1_min} -- "
            "--id=@q2_{port} create Queue other-config:max-rate={q2_max}"
        ).format(
            port=port,
            max_rate=max_rate_bps,
            q0_max=q0_max, q0_min=q0_min,
            q1_max=q1_max, q1_min=q1_min,
            q2_max=q2_max,
        )
        os.system(cmd)

    print("\n*** PATH / PORT MAPPING (for your Ryu controller and data-collection script) ***")
    print("%-14s %-30s %-10s %-10s %-8s %-8s" % (
        "path_label", "description", "pe1_port", "pe2_port", "bw(Mb)", "delay"))
    for info in link_info:
        print("%-14s %-30s %-10s %-10s %-8s %-8s" % (
            info["path_label"], info["description"], info["pe1_port"],
            info["pe2_port"], info["bw"], info["delay"]))

    print("\n*** Switching to Mininet CLI ***")
    CLI(net)

    print("*** Stopping Network ***")
    net.stop()


if __name__ == '__main__':
    setLogLevel('info')
    build_mesh_topology()