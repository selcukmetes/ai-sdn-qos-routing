#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
data_collection_controller.py
==============================================================================
Custom Ryu OpenFlow 1.3 controller for a static, single-active-path,
4-path (A / B / C / D) Mininet topology used for controlled data collection.

TOPOLOGY (as provided)
------------------------------------------------------------------------------
  s1 (DPID 1): port 1 -> h1 | port 2 -> Path A | port 3 -> Path B
                              | port 4 -> Path C | port 5 -> Path D
  s2 (DPID 2): port 1 -> h2 | ports 2/3/4/5 -> Paths A/B/C/D respectively
  s3-s6 (DPID 3-6): core switches, transparent 2-port bridges (port1<->port2)

DESIGN / ANTI-BROADCAST-STORM RATIONALE
------------------------------------------------------------------------------
This topology has 4 parallel paths between s1 and s2. If more than one of
those paths were ever forwarding at layer 2 simultaneously (e.g. via MAC
learning + flooding on an unknown destination), you would create a physical
loop and a broadcast storm. To prevent that entirely, this controller:

  1. NEVER uses OFPP_FLOOD, anywhere, under any circumstance.
  2. NEVER learns or forwards based on MAC addresses.
  3. Proactively pushes static, permanent FlowMods for every switch as soon
     as it connects (in the SwitchFeatures handshake), pinning ALL traffic
     onto exactly one path: the port named by ACTIVE_PATH_PORT.
  4. The PacketIn handler is kept only as a safety net for the brief window
     before static flows are installed (or any packet that manages to slip
     through). It replicates the exact same static policy and explicitly
     DROPS (never floods) anything that doesn't match the expected topology.

Because flows are installed proactively with no idle/hard timeout, the
controller is NOT in the per-packet forwarding path during normal operation.

QoS DESIGN
------------------------------------------------------------------------------
Traffic generator uses IP TOS values 16 / 32 / 48. In OpenFlow 1.3 we match
on `ip_dscp`, where DSCP = TOS >> 2:

    TOS 16 -> ip_dscp = 4  -> Queue 0 (VIP)
    TOS 32 -> ip_dscp = 8  -> Queue 1 (MEDIUM)
    TOS 48 -> ip_dscp = 12 -> Queue 2 (LOW)

Only s1's "host port 1 -> ACTIVE_PATH_PORT" direction gets DSCP-specific
flows (set_queue + output). ARP and any other IPv4 traffic (i.e. not DSCP
4/8/12) fall through to a plain output-only flow, per spec.

IMPORTANT PREREQUISITE (not something the controller can do for you):
The OVS queues (0, 1, 2) referenced by `set_queue` must already exist on
the physical egress port of s1 that corresponds to ACTIVE_PATH_PORT. The
controller only *selects* a queue ID per packet — it does not create queues
or their bandwidth limits. Configure them e.g. via:

    ovs-vsctl set port <s1-active-path-iface> qos=@newqos -- \
      --id=@newqos create qos type=linux-htb \
      other-config:max-rate=1000000000 \
      queues:0=@q0 queues:1=@q1 queues:2=@q2 -- \
      --id=@q0 create queue other-config:max-rate=1000000000 -- \
      --id=@q1 create queue other-config:max-rate=1000000000 -- \
      --id=@q2 create queue other-config:max-rate=1000000000

(Adjust rates/iface to your test plan. If you built the topology with
Mininet's TCLink and per-link queues already, confirm the queue IDs there
line up with 0/1/2 as used below.)

HOW TO CHANGE THE ACTIVE PATH
------------------------------------------------------------------------------
Edit ACTIVE_PATH_PORT below (2=A, 3=B, 4=C, 5=D) and restart ryu-manager.
On every switch (re)connection this controller flushes ALL existing flows
before reinstalling, so you do NOT need to restart Mininet/OVS to switch
paths between test runs — just restart the controller and let the switches
re-handshake (or run `ovs-ofctl del-flows <sX>` + let them resync, or simply
restart ryu-manager while the switches are still connected — Ryu will
re-run the handshake on reconnect; if your switches stay connected across
controller restarts, use `ovs-vsctl` / a quick `mn -c` cycle instead).

RUN
------------------------------------------------------------------------------
    ryu-manager data_collection_controller.py
==============================================================================
"""

from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib.packet import packet, ethernet, ether_types


# =============================================================================
# GLOBAL CONFIGURATION
# =============================================================================

# The single active path for this data-collection run.
#   2 = Path A   3 = Path B   4 = Path C   5 = Path D
# Same port number is used on BOTH s1 and s2, matching the topology's
# consistent A/B/C/D -> 2/3/4/5 numbering on both edge switches.
ACTIVE_PATH_PORT = 5

# Host-facing ports (fixed by the topology; independent of ACTIVE_PATH_PORT)
S1_HOST_PORT = 1
S2_HOST_PORT = 1

# Core switches are transparent 2-port bridges
CORE_DPIDS = [3, 4, 5, 6]
CORE_PORT_A = 1
CORE_PORT_B = 2

# DSCP -> Queue mapping (DSCP = TOS >> 2)
#   TOS 16 -> DSCP 4  -> Queue 0 (VIP)
#   TOS 32 -> DSCP 8  -> Queue 1 (MEDIUM)
#   TOS 48 -> DSCP 12 -> Queue 2 (LOW)
DSCP_QUEUE_MAP = {
    4: 0,   # VIP
    8: 1,   # MEDIUM
    12: 2,  # LOW
}

# Flow priorities (higher number = evaluated first)
PRIO_DEFAULT = 1    # table-miss -> controller (safety net only)
PRIO_BRIDGE = 10    # core switch transparent bridging
PRIO_L3_BASE = 20   # s1/s2 base forwarding (ARP + non-DSCP-matched IPv4)
PRIO_QOS = 30       # DSCP-matched QoS flows on s1 (must outrank base rule)


class DataCollectionController(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(DataCollectionController, self).__init__(*args, **kwargs)
        self.logger.info(
            "Data Collection Controller starting. ACTIVE_PATH_PORT=%d",
            ACTIVE_PATH_PORT,
        )

    # -------------------------------------------------------------------
    # Switch connection handshake: flush old flows, then proactively
    # install every static flow this switch needs. After this handler
    # runs, the controller is off the per-packet path for this switch.
    # -------------------------------------------------------------------
    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        datapath = ev.msg.datapath
        dpid = datapath.id
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        self.logger.info("Switch connected: DPID=%d", dpid)

        # 0) Wipe any pre-existing flows. Makes controller restarts (e.g.
        #    after changing ACTIVE_PATH_PORT) safe and idempotent instead
        #    of stacking old and new static paths on top of each other.
        self._clear_flows(datapath)

        # 1) Table-miss flow: send unmatched packets to the controller.
        #    This is a safety net only; it should rarely fire once the
        #    static flows below are in place, since they cover every
        #    traffic pattern this topology is expected to see.
        match = parser.OFPMatch()
        actions = [
            parser.OFPActionOutput(ofproto.OFPP_CONTROLLER, ofproto.OFPCML_NO_BUFFER)
        ]
        self.add_flow(datapath, PRIO_DEFAULT, match, actions)

        # 2) Topology-specific static forwarding (+ QoS on s1)
        if dpid == 1:
            self._install_s1_flows(datapath)
        elif dpid == 2:
            self._install_edge_flows(datapath, host_port=S2_HOST_PORT)
        elif dpid in CORE_DPIDS:
            self._install_core_flows(datapath)
        else:
            self.logger.warning(
                "DPID %d is not part of the known topology - no flows installed.",
                dpid,
            )

    # -------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------
    def _clear_flows(self, datapath):
        """Delete all existing flow entries on this switch before we
        install our own static set. Prevents stale flows from a previous
        ACTIVE_PATH_PORT lingering after a controller restart.
        """
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        mod = parser.OFPFlowMod(
            datapath=datapath,
            command=ofproto.OFPFC_DELETE,
            table_id=ofproto.OFPTT_ALL,
            out_port=ofproto.OFPP_ANY,
            out_group=ofproto.OFPG_ANY,
            match=parser.OFPMatch(),
        )
        datapath.send_msg(mod)

    def add_flow(self, datapath, priority, match, actions,
                 buffer_id=None, idle_timeout=0, hard_timeout=0):
        """Build and send a permanent (by default) FlowMod."""
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]

        if buffer_id is not None:
            mod = parser.OFPFlowMod(
                datapath=datapath, buffer_id=buffer_id, priority=priority,
                match=match, instructions=inst,
                idle_timeout=idle_timeout, hard_timeout=hard_timeout,
            )
        else:
            mod = parser.OFPFlowMod(
                datapath=datapath, priority=priority,
                match=match, instructions=inst,
                idle_timeout=idle_timeout, hard_timeout=hard_timeout,
            )
        datapath.send_msg(mod)

    # -------------------------------------------------------------------
    # DPID 1 (s1): host port 1 <-> ACTIVE_PATH_PORT, with QoS classification
    # applied on the host -> ACTIVE_PATH_PORT (forward) direction only.
    # -------------------------------------------------------------------
    def _install_s1_flows(self, datapath):
        parser = datapath.ofproto_parser

        # --- Return direction: ACTIVE_PATH_PORT -> host port 1 ---
        match = parser.OFPMatch(in_port=ACTIVE_PATH_PORT)
        actions = [parser.OFPActionOutput(S1_HOST_PORT)]
        self.add_flow(datapath, PRIO_L3_BASE, match, actions)

        # --- Forward direction: host port 1 -> ACTIVE_PATH_PORT ---

        # (a) Base / catch-all rule: ARP and any IPv4 traffic that is NOT
        #     one of the 3 recognized DSCP values. Plain output, no queue.
        match = parser.OFPMatch(in_port=S1_HOST_PORT)
        actions = [parser.OFPActionOutput(ACTIVE_PATH_PORT)]
        self.add_flow(datapath, PRIO_L3_BASE, match, actions)

        # (b) QoS rules: IPv4 + specific ip_dscp -> set_queue, then output.
        #     Higher priority than (a) so matching DSCP values win.
        for dscp, queue_id in DSCP_QUEUE_MAP.items():
            match = parser.OFPMatch(
                in_port=S1_HOST_PORT,
                eth_type=ether_types.ETH_TYPE_IP,
                ip_dscp=dscp,
            )
            actions = [
                parser.OFPActionSetQueue(queue_id),
                parser.OFPActionOutput(ACTIVE_PATH_PORT),
            ]
            self.add_flow(datapath, PRIO_QOS, match, actions)
            self.logger.info(
                "S1: installed QoS flow ip_dscp=%d -> queue %d -> port %d",
                dscp, queue_id, ACTIVE_PATH_PORT,
            )

    # -------------------------------------------------------------------
    # DPID 2 (s2): host port 1 <-> ACTIVE_PATH_PORT. No QoS classification
    # is required here per spec (classification happens once, at s1).
    # -------------------------------------------------------------------
    def _install_edge_flows(self, datapath, host_port):
        parser = datapath.ofproto_parser

        match = parser.OFPMatch(in_port=host_port)
        actions = [parser.OFPActionOutput(ACTIVE_PATH_PORT)]
        self.add_flow(datapath, PRIO_L3_BASE, match, actions)

        match = parser.OFPMatch(in_port=ACTIVE_PATH_PORT)
        actions = [parser.OFPActionOutput(host_port)]
        self.add_flow(datapath, PRIO_L3_BASE, match, actions)

    # -------------------------------------------------------------------
    # Core switches (s3-s6): transparent 2-port bridge, port 1 <-> port 2.
    # Identical on every core switch regardless of which path is active -
    # switches on inactive paths simply never receive traffic, since s1/s2
    # only ever send out ACTIVE_PATH_PORT.
    # -------------------------------------------------------------------
    def _install_core_flows(self, datapath):
        parser = datapath.ofproto_parser

        match = parser.OFPMatch(in_port=CORE_PORT_A)
        actions = [parser.OFPActionOutput(CORE_PORT_B)]
        self.add_flow(datapath, PRIO_BRIDGE, match, actions)

        match = parser.OFPMatch(in_port=CORE_PORT_B)
        actions = [parser.OFPActionOutput(CORE_PORT_A)]
        self.add_flow(datapath, PRIO_BRIDGE, match, actions)

    # -------------------------------------------------------------------
    # PacketIn: safety net ONLY. Should rarely fire once static flows are
    # installed. Mirrors the exact same static policy — never floods,
    # never learns MACs. Anything that doesn't match the expected
    # topology is explicitly dropped rather than flooded.
    # -------------------------------------------------------------------
    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def packet_in_handler(self, ev):
        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        dpid = datapath.id
        in_port = msg.match['in_port']

        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocol(ethernet.ethernet)
        if eth is None:
            return

        # Ignore LLDP / other discovery traffic - not part of this design.
        if eth.ethertype == ether_types.ETH_TYPE_LLDP:
            return

        out_port = self._resolve_out_port(dpid, in_port)

        if out_port is None:
            self.logger.warning(
                "DPID %d: PacketIn on unexpected in_port=%d - dropping "
                "(no static rule applies; never flooding).",
                dpid, in_port,
            )
            return  # explicit drop

        actions = [parser.OFPActionOutput(out_port)]

        data = None
        if msg.buffer_id == ofproto.OFP_NO_BUFFER:
            data = msg.data

        out = parser.OFPPacketOut(
            datapath=datapath, buffer_id=msg.buffer_id,
            in_port=in_port, actions=actions, data=data,
        )
        datapath.send_msg(out)

    def _resolve_out_port(self, dpid, in_port):
        """Mirrors the static topology policy used at handshake time; used
        only as a fallback for packets that reach the controller before
        (or despite) the proactive flows being installed."""
        if dpid == 1:
            if in_port == S1_HOST_PORT:
                return ACTIVE_PATH_PORT
            elif in_port == ACTIVE_PATH_PORT:
                return S1_HOST_PORT
        elif dpid == 2:
            if in_port == S2_HOST_PORT:
                return ACTIVE_PATH_PORT
            elif in_port == ACTIVE_PATH_PORT:
                return S2_HOST_PORT
        elif dpid in CORE_DPIDS:
            if in_port == CORE_PORT_A:
                return CORE_PORT_B
            elif in_port == CORE_PORT_B:
                return CORE_PORT_A
        return None