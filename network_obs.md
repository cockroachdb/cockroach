# CockroachDB Host-Level Network Metrics

This document provides an overview of the host-level networking metrics available in CockroachDB that are sourced directly from the Linux kernel. These metrics are essential for diagnosing network-related performance issues and packet loss.

## Summary of Metrics

| Metric Name | Linux Counter | Likely Cause for Incrementing |
| :--- | :--- | :--- |
| **`sys.host.net.send.tcp.retrans_segs`** | `Tcp:RetransSegs` | Packets are being lost or delayed in transit on the network. |
| **`sys.host.net.recv.err`** | `rx_errors` | Packets are being corrupted in transit by faulty hardware. |
| **`sys.host.net.recv.drop`** | `rx_dropped` | The receiving host is overloaded and cannot process incoming packets fast enough. |
| **`sys.host.net.send.err`** | `tx_errors` | The sending host's network hardware is having trouble transmitting data. |
| **`sys.host.net.send.drop`** | `tx_dropped` | The sending host is overloaded and dropping packets before they are sent. |
| `sys.host.net.recv.bytes` | `rx_bytes` | Measures volume of incoming traffic. |
| `sys.host.net.recv.packets` | `rx_packets` | Measures count of incoming packets. |
| `sys.host.net.send.bytes` | `tx_bytes` | Measures volume of outgoing traffic. |
| `sys.host.net.send.packets` | `tx_packets` | Measures count of outgoing packets. |

---

## Detailed Metric Analysis

### `sys.host.net.send.tcp.retrans_segs`
*   **Linux Counter:** `Tcp:RetransSegs`

This is the most reliable indicator that packets are being lost or significantly delayed on the network path, forcing the host to send them again.

#### Captures
*   **Network Congestion:** Packets dropped by overloaded switches or routers between hosts.
*   **Faulty Network Hardware:** Loss from bad cables, failing switch ports, or NICs anywhere on the path.
*   **Silent Drops:** Any scenario where a packet simply vanishes in transit without causing a visible error on either host.

#### Misses
*   The specific *location* or *reason* for the loss. It's a symptom, not a diagnosis.
*   Packet loss for non-TCP traffic (e.g., UDP).

---

### `sys.host.net.recv.err`
*   **Linux Counter:** `rx_errors`

This metric indicates that packets are arriving at the host's network card, but they are damaged or malformed.

#### Captures
*   **Data Corruption:** Strongly points to physical layer issues like a faulty network cable, a bad fiber transceiver, or a failing switch port that is corrupting data frames.

#### Misses
*   "Clean" packet loss where packets are dropped entirely by an intermediate device and never reach the host. If the packet doesn't arrive, it can't be counted as an error.

---

### `sys.host.net.recv.drop`
*   **Linux Counter:** `rx_dropped`

This metric indicates the receiving host's kernel is discarding fully-formed, valid packets after they have been received by the network card.

#### Captures
*   **Receiver Overload:** The host's kernel is dropping packets because its network buffers are full. This is a classic sign that the node cannot process data as fast as it's arriving, often a symptom of extreme network congestion causing traffic bursts.

#### Misses
*   Packet loss that occurs anywhere *before* the receiving host. If an upstream switch drops the packet, this counter won't change.

---

### `sys.host.net.send.err`
*   **Linux Counter:** `tx_errors`

This metric indicates the sending host's network card failed to transmit a packet successfully.

#### Captures
*   **Sender Hardware/Link Issues:** Points to problems with the local NIC, the cable plugged into it, or the immediate switch port it connects to (e.g., loss of carrier signal). It tells you the host is struggling to physically transmit the data.

#### Misses
*   Any packet loss that occurs after the data has successfully left the sending machine's NIC.

---

### `sys.host.net.send.drop`
*   **Linux Counter:** `tx_dropped`

This metric indicates that the sending host's kernel dropped a packet before it was even handed to the network card for transmission.

#### Captures
*   **Sender-Side Congestion/Bottleneck:** The host's kernel is dropping packets because its transmit queues are full. This typically happens when an application is generating traffic faster than the OS/NIC can handle.

#### Misses
*   Any packet loss that occurs after the packet has been successfully passed to the network hardware.

---

### `sys.host.net.recv.bytes`
*   **Linux Counter:** `rx_bytes`

This is a simple counter for the volume of data arriving at the host.

#### Captures
*   Measures the volume of incoming traffic. Useful as a baseline and for identifying network saturation.

#### Misses
*   Does not directly indicate packet loss, only the volume of data that successfully arrived.

---

### `sys.host.net.recv.packets`
*   **Linux Counter:** `rx_packets`

This is a simple counter for the number of packets arriving at the host.

#### Captures
*   Measures the number of incoming packets. When correlated with the sender's `tx_packets`, it can provide evidence of intermediate loss, but this is difficult in a many-to-many communication pattern.

#### Misses
*   Does not directly indicate packet loss on its own.

---

### `sys.host.net.send.bytes`
*   **Linux Counter:** `tx_bytes`

This is a simple counter for the volume of data being sent by the host.

#### Captures
*   Measures the volume of outgoing traffic. Useful for identifying which nodes are generating the most traffic.

#### Misses
*   Does not indicate if that traffic successfully reached its destination.

---

### `sys.host.net.send.packets`
*   **Linux Counter:** `tx_packets`

This is a simple counter for the number of packets being sent by the host.

#### Captures
*   Measures the number of outgoing packets. Can be correlated with receiver stats to infer loss.

#### Misses
*   Does not indicate successful delivery on its own. 