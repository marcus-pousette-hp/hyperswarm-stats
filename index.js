const HyperDhtStats = require('hyperdht-stats')

class HyperswarmStats {
  constructor (swarm) {
    this.swarm = swarm
    this.dhtStats = new HyperDhtStats(this.swarm.dht)

    this._bytesTransmittedOverClosedSwarmStreams = 0
    this._packetsTransmittedOverClosedSwarmStreams = 0
    this._bytesReceivedOverClosedSwarmStreams = 0
    this._packetsReceivedOverClosedSwarmStreams = 0
    this._retransmitsOfClosedSwarmStreams = 0
    this._fastRecoveriesOfClosedSwarmStreams = 0
    this._rtoCountOfClosedSwarmStreams = 0
    this._activeConnections = new Set()

    for (const conn of swarm.connections) this._trackConnection(conn)

    swarm.on('connection', conn => this._trackConnection(conn))
  }

  get connects () {
    return this.swarm.stats.connects
  }

  get updates () {
    return this.swarm.stats.updates
  }

  getAvgCongestionWindow () {
    let totalCongestionWindow = 0
    let count = 0
    for (const conn of this.swarm.connections) {
      if (conn.rawStream) {
        count++
        totalCongestionWindow += conn.rawStream.cwnd
      }
    }

    return totalCongestionWindow / count
  }

  getAvgMTU () {
    let totalMTU = 0
    let count = 0
    for (const conn of this.swarm.connections) {
      if (conn.rawStream) {
        count++
        totalMTU += conn.rawStream.mtu
      }
    }

    return totalMTU / count
  }

  getRetransmitsAcrossAllStreams () {
    return this._getActiveStreamCounter('retransmits') + this._retransmitsOfClosedSwarmStreams
  }

  getFastRecoveriesAcrossAllStreams () {
    return this._getActiveStreamCounter('fastRecoveries') + this._fastRecoveriesOfClosedSwarmStreams
  }

  getRTOCountAcrossAllStreams () {
    return this._getActiveStreamCounter('rtoCount') + this._rtoCountOfClosedSwarmStreams
  }

  getBytesTransmittedAcrossAllStreams () {
    return this._getActiveStreamCounter('bytesTransmitted') + this._bytesTransmittedOverClosedSwarmStreams
  }

  getBytesReceivedAcrossAllStreams () {
    return this._getActiveStreamCounter('bytesReceived') + this._bytesReceivedOverClosedSwarmStreams
  }

  getPacketsTransmittedAcrossAllStreams () {
    return this._getActiveStreamCounter('packetsTransmitted') + this._packetsTransmittedOverClosedSwarmStreams
  }

  getPacketsReceivedAcrossAllStreams () {
    return this._getActiveStreamCounter('packetsReceived') + this._packetsReceivedOverClosedSwarmStreams
  }

  get nrPeers () {
    return this.swarm.peers.size
  }

  toJson () {
    return {
      dht: this.dhtStats.toJson(),
      nrPeers: this.nrPeers,
      connects: JSON.parse(JSON.stringify(this.connects)), // quick-and-dirty deep copy
      bytesTransmittedOverSwarmStreams: this.getBytesTransmittedAcrossAllStreams(),
      bytesReceivedOverSwarmStreams: this.getBytesReceivedAcrossAllStreams(),
      packetsTransmittedOverSwarmStreams: this.getPacketsTransmittedAcrossAllStreams(),
      packetsReceivedOverSwarmStreams: this.getPacketsReceivedAcrossAllStreams(),
      avgCongestionWindow: this.getAvgCongestionWindow(),
      avgMtu: this.getAvgMTU(),
      retransmitsOverSwarmStreams: this.getRetransmitsAcrossAllStreams(),
      fastRecoveriesOverSwarmStreams: this.getFastRecoveriesAcrossAllStreams(),
      rtoCountOverSwarmStreams: this.getRTOCountAcrossAllStreams()
    }
  }

  toString () {
    return `Hyperswarm Stats
  - hyperswarm_nr_peers: ${this.nrPeers}
  - hyperswarm_client_connections_opened: ${this.connects.client.opened}
  - hyperswarm_client_connections_closed: ${this.connects.client.closed}
  - hyperswarm_client_connections_attempted: ${this.connects.client.attempted}
  - hyperswarm_server_connections_opened: ${this.connects.server.opened}
  - hyperswarm_server_connections_closed: ${this.connects.server.closed}
  - hyperswarm_total_bytes_transmitted_over_swarm_streams: ${this.getBytesTransmittedAcrossAllStreams()}
  - hyperswarm_total_bytes_received_over_swarm_streams: ${this.getBytesReceivedAcrossAllStreams()}
  - hyperswarm_total_packets_transmitted_over_swarm_streams: ${this.getPacketsTransmittedAcrossAllStreams()}
  - hyperswarm_total_packets_received_over_swarm_streams: ${this.getPacketsReceivedAcrossAllStreams()}
  - hyperswarm_avg_congestion_window: ${this.getAvgCongestionWindow()}
  - hyperswarm_avg_mtu: ${this.getAvgMTU()}
  - hyperswarm_total_retransmits_over_swarm_streams: ${this.getRetransmitsAcrossAllStreams()}
  - hyperswarm_total_fast_recoveries_over_swarm_streams: ${this.getFastRecoveriesAcrossAllStreams()}
  - hyperswarm_total_rto_count_over_swarm_streams: ${this.getRTOCountAcrossAllStreams()}
${this.dhtStats.toString()}`
  }

  registerPrometheusMetrics (promClient) {
    this.dhtStats.registerPrometheusMetrics(promClient)

    const self = this
    new promClient.Gauge({ // eslint-disable-line no-new
      name: 'hyperswarm_nr_peers',
      help: 'Number of peers this swarm is connected to',
      collect () {
        this.set(self.nrPeers)
      }
    })

    new promClient.Gauge({ // eslint-disable-line no-new
      name: 'hyperswarm_client_connections_opened',
      help: 'Total number of client connections opened by the swarm',
      collect () {
        this.set(self.connects.client.opened)
      }
    })
    new promClient.Gauge({ // eslint-disable-line no-new
      name: 'hyperswarm_client_connections_closed',
      help: 'Total number of client connections closed by the swarm',
      collect () {
        this.set(self.connects.client.closed)
      }
    })
    new promClient.Gauge({ // eslint-disable-line no-new
      name: 'hyperswarm_client_connections_attempted',
      help: 'Total number of client connections attempted by the swarm',
      collect () {
        this.set(self.connects.client.attempted)
      }
    })

    new promClient.Gauge({ // eslint-disable-line no-new
      name: 'hyperswarm_server_connections_opened',
      help: 'Total number of server connections opened by the swarm',
      collect () {
        this.set(self.connects.server.opened)
      }
    })
    new promClient.Gauge({ // eslint-disable-line no-new
      name: 'hyperswarm_server_connections_closed',
      help: 'Total number of server connections closed by the swarm',
      collect () {
        this.set(self.connects.server.closed)
      }
    })

    new promClient.Gauge({ // eslint-disable-line no-new
      name: 'hyperswarm_total_bytes_transmitted_over_swarm_streams',
      help: 'Total bytes transmitted over the streams exposed explicitly by hyperswarm connections',
      collect () {
        this.set(self.getBytesTransmittedAcrossAllStreams())
      }
    })
    new promClient.Gauge({ // eslint-disable-line no-new
      name: 'hyperswarm_total_bytes_received_over_swarm_streams',
      help: 'Total bytes received over the streams exposed explicitly by hyperswarm connections',
      collect () {
        this.set(self.getBytesReceivedAcrossAllStreams())
      }
    })
    new promClient.Gauge({ // eslint-disable-line no-new
      name: 'hyperswarm_total_packets_transmitted_over_swarm_streams',
      help: 'Total packets transmitted over the streams exposed explicitly by hyperswarm connections',
      collect () {
        this.set(self.getPacketsTransmittedAcrossAllStreams())
      }
    })
    new promClient.Gauge({ // eslint-disable-line no-new
      name: 'hyperswarm_total_packets_received_over_swarm_streams',
      help: 'Total packets received over the streams exposed explicitly by hyperswarm connections',
      collect () {
        this.set(self.getPacketsReceivedAcrossAllStreams())
      }
    })

    new promClient.Gauge({ // eslint-disable-line no-new
      name: 'hyperswarm_avg_congestion_window',
      help: 'Average size of the congestion window (over all hyperswarm connections)',
      collect () {
        this.set(self.getAvgCongestionWindow())
      }
    })

    new promClient.Gauge({ // eslint-disable-line no-new
      name: 'hyperswarm_avg_mtu',
      help: 'Average size of the Maximum Transmission Unit (over all hyperswarm connections)',
      collect () {
        this.set(self.getAvgMTU())
      }
    })

    new promClient.Gauge({ // eslint-disable-line no-new
      name: 'hyperswarm_total_retransmits_over_swarm_streams',
      help: 'Total UDX retransmits (after a lost packet), summed across the streams exposed explicitly by hyperswarm connections',
      collect () {
        this.set(self.getRetransmitsAcrossAllStreams())
      }
    })
    new promClient.Gauge({ // eslint-disable-line no-new
      name: 'hyperswarm_total_fast_recoveries_over_swarm_streams',
      help: 'Total UDX fast recoveries summed across the streams exposed explicitly by hyperswarm connections',
      collect () {
        this.set(self.getFastRecoveriesAcrossAllStreams())
      }
    })
    new promClient.Gauge({ // eslint-disable-line no-new
      name: 'hyperswarm_total_rto_count_over_swarm_streams',
      help: 'Total UDX retransmission time-outs, summed across the streams exposed explicitly by hyperswarm connections',
      collect () {
        this.set(self.getRTOCountAcrossAllStreams())
      }
    })
  }

  _getActiveStreamCounter (name) {
    let total = 0
    for (const conn of this._activeConnections) {
      total += conn.rawStream?.[name] || 0
    }
    return total
  }

  _trackConnection (conn) {
    if (this._activeConnections.has(conn)) return

    this._activeConnections.add(conn)

    conn.on('close', () => this._closeConnection(conn))
  }

  _closeConnection (conn) {
    if (!this._activeConnections.has(conn)) return

    const rawStream = conn.rawStream

    this._bytesTransmittedOverClosedSwarmStreams += rawStream?.bytesTransmitted || 0
    this._packetsTransmittedOverClosedSwarmStreams += rawStream?.packetsTransmitted || 0
    this._bytesReceivedOverClosedSwarmStreams += rawStream?.bytesReceived || 0
    this._packetsReceivedOverClosedSwarmStreams += rawStream?.packetsReceived || 0
    this._retransmitsOfClosedSwarmStreams += rawStream?.retransmits || 0
    this._fastRecoveriesOfClosedSwarmStreams += rawStream?.fastRecoveries || 0
    this._rtoCountOfClosedSwarmStreams += rawStream?.rtoCount || 0

    this._activeConnections.delete(conn)
  }
}

module.exports = HyperswarmStats
