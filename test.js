const test = require('brittle')
const { once } = require('events')
const Hyperswarm = require('hyperswarm')
const promClient = require('prom-client')
const createTestnet = require('hyperdht/testnet')
const b4a = require('b4a')
const SwarmStats = require('.')
const HyperswarmStats = require('.')

const DEBUG = false

test('prometheus metrics', async (t) => {
  const tPrep = t.test('prep')
  tPrep.plan(6) // 3 times swarm1 (to all), others only 1 (to swarm1)

  const tDisconnectClient = t.test('disconnect from client')
  tDisconnectClient.plan(1)

  const tDisconnectServer = t.test('disconnect from server')
  tDisconnectServer.plan(1)

  const testnet = await createTestnet()
  const bootstrap = testnet.bootstrap

  let nrClosed = 0
  const swarm1 = new Hyperswarm({ bootstrap })
  swarm1.on('connection', (conn) => {
    tPrep.pass('swarm1 connected')
    conn.on('error', () => {})
    conn.on('close', () => {
      if (nrClosed++ === 0) {
        tDisconnectClient.pass('swarm1 disconnected from one client')
      }
      if (nrClosed++ === 1) {
        tDisconnectServer.pass('swarm1 disconnected from a server')
      }
    })
  })

  const swarm2 = new Hyperswarm({ bootstrap })
  swarm2.on('connection', (conn) => {
    tPrep.pass('swarm2 connected')
    conn.on('error', () => {})
  })

  const swarm3 = new Hyperswarm({ bootstrap })
  swarm3.on('connection', (conn) => {
    tPrep.pass('swarm3 connected')
    conn.on('error', () => {})
  })

  const swarm4 = new Hyperswarm({ bootstrap })
  swarm4.on('connection', (conn) => {
    tPrep.pass('swarm4 connected')
    conn.on('error', () => {})
  })

  t.teardown(async () => {
    await swarm4.destroy()
    await swarm3.destroy()
    await swarm2.destroy()
    await swarm1.destroy()
    await testnet.destroy()
  })

  const stats = new SwarmStats(swarm1)
  stats.registerPrometheusMetrics(promClient)

  const key1 = b4a.from('a'.repeat(64), 'hex')
  const key2 = b4a.from('b'.repeat(64), 'hex')
  const key3 = b4a.from('c'.repeat(64), 'hex')

  swarm3.join(key3, { server: true, client: false })
  await swarm3.flush()
  swarm4.join(key3, { server: true, client: false })
  await swarm4.flush()

  swarm1.join(key1, { server: true, client: false })
  swarm1.join(key2, { server: true, client: false })
  swarm1.join(key3, { server: false, client: true })
  await swarm1.flush()

  swarm2.join(key1, { server: false, client: true })
  swarm2.join(key2, { server: false, client: true })

  await tPrep

  // close 1 client
  await swarm3.destroy()
  await tDisconnectClient

  {
    const metrics = await promClient.register.metrics()
    const lines = metrics.split('\n')

    if (DEBUG) console.log(metrics)

    t.is(getMetricValue(lines, 'dht_total_queries') > 1, true, 'dht_total_queries')
    t.is(getMetricValue(lines, 'hyperswarm_nr_peers'), 3, 'hyperswarm_nr_peers')
    t.is(getMetricValue(lines, 'hyperswarm_client_connections_opened'), 2, 'hyperswarm_client_connections_opened')
    t.is(getMetricValue(lines, 'hyperswarm_client_connections_closed'), 1, 'hyperswarm_client_connections_opened')
    t.is(getMetricValue(lines, 'hyperswarm_client_connections_attempted'), 2, 'hyperswarm_client_connections_opened')
    t.is(getMetricValue(lines, 'hyperswarm_server_connections_opened'), 1, 'hyperswarm_server_connections_opened')
    t.is(getMetricValue(lines, 'hyperswarm_server_connections_closed'), 0, 'hyperswarm_server_connections_closed')
    t.is(hasMetric(lines, 'dht_consistent_punches'), true, 'dht_consistent_punches')
    t.is(hasMetric(lines, 'dht_random_punches'), true, 'dht_random_punches')
    t.is(hasMetric(lines, 'dht_open_punches'), true, 'dht_open_punches')
    t.is(hasMetric(lines, 'dht_active_queries'), true, 'dht_active_queries')
    t.is(hasMetric(lines, 'dht_total_queries'), true, 'dht_total_queries')
    t.is(getMetricValue(lines, 'udx_total_bytes_transmitted') > 1, true, 'hyperswarm_total_bytes_transmitted')
    t.is(getMetricValue(lines, 'udx_total_packets_transmitted') > 1, true, 'hyperswarm_total_packets_transmitted')
    t.is(getMetricValue(lines, 'udx_total_bytes_received') > 1, true, 'hyperswarm_total_bytes_received')
    t.is(getMetricValue(lines, 'udx_total_packets_received') > 1, true, 'hyperswarm_total_packets_received')
    t.is(getMetricValue(lines, 'dht_client_socket_bytes_transmitted') > 1, true, 'dht_client_socket_bytes_transmitted')
    t.is(getMetricValue(lines, 'dht_client_socket_packets_transmitted') > 1, true, 'dht_client_socket_packets_transmitted')
    t.is(getMetricValue(lines, 'dht_client_socket_bytes_received') > 1, true, 'dht_client_socket_bytes_received')
    t.is(getMetricValue(lines, 'dht_client_socket_packets_received') > 1, true, 'dht_client_socket_packets_received')
    t.is(getMetricValue(lines, 'dht_server_socket_bytes_transmitted') > 1, true, 'dht_server_socket_bytes_transmitted')
    t.is(getMetricValue(lines, 'dht_server_socket_packets_transmitted') > 1, true, 'dht_server_socket_packets_transmitted')
    t.is(getMetricValue(lines, 'dht_server_socket_bytes_received') > 1, true, 'dht_server_socket_bytes_received')
    t.is(getMetricValue(lines, 'dht_server_socket_packets_received') > 1, true, 'dht_server_socket_packets_received')
    t.is(getMetricValue(lines, 'hyperswarm_total_bytes_transmitted_over_swarm_streams') > 1, true, 'hyperswarm_total_bytes_transmitted_over_swarm_streams')
    t.is(getMetricValue(lines, 'hyperswarm_total_bytes_received_over_swarm_streams') > 1, true, 'hyperswarm_total_bytes_received_over_swarm_streams')
    t.is(getMetricValue(lines, 'hyperswarm_total_packets_transmitted_over_swarm_streams') > 1, true, 'hyperswarm_total_packets_transmitted_over_swarm_streams')
    t.is(getMetricValue(lines, 'hyperswarm_total_packets_received_over_swarm_streams') > 1, true, 'hyperswarm_total_packets_received_over_swarm_streams')
    t.is(getMetricValue(lines, 'hyperswarm_avg_congestion_window') > 1, true, 'hyperswarm_avg_congestion_window')
    t.is(getMetricValue(lines, 'hyperswarm_avg_mtu') > 1, true, 'hyperswarm_avg_mtu')
    t.is(getMetricValue(lines, 'hyperswarm_total_retransmits_over_swarm_streams'), 0, 'hyperswarm_total_retransmits_over_swarm_streams')
    t.is(getMetricValue(lines, 'hyperswarm_total_fast_recoveries_over_swarm_streams'), 0, 'hyperswarm_total_fast_recoveries_over_swarm_streams')
    t.is(getMetricValue(lines, 'hyperswarm_total_rto_count_over_swarm_streams'), 0, 'hyperswarm_total_rto_count_over_swarm_streams')
  }

  await swarm2.destroy()
  await tDisconnectServer
  await new Promise(resolve => setImmediate(resolve)) // apparently the nr of closed connections isn't updated immediately

  {
    const metrics = await promClient.register.metrics()
    const lines = metrics.split('\n')

    t.is(getMetricValue(lines, 'hyperswarm_server_connections_closed'), 1, 'hyperswarm_server_connections_closed')
  }
})

test('toString', async (t) => {
  const testnet = await createTestnet()
  const bootstrap = testnet.bootstrap

  const swarm = new Hyperswarm({ bootstrap })
  const stats = new HyperswarmStats(swarm)
  const str = stats.toString()
  t.ok(str.includes('Hyperswarm Stats'), 'toString includes swarm')
  t.ok(str.includes('DHT Stats'), 'toString includes dht stats')
  t.ok(str.includes('UDX Stats'), 'toString includes udx stats')

  await swarm.destroy()
  await testnet.destroy()
})

test('toJson', async (t) => {
  const testnet = await createTestnet()
  const bootstrap = testnet.bootstrap

  const swarm = new Hyperswarm({ bootstrap })
  const stats = new HyperswarmStats(swarm)
  const jsonStats = stats.toJson()

  // Note: we do not do a proper test on whether it includes
  // the same stats as the prometheus metrics and toString,
  // because it's not trivial due to the nested nature of the obj
  t.is(jsonStats.nrPeers, 0)

  await swarm.destroy()
  await testnet.destroy()
})

test('swarm stream counters include connections present before stats setup and have correct close accounting', async (t) => {
  const tSetup = t.test('setup')
  tSetup.plan(1)
  const testnet = await createTestnet()
  const bootstrap = testnet.bootstrap

  const swarm = new Hyperswarm({ bootstrap })
  const swarm2 = new Hyperswarm({ bootstrap })

  swarm2.listen()
  await new Promise(resolve => setTimeout(resolve, 200)) // announce

  let conn = null
  swarm.on('connection', c => {
    if (conn) return // re-connect
    c.on('error', () => {})
    conn = c
    conn.send(b4a.from('some data'))
    setTimeout(() => tSetup.pass('setup'), 200) // stats not incremented in same or next tick
  })
  swarm2.on('connection', c => {
    c.on('error', () => {})
  })
  swarm.joinPeer(swarm2.keyPair.publicKey)

  await tSetup

  const initBytesTransmitted = conn.rawStream.bytesTransmitted
  t.ok(initBytesTransmitted > 0, 'sanity check')

  const stats = new HyperswarmStats(swarm)
  t.is(stats.getBytesTransmittedAcrossAllStreams(), initBytesTransmitted, 'correct init of existing streams')
  const closedProm = once(conn, 'close')
  conn.destroy()
  const finalBytesTransmitted = conn.rawStream.bytesTransmitted

  await closedProm

  t.is(stats.swarm.connections.size, 0, 'closed (sanity check)')
  t.is(stats.getBytesTransmittedAcrossAllStreams(), finalBytesTransmitted, 'Correct accounting after existing stream closed')

  await swarm.destroy()
  await swarm2.destroy()
  await testnet.destroy()
})

function getMetricValue (lines, name) {
  const match = lines.find((l) => l.startsWith(`${name} `))
  if (!match) throw new Error(`No match for ${name}`)

  const value = parseInt(match.split(' ')[1])
  if (DEBUG) console.log(name, '->', value)

  return value
}

function hasMetric (lines, name) {
  const match = lines.find((l) => l.startsWith(`${name} `))
  return match !== undefined
}
