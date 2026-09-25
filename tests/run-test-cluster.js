import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { startCluster } from '../src/index.js';

const port = parseInt(process.argv[2] || '9880', 10);
const dataDir = process.argv[3] || path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../data-test-py');

const cluster = await startCluster({
  gatewayPort: port,
  dataDir,
  nodes: [
    { nodeId: 'pynode-1', port: port + 1, rack: 'rack-1' },
    { nodeId: 'pynode-2', port: port + 2, rack: 'rack-1' },
    { nodeId: 'pynode-3', port: port + 3, rack: 'rack-2' },
    { nodeId: 'pynode-4', port: port + 4, rack: 'rack-2' },
    { nodeId: 'pynode-5', port: port + 5, rack: 'rack-3' },
  ],
});

process.on('SIGTERM', async () => {
  await cluster.shutdown();
  process.exit(0);
});

process.on('SIGINT', async () => {
  await cluster.shutdown();
  process.exit(0);
});
