/**
 * Vault Object Storage - Background Disk Scrubber
 * Periodically audits storage disks to detect silent hardware corruption and bitrot at rest.
 */

import { NodeClient } from '../cluster/node-client.js';
import { createLogger } from '../common/logger.js';

export class BackgroundScrubber {
  constructor(topology, healingDaemon, options = {}) {
    this.topology = topology;
    this.healingDaemon = healingDaemon;
    this.intervalMs = options.scrubIntervalMs || 15000;
    this.nodeClient = new NodeClient('scrubber');
    this.logger = createLogger('Scrubber');
    this.timer = null;
    this.isRunning = false;
    this.lastScrubReport = null;
  }

  start() {
    if (this.isRunning) return;
    this.isRunning = true;
    this.timer = setInterval(() => this.runScrub(), this.intervalMs);
  }

  stop() {
    this.isRunning = false;
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  async runScrub() {
    const nodes = this.topology.getAliveNodes();
    const reports = [];

    for (const node of nodes) {
      try {
        const report = await this.nodeClient.triggerScrub(node);
        reports.push(report);

        if (report.corrupted && report.corrupted.length > 0) {
          this.logger.error(`Scrubber detected bitrot on node ${node.nodeId}: ${report.corrupted.length} corrupted chunks!`);
          for (const c of report.corrupted) {
            // Queue critical healing for this corrupted chunk
            this.healingDaemon.reportCorruptedChunk(node.nodeId, c.chunkId);
          }
        } else {
          this.logger.debug(`Node ${node.nodeId} scrub clean: ${report.totalScanned} chunks verified`);
        }
      } catch (err) {
        this.logger.warn(`Failed to scrub node ${node.nodeId}: ${err.message}`);
      }
    }

    this.lastScrubReport = {
      timestamp: Date.now(),
      nodesScanned: nodes.length,
      reports,
    };

    return this.lastScrubReport;
  }
}
