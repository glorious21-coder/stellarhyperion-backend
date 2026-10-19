/**
 * Prometheus metrics registry for operational monitoring.
 */

export interface GaugeMetric {
  readonly name: string;
  readonly help: string;
  readonly labels: Readonly<Record<string, string>>;
  readonly value: number;
}

class MetricsRegistry {
  private readonly blockLag = new Map<string, { chain: string; value: number }>();
  private readonly attestationDuration = new Map<string, { route: string; value: number }>();

  setWatcherBlockLag(chain: string, lag: number): void {
    this.blockLag.set(chain, {
      chain,
      value: Math.max(0, lag),
    });
  }

  setRailAttestationDuration(route: string, durationSeconds: number): void {
    this.attestationDuration.set(route, {
      route,
      value: Math.max(0, Number(durationSeconds.toFixed(3))),
    });
  }

  getWatcherBlockLag(chain: string): number | undefined {
    return this.blockLag.get(chain)?.value;
  }

  getRailAttestationDuration(route: string): number | undefined {
    return this.attestationDuration.get(route)?.value;
  }

  reset(): void {
    this.blockLag.clear();
    this.attestationDuration.clear();
  }

  renderPrometheus(): string {
    const lines: string[] = [];

    lines.push(
      "# HELP hyperion_watcher_block_lag Difference between the latest chain head and the indexer cursor",
    );
    lines.push("# TYPE hyperion_watcher_block_lag gauge");
    for (const entry of this.blockLag.values()) {
      lines.push(`hyperion_watcher_block_lag{chain="${entry.chain}"} ${String(entry.value)}`);
    }

    lines.push(
      "# HELP hyperion_rail_attestation_duration_seconds Time elapsed between outbound transfer observation and rail attestation",
    );
    lines.push("# TYPE hyperion_rail_attestation_duration_seconds gauge");
    for (const entry of this.attestationDuration.values()) {
      lines.push(
        `hyperion_rail_attestation_duration_seconds{route="${entry.route}"} ${String(entry.value)}`,
      );
    }

    return lines.join("\n") + "\n";
  }
}

export const metricsRegistry = new MetricsRegistry();
