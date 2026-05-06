<script>
  // Two color-coded KV clusters span az1 + az2; a shared witness service
  // lives in az3. Plain SVG, no d3 — easy to tweak.
  const width = 760;
  const height = 360;

  // AZ columns
  const azW = 220;
  const azH = 280;
  const azY = 40;
  const azXs = [40, 270, 500]; // az1, az2, az3
  const azLabels = ['az1', 'az2', 'az3'];

  // Cluster colors (palette friendly to light + dark mode).
  const clusterA = { name: 'crl-prod-jx8', fill: '#bfe7c8', stroke: '#3a8a4d' };
  const clusterB = { name: 'crl-prod-38z', fill: '#bcd8f3', stroke: '#2c6db5' };
  const witness  = { fill: '#f0e2b6',                       stroke: '#9a7a16' };

  // Node box dims
  const nodeW = 170;
  const nodeH = 60;
  const nodeX = (azIdx) => azXs[azIdx] + (azW - nodeW) / 2;

  // Two nodes per AZ for az1+az2 (one per cluster).
  const aTop = azY + 50;
  const bTop = aTop + nodeH + 30;

  // Witness service box in az3.
  const witnessTop = azY + 90;
  const witnessH = 130;

  // Edge endpoints (cluster A and B az2 node -> witness service).
  const fromAX = nodeX(1) + nodeW;
  const fromAY = aTop + nodeH / 2;
  const fromBX = nodeX(1) + nodeW;
  const fromBY = bTop + nodeH / 2;
  const toX    = nodeX(2);
  const toAY   = witnessTop + witnessH * 0.35;
  const toBY   = witnessTop + witnessH * 0.65;

  // az1 outage cross overlay.
  const outageStroke = '#c1272d';
  const az1X = azXs[0];
  const az1L = az1X;
  const az1R = az1X + azW;
  const az1T = azY;
  const az1B = azY + azH;
</script>

<figure>
  <svg viewBox="0 0 {width} {height}" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Two clusters sharing a witness service across three AZs">
    <!-- AZ frames -->
    {#each azXs as x, i}
      <rect x={x} y={azY} width={azW} height={azH} rx="10"
            fill="none" stroke="currentColor" stroke-dasharray="4 4" opacity="0.5" />
      <text x={x + azW / 2} y={azY + 22} text-anchor="middle"
            font-size="14" font-weight="600" fill="currentColor">{azLabels[i]}</text>
    {/each}

    <!-- Cluster A nodes in az1 + az2 (az1 dimmed: outaged) -->
    {#each [0, 1] as i}
      <g opacity={i === 0 ? 0.35 : 1}>
        <rect x={nodeX(i)} y={aTop} width={nodeW} height={nodeH} rx="6"
              fill={clusterA.fill} stroke={clusterA.stroke} stroke-width="1.5" />
        <text x={nodeX(i) + nodeW / 2} y={aTop + 24} text-anchor="middle"
              font-size="12" font-weight="600" fill="#1a1a1a">{clusterA.name}</text>
        <text x={nodeX(i) + nodeW / 2} y={aTop + 42} text-anchor="middle"
              font-size="11" fill="#1a1a1a">node in {azLabels[i]}</text>
      </g>
    {/each}

    <!-- Cluster B nodes in az1 + az2 (az1 dimmed: outaged) -->
    {#each [0, 1] as i}
      <g opacity={i === 0 ? 0.35 : 1}>
        <rect x={nodeX(i)} y={bTop} width={nodeW} height={nodeH} rx="6"
              fill={clusterB.fill} stroke={clusterB.stroke} stroke-width="1.5" />
        <text x={nodeX(i) + nodeW / 2} y={bTop + 24} text-anchor="middle"
              font-size="12" font-weight="600" fill="#1a1a1a">{clusterB.name}</text>
        <text x={nodeX(i) + nodeW / 2} y={bTop + 42} text-anchor="middle"
              font-size="11" fill="#1a1a1a">node in {azLabels[i]}</text>
      </g>
    {/each}

    <!-- az1 outage cross overlay -->
    <line x1={az1L} y1={az1T} x2={az1R} y2={az1B}
          stroke={outageStroke} stroke-width="3" stroke-linecap="round" opacity="0.85" />
    <line x1={az1R} y1={az1T} x2={az1L} y2={az1B}
          stroke={outageStroke} stroke-width="3" stroke-linecap="round" opacity="0.85" />
    <text x={az1L + azW / 2} y={az1B - 12} text-anchor="middle"
          font-size="11" font-weight="700" fill={outageStroke}>az1 outage</text>

    <!-- Witness service in az3 -->
    <rect x={nodeX(2)} y={witnessTop} width={nodeW} height={witnessH} rx="6"
          fill={witness.fill} stroke={witness.stroke} stroke-width="1.5" />
    <text x={nodeX(2) + nodeW / 2} y={witnessTop + 26} text-anchor="middle"
          font-size="13" font-weight="700" fill="#1a1a1a">Witness Service</text>
    <text x={nodeX(2) + nodeW / 2} y={witnessTop + 50} text-anchor="middle"
          font-size="11" fill="#1a1a1a">shared across</text>
    <text x={nodeX(2) + nodeW / 2} y={witnessTop + 66} text-anchor="middle"
          font-size="11" fill="#1a1a1a">crl-prod-jx8</text>
    <text x={nodeX(2) + nodeW / 2} y={witnessTop + 82} text-anchor="middle"
          font-size="11" fill="#1a1a1a">crl-prod-38z</text>
    <text x={nodeX(2) + nodeW / 2} y={witnessTop + 100} text-anchor="middle"
          font-size="11" font-style="italic" fill="#1a1a1a">…and others</text>

    <!-- Edges: each cluster's az2 node points at witness service -->
    <path d={`M ${fromAX} ${fromAY} C ${(fromAX + toX) / 2} ${fromAY}, ${(fromAX + toX) / 2} ${toAY}, ${toX} ${toAY}`}
          fill="none" stroke={clusterA.stroke} stroke-width="1.5" />
    <path d={`M ${fromBX} ${fromBY} C ${(fromBX + toX) / 2} ${fromBY}, ${(fromBX + toX) / 2} ${toBY}, ${toX} ${toBY}`}
          fill="none" stroke={clusterB.stroke} stroke-width="1.5" />
  </svg>
  <figcaption>
    Two CockroachDB host clusters (<code>crl-prod-jx8</code>,
    <code>crl-prod-38z</code>) each run nodes in <code>az1</code> and
    <code>az2</code> and share a witness service in <code>az3</code>.
    With <code>az1</code> outaged, the surviving <code>az2</code> node of
    each cluster still forms quorum with the witness — both clusters stay
    available.
  </figcaption>
</figure>

<style>
  figure { margin: 1.5rem 0; }
  svg { max-width: 100%; height: auto; display: block; }
  figcaption {
    margin-top: 0.5rem;
    font-size: 0.85rem;
    color: var(--color-text-secondary, #555);
    text-align: center;
  }
</style>
