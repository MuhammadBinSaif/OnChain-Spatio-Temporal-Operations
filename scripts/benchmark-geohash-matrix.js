import { network } from "hardhat";
import { execFileSync } from "node:child_process";
import { existsSync } from "node:fs";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

const ROUND_COUNT = Number(process.env.BENCH_ROUNDS ?? 10);
const WRITE_GAS_LIMIT = BigInt(process.env.BENCH_WRITE_GAS_LIMIT ?? "12000000");

const PARCEL_OFFSETS = [0, 10, 20];
const LOT_OFFSETS = [21, 22, 23, 24];
const CHART_COLORS = ["#0f766e", "#f97316", "#7c3aed", "#2563eb", "#dc2626"];
const CHART_DIRNAME = "geohash-charts";

const OPERATION_LABELS = {
  registerParcel: "Register Parcel",
  registerLot: "Register Lot",
  registerBatch: "Register Batch",
  findBatchesInArea: "Q1 Batches In Area",
  findBatchesInTimePeriod: "Q2 Batches In Time Period",
  findBatchesInAreaDuringTime_SpatialFirst: "Q3 Area During Time (Spatial First)",
  findBatchesInAreaDuringTime_TemporalFirst: "Q4 Area During Time (Temporal First)",
  findBatchesInsideParcel: "Q5 Batches Inside Parcel",
  findBatchesInsideParcelDuringTime: "Q6 Batches Inside Parcel During Time",
  countBatchesInArea: "Q7 Count Batches In Area",
  findLotsInArea: "Q8 Lots In Area",
  getLotTrajectory: "Q9 Lot Trajectory",
  getLotLocationAtTime: "Q10 Lot Location At Time",
  findParcelsAtLocation: "Q11 Parcels At Location",
  findLotsInsideParcelDuringTime: "Q12 Lots Inside Parcel During Time"
};

const SECTION_LABELS = {
  setupGas: "Setup Gas",
  spatialGas: "Spatial Tree Gas",
  temporalGas: "Temporal Tree Gas",
  sharedGas: "Shared Linkage Gas"
};

const SWEEPS = [
  {
    key: "temporal_span",
    label: "Temporal Span Sweep",
    operations: [
      "registerBatch",
      "findBatchesInTimePeriod",
      "findBatchesInAreaDuringTime_SpatialFirst",
      "findBatchesInAreaDuringTime_TemporalFirst",
      "findBatchesInsideParcelDuringTime"
    ],
    measureSections: true,
    scenarios: [
      {
        key: "t1_same_day",
        label: "T1 Same Day",
        xLabel: "1 day",
        metadata: { activeDays: 1 },
        build: (round) => buildTemporalSpanScenario(round, 1)
      },
      {
        key: "t2_week",
        label: "T2 One Week",
        xLabel: "7 days",
        metadata: { activeDays: 7 },
        build: (round) => buildTemporalSpanScenario(round, 7)
      },
      {
        key: "t3_month",
        label: "T3 One Month",
        xLabel: "30 days",
        metadata: { activeDays: 30 },
        build: (round) => buildTemporalSpanScenario(round, 30)
      },
      {
        key: "t4_quarter",
        label: "T4 One Quarter",
        xLabel: "90 days",
        metadata: { activeDays: 90 },
        build: (round) => buildTemporalSpanScenario(round, 90)
      }
    ]
  },
  {
    key: "spatial_selectivity",
    label: "Spatial Selectivity Sweep",
    operations: [
      "findBatchesInArea",
      "findBatchesInAreaDuringTime_SpatialFirst",
      "findBatchesInAreaDuringTime_TemporalFirst",
      "countBatchesInArea",
      "findLotsInArea"
    ],
    measureSections: false,
    scenarios: [
      {
        key: "s1_selective",
        label: "S1 Selective",
        xLabel: "Selective",
        metadata: { targetAreas: 3, targetParcelLeaves: 4 },
        build: (round) => buildSpatialSelectivityScenario(round, {
          parcelSpecs: [
            { family: "a", leafCount: 4, startIndex: 1 },
            { family: "b", leafCount: 4, startIndex: 1 },
            { family: "c", leafCount: 4, startIndex: 1 }
          ]
        })
      },
      {
        key: "s2_moderate",
        label: "S2 Moderate",
        xLabel: "Moderate",
        metadata: { targetAreas: 2, targetParcelLeaves: 8 },
        build: (round) => buildSpatialSelectivityScenario(round, {
          parcelSpecs: [
            { family: "a", leafCount: 8, startIndex: 1 },
            { family: "a", leafCount: 8, startIndex: 21 },
            { family: "b", leafCount: 8, startIndex: 1 }
          ]
        })
      },
      {
        key: "s3_dense",
        label: "S3 Dense",
        xLabel: "Dense",
        metadata: { targetAreas: 1, targetParcelLeaves: 16 },
        build: (round) => buildSpatialSelectivityScenario(round, {
          parcelSpecs: [
            { family: "a", leafCount: 16, startIndex: 1 },
            { family: "a", leafCount: 16, startIndex: 33 },
            { family: "a", leafCount: 16, startIndex: 65 }
          ]
        })
      }
    ]
  },
  {
    key: "parcel_coverage",
    label: "Parcel Coverage Sweep",
    operations: [
      "registerParcel",
      "findBatchesInsideParcel",
      "findBatchesInsideParcelDuringTime",
      "findLotsInsideParcelDuringTime"
    ],
    measureSections: false,
    scenarios: [
      {
        key: "p1_4_leaves",
        label: "P1 4 Leaves",
        xLabel: "4 leaves",
        metadata: { targetLeaves: 4 },
        build: (round) => buildParcelCoverageScenario(round, 4)
      },
      {
        key: "p2_8_leaves",
        label: "P2 8 Leaves",
        xLabel: "8 leaves",
        metadata: { targetLeaves: 8 },
        build: (round) => buildParcelCoverageScenario(round, 8)
      },
      {
        key: "p3_16_leaves",
        label: "P3 16 Leaves",
        xLabel: "16 leaves",
        metadata: { targetLeaves: 16 },
        build: (round) => buildParcelCoverageScenario(round, 16)
      },
      {
        key: "p4_32_leaves",
        label: "P4 32 Leaves",
        xLabel: "32 leaves",
        metadata: { targetLeaves: 32 },
        build: (round) => buildParcelCoverageScenario(round, 32)
      }
    ]
  },
  {
    key: "lot_history",
    label: "Lot History Sweep",
    operations: [
      "getLotTrajectory",
      "getLotLocationAtTime"
    ],
    measureSections: false,
    scenarios: [
      {
        key: "l1_3_batches",
        label: "L1 3 Batches/Lot",
        xLabel: "3/lot",
        metadata: { batchesPerLot: 3 },
        build: (round) => buildLotHistoryScenario(round, 3)
      },
      {
        key: "l2_10_batches",
        label: "L2 10 Batches/Lot",
        xLabel: "10/lot",
        metadata: { batchesPerLot: 10 },
        build: (round) => buildLotHistoryScenario(round, 10)
      },
      {
        key: "l3_25_batches",
        label: "L3 25 Batches/Lot",
        xLabel: "25/lot",
        metadata: { batchesPerLot: 25 },
        build: (round) => buildLotHistoryScenario(round, 25)
      }
    ]
  }
];

let geohashArtifactPromise;

function mean(values) {
  if (values.length === 0) return 0;
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function stddev(values) {
  if (values.length <= 1) return 0;
  const avg = mean(values);
  const variance = values.reduce((sum, value) => {
    const diff = value - avg;
    return sum + diff * diff;
  }, 0) / values.length;
  return Math.sqrt(variance);
}

function roundValue(value, digits = 2) {
  return Number(value.toFixed(digits));
}

function asciiHex(value) {
  return `0x${Buffer.from(value, "utf8").toString("hex")}`;
}

function makeWkb(label, round, index) {
  return asciiHex(`${label}-${round}-${index}`.slice(0, 7));
}

function leaf(prefix4, suffixIndex) {
  return `${prefix4}${String(suffixIndex).padStart(3, "0")}`;
}

function prefixForFamily(family, round) {
  return `${family}${round % 10}x1`;
}

function makeLeafRange(prefix4, count, startIndex = 1) {
  return Array.from({ length: count }, (_, index) => leaf(prefix4, startIndex + index));
}

function spreadOffsets(count, activeDays, startOffset = 30) {
  if (count === 0) return [];
  if (count === 1) return [startOffset];
  if (activeDays <= 1) {
    return Array(count).fill(startOffset);
  }

  return Array.from({ length: count }, (_, index) => {
    const day = Math.floor((index * (activeDays - 1)) / (count - 1));
    return startOffset + day;
  });
}

function buildStandardLots(round) {
  return [
    { parcelId: 1, wkb: makeWkb("lot", round, 1) },
    { parcelId: 1, wkb: makeWkb("lot", round, 2) },
    { parcelId: 2, wkb: makeWkb("lot", round, 3) },
    { parcelId: 3, wkb: makeWkb("lot", round, 4) }
  ];
}

function buildParcels(round, parcelSpecs) {
  return parcelSpecs.map((spec, index) => {
    const prefix4 = spec.prefix4 ?? prefixForFamily(spec.family, round);
    return {
      parcelId: index + 1,
      prefix4,
      leaves: makeLeafRange(prefix4, spec.leafCount, spec.startIndex ?? 1),
      wkb: makeWkb("parcel", round, index + 1)
    };
  });
}

function buildRoundRobinBatches(parcels, batchOffsets, round, labelPrefix = "batch") {
  const lots = buildStandardLots(round);
  const lotLeafChoices = [
    parcels[0].leaves,
    [...parcels[0].leaves].reverse(),
    parcels[1].leaves,
    parcels[2].leaves
  ];
  const counters = Array(lotLeafChoices.length).fill(0);

  const batches = batchOffsets.map((offset, index) => {
    const lotIndex = index % lotLeafChoices.length;
    const leafChoices = lotLeafChoices[lotIndex];
    const leafIndex = counters[lotIndex] % leafChoices.length;
    counters[lotIndex] += 1;

    return {
      lotId: lotIndex + 1,
      geohash: leafChoices[leafIndex],
      offset,
      wkb: makeWkb(labelPrefix, round, index + 1)
    };
  });

  return { lots, batches };
}

function buildHistoryBatches(parcels, batchesPerLot, round) {
  const lots = buildStandardLots(round);
  const lotLeafChoices = [
    parcels[0].leaves,
    [...parcels[0].leaves].reverse(),
    parcels[1].leaves,
    parcels[2].leaves
  ];
  const totalBatches = batchesPerLot * lots.length;
  const batchOffsets = spreadOffsets(totalBatches, Math.max(totalBatches, 30), 30);
  const counters = Array(lotLeafChoices.length).fill(0);
  const batches = [];
  const lotOffsets = Array.from({ length: lots.length }, () => []);
  let globalIndex = 0;

  for (let batchStep = 0; batchStep < batchesPerLot; batchStep += 1) {
    for (let lotIndex = 0; lotIndex < lots.length; lotIndex += 1) {
      const leafChoices = lotLeafChoices[lotIndex];
      const leafIndex = counters[lotIndex] % leafChoices.length;
      counters[lotIndex] += 1;
      const offset = batchOffsets[globalIndex];
      lotOffsets[lotIndex].push(offset);
      batches.push({
        lotId: lotIndex + 1,
        geohash: leafChoices[leafIndex],
        offset,
        wkb: makeWkb("history", round, globalIndex + 1)
      });
      globalIndex += 1;
    }
  }

  const lot1History = lotOffsets[0];
  const targetLocationTimeOffset = lot1History[Math.min(
    lot1History.length - 1,
    Math.floor((lot1History.length - 1) * 0.75)
  )];

  return {
    lots,
    batches,
    targetLocationTimeOffset
  };
}

function buildScenarioMetadata(parcels, batches, targetLotId = 1) {
  const batchOffsets = batches.map((batch) => batch.offset);
  const startOffset = Math.min(...batchOffsets);
  const endOffset = Math.max(...batchOffsets);
  return {
    targetPrefix4: asciiHex(parcels[0].prefix4),
    targetParcelId: 1,
    targetLeaf: asciiHex(parcels[0].leaves[Math.min(1, parcels[0].leaves.length - 1)]),
    targetLotId,
    queryWindow: { start: startOffset, end: endOffset }
  };
}

function buildTemporalSpanScenario(round, activeDays) {
  const parcels = buildParcels(round, [
    { family: "a", leafCount: 4, startIndex: 1 },
    { family: "b", leafCount: 4, startIndex: 1 },
    { family: "c", leafCount: 4, startIndex: 1 }
  ]);
  const batchOffsets = spreadOffsets(12, activeDays, 30);
  const { lots, batches } = buildRoundRobinBatches(parcels, batchOffsets, round, "temp");
  const metadata = buildScenarioMetadata(parcels, batches);

  return {
    parcels,
    lots,
    batches,
    ...metadata,
    targetLocationTimeOffset: batchOffsets[Math.max(0, batchOffsets.length - 3)]
  };
}

function buildSpatialSelectivityScenario(round, config) {
  const parcels = buildParcels(round, config.parcelSpecs);
  const batchOffsets = spreadOffsets(12, 30, 30);
  const { lots, batches } = buildRoundRobinBatches(parcels, batchOffsets, round, "spatial");
  const metadata = buildScenarioMetadata(parcels, batches);

  return {
    parcels,
    lots,
    batches,
    ...metadata,
    targetLocationTimeOffset: batchOffsets[Math.max(0, batchOffsets.length - 3)]
  };
}

function buildParcelCoverageScenario(round, targetLeafCount) {
  const parcels = buildParcels(round, [
    { family: "a", leafCount: targetLeafCount, startIndex: 1 },
    { family: "b", leafCount: 4, startIndex: 1 },
    { family: "c", leafCount: 4, startIndex: 1 }
  ]);
  const batchOffsets = spreadOffsets(12, 30, 30);
  const { lots, batches } = buildRoundRobinBatches(parcels, batchOffsets, round, "parcel");
  const metadata = buildScenarioMetadata(parcels, batches);

  return {
    parcels,
    lots,
    batches,
    ...metadata,
    targetLocationTimeOffset: batchOffsets[Math.max(0, batchOffsets.length - 3)]
  };
}

function buildLotHistoryScenario(round, batchesPerLot) {
  const parcels = buildParcels(round, [
    { family: "a", leafCount: 4, startIndex: 1 },
    { family: "b", leafCount: 4, startIndex: 1 },
    { family: "c", leafCount: 4, startIndex: 1 }
  ]);
  const history = buildHistoryBatches(parcels, batchesPerLot, round);
  const metadata = buildScenarioMetadata(parcels, history.batches);

  return {
    parcels,
    lots: history.lots,
    batches: history.batches,
    ...metadata,
    targetLocationTimeOffset: history.targetLocationTimeOffset
  };
}

function findSolcBinary() {
  const home = os.homedir();
  const version = "solc-macosx-amd64-v0.8.24+commit.e11b9ed9";
  const candidates = [
    path.join(home, "Library/Caches/hardhat-nodejs/compilers-v3/macosx-amd64", version),
    path.join(home, "Library/Caches/hardhat-nodejs/compilers-v2/macosx-amd64", version)
  ];

  for (const candidate of candidates) {
    if (existsSync(candidate)) {
      return candidate;
    }
  }

  throw new Error("Unable to locate cached solc 0.8.24 binary for Geohash compilation.");
}

async function compileGeohashArtifact() {
  const sourcePath = path.join(process.cwd(), "contracts/Geohash.sol");
  const source = await readFile(sourcePath, "utf8");
  const input = {
    language: "Solidity",
    sources: {
      "Geohash.sol": { content: source }
    },
    settings: {
      optimizer: {
        enabled: true,
        runs: 200
      },
      viaIR: true,
      outputSelection: {
        "*": {
          "*": ["abi", "evm.bytecode.object"]
        }
      }
    }
  };

  const solcOutput = execFileSync(findSolcBinary(), ["--standard-json"], {
    input: JSON.stringify(input),
    encoding: "utf8",
    maxBuffer: 32 * 1024 * 1024
  });
  const parsed = JSON.parse(solcOutput);
  const errors = (parsed.errors ?? []).filter((entry) => entry.severity === "error");

  if (errors.length > 0) {
    throw new Error(
      `Geohash compilation failed:\n${errors.map((entry) => entry.formattedMessage).join("\n")}`
    );
  }

  const contractOutput = parsed.contracts["Geohash.sol"].Geohash;
  return {
    abi: contractOutput.abi,
    bytecode: `0x${contractOutput.evm.bytecode.object}`
  };
}

async function loadGeohashArtifact() {
  if (!geohashArtifactPromise) {
    geohashArtifactPromise = compileGeohashArtifact();
  }
  return geohashArtifactPromise;
}

async function deployGeohash(ethers) {
  const [deployer] = await ethers.getSigners();
  const artifact = await loadGeohashArtifact();
  const factory = new ethers.ContractFactory(artifact.abi, artifact.bytecode, deployer);
  const contract = await factory.deploy();
  await contract.waitForDeployment();
  return contract;
}

async function setNextTimestamp(provider, timestamp) {
  await provider.send("evm_setNextBlockTimestamp", [timestamp]);
}

async function measureTransactionGas(contract, method, args, timestamp) {
  try {
    await setNextTimestamp(contract.runner.provider, timestamp);
    const tx = await contract[method](...args, { gasLimit: WRITE_GAS_LIMIT });
    const receipt = await tx.wait();
    return { gas: Number(receipt.gasUsed), status: "ok" };
  } catch (error) {
    return {
      gas: null,
      status: error?.shortMessage || error?.message || "transaction_failed"
    };
  }
}

async function measureBatchWithBreakdown(contract, args, timestamp) {
  try {
    await setNextTimestamp(contract.runner.provider, timestamp);
    const tx = await contract.registerBatchMeasured(...args, { gasLimit: WRITE_GAS_LIMIT });
    const receipt = await tx.wait();

    let breakdown;
    for (const log of receipt.logs) {
      try {
        const parsed = contract.interface.parseLog(log);
        if (parsed?.name === "RegisterBatchGasBreakdown") {
          breakdown = parsed.args;
          break;
        }
      } catch {
        // ignore unrelated logs
      }
    }

    if (!breakdown) {
      throw new Error("missing RegisterBatchGasBreakdown event");
    }

    return {
      gas: Number(receipt.gasUsed),
      setupGas: Number(breakdown.setupGas),
      spatialGas: Number(breakdown.spatialGas),
      temporalGas: Number(breakdown.temporalGas),
      sharedGas: Number(breakdown.sharedGas),
      status: "ok"
    };
  } catch (error) {
    return {
      gas: null,
      setupGas: null,
      spatialGas: null,
      temporalGas: null,
      sharedGas: null,
      status: error?.shortMessage || error?.message || "transaction_failed"
    };
  }
}

async function measureReadGas(contract, method, args = []) {
  try {
    return {
      gas: Number(await contract[method].estimateGas(...args)),
      status: "ok"
    };
  } catch (error) {
    return {
      gas: null,
      status: error?.shortMessage || error?.message || "read_failed"
    };
  }
}

function summarizeWriteSamples(samples) {
  const successful = samples.filter((sample) => sample.gas !== null);
  const failures = samples.length - successful.length;

  if (successful.length === 0) {
    return {
      gas: null,
      status: "failed",
      successCount: 0,
      failureCount: failures
    };
  }

  return {
    gas: mean(successful.map((sample) => sample.gas)),
    status: failures === 0 ? "ok" : "partial",
    successCount: successful.length,
    failureCount: failures
  };
}

function summarizeFieldSamples(samples, field) {
  const successful = samples.filter((sample) => sample[field] !== null && sample[field] !== undefined);
  const failures = samples.length - successful.length;

  if (successful.length === 0) {
    return {
      gas: null,
      status: "failed",
      successCount: 0,
      failureCount: failures
    };
  }

  return {
    gas: mean(successful.map((sample) => sample[field])),
    status: failures === 0 ? "ok" : "partial",
    successCount: successful.length,
    failureCount: failures
  };
}

function aggregateOperationSamples(samples) {
  const successful = samples.filter((sample) => sample.gas !== null);
  const failures = samples.length - successful.length;

  if (successful.length === 0) {
    return {
      avgGas: null,
      stdGas: null,
      minGas: null,
      maxGas: null,
      rounds: samples.length,
      successRounds: 0,
      failureRounds: failures,
      status: "failed"
    };
  }

  const values = successful.map((sample) => sample.gas);
  return {
    avgGas: roundValue(mean(values)),
    stdGas: roundValue(stddev(values)),
    minGas: roundValue(Math.min(...values)),
    maxGas: roundValue(Math.max(...values)),
    rounds: samples.length,
    successRounds: successful.length,
    failureRounds: failures,
    status: failures === 0 ? "ok" : "partial"
  };
}

function aggregateMetricMap(rawResults, keys) {
  const buckets = {};

  for (const roundResult of rawResults) {
    for (const key of keys) {
      if (!buckets[key]) {
        buckets[key] = [];
      }
      buckets[key].push(roundResult[key]);
    }
  }

  return Object.fromEntries(
    Object.entries(buckets).map(([key, samples]) => [key, aggregateOperationSamples(samples)])
  );
}

function csvCell(value) {
  const text = value === null || value === undefined ? "" : String(value);
  if (text.includes(",") || text.includes('"') || text.includes("\n")) {
    return `"${text.replace(/"/g, '""')}"`;
  }
  return text;
}

async function populateScenario(contract, scenario, useMeasuredBatches = false) {
  const provider = contract.runner.provider;
  const latestBlock = await provider.getBlock("latest");
  const baseDay = Math.floor(Number(latestBlock.timestamp) / 86400) + 1;

  const writeSamples = {
    registerParcel: [],
    registerLot: [],
    registerBatch: []
  };
  const measuredBatchSamples = [];

  for (let index = 0; index < scenario.parcels.length; index += 1) {
    const timestamp = (baseDay + PARCEL_OFFSETS[index]) * 86400 + index;
    const parcel = scenario.parcels[index];
    const sample = await measureTransactionGas(contract, "registerParcel", [
      parcel.wkb,
      parcel.leaves.map(asciiHex)
    ], timestamp);
    writeSamples.registerParcel.push(sample);
  }

  for (let index = 0; index < scenario.lots.length; index += 1) {
    const timestamp = (baseDay + LOT_OFFSETS[index]) * 86400 + index;
    const lot = scenario.lots[index];
    const sample = await measureTransactionGas(contract, "registerLot", [
      lot.parcelId,
      lot.wkb
    ], timestamp);
    writeSamples.registerLot.push(sample);
  }

  for (let index = 0; index < scenario.batches.length; index += 1) {
    const batch = scenario.batches[index];
    const timestamp = (baseDay + batch.offset) * 86400 + index;
    if (useMeasuredBatches) {
      measuredBatchSamples.push(await measureBatchWithBreakdown(contract, [
        batch.lotId,
        batch.wkb,
        batch.geohash,
        timestamp
      ], timestamp));
    } else {
      writeSamples.registerBatch.push(await measureTransactionGas(contract, "registerBatch", [
        batch.lotId,
        batch.wkb,
        batch.geohash,
        timestamp
      ], timestamp));
    }
  }

  return {
    baseDay,
    startBucket: baseDay + scenario.queryWindow.start,
    endBucket: baseDay + scenario.queryWindow.end,
    targetTime: (baseDay + scenario.targetLocationTimeOffset) * 86400,
    writeSamples,
    measuredBatchSamples
  };
}

function getReadSpecs(scenario, populated) {
  return {
    findBatchesInArea: [
      "findBatchesInArea",
      [scenario.targetPrefix4]
    ],
    findBatchesInTimePeriod: [
      "findBatchesInTimePeriod",
      [populated.startBucket, populated.endBucket]
    ],
    findBatchesInAreaDuringTime_SpatialFirst: [
      "findBatchesInAreaDuringTime_SpatialFirst",
      [scenario.targetPrefix4, populated.startBucket, populated.endBucket]
    ],
    findBatchesInAreaDuringTime_TemporalFirst: [
      "findBatchesInAreaDuringTime_TemporalFirst",
      [scenario.targetPrefix4, populated.startBucket, populated.endBucket]
    ],
    findBatchesInsideParcel: [
      "findBatchesInsideParcel",
      [scenario.targetParcelId]
    ],
    findBatchesInsideParcelDuringTime: [
      "findBatchesInsideParcelDuringTime",
      [scenario.targetParcelId, populated.startBucket, populated.endBucket]
    ],
    countBatchesInArea: [
      "countBatchesInArea",
      [scenario.targetPrefix4]
    ],
    findLotsInArea: [
      "findLotsInArea",
      [scenario.targetPrefix4]
    ],
    getLotTrajectory: [
      "getLotTrajectory",
      [scenario.targetLotId]
    ],
    getLotLocationAtTime: [
      "getLotLocationAtTime",
      [scenario.targetLotId, populated.targetTime]
    ],
    findParcelsAtLocation: [
      "findParcelsAtLocation",
      [scenario.targetLeaf]
    ],
    findLotsInsideParcelDuringTime: [
      "findLotsInsideParcelDuringTime",
      [scenario.targetParcelId, populated.startBucket, populated.endBucket]
    ]
  };
}

async function benchmarkScenarioRound(ethers, scenarioBuilder, operations, round) {
  const scenario = scenarioBuilder(round);
  const contract = await deployGeohash(ethers);
  const populated = await populateScenario(contract, scenario, false);
  const specs = getReadSpecs(scenario, populated);
  const metrics = {
    registerParcel: summarizeWriteSamples(populated.writeSamples.registerParcel),
    registerLot: summarizeWriteSamples(populated.writeSamples.registerLot),
    registerBatch: summarizeWriteSamples(populated.writeSamples.registerBatch)
  };

  for (const operationKey of operations) {
    if (operationKey.startsWith("register")) {
      continue;
    }
    const [method, args] = specs[operationKey];
    metrics[operationKey] = await measureReadGas(contract, method, args);
  }

  return metrics;
}

async function benchmarkSectionRound(ethers, scenarioBuilder, round) {
  const scenario = scenarioBuilder(round);
  const contract = await deployGeohash(ethers);
  const populated = await populateScenario(contract, scenario, true);

  return {
    setupGas: summarizeFieldSamples(populated.measuredBatchSamples, "setupGas"),
    spatialGas: summarizeFieldSamples(populated.measuredBatchSamples, "spatialGas"),
    temporalGas: summarizeFieldSamples(populated.measuredBatchSamples, "temporalGas"),
    sharedGas: summarizeFieldSamples(populated.measuredBatchSamples, "sharedGas")
  };
}

function buildCombinedCsv(rows) {
  const header = [
    "sweep",
    "sweep_label",
    "scenario",
    "scenario_label",
    "operation",
    "operation_label",
    "avg_gas",
    "std_gas",
    "min_gas",
    "max_gas",
    "rounds",
    "success_rounds",
    "failure_rounds",
    "status"
  ];
  const lines = [header.join(",")];

  for (const row of rows) {
    lines.push([
      row.sweep,
      row.sweepLabel,
      row.scenario,
      row.scenarioLabel,
      row.operation,
      row.operationLabel,
      row.avgGas,
      row.stdGas,
      row.minGas,
      row.maxGas,
      row.rounds,
      row.successRounds,
      row.failureRounds,
      row.status
    ].map(csvCell).join(","));
  }

  return lines.join("\n");
}

function buildSectionCsv(rows) {
  const header = [
    "sweep",
    "scenario",
    "scenario_label",
    "section",
    "section_label",
    "avg_gas",
    "std_gas",
    "min_gas",
    "max_gas",
    "rounds",
    "success_rounds",
    "failure_rounds",
    "status"
  ];
  const lines = [header.join(",")];

  for (const row of rows) {
    lines.push([
      row.sweep,
      row.scenario,
      row.scenarioLabel,
      row.section,
      row.sectionLabel,
      row.avgGas,
      row.stdGas,
      row.minGas,
      row.maxGas,
      row.rounds,
      row.successRounds,
      row.failureRounds,
      row.status
    ].map(csvCell).join(","));
  }

  return lines.join("\n");
}

function chartSeriesForSweep(sweepKey, scenarioMap) {
  const ordered = Object.values(scenarioMap);

  if (sweepKey === "temporal_span") {
    return [
      {
        file: "geohash-temporal-span-queries.svg",
        title: "Temporal Span Sweep: Spatio-Temporal Query Gas",
        series: [
          {
            label: "Time Period",
            operation: "findBatchesInTimePeriod"
          },
          {
            label: "Spatial First",
            operation: "findBatchesInAreaDuringTime_SpatialFirst"
          },
          {
            label: "Temporal First",
            operation: "findBatchesInAreaDuringTime_TemporalFirst"
          },
          {
            label: "Parcel + Time",
            operation: "findBatchesInsideParcelDuringTime"
          }
        ],
        categories: ordered.map((scenario) => scenario.xLabel)
      },
      {
        file: "geohash-temporal-span-writes.svg",
        title: "Temporal Span Sweep: Batch Write Decomposition",
        sections: [
          { label: "Register Batch", operation: "registerBatch" },
          { label: "Spatial Tree", section: "spatialGas" },
          { label: "Temporal Tree", section: "temporalGas" },
          { label: "Shared", section: "sharedGas" }
        ],
        categories: ordered.map((scenario) => scenario.xLabel)
      }
    ];
  }

  if (sweepKey === "spatial_selectivity") {
    return [
      {
        file: "geohash-spatial-selectivity.svg",
        title: "Spatial Selectivity Sweep: Area Query Comparison",
        series: [
          { label: "Area", operation: "findBatchesInArea" },
          { label: "Spatial First", operation: "findBatchesInAreaDuringTime_SpatialFirst" },
          { label: "Temporal First", operation: "findBatchesInAreaDuringTime_TemporalFirst" },
          { label: "Count Area", operation: "countBatchesInArea" },
          { label: "Lots In Area", operation: "findLotsInArea" }
        ],
        categories: ordered.map((scenario) => scenario.xLabel)
      }
    ];
  }

  if (sweepKey === "parcel_coverage") {
    return [
      {
        file: "geohash-parcel-coverage.svg",
        title: "Parcel Coverage Sweep: Parcel Join Gas",
        series: [
          { label: "Register Parcel", operation: "registerParcel" },
          { label: "Batches In Parcel", operation: "findBatchesInsideParcel" },
          { label: "Parcel + Time", operation: "findBatchesInsideParcelDuringTime" },
          { label: "Lots In Parcel + Time", operation: "findLotsInsideParcelDuringTime" }
        ],
        categories: ordered.map((scenario) => scenario.xLabel)
      }
    ];
  }

  if (sweepKey === "lot_history") {
    return [
      {
        file: "geohash-lot-history.svg",
        title: "Lot History Sweep: Trajectory Query Gas",
        series: [
          { label: "Trajectory", operation: "getLotTrajectory" },
          { label: "Location At Time", operation: "getLotLocationAtTime" }
        ],
        categories: ordered.map((scenario) => scenario.xLabel)
      }
    ];
  }

  return [];
}

function escapeXml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&apos;");
}

function niceTick(value) {
  if (value <= 0) return 1;
  const exponent = Math.floor(Math.log10(value));
  const fraction = value / 10 ** exponent;
  let niceFraction;
  if (fraction <= 1) niceFraction = 1;
  else if (fraction <= 2) niceFraction = 2;
  else if (fraction <= 5) niceFraction = 5;
  else niceFraction = 10;
  return niceFraction * 10 ** exponent;
}

function buildGroupedBarSvg({ title, categories, series }) {
  const width = 1200;
  const height = 680;
  const margin = { top: 90, right: 30, bottom: 140, left: 95 };
  const plotWidth = width - margin.left - margin.right;
  const plotHeight = height - margin.top - margin.bottom;
  const maxValue = Math.max(
    1,
    ...series.flatMap((entry) => entry.values).filter((value) => Number.isFinite(value))
  );
  const tickStep = niceTick(maxValue / 5);
  const chartMax = Math.ceil(maxValue / tickStep) * tickStep;
  const groupWidth = plotWidth / categories.length;
  const innerWidth = groupWidth * 0.74;
  const barWidth = innerWidth / Math.max(series.length, 1);
  const elements = [];

  elements.push(`<rect x="0" y="0" width="${width}" height="${height}" fill="#ffffff"/>`);
  elements.push(`<text x="${width / 2}" y="42" text-anchor="middle" font-size="24" font-family="Arial" font-weight="700" fill="#111827">${escapeXml(title)}</text>`);
  elements.push(`<text x="${margin.left - 58}" y="${margin.top - 18}" text-anchor="start" font-size="14" font-family="Arial" fill="#374151">Gas</text>`);

  for (let tick = 0; tick <= chartMax; tick += tickStep) {
    const y = margin.top + plotHeight - (tick / chartMax) * plotHeight;
    elements.push(`<line x1="${margin.left}" y1="${y}" x2="${width - margin.right}" y2="${y}" stroke="#e5e7eb" stroke-width="1"/>`);
    elements.push(`<text x="${margin.left - 12}" y="${y + 5}" text-anchor="end" font-size="12" font-family="Arial" fill="#6b7280">${escapeXml(tick.toLocaleString())}</text>`);
  }

  elements.push(`<line x1="${margin.left}" y1="${margin.top}" x2="${margin.left}" y2="${margin.top + plotHeight}" stroke="#111827" stroke-width="1.5"/>`);
  elements.push(`<line x1="${margin.left}" y1="${margin.top + plotHeight}" x2="${width - margin.right}" y2="${margin.top + plotHeight}" stroke="#111827" stroke-width="1.5"/>`);

  series.forEach((entry, seriesIndex) => {
    const color = CHART_COLORS[seriesIndex % CHART_COLORS.length];
    const legendX = margin.left + seriesIndex * 175;
    elements.push(`<rect x="${legendX}" y="58" width="16" height="16" fill="${color}" rx="2"/>`);
    elements.push(`<text x="${legendX + 24}" y="71" font-size="13" font-family="Arial" fill="#374151">${escapeXml(entry.label)}</text>`);
  });

  categories.forEach((category, categoryIndex) => {
    const groupStart = margin.left + categoryIndex * groupWidth + (groupWidth - innerWidth) / 2;
    const labelX = margin.left + categoryIndex * groupWidth + groupWidth / 2;

    series.forEach((entry, seriesIndex) => {
      const color = CHART_COLORS[seriesIndex % CHART_COLORS.length];
      const value = entry.values[categoryIndex] ?? 0;
      const barHeight = (value / chartMax) * plotHeight;
      const x = groupStart + seriesIndex * barWidth;
      const y = margin.top + plotHeight - barHeight;
      elements.push(`<rect x="${x}" y="${y}" width="${Math.max(barWidth - 4, 4)}" height="${barHeight}" fill="${color}" rx="3"/>`);
      elements.push(`<text x="${x + Math.max(barWidth - 4, 4) / 2}" y="${y - 6}" text-anchor="middle" font-size="11" font-family="Arial" fill="#374151">${escapeXml(Math.round(value).toLocaleString())}</text>`);
    });

    elements.push(`<text x="${labelX}" y="${height - 44}" text-anchor="middle" font-size="13" font-family="Arial" fill="#111827">${escapeXml(category)}</text>`);
  });

  return `<?xml version="1.0" encoding="UTF-8"?>\n<svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}" viewBox="0 0 ${width} ${height}">\n${elements.join("\n")}\n</svg>\n`;
}

async function writeCharts(resultsDir, aggregatedBySweep) {
  const chartDir = path.join(resultsDir, CHART_DIRNAME);
  await mkdir(chartDir, { recursive: true });
  const writtenCharts = [];

  for (const sweep of SWEEPS) {
    const scenarioMap = aggregatedBySweep[sweep.key];
    const chartConfigs = chartSeriesForSweep(sweep.key, scenarioMap);

    for (const config of chartConfigs) {
      let series;
      if (config.series) {
        series = config.series.map((entry) => ({
          label: entry.label,
          values: Object.values(scenarioMap).map((scenario) => scenario.operations[entry.operation].avgGas)
        }));
      } else {
        series = config.sections.map((entry) => ({
          label: entry.label,
          values: Object.values(scenarioMap).map((scenario) => {
            if (entry.operation) {
              return scenario.operations[entry.operation].avgGas;
            }
            return scenario.sections[entry.section].avgGas;
          })
        }));
      }

      const svg = buildGroupedBarSvg({
        title: config.title,
        categories: config.categories,
        series
      });
      const fullPath = path.join(chartDir, config.file);
      await writeFile(fullPath, svg);
      writtenCharts.push({
        sweep: sweep.key,
        title: config.title,
        path: fullPath
      });
    }
  }

  return writtenCharts;
}

function buildSummaryMarkdown(aggregatedBySweep, chartPaths, resultsDir) {
  const lines = [];
  lines.push("# Geohash Workload Matrix Benchmark");
  lines.push("");
  lines.push("## Setup");
  lines.push("");
  lines.push(`- Rounds per scenario: ${ROUND_COUNT}`);
  lines.push("- Contract: `Geohash.sol`");
  lines.push("- Writes measured from transaction receipts");
  lines.push("- Reads measured from `eth_estimateGas`");
  lines.push("- Focus: workload-sensitive gas behavior of spatio-temporal queries");
  lines.push("");

  for (const sweep of SWEEPS) {
    const scenarioMap = aggregatedBySweep[sweep.key];
    const scenarioEntries = Object.values(scenarioMap);
    lines.push(`## ${sweep.label}`);
    lines.push("");

    const header = ["Scenario", ...sweep.operations.map((operation) => OPERATION_LABELS[operation])];
    lines.push(`| ${header.join(" | ")} |`);
    lines.push(`| ${header.map(() => "---").join(" | ")} |`);
    for (const scenario of scenarioEntries) {
      const row = [scenario.label];
      for (const operation of sweep.operations) {
        row.push((scenario.operations[operation].avgGas ?? "N/A").toLocaleString());
      }
      lines.push(`| ${row.join(" | ")} |`);
    }
    lines.push("");

    if (sweep.measureSections) {
      const sectionHeader = ["Scenario", ...Object.values(SECTION_LABELS)];
      lines.push(`| ${sectionHeader.join(" | ")} |`);
      lines.push(`| ${sectionHeader.map(() => "---").join(" | ")} |`);
      for (const scenario of scenarioEntries) {
        const row = [scenario.label];
        for (const sectionKey of Object.keys(SECTION_LABELS)) {
          row.push((scenario.sections[sectionKey].avgGas ?? "N/A").toLocaleString());
        }
        lines.push(`| ${row.join(" | ")} |`);
      }
      lines.push("");
    }

    const sweepCharts = chartPaths.filter((chart) => chart.sweep === sweep.key);
    for (const chart of sweepCharts) {
      lines.push(`Chart: ${chart.path}`);
      lines.push("");
    }

    if (sweep.key === "temporal_span") {
      const earliest = scenarioEntries[0];
      const latest = scenarioEntries[scenarioEntries.length - 1];
      const spatialFirstStart = earliest.operations.findBatchesInAreaDuringTime_SpatialFirst.avgGas;
      const spatialFirstEnd = latest.operations.findBatchesInAreaDuringTime_SpatialFirst.avgGas;
      const temporalFirstStart = earliest.operations.findBatchesInAreaDuringTime_TemporalFirst.avgGas;
      const temporalFirstEnd = latest.operations.findBatchesInAreaDuringTime_TemporalFirst.avgGas;
      lines.push(`Spatial-first ` +
        `gas changes from ${spatialFirstStart.toLocaleString()} to ${spatialFirstEnd.toLocaleString()}, ` +
        `while temporal-first changes from ${temporalFirstStart.toLocaleString()} to ${temporalFirstEnd.toLocaleString()}.`);
      lines.push("");
    }

    if (sweep.key === "spatial_selectivity") {
      const dense = scenarioEntries[scenarioEntries.length - 1];
      const selective = scenarioEntries[0];
      lines.push(
        `In the selective case, spatial-first costs ${selective.operations.findBatchesInAreaDuringTime_SpatialFirst.avgGas.toLocaleString()} gas and temporal-first costs ${selective.operations.findBatchesInAreaDuringTime_TemporalFirst.avgGas.toLocaleString()} gas. ` +
        `In the dense case, the same operations cost ${dense.operations.findBatchesInAreaDuringTime_SpatialFirst.avgGas.toLocaleString()} and ${dense.operations.findBatchesInAreaDuringTime_TemporalFirst.avgGas.toLocaleString()} gas, respectively.`
      );
      lines.push("");
    }
  }

  lines.push("## Output Files");
  lines.push("");
  lines.push(`- Summary text: ${path.join(resultsDir, "geohash-workload-matrix-summary.txt")}`);
  lines.push(`- Combined CSV: ${path.join(resultsDir, "geohash-workload-matrix.csv")}`);
  lines.push(`- Combined JSON: ${path.join(resultsDir, "geohash-workload-matrix.json")}`);
  lines.push(`- Temporal section CSV: ${path.join(resultsDir, "geohash-workload-sections.csv")}`);
  lines.push("");

  return lines.join("\n");
}

async function main() {
  const { ethers } = await network.create("benchnet");
  const aggregatedBySweep = {};
  const flatOperationRows = [];
  const flatSectionRows = [];

  console.log("Starting Geohash workload-matrix benchmark...");
  console.log(`Rounds per scenario: ${ROUND_COUNT}`);

  for (const sweep of SWEEPS) {
    console.log(`\n[${sweep.label}]`);
    aggregatedBySweep[sweep.key] = {};

    for (const scenarioDef of sweep.scenarios) {
      console.log(`- ${scenarioDef.label}`);
      const rawRoundResults = [];
      const rawSectionResults = [];

      for (let round = 1; round <= ROUND_COUNT; round += 1) {
        rawRoundResults.push(await benchmarkScenarioRound(ethers, scenarioDef.build, sweep.operations, round));
        if (sweep.measureSections) {
          rawSectionResults.push(await benchmarkSectionRound(ethers, scenarioDef.build, round));
        }
      }

      const aggregatedOperations = aggregateMetricMap(rawRoundResults, sweep.operations);
      const aggregatedScenario = {
        label: scenarioDef.label,
        xLabel: scenarioDef.xLabel,
        metadata: scenarioDef.metadata,
        operations: aggregatedOperations,
        sections: {}
      };

      if (sweep.measureSections) {
        aggregatedScenario.sections = aggregateMetricMap(rawSectionResults, Object.keys(SECTION_LABELS));
      }

      aggregatedBySweep[sweep.key][scenarioDef.key] = aggregatedScenario;

      for (const operationKey of sweep.operations) {
        const stats = aggregatedOperations[operationKey];
        flatOperationRows.push({
          sweep: sweep.key,
          sweepLabel: sweep.label,
          scenario: scenarioDef.key,
          scenarioLabel: scenarioDef.label,
          operation: operationKey,
          operationLabel: OPERATION_LABELS[operationKey],
          ...stats
        });
      }

      if (sweep.measureSections) {
        for (const sectionKey of Object.keys(SECTION_LABELS)) {
          const stats = aggregatedScenario.sections[sectionKey];
          flatSectionRows.push({
            sweep: sweep.key,
            scenario: scenarioDef.key,
            scenarioLabel: scenarioDef.label,
            section: sectionKey,
            sectionLabel: SECTION_LABELS[sectionKey],
            ...stats
          });
        }
      }
    }
  }

  const resultsDir = path.join(process.cwd(), "results");
  await mkdir(resultsDir, { recursive: true });
  const chartPaths = await writeCharts(resultsDir, aggregatedBySweep);

  await writeFile(
    path.join(resultsDir, "geohash-workload-matrix.json"),
    `${JSON.stringify({
      metadata: {
        benchmarkType: "geohash_workload_matrix",
        rounds: ROUND_COUNT
      },
      sweeps: aggregatedBySweep
    }, null, 2)}\n`
  );
  await writeFile(
    path.join(resultsDir, "geohash-workload-matrix.csv"),
    `${buildCombinedCsv(flatOperationRows)}\n`
  );
  await writeFile(
    path.join(resultsDir, "geohash-workload-sections.csv"),
    `${buildSectionCsv(flatSectionRows)}\n`
  );
  await writeFile(
    path.join(resultsDir, "geohash-workload-matrix-summary.txt"),
    `${buildSummaryMarkdown(aggregatedBySweep, chartPaths, resultsDir)}\n`
  );

  console.log(`Results written to ${resultsDir}`);
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
