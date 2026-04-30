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
const BATCH_OFFSETS = [28, 35, 42, 49, 56, 63, 70, 77, 84, 91, 98, 105];
const QUERY_WINDOW = { start: 56, end: 91 };
const LOT_LOCATION_TARGET_OFFSET = 80;

const OPERATION_ORDER = [
  "registerParcel",
  "registerLot",
  "registerBatch",
  "findBatchesInArea",
  "findBatchesInTimePeriod",
  "findBatchesInAreaDuringTime_SpatialFirst",
  "findBatchesInAreaDuringTime_TemporalFirst",
  "findBatchesInsideParcel",
  "findBatchesInsideParcelDuringTime",
  "countBatchesInArea",
  "findLotsInArea",
  "getLotTrajectory",
  "getLotLocationAtTime",
  "findParcelsAtLocation",
  "findLotsInsideParcelDuringTime"
];

const BREAKDOWN_ORDER = [
  "existingLeaf_existingBucket",
  "existingLeaf_newBucket",
  "newLeaf_existingBucket",
  "newLeaf_newBucket"
];

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

const BREAKDOWN_LABELS = {
  existingLeaf_existingBucket: "Same Leaf, Same Time Bucket",
  existingLeaf_newBucket: "Same Leaf, New Time Bucket",
  newLeaf_existingBucket: "New Leaf, Existing Time Bucket",
  newLeaf_newBucket: "New Leaf, New Time Bucket"
};

const SECTION_ORDER = ["gas", "setupGas", "spatialGas", "temporalGas", "sharedGas"];

const SECTION_LABELS = {
  gas: "Measured Transaction Gas",
  setupGas: "Common Setup Gas",
  spatialGas: "Spatial Tree Gas",
  temporalGas: "Temporal Tree Gas",
  sharedGas: "Shared Tail Gas"
};

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

function leaf(prefix4, suffix) {
  return `${prefix4}${suffix}`;
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

function aggregateFieldSamples(samples, field) {
  const successful = samples.filter((sample) => sample[field] !== null && sample[field] !== undefined);
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

  const values = successful.map((sample) => sample[field]);
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

function flattenMetricSamples(metricMap) {
  return Object.fromEntries(
    Object.entries(metricMap).map(([operationKey, samples]) => [operationKey, samples[0]])
  );
}

function pushMetric(storage, operationKey, sample) {
  if (!storage[operationKey]) {
    storage[operationKey] = [];
  }
  storage[operationKey].push(sample);
}

function csvCell(value) {
  const text = value === null || value === undefined ? "" : String(value);
  if (text.includes(",") || text.includes('"') || text.includes("\n")) {
    return `"${text.replace(/"/g, '""')}"`;
  }
  return text;
}

function buildScenario(round) {
  const digit = String(round % 10);
  const areaA = `a${digit}x1`;
  const areaB = `b${digit}x1`;
  const areaC = `c${digit}x1`;

  const parcel1Leaves = ["001", "002", "003", "004"].map((suffix) => leaf(areaA, suffix));
  const parcel2Leaves = ["001", "002", "003", "004"].map((suffix) => leaf(areaB, suffix));
  const parcel3Leaves = ["001", "002", "003", "004"].map((suffix) => leaf(areaC, suffix));

  const parcels = [
    { wkb: makeWkb("parcel", round, 1), leaves: parcel1Leaves },
    { wkb: makeWkb("parcel", round, 2), leaves: parcel2Leaves },
    { wkb: makeWkb("parcel", round, 3), leaves: parcel3Leaves }
  ];

  const lots = [
    { parcelId: 1, wkb: makeWkb("lot", round, 1) },
    { parcelId: 1, wkb: makeWkb("lot", round, 2) },
    { parcelId: 2, wkb: makeWkb("lot", round, 3) },
    { parcelId: 3, wkb: makeWkb("lot", round, 4) }
  ];

  const batches = [
    { lotId: 1, geohash: parcel1Leaves[0], offset: BATCH_OFFSETS[0] },
    { lotId: 2, geohash: parcel1Leaves[3], offset: BATCH_OFFSETS[1] },
    { lotId: 3, geohash: parcel2Leaves[0], offset: BATCH_OFFSETS[2] },
    { lotId: 4, geohash: parcel3Leaves[0], offset: BATCH_OFFSETS[3] },
    { lotId: 1, geohash: parcel1Leaves[1], offset: BATCH_OFFSETS[4] },
    { lotId: 2, geohash: parcel1Leaves[1], offset: BATCH_OFFSETS[5] },
    { lotId: 3, geohash: parcel2Leaves[2], offset: BATCH_OFFSETS[6] },
    { lotId: 4, geohash: parcel3Leaves[1], offset: BATCH_OFFSETS[7] },
    { lotId: 1, geohash: parcel1Leaves[2], offset: BATCH_OFFSETS[8] },
    { lotId: 2, geohash: parcel1Leaves[0], offset: BATCH_OFFSETS[9] },
    { lotId: 3, geohash: parcel2Leaves[3], offset: BATCH_OFFSETS[10] },
    { lotId: 4, geohash: parcel3Leaves[3], offset: BATCH_OFFSETS[11] }
  ].map((entry, index) => ({
    ...entry,
    wkb: makeWkb("batch", round, index + 1)
  }));

  return {
    round,
    parcels,
    lots,
    batches,
    targetPrefix4: asciiHex(areaA),
    targetParcelId: 1,
    targetLotId: 1,
    targetLeaf: asciiHex(parcel1Leaves[1]),
    targetLocationTimeOffset: LOT_LOCATION_TARGET_OFFSET,
    queryWindow: QUERY_WINDOW
  };
}

async function populateScenario(contract, scenario) {
  const provider = contract.runner.provider;
  const latestBlock = await provider.getBlock("latest");
  const baseDay = Math.floor(Number(latestBlock.timestamp) / 86400) + 1;

  const writeSamples = {
    registerParcel: [],
    registerLot: [],
    registerBatch: []
  };

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
    const sample = await measureTransactionGas(contract, "registerBatch", [
      batch.lotId,
      batch.wkb,
      batch.geohash,
      timestamp
    ], timestamp);
    writeSamples.registerBatch.push(sample);
  }

  return {
    baseDay,
    startBucket: baseDay + scenario.queryWindow.start,
    endBucket: baseDay + scenario.queryWindow.end,
    targetTime: (baseDay + scenario.targetLocationTimeOffset) * 86400,
    writeSamples
  };
}

async function populateScenarioMeasured(contract, scenario) {
  const provider = contract.runner.provider;
  const latestBlock = await provider.getBlock("latest");
  const baseDay = Math.floor(Number(latestBlock.timestamp) / 86400) + 1;
  const measuredBatchSamples = [];

  for (let index = 0; index < scenario.parcels.length; index += 1) {
    const timestamp = (baseDay + PARCEL_OFFSETS[index]) * 86400 + index;
    const parcel = scenario.parcels[index];
    await measureTransactionGas(contract, "registerParcel", [
      parcel.wkb,
      parcel.leaves.map(asciiHex)
    ], timestamp);
  }

  for (let index = 0; index < scenario.lots.length; index += 1) {
    const timestamp = (baseDay + LOT_OFFSETS[index]) * 86400 + index;
    const lot = scenario.lots[index];
    await measureTransactionGas(contract, "registerLot", [
      lot.parcelId,
      lot.wkb
    ], timestamp);
  }

  for (let index = 0; index < scenario.batches.length; index += 1) {
    const batch = scenario.batches[index];
    const timestamp = (baseDay + batch.offset) * 86400 + index;
    const sample = await measureBatchWithBreakdown(contract, [
      batch.lotId,
      batch.wkb,
      batch.geohash,
      timestamp
    ], timestamp);
    measuredBatchSamples.push(sample);
  }

  return measuredBatchSamples;
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

async function benchmarkMainRound(ethers, round) {
  const scenario = buildScenario(round);
  const contract = await deployGeohash(ethers);
  const populated = await populateScenario(contract, scenario);
  const specs = getReadSpecs(scenario, populated);
  const metrics = {};

  pushMetric(metrics, "registerParcel", summarizeWriteSamples(populated.writeSamples.registerParcel));
  pushMetric(metrics, "registerLot", summarizeWriteSamples(populated.writeSamples.registerLot));
  pushMetric(metrics, "registerBatch", summarizeWriteSamples(populated.writeSamples.registerBatch));

  for (const operationKey of OPERATION_ORDER) {
    if (operationKey.startsWith("register")) {
      continue;
    }
    const [method, args] = specs[operationKey];
    const sample = await measureReadGas(contract, method, args);
    pushMetric(metrics, operationKey, sample);
  }

  return metrics;
}

async function benchmarkMeasuredMainRound(ethers, round) {
  const scenario = buildScenario(round);
  const contract = await deployGeohash(ethers);
  return populateScenarioMeasured(contract, scenario);
}

function buildBreakdownScenario(round) {
  const digit = String(round % 10);
  const prefixX = `x${digit}y1`;
  const prefixY = `y${digit}y1`;
  const prefixZ = `z${digit}y1`;

  return {
    parcelLeaves: [leaf(prefixX, "001"), leaf(prefixY, "001"), leaf(prefixZ, "001")],
    leafX: leaf(prefixX, "001"),
    leafY: leaf(prefixY, "001"),
    leafZ: leaf(prefixZ, "001"),
    parcelWkb: makeWkb("pseed", round, 1),
    lotWkb: makeWkb("lseed", round, 1)
  };
}

async function benchmarkBreakdownRound(ethers, round) {
  const scenario = buildBreakdownScenario(round);
  const contract = await deployGeohash(ethers);
  const provider = contract.runner.provider;
  const latestBlock = await provider.getBlock("latest");
  const baseDay = Math.floor(Number(latestBlock.timestamp) / 86400) + 1;

  await measureTransactionGas(contract, "registerParcel", [
    scenario.parcelWkb,
    scenario.parcelLeaves.map(asciiHex)
  ], baseDay * 86400);

  await measureTransactionGas(contract, "registerLot", [
    1,
    scenario.lotWkb
  ], baseDay * 86400 + 1);

  await measureTransactionGas(contract, "registerBatch", [
    1,
    makeWkb("seed", round, 1),
    scenario.leafX,
    baseDay * 86400 + 10
  ], baseDay * 86400 + 10);

  const existingLeafExistingBucket = await measureBatchWithBreakdown(contract, [
    1,
    makeWkb("caseA", round, 1),
    scenario.leafX,
    baseDay * 86400 + 20
  ], baseDay * 86400 + 20);

  const newLeafExistingBucket = await measureBatchWithBreakdown(contract, [
    1,
    makeWkb("caseC", round, 1),
    scenario.leafY,
    baseDay * 86400 + 30
  ], baseDay * 86400 + 30);

  const existingLeafNewBucket = await measureBatchWithBreakdown(contract, [
    1,
    makeWkb("caseB", round, 1),
    scenario.leafX,
    (baseDay + 1) * 86400 + 10
  ], (baseDay + 1) * 86400 + 10);

  const newLeafNewBucket = await measureBatchWithBreakdown(contract, [
    1,
    makeWkb("caseD", round, 1),
    scenario.leafZ,
    (baseDay + 2) * 86400 + 10
  ], (baseDay + 2) * 86400 + 10);

  return {
    existingLeaf_existingBucket: existingLeafExistingBucket,
    existingLeaf_newBucket: existingLeafNewBucket,
    newLeaf_existingBucket: newLeafExistingBucket,
    newLeaf_newBucket: newLeafNewBucket
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

function aggregateMeasuredSections(samples) {
  return Object.fromEntries(
    SECTION_ORDER.map((field) => [field, aggregateFieldSamples(samples, field)])
  );
}

function aggregateBreakdownCases(rawResults) {
  const out = {};

  for (const caseKey of BREAKDOWN_ORDER) {
    const samples = rawResults.map((roundResult) => roundResult[caseKey]);
    out[caseKey] = Object.fromEntries(
      SECTION_ORDER.map((field) => [field, aggregateFieldSamples(samples, field)])
    );
  }

  return out;
}

function buildMainCsv(aggregatedResults) {
  const lines = [[
    "operation",
    "label",
    "avg_gas",
    "std_gas",
    "min_gas",
    "max_gas",
    "rounds",
    "success_rounds",
    "failure_rounds",
    "status"
  ].join(",")];

  for (const operationKey of OPERATION_ORDER) {
    const stats = aggregatedResults[operationKey];
    lines.push([
      operationKey,
      OPERATION_LABELS[operationKey],
      stats?.avgGas ?? "",
      stats?.stdGas ?? "",
      stats?.minGas ?? "",
      stats?.maxGas ?? "",
      stats?.rounds ?? "",
      stats?.successRounds ?? "",
      stats?.failureRounds ?? "",
      stats?.status ?? ""
    ].map(csvCell).join(","));
  }

  return lines.join("\n");
}

function buildSectionCsv(aggregatedSections) {
  const lines = [[
    "section",
    "label",
    "avg_gas",
    "std_gas",
    "min_gas",
    "max_gas",
    "rounds",
    "success_rounds",
    "failure_rounds",
    "status"
  ].join(",")];

  for (const sectionKey of SECTION_ORDER) {
    const stats = aggregatedSections[sectionKey];
    lines.push([
      sectionKey,
      SECTION_LABELS[sectionKey],
      stats?.avgGas ?? "",
      stats?.stdGas ?? "",
      stats?.minGas ?? "",
      stats?.maxGas ?? "",
      stats?.rounds ?? "",
      stats?.successRounds ?? "",
      stats?.failureRounds ?? "",
      stats?.status ?? ""
    ].map(csvCell).join(","));
  }

  return lines.join("\n");
}

function buildBreakdownCsv(aggregatedBreakdown) {
  const header = ["case", "label"];
  for (const field of SECTION_ORDER) {
    header.push(
      `${field}_avg_gas`,
      `${field}_std_gas`,
      `${field}_min_gas`,
      `${field}_max_gas`,
      `${field}_rounds`,
      `${field}_success_rounds`,
      `${field}_failure_rounds`,
      `${field}_status`
    );
  }
  const lines = [header.join(",")];

  for (const caseKey of BREAKDOWN_ORDER) {
    const statsByField = aggregatedBreakdown[caseKey];
    const row = [caseKey, BREAKDOWN_LABELS[caseKey]];
    for (const field of SECTION_ORDER) {
      const stats = statsByField[field];
      row.push(
        stats?.avgGas ?? "",
        stats?.stdGas ?? "",
        stats?.minGas ?? "",
        stats?.maxGas ?? "",
        stats?.rounds ?? "",
        stats?.successRounds ?? "",
        stats?.failureRounds ?? "",
        stats?.status ?? ""
      );
    }
    lines.push([
      ...row
    ].map(csvCell).join(","));
  }

  return lines.join("\n");
}

function derivedBreakdown(aggregatedBreakdown) {
  const baseCase = aggregatedBreakdown.existingLeaf_existingBucket;
  const temporalCase = aggregatedBreakdown.existingLeaf_newBucket;
  const leafCase = aggregatedBreakdown.newLeaf_existingBucket;
  const bothCase = aggregatedBreakdown.newLeaf_newBucket;

  const baseTemporal = baseCase?.temporalGas?.avgGas ?? 0;
  const baseSpatial = baseCase?.spatialGas?.avgGas ?? 0;
  const temporal = temporalCase?.temporalGas?.avgGas ?? 0;
  const leaf = leafCase?.spatialGas?.avgGas ?? 0;
  const both = bothCase?.gas?.avgGas ?? 0;

  return {
    baseMeasuredTxGas: roundValue(baseCase?.gas?.avgGas ?? 0),
    baseSpatialGas: roundValue(baseSpatial),
    baseTemporalGas: roundValue(baseTemporal),
    additionalTemporalTreeGasForNewBucket: roundValue(temporal - baseTemporal),
    additionalSpatialTreeGasForNewLeaf: roundValue(leaf - baseSpatial),
    measuredTxGasWhenBothAreNew: roundValue(both)
  };
}

function buildMarkdown(aggregatedResults, aggregatedSections, aggregatedBreakdown) {
  const derived = derivedBreakdown(aggregatedBreakdown);
  const lines = [];

  lines.push("# Geohash Gas Benchmark");
  lines.push("");
  lines.push("## Scenario");
  lines.push("");
  lines.push(`- Rounds: ${ROUND_COUNT}`);
  lines.push("- Contract: `Geohash.sol` only");
  lines.push("- Parcels: 3");
  lines.push("- Lots: 4");
  lines.push("- Batches: 12");
  lines.push("- Parcel declaration gaps: 10 days");
  lines.push("- Batch creation gaps: 7 days");
  lines.push("- Query time window: day offsets 56 to 91");
  lines.push("- Area query target: the first parcel area (prefix4 of area A)");
  lines.push("");
  lines.push("Gas for writes comes from transaction receipts. Gas for reads comes from `estimateGas`.");
  lines.push("");
  lines.push("## Main Operations");
  lines.push("");
  lines.push("| Operation | Avg Gas | Std Dev | Min | Max | Status |");
  lines.push("|---|---:|---:|---:|---:|---|");

  for (const operationKey of OPERATION_ORDER) {
    const stats = aggregatedResults[operationKey];
    lines.push(
      `| ${operationKey} | ${stats?.avgGas ?? "N/A"} | ${stats?.stdGas ?? "N/A"} | ${stats?.minGas ?? "N/A"} | ${stats?.maxGas ?? "N/A"} | ${stats?.status ?? "n/a"} |`
    );
  }

  lines.push("");
  lines.push("## RegisterBatch Section Split");
  lines.push("");
  lines.push("| Section | Avg Gas | Std Dev | Min | Max | Status |");
  lines.push("|---|---:|---:|---:|---:|---|");

  for (const sectionKey of SECTION_ORDER) {
    const stats = aggregatedSections[sectionKey];
    lines.push(
      `| ${sectionKey} | ${stats?.avgGas ?? "N/A"} | ${stats?.stdGas ?? "N/A"} | ${stats?.minGas ?? "N/A"} | ${stats?.maxGas ?? "N/A"} | ${stats?.status ?? "n/a"} |`
    );
  }

  lines.push("");
  lines.push("## Controlled RegisterBatch Cases");
  lines.push("");
  lines.push("| Case | Tx Gas | Setup | Spatial | Temporal | Shared |");
  lines.push("|---|---:|---:|---:|---:|---:|");

  for (const caseKey of BREAKDOWN_ORDER) {
    const stats = aggregatedBreakdown[caseKey];
    lines.push(
      `| ${caseKey} | ${stats?.gas?.avgGas ?? "N/A"} | ${stats?.setupGas?.avgGas ?? "N/A"} | ${stats?.spatialGas?.avgGas ?? "N/A"} | ${stats?.temporalGas?.avgGas ?? "N/A"} | ${stats?.sharedGas?.avgGas ?? "N/A"} |`
    );
  }

  lines.push("");
  lines.push("## Write Interpretation");
  lines.push("");
  lines.push(`- Exact average \`registerBatch\` gas in the main scenario: ${aggregatedResults.registerBatch.avgGas}`);
  lines.push(`- Exact average measured setup gas inside \`registerBatchMeasured\`: ${aggregatedSections.setupGas.avgGas}`);
  lines.push(`- Exact average measured spatial-tree gas inside \`registerBatchMeasured\`: ${aggregatedSections.spatialGas.avgGas}`);
  lines.push(`- Exact average measured temporal-tree gas inside \`registerBatchMeasured\`: ${aggregatedSections.temporalGas.avgGas}`);
  lines.push(`- Exact average measured shared-tail gas inside \`registerBatchMeasured\`: ${aggregatedSections.sharedGas.avgGas}`);
  lines.push(`- Base measured transaction gas when the leaf and time bucket already exist: ${derived.baseMeasuredTxGas}`);
  lines.push(`- Base spatial-tree gas in that base case: ${derived.baseSpatialGas}`);
  lines.push(`- Base temporal-tree gas in that base case: ${derived.baseTemporalGas}`);
  lines.push(`- Additional temporal-tree gas when opening a new time bucket: ${derived.additionalTemporalTreeGasForNewBucket}`);
  lines.push(`- Additional spatial-tree gas when inserting a new leaf into an existing time bucket: ${derived.additionalSpatialTreeGasForNewLeaf}`);
  lines.push(`- Measured transaction gas when both a new leaf and a new time bucket are introduced: ${derived.measuredTxGasWhenBothAreNew}`);
  lines.push("");
  lines.push("The original `registerBatch` path is still used to report real total write gas.");
  lines.push("The measured path `registerBatchMeasured` is used only to extract exact internal section costs for setup, spatial-tree writes, temporal-tree writes, and shared bookkeeping.");
  lines.push("");

  return lines.join("\n");
}

async function main() {
  const { ethers } = await network.create("benchnet");
  const rawMainResults = [];
  const rawMeasuredSectionSamples = [];
  const rawBreakdownResults = [];

  console.log("Starting Geohash benchmark...");
  console.log("Scenario: 3 parcels, 4 lots, 12 batches, 10 rounds");

  for (let round = 1; round <= ROUND_COUNT; round += 1) {
    console.log(`Round ${round}/${ROUND_COUNT}`);
    rawMainResults.push(flattenMetricSamples(await benchmarkMainRound(ethers, round)));
    rawMeasuredSectionSamples.push(...await benchmarkMeasuredMainRound(ethers, round));
    rawBreakdownResults.push(await benchmarkBreakdownRound(ethers, round));
  }

  const aggregatedResults = aggregateMetricMap(rawMainResults, OPERATION_ORDER);
  const aggregatedSections = aggregateMeasuredSections(rawMeasuredSectionSamples);
  const aggregatedBreakdown = aggregateBreakdownCases(rawBreakdownResults);

  const output = {
    metadata: {
      benchmarkType: "geohash_gas_only",
      rounds: ROUND_COUNT,
      parcels: 3,
      lots: 4,
      batches: 12,
      parcelOffsets: PARCEL_OFFSETS,
      lotOffsets: LOT_OFFSETS,
      batchOffsets: BATCH_OFFSETS,
      queryWindow: QUERY_WINDOW,
      locationTargetOffset: LOT_LOCATION_TARGET_OFFSET
    },
    aggregatedResults,
    aggregatedSections,
    aggregatedBreakdown,
    derivedBreakdown: derivedBreakdown(aggregatedBreakdown)
  };

  const resultsDir = path.join(process.cwd(), "results");
  await mkdir(resultsDir, { recursive: true });

  await writeFile(
    path.join(resultsDir, "geohash-gas-results.json"),
    JSON.stringify(output, null, 2)
  );
  await writeFile(
    path.join(resultsDir, "geohash-gas-results.csv"),
    `${buildMainCsv(aggregatedResults)}\n`
  );
  await writeFile(
    path.join(resultsDir, "geohash-write-breakdown.csv"),
    `${buildBreakdownCsv(aggregatedBreakdown)}\n`
  );
  await writeFile(
    path.join(resultsDir, "geohash-registerbatch-sections.csv"),
    `${buildSectionCsv(aggregatedSections)}\n`
  );
  await writeFile(
    path.join(resultsDir, "geohash-gas-summary.txt"),
    `${buildMarkdown(aggregatedResults, aggregatedSections, aggregatedBreakdown)}\n`
  );

  console.log(`Results written to ${resultsDir}`);
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
