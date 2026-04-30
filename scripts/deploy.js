import { execFileSync } from "node:child_process";
import { existsSync } from "node:fs";
import { network } from "hardhat";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";

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

async function main() {
  const { ethers } = await network.create();
  const signers = await ethers.getSigners();

  if (signers.length === 0) {
    throw new Error(
      "No deployer account available. For Sepolia, set SEPOLIA_PRIVATE_KEY and optionally SEPOLIA_RPC_URL."
    );
  }

  const [deployer] = signers;
  const providerNetwork = await ethers.provider.getNetwork();
  const selectedNetwork = Number(providerNetwork.chainId) === 11155111 ? "sepolia" : "benchnet";

  console.log(
    `Deploying Geohash to ${selectedNetwork} (chainId ${providerNetwork.chainId}) with account: ${deployer.address}`
  );

  const artifact = await compileGeohashArtifact();
  const factory = new ethers.ContractFactory(artifact.abi, artifact.bytecode, deployer);
  const contract = await factory.deploy();
  await contract.waitForDeployment();

  const address = await contract.getAddress();
  const deploymentTx = contract.deploymentTransaction();
  const output = {
    contract: "Geohash",
    network: selectedNetwork,
    chainId: Number(providerNetwork.chainId),
    address,
    deployer: deployer.address,
    transactionHash: deploymentTx?.hash ?? null
  };

  const deploymentsDir = path.join(process.cwd(), "deployments");
  await mkdir(deploymentsDir, { recursive: true });
  await writeFile(
    path.join(deploymentsDir, `${selectedNetwork}.json`),
    `${JSON.stringify(output, null, 2)}\n`
  );

  console.log(JSON.stringify(output, null, 2));
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
