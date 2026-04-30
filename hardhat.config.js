import hardhatEthers from "@nomicfoundation/hardhat-ethers";
import { defineConfig } from "hardhat/config";

const sepoliaPrivateKey = process.env.SEPOLIA_PRIVATE_KEY;
const sepoliaAccounts = sepoliaPrivateKey === undefined || sepoliaPrivateKey === ""
  ? []
  : [sepoliaPrivateKey.startsWith("0x") ? sepoliaPrivateKey : `0x${sepoliaPrivateKey}`];

export default defineConfig({
  plugins: [hardhatEthers],
  solidity: {
    version: "0.8.24",
    settings: {
      viaIR: true,
      optimizer: {
        enabled: true,
        runs: 200
      }
    }
  },
  networks: {
    benchnet: {
      type: "edr-simulated",
      chainType: "l1",
      blockGasLimit: 200_000_000n
    },
    sepolia: {
      type: "http",
      chainType: "l1",
      url: process.env.SEPOLIA_RPC_URL ?? "https://ethereum-sepolia-rpc.publicnode.com",
      accounts: sepoliaAccounts
    }
  },
  paths: {
    sources: "./contracts",
    cache: "./cache",
    artifacts: "./artifacts"
  }
});
