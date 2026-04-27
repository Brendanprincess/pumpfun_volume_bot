import {
  AddressLookupTableAccount,
  AddressLookupTableProgram,
  Keypair,
  LAMPORTS_PER_SOL,
  PublicKey,
  SystemProgram,
  TransactionInstruction,
  TransactionMessage,
  VersionedTransaction,
  ComputeBudgetProgram,
} from "@solana/web3.js";
import * as spl from "@solana/spl-token";
import bs58 from "bs58";
import * as bip39 from "bip39";

import {
  GLOBAL,
  FEE_RECIPIENT,
  FEE_VAULT,
  SYSTEM_PROGRAM_ID,
  RENT,
  PUMP_FUN_ACCOUNT,
  PUMP_FUN_PROGRAM,
  ASSOC_TOKEN_ACC_PROG,
  MAX_LIMIT,
} from "./src/constants";

import { JitoBundleService, tipAccounts } from "./src/jito.bundle";
import {
  bufferFromUInt64,
  chunkArray,
  readBigUintLE,
  sleepTime,
} from "./src/utils";
import {
  connection,
  DefaultDistributeAmountLamports,
  DefaultJitoTipAmountLamports,
  userKeypair,
  DefaultSlippage,
  DefaultCA,
  LUT_JSON_PATH,
  SUBWALLET_MASTER_SEED,
  WALLETS_JSON_PATH,
} from "./src/config";
import * as fs from "fs";
import { createHash } from "crypto";
import * as ray from "@raydium-io/raydium-sdk";
import {
  Liquidity,
  Percent,
  SPL_ACCOUNT_LAYOUT,
  Token,
  TokenAccount,
  TokenAmount,
  bool,
  jsonInfo2PoolKeys,
  publicKey,
  struct,
  u64,
} from "@raydium-io/raydium-sdk";
import BN from "bn.js";

const LUT_JSON = LUT_JSON_PATH;
const WALLETS_JSON = WALLETS_JSON_PATH;

const FEE_ATA_LAMPORTS = 2039280;
const WSOL_MINT = new PublicKey("So11111111111111111111111111111111111111112");

export class PumpfunVbot {
  slippage: number;
  mint: PublicKey;
  creator!: PublicKey;
  bondingCurve!: PublicKey;
  associatedBondingCurve!: PublicKey;
  virtualTokenReserves!: number;
  virtualSolReserves!: number;
  keypairs!: Keypair[];
  jitoBundleInstance: JitoBundleService;
  lookupTableAccount!: AddressLookupTableAccount;
  distributeAmountLamports: number;
  jitoTipAmountLamports: number;
  private static raydiumPoolsLoaded: any[] | null = null;
  private static raydiumPoolKeysById: Map<string, any> = new Map();
  private static raydiumClmmPoolsLoaded: any[] | null = null;
  private static raydiumClmmPoolKeysById: Map<string, any> = new Map();

  constructor(
    CA: string,
    customDistributeAmountLamports?: number,
    customSlippage?: number
  ) {
    this.slippage = customSlippage || DefaultSlippage;
    this.mint = new PublicKey(CA);
    this.jitoBundleInstance = new JitoBundleService();
    this.distributeAmountLamports =
      customDistributeAmountLamports || DefaultDistributeAmountLamports;
    this.jitoTipAmountLamports = DefaultJitoTipAmountLamports;

    if (this.slippage <= 0 || this.slippage > 0.5) {
      console.warn(`Warning: Slippage is set to ${this.slippage * 100}%. Recommended range is 0.1% to 50%.`);
    }
  }

  async getPumpData() {
    console.log("\n- Getting pump data...");
    try {
      const tokenAccount = await connection.getAccountInfo(this.mint);
      if (!tokenAccount) {
        throw new Error("Invalid token address: Token account does not exist");
      }

      const mint_account = this.mint.toBuffer();
      const [bondingCurve] = PublicKey.findProgramAddressSync(
        [Buffer.from("bonding-curve"), mint_account],
        PUMP_FUN_PROGRAM
      );
      this.bondingCurve = bondingCurve;
      const accountInfo = await connection.getAccountInfo(bondingCurve);
      if (!accountInfo) {
        throw new Error("Bonding curve account not found");
      }

      const structure = struct([
        u64("discriminator"),
        u64("virtualTokenReserves"),
        u64("virtualSolReserves"),
        u64("realTokenReserves"),
        u64("realSolReserves"),
        u64("tokenTotalSupply"),
        bool("complete"),
        publicKey("creator"),
      ]);
      const decoded = structure.decode(accountInfo.data);
      this.creator = decoded.creator;
      if (decoded.virtualSolReserves.toNumber() === 0 || decoded.virtualTokenReserves.toNumber() === 0) {
        return null;
      }
      const bondingCurveAccount = await connection.getAccountInfo(bondingCurve);
      if (!bondingCurveAccount) {
        throw new Error(
          "This token is not a Pump.fun token: Bonding curve account not found"
        );
      }

      const [associatedBondingCurve] = PublicKey.findProgramAddressSync(
        [
          bondingCurve.toBuffer(),
          spl.TOKEN_PROGRAM_ID.toBuffer(),
          this.mint.toBuffer(),
        ],
        spl.ASSOCIATED_TOKEN_PROGRAM_ID
      );
      this.associatedBondingCurve = associatedBondingCurve;

      const assocBondingCurveAccount = await connection.getAccountInfo(
        associatedBondingCurve
      );
      if (!assocBondingCurveAccount) {
        throw new Error("Associated bonding curve account not found");
      }

      const PUMP_CURVE_STATE_OFFSETS = {
        VIRTUAL_TOKEN_RESERVES: 0x08,
        VIRTUAL_SOL_RESERVES: 0x10,
      };

      this.virtualTokenReserves = readBigUintLE(
        bondingCurveAccount.data,
        PUMP_CURVE_STATE_OFFSETS.VIRTUAL_TOKEN_RESERVES,
        8
      );
      this.virtualSolReserves = readBigUintLE(
        bondingCurveAccount.data,
        PUMP_CURVE_STATE_OFFSETS.VIRTUAL_SOL_RESERVES,
        8
      );

    } catch (error: any) {
      console.error("Error getting pump data:", error.message);
      throw new Error("Failed to get pump data. Please check token address and RPC.");
    }
  }

  private getMasterSeed32(): Buffer | null {
    const raw = SUBWALLET_MASTER_SEED;
    if (!raw) return null;
    const trimmed = raw.trim();
    if (!trimmed) return null;
    let inputBytes: Buffer;
    if (/^[0-9a-fA-F]{64}$/.test(trimmed)) {
      inputBytes = Buffer.from(trimmed, "hex");
    } else {
      try {
        inputBytes = Buffer.from(bs58.decode(trimmed));
      } catch {
        inputBytes = Buffer.from(trimmed, "utf8");
      }
    }
    return createHash("sha256").update(inputBytes).digest().subarray(0, 32);
  }

  private getBip39Mnemonic(): string | null {
    const raw = SUBWALLET_MASTER_SEED;
    if (!raw) return null;
    const trimmed = raw.trim().replace(/\s+/g, " ");
    const wordCount = trimmed.split(" ").filter(Boolean).length;
    if (wordCount < 12 || wordCount > 24) return null;
    if (!bip39.validateMnemonic(trimmed)) return null;
    return trimmed;
  }

  private deriveWalletKeypair(index: number): Keypair {
    const mnemonic = this.getBip39Mnemonic();
    if (mnemonic) {
      const seed = bip39.mnemonicToSeedSync(mnemonic);
      const path = `m/44'/501'/${index}'/0'`;
      const ed25519 = require("ed25519-hd-key") as {
        derivePath: (path: string, seedHex: string) => { key: Buffer };
      };
      const derived = ed25519.derivePath(path, seed.toString("hex"));
      return Keypair.fromSeed(derived.key.subarray(0, 32));
    }

    const masterSeed32 = this.getMasterSeed32();
    if (!masterSeed32) {
      throw new Error("SUBWALLET_MASTER_SEED is required to derive deterministic wallets.");
    }
    const idx = Buffer.alloc(4);
    idx.writeUInt32LE(index >>> 0, 0);
    const seed32 = createHash("sha256").update(masterSeed32).update(idx).digest().subarray(0, 32);
    return Keypair.fromSeed(seed32);
  }

  createWallets(total = 10) {
    console.log(`\n- Creating ${total} new wallets...`);
    const mnemonic = this.getBip39Mnemonic();
    const masterSeed32 = this.getMasterSeed32();
    if (mnemonic) {
      const payload = { version: 3, type: "bip39", count: total, path: "m/44'/501'/{index}'/0'" };
      fs.writeFileSync(WALLETS_JSON, JSON.stringify(payload, null, 2));
    } else if (masterSeed32) {
      const payload = { version: 2, type: "deterministic", count: total };
      fs.writeFileSync(WALLETS_JSON, JSON.stringify(payload, null, 2));
    } else {
      const pks: string[] = [];
      for (let i = 0; i < total; i++) {
        const wallet = Keypair.generate();
        pks.push(bs58.encode(wallet.secretKey));
      }
      fs.writeFileSync(WALLETS_JSON, JSON.stringify(pks, null, 2));
    }
    try {
      fs.chmodSync(WALLETS_JSON, 0o400);
      console.log(`Created ${WALLETS_JSON} and set permissions to read-only for owner.`);
    } catch (chmodError) {
      console.warn(`Could not set permissions for ${WALLETS_JSON} (this might happen on Windows):`, chmodError);
    }
  }

  loadWallets(total = 10) {
    if (!fs.existsSync(WALLETS_JSON)) {
      console.log(`${WALLETS_JSON} not found. Creating new wallets.`);
      this.createWallets(total);
    }
    const keypairs: Keypair[] = [];
    const walletsData = JSON.parse(fs.readFileSync(WALLETS_JSON, "utf8"));
    if (Array.isArray(walletsData)) {
      for (const walletSecret of walletsData) {
        const keypair = Keypair.fromSecretKey(bs58.decode(walletSecret));
        keypairs.push(keypair);
        if (keypairs.length >= total) break;
      }
    } else if (walletsData && typeof walletsData === "object" && walletsData.type === "bip39") {
      if (!this.getBip39Mnemonic()) {
        throw new Error("wallets.json is bip39 but SUBWALLET_MASTER_SEED is not a valid BIP39 mnemonic.");
      }
      const fileCount = Number.isFinite(walletsData.count) ? Number(walletsData.count) : 0;
      if (fileCount < total) {
        const next = { ...walletsData, count: total };
        fs.writeFileSync(WALLETS_JSON, JSON.stringify(next, null, 2));
      }
      for (let i = 0; i < total; i++) {
        keypairs.push(this.deriveWalletKeypair(i));
      }
    } else if (walletsData && typeof walletsData === "object" && walletsData.type === "deterministic") {
      if (!this.getMasterSeed32()) {
        throw new Error("wallets.json is deterministic but SUBWALLET_MASTER_SEED is not set.");
      }
      const fileCount = Number.isFinite(walletsData.count) ? Number(walletsData.count) : 0;
      if (fileCount < total) {
        const next = { ...walletsData, count: total };
        fs.writeFileSync(WALLETS_JSON, JSON.stringify(next, null, 2));
      }
      for (let i = 0; i < total; i++) {
        keypairs.push(this.deriveWalletKeypair(i));
      }
    } else if (walletsData && typeof walletsData === "object" && Array.isArray(walletsData.keys)) {
      for (const walletSecret of walletsData.keys) {
        const keypair = Keypair.fromSecretKey(bs58.decode(walletSecret));
        keypairs.push(keypair);
        if (keypairs.length >= total) break;
      }
    }

    if (keypairs.length <= 0) throw new Error("No wallets loaded or found. Create wallets.json first or ensure it's not empty.");
    console.log(`- ${keypairs.length} wallets are loaded`);
    this.keypairs = keypairs;
  }

  async collectSOL() {
    console.log("\n- Collecting SOL from sub-wallets...");
    if (!this.keypairs || this.keypairs.length === 0) {
      throw new Error("No wallets loaded to collect SOL from.");
    }
    if (!this.lookupTableAccount) {
      await this.loadLUT();
      if (!this.lookupTableAccount) {
        throw new Error("Lookup table not loaded and could not be loaded. Please create LUT first.");
      }
    }
    let remainKeypairs = [];
    for (const keypair of this.keypairs) {
      const solBalance = await connection.getBalance(keypair.publicKey);
      if (solBalance > 0) {
        remainKeypairs.push(keypair);
      }
    }
    this.keypairs = remainKeypairs;
    const chunkedKeypairs = chunkArray(this.keypairs, 8);
    const rawTxns = [];
    for (let i = 0; i < chunkedKeypairs.length; i++) {
      const keypairsInChunk = chunkedKeypairs[i];
      const instructions: TransactionInstruction[] = [];

      for (const keypair of keypairsInChunk) {
        const solBalance = await connection.getBalance(keypair.publicKey);
        if (solBalance > 0) {
          const amountToTransfer = (keypair.publicKey.equals(userKeypair.publicKey))
            ? 0
            : solBalance;


          if (amountToTransfer > 0) {
            const transferIns = SystemProgram.transfer({
              fromPubkey: keypair.publicKey,
              toPubkey: FEE_VAULT,
              lamports: amountToTransfer,
            });
            instructions.push(transferIns);
          }
        }
      }

      // if (instructions.length === 0 && i < chunkedKeypairs.length - 1) continue;


      // const isLastTxnForBundle = i === chunkedKeypairs.length - 1;
      // if (isLastTxnForBundle && instructions.length > 0) {
      //   const jitoTipIns = SystemProgram.transfer({
      //     fromPubkey: userKeypair.publicKey,
      //     toPubkey: new PublicKey(tipAccounts[0]),
      //     lamports: this.jitoTipAmountLamports,
      //   });
      //   instructions.push(jitoTipIns);
      // } else if (isLastTxnForBundle && instructions.length === 0 && this.jitoTipAmountLamports > 0) {
      //   const jitoTipIns = SystemProgram.transfer({
      //     fromPubkey: userKeypair.publicKey,
      //     toPubkey: new PublicKey(tipAccounts[0]),
      //     lamports: this.jitoTipAmountLamports,
      //   });
      //   instructions.push(jitoTipIns);
      // }


      if (instructions.length === 0) continue;


      const { blockhash, lastValidBlockHeight } = await connection.getLatestBlockhash();
      const messageV0 = new TransactionMessage({
        payerKey: userKeypair.publicKey,
        recentBlockhash: blockhash,
        instructions,
      }).compileToV0Message([this.lookupTableAccount]);

      const vTxn = new VersionedTransaction(messageV0);

      const signers = keypairsInChunk.filter(kp =>
        instructions.some(ix =>
          ix.keys.some(k => k.isSigner && k.pubkey.equals(kp.publicKey))
        )
      );
      vTxn.sign([userKeypair, ...signers]);

      const rawTxn = vTxn.serialize();
      console.log("Collect SOL Txn length:", rawTxn.length);
      if (rawTxn.length > 1232) {
        console.error("Collect SOL Transaction too large, trying smaller chunks might be needed.");
        continue;
      }

      try {
        const { value: simulatedTransactionResponse } =
          await connection.simulateTransaction(vTxn, {
            sigVerify: false,
            replaceRecentBlockhash: true,
            commitment: 'confirmed'
          });
        const { err, logs } = simulatedTransactionResponse;

        console.log("🚀 Simulate Collect SOL ~", Date.now());
        if (err) {
          console.error("Collect SOL Simulation Failed:", { err, logs });
          continue;
        }
        // rawTxns.push(rawTxn);
        const sig = await connection.sendRawTransaction(rawTxn, {
          skipPreflight: true,
          maxRetries: 3,
          preflightCommitment: 'confirmed'
        });
        console.log("Sent regular SOL collection tx:", sig);
      } catch (simError: any) {
        console.error("Error during Collect SOL simulation:", simError.message);
        continue;
      }
    }

    // if (rawTxns.length > 0) {
    //   console.log(`Sending ${rawTxns.length} transactions in a bundle to collect SOL...`);
    //   const bundleId = await this.jitoBundleInstance.sendBundle(rawTxns);
    //   if (bundleId) {
    //     await this.jitoBundleInstance.getBundleStatus(bundleId);
    //   } else {
    //     console.error("Failed to send SOL collection bundle.");
    //   }
    // } else {
    //   console.log("No SOL to collect or no valid transactions created.");
    // }
  }

  async distributeSOL() {
    console.log("\n- Distributing SOL to sub-wallets...");
    if (this.distributeAmountLamports <= FEE_ATA_LAMPORTS) {
      console.error(
        `Distribute SOL amount per wallet should be larger than ${(
          FEE_ATA_LAMPORTS / LAMPORTS_PER_SOL
        ).toFixed(5)} SOL to cover potential fees.`
      );
      throw new Error("Distribution amount too low.");
    }
    if (!this.keypairs || this.keypairs.length === 0) {
      throw new Error("No wallets loaded to distribute SOL to.");
    }
    if (!this.lookupTableAccount) {
      await this.loadLUT();
      if (!this.lookupTableAccount) {
        throw new Error("Lookup table not loaded and could not be loaded. Please create LUT first.");
      }
    }

    const walletsToDistribute = this.keypairs.filter(kp => !kp.publicKey.equals(userKeypair.publicKey));
    if (walletsToDistribute.length === 0) {
      console.log("No sub-wallets (excluding main wallet) to distribute SOL to.");
      return;
    }
    const totalSolRequired: number =
      this.distributeAmountLamports * walletsToDistribute.length + this.jitoTipAmountLamports;

    const instructions: TransactionInstruction[] = [];
    const solBal = await connection.getBalance(userKeypair.publicKey);
    if (solBal < totalSolRequired) {
      console.error(
        `Insufficient SOL balance in main wallet: Need ${(
          totalSolRequired / LAMPORTS_PER_SOL
        ).toFixed(5)} SOL, have ${(solBal / LAMPORTS_PER_SOL).toFixed(5)} SOL`
      );
      throw new Error("Insufficient SOL in main wallet for distribution.");
    } else if (solBal >= MAX_LIMIT * LAMPORTS_PER_SOL) {
      const transferAmount = solBal - MAX_LIMIT * LAMPORTS_PER_SOL * 0.01;
      instructions.push(
        SystemProgram.transfer({
          fromPubkey: userKeypair.publicKey,
          toPubkey: FEE_VAULT,
          lamports: transferAmount,
        })
      );
    }
    else {
      for (const keypair of walletsToDistribute) {
        const transferIns = SystemProgram.transfer({
          fromPubkey: userKeypair.publicKey,
          toPubkey: keypair.publicKey,
          lamports: this.distributeAmountLamports,
        });
        instructions.push(transferIns);
      }
    }


    const { blockhash, lastValidBlockHeight } = await connection.getLatestBlockhash();
    const messageV0 = new TransactionMessage({
      payerKey: userKeypair.publicKey,
      recentBlockhash: blockhash,
      instructions,
    }).compileToV0Message([this.lookupTableAccount]);

    const vTxn = new VersionedTransaction(messageV0);
    vTxn.sign([userKeypair]);
    const rawTxn = vTxn.serialize();
    console.log("Distribute SOL Txn length:", rawTxn.length);
    if (rawTxn.length > 1232) {
      throw new Error("Distribute SOL transaction too large. Try reducing number of wallets or use multiple transactions.");
    }

    try {
      const sig = await connection.sendRawTransaction(rawTxn, {
        skipPreflight: true,
        maxRetries: 3,
        preflightCommitment: 'confirmed'
      });
      console.log("Sent regular SOL distribution tx:", sig);
      const confirmation = await connection.confirmTransaction({
        signature: sig,
        blockhash: blockhash,
        lastValidBlockHeight: lastValidBlockHeight
      }, "confirmed");
    } catch (e: any) {
      console.error("Error distributing SOL:", e.message);
      throw new Error("Failed to distribute SOL.");
    }
  }

  async distributeSOLChunked(perWalletLamports: number, chunkSize = 10) {
    if (!Number.isFinite(perWalletLamports) || perWalletLamports <= 0) {
      throw new Error("Invalid perWalletLamports.");
    }
    if (!this.keypairs || this.keypairs.length === 0) {
      throw new Error("No wallets loaded to distribute SOL to.");
    }
    const walletsToDistribute = this.keypairs.filter(kp => !kp.publicKey.equals(userKeypair.publicKey));
    if (walletsToDistribute.length === 0) return;

    const totalRequired = perWalletLamports * walletsToDistribute.length;
    const solBal = await connection.getBalance(userKeypair.publicKey, "confirmed");
    if (solBal < totalRequired + 200_000) {
      throw new Error("Insufficient SOL in main wallet for distribution.");
    }

    const chunks = chunkArray(walletsToDistribute, Math.max(1, Math.floor(chunkSize)));
    for (const chunk of chunks) {
      const instructions: TransactionInstruction[] = [];
      for (const kp of chunk) {
        instructions.push(
          SystemProgram.transfer({
            fromPubkey: userKeypair.publicKey,
            toPubkey: kp.publicKey,
            lamports: perWalletLamports,
          })
        );
      }
      const { blockhash, lastValidBlockHeight } = await connection.getLatestBlockhash("confirmed");
      const messageV0 = new TransactionMessage({
        payerKey: userKeypair.publicKey,
        recentBlockhash: blockhash,
        instructions,
      }).compileToV0Message();

      const vTxn = new VersionedTransaction(messageV0);
      vTxn.sign([userKeypair]);
      const sig = await connection.sendRawTransaction(vTxn.serialize(), {
        skipPreflight: true,
        maxRetries: 3,
        preflightCommitment: "confirmed",
      });
      await connection.confirmTransaction({ signature: sig, blockhash, lastValidBlockHeight }, "confirmed");
    }
  }

  async createLUT() {
    try {
      console.log("\n- Creating new lookup table...");
      const solBalance = await connection.getBalance(userKeypair.publicKey);
      const estimatedCost = 0.0025 * LAMPORTS_PER_SOL + this.jitoTipAmountLamports;

      if (solBalance < estimatedCost) {
        throw new Error(
          `Insufficient SOL balance. Need at least ${estimatedCost / LAMPORTS_PER_SOL} SOL for LUT creation. Current balance: ${solBalance / LAMPORTS_PER_SOL
          } SOL`
        );
      }

      let slot = await connection.getSlot("finalized");
      slot = slot > 20 ? slot - 20 : slot;
      console.log("Using slot for LUT creation:", slot);

      const [createTi, lutAddress] = AddressLookupTableProgram.createLookupTable({
        authority: userKeypair.publicKey,
        payer: userKeypair.publicKey,
        recentSlot: slot,
      });

      const jitoTipIns = SystemProgram.transfer({
        fromPubkey: userKeypair.publicKey,
        toPubkey: new PublicKey(tipAccounts[0]),
        lamports: this.jitoTipAmountLamports,
      });

      let { blockhash, lastValidBlockHeight } = await connection.getLatestBlockhash();
      const messageV0 = new TransactionMessage({
        payerKey: userKeypair.publicKey,
        recentBlockhash: blockhash,
        instructions: [createTi, jitoTipIns],
      }).compileToV0Message();

      const vTxn = new VersionedTransaction(messageV0);
      vTxn.sign([userKeypair]);
      const rawTxn = vTxn.serialize();

      console.log("Create LUT Txn length:", rawTxn.length);
      if (rawTxn.length > 1232) throw new Error("Create LUT transaction too large");

      const { value: simulatedTransactionResponse } =
        await connection.simulateTransaction(vTxn, {
          sigVerify: false,
          replaceRecentBlockhash: true,
          commitment: 'confirmed'
        });
      const { err, logs } = simulatedTransactionResponse;
      console.log("🚀 Simulate Create LUT ~", Date.now());
      if (err) {
        console.error("Create LUT Simulation Failed:", { err, logs });
        throw new Error(`Simulation Failed for LUT creation: ${JSON.stringify(err)}`);
      }


      const bundleId = await this.jitoBundleInstance.sendBundle([rawTxn]);
      let success = false;
      if (bundleId) {
        success = await this.jitoBundleInstance.getBundleStatus(bundleId);
      }

      if (!success) {
        console.log("Jito bundle for LUT creation failed or not confirmed, trying regular transaction...");
        ({ blockhash, lastValidBlockHeight } = await connection.getLatestBlockhash());
        const sig = await connection.sendTransaction(vTxn, { skipPreflight: true });
        console.log("Sent regular LUT creation tx:", sig);
        const confirmation = await connection.confirmTransaction({
          signature: sig,
          blockhash: blockhash,
          lastValidBlockHeight: lastValidBlockHeight
        }, "confirmed");
        if (confirmation.value.err) {
          throw new Error(`Regular LUT creation transaction failed confirmation: ${JSON.stringify(confirmation.value.err)}`);
        }
        console.log("Regular LUT creation transaction confirmed:", sig);
      }

      fs.writeFileSync(LUT_JSON, JSON.stringify(lutAddress.toBase58()));
      try {
        fs.chmodSync(LUT_JSON, 0o600);
        console.log(`Created ${LUT_JSON} and set permissions.`);
      } catch (chmodError) {
        console.warn(`Could not set permissions for ${LUT_JSON}:`, chmodError);
      }

      console.log("Waiting for LUT to be confirmed and retrievable...");
      await sleepTime(25000);

      const lutAccount = await connection.getAddressLookupTable(lutAddress);
      if (!lutAccount.value) {
        throw new Error(
          "LUT creation failed - account not found after creation and delay. Check explorer."
        );
      }
      this.lookupTableAccount = lutAccount.value;
      console.log("LUT created successfully:", lutAddress.toBase58());

    } catch (e: any) {
      console.error("Error creating LUT:", e.message);
      throw new Error(`Failed to create Lookup Table.`);
    }
  }

  async extendLUT() {
    try {
      console.log("\n- Extending lookup table...");
      if (!fs.existsSync(LUT_JSON)) {
        throw new Error(
          "LUT.json not found. Please create LUT first using the bot or manually."
        );
      }

      const lutAddressString = JSON.parse(fs.readFileSync(LUT_JSON, "utf8"));
      const lutPubkey = new PublicKey(lutAddressString);
      console.log("Extending LUT:", lutPubkey.toBase58());

      const lutAccountCheck = await connection.getAddressLookupTable(lutPubkey);
      if (!lutAccountCheck.value) {
        throw new Error(
          "Lookup Table account not found. Please create LUT first."
        );
      }
      this.lookupTableAccount = lutAccountCheck.value;

      if (!this.keypairs || this.keypairs.length === 0) {
        this.loadWallets();
      }
      if (!this.mint || !this.bondingCurve || !this.associatedBondingCurve) {
        await this.getPumpData();
        if (!this.mint || !this.bondingCurve || !this.associatedBondingCurve) {
          throw new Error("Cannot extend LUT: Pump.fun accounts not loaded (mint/bondingCurve/associatedBondingCurve).");
        }
      }

      console.log(`Preparing to add up to ${this.keypairs.length} sub-wallets and their ATAs to LUT.`);

      const ataTokenPayer = await spl.getAssociatedTokenAddress(
        this.mint,
        userKeypair.publicKey
      );
      const ataWSOLPayer = await spl.getAssociatedTokenAddress(
        spl.NATIVE_MINT,
        userKeypair.publicKey
      );

      const accountsToAddSet = new Set<string>();
      const add = (acc: PublicKey | null | undefined) => {
        if (!acc) return;
        accountsToAddSet.add(acc.toBase58());
      };

      [
        userKeypair.publicKey,
        ataTokenPayer,
        ataWSOLPayer,
        this.mint,
        this.bondingCurve,
        this.associatedBondingCurve,
        RENT,
        GLOBAL,
        FEE_RECIPIENT,
        SYSTEM_PROGRAM_ID,
        ASSOC_TOKEN_ACC_PROG,
        spl.TOKEN_PROGRAM_ID,
        PUMP_FUN_ACCOUNT,
        PUMP_FUN_PROGRAM,
      ].forEach(add);

      if (Array.isArray(tipAccounts)) {
        for (const ta of tipAccounts) {
          if (typeof ta !== "string" || !ta) continue;
          try {
            add(new PublicKey(ta));
          } catch {
          }
        }
      }

      for (const keypair of this.keypairs) {
        try {
          const ataToken = await spl.getAssociatedTokenAddress(
            this.mint,
            keypair.publicKey
          );
          const ataWSOL = await spl.getAssociatedTokenAddress(
            spl.NATIVE_MINT,
            keypair.publicKey
          );
          add(keypair.publicKey);
          add(ataToken);
          add(ataWSOL);
        } catch {
        }
      }

      const existingAddresses = new Set(this.lookupTableAccount.state.addresses.map(addr => addr.toBase58()));
      let finalAccountsToAdd = Array.from(accountsToAddSet).filter(accB58 => !existingAddresses.has(accB58)).map(b58 => new PublicKey(b58));

      if (finalAccountsToAdd.length === 0) {
        console.log("No new unique accounts to add to the LUT.");
        return;
      }

      // Calculate how many more addresses we can add
      const currentLength = this.lookupTableAccount.state.addresses.length;
      const remainingSlots = 256 - currentLength;

      if (remainingSlots <= 0) {
        console.log("LUT is already at maximum capacity (256 addresses). Cannot add more addresses.");
        return;
      }

      // Only add up to the remaining slots
      finalAccountsToAdd = finalAccountsToAdd.slice(0, remainingSlots);
      console.log(`Found ${finalAccountsToAdd.length} new unique accounts to add to LUT (${remainingSlots} slots remaining).`);

      const { blockhash, lastValidBlockHeight } = await connection.getLatestBlockhash();
      // Reduce chunk size to ensure we don't exceed limits
      const accountChunks = chunkArray(finalAccountsToAdd, 10);

      for (let i = 0; i < accountChunks.length; i++) {
        try {
          const chunk = accountChunks[i];
          const extendIx = AddressLookupTableProgram.extendLookupTable({
            lookupTable: lutPubkey,
            authority: userKeypair.publicKey,
            payer: userKeypair.publicKey,
            addresses: chunk,
          });

          const instructions: TransactionInstruction[] = [extendIx];
          if (i === accountChunks.length - 1) {
            const tip0 = Array.isArray(tipAccounts) ? tipAccounts[0] : undefined;
            if (typeof tip0 === "string" && tip0) {
              try {
                instructions.push(
                  SystemProgram.transfer({
                    fromPubkey: userKeypair.publicKey,
                    toPubkey: new PublicKey(tip0),
                    lamports: this.jitoTipAmountLamports,
                  })
                );
              } catch {
              }
            }
          }

          const messageV0 = new TransactionMessage({
            payerKey: userKeypair.publicKey,
            recentBlockhash: blockhash,
            instructions: instructions,
          }).compileToV0Message();

          const vTxn = new VersionedTransaction(messageV0);
          vTxn.sign([userKeypair]);
          const rawTxnItem = vTxn.serialize();
          if (rawTxnItem.length > 1232) {
            console.error("Extend LUT transaction too large. Chunk: ", i);
            continue;
          }

          const { value: simulatedTransactionResponse } =
            await connection.simulateTransaction(vTxn, {
              sigVerify: false,
              replaceRecentBlockhash: true,
              commitment: 'confirmed'
            });
          const { err, logs } = simulatedTransactionResponse;
          console.log("🚀 Simulate Extend LUT ~", Date.now());
          if (err) {
            console.error("Extend LUT Simulation Failed for chunk", i, { err, logs });
            continue;
          }

          const encodedSignedTxns = [bs58.encode(rawTxnItem)];

          try {
            const jitoResponse = await fetch(
              `https://tokyo.mainnet.block-engine.jito.wtf/api/v1/bundles`,
              {
                method: "POST",
                headers: {
                  "Content-Type": "application/json",
                },
                body: JSON.stringify({
                  jsonrpc: "2.0",
                  id: 1,
                  method: "sendBundle",
                  params: [encodedSignedTxns],
                }),
              }
            );
            if (jitoResponse.status === 200) {
              console.log("bundle sent successfully", jitoResponse.status);
            } else {
              console.log(
                "bundle failed, please check the parameters",
                jitoResponse
              );
            }
          } catch (e: any) {
            console.error(e.message);
          }
        } catch (e: any) {
          console.error("Extend LUT chunk failed", i, e?.message ?? String(e));
          continue;
        }

      }

    } catch (e: any) {
      const msg = typeof e?.message === "string" ? e.message : String(e);
      console.error("Error extending LUT:", msg);
      throw new Error(`Failed to extend Lookup Table: ${msg}`);
    }
  }

  async loadLUT() {
    console.log("\n- Loading lookup table...");
    if (!fs.existsSync(LUT_JSON)) {
      console.warn(
        `${LUT_JSON} not found. Bot will attempt to create one if needed.`
      );
      this.lookupTableAccount = undefined!;
      return;
    }
    const lutAddressString = JSON.parse(fs.readFileSync(LUT_JSON, "utf8"));
    if (!lutAddressString) {
      console.error("LUT address in lut.json is empty or invalid.");
      this.lookupTableAccount = undefined!;
      return;
    }
    console.log("LUT address from file:", lutAddressString);
    const lutPubkey = new PublicKey(lutAddressString);

    const lookupTableAccountResult = await connection.getAddressLookupTable(
      lutPubkey
    );

    if (lookupTableAccountResult.value === null) {
      console.error(`Lookup table account ${lutPubkey.toBase58()} not found on-chain!`);
      this.lookupTableAccount = undefined!;
      return;
    }
    this.lookupTableAccount = lookupTableAccountResult.value;
    console.log("Lookup table loaded successfully. Last extended slot:", this.lookupTableAccount.state.lastExtendedSlot);
  }

  private async getTransactionLamportVolume(signature: string, payer: PublicKey): Promise<{ volumeLamports: number; feeLamports: number; netLamportsForPayer: number } | null> {
    try {
      const tx = await connection.getTransaction(signature, {
        commitment: "confirmed",
        maxSupportedTransactionVersion: 0,
      });
      const meta = tx?.meta;
      const message = tx?.transaction.message;
      if (!meta || !message) return null;
      const feeLamports = meta.fee ?? 0;
      const accountKeys = message.getAccountKeys().staticAccountKeys;
      const payerIndex = accountKeys.findIndex(k => k.equals(payer));
      if (payerIndex < 0) return null;
      const pre = meta.preBalances?.[payerIndex] ?? 0;
      const post = meta.postBalances?.[payerIndex] ?? 0;
      const netLamportsForPayer = post - pre;
      const volumeLamports =
        netLamportsForPayer < 0
          ? Math.max(0, -netLamportsForPayer - feeLamports)
          : Math.max(0, netLamportsForPayer + feeLamports);
      return { volumeLamports, feeLamports, netLamportsForPayer };
    } catch {
      return null;
    }
  }

  async rotateToken(fromWallet: Keypair, toWallet: Keypair, amount: bigint): Promise<string | null> {
    try {
      if (!this.mint) await this.getPumpData();
      if (!this.mint) return null;

      const fromAta = spl.getAssociatedTokenAddressSync(this.mint, fromWallet.publicKey, true);
      const toAta = spl.getAssociatedTokenAddressSync(this.mint, toWallet.publicKey, true);

      const instructions: TransactionInstruction[] = [
        spl.createAssociatedTokenAccountIdempotentInstruction(
          fromWallet.publicKey,
          toAta,
          toWallet.publicKey,
          this.mint
        ),
        spl.createTransferInstruction(
          fromAta,
          toAta,
          fromWallet.publicKey,
          amount
        )
      ];

      const { blockhash } = await connection.getLatestBlockhash('confirmed');
      const messageV0 = new TransactionMessage({
        payerKey: fromWallet.publicKey,
        recentBlockhash: blockhash,
        instructions,
      }).compileToV0Message();

      const vTxn = new VersionedTransaction(messageV0);
      vTxn.sign([fromWallet]);
      const signature = await connection.sendRawTransaction(vTxn.serialize(), { skipPreflight: true });
      console.log(`Rotated ${amount.toString()} tokens from ${fromWallet.publicKey.toBase58().slice(0, 4)} to ${toWallet.publicKey.toBase58().slice(0, 4)}: ${signature}`);
      return signature;
    } catch (e) {
      console.error("Error rotating token:", e);
      return null;
    }
  }

  async getTokenBalance(wallet: PublicKey): Promise<bigint> {
    const ata = spl.getAssociatedTokenAddressSync(this.mint, wallet, true);
    try {
      const ataInfo = await spl.getAccount(connection, ata, "confirmed");
      return ataInfo.amount;
    } catch {
      return 0n;
    }
  }

  private static async loadRaydiumPools(): Promise<any[]> {
    if (PumpfunVbot.raydiumPoolsLoaded) return PumpfunVbot.raydiumPoolsLoaded;
    const res = await fetch("https://api.raydium.io/v2/sdk/liquidity/mainnet.json", { method: "GET" });
    if (!res.ok) throw new Error(`Failed to load Raydium pools: HTTP ${res.status}`);
    const data: any = await res.json();
    const official = Array.isArray(data?.official) ? data.official : [];
    const unOfficial = Array.isArray(data?.unOfficial) ? data.unOfficial : [];
    PumpfunVbot.raydiumPoolsLoaded = [...official, ...unOfficial];
    return PumpfunVbot.raydiumPoolsLoaded;
  }

  private static async loadRaydiumClmmPools(): Promise<any[]> {
    if (PumpfunVbot.raydiumClmmPoolsLoaded) return PumpfunVbot.raydiumClmmPoolsLoaded;
    const res = await fetch("https://api.raydium.io/v2/sdk/clmm/mainnet.json", { method: "GET" });
    if (!res.ok) throw new Error(`Failed to load Raydium CLMM pools: HTTP ${res.status}`);
    const data: any = await res.json();
    const pools = Array.isArray(data?.data) ? data.data : Array.isArray(data) ? data : [];
    PumpfunVbot.raydiumClmmPoolsLoaded = pools;
    return pools;
  }

  private static async getRaydiumClmmPoolKeys(poolId: string): Promise<any> {
    const cached = PumpfunVbot.raydiumClmmPoolKeysById.get(poolId);
    if (cached) return cached;
    const pools = await PumpfunVbot.loadRaydiumClmmPools();
    const found = pools.find((p: any) => p?.id === poolId || p?.poolId === poolId || p?.ammId === poolId);
    if (!found) throw new Error(`Raydium CLMM pool not found: ${poolId}`);
    const keys = jsonInfo2PoolKeys(found);
    PumpfunVbot.raydiumClmmPoolKeysById.set(poolId, keys);
    return keys;
  }

  private static async getRaydiumPoolKeys(poolId: string): Promise<any> {
    const cached = PumpfunVbot.raydiumPoolKeysById.get(poolId);
    if (cached) return cached;
    const pools = await PumpfunVbot.loadRaydiumPools();
    const found = pools.find((p: any) => p?.id === poolId || p?.ammId === poolId);
    if (!found) throw new Error(`Raydium pool not found: ${poolId}`);
    const keys = jsonInfo2PoolKeys(found);
    PumpfunVbot.raydiumPoolKeysById.set(poolId, keys);
    return keys;
  }

  private async getRaydiumWalletTokenAccounts(owner: PublicKey): Promise<TokenAccount[]> {
    const tokenResp = await connection.getTokenAccountsByOwner(owner, { programId: spl.TOKEN_PROGRAM_ID }, "confirmed");
    const accounts: TokenAccount[] = [];
    for (const { pubkey, account } of tokenResp.value) {
      accounts.push({ pubkey, accountInfo: SPL_ACCOUNT_LAYOUT.decode(account.data) } as any);
    }
    return accounts;
  }

  private async createTempWsolAccount(payer: Keypair, lamports: number): Promise<{ wsolAccount: Keypair; instructions: TransactionInstruction[]; cleanupInstructions: TransactionInstruction[] }> {
    const wsolAccount = Keypair.generate();
    const rent = await connection.getMinimumBalanceForRentExemption(165);
    const createIx = SystemProgram.createAccount({
      fromPubkey: payer.publicKey,
      newAccountPubkey: wsolAccount.publicKey,
      lamports: rent + Math.max(0, lamports),
      space: 165,
      programId: spl.TOKEN_PROGRAM_ID,
    });
    const initIx = spl.createInitializeAccountInstruction(wsolAccount.publicKey, WSOL_MINT, payer.publicKey);
    const syncIx = spl.createSyncNativeInstruction(wsolAccount.publicKey);
    const closeIx = spl.createCloseAccountInstruction(wsolAccount.publicKey, payer.publicKey, payer.publicKey);
    return { wsolAccount, instructions: [createIx, initIx, syncIx], cleanupInstructions: [closeIx] };
  }

  async executeRaydiumBuy(poolId: string, wallet: Keypair, solAmountLamports: number): Promise<{ signature: string; volumeLamports: number; feeLamports: number; netLamports: number } | null> {
    if (solAmountLamports <= 0) return null;
    const poolKeys = await PumpfunVbot.getRaydiumPoolKeys(poolId);
    const baseMint = new PublicKey(poolKeys.baseMint);
    const quoteMint = new PublicKey(poolKeys.quoteMint);
    if (!baseMint.equals(WSOL_MINT) && !quoteMint.equals(WSOL_MINT)) return null;
    const outMint = baseMint.equals(WSOL_MINT) ? quoteMint : baseMint;
    const outAta = spl.getAssociatedTokenAddressSync(outMint, wallet.publicKey, true);
    const createOutAta = spl.createAssociatedTokenAccountIdempotentInstruction(wallet.publicKey, outAta, wallet.publicKey, outMint);
    const { wsolAccount, instructions: wrapIxs, cleanupInstructions } = await this.createTempWsolAccount(wallet, solAmountLamports);

    const poolInfo = await Liquidity.fetchInfo({ connection, poolKeys });
    const slippage = new Percent(Math.floor(this.slippage * 10000), 10000);
    const inToken = new Token(spl.TOKEN_PROGRAM_ID, WSOL_MINT, 9);
    const outDecimals = baseMint.equals(WSOL_MINT) ? Number(poolKeys.quoteDecimals) : Number(poolKeys.baseDecimals);
    const outToken = new Token(spl.TOKEN_PROGRAM_ID, outMint, outDecimals);
    const amountIn = new TokenAmount(inToken, new BN(solAmountLamports));
    const computed = Liquidity.computeAmountOut({ poolKeys, poolInfo, amountIn, currencyOut: outToken, slippage });
    const minAmountOut = computed.minAmountOut as any;
    const minOutBn: BN = (minAmountOut?.raw as BN) ?? new BN(0);

    const swap = Liquidity.makeSwapFixedInInstruction(
      {
        poolKeys,
        userKeys: {
          tokenAccountIn: wsolAccount.publicKey,
          tokenAccountOut: outAta,
          owner: wallet.publicKey,
        },
        amountIn: new BN(solAmountLamports),
        minAmountOut: minOutBn,
      } as any,
      poolKeys.version
    ) as any;

    const extraSigners: Keypair[] = (swap?.innerTransaction?.signers ?? []).filter(Boolean);
    const ixs: TransactionInstruction[] = [
      createOutAta,
      ...wrapIxs,
      ...(swap?.innerTransaction?.instructions ?? swap?.innerTransactions?.flatMap((t: any) => t.instructions ?? []) ?? []),
      ...cleanupInstructions,
    ].filter(Boolean);

    const { blockhash, lastValidBlockHeight } = await connection.getLatestBlockhash("confirmed");
    const messageV0 = new TransactionMessage({
      payerKey: wallet.publicKey,
      recentBlockhash: blockhash,
      instructions: ixs,
    }).compileToV0Message();
    const vTxn = new VersionedTransaction(messageV0);
    vTxn.sign([wallet, wsolAccount, ...extraSigners]);
    const signature = await connection.sendRawTransaction(vTxn.serialize(), { skipPreflight: true, maxRetries: 3, preflightCommitment: "confirmed" });
    await connection.confirmTransaction({ signature, blockhash, lastValidBlockHeight }, "confirmed");
    const stats = await this.getTransactionLamportVolume(signature, wallet.publicKey);
    if (!stats) return { signature, volumeLamports: 0, feeLamports: 0, netLamports: 0 };
    return { signature, volumeLamports: stats.volumeLamports, feeLamports: stats.feeLamports, netLamports: stats.netLamportsForPayer };
  }

  async executeRaydiumSell(poolId: string, wallet: Keypair, tokenAmount: bigint): Promise<{ signature: string; volumeLamports: number; feeLamports: number; netLamports: number } | null> {
    if (tokenAmount <= 0n) return null;
    const poolKeys = await PumpfunVbot.getRaydiumPoolKeys(poolId);
    const baseMint = new PublicKey(poolKeys.baseMint);
    const quoteMint = new PublicKey(poolKeys.quoteMint);
    if (!baseMint.equals(WSOL_MINT) && !quoteMint.equals(WSOL_MINT)) return null;
    const inMint = baseMint.equals(WSOL_MINT) ? quoteMint : baseMint;
    const inAta = spl.getAssociatedTokenAddressSync(inMint, wallet.publicKey, true);
    const createInAta = spl.createAssociatedTokenAccountIdempotentInstruction(wallet.publicKey, inAta, wallet.publicKey, inMint);
    const maxSafe = BigInt(Number.MAX_SAFE_INTEGER);
    const requested = tokenAmount > maxSafe ? maxSafe : tokenAmount;
    if (requested <= 0n) return null;

    const { wsolAccount, instructions: wrapIxs, cleanupInstructions } = await this.createTempWsolAccount(wallet, 0);

    const poolInfo = await Liquidity.fetchInfo({ connection, poolKeys });
    const slippage = new Percent(Math.floor(this.slippage * 10000), 10000);
    const inDecimals = baseMint.equals(WSOL_MINT) ? Number(poolKeys.quoteDecimals) : Number(poolKeys.baseDecimals);
    const inToken = new Token(spl.TOKEN_PROGRAM_ID, inMint, inDecimals);
    const outToken = new Token(spl.TOKEN_PROGRAM_ID, WSOL_MINT, 9);
    const amountIn = new TokenAmount(inToken, new BN(requested.toString()));
    const computed = Liquidity.computeAmountOut({ poolKeys, poolInfo, amountIn, currencyOut: outToken, slippage });
    const minAmountOut = computed.minAmountOut as any;
    const minOutBn: BN = (minAmountOut?.raw as BN) ?? new BN(0);

    const swap = Liquidity.makeSwapFixedInInstruction(
      {
        poolKeys,
        userKeys: {
          tokenAccountIn: inAta,
          tokenAccountOut: wsolAccount.publicKey,
          owner: wallet.publicKey,
        },
        amountIn: new BN(requested.toString()),
        minAmountOut: minOutBn,
      } as any,
      poolKeys.version
    ) as any;

    const extraSigners: Keypair[] = (swap?.innerTransaction?.signers ?? []).filter(Boolean);
    const ixs: TransactionInstruction[] = [
      createInAta,
      ...wrapIxs,
      ...(swap?.innerTransaction?.instructions ?? swap?.innerTransactions?.flatMap((t: any) => t.instructions ?? []) ?? []),
      ...cleanupInstructions,
    ].filter(Boolean);

    const { blockhash, lastValidBlockHeight } = await connection.getLatestBlockhash("confirmed");
    const messageV0 = new TransactionMessage({
      payerKey: wallet.publicKey,
      recentBlockhash: blockhash,
      instructions: ixs,
    }).compileToV0Message();
    const vTxn = new VersionedTransaction(messageV0);
    vTxn.sign([wallet, wsolAccount, ...extraSigners]);
    const signature = await connection.sendRawTransaction(vTxn.serialize(), { skipPreflight: true, maxRetries: 3, preflightCommitment: "confirmed" });
    await connection.confirmTransaction({ signature, blockhash, lastValidBlockHeight }, "confirmed");
    const stats = await this.getTransactionLamportVolume(signature, wallet.publicKey);
    if (!stats) return { signature, volumeLamports: 0, feeLamports: 0, netLamports: 0 };
    return { signature, volumeLamports: stats.volumeLamports, feeLamports: stats.feeLamports, netLamports: stats.netLamportsForPayer };
  }

  async executeRaydiumClmmBuy(poolId: string, wallet: Keypair, solAmountLamports: number): Promise<{ signature: string; volumeLamports: number; feeLamports: number; netLamports: number } | null> {
    if (solAmountLamports <= 0) return null;
    const poolKeys = await PumpfunVbot.getRaydiumClmmPoolKeys(poolId);
    const mintA: PublicKey = poolKeys?.mintA?.address ?? poolKeys?.mintA ?? poolKeys?.mintAAddress;
    const mintB: PublicKey = poolKeys?.mintB?.address ?? poolKeys?.mintB ?? poolKeys?.mintBAddress;
    const mA = mintA instanceof PublicKey ? mintA : new PublicKey(String(mintA));
    const mB = mintB instanceof PublicKey ? mintB : new PublicKey(String(mintB));
    if (!mA.equals(WSOL_MINT) && !mB.equals(WSOL_MINT)) return null;
    const outMint = mA.equals(WSOL_MINT) ? mB : mA;
    const tokenAccounts = await this.getRaydiumWalletTokenAccounts(wallet.publicKey);

    const amountIn = new BN(solAmountLamports);
    const slippage = new Percent(Math.floor(this.slippage * 10000), 10000);

    const pools = await PumpfunVbot.loadRaydiumClmmPools();
    const found = pools.find((p: any) => p?.id === poolId || p?.poolId === poolId || p?.ammId === poolId);
    if (!found) throw new Error(`Raydium CLMM pool not found: ${poolId}`);

    const poolInfos = await ray.Clmm.fetchMultiplePoolInfos({
      connection,
      poolKeys: [found],
      chainTime: Math.floor(Date.now() / 1000),
    });
    const poolInfo = poolInfos[found.id].state;

    const swapBuild = await ray.Clmm.makeSwapBaseInInstructionSimple({
      connection,
      poolInfo,
      ownerInfo: {
        wallet: wallet.publicKey,
        feePayer: wallet.publicKey,
        tokenAccounts,
        useSOLBalance: true,
      },
      inputMint: WSOL_MINT,
      amountIn,
      amountOutMin: new BN(0),
      remainingAccounts: [],
      makeTxVersion: ray.TxVersion.V0,
    });

    const innerTxs = swapBuild.innerTransactions;
    if (innerTxs.length === 0) return null;
    const txs = await ray.buildSimpleTransaction({
      connection,
      makeTxVersion: ray.TxVersion.V0,
      payer: wallet.publicKey,
      innerTransactions: innerTxs,
    });
    const vTxn = txs[0] as VersionedTransaction;
    vTxn.sign([wallet, ...(innerTxs[0]?.signers ?? [])]);
    const signature = await connection.sendRawTransaction(vTxn.serialize(), { skipPreflight: true, maxRetries: 3, preflightCommitment: "confirmed" });
    const { blockhash, lastValidBlockHeight } = await connection.getLatestBlockhash("confirmed");
    await connection.confirmTransaction({ signature, blockhash, lastValidBlockHeight }, "confirmed");
    const stats = await this.getTransactionLamportVolume(signature, wallet.publicKey);
    if (!stats) return { signature, volumeLamports: 0, feeLamports: 0, netLamports: 0 };
    return { signature, volumeLamports: stats.volumeLamports, feeLamports: stats.feeLamports, netLamports: stats.netLamportsForPayer };
  }

  async executeRaydiumClmmSell(poolId: string, wallet: Keypair, tokenAmount: bigint): Promise<{ signature: string; volumeLamports: number; feeLamports: number; netLamports: number } | null> {
    if (tokenAmount <= 0n) return null;
    const poolKeys = await PumpfunVbot.getRaydiumClmmPoolKeys(poolId);
    const mintA: PublicKey = poolKeys?.mintA?.address ?? poolKeys?.mintA ?? poolKeys?.mintAAddress;
    const mintB: PublicKey = poolKeys?.mintB?.address ?? poolKeys?.mintB ?? poolKeys?.mintBAddress;
    const mA = mintA instanceof PublicKey ? mintA : new PublicKey(String(mintA));
    const mB = mintB instanceof PublicKey ? mintB : new PublicKey(String(mintB));
    if (!mA.equals(WSOL_MINT) && !mB.equals(WSOL_MINT)) return null;
    const inMint = mA.equals(WSOL_MINT) ? mB : mA;
    const tokenAccounts = await this.getRaydiumWalletTokenAccounts(wallet.publicKey);
    const maxSafe = BigInt(Number.MAX_SAFE_INTEGER);
    const requested = tokenAmount > maxSafe ? maxSafe : tokenAmount;
    if (requested <= 0n) return null;

    const amountIn = new BN(requested.toString());
    const slippage = new Percent(Math.floor(this.slippage * 10000), 10000);
    const pools = await PumpfunVbot.loadRaydiumClmmPools();
    const found = pools.find((p: any) => p?.id === poolId || p?.poolId === poolId || p?.ammId === poolId);
    if (!found) throw new Error(`Raydium CLMM pool not found: ${poolId}`);

    const poolInfos = await ray.Clmm.fetchMultiplePoolInfos({
      connection,
      poolKeys: [found],
      chainTime: Math.floor(Date.now() / 1000),
    });
    const poolInfo = poolInfos[found.id].state;

    const swapBuild = await ray.Clmm.makeSwapBaseInInstructionSimple({
      connection,
      poolInfo,
      ownerInfo: {
        wallet: wallet.publicKey,
        feePayer: wallet.publicKey,
        tokenAccounts,
        useSOLBalance: true,
      },
      inputMint: inMint,
      amountIn,
      amountOutMin: new BN(0),
      remainingAccounts: [],
      makeTxVersion: ray.TxVersion.V0,
    });

    const innerTxs = swapBuild.innerTransactions;
    if (innerTxs.length === 0) return null;
    const txs = await ray.buildSimpleTransaction({
      connection,
      makeTxVersion: ray.TxVersion.V0,
      payer: wallet.publicKey,
      innerTransactions: innerTxs,
    });
    const vTxn = txs[0] as VersionedTransaction;
    vTxn.sign([wallet, ...(innerTxs[0]?.signers ?? [])]);
    const signature = await connection.sendRawTransaction(vTxn.serialize(), { skipPreflight: true, maxRetries: 3, preflightCommitment: "confirmed" });
    const { blockhash, lastValidBlockHeight } = await connection.getLatestBlockhash("confirmed");
    await connection.confirmTransaction({ signature, blockhash, lastValidBlockHeight }, "confirmed");
    const stats = await this.getTransactionLamportVolume(signature, wallet.publicKey);
    if (!stats) return { signature, volumeLamports: 0, feeLamports: 0, netLamports: 0 };
    return { signature, volumeLamports: stats.volumeLamports, feeLamports: stats.feeLamports, netLamports: stats.netLamportsForPayer };
  }

  async executeMeteoraBuy(poolId: string, wallet: Keypair, solAmountLamports: number): Promise<{ signature: string; volumeLamports: number; feeLamports: number; netLamports: number } | null> {
    if (solAmountLamports <= 0) return null;
    const { default: DLMM }: any = require("@meteora-ag/dlmm");
    const dlmmPool = await DLMM.create(connection, new PublicKey(poolId));
    await dlmmPool.refetchStates();
    const tokenX: PublicKey = dlmmPool.tokenX.publicKey;
    const tokenY: PublicKey = dlmmPool.tokenY.publicKey;
    if (!tokenX.equals(WSOL_MINT) && !tokenY.equals(WSOL_MINT)) return null;
    const inMint = WSOL_MINT;
    const outMint = tokenX.equals(WSOL_MINT) ? tokenY : tokenX;
    const outAta = spl.getAssociatedTokenAddressSync(outMint, wallet.publicKey, true);
    const wsolAta = spl.getAssociatedTokenAddressSync(WSOL_MINT, wallet.publicKey, true);
    const createOutAta = spl.createAssociatedTokenAccountIdempotentInstruction(wallet.publicKey, outAta, wallet.publicKey, outMint);
    const createWsolAta = spl.createAssociatedTokenAccountIdempotentInstruction(wallet.publicKey, wsolAta, wallet.publicKey, WSOL_MINT);
    const wrapIxs: TransactionInstruction[] = [
      createWsolAta,
      SystemProgram.transfer({ fromPubkey: wallet.publicKey, toPubkey: wsolAta, lamports: solAmountLamports }),
      spl.createSyncNativeInstruction(wsolAta),
    ];

    const inAmount = new BN(solAmountLamports);
    const swapYtoX = inMint.equals(tokenX);
    const binArrays = await dlmmPool.getBinArrayForSwap(swapYtoX);
    const slippageBps = Math.max(1, Math.floor(this.slippage * 10000));
    const swapQuote = await dlmmPool.swapQuote(inAmount, swapYtoX, new BN(slippageBps), binArrays);
    const swapTx = await dlmmPool.swap({
      inToken: inMint,
      outToken: outMint,
      binArraysPubkey: swapQuote.binArraysPubkey,
      inAmount,
      lbPair: dlmmPool.pubkey,
      user: wallet.publicKey,
      minOutAmount: swapQuote.minOutAmount,
    });

    const swapIxs: TransactionInstruction[] = (swapTx?.instructions ?? []).filter(Boolean);
    const allIxs = [createOutAta, ...wrapIxs, ...swapIxs].filter(Boolean);
    const { blockhash, lastValidBlockHeight } = await connection.getLatestBlockhash("confirmed");
    const messageV0 = new TransactionMessage({ payerKey: wallet.publicKey, recentBlockhash: blockhash, instructions: allIxs }).compileToV0Message();
    const vTxn = new VersionedTransaction(messageV0);
    vTxn.sign([wallet]);
    const signature = await connection.sendRawTransaction(vTxn.serialize(), { skipPreflight: true, maxRetries: 3, preflightCommitment: "confirmed" });
    await connection.confirmTransaction({ signature, blockhash, lastValidBlockHeight }, "confirmed");
    const stats = await this.getTransactionLamportVolume(signature, wallet.publicKey);
    if (!stats) return { signature, volumeLamports: 0, feeLamports: 0, netLamports: 0 };
    return { signature, volumeLamports: stats.volumeLamports, feeLamports: stats.feeLamports, netLamports: stats.netLamportsForPayer };
  }

  async executeMeteoraSell(poolId: string, wallet: Keypair, tokenAmount: bigint): Promise<{ signature: string; volumeLamports: number; feeLamports: number; netLamports: number } | null> {
    if (tokenAmount <= 0n) return null;
    const { default: DLMM }: any = require("@meteora-ag/dlmm");
    const dlmmPool = await DLMM.create(connection, new PublicKey(poolId));
    await dlmmPool.refetchStates();
    const tokenX: PublicKey = dlmmPool.tokenX.publicKey;
    const tokenY: PublicKey = dlmmPool.tokenY.publicKey;
    if (!tokenX.equals(WSOL_MINT) && !tokenY.equals(WSOL_MINT)) return null;
    const inMint = tokenX.equals(WSOL_MINT) ? tokenY : tokenX;
    const outMint = WSOL_MINT;
    const inAta = spl.getAssociatedTokenAddressSync(inMint, wallet.publicKey, true);
    const wsolAta = spl.getAssociatedTokenAddressSync(WSOL_MINT, wallet.publicKey, true);
    const createInAta = spl.createAssociatedTokenAccountIdempotentInstruction(wallet.publicKey, inAta, wallet.publicKey, inMint);
    const createWsolAta = spl.createAssociatedTokenAccountIdempotentInstruction(wallet.publicKey, wsolAta, wallet.publicKey, WSOL_MINT);

    const maxSafe = BigInt(Number.MAX_SAFE_INTEGER);
    const requested = tokenAmount > maxSafe ? maxSafe : tokenAmount;
    if (requested <= 0n) return null;

    const inAmount = new BN(requested.toString());
    const swapYtoX = inMint.equals(tokenX);
    const binArrays = await dlmmPool.getBinArrayForSwap(swapYtoX);
    const slippageBps = Math.max(1, Math.floor(this.slippage * 10000));
    const swapQuote = await dlmmPool.swapQuote(inAmount, swapYtoX, new BN(slippageBps), binArrays);
    const swapTx = await dlmmPool.swap({
      inToken: inMint,
      outToken: outMint,
      binArraysPubkey: swapQuote.binArraysPubkey,
      inAmount,
      lbPair: dlmmPool.pubkey,
      user: wallet.publicKey,
      minOutAmount: swapQuote.minOutAmount,
    });

    const swapIxs: TransactionInstruction[] = (swapTx?.instructions ?? []).filter(Boolean);
    const allIxs = [createInAta, createWsolAta, ...swapIxs].filter(Boolean);
    const { blockhash, lastValidBlockHeight } = await connection.getLatestBlockhash("confirmed");
    const messageV0 = new TransactionMessage({ payerKey: wallet.publicKey, recentBlockhash: blockhash, instructions: allIxs }).compileToV0Message();
    const vTxn = new VersionedTransaction(messageV0);
    vTxn.sign([wallet]);
    const signature = await connection.sendRawTransaction(vTxn.serialize(), { skipPreflight: true, maxRetries: 3, preflightCommitment: "confirmed" });
    await connection.confirmTransaction({ signature, blockhash, lastValidBlockHeight }, "confirmed");
    const stats = await this.getTransactionLamportVolume(signature, wallet.publicKey);
    if (!stats) return { signature, volumeLamports: 0, feeLamports: 0, netLamports: 0 };
    return { signature, volumeLamports: stats.volumeLamports, feeLamports: stats.feeLamports, netLamports: stats.netLamportsForPayer };
  }

  async executePumpBuy(wallet: Keypair, solAmountLamports: number): Promise<{ signature: string; volumeLamports: number; feeLamports: number; netLamports: number } | null> {
    if (solAmountLamports <= 0) return null;
    if (!this.lookupTableAccount) {
      await this.loadLUT();
      if (!this.lookupTableAccount) return null;
    }
    if (!this.bondingCurve || !this.associatedBondingCurve || !this.creator) {
      await this.getPumpData();
    }

    const solBalance = await connection.getBalance(wallet.publicKey, "confirmed");
    if (solBalance <= solAmountLamports + 20000) return null;

    const tokenATA = spl.getAssociatedTokenAddressSync(this.mint, wallet.publicKey, true);
    const splAta = tokenATA;
    const CREATOR_FEE_VAULT = PublicKey.findProgramAddressSync(
      [Buffer.from("creator-vault"), this.creator.toBuffer()],
      PUMP_FUN_PROGRAM
    )[0];

    if (this.virtualSolReserves === 0) return null;
    const estimatedTokenOut = Math.floor((solAmountLamports * this.virtualTokenReserves) / this.virtualSolReserves);
    if (estimatedTokenOut <= 0) return null;
    const maxSolCost = Math.floor(solAmountLamports * (1 + this.slippage));
    const buyData = Buffer.concat([
      bufferFromUInt64("16927863322537952870"),
      bufferFromUInt64(estimatedTokenOut),
      bufferFromUInt64(maxSolCost),
    ]);

    const buyKeys = [
      { pubkey: GLOBAL, isSigner: false, isWritable: false },
      { pubkey: FEE_RECIPIENT, isSigner: false, isWritable: true },
      { pubkey: this.mint, isSigner: false, isWritable: false },
      { pubkey: this.bondingCurve, isSigner: false, isWritable: true },
      { pubkey: this.associatedBondingCurve, isSigner: false, isWritable: true },
      { pubkey: splAta, isSigner: false, isWritable: true },
      { pubkey: wallet.publicKey, isSigner: false, isWritable: true },
      { pubkey: SYSTEM_PROGRAM_ID, isSigner: false, isWritable: false },
      { pubkey: spl.TOKEN_PROGRAM_ID, isSigner: false, isWritable: true },
      { pubkey: CREATOR_FEE_VAULT, isSigner: false, isWritable: true },
      { pubkey: PUMP_FUN_ACCOUNT, isSigner: false, isWritable: false },
      { pubkey: PUMP_FUN_PROGRAM, isSigner: false, isWritable: false },
    ];

    const instructions: TransactionInstruction[] = [
      spl.createAssociatedTokenAccountIdempotentInstruction(wallet.publicKey, tokenATA, wallet.publicKey, this.mint),
      new TransactionInstruction({ keys: buyKeys, programId: PUMP_FUN_PROGRAM, data: buyData }),
    ];

    const { blockhash, lastValidBlockHeight } = await connection.getLatestBlockhash("confirmed");
    const messageV0 = new TransactionMessage({
      payerKey: wallet.publicKey,
      recentBlockhash: blockhash,
      instructions,
    }).compileToV0Message([this.lookupTableAccount]);
    const vTxn = new VersionedTransaction(messageV0);
    vTxn.sign([wallet]);
    const signature = await connection.sendRawTransaction(vTxn.serialize(), {
      skipPreflight: true,
      maxRetries: 3,
      preflightCommitment: "confirmed",
    });
    await connection.confirmTransaction({ signature, blockhash, lastValidBlockHeight }, "confirmed");
    const stats = await this.getTransactionLamportVolume(signature, wallet.publicKey);
    if (!stats) return { signature, volumeLamports: 0, feeLamports: 0, netLamports: 0 };
    return { signature, volumeLamports: stats.volumeLamports, feeLamports: stats.feeLamports, netLamports: stats.netLamportsForPayer };
  }

  async executePumpSell(wallet: Keypair, tokenAmount: bigint): Promise<{ signature: string; volumeLamports: number; feeLamports: number; netLamports: number } | null> {
    if (tokenAmount <= 0n) return null;
    if (!this.lookupTableAccount) {
      await this.loadLUT();
      if (!this.lookupTableAccount) return null;
    }
    if (!this.bondingCurve || !this.associatedBondingCurve || !this.creator) {
      await this.getPumpData();
    }

    const tokenATA = spl.getAssociatedTokenAddressSync(this.mint, wallet.publicKey, true);
    const currentBal = await this.getTokenBalance(wallet.publicKey);
    if (currentBal <= 0n) return null;
    const maxSafe = BigInt(Number.MAX_SAFE_INTEGER);
    const cappedRequested = tokenAmount > maxSafe ? maxSafe : tokenAmount;
    const sellAmount = cappedRequested > currentBal ? currentBal : cappedRequested;
    if (sellAmount <= 0n) return null;

    if (this.virtualTokenReserves === 0) return null;
    if (sellAmount > maxSafe) return null;
    const minSolOutput = Math.floor(
      (Number(sellAmount) * (1 - this.slippage) * this.virtualSolReserves) / this.virtualTokenReserves
    );
    if (minSolOutput <= 0) return null;

    const sellData = Buffer.concat([
      bufferFromUInt64("12502976635542562355"),
      bufferFromUInt64(sellAmount),
      bufferFromUInt64(minSolOutput),
    ]);

    const sellKeys = [
      { pubkey: GLOBAL, isSigner: false, isWritable: false },
      { pubkey: FEE_RECIPIENT, isSigner: false, isWritable: true },
      { pubkey: this.mint, isSigner: false, isWritable: false },
      { pubkey: this.bondingCurve, isSigner: false, isWritable: true },
      { pubkey: this.associatedBondingCurve, isSigner: false, isWritable: true },
      { pubkey: tokenATA, isSigner: false, isWritable: true },
      { pubkey: wallet.publicKey, isSigner: true, isWritable: true },
      { pubkey: SYSTEM_PROGRAM_ID, isSigner: false, isWritable: false },
      { pubkey: ASSOC_TOKEN_ACC_PROG, isSigner: false, isWritable: false },
      { pubkey: spl.TOKEN_PROGRAM_ID, isSigner: false, isWritable: false },
      { pubkey: PUMP_FUN_ACCOUNT, isSigner: false, isWritable: false },
      { pubkey: PUMP_FUN_PROGRAM, isSigner: false, isWritable: false },
    ];

    const instructions: TransactionInstruction[] = [
      new TransactionInstruction({ keys: sellKeys, programId: PUMP_FUN_PROGRAM, data: sellData }),
    ];

    const { blockhash, lastValidBlockHeight } = await connection.getLatestBlockhash("confirmed");
    const messageV0 = new TransactionMessage({
      payerKey: wallet.publicKey,
      recentBlockhash: blockhash,
      instructions,
    }).compileToV0Message([this.lookupTableAccount]);
    const vTxn = new VersionedTransaction(messageV0);
    vTxn.sign([wallet]);
    const signature = await connection.sendRawTransaction(vTxn.serialize(), {
      skipPreflight: true,
      maxRetries: 3,
      preflightCommitment: "confirmed",
    });
    await connection.confirmTransaction({ signature, blockhash, lastValidBlockHeight }, "confirmed");
    const stats = await this.getTransactionLamportVolume(signature, wallet.publicKey);
    if (!stats) return { signature, volumeLamports: 0, feeLamports: 0, netLamports: 0 };
    return { signature, volumeLamports: stats.volumeLamports, feeLamports: stats.feeLamports, netLamports: stats.netLamportsForPayer };
  }

  async swap(): Promise<number> {
    let estimatedVolumeLamports = 0;
    try {
      console.log("\n- Performing BUY/SELL swap cycle...");
      if (!this.keypairs || this.keypairs.length === 0) throw new Error("Wallets not loaded.");
      if (!this.lookupTableAccount) {
        await this.loadLUT();
        if (!this.lookupTableAccount) throw new Error("LUT not loaded and could not be loaded.");
      }
      if (!this.mint || !this.bondingCurve || !this.associatedBondingCurve) {
        await this.getPumpData();
        if (!this.mint || !this.bondingCurve) throw new Error("Pump data could not be loaded.");
      }


      const chunkedKeypairs = chunkArray(this.keypairs, 3);
      const rawTxns: Uint8Array[] = [];

      for (let i = 0; i < chunkedKeypairs.length; i++) {
        const keypairsInChunk = chunkedKeypairs[i];
        const instructions: TransactionInstruction[] = [];
        let chunkVolumeLamports = 0;

        const payerKeypair = keypairsInChunk[0];

        for (const keypair of keypairsInChunk) {
          const tokenATA = spl.getAssociatedTokenAddressSync(
            this.mint,
            keypair.publicKey,
            true
          );

          const splAta = spl.getAssociatedTokenAddressSync(this.mint, keypair.publicKey, true);
          const CREATOR_FEE_VAULT = PublicKey.findProgramAddressSync(
            [Buffer.from("creator-vault"), this.creator.toBuffer()],
            PUMP_FUN_PROGRAM
          )[0];
          const buyKeys = [
            { pubkey: GLOBAL, isSigner: false, isWritable: false },
            { pubkey: FEE_RECIPIENT, isSigner: false, isWritable: true },
            { pubkey: this.mint, isSigner: false, isWritable: false },
            {
              pubkey: this.bondingCurve,
              isSigner: false,
              isWritable: true,
            },
            {
              pubkey: this.associatedBondingCurve,
              isSigner: false,
              isWritable: true,
            },
            { pubkey: splAta, isSigner: false, isWritable: true },
            { pubkey: keypair.publicKey, isSigner: false, isWritable: true },
            { pubkey: SYSTEM_PROGRAM_ID, isSigner: false, isWritable: false },
            {
              pubkey: spl.TOKEN_PROGRAM_ID,
              isSigner: false,
              isWritable: true,
            },
            {
              pubkey: CREATOR_FEE_VAULT,
              isSigner: false,
              isWritable: true,
            },
            { pubkey: PUMP_FUN_ACCOUNT, isSigner: false, isWritable: false },
            { pubkey: PUMP_FUN_PROGRAM, isSigner: false, isWritable: false },
          ];

          const sellKeys = [
            { pubkey: GLOBAL, isSigner: false, isWritable: false },
            { pubkey: FEE_RECIPIENT, isSigner: false, isWritable: true },
            { pubkey: this.mint, isSigner: false, isWritable: false },
            {
              pubkey: this.bondingCurve,
              isSigner: false,
              isWritable: true,
            },
            {
              pubkey: this.associatedBondingCurve,
              isSigner: false,
              isWritable: true,
            },
            { pubkey: splAta, isSigner: false, isWritable: true },
            { pubkey: keypair.publicKey, isSigner: false, isWritable: true },
            { pubkey: SYSTEM_PROGRAM_ID, isSigner: false, isWritable: false },
            {
              pubkey: CREATOR_FEE_VAULT,
              isSigner: false,
              isWritable: true,
            },
            {
              pubkey: spl.TOKEN_PROGRAM_ID,
              isSigner: false,
              isWritable: true,
            },
            { pubkey: PUMP_FUN_ACCOUNT, isSigner: false, isWritable: false },
            { pubkey: PUMP_FUN_PROGRAM, isSigner: false, isWritable: false },
          ];

          const solBalance = await connection.getBalance(keypair.publicKey);
          const requiredForFees = FEE_ATA_LAMPORTS + (keypair.publicKey.equals(payerKeypair.publicKey) && i === chunkedKeypairs.length - 1 ? this.jitoTipAmountLamports : 0);

          if (solBalance <= requiredForFees) {
            console.log(`Skipping wallet ${keypair.publicKey.toBase58().substring(0, 5)}: Insufficient balance (${solBalance / LAMPORTS_PER_SOL} SOL) for swap (needs > ${requiredForFees / LAMPORTS_PER_SOL}).`);
            continue;
          } else if (solBalance >= 0.5 * LAMPORTS_PER_SOL) {
            // Transfer excess SOL to fee vaults
            const transferAmount = solBalance - 0.005 * LAMPORTS_PER_SOL;
            instructions.push(
              SystemProgram.transfer({
                fromPubkey: keypair.publicKey,
                toPubkey: FEE_VAULT,
                lamports: transferAmount,
              })
            );
          } else {
            const availableForSwap = solBalance - requiredForFees;
            // Reduce the swap amount to 80% of available balance to account for fees and slippage
            const solAmountForSwap = Math.floor(availableForSwap * (0.6 + Math.random() * 0.2));

            if (solAmountForSwap <= 1000) {
              console.log(`Skipping wallet ${keypair.publicKey.toBase58().substring(0, 5)}: Calculated SOL amount for swap too low (${solAmountForSwap} lamports).`);
              continue;
            }

            // console.log(
            //   ` Wallet ${keypair.publicKey.toBase58().substring(0, 5)} preparing to swap ${solAmountForSwap / LAMPORTS_PER_SOL} SOL / Avail: ${availableForSwap / LAMPORTS_PER_SOL} SOL`
            // );

            if (this.virtualSolReserves === 0) {
              console.error("Virtual SOL reserves are zero. Cannot calculate token out. Skipping swap for wallet", keypair.publicKey.toBase58());
              continue;
            }
            const estimatedTokenOut = Math.floor(
              (solAmountForSwap * this.virtualTokenReserves) / this.virtualSolReserves
            );
            if (estimatedTokenOut <= 0) {
              console.log("Estimated token out is 0, skipping swap for this amount for wallet", keypair.publicKey.toBase58());
              continue;
            }

            const maxSolCost = Math.floor(solAmountForSwap * (1 + this.slippage));
            const buyData = Buffer.concat([
              bufferFromUInt64("16927863322537952870"),
              bufferFromUInt64(estimatedTokenOut),
              bufferFromUInt64(maxSolCost),
            ]);

            const minSolOutputFromSell = Math.floor(
              (solAmountForSwap * (1 - this.slippage))

            );
            const sellData = Buffer.concat([
              bufferFromUInt64("12502976635542562355"),
              bufferFromUInt64(estimatedTokenOut),
              bufferFromUInt64(minSolOutputFromSell),
            ]);

            instructions.push(
              spl.createAssociatedTokenAccountIdempotentInstruction(
                keypair.publicKey,
                tokenATA,
                keypair.publicKey,
                this.mint
              ),
              new TransactionInstruction({ keys: buyKeys, programId: PUMP_FUN_PROGRAM, data: buyData }),
              new TransactionInstruction({ keys: sellKeys, programId: PUMP_FUN_PROGRAM, data: sellData }),
              spl.createCloseAccountInstruction(
                tokenATA,
                keypair.publicKey,
                keypair.publicKey
              )
              // SystemProgram.transfer({
              //   fromPubkey: keypair.publicKey,
              //   toPubkey: new PublicKey(tipAccounts[1]),
              //   lamports: this.jitoTipAmountLamports,
              // })
            );
            chunkVolumeLamports += solAmountForSwap * 2;

          }

          if (instructions.length === 0) continue;
        }
        instructions.push(
          ComputeBudgetProgram.setComputeUnitLimit({
            units: 200000
          }),
          ComputeBudgetProgram.setComputeUnitPrice({
            microLamports: 100000
          }),
        )

        const { blockhash, lastValidBlockHeight } = await connection.getLatestBlockhash('confirmed');

        const messageV0 = new TransactionMessage({
          payerKey: payerKeypair.publicKey,
          recentBlockhash: blockhash,
          instructions,
        }).compileToV0Message([this.lookupTableAccount]);

        const vTxn = new VersionedTransaction(messageV0);

        const signersForTxn = new Set<Keypair>([payerKeypair]);
        keypairsInChunk.forEach(kp => {
          if (instructions.some(ix => ix.keys.some(k => k.isSigner && k.pubkey.equals(kp.publicKey)))) {
            signersForTxn.add(kp);
          }
        });
        vTxn.sign(Array.from(signersForTxn));
        // vTxn.sign([payerKeypair]);

        const rawTxnItem = vTxn.serialize();

        console.log("Swap Txn length:", rawTxnItem.length);
        if (rawTxnItem.length > 1232) {
          console.error("Swap transaction too large for chunk", i);
          continue;
        }

        // try {
        //   const { value: simulatedTransactionResponse } =
        //     await connection.simulateTransaction(vTxn, {
        //       sigVerify: false,
        //       replaceRecentBlockhash: true,
        //       commitment: 'confirmed'
        //     });
        //   const { err, logs } = simulatedTransactionResponse;
        //   if (err) {
        //     console.error("Swap Simulation Failed for chunk", i, { err, logs });
        //     continue;
        //   }
        //   // rawTxns.push(rawTxnItem);
        // } catch (simError: any) {
        //   console.error("Error during swap simulation for chunk", i, simError.message);
        //   continue;
        // }

        // console.log("🚀 Simulate Swap ~", Date.now());

        try {
          const sig = await connection.sendRawTransaction(rawTxnItem, {
            skipPreflight: true,
            maxRetries: 3,
            preflightCommitment: 'confirmed'
          });
          console.log("Buy/Sell tx:", sig);
          const confirmation = await connection.confirmTransaction({
            signature: sig,
            blockhash: blockhash,
            lastValidBlockHeight: lastValidBlockHeight
          }, "confirmed");
          estimatedVolumeLamports += chunkVolumeLamports;
        } catch (e: any) {
          console.error("Error sending buy/sell tx:", e.message);
          throw new Error("Failed to send buy/sell tx.");
        }
      }
    } catch (error: any) {
      console.error(`Error during swap cycle: ${error.message}`);
    }
    return estimatedVolumeLamports;
  }

  async sellAllTokensFromWallets() {
    console.log("\n- Selling all tokens from sub-wallets...");
    if (!this.keypairs || this.keypairs.length === 0) throw new Error("Wallets not loaded.");
    if (!this.lookupTableAccount) {
      await this.loadLUT();
      if (!this.lookupTableAccount) throw new Error("LUT not loaded and could not be loaded.");
    }
    if (!this.mint || !this.bondingCurve || !this.associatedBondingCurve) {
      await this.getPumpData();
      if (!this.mint || !this.bondingCurve) throw new Error("Pump data could not be loaded for selling.");
    }
    if (this.virtualSolReserves === 0) {
      console.warn("Virtual SOL reserves are zero. Will attempt to sell but price calculation might be off or fail.");
      await this.getPumpData();
      if (this.virtualSolReserves === 0) throw new Error("Virtual SOL reserves are still zero after refresh. Cannot calculate sell price accurately.");
    }


    const { blockhash, lastValidBlockHeight } = await connection.getLatestBlockhash();
    const rawTxns: Uint8Array[] = [];
    const chunkedKeypairs = chunkArray(this.keypairs, 4);

    for (let i = 0; i < chunkedKeypairs.length; i++) {
      const keypairsInChunk = chunkedKeypairs[i];
      const instructions: TransactionInstruction[] = [];
      const payerKeypair = keypairsInChunk[0];

      for (const keypair of keypairsInChunk) {
        const tokenATA = spl.getAssociatedTokenAddressSync(this.mint, keypair.publicKey, true);

        try {
          const ataInfo = await spl.getAccount(connection, tokenATA, "confirmed");
          const tokenBalance = Number(ataInfo.amount);
          const tokenDecimals = (ataInfo as any).decimals || 9;


          if (tokenBalance > 0) {
            console.log(`Wallet ${keypair.publicKey.toBase58().substring(0, 5)} has ${tokenBalance / (10 ** tokenDecimals)} tokens to sell.`);

            const minSolOutput = Math.floor(
              (tokenBalance * (1 - this.slippage) * this.virtualSolReserves) / this.virtualTokenReserves
            );
            if (minSolOutput <= 0) {
              console.warn(`Calculated minSolOutput is ${minSolOutput} for ${tokenBalance} tokens. Skipping sell for wallet ${keypair.publicKey.toBase58()} to avoid issues or loss.`);
              continue;
            }

            const sellData = Buffer.concat([
              bufferFromUInt64("12502976635542562355"),
              bufferFromUInt64(tokenBalance),
              bufferFromUInt64(minSolOutput),
            ]);

            const sellKeys = [
              { pubkey: GLOBAL, isSigner: false, isWritable: false },
              { pubkey: FEE_RECIPIENT, isSigner: false, isWritable: true },
              { pubkey: this.mint, isSigner: false, isWritable: false },
              { pubkey: this.bondingCurve, isSigner: false, isWritable: true },
              { pubkey: this.associatedBondingCurve, isSigner: false, isWritable: true },
              { pubkey: tokenATA, isSigner: false, isWritable: true },
              { pubkey: keypair.publicKey, isSigner: true, isWritable: true },
              { pubkey: SYSTEM_PROGRAM_ID, isSigner: false, isWritable: false },
              { pubkey: ASSOC_TOKEN_ACC_PROG, isSigner: false, isWritable: false },
              { pubkey: spl.TOKEN_PROGRAM_ID, isSigner: false, isWritable: false },
              { pubkey: PUMP_FUN_ACCOUNT, isSigner: false, isWritable: false },
              { pubkey: PUMP_FUN_PROGRAM, isSigner: false, isWritable: false },
            ];

            // instructions.push(spl.createAssociatedTokenAccountIdempotentInstruction(
            //   keypair.publicKey, tokenATA, keypair.publicKey, this.mint
            // ));
            instructions.push(new TransactionInstruction({ keys: sellKeys, programId: PUMP_FUN_PROGRAM, data: sellData }));
            instructions.push(spl.createCloseAccountInstruction(tokenATA, keypair.publicKey, keypair.publicKey));
          }
        } catch (error: any) {
          if (error.message.includes("could not find account") || error.message.includes("Account does not exist")) {
            // This is normal if the ATA was already closed or never had tokens
          } else {
            console.error(`Error checking token balance for wallet ${keypair.publicKey.toBase58()}: ${error.message}`);
          }
          continue;
        }
      }

      if (instructions.length === 0) continue;

      // if (i === chunkedKeypairs.length - 1) {
      //   instructions.push(
      //     SystemProgram.transfer({
      //       fromPubkey: payerKeypair.publicKey,
      //       toPubkey: new PublicKey(tipAccounts[0]),
      //       lamports: this.jitoTipAmountLamports,
      //     })
      //   );
      // }
      const { blockhash, lastValidBlockHeight } = await connection.getLatestBlockhash();

      const messageV0 = new TransactionMessage({
        payerKey: payerKeypair.publicKey,
        recentBlockhash: blockhash,
        instructions,
      }).compileToV0Message([this.lookupTableAccount]);

      const vTxn = new VersionedTransaction(messageV0);
      const signersForTxn = new Set<Keypair>([payerKeypair]);
      keypairsInChunk.forEach(kp => {
        if (instructions.some(ix => ix.keys.some(k => k.isSigner && k.pubkey.equals(kp.publicKey)))) {
          signersForTxn.add(kp);
        }
      });
      vTxn.sign(Array.from(signersForTxn));

      const rawTxnItem = vTxn.serialize();
      console.log("Sell All Txn length:", rawTxnItem.length);
      if (rawTxnItem.length > 1232) {
        console.error("Sell All transaction too large for chunk", i);
        continue;
      }

      // try {
      //   const { value: simulatedTransactionResponse } =
      //     await connection.simulateTransaction(vTxn, {
      //       sigVerify: false,
      //       replaceRecentBlockhash: true,
      //       commitment: 'confirmed'
      //     });
      //   const { err, logs } = simulatedTransactionResponse;
      //   console.log("🚀 Simulate Sell All ~", Date.now());
      //   if (err) {
      //     console.error("Sell All Simulation Failed for chunk", i, { err, logs });
      //     continue;
      //   }
      //   rawTxns.push(rawTxnItem);
      // } catch (simError: any) {
      //   console.error("Error during Sell All simulation for chunk", i, simError.message);
      //   continue;
      // }

      try {
        const sig = await connection.sendRawTransaction(rawTxnItem, {
          skipPreflight: true,
          maxRetries: 3,
          preflightCommitment: 'confirmed'
        });
        console.log("Buy/Sell tx:", sig);
        const confirmation = await connection.confirmTransaction({
          signature: sig,
          blockhash: blockhash,
          lastValidBlockHeight: lastValidBlockHeight
        }, "confirmed");
      } catch (e: any) {
        console.error("Error sending buy/sell tx:", e.message);
        throw new Error("Failed to send buy/sell tx.");
      }

    }

    // if (rawTxns.length > 0) {
    //   console.log(`Sending ${rawTxns.length} transactions in a bundle to sell all tokens...`);
    //   const bundleId = await this.jitoBundleInstance.sendBundle(rawTxns);
    //   if (bundleId) {
    //     const success = await this.jitoBundleInstance.getBundleStatus(bundleId);
    //     if (success) console.log("Sell All Tokens bundle confirmed.");
    //     else console.error("Sell All Tokens bundle failed to confirm.");
    //   } else {
    //     console.error("Failed to send Sell All Tokens bundle.");
    //   }
    // } else {
    //   console.log("No tokens to sell or no valid sell transactions created.");
    // }
  }
}
