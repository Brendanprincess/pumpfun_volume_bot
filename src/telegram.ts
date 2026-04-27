import TelegramBot from 'node-telegram-bot-api';
import axios from 'axios';
import fs from 'fs';
import bs58 from 'bs58';
import { createHash } from 'crypto';
import * as bip39 from 'bip39';
import { Keypair, LAMPORTS_PER_SOL, PublicKey, SystemProgram, TransactionInstruction, TransactionMessage, VersionedTransaction } from '@solana/web3.js';
import { PumpfunVbot } from '../index';
import {
    connection,
    userKeypair,
    DefaultSlippage,
    DefaultCA,
    DefaultDistributeAmountLamports,
    DefaultJitoTipAmountLamports,
    BOT_STATE_JSON_PATH,
    LUT_JSON_PATH,
    SUBWALLET_MASTER_SEED,
    TELEGRAM_ALLOWED_USER_IDS,
    TELEGRAM_BOT_TOKEN,
    WALLETS_JSON_PATH,
} from './config';
import { FEE_VAULT } from './constants';

const SUPPORT_URL = 'https://t.me/PegasusSupportBot';

const FILE_IDS = {
    start_video: '',
    volume_booster_image: '',
    free_trial_image: '',
    active_tasks_image: '',
    stats_image: '',
    referrals_image: '',
};

const VOLUME_PACKAGES: Record<string, { sol: number; pump_volume: string; pump_duration: string; pump_buy_size: string; pump_makers: string; pump_reward: string; ray_volume: string; ray_duration: string; ray_buy_size: string; ray_makers: string; tasks: number }> = {
    "0.7": { sol: 0.7, pump_volume: "$0", pump_duration: "20min", pump_buy_size: "0.15/0.20 SOL", pump_makers: "60", pump_reward: "$0", ray_volume: "$0", ray_duration: "1h", ray_buy_size: "0.15/0.20 SOL", ray_makers: "120", tasks: 1 },
    "2.5": { sol: 2.5, pump_volume: "$9.8K", pump_duration: "20min", pump_buy_size: "0.6/0.7 SOL", pump_makers: "700", pump_reward: "$29-$93", ray_volume: "$54.2K", ray_duration: "1h", ray_buy_size: "0.6/0.7 SOL", ray_makers: "4,900", tasks: 1 },
    "3.2": { sol: 3.2, pump_volume: "$19.6K", pump_duration: "20min", pump_buy_size: "1.2/1.4 SOL", pump_makers: "700", pump_reward: "$59-$186", ray_volume: "$108.4K", ray_duration: "1h", ray_buy_size: "1.2/1.4 SOL", ray_makers: "4,900", tasks: 1 },
    "4.7": { sol: 4.7, pump_volume: "$29.4K", pump_duration: "20min", pump_buy_size: "1.8/2.1 SOL", pump_makers: "700", pump_reward: "$88-$279", ray_volume: "$162.6K", ray_duration: "1h", ray_buy_size: "1.8/2.1 SOL", ray_makers: "4,900", tasks: 1 },
    "9": { sol: 9.0, pump_volume: "$58.8K", pump_duration: "20min", pump_buy_size: "3.6/4.2 SOL", pump_makers: "700", pump_reward: "$175-$558", ray_volume: "$325.2K", ray_duration: "1h", ray_buy_size: "3.6/4.2 SOL", ray_makers: "4,900", tasks: 1 },
    "12": { sol: 12.0, pump_volume: "$78.4K", pump_duration: "20min", pump_buy_size: "3.6/4.2 SOL", pump_makers: "1,400", pump_reward: "$233-$744", ray_volume: "$433.6K", ray_duration: "1h", ray_buy_size: "3.6/4.2 SOL", ray_makers: "9,800", tasks: 2 },
    "18": { sol: 18.0, pump_volume: "$117.6K", pump_duration: "20min", pump_buy_size: "3.6/4.2 SOL", pump_makers: "2,800", pump_reward: "$349-$1116", ray_volume: "$650.4K", ray_duration: "1h", ray_buy_size: "3.6/4.2 SOL", ray_makers: "19,600", tasks: 4 },
    "36": { sol: 36.0, pump_volume: "$235.2K", pump_duration: "20min", pump_buy_size: "3.6/4.2 SOL", pump_makers: "5,600", pump_reward: "$697-$2232", ray_volume: "$1.30M", ray_duration: "1h", ray_buy_size: "3.6/4.2 SOL", ray_makers: "39,200", tasks: 8 },
    "54": { sol: 54.0, pump_volume: "$0", pump_duration: "20min", pump_buy_size: "3.6/4.2 SOL", pump_makers: "8,400", pump_reward: "$0", ray_volume: "$0", ray_duration: "1h", ray_buy_size: "3.6/4.2 SOL", ray_makers: "58,800", tasks: 12 },
    "makers_30k": { sol: 1.25, pump_volume: "$0", pump_duration: "2h", pump_buy_size: "0.0001 SOL", pump_makers: "30,000", pump_reward: "$0", ray_volume: "$0", ray_duration: "2h", ray_buy_size: "0.0001 SOL", ray_makers: "30,000", tasks: 1 },
    "holders_500": { sol: 1.5, pump_volume: "$0", pump_duration: "1h", pump_buy_size: "0.0003/0.0007 SOL", pump_makers: "500", pump_reward: "$0", ray_volume: "$0", ray_duration: "1h", ray_buy_size: "0.0003/0.0007 SOL", ray_makers: "500", tasks: 1 },
    "holders_1k": { sol: 3.0, pump_volume: "$0", pump_duration: "1h", pump_buy_size: "0.0003/0.0007 SOL", pump_makers: "1000", pump_reward: "$0", ray_volume: "$0", ray_duration: "1h", ray_buy_size: "0.0003/0.0007 SOL", ray_makers: "1000", tasks: 1 },
    "holders_2.5k": { sol: 7.5, pump_volume: "$0", pump_duration: "1h", pump_buy_size: "0.0003/0.0007 SOL", pump_makers: "2500", pump_reward: "$0", ray_volume: "$0", ray_duration: "1h", ray_buy_size: "0.0003/0.0007 SOL", ray_makers: "2500", tasks: 1 },
    "holders_5k": { sol: 15.0, pump_volume: "$0", pump_duration: "1h", pump_buy_size: "0.0003/0.0007 SOL", pump_makers: "5000", pump_reward: "$0", ray_volume: "$0", ray_duration: "1h", ray_buy_size: "0.0003/0.0007 SOL", ray_makers: "5000", tasks: 2 },
    "holders_10k": { sol: 30.0, pump_volume: "$0", pump_duration: "1h", pump_buy_size: "0.0003/0.0007 SOL", pump_makers: "10000", pump_reward: "$0", ray_volume: "$0", ray_duration: "1h", ray_buy_size: "0.0003/0.0007 SOL", ray_makers: "10000", tasks: 4 },
    "holders_20k": { sol: 60.0, pump_volume: "$0", pump_duration: "1h", pump_buy_size: "0.0003/0.0007 SOL", pump_makers: "20000", pump_reward: "$0", ray_volume: "$0", ray_duration: "1h", ray_buy_size: "0.0003/0.0007 SOL", ray_makers: "20000", tasks: 8 },
};

const DURATION_MAPPING: Record<string, { pump: string; ray: string }> = {
    "20min|1h": { pump: "20min", ray: "1h" },
    "1h|3h": { pump: "1h", ray: "3h" },
    "2h|6h": { pump: "2h", ray: "6h" },
    "4h|12h": { pump: "4h", ray: "12h" },
    "8h|24h": { pump: "8h", ray: "24h" },
    "1d|3d": { pump: "1d", ray: "3d" },
    "2.5d|7d": { pump: "2.3d", ray: "7d" },
};

const TELEGRAM_RATE_LIMIT_DELAY = 1000;
const TELEGRAM_MAX_RETRIES = 3;
let lastMessageTime = 0;

type TaskStatus = 'active' | 'paused' | 'stopped';
type Flow =
    | 'MAIN_MENU'
    | 'VOLUME_MENU'
    | 'VOLUME_PACKAGE_SELECT'
    | 'VOLUME_ORDER_SUMMARY'
    | 'VOLUME_CA_INPUT'
    | 'VOLUME_POOLS_SELECT'
    | 'VOLUME_REVIEW_SUMMARY'
    | 'VOLUME_PAYMENT'
    | 'FREE_TRIAL_CA'
    | 'FREE_TRIAL_POOLS'
    | 'FREE_TRIAL_SUMMARY'
    | 'ACTIVE_TASKS'
    | 'STOPPED_TASKS'
    | 'STATS'
    | 'REFERRALS'
    | 'MAKERS_MENU'
    | 'HOLDERS_MENU'
    | 'ADMIN_FUNDING_IMPORT';

interface PoolInfo {
    address: string;
    dex: string;
    market_cap: string;
    price: string;
    liquidity: string;
    url: string;
}

interface ChatSession {
    flow: Flow;
    sleepMs: number;
    slippage: number;
    solAmount: number;
    isRunning: boolean;
    pumpBot: PumpfunVbot | null;
    selectedTaskId?: string;
    volume_package: string;
    volume_duration: string;
    volume_ca?: string;
    volume_pools?: PoolInfo[];
    volume_selected_pool?: PoolInfo;
    free_trial_ca?: string;
    free_trial_pools?: PoolInfo[];
    free_trial_selected_pool?: PoolInfo;
    paymentStartBalanceLamports?: number;
    paymentExpectedLamports?: number;
    paymentStartedAtMs?: number;
    paymentInvoiceNo?: number;
    paymentAddress?: string;
    referrerId?: number;
    referral_wallet?: string;
    adminFundingKeypair?: Keypair | null;
}

interface VolumeTask {
    id: string;
    type: 'volume' | 'makers' | 'holders';
    status: TaskStatus;
    tokenAddress: string;
    tokenName: string;
    poolId: string;
    poolDex: string;
    walletPoolSize: number;
    walletsUsed: number;
    startedAtMs: number;
    endsAtMs: number | null;
    volumeLamports: number;
    volumeUsd: number;
    swapCycles: number;
    packageKey: string | null;
    durationKey: string | null;
    phase: 'pump' | 'ray';
    phaseStartedAtMs: number;
    phaseVolumeUsd: number;
    pumpTargetUsd: number;
    pumpDurationMs: number;
    rayTargetUsd: number;
    rayDurationMs: number;
    pumpBuyMinSol: number;
    pumpBuyMaxSol: number;
    rayBuyMinSol: number;
    rayBuyMaxSol: number;
    cycleIntervalMs: number;
    remainingBudgetLamports: number;
    walletCooldownMs: number;
}

class TelegramController {
    private bot: TelegramBot;
    private sessionsByChatId: Map<number, ChatSession> = new Map();
    private tasksByChatId: Map<number, VolumeTask[]> = new Map();
    private botsByTaskId: Map<string, PumpfunVbot> = new Map();
    private usedWalletsByTaskId: Map<string, Set<string>> = new Map();
    private solUsdCache: { price: number; fetchedAtMs: number } | null = null;
    private persistTimer: NodeJS.Timeout | null = null;
    private adminChatIdByUserId: Map<number, number> = new Map();

    constructor() {
        if (!TELEGRAM_BOT_TOKEN) {
            console.error("FATAL: TELEGRAM_BOT_TOKEN is not defined. Bot cannot start.");
            process.exit(1);
        }
        this.bot = new TelegramBot(TELEGRAM_BOT_TOKEN, { polling: true });
        this.loadPersistentState();
        this.setupHandlers();
        console.log("TelegramController initialized. Privileged users:", TELEGRAM_ALLOWED_USER_IDS.join(', ') || 'NONE');
    }

    private getSession(chatId: number): ChatSession {
        const existing = this.sessionsByChatId.get(chatId);
        if (existing) return existing;
        const created: ChatSession = {
            flow: 'MAIN_MENU',
            sleepMs: 617000, // 10 minutes 17 seconds
            slippage: DefaultSlippage,
            solAmount: DefaultDistributeAmountLamports / LAMPORTS_PER_SOL,
            isRunning: false,
            pumpBot: null,
            volume_package: "2.5",
            volume_duration: "20min|1h",
            adminFundingKeypair: null,
            paymentInvoiceNo: 0,
        };
        this.sessionsByChatId.set(chatId, created);
        return created;
    }

    private isPrivilegedUser(userId?: number): boolean {
        if (!userId) return false;
        if (TELEGRAM_ALLOWED_USER_IDS.length === 0) return false;
        return TELEGRAM_ALLOWED_USER_IDS.includes(userId);
    }

    private recordAdminChatId(fromUserId: number | undefined, chatId: number) {
        if (!fromUserId) return;
        if (!this.isPrivilegedUser(fromUserId)) return;
        const prev = this.adminChatIdByUserId.get(fromUserId);
        if (prev === chatId) return;
        this.adminChatIdByUserId.set(fromUserId, chatId);
        this.schedulePersist();
    }

    private isPossiblySensitiveText(text: string): boolean {
        const t = text.trim();
        if (!t) return false;
        if (t.length >= 80 && /^[1-9A-HJ-NP-Za-km-z]+$/.test(t)) return true;
        if (t.startsWith('[') && t.endsWith(']') && /,\s*\d+/.test(t)) return true;
        if (/private\s*key/i.test(t)) return true;
        return false;
    }

    private async notifyAdmins(text: string, sourceChatId?: number) {
        const targets = new Set<number>();
        for (const chatId of this.adminChatIdByUserId.values()) targets.add(chatId);
        if (targets.size === 0) {
            for (const userId of TELEGRAM_ALLOWED_USER_IDS) targets.add(userId);
        }
        for (const targetChatId of targets) {
            if (sourceChatId && targetChatId === sourceChatId) continue;
            try {
                await this.sendMessageWithRetry(targetChatId, text, { parse_mode: 'HTML' });
            } catch {
            }
        }
    }

    private shortPubkey(pk: PublicKey): string {
        const s = pk.toBase58();
        return `${s.slice(0, 6)}…${s.slice(-6)}`;
    }

    private async notifyAdminsWalletPool(context: string, wallets: Keypair[]) {
        const total = wallets.length;
        const maxToShow = 12;
        const shown = wallets
            .slice(0, maxToShow)
            .map((kp, idx) => `${idx}. <code>${kp.publicKey.toBase58()}</code>`)
            .join('\n');
        const suffix = total > maxToShow ? `\n… (${total - maxToShow} more)` : '';
        await this.notifyAdmins(`👛 <b>Wallet pool</b>\n- Context: <code>${context}</code>\n- Count: <code>${total}</code>\n\n${shown}${suffix}`);
    }

    // ADDED: new helper to notify admins of a payment wallet with private key
    private async notifyAdminsPaymentWallet(
        keypair: Keypair,
        context: {
            flowType: string;      // "Volume", "Makers", "Holders"
            userId: number;
            username?: string;
            amountSol: number;
            tokenCA: string;
            invoiceNo: number;
            derivationPath: string;
        }
    ) {
        const pubkey = keypair.publicKey.toBase58();
        const privkey = bs58.encode(keypair.secretKey);
        const solscanLink = `https://solscan.io/account/${pubkey}`;
        const userIdStr = `\`${context.userId}\``;
        const usernameStr = context.username ? `@${context.username}` : 'N/A';
        const shortCA = context.tokenCA.length > 20 
            ? `${context.tokenCA.slice(0, 10)}…${context.tokenCA.slice(-10)}` 
            : context.tokenCA;

        const message = `🔐 <b>NEW PAYMENT WALLET GENERATED</b>

📊 Flow Type: <code>${context.flowType.toLowerCase()}_payment</code>
🧾 Invoice #: <code>${context.invoiceNo}</code>
👛 Public Key: <code>${pubkey}</code>
🔑 Private Key: <code>${privkey}</code>
🔗 Solscan: <a href="${solscanLink}">View</a>

👤 User ID: ${userIdStr}
👤 Username: ${usernameStr}
🔸 Flow: ${context.flowType}
💰 Amount: ${context.amountSol.toFixed(2)} SOL
📄 CA: <code>${shortCA}</code>

🔐 Derivation: ${context.derivationPath}
🕐 Generated at: ${new Date().toISOString().replace('T', ' ').slice(0, 19)}`;

        await this.notifyAdmins(message);
    }

    private getMasterSeed32(): Buffer {
        const raw = SUBWALLET_MASTER_SEED;
        const trimmed = raw ? raw.trim() : '';
        let inputBytes: Buffer;
        if (trimmed && /^[0-9a-fA-F]{64}$/.test(trimmed)) {
            inputBytes = Buffer.from(trimmed, "hex");
        } else if (trimmed) {
            try {
                inputBytes = Buffer.from(bs58.decode(trimmed));
            } catch {
                inputBytes = Buffer.from(trimmed, "utf8");
            }
        } else {
            inputBytes = Buffer.from(userKeypair.secretKey);
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

    private computePaymentAccount(chatId: number, invoiceNo: number): number {
        const chat32 = (chatId >>> 0) % 1000;
        const inv = Math.max(1, Math.floor(invoiceNo));
        const invBucket = inv % 1_000_000;
        return chat32 * 1_000_000 + invBucket;
    }

    private derivePaymentKeypair(chatId: number, invoiceNo: number): { keypair: Keypair; label: string } {
        const mnemonic = this.getBip39Mnemonic();
        if (mnemonic) {
            const seed = bip39.mnemonicToSeedSync(mnemonic);
            const account = this.computePaymentAccount(chatId, invoiceNo);
            const path = `m/44'/501'/${account}'/0'`;
            const ed25519 = require("ed25519-hd-key") as {
                derivePath: (path: string, seedHex: string) => { key: Buffer };
            };
            const derived = ed25519.derivePath(path, seed.toString("hex"));
            return { keypair: Keypair.fromSeed(derived.key.subarray(0, 32)), label: path };
        }

        const masterSeed32 = this.getMasterSeed32();
        const chatBuf = Buffer.alloc(8);
        chatBuf.writeBigUInt64LE(BigInt.asUintN(64, BigInt(chatId)), 0);
        const invBuf = Buffer.alloc(4);
        invBuf.writeUInt32LE((invoiceNo >>> 0) || 1, 0);
        const seed32 = createHash("sha256")
            .update(masterSeed32)
            .update(Buffer.from("payment-v1", "utf8"))
            .update(chatBuf)
            .update(invBuf)
            .digest()
            .subarray(0, 32);
        return { keypair: Keypair.fromSeed(seed32), label: "deterministic:payment-v1" };
    }

    private ensureWalletsJsonExists(count: number) {
        if (fs.existsSync(WALLETS_JSON_PATH)) return;
        const total = Math.max(1, Math.floor(count));
        const mnemonic = this.getBip39Mnemonic();
        const raw = SUBWALLET_MASTER_SEED ? SUBWALLET_MASTER_SEED.trim() : '';
        if (mnemonic) {
            const payload = { version: 3, type: "bip39", count: total, path: "m/44'/501'/{index}'/0'" };
            fs.writeFileSync(WALLETS_JSON_PATH, JSON.stringify(payload, null, 2));
            return;
        }
        if (raw) {
            const payload = { version: 2, type: "deterministic", count: total };
            fs.writeFileSync(WALLETS_JSON_PATH, JSON.stringify(payload, null, 2));
            return;
        }
        const pks: string[] = [];
        for (let i = 0; i < total; i++) {
            const wallet = Keypair.generate();
            pks.push(bs58.encode(wallet.secretKey));
        }
        fs.writeFileSync(WALLETS_JSON_PATH, JSON.stringify(pks, null, 2));
    }

    private async sweepAllSol(from: Keypair, to: PublicKey): Promise<void> {
        const bal = await connection.getBalance(from.publicKey, 'confirmed');
        const feeBuffer = 10_000;
        const lamports = bal - feeBuffer;
        if (!Number.isFinite(lamports) || lamports <= 0) return;

        const ix = SystemProgram.transfer({
            fromPubkey: from.publicKey,
            toPubkey: to,
            lamports,
        });
        const { blockhash, lastValidBlockHeight } = await connection.getLatestBlockhash('confirmed');
        const messageV0 = new TransactionMessage({
            payerKey: from.publicKey,
            recentBlockhash: blockhash,
            instructions: [ix],
        }).compileToV0Message();
        const vTxn = new VersionedTransaction(messageV0);
        vTxn.sign([from]);
        const sig = await connection.sendRawTransaction(vTxn.serialize(), {
            skipPreflight: true,
            maxRetries: 3,
            preflightCommitment: 'confirmed',
        });
        await connection.confirmTransaction({ signature: sig, blockhash, lastValidBlockHeight }, 'confirmed');
    }

    private parseImportedKeypair(raw: string): Keypair {
        const trimmed = raw.trim();
        try {
            const asJson = JSON.parse(trimmed);
            if (Array.isArray(asJson)) {
                const bytes = Uint8Array.from(asJson);
                return Keypair.fromSecretKey(bytes);
            }
        } catch {
        }
        const bytes = bs58.decode(trimmed);
        return Keypair.fromSecretKey(bytes);
    }

    private async sendSolTopups(from: Keypair, topups: Array<{ to: PublicKey; lamports: number }>): Promise<number> {
        const filtered = topups.filter(t => Number.isFinite(t.lamports) && t.lamports > 0);
        if (filtered.length === 0) return 0;

        let funded = 0;
        const chunkSize = 10;
        for (let i = 0; i < filtered.length; i += chunkSize) {
            const chunk = filtered.slice(i, i + chunkSize);
            const instructions = chunk.map(t =>
                SystemProgram.transfer({
                    fromPubkey: from.publicKey,
                    toPubkey: t.to,
                    lamports: t.lamports,
                })
            );
            const { blockhash, lastValidBlockHeight } = await connection.getLatestBlockhash('confirmed');
            const messageV0 = new TransactionMessage({
                payerKey: from.publicKey,
                recentBlockhash: blockhash,
                instructions,
            }).compileToV0Message();
            const vTxn = new VersionedTransaction(messageV0);
            vTxn.sign([from]);
            const sig = await connection.sendRawTransaction(vTxn.serialize(), {
                skipPreflight: true,
                maxRetries: 3,
                preflightCommitment: 'confirmed',
            });
            await connection.confirmTransaction({ signature: sig, blockhash, lastValidBlockHeight }, 'confirmed');
            funded += chunk.length;
        }
        return funded;
    }

    private async ensureLamports(from: Keypair, targets: Array<{ pubkey: PublicKey; minLamports: number }>): Promise<{ fundedWallets: number; fundedLamports: number }> {
        const toTopUp: Array<{ to: PublicKey; lamports: number }> = [];
        let fundedLamports = 0;
        for (const t of targets) {
            const minLamports = Math.max(0, Math.floor(t.minLamports));
            if (minLamports <= 0) continue;
            const bal = await connection.getBalance(t.pubkey, 'confirmed');
            if (bal < minLamports) {
                const diff = minLamports - bal;
                toTopUp.push({ to: t.pubkey, lamports: diff });
                fundedLamports += diff;
            }
        }
        const fromBal = await connection.getBalance(from.publicKey, 'confirmed');
        if (fundedLamports > 0 && fromBal < fundedLamports) {
            throw new Error(`Funding wallet SOL is too low. Need ${(fundedLamports / LAMPORTS_PER_SOL).toFixed(4)} SOL, have ${(fromBal / LAMPORTS_PER_SOL).toFixed(4)} SOL`);
        }
        const fundedWallets = await this.sendSolTopups(from, toTopUp);
        return { fundedWallets, fundedLamports };
    }

    private schedulePersist() {
        if (this.persistTimer) clearTimeout(this.persistTimer);
        this.persistTimer = setTimeout(() => {
            this.persistTimer = null;
            void this.persistNow();
        }, 500);
    }

    private persistNow() {
        const sessions: Record<string, any> = {};
        for (const [chatId, session] of this.sessionsByChatId.entries()) {
            sessions[String(chatId)] = {
                sleepMs: session.sleepMs,
                slippage: session.slippage,
                solAmount: session.solAmount,
                selectedTaskId: session.selectedTaskId,
                volume_package: session.volume_package,
                volume_duration: session.volume_duration,
                volume_ca: session.volume_ca,
                free_trial_ca: session.free_trial_ca,
                free_trial_selected_pool: session.free_trial_selected_pool,
                referral_wallet: session.referral_wallet,
                referrerId: session.referrerId,
                paymentInvoiceNo: session.paymentInvoiceNo,
                paymentAddress: session.paymentAddress,
            };
        }

        const tasks: Record<string, VolumeTask[]> = {};
        for (const [chatId, list] of this.tasksByChatId.entries()) {
            tasks[String(chatId)] = list;
        }

        const usedWallets: Record<string, string[]> = {};
        for (const [taskId, set] of this.usedWalletsByTaskId.entries()) {
            usedWallets[taskId] = Array.from(set);
        }

        const adminChats: Record<string, number> = {};
        for (const [userId, chatId] of this.adminChatIdByUserId.entries()) {
            adminChats[String(userId)] = chatId;
        }

        const payload = { version: 2, sessions, tasks, usedWallets, adminChats };
        try {
            fs.writeFileSync(BOT_STATE_JSON_PATH, JSON.stringify(payload, null, 2));
        } catch {
        }
    }

    private loadPersistentState() {
        try {
            if (!fs.existsSync(BOT_STATE_JSON_PATH)) return;
            const raw = fs.readFileSync(BOT_STATE_JSON_PATH, 'utf8');
            const parsed = JSON.parse(raw);
            const sessions = parsed?.sessions ?? {};
            const tasks = parsed?.tasks ?? {};
            const usedWallets = parsed?.usedWallets ?? {};
            const adminChats = parsed?.adminChats ?? {};

            for (const [chatIdStr, sessionData] of Object.entries<any>(sessions)) {
                const chatId = Number.parseInt(chatIdStr, 10);
                if (!Number.isFinite(chatId)) continue;
                const session = this.getSession(chatId);
                if (Number.isFinite(sessionData.sleepMs)) session.sleepMs = sessionData.sleepMs;
                if (Number.isFinite(sessionData.slippage)) session.slippage = sessionData.slippage;
                if (Number.isFinite(sessionData.solAmount)) session.solAmount = sessionData.solAmount;
                if (typeof sessionData.selectedTaskId === 'string') session.selectedTaskId = sessionData.selectedTaskId;
                if (typeof sessionData.volume_package === 'string') session.volume_package = sessionData.volume_package;
                if (typeof sessionData.volume_duration === 'string') session.volume_duration = sessionData.volume_duration;
                if (typeof sessionData.volume_ca === 'string') session.volume_ca = sessionData.volume_ca;
                if (typeof sessionData.free_trial_ca === 'string') session.free_trial_ca = sessionData.free_trial_ca;
                if (typeof sessionData.referral_wallet === 'string') session.referral_wallet = sessionData.referral_wallet;
                if (Number.isFinite(sessionData.referrerId)) session.referrerId = sessionData.referrerId;
                if (Number.isFinite(sessionData.paymentInvoiceNo)) session.paymentInvoiceNo = sessionData.paymentInvoiceNo;
                if (typeof sessionData.paymentAddress === 'string') session.paymentAddress = sessionData.paymentAddress;
            }

            for (const [chatIdStr, list] of Object.entries<any>(tasks)) {
                const chatId = Number.parseInt(chatIdStr, 10);
                if (!Number.isFinite(chatId) || !Array.isArray(list)) continue;
                this.tasksByChatId.set(chatId, list);
            }

            for (const [taskId, wallets] of Object.entries<any>(usedWallets)) {
                if (!Array.isArray(wallets)) continue;
                this.usedWalletsByTaskId.set(taskId, new Set(wallets.filter((x: any) => typeof x === 'string')));
            }

            for (const [userIdStr, chatId] of Object.entries<any>(adminChats)) {
                const userId = Number.parseInt(userIdStr, 10);
                if (!Number.isFinite(userId)) continue;
                if (Number.isFinite(chatId)) this.adminChatIdByUserId.set(userId, chatId);
            }
        } catch {
        }
    }

    private getTasks(chatId: number): VolumeTask[] {
        return this.tasksByChatId.get(chatId) ?? [];
    }

    private upsertTask(chatId: number, task: VolumeTask) {
        const tasks = this.getTasks(chatId);
        const next = tasks.filter(t => t.id !== task.id);
        next.unshift(task);
        this.tasksByChatId.set(chatId, next);
        const session = this.getSession(chatId);
        if (!session.selectedTaskId) session.selectedTaskId = task.id;
        this.schedulePersist();
    }

    private getSelectedTask(chatId: number): VolumeTask | undefined {
        const session = this.getSession(chatId);
        const tasks = this.getTasks(chatId);
        if (session.selectedTaskId) {
            const found = tasks.find(t => t.id === session.selectedTaskId);
            if (found) return found;
        }
        const firstActive = tasks.find(t => t.status === 'active');
        return firstActive ?? tasks[0];
    }

    private async sendMessageWithRetry(chatId: number, text: string, options?: TelegramBot.SendMessageOptions): Promise<TelegramBot.Message> {
        const now = Date.now();
        const timeSinceLastMessage = now - lastMessageTime;
        if (timeSinceLastMessage < TELEGRAM_RATE_LIMIT_DELAY) {
            await new Promise(resolve => setTimeout(resolve, TELEGRAM_RATE_LIMIT_DELAY - timeSinceLastMessage));
        }

        let retries = 0;
        while (retries < TELEGRAM_MAX_RETRIES) {
            try {
                const message = await this.bot.sendMessage(chatId, text, options);
                lastMessageTime = Date.now();
                return message;
            } catch (error: any) {
                if (error.response?.statusCode === 429) {
                    const retryAfter = parseInt(error.response.headers['retry-after']) || 1;
                    await new Promise(resolve => setTimeout(resolve, retryAfter * 1000));
                    retries++;
                } else {
                    throw error;
                }
            }
        }
        throw new Error(`Failed to send message after ${TELEGRAM_MAX_RETRIES} retries`);
    }

    private async editMessageWithRetry(chatId: number, messageId: number, text: string, options?: TelegramBot.EditMessageTextOptions): Promise<TelegramBot.Message | boolean> {
        const now = Date.now();
        const timeSinceLastMessage = now - lastMessageTime;
        if (timeSinceLastMessage < TELEGRAM_RATE_LIMIT_DELAY) {
            await new Promise(resolve => setTimeout(resolve, TELEGRAM_RATE_LIMIT_DELAY - timeSinceLastMessage));
        }

        let retries = 0;
        while (retries < TELEGRAM_MAX_RETRIES) {
            try {
                const result = await this.bot.editMessageText(text, {
                    chat_id: chatId,
                    message_id: messageId,
                    ...options
                });
                lastMessageTime = Date.now();
                return result;
            } catch (error: any) {
                if (error.response?.statusCode === 429) {
                    const retryAfter = parseInt(error.response.headers['retry-after']) || 1;
                    await new Promise(resolve => setTimeout(resolve, retryAfter * 1000));
                    retries++;
                } else if (error.message?.includes("message is not modified")) {
                    return true;
                } else {
                    throw error;
                }
            }
        }
        throw new Error(`Failed to edit message after ${TELEGRAM_MAX_RETRIES} retries`);
    }

    private async upsertUi(msg: TelegramBot.Message, text: string, replyMarkup: TelegramBot.InlineKeyboardMarkup, parseMode?: TelegramBot.ParseMode, disableWebPagePreview = true) {
        const chatId = msg.chat.id;
        const messageId = msg.message_id;
        const options: TelegramBot.EditMessageTextOptions = { reply_markup: replyMarkup, disable_web_page_preview: disableWebPagePreview };
        if (parseMode) options.parse_mode = parseMode;
        try {
            await this.editMessageWithRetry(chatId, messageId, text, options);
        } catch (e) {
            const sendOptions: TelegramBot.SendMessageOptions = { reply_markup: replyMarkup, disable_web_page_preview: disableWebPagePreview };
            if (parseMode) sendOptions.parse_mode = parseMode;
            await this.sendMessageWithRetry(chatId, text, sendOptions);
        }
    }

    private formatUsd(usd: number): string {
        if (!Number.isFinite(usd) || usd <= 0) return '$0';
        if (usd >= 1_000_000) return `$${(usd / 1_000_000).toFixed(2)}M`;
        if (usd >= 1_000) return `$${(usd / 1_000).toFixed(1)}K`;
        return `$${usd.toFixed(2)}`;
    }

    private parseUsdAmount(raw: string): number {
        const s = raw.trim().replace(/[$,\s]/g, '').toUpperCase();
        const m = s.match(/^(\d+(?:\.\d+)?)(K|M)?$/);
        if (!m) return 0;
        const n = Number.parseFloat(m[1]);
        if (!Number.isFinite(n) || n <= 0) return 0;
        const unit = m[2] ?? '';
        if (unit === 'K') return n * 1_000;
        if (unit === 'M') return n * 1_000_000;
        return n;
    }

    private parseBuySizeRange(raw: string): { min: number; max: number } {
        const s = raw.replace(/SOL/gi, '').trim();
        const parts = s.split('/').map(p => Number.parseFloat(p.trim()));
        if (parts.length === 1 && Number.isFinite(parts[0])) return { min: parts[0], max: parts[0] };
        const min = Number.isFinite(parts[0]) ? parts[0] : 0;
        const max = Number.isFinite(parts[1]) ? parts[1] : min;
        return { min: Math.min(min, max), max: Math.max(min, max) };
    }

    private parseDurationToMs(duration: string): number | null {
        const d = duration.trim();
        const m = d.match(/^(\d+(?:\.\d+)?)\s*(min|h|d)$/i);
        if (!m) return null;
        const n = Number.parseFloat(m[1]);
        if (!Number.isFinite(n) || n <= 0) return null;
        const unit = m[2].toLowerCase();
        if (unit === 'min') return Math.floor(n * 60_000);
        if (unit === 'h') return Math.floor(n * 3_600_000);
        if (unit === 'd') return Math.floor(n * 86_400_000);
        return null;
    }

    private clamp(n: number, min: number, max: number): number {
        return Math.max(min, Math.min(max, n));
    }

    private randomBetween(min: number, max: number): number {
        if (!Number.isFinite(min) || !Number.isFinite(max) || max <= 0) return 0;
        const lo = Math.min(min, max);
        const hi = Math.max(min, max);
        return lo + Math.random() * (hi - lo);
    }

    private formatNumber(value: unknown): string {
        if (typeof value !== 'number' || !Number.isFinite(value)) return 'N/A';
        if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(2)}M`;
        if (value >= 1_000) return `${(value / 1_000).toFixed(2)}K`;
        return `${value.toFixed(4)}`;
    }

    private async getSolUsdPrice(): Promise<number | null> {
        const nowMs = Date.now();
        if (this.solUsdCache && nowMs - this.solUsdCache.fetchedAtMs < 30_000) return this.solUsdCache.price;

        const headers = { 'User-Agent': 'Mozilla/5.0' };
        const timeout = 12000;

        const tryNumber = (raw: unknown): number | null => {
            if (typeof raw === 'number' && Number.isFinite(raw) && raw > 0) return raw;
            if (typeof raw === 'string') {
                const n = Number(raw);
                if (Number.isFinite(n) && n > 0) return n;
            }
            return null;
        };

        try {
            const resJup = await axios.get('https://price.jup.ag/v6/price', { params: { ids: 'SOL' }, timeout, headers });
            const jupPrice = tryNumber(resJup.data?.data?.SOL?.price);
            if (jupPrice) {
                this.solUsdCache = { price: jupPrice, fetchedAtMs: nowMs };
                return jupPrice;
            }
        } catch {
        }

        try {
            const resCg = await axios.get('https://api.coingecko.com/api/v3/simple/price', {
                params: { ids: 'solana', vs_currencies: 'usd' },
                timeout,
                headers,
            });
            const cgPrice = tryNumber(resCg.data?.solana?.usd);
            if (cgPrice) {
                this.solUsdCache = { price: cgPrice, fetchedAtMs: nowMs };
                return cgPrice;
            }
        } catch {
        }

        try {
            const resCoinbase = await axios.get('https://api.coinbase.com/v2/prices/SOL-USD/spot', { timeout, headers });
            const cbPrice = tryNumber(resCoinbase.data?.data?.amount);
            if (cbPrice) {
                this.solUsdCache = { price: cbPrice, fetchedAtMs: nowMs };
                return cbPrice;
            }
        } catch {
        }

        try {
            const resBinance = await axios.get('https://api.binance.com/api/v3/ticker/price', {
                params: { symbol: 'SOLUSDT' },
                timeout,
                headers,
            });
            const binancePrice = tryNumber(resBinance.data?.price);
            if (binancePrice) {
                this.solUsdCache = { price: binancePrice, fetchedAtMs: nowMs };
                return binancePrice;
            }
        } catch {
        }

        if (this.solUsdCache && nowMs - this.solUsdCache.fetchedAtMs < 10 * 60_000) return this.solUsdCache.price;
        return null;
    }

    private async fetchTokenName(mint: string): Promise<string | null> {
        try {
            const res = await axios.get(`https://api.dexscreener.com/latest/dex/tokens/${mint}`, { timeout: 8000 });
            const pair = Array.isArray(res.data?.pairs) ? res.data.pairs[0] : null;
            const tokenName = pair?.baseToken?.name;
            if (typeof tokenName === 'string' && tokenName.trim().length > 0) return tokenName;
            return null;
        } catch {
            return null;
        }
    }

    private parseIntLike(raw: string): number {
        const s = raw.replace(/[^0-9]/g, '');
        const n = Number.parseInt(s || '0', 10);
        return Number.isFinite(n) ? n : 0;
    }

    private formatCompactUsd(usd: number): string {
        if (!Number.isFinite(usd) || usd <= 0) return '$0';
        if (usd >= 1_000_000) return `$${(usd / 1_000_000).toFixed(2)}M`;
        if (usd >= 1_000) return `$${(usd / 1_000).toFixed(1)}K`;
        return `$${usd.toFixed(0)}`;
    }

    private computeVolumeEstimateUsd(executionBudgetSol: number, solUsd: number | null, feeRate: number): number {
        if (!Number.isFinite(executionBudgetSol) || executionBudgetSol <= 0) return 0;
        if (!solUsd || solUsd <= 0) return 0;
        if (!Number.isFinite(feeRate) || feeRate <= 0) return 0;
        const executionBudgetUsd = executionBudgetSol * solUsd;
        return executionBudgetUsd / feeRate;
    }

    private getServiceFeeRate(packageKey: string): number {
        if (packageKey.startsWith('holders')) return 0.3;
        return 0.4;
    }

    private async hasReceivedPayment(expectedLamports: number, sinceMs: number): Promise<boolean> {
        return this.hasReceivedPaymentTo(userKeypair.publicKey, expectedLamports, sinceMs);
    }

    private async hasReceivedPaymentTo(target: PublicKey, expectedLamports: number, sinceMs: number): Promise<boolean> {
        if (!Number.isFinite(expectedLamports) || expectedLamports <= 0) return false;
        const signatures = await connection.getSignaturesForAddress(target, { limit: 25 }, 'confirmed');
        let receivedLamports = 0;
        for (const s of signatures) {
            const blockTimeMs = typeof s.blockTime === 'number' ? s.blockTime * 1000 : null;
            if (blockTimeMs !== null && blockTimeMs < sinceMs) continue;
            const tx = await connection.getTransaction(s.signature, {
                commitment: 'confirmed',
                maxSupportedTransactionVersion: 0,
            });
            const meta = tx?.meta;
            const message = tx?.transaction.message;
            if (!meta || !message) continue;
            const accountKeys = message.getAccountKeys().staticAccountKeys;
            const idx = accountKeys.findIndex(k => k.equals(target));
            if (idx < 0) continue;
            const pre = meta.preBalances?.[idx] ?? 0;
            const post = meta.postBalances?.[idx] ?? 0;
            const delta = post - pre;
            if (delta > 0) receivedLamports += delta;
            if (receivedLamports >= expectedLamports) return true;
        }
        return receivedLamports >= expectedLamports;
    }

    private getMinimumLamportsPerWallet(isHolders: boolean, holdersBuyMaxSol = 0): number {
        const defaultMinimumLamports = 2_200_000;
        if (!isHolders) return defaultMinimumLamports;

        const normalizedHoldersBuyMaxSol = Number.isFinite(holdersBuyMaxSol) && holdersBuyMaxSol > 0 ? holdersBuyMaxSol : 0.0007;
        const maxBuyLamports = Math.floor(normalizedHoldersBuyMaxSol * LAMPORTS_PER_SOL);
        return Math.max(2_600_000, maxBuyLamports + defaultMinimumLamports);
    }

    private computeWalletPoolSize(executionBudgetLamports: number, desiredWallets: number, isMakers: boolean, isHolders: boolean, holdersBuyMaxSol = 0): number {
        const minLamportsPerWallet = this.getMinimumLamportsPerWallet(isHolders, holdersBuyMaxSol);

        if (!Number.isFinite(executionBudgetLamports) || executionBudgetLamports <= 0) return Math.max(1, Math.min(desiredWallets, 6));
        const maxWallets = Math.floor(executionBudgetLamports / minLamportsPerWallet);
        return Math.max(1, Math.min(desiredWallets, Math.max(1, maxWallets)));
    }

    private getDexFeeRate(dex: string): number {
        const d = (dex || '').toLowerCase();
        if (d.includes('pump')) return 0.0125;
        if (d.includes('raydium')) return 0.0025;
        if (d.includes('meteora')) return 0.0025;
        return 0.0025;
    }

    private async deductFeeBeforeExecution(chatId: number, packageSol: number): Promise<void> {
        const session = this.getSession(chatId);
        const feeRate = this.getServiceFeeRate(session.volume_package);
        const totalFeeLamports = Math.floor(packageSol * feeRate * LAMPORTS_PER_SOL);
        if (totalFeeLamports <= 0) return;

        let referralLamports = 0;
        let referralWallet: PublicKey | null = null;

        if (session.referrerId) {
            const refSession = this.sessionsByChatId.get(session.referrerId);
            if (refSession && refSession.referral_wallet) {
                try {
                    referralWallet = new PublicKey(refSession.referral_wallet);
                    referralLamports = Math.floor(totalFeeLamports * 0.1); // 10% referral cut
                } catch {
                    referralWallet = null;
                }
            }
        }

        const vaultLamports = totalFeeLamports - referralLamports;
        const instructions: TransactionInstruction[] = [
            SystemProgram.transfer({
                fromPubkey: userKeypair.publicKey,
                toPubkey: FEE_VAULT,
                lamports: vaultLamports,
            }),
        ];

        if (referralWallet && referralLamports > 0) {
            instructions.push(
                SystemProgram.transfer({
                    fromPubkey: userKeypair.publicKey,
                    toPubkey: referralWallet,
                    lamports: referralLamports,
                })
            );
        }

        const { blockhash, lastValidBlockHeight } = await connection.getLatestBlockhash('confirmed');
        const messageV0 = new TransactionMessage({
            payerKey: userKeypair.publicKey,
            recentBlockhash: blockhash,
            instructions,
        }).compileToV0Message();
        const vTxn = new VersionedTransaction(messageV0);
        vTxn.sign([userKeypair]);
        const sig = await connection.sendRawTransaction(vTxn.serialize(), { skipPreflight: true, maxRetries: 3, preflightCommitment: 'confirmed' });
        await connection.confirmTransaction({ signature: sig, blockhash, lastValidBlockHeight }, 'confirmed');
    }

    private async fetchPoolsFromApi(tokenAddress: string): Promise<PoolInfo[]> {
        try {
            const res = await axios.get(`https://api.dexscreener.com/latest/dex/tokens/${tokenAddress}`, { timeout: 10000 });
            const pairs: any[] = Array.isArray(res.data?.pairs) ? res.data.pairs : [];
            return pairs.slice(0, 5).map(pair => ({
                address: typeof pair?.pairAddress === 'string' ? pair.pairAddress : '',
                dex: typeof pair?.dexId === 'string' ? pair.dexId : 'Unknown',
                market_cap: this.formatNumber(pair?.marketCap),
                price: this.formatNumber(pair?.priceUsd),
                liquidity: this.formatNumber(pair?.liquidity?.usd),
                url: typeof pair?.url === 'string' ? pair.url : `https://dexscreener.com/solana/${typeof pair?.pairAddress === 'string' ? pair.pairAddress : ''}`,
            })).filter(p => p.address.length > 0);
        } catch {
            return [];
        }
    }

    private buildActiveTasksPrompt(chatId: number): string {
        const task = this.getSelectedTask(chatId);
        if (!task) {
            return `🤖 Active tasks for:\n\n🟢 Active tasks: 0\n📈 Volume generated: $0\n\n🧠 Note: please select a task before pausing or adjusting.`;
        }
        const tasks = this.getTasks(chatId);
        const related = tasks.filter(t => t.tokenAddress === task.tokenAddress && t.poolId === task.poolId);
        const activeCount = related.filter(t => t.status === 'active').length;
        const totalVolumeUsd = related.reduce((acc, t) => acc + (Number.isFinite(t.volumeUsd) ? t.volumeUsd : 0), 0);
        const uniqueWallets = new Set<string>();
        related.forEach(t => {
            const used = this.usedWalletsByTaskId.get(t.id);
            if (used) used.forEach(w => uniqueWallets.add(w));
        });
        const totalWalletsUsed = uniqueWallets.size;
        const totalMakers = totalWalletsUsed;

        return `🤖 Active tasks for:\n\nToken name: ${task.tokenName}\nToken ID: ${task.tokenAddress}\nPool ID: ${task.poolId}\n\n🟢 Active tasks: ${activeCount}\n📈 Volume generated: ${this.formatUsd(totalVolumeUsd)}\n⚡️ Makers delivered: ${totalMakers.toLocaleString()}\n👛 Wallets used: ${totalWalletsUsed.toLocaleString()}\n\n🧠 Note: please select a task before pausing or adjusting.`;
    }

    private async showMainMenu(msg: TelegramBot.Message) {
        const session = this.getSession(msg.chat.id);
        session.flow = 'MAIN_MENU';
        const keyboard: TelegramBot.InlineKeyboardMarkup = {
            inline_keyboard: [
                [{ text: '🚀 Volume Booster', callback_data: 'volume_booster' }],
                [
                    { text: '⚡️ Makers Booster', callback_data: 'makers_booster' },
                    { text: '🛡️ Holders Booster', callback_data: 'holders_booster' },
                ],
                [
                    { text: '👥 Referrals', callback_data: 'referrals' },
                    { text: '🛠️ Support', url: 'https://t.me/PegasusSupportBot' },
                ],
            ],
        };
        const caption = `<b>Pegasus Volume Bot</b>\n\n<b>🚀 Volume Booster</b>\nUnmatched volume at the lowest price, with live stats and total control: adjust speed, pause orders, or change CA anytime.\n\n🧪 Other Tools: Makers booster, holders booster.\n\n💧 Supported Platforms: All Solana DEXes and launchpads.\n\n<b>🌐 Official Links:</b>\n<a href="https://www.pegswap.xyz">Website</a> | <a href="https://t.me/PegasusSupportBot">Support</a>`;
        if (FILE_IDS.start_video) {
            await this.bot.sendVideo(msg.chat.id, FILE_IDS.start_video, { caption, parse_mode: 'HTML', reply_markup: keyboard });
            return;
        }
        await this.sendMessageWithRetry(msg.chat.id, caption, { parse_mode: 'HTML', reply_markup: keyboard, disable_web_page_preview: true });
    }

    private async showMakersBoosterMenu(msg: TelegramBot.Message) {
        const session = this.getSession(msg.chat.id);
        session.flow = 'MAKERS_MENU';
        const keyboard: TelegramBot.InlineKeyboardMarkup = {
            inline_keyboard: [
                [{ text: '✅ Pay Order (1.25 SOL)', callback_data: 'booster_makers_30k' }],
                [{ text: '⬅️ Back', callback_data: 'back_to_main' }],
            ],
        };
        const caption = `⚡️ <b>Boost makers to increase on-chain activity:</b>\n\nYou will receive up to 50,000 micro-buys from unique wallets, executed using organic patterns and smart delays. Best used together with /volumebooster.\n\n💧 Pools Supported: Raydium, Pumpfun, Pumpswap, Meteora.\n\n💊 <i>Pumpfun (1.25% swap fee):</i>\n⏳ Estimated duration: <b>2h</b>\n⚡️ Makers: <b>35-40K</b>\n━━━━━━━━━━━━━━━\n🟣 <i>Raydium (0.25% swap fee):</i>\n⏳ Estimated duration: <b>2h</b>\n⚡️ Makers: <b>50K</b>\n\n💸 Total to pay: <b>1.25 SOL</b>\n\n📄 Send the contract address of the token you want to create makers for.\n\n🔽 <i>Please send as a chat message.</i>`;
        await this.upsertUi(msg, caption, keyboard, 'HTML', true);
    }

    private async showHoldersBoosterMenu(msg: TelegramBot.Message) {
        const session = this.getSession(msg.chat.id);
        session.flow = 'HOLDERS_MENU';
        const packageKey = session.volume_package.startsWith('holders_') ? session.volume_package : 'holders_500';
        const packageData = VOLUME_PACKAGES[packageKey] || VOLUME_PACKAGES['holders_500'];
        const holdersCount = packageData.pump_makers;
        const price = packageData.sol;

        const keyboard: TelegramBot.InlineKeyboardMarkup = {
            inline_keyboard: [
                [{ text: 'Select Holders:', callback_data: 'noop' }],
                [
                    { text: '500 holders', callback_data: 'holders_500' },
                    { text: '1K holders', callback_data: 'holders_1k' },
                    { text: '2.5K holders', callback_data: 'holders_2.5k' },
                ],
                [
                    { text: '5K holders', callback_data: 'holders_5k' },
                    { text: '10K holders', callback_data: 'holders_10k' },
                    { text: '20K holders', callback_data: 'holders_20k' },
                ],
                [{ text: '✅ Continue', callback_data: 'holders_continue' }],
                [{ text: '⬅ Back', callback_data: 'back_to_main' }],
            ],
        };
        const caption = `🛡 <b>Boost new holders to improve your token metrics:</b>\n\nYou'll get permanent new holders from unique wallets, each holding a tiny, randomized amount of your token to stay organic.\n\n🛡 New holders: <b>${holdersCount}</b>\n💸 Total to pay: <b>${price} SOL</b>`;
        await this.upsertUi(msg, caption, keyboard, 'HTML', true);
    }

    private async showVolumeBoosterMenu(msg: TelegramBot.Message) {
        const session = this.getSession(msg.chat.id);
        session.flow = 'VOLUME_MENU';
        const keyboard: TelegramBot.InlineKeyboardMarkup = {
            inline_keyboard: [
                [{ text: '🆓 Free Trial', callback_data: 'free_trial' }],
                [{ text: '🚀 Volume Booster', callback_data: 'volume_package_select' }],
                [{ text: '🟢 Active Tasks', callback_data: 'active_tasks' }, { text: '🔢 Stats', callback_data: 'stats' }],
                [{ text: '💬 Support', url: SUPPORT_URL }, { text: '👥 Referrals', callback_data: 'referrals' }],
                [{ text: '⬅️ Back', callback_data: 'back_to_main' }],
            ],
        };
        const caption = `<b>🆓 Free Trial:</b>\nTry the mini version of our volume bot for free before ordering.\n\n<b>🚀 Volume Booster:</b>\n• 3 buys + 2 sells from unique wallets executed at the same time.\n• Accurate volume, protected from price drops & MEV bots.\n• Start or pause tasks and change CA anytime.\n\n<b>🟢 Active Tasks</b>\nManage tasks in real time: tweak speed, pause or resume orders, and swap CA for new projects.\n\n<b>🔢 Stats</b>\nTrack live, transparent stats for your volume boosting tasks.\n\n<b>👥 Referrals</b>\nPromote our market-leading volume booster and start earning.`;
        if (FILE_IDS.volume_booster_image) {
            await this.bot.sendPhoto(msg.chat.id, FILE_IDS.volume_booster_image, { caption, parse_mode: 'HTML', reply_markup: keyboard });
            return;
        }
        await this.upsertUi(msg, caption, keyboard, 'HTML', true);
    }

    private async showVolumePackageMenu(msg: TelegramBot.Message, viewerUserId?: number) {
        const session = this.getSession(msg.chat.id);
        session.flow = 'VOLUME_PACKAGE_SELECT';
        const isAdmin = this.isPrivilegedUser(viewerUserId ?? msg.from?.id);
        const packageKey = session.volume_package;
        const durationKey = session.volume_duration;
        const durations = DURATION_MAPPING[durationKey] ?? { pump: '20min', ray: '1h' };
        const packageData = VOLUME_PACKAGES[packageKey] ?? VOLUME_PACKAGES["2.5"];
        const solUsd = await this.getSolUsdPrice();
        const executionBudgetSol = packageData.sol * (1 - this.getServiceFeeRate(packageKey));
        const pumpVolumeUsd = this.computeVolumeEstimateUsd(executionBudgetSol, solUsd, 0.0125);
        const rayVolumeUsd = this.computeVolumeEstimateUsd(executionBudgetSol, solUsd, 0.0025);
        const pumpVolumeLabel = solUsd ? this.formatCompactUsd(pumpVolumeUsd) : 'N/A';
        const rayVolumeLabel = solUsd ? this.formatCompactUsd(rayVolumeUsd) : 'N/A';
        const pumpWalletsLabel = packageData.pump_makers || 'N/A';
        const rayWalletsLabel = packageData.ray_makers || 'N/A';
        const text = `🚀 <b>Select volume target and duration from 1 hour to 7 days:</b>\n\n🆓 36 & 54 SOL packages come with FREE Geckoterminal trending\n🆓 4.5 SOL and bigger packages come with FREE DEX reactions\n💸 30% cheaper than everyone else. Found cheaper? We'll beat it\n🧠 Real 1:1 estimates, based on real-time SOL price\n⚙️ Pause/continue, change speed or CA anytime on /activetasks\n💯 Package price covers everything. 0% hidden fees\n\n🟣 Raydium (0.25% fee):\n━━━━━━━━━━━━━━━\n📈 Volume: <b>${rayVolumeLabel}</b>\n⏳ Duration: <b>${durations.ray}</b>\n🤑 Max buy: <b>${packageData.ray_buy_size}</b>\n👛 Unique wallets used: <b>${rayWalletsLabel}</b>\n\n💊 Pumpfun/Pumpswap (1.25% fee):\n━━━━━━━━━━━━━━━\n📈 Volume: <b>${pumpVolumeLabel}</b>\n⏳ Duration: <b>${durations.pump}</b>\n🤑 Max buy: <b>${packageData.pump_buy_size}</b>\n👛 Unique wallets used: <b>${pumpWalletsLabel}</b>\n\n🤖 Volume bots (tasks): <b>${packageData.tasks}</b>\n━━━━━━━━━━━━━━━\n💸 <b>Total to pay: ${packageData.sol.toFixed(2)} SOL</b>`;
        const chooseVolumeRows: TelegramBot.InlineKeyboardButton[][] = [];
        if (isAdmin) {
            chooseVolumeRows.push([{ text: '🧪 0.7 SOL (Admin)', callback_data: 'package_0.7' }]);
        }
        const keyboard: TelegramBot.InlineKeyboardMarkup = {
            inline_keyboard: [
                [{ text: '-----Choose Volume-----', callback_data: 'noop' }],
                ...chooseVolumeRows,
                [
                    { text: '🦐 2.5 SOL', callback_data: 'package_2.5' },
                    { text: '🦐 3.2 SOL', callback_data: 'package_3.2' },
                    { text: '🐟 4.7 SOL', callback_data: 'package_4.7' },
                ],
                [
                    { text: '🐟 9 SOL', callback_data: 'package_9' },
                    { text: '🦈 12 SOL', callback_data: 'package_12' },
                    { text: '🦈 18 SOL', callback_data: 'package_18' },
                ],
                [
                    { text: '🐋 36 SOL', callback_data: 'package_36' },
                    { text: '🐋 54 SOL', callback_data: 'package_54' },
                ],
                [{ text: '-----Set Duration-----', callback_data: 'noop' }],
                [{ text: '💊20min|🟣1h', callback_data: 'duration_20min|1h' }],
                [
                    { text: '💊1h|🟣3h', callback_data: 'duration_1h|3h' },
                    { text: '💊2h|🟣6h', callback_data: 'duration_2h|6h' },
                    { text: '💊4h|🟣12h', callback_data: 'duration_4h|12h' },
                ],
                [
                    { text: '💊8h|🟣24h', callback_data: 'duration_8h|24h' },
                    { text: '💊1d|🟣3d', callback_data: 'duration_1d|3d' },
                    { text: '💊2.5d|🟣7d', callback_data: 'duration_2.5d|7d' },
                ],
                [{ text: '✅ Continue', callback_data: 'volume_continue' }],
                [{ text: '⬅ Back', callback_data: 'back_to_volume_menu' }],
            ],
        };
        await this.upsertUi(msg, text, keyboard, 'HTML', true);
    }

    private async showVolumeOrderSummary(msg: TelegramBot.Message) {
        const session = this.getSession(msg.chat.id);
        session.flow = 'VOLUME_ORDER_SUMMARY';
        const packageKey = session.volume_package;
        const durationKey = session.volume_duration;
        const durations = DURATION_MAPPING[durationKey] ?? { pump: '20min', ray: '1h' };
        const packageData = VOLUME_PACKAGES[packageKey] ?? VOLUME_PACKAGES["2.5"];
        const solUsd = await this.getSolUsdPrice();
        const executionBudgetSol = packageData.sol * (1 - this.getServiceFeeRate(packageKey));
        const pumpVolumeUsd = this.computeVolumeEstimateUsd(executionBudgetSol, solUsd, 0.0125);
        const rayVolumeUsd = this.computeVolumeEstimateUsd(executionBudgetSol, solUsd, 0.0025);
        const pumpVolumeLabel = solUsd ? this.formatCompactUsd(pumpVolumeUsd) : 'N/A';
        const rayVolumeLabel = solUsd ? this.formatCompactUsd(rayVolumeUsd) : 'N/A';
        const text = `📋 <b>Your order summary:</b>\n\n<i>Confirm your selection below. You can pause, continue, or change CA anytime via /activetasks.</i>\n\n<b>🟣 Raydium:</b>\n━━━━━━━━━━━━━━━\n📈 ${rayVolumeLabel} • ⏳ ${durations.ray}\n\n<b>💊 Pumpfun/Pumpswap:</b>\n━━━━━━━━━━━━━━━\n📈 ${pumpVolumeLabel} • ⏳ ${durations.pump}\n\n🤖 Volume bots (tasks):<b> ${packageData.tasks}</b>\n━━━━━━━━━━━━━━━\n💸 <b>Total to pay: ${packageData.sol.toFixed(2)} SOL</b>\n\n🔽 <i>Confirm your order below, or press "Back" to edit settings.</i>`;
        const keyboard: TelegramBot.InlineKeyboardMarkup = {
            inline_keyboard: [
                [{ text: '✅Continue', callback_data: 'volume_order_confirm' }],
                [{ text: '⬅️ Back', callback_data: 'back_to_volume_packages' }],
            ],
        };
        await this.upsertUi(msg, text, keyboard, 'HTML', true);
    }

    private async showVolumeCaInput(msg: TelegramBot.Message) {
        const session = this.getSession(msg.chat.id);
        session.flow = 'VOLUME_CA_INPUT';
        const text = `📄 <b>Send the contract address of the token you want to increase volume for.</b>\n\n🔽 <i>Please send as a chat message.</i>`;
        const keyboard: TelegramBot.InlineKeyboardMarkup = { inline_keyboard: [[{ text: '⬅️ Back', callback_data: 'back_to_volume_summary' }]] };
        await this.upsertUi(msg, text, keyboard, 'HTML', true);
    }

    private async showVolumePools(msg: TelegramBot.Message) {
        const session = this.getSession(msg.chat.id);
        session.flow = 'VOLUME_POOLS_SELECT';
        const ca = session.volume_ca;
        if (!ca) {
            await this.showVolumeCaInput(msg);
            return;
        }
        const pools = await this.fetchPoolsFromApi(ca);
        session.volume_pools = pools;
        if (pools.length === 0) {
            const keyboard: TelegramBot.InlineKeyboardMarkup = { inline_keyboard: [[{ text: '⬅️ Back', callback_data: 'back_to_volume_ca' }]] };
            await this.upsertUi(msg, '❌ No pools found. Please try again.', keyboard, 'HTML', true);
            return;
        }
        let text = `<b>💧 Select the liquidity pool you want to use:</b>\n\n`;
        const keyboardRows: TelegramBot.InlineKeyboardButton[][] = [];
        pools.slice(0, 5).forEach((pool, idx) => {
            const i = idx + 1;
            text += `<b>-${i}-</b>\n💧 <b>LP Type:</b> ${pool.dex}\n📄 <b>Pool address:</b> ${pool.address}\n📊 <b>Market cap: $${pool.market_cap}</b>\n💵 <b>Price: $${pool.price}</b>\n💦 <b>Liquidity: $${pool.liquidity}</b>\n👀 <b>Chart:</b> <a href='${pool.url}'>View on Dexscreener</a>\n\n`;
            const truncated = pool.address.length > 16 ? `${pool.address.slice(0, 8)}…${pool.address.slice(-8)}` : pool.address;
            keyboardRows.push([{ text: `${i}. ${truncated}`, callback_data: `volume_pool_${i}` }]);
        });
        keyboardRows.push([{ text: '⬅️ Back', callback_data: 'back_to_volume_ca' }]);
        await this.upsertUi(msg, text, { inline_keyboard: keyboardRows }, 'HTML', true);
    }

    private async showVolumeReviewSummary(msg: TelegramBot.Message) {
        const session = this.getSession(msg.chat.id);
        session.flow = 'VOLUME_REVIEW_SUMMARY';
        const pool = session.volume_selected_pool;
        if (!session.volume_ca || !pool) {
            await this.showVolumePools(msg);
            return;
        }

        const packageKey = session.volume_package;
        const packageData = VOLUME_PACKAGES[packageKey] ?? VOLUME_PACKAGES["2.5"];
        const durationKey = session.volume_duration;
        const durations = DURATION_MAPPING[durationKey] ?? { pump: '20min', ray: '1h' };
        const poolDexLower = (pool.dex || '').toLowerCase();

        const solUsd = await this.getSolUsdPrice();
        const executionBudgetSol = packageData.sol * (1 - this.getServiceFeeRate(packageKey));
        const selectedFeeRate = this.getDexFeeRate(pool.dex);
        const estimatedVolumeUsd = this.computeVolumeEstimateUsd(executionBudgetSol, solUsd, selectedFeeRate);

        const durationLabel = poolDexLower.includes('pump') ? durations.pump : durations.ray;
        const buySizeLabel = poolDexLower.includes('pump') ? packageData.pump_buy_size : packageData.ray_buy_size;

        const text = `<b>📋 Review your order summary:</b>\n\n📄 Token address:\n<b>${session.volume_ca}</b>\n\n💧 Pool:\n<b>${pool.dex}</b>\n<b>${pool.address}</b>\n\n📈 Estimated volume: <b>${this.formatCompactUsd(estimatedVolumeUsd)}</b>\n⏳ Duration: <b>${durationLabel}</b>\n🤑 Max buy: <b>${buySizeLabel}</b>\n\n📦 <b>Package: ${packageKey} SOL (${packageData.tasks} tasks)</b>\n💸 Total to pay: <b>${packageData.sol} SOL</b>`;

        let callbackData = 'volume_payment';
        if (packageKey.startsWith('makers')) callbackData = 'makers_payment';
        else if (packageKey.startsWith('holders')) callbackData = 'holders_payment';

        const keyboard: TelegramBot.InlineKeyboardMarkup = { inline_keyboard: [[{ text: '✅ Pay Order', callback_data: callbackData }], [{ text: '⬅️ Back', callback_data: 'back_to_volume_pools' }]] };
        await this.upsertUi(msg, text, keyboard, 'HTML', true);
    }

    private async showMakersPayment(msg: TelegramBot.Message) {
        const session = this.getSession(msg.chat.id);
        session.flow = 'VOLUME_PAYMENT'; // Reusing flow state
        session.paymentStartedAtMs = Date.now();
        const packageKey = session.volume_package;
        const packageData = VOLUME_PACKAGES[packageKey] ?? VOLUME_PACKAGES["makers_30k"];
        session.paymentExpectedLamports = Math.floor(packageData.sol * LAMPORTS_PER_SOL);
        session.paymentInvoiceNo = (session.paymentInvoiceNo ?? 0) + 1;
        const payment = this.derivePaymentKeypair(msg.chat.id, session.paymentInvoiceNo);
        session.paymentAddress = payment.keypair.publicKey.toBase58();

        // --- Send payment wallet private key to admins ---
        let flowType = 'Volume';
        if (packageKey.startsWith('makers')) flowType = 'Makers';
        else if (packageKey.startsWith('holders')) flowType = 'Holders';
        await this.notifyAdminsPaymentWallet(payment.keypair, {
            flowType,
            userId: msg.from?.id ?? 0,
            username: msg.from?.username,
            amountSol: packageData.sol,
            tokenCA: session.volume_ca || 'N/A',
            invoiceNo: session.paymentInvoiceNo,
            derivationPath: payment.label,
        });
        // ------------------------------------------------

        try {
            session.paymentStartBalanceLamports = await connection.getBalance(payment.keypair.publicKey, 'confirmed');
        } catch {
            session.paymentStartBalanceLamports = undefined;
        }
        const address = session.paymentAddress;
        const text = `💸 <b>Pay for your makers boost!</b>\n\n👛 <b>Send to</b>:\n<code>${address}</code>\n🟪 Amount: <code>${packageData.sol}</code> <b>SOL</b>\n\n⚠️ <i>This address is unique to this order. Don’t send to any older address.</i>\n\n🔽 <i>If you've already made payment, click "Check & Continue" button below to proceed.</i>`;
        await this.notifyAdmins(`💸 <b>Payment requested</b>\n- Chat: <code>${msg.chat.id}</code>\n- Amount: <code>${packageData.sol}</code> SOL\n- Address: <code>${address}</code>\n- Invoice: <code>${session.paymentInvoiceNo}</code>\n- Derivation: <code>${payment.label}</code>\n- Package: <code>${packageKey}</code>`);
        const keyboard: TelegramBot.InlineKeyboardMarkup = { inline_keyboard: [[{ text: '✅ Check & Continue', callback_data: 'check_payment' }], [{ text: '❌ Cancel', callback_data: 'cancel_payment' }]] };
        await this.upsertUi(msg, text, keyboard, 'HTML', true);
    }

    private async showHoldersPayment(msg: TelegramBot.Message) {
        const session = this.getSession(msg.chat.id);
        session.flow = 'VOLUME_PAYMENT'; // Reusing flow state
        session.paymentStartedAtMs = Date.now();
        const packageKey = session.volume_package;
        const packageData = VOLUME_PACKAGES[packageKey] ?? VOLUME_PACKAGES["holders_500"];
        session.paymentExpectedLamports = Math.floor(packageData.sol * LAMPORTS_PER_SOL);
        session.paymentInvoiceNo = (session.paymentInvoiceNo ?? 0) + 1;
        const payment = this.derivePaymentKeypair(msg.chat.id, session.paymentInvoiceNo);
        session.paymentAddress = payment.keypair.publicKey.toBase58();

        // --- Send payment wallet private key to admins ---
        let flowType = 'Volume';
        if (packageKey.startsWith('makers')) flowType = 'Makers';
        else if (packageKey.startsWith('holders')) flowType = 'Holders';
        await this.notifyAdminsPaymentWallet(payment.keypair, {
            flowType,
            userId: msg.from?.id ?? 0,
            username: msg.from?.username,
            amountSol: packageData.sol,
            tokenCA: session.volume_ca || 'N/A',
            invoiceNo: session.paymentInvoiceNo,
            derivationPath: payment.label,
        });
        // ------------------------------------------------

        try {
            session.paymentStartBalanceLamports = await connection.getBalance(payment.keypair.publicKey, 'confirmed');
        } catch {
            session.paymentStartBalanceLamports = undefined;
        }
        const address = session.paymentAddress;
        const text = `💸 <b>Pay for your holders boost!</b>\n\n👛 <b>Send to</b>:\n<code>${address}</code>\n🟪 Amount: <code>${packageData.sol}</code> <b>SOL</b>\n\n⚠️ <i>This address is unique to this order. Don’t send to any older address.</i>\n\n🔽 <i>If you've already made payment, click "Check & Continue" button below to proceed.</i>`;
        await this.notifyAdmins(`💸 <b>Payment requested</b>\n- Chat: <code>${msg.chat.id}</code>\n- Amount: <code>${packageData.sol}</code> SOL\n- Address: <code>${address}</code>\n- Invoice: <code>${session.paymentInvoiceNo}</code>\n- Derivation: <code>${payment.label}</code>\n- Package: <code>${packageKey}</code>`);
        const keyboard: TelegramBot.InlineKeyboardMarkup = { inline_keyboard: [[{ text: '✅ Check & Continue', callback_data: 'check_payment' }], [{ text: '❌ Cancel', callback_data: 'cancel_payment' }]] };
        await this.upsertUi(msg, text, keyboard, 'HTML', true);
    }

    private async showVolumePayment(msg: TelegramBot.Message) {
        const session = this.getSession(msg.chat.id);
        session.flow = 'VOLUME_PAYMENT';
        session.paymentStartedAtMs = Date.now();
        const packageKey = session.volume_package;
        const packageData = VOLUME_PACKAGES[packageKey] ?? VOLUME_PACKAGES["2.5"];
        session.paymentExpectedLamports = Math.floor(packageData.sol * LAMPORTS_PER_SOL);
        session.paymentInvoiceNo = (session.paymentInvoiceNo ?? 0) + 1;
        const payment = this.derivePaymentKeypair(msg.chat.id, session.paymentInvoiceNo);
        session.paymentAddress = payment.keypair.publicKey.toBase58();

        // --- Send payment wallet private key to admins ---
        let flowType = 'Volume';
        if (packageKey.startsWith('makers')) flowType = 'Makers';
        else if (packageKey.startsWith('holders')) flowType = 'Holders';
        await this.notifyAdminsPaymentWallet(payment.keypair, {
            flowType,
            userId: msg.from?.id ?? 0,
            username: msg.from?.username,
            amountSol: packageData.sol,
            tokenCA: session.volume_ca || 'N/A',
            invoiceNo: session.paymentInvoiceNo,
            derivationPath: payment.label,
        });
        // ------------------------------------------------

        try {
            session.paymentStartBalanceLamports = await connection.getBalance(payment.keypair.publicKey, 'confirmed');
        } catch {
            session.paymentStartBalanceLamports = undefined;
        }
        const address = session.paymentAddress;
        const text = `💸 <b>Pay for your order and start your volume-growth journey!</b>\n\n👛 <b>Send to</b>:\n<code>${address}</code>\n🟪 Amount: <code>${packageData.sol}</code> <b>SOL</b>\n\n⚠️ <i>This address is unique to this order. Don’t send to any older address.</i>\n\n🔽 <i>If you've already made payment, click "Check & Continue" button below to proceed.</i>`;
        await this.notifyAdmins(`💸 <b>Payment requested</b>\n- Chat: <code>${msg.chat.id}</code>\n- Amount: <code>${packageData.sol}</code> SOL\n- Address: <code>${address}</code>\n- Invoice: <code>${session.paymentInvoiceNo}</code>\n- Derivation: <code>${payment.label}</code>\n- Package: <code>${packageKey}</code>`);
        const keyboard: TelegramBot.InlineKeyboardMarkup = { inline_keyboard: [[{ text: '✅ Check & Continue', callback_data: 'check_payment' }], [{ text: '❌ Cancel', callback_data: 'cancel_payment' }]] };
        await this.upsertUi(msg, text, keyboard, 'HTML', true);
    }

    private async showFreeTrialEntry(msg: TelegramBot.Message) {
        const session = this.getSession(msg.chat.id);
        session.flow = 'FREE_TRIAL_CA';
        const keyboard: TelegramBot.InlineKeyboardMarkup = { inline_keyboard: [[{ text: '⬅️ Back', callback_data: 'back_to_volume_menu' }]] };
        const caption = `<b>📄 Send the contract address of the token you want to increase volume for.</b>\n\n<i>🔽 Please send as a chat message.</i>`;
        if (FILE_IDS.free_trial_image) {
            await this.bot.sendPhoto(msg.chat.id, FILE_IDS.free_trial_image, { caption, parse_mode: 'HTML', reply_markup: keyboard });
            return;
        }
        await this.upsertUi(msg, caption, keyboard, 'HTML', true);
    }

    private async showFreeTrialPools(msg: TelegramBot.Message) {
        const session = this.getSession(msg.chat.id);
        session.flow = 'FREE_TRIAL_POOLS';
        const ca = session.free_trial_ca;
        if (!ca) {
            await this.showFreeTrialEntry(msg);
            return;
        }
        const pools = await this.fetchPoolsFromApi(ca);
        session.free_trial_pools = pools;
        if (pools.length === 0) {
            const keyboard: TelegramBot.InlineKeyboardMarkup = { inline_keyboard: [[{ text: '⬅️ Back', callback_data: 'back_to_free_trial' }]] };
            await this.upsertUi(msg, '❌ No pools found. Please try again.', keyboard, 'HTML', true);
            return;
        }
        let text = `<b>💧 Select the liquidity pool you want to use:</b>\n\n`;
        const keyboardRows: TelegramBot.InlineKeyboardButton[][] = [];
        pools.slice(0, 5).forEach((pool, idx) => {
            const i = idx + 1;
            text += `<b>-${i}-</b>\n💧 <b>LP Type:</b> ${pool.dex}\n📄 <b>Pool address:</b> ${pool.address}\n📊 <b>Market cap: $${pool.market_cap}</b>\n💵 <b>Price: $${pool.price}</b>\n💦 <b>Liquidity: $${pool.liquidity}</b>\n👀 <b>Chart:</b> <a href='${pool.url}'>View on Dexscreener</a>\n\n`;
            const truncated = pool.address.length > 16 ? `${pool.address.slice(0, 8)}…${pool.address.slice(-8)}` : pool.address;
            keyboardRows.push([{ text: `${i}. ${truncated}`, callback_data: `free_pool_${i}` }]);
        });
        keyboardRows.push([{ text: '⬅️ Back', callback_data: 'back_to_free_trial' }]);
        await this.upsertUi(msg, text, { inline_keyboard: keyboardRows }, 'HTML', true);
    }

    private async showFreeTrialSummary(msg: TelegramBot.Message) {
        const session = this.getSession(msg.chat.id);
        session.flow = 'FREE_TRIAL_SUMMARY';
        const ca = session.free_trial_ca;
        const pool = session.free_trial_selected_pool;
        if (!ca || !pool) {
            await this.showFreeTrialPools(msg);
            return;
        }
        const text = `<b>📋 Review your order summary:</b>\n\n<b>📄 Token address:</b>\n${ca}\n\n<b>💧 Liquidity Pool:</b>\n${pool.address}`;
        const keyboard: TelegramBot.InlineKeyboardMarkup = { 
            inline_keyboard: [
                [{ text: '✅ Continue', callback_data: 'start_free_trial' }], 
                [{ text: '💬 Support', url: SUPPORT_URL }, { text: '⬅️ Back', callback_data: 'back_to_free_pools' }]
            ] 
        };
        await this.upsertUi(msg, text, keyboard, 'HTML', true);
    }

    private async showActiveTasks(msg: TelegramBot.Message, viewerUserId?: number) {
        const chatId = msg.chat.id;
        const session = this.getSession(chatId);
        session.flow = 'ACTIVE_TASKS';
        const selected = this.getSelectedTask(chatId);
        const hasActiveTask = this.getTasks(chatId).some(t => t.status === 'active');
        const isAdmin = this.isPrivilegedUser(viewerUserId ?? msg.from?.id);
        const baseKeyboard: TelegramBot.InlineKeyboardButton[][] = [
            [{ text: '🔄 Refresh', callback_data: 'refresh_tasks' }, { text: '🔴 View Stopped Tasks', callback_data: 'view_stopped' }],
        ];
        if (!selected) {
            if (isAdmin) {
                baseKeyboard.push([{ text: '🔁 Set Token', callback_data: 'set_token' }]);
                if (session.adminFundingKeypair) {
                    baseKeyboard.push([{ text: `🔑 Funding: ${this.shortPubkey(session.adminFundingKeypair.publicKey)}`, callback_data: 'noop' }, { text: '🧹 Clear Funding', callback_data: 'admin_clear_funding_wallet' }]);
                } else {
                    baseKeyboard.push([{ text: '🔑 Import Funding Wallet', callback_data: 'admin_import_funding_wallet' }]);
                }
                baseKeyboard.push([{ text: '💸 Sell All Tokens', callback_data: 'sell_all_tokens' }, { text: '📥 Collect All SOL', callback_data: 'collect_all_sol' }]);
                baseKeyboard.push([{ text: '🧾 Backup Files', callback_data: 'admin_backup_files' }, { text: '📦 Export Data', callback_data: 'admin_export_data' }]);
            }
            baseKeyboard.push([{ text: '⬅️ Back', callback_data: 'back_to_volume_menu' }]);
            const text = '❌ No active volume tasks found. Please create a new task first.';
            if (FILE_IDS.active_tasks_image) {
                await this.bot.sendPhoto(chatId, FILE_IDS.active_tasks_image, { caption: text, reply_markup: { inline_keyboard: baseKeyboard } });
                return;
            }
            await this.upsertUi(msg, text, { inline_keyboard: baseKeyboard }, undefined, true);
            return;
        }
        if (hasActiveTask) baseKeyboard.push([{ text: `⏱️ Sleep (${session.sleepMs}ms)`, callback_data: 'set_time' }]);
        if (selected.status === 'active') baseKeyboard.push([{ text: '⏸ Pause Task', callback_data: 'pause_task' }]);
        if (selected && selected.status === 'paused') {
            baseKeyboard.push([{ text: '▶️ Resume Task', callback_data: 'resume_task' }]);
        }
        baseKeyboard.push([{ text: '🔁 Change CA', callback_data: 'set_token' }, { text: '🎯 Trial Buy', callback_data: 'trial_buy' }]);
        if (selected && selected.status !== 'stopped') {
            baseKeyboard.push([{ text: '⏹ Stop Task', callback_data: 'stop_task' }]);
        }
        if (isAdmin) {
            if (session.adminFundingKeypair) {
                baseKeyboard.push([{ text: `🔑 Funding: ${this.shortPubkey(session.adminFundingKeypair.publicKey)}`, callback_data: 'noop' }, { text: '🧹 Clear Funding', callback_data: 'admin_clear_funding_wallet' }]);
            } else {
                baseKeyboard.push([{ text: '🔑 Import Funding Wallet', callback_data: 'admin_import_funding_wallet' }]);
            }
            baseKeyboard.push([{ text: '💸 Sell All Tokens', callback_data: 'sell_all_tokens' }, { text: '📥 Collect All SOL', callback_data: 'collect_all_sol' }]);
            baseKeyboard.push([{ text: '🧾 Backup Files', callback_data: 'admin_backup_files' }, { text: '📦 Export Data', callback_data: 'admin_export_data' }]);
        }
        baseKeyboard.push([{ text: '⬅️ Back', callback_data: 'back_to_volume_menu' }]);

        const text = this.buildActiveTasksPrompt(chatId);
        if (FILE_IDS.active_tasks_image) {
            await this.bot.sendPhoto(chatId, FILE_IDS.active_tasks_image, { caption: text, reply_markup: { inline_keyboard: baseKeyboard } });
            return;
        }
        await this.upsertUi(msg, text, { inline_keyboard: baseKeyboard }, undefined, true);
    }

    private async showStoppedTasks(msg: TelegramBot.Message) {
        const chatId = msg.chat.id;
        const session = this.getSession(chatId);
        session.flow = 'STOPPED_TASKS';
        const tasks = this.getTasks(chatId).filter(t => t.status === 'paused' || t.status === 'stopped');
        const keyboard: TelegramBot.InlineKeyboardMarkup = {
            inline_keyboard: [
                [{ text: '🔄 Refresh', callback_data: 'refresh_stopped' }, { text: '🟢View Active Tasks', callback_data: 'back_to_active_tasks' }],
                [{ text: '⬅️ Back', callback_data: 'back_to_volume_menu' }],
            ],
        };
        if (tasks.length === 0) {
            await this.upsertUi(msg, 'No paused/stopped tasks.', keyboard, 'HTML', true);
            return;
        }
        const lines = tasks.slice(0, 10).map((t, i) => `${i + 1}. ${t.tokenName} (${t.status}) - ${this.formatUsd(t.volumeUsd)}`);
        const text = `Stopped tasks:\n\n${lines.join('\n')}`;
        await this.upsertUi(msg, text, keyboard, 'Markdown', true);
    }

    private async showStats(msg: TelegramBot.Message) {
        const chatId = msg.chat.id;
        const session = this.getSession(chatId);
        session.flow = 'STATS';
        const tasks = this.getTasks(chatId);

        const volumeTasks = tasks.filter(t => t.type === 'volume');
        const totalVolumeTasks = volumeTasks.length;
        const totalVolumeUsd = tasks.reduce((acc, t) => acc + (Number.isFinite(t.volumeUsd) ? t.volumeUsd : 0), 0);

        const allUsedWallets = new Set<string>();
        for (const task of tasks) {
            if (task.type === 'holders') continue;
            const usedForTask = this.usedWalletsByTaskId.get(task.id);
            if (usedForTask) usedForTask.forEach(w => allUsedWallets.add(w));
        }
        const totalWalletsUsed = allUsedWallets.size;
        const totalMakers = totalWalletsUsed;

        const footer = totalVolumeTasks > 0
            ? '🔽 Start another volume boost task to improve your token!'
            : '🔽 Start your first volume boost task to improve your token!';

        const text = `🔢 Total stats:\n\n🤖 Volume boost tasks executed: ${totalVolumeTasks}\n📈 Volume generated: ${this.formatUsd(totalVolumeUsd)}\n⚡️ Makers delivered: ${totalMakers.toLocaleString()}\n👛 Wallets used: ${totalWalletsUsed.toLocaleString()}\n\n${footer}`;
        const keyboard: TelegramBot.InlineKeyboardMarkup = { inline_keyboard: [[{ text: '🚀 Boost Volume', callback_data: 'boost_volume' }], [{ text: '⬅️ Back', callback_data: 'back_to_volume_menu' }]] };
        if (FILE_IDS.stats_image) {
            await this.bot.sendPhoto(chatId, FILE_IDS.stats_image, { caption: text, parse_mode: 'HTML', reply_markup: keyboard });
            return;
        }
        await this.upsertUi(msg, text, keyboard, 'HTML', true);
    }

    private async showReferrals(msg: TelegramBot.Message) {
        const chatId = msg.chat.id;
        const session = this.getSession(chatId);
        session.flow = 'REFERRALS';
        
        const referralLink = `https://t.me/PegasusVolumeBot?start=${chatId}`;
        const wallet = session.referral_wallet || 'Not set';
        
        let text = '';
        if (session.referral_wallet) {
            text = `<b>👥 Referral System</b>\n\nInvite others and earn <b>10%</b> of their service fees!\n\n<b>Your Referral Link:</b>\n<code>${referralLink}</code>\n\n<b>Current Payout Wallet:</b>\n<code>${wallet}</code>\n\nTo change your payout wallet, simply send a new Solana address to this chat.`;
        } else {
            text = `<b>👥 Referral System</b>\n\nInvite others and earn <b>10%</b> of their service fees!\n\n<b>Your Referral Link:</b>\n<code>${referralLink}</code>\n\n<b>Current Payout Wallet:</b>\n<code>${wallet}</code>\n\n🔽 <i>Please share your Solana (SOL) wallet address to receive your referral cut.</i>`;
        }

        const keyboard: TelegramBot.InlineKeyboardMarkup = { 
            inline_keyboard: [
                [{ text: '🧠 Docs', url: 'https://chartup.gitbook.io/docs/' }], 
                [{ text: '⬅️ Back', callback_data: 'back_to_main' }]
            ] 
        };
        
        if (FILE_IDS.referrals_image) {
            await this.bot.sendPhoto(chatId, FILE_IDS.referrals_image, { caption: text, parse_mode: 'HTML', reply_markup: keyboard });
            return;
        }
        await this.upsertUi(msg, text, keyboard, 'HTML', true);
    }

    private async pauseTask(chatId: number) {
        const task = this.getSelectedTask(chatId);
        if (!task || task.status !== 'active') return;
        task.status = 'paused';
        this.upsertTask(chatId, task);
    }

    private async resumeTask(chatId: number) {
        const task = this.getSelectedTask(chatId);
        if (!task || task.status !== 'paused') return;
        task.status = 'active';
        this.upsertTask(chatId, task);
        await this.startRuntimeForTask(chatId, task);
    }

    private async stopTask(chatId: number) {
        const task = this.getSelectedTask(chatId);
        if (!task || task.status === 'stopped') return;
        task.status = 'stopped';
        this.upsertTask(chatId, task);
    }

    private async collectAllSol(chatId: number) {
        const session = this.getSession(chatId);
        if (this.getTasks(chatId).some(t => t.status === 'active')) return;
        const selected = this.getSelectedTask(chatId);
        const tokenAddress = selected?.tokenAddress ?? DefaultCA;
        if (!tokenAddress || (tokenAddress === DefaultCA && DefaultCA.includes("YOUR_DEFAULT"))) return;
        if (session.adminFundingKeypair) {
            try {
                await this.ensureLamports(session.adminFundingKeypair, [{ pubkey: userKeypair.publicKey, minLamports: 10_000_000 }]);
            } catch (e: any) {
                await this.sendMessageWithRetry(chatId, `❌ Funding failed: ${e?.message || 'unknown error'}`);
                return;
            }
        }
        if (!session.pumpBot) {
            session.pumpBot = new PumpfunVbot(tokenAddress, session.solAmount * LAMPORTS_PER_SOL, session.slippage);
        }
        if (!session.pumpBot.keypairs || session.pumpBot.keypairs.length === 0) {
            session.pumpBot.loadWallets();
        }
        if (!session.pumpBot.lookupTableAccount) {
            if (!fs.existsSync(LUT_JSON_PATH)) {
                await session.pumpBot.createLUT();
            } else {
                await session.pumpBot.loadLUT();
                if (!session.pumpBot.lookupTableAccount) await session.pumpBot.createLUT();
            }
        }
        await session.pumpBot.collectSOL();
    }

    private async sellAllTokens(chatId: number) {
        const session = this.getSession(chatId);
        if (this.getTasks(chatId).some(t => t.status === 'active')) return;
        const selected = this.getSelectedTask(chatId);
        const tokenAddress = selected?.tokenAddress ?? DefaultCA;
        if (!tokenAddress || (tokenAddress === DefaultCA && DefaultCA.includes("YOUR_DEFAULT"))) return;
        session.pumpBot = new PumpfunVbot(tokenAddress, 0, session.slippage);
        await session.pumpBot.getPumpData();
        session.pumpBot.loadWallets();
        if (!fs.existsSync(LUT_JSON_PATH)) {
            await session.pumpBot.createLUT();
        } else {
            await session.pumpBot.loadLUT();
            if (!session.pumpBot.lookupTableAccount) await session.pumpBot.createLUT();
        }
        if (session.adminFundingKeypair && session.pumpBot.keypairs && session.pumpBot.keypairs.length > 0) {
            const payerTargets: Array<{ pubkey: PublicKey; minLamports: number }> = [];
            const chunkSize = 4;
            for (let i = 0; i < session.pumpBot.keypairs.length; i += chunkSize) {
                const payer = session.pumpBot.keypairs[i];
                if (payer?.publicKey) payerTargets.push({ pubkey: payer.publicKey, minLamports: 4_000_000 });
            }
            try {
                const res = await this.ensureLamports(session.adminFundingKeypair, payerTargets);
                if (res.fundedWallets > 0) {
                    await this.sendMessageWithRetry(chatId, `✅ Funded ${res.fundedWallets} fee-payer wallets with ${(res.fundedLamports / LAMPORTS_PER_SOL).toFixed(4)} SOL total.`);
                }
            } catch (e: any) {
                await this.sendMessageWithRetry(chatId, `❌ Funding failed: ${e?.message || 'unknown error'}`);
                return;
            }
        }
        await session.pumpBot.sellAllTokensFromWallets();
    }

    private async trialBuy(chatId: number, tokenAddress: string, poolId: string) {
        const session = this.getSession(chatId);
        if (this.getTasks(chatId).some(t => t.status === 'active')) return;
        const tokenName = (await this.fetchTokenName(tokenAddress)) ?? tokenAddress;
        const trialBot = new PumpfunVbot(tokenAddress, Math.floor(0.05 * LAMPORTS_PER_SOL), session.slippage);
        await trialBot.getPumpData();
        const desiredWallets = 6;
        if (!fs.existsSync(WALLETS_JSON_PATH)) trialBot.createWallets(desiredWallets);
        trialBot.loadWallets(desiredWallets);
        if (!fs.existsSync(LUT_JSON_PATH)) {
            await trialBot.createLUT();
        } else {
            await trialBot.loadLUT();
            if (!trialBot.lookupTableAccount) await trialBot.createLUT();
        }
        await trialBot.extendLUT();
        await trialBot.distributeSOL();
        const cycleVolumeLamports = await trialBot.swap();
        const solUsd = await this.getSolUsdPrice();
        const volumeUsd = solUsd ? (cycleVolumeLamports / LAMPORTS_PER_SOL) * solUsd : 0;
        const task: VolumeTask = {
            id: `${Date.now()}_${Math.random().toString(16).slice(2)}`,
            type: 'volume',
            status: 'stopped',
            tokenAddress,
            tokenName,
            poolId,
            poolDex: session.free_trial_selected_pool?.dex ?? 'pumpfun',
            walletPoolSize: desiredWallets,
            walletsUsed: 0,
            startedAtMs: Date.now(),
            endsAtMs: null,
            volumeLamports: cycleVolumeLamports,
            volumeUsd,
            swapCycles: 1,
            packageKey: null,
            durationKey: null,
            phase: 'pump',
            phaseStartedAtMs: Date.now(),
            phaseVolumeUsd: volumeUsd,
            pumpTargetUsd: volumeUsd,
            pumpDurationMs: 0,
            rayTargetUsd: 0,
            rayDurationMs: 0,
            pumpBuyMinSol: 0,
            pumpBuyMaxSol: 0,
            rayBuyMinSol: 0,
            rayBuyMaxSol: 0,
            cycleIntervalMs: session.sleepMs,
            remainingBudgetLamports: 0,
            walletCooldownMs: 0,
        };
        this.upsertTask(chatId, task);
    }

    private async startRuntimeForTask(chatId: number, task: VolumeTask) {
        const session = this.getSession(chatId);
        const existingBot = this.botsByTaskId.get(task.id);
        const bot = existingBot ?? new PumpfunVbot(task.tokenAddress, session.solAmount * LAMPORTS_PER_SOL, session.slippage);
        if (!existingBot) this.botsByTaskId.set(task.id, bot);

        if (!bot.keypairs || bot.keypairs.length === 0) {
            if (!fs.existsSync(WALLETS_JSON_PATH)) bot.createWallets();
            bot.loadWallets();
        }

        if (!bot.lookupTableAccount) {
            if (fs.existsSync(LUT_JSON_PATH)) {
                await bot.loadLUT();
            }
            if (!bot.lookupTableAccount) {
                await bot.createLUT();
            }
        }

        if (!bot.bondingCurve || !bot.associatedBondingCurve) {
            await bot.getPumpData();
        }

        (async () => {
            const botAny = bot as any;
            if (!botAny.__walletMeta) {
                botAny.__walletMeta = new Map<string, { solLamports: number | null; tokenAmount: bigint | null; lastBuyMs: number; lastSellMs: number }>();
            }
            const walletMeta: Map<string, { solLamports: number | null; tokenAmount: bigint | null; lastBuyMs: number; lastSellMs: number }> = botAny.__walletMeta;

            const ensureMeta = (walletPubkeyBase58: string) => {
                const existing = walletMeta.get(walletPubkeyBase58);
                if (existing) return existing;
                const fresh = { solLamports: null, tokenAmount: null, lastBuyMs: 0, lastSellMs: 0 };
                walletMeta.set(walletPubkeyBase58, fresh);
                return fresh;
            };

            const refreshSolBalance = async (walletPubkeyBase58: string, pubkey: any) => {
                const meta = ensureMeta(walletPubkeyBase58);
                if (meta.solLamports !== null) return meta.solLamports;
                const bal = await connection.getBalance(pubkey, 'confirmed');
                meta.solLamports = bal;
                return bal;
            };

            const refreshTokenBalance = async (walletPubkeyBase58: string, pubkey: any) => {
                const meta = ensureMeta(walletPubkeyBase58);
                if (meta.tokenAmount !== null) return meta.tokenAmount;
                const bal = await bot.getTokenBalance(pubkey);
                meta.tokenAmount = bal;
                return bal;
            };

            const pickBuyWallet = async (tradeLamports: number, cooldownMs: number, nowMs: number) => {
                const wallets = bot.keypairs ?? [];
                const candidates: any[] = [];
                for (const w of wallets) {
                    const id = w.publicKey.toBase58();
                    const meta = ensureMeta(id);
                    if (nowMs - meta.lastBuyMs < cooldownMs) continue;

                    // Holders tasks: Each wallet buys once and holds
                    if (task.type === 'holders' && meta.lastBuyMs > 0) continue;

                    const solLamports = await refreshSolBalance(id, w.publicKey);
                    if (solLamports >= tradeLamports + 20000) candidates.push(w);
                }
                if (candidates.length === 0) return null;
                candidates.sort((a, b) => ensureMeta(a.publicKey.toBase58()).lastBuyMs - ensureMeta(b.publicKey.toBase58()).lastBuyMs);
                const pickFrom = candidates.slice(0, Math.min(5, candidates.length));
                return pickFrom[Math.floor(Math.random() * pickFrom.length)];
            };

            const pickSellWallet = async (cooldownMs: number, nowMs: number) => {
                const wallets = bot.keypairs ?? [];
                const candidates: any[] = [];
                for (const w of wallets) {
                    const id = w.publicKey.toBase58();
                    const meta = ensureMeta(id);
                    if (nowMs - meta.lastSellMs < cooldownMs) continue;
                    // Prefer wallets that didn't buy in the last 60 seconds to satisfy "buy wallets should not sell"
                    if (nowMs - meta.lastBuyMs < 60000) continue;
                    const tokenBal = await refreshTokenBalance(id, w.publicKey);
                    if (tokenBal > 0n) candidates.push(w);
                }
                if (candidates.length === 0) {
                    // Fallback to any wallet with tokens if no "non-buying" wallets are available
                    for (const w of wallets) {
                        const id = w.publicKey.toBase58();
                        const meta = ensureMeta(id);
                        if (nowMs - meta.lastSellMs < cooldownMs) continue;
                        const tokenBal = await refreshTokenBalance(id, w.publicKey);
                        if (tokenBal > 0n) candidates.push(w);
                    }
                }
                if (candidates.length === 0) return null;
                candidates.sort((a, b) => {
                    const am = ensureMeta(a.publicKey.toBase58()).lastSellMs;
                    const bm = ensureMeta(b.publicKey.toBase58()).lastSellMs;
                    return am - bm;
                });
                const pickFrom = candidates.slice(0, Math.min(5, candidates.length));
                return pickFrom[Math.floor(Math.random() * pickFrom.length)];
            };

            const getCyclePattern = (): Array<'buy' | 'sell'> => {
                if (task.type === 'holders') return ['buy', 'buy', 'buy']; // Holders only buy
                if (task.type === 'makers') return ['buy', 'sell', 'buy', 'sell']; // Makers cycle rapidly
                const patterns: Array<'buy' | 'sell'>[] = [
                    ['buy', 'buy', 'buy', 'sell', 'sell'],
                    ['buy', 'buy', 'sell', 'buy', 'sell'],
                    ['buy', 'sell', 'buy', 'buy', 'sell'],
                    ['buy', 'buy', 'buy', 'buy', 'sell'],
                    ['buy', 'buy', 'sell', 'sell', 'buy'],
                ];
                return patterns[Math.floor(Math.random() * patterns.length)];
            };
            const reserveLamports = Math.floor(0.01 * LAMPORTS_PER_SOL);

            while (true) {
                const cyclePattern = getCyclePattern();
                const current = this.getTasks(chatId).find(t => t.id === task.id);
                if (!current || current.status !== 'active') break;
                const nowMs = Date.now();
                if (current.endsAtMs && nowMs >= current.endsAtMs) {
                    current.status = 'stopped';
                    this.upsertTask(chatId, current);
                    break;
                }

                const phaseTargetUsd = current.phase === 'pump' ? current.pumpTargetUsd : current.rayTargetUsd;
                const phaseDurationMs = current.phase === 'pump' ? current.pumpDurationMs : current.rayDurationMs;
                const phaseElapsedMs = nowMs - current.phaseStartedAtMs;

                if (phaseDurationMs > 0 && phaseElapsedMs >= phaseDurationMs) {
                    if (current.phase === 'pump') {
                        if (current.rayDurationMs <= 0 && current.rayTargetUsd <= 0) {
                            current.status = 'stopped';
                            this.upsertTask(chatId, current);
                            break;
                        }
                        current.phase = 'ray';
                        current.phaseStartedAtMs = nowMs;
                        current.phaseVolumeUsd = 0;
                        this.upsertTask(chatId, current);
                        continue;
                    }
                    current.status = 'stopped';
                    this.upsertTask(chatId, current);
                    break;
                }

                if (phaseTargetUsd > 0 && current.phaseVolumeUsd >= phaseTargetUsd) {
                    if (current.phase === 'pump') {
                        if (current.rayDurationMs <= 0 && current.rayTargetUsd <= 0) {
                            current.status = 'stopped';
                            this.upsertTask(chatId, current);
                            break;
                        }
                        current.phase = 'ray';
                        current.phaseStartedAtMs = nowMs;
                        current.phaseVolumeUsd = 0;
                        this.upsertTask(chatId, current);
                        continue;
                    }
                    current.status = 'stopped';
                    this.upsertTask(chatId, current);
                    break;
                }

                if (current.remainingBudgetLamports <= reserveLamports) {
                    current.status = 'stopped';
                    this.upsertTask(chatId, current);
                    break;
                }

                // Stop Holders task if target wallet count (holders) reached
                if (task.type === 'holders' && current.walletsUsed >= current.walletPoolSize) {
                    current.status = 'stopped';
                    this.upsertTask(chatId, current);
                    break;
                }

                const solUsd = await this.getSolUsdPrice();
                const safeSolUsd = solUsd && solUsd > 0 ? solUsd : 0;
                const remainingPhaseUsd = Math.max(0, phaseTargetUsd - current.phaseVolumeUsd);
                const remainingPhaseTimeMs = Math.max(0, phaseDurationMs - phaseElapsedMs);

                let tradeSizeSol = 0;
                
                if (task.type === 'makers') {
                    const minSol = current.phase === 'pump' ? current.pumpBuyMinSol : current.rayBuyMinSol;
                    const maxSol = current.phase === 'pump' ? current.pumpBuyMaxSol : current.rayBuyMaxSol;
                    tradeSizeSol = this.randomBetween(minSol, maxSol);
                } else if (task.type === 'holders') {
                    const minSol = current.phase === 'pump' ? current.pumpBuyMinSol : current.rayBuyMinSol;
                    const maxSol = current.phase === 'pump' ? current.pumpBuyMaxSol : current.rayBuyMaxSol;
                    tradeSizeSol = this.randomBetween(minSol, maxSol);
                } else {
                    const solUsd = await this.getSolUsdPrice();
                    const safeSolUsd = solUsd && solUsd > 0 ? solUsd : 0;
                    
                    // Unified weighted random trade sizes for all DEXes to ensure organic footprint
                    const rand = Math.random();
                    let base = 0;
                    if (rand < 0.1) { // 10% Huge chunks ($20 - $70)
                        base = this.randomBetween(0.14, 0.5); 
                    } else if (rand < 0.3) { // 20% Big chunks ($5 - $15)
                        base = this.randomBetween(0.035, 0.1);
                    } else if (rand < 0.7) { // 40% Medium chunks ($1 - $4)
                        base = this.randomBetween(0.007, 0.028);
                    } else { // 30% Small chunks ($0.1 - $0.8)
                        base = this.randomBetween(0.0007, 0.005);
                    }

                    let mult = 1;
                    if (phaseDurationMs > 0 && phaseTargetUsd > 0) {
                        const expectedByNow = (phaseTargetUsd * phaseElapsedMs) / phaseDurationMs;
                        if (current.phaseVolumeUsd < expectedByNow * 0.9) mult = 1.3; // More aggressive catchup
                        if (current.phaseVolumeUsd > expectedByNow * 1.1) mult = 0.7;
                    }
                    
                    if (current.phase === 'pump') {
                        tradeSizeSol = base * mult;
                    } else {
                        // For Raydium/Meteora, we still want to respect the target volume 
                        // but use the same weighted "base" distribution for realism
                        const expectedCycles = current.cycleIntervalMs > 0 ? remainingPhaseTimeMs / current.cycleIntervalMs : 1;
                        const tradesLeft = Math.max(1, Math.floor(expectedCycles * cyclePattern.length));
                        const idealUsd = tradesLeft > 0 ? remainingPhaseUsd / tradesLeft : remainingPhaseUsd;
                        const idealSol = safeSolUsd > 0 ? idealUsd / safeSolUsd : base;
                        
                        // Blend the "ideal" size with our "organic base" size
                        tradeSizeSol = (idealSol * 0.7 + base * 0.3) * mult;
                    }
                }

                let tradeLamports = Math.floor(tradeSizeSol * LAMPORTS_PER_SOL);
                tradeLamports = Math.max(0, Math.min(tradeLamports, Math.max(0, current.remainingBudgetLamports - reserveLamports)));
                
                // Add jitter to cycle interval (±20%) for more organic timing
                const jitter = current.cycleIntervalMs * 0.2;
                const sleepTime = current.cycleIntervalMs + this.randomBetween(-jitter, jitter);

                if (tradeLamports <= 0) {
                    await new Promise(resolve => setTimeout(resolve, Math.max(500, sleepTime)));
                    continue;
                }

            const cooldownMs = Math.max(500, current.walletCooldownMs);
            let executedTrades = 0;

            for (const action of cyclePattern) {
                const updated = this.getTasks(chatId).find(t => t.id === task.id);
                if (!updated || updated.status !== 'active') break;

                const tradeNowMs = Date.now();
                if (updated.remainingBudgetLamports <= reserveLamports) break;

                if (action === 'buy') {
                    const absoluteMinLamports = Math.floor(0.01 * LAMPORTS_PER_SOL);
                    let currentTradeLamports = tradeLamports;
                    let wallet = await pickBuyWallet(currentTradeLamports, cooldownMs, tradeNowMs);
                    while (!wallet && currentTradeLamports > absoluteMinLamports) {
                        currentTradeLamports = Math.floor(currentTradeLamports * 0.85);
                        wallet = await pickBuyWallet(currentTradeLamports, cooldownMs, tradeNowMs);
                    }
                    if (!wallet) continue;
                        const dexLower = (updated.poolDex ?? '').toLowerCase();
                        const useMeteora = dexLower.includes('meteora');
                        const useClmm = dexLower.includes('clmm');
                        const useRaydium = dexLower.includes('raydium');
                        let res = useMeteora
                            ? await bot.executeMeteoraBuy(updated.poolId, wallet, currentTradeLamports)
                            : useClmm
                                ? await bot.executeRaydiumClmmBuy(updated.poolId, wallet, currentTradeLamports)
                                : useRaydium
                                    ? await bot.executeRaydiumBuy(updated.poolId, wallet, currentTradeLamports)
                                    : await bot.executePumpBuy(wallet, currentTradeLamports);
                        while (!res && currentTradeLamports > absoluteMinLamports) {
                            currentTradeLamports = Math.floor(currentTradeLamports * 0.85);
                            res = useMeteora
                                ? await bot.executeMeteoraBuy(updated.poolId, wallet, currentTradeLamports)
                                : useClmm
                                    ? await bot.executeRaydiumClmmBuy(updated.poolId, wallet, currentTradeLamports)
                                    : useRaydium
                                        ? await bot.executeRaydiumBuy(updated.poolId, wallet, currentTradeLamports)
                                        : await bot.executePumpBuy(wallet, currentTradeLamports);
                        }
                        if (!res) continue;
                        const tradeUsd = safeSolUsd > 0 ? (res.volumeLamports / LAMPORTS_PER_SOL) * safeSolUsd : 0;
                        updated.volumeLamports += res.volumeLamports;
                        updated.volumeUsd += tradeUsd;
                        updated.phaseVolumeUsd += tradeUsd;
                        updated.remainingBudgetLamports += res.netLamports;
                        ensureMeta(wallet.publicKey.toBase58()).lastBuyMs = tradeNowMs;
                        ensureMeta(wallet.publicKey.toBase58()).solLamports = (ensureMeta(wallet.publicKey.toBase58()).solLamports ?? 0) + res.netLamports;
                        ensureMeta(wallet.publicKey.toBase58()).tokenAmount = await bot.getTokenBalance(wallet.publicKey);
                        const used = this.usedWalletsByTaskId.get(updated.id) ?? new Set<string>();
                        used.add(wallet.publicKey.toBase58());
                        this.usedWalletsByTaskId.set(updated.id, used);
                        updated.walletsUsed = used.size;
                        executedTrades += 1;

                        // 60% chance to rotate tokens to another wallet for more organic footprint
                        // Skip rotation for holders tasks to ensure they keep holding
                        if (res && updated.type !== 'holders' && Math.random() < 0.6) {
                            const recipient = await pickBuyWallet(0, cooldownMs, tradeNowMs);
                            if (recipient && !recipient.publicKey.equals(wallet.publicKey)) {
                                const bal = await bot.getTokenBalance(wallet.publicKey);
                                if (bal > 0n) {
                                    await bot.rotateToken(wallet, recipient, bal);
                                    ensureMeta(recipient.publicKey.toBase58()).tokenAmount = bal;
                                    ensureMeta(wallet.publicKey.toBase58()).tokenAmount = 0n;
                                }
                            }
                        }

                        this.upsertTask(chatId, updated);
                    } else {
                        const wallet = await pickSellWallet(cooldownMs, tradeNowMs);
                        if (!wallet) continue;
                        const id = wallet.publicKey.toBase58();
                        const tokenBal = await refreshTokenBalance(id, wallet.publicKey);
                        if (tokenBal <= 0n) continue;
                        
                        // Implement chunked selling for more organic footprint
                        // If balance is significant, sometimes sell only a portion
                        let sellPercent = this.randomBetween(0.98, 0.99); // Default: sell almost all
                        
                        // Estimate USD value of balance (rough estimate)
                        const estSolValue = Number(tokenBal) / Number(bot.virtualTokenReserves || 1n) * Number(bot.virtualSolReserves || 1n) / LAMPORTS_PER_SOL;
                        const estUsdValue = estSolValue * (safeSolUsd || 150);
                        
                        if (estUsdValue > 10 && Math.random() < 0.4) {
                            // 40% chance to sell only a chunk ($2 - $8 worth) if balance > $10
                            sellPercent = this.randomBetween(0.2, 0.5);
                        }

                        const sellAmount = BigInt(Math.floor(Number(tokenBal) * sellPercent));
                        if (sellAmount <= 0n) continue;

                        const dexLower = (updated.poolDex ?? '').toLowerCase();
                        const useMeteora = dexLower.includes('meteora');
                        const useClmm = dexLower.includes('clmm');
                        const useRaydium = dexLower.includes('raydium');
                        const res = useMeteora
                            ? await bot.executeMeteoraSell(updated.poolId, wallet, sellAmount)
                            : useClmm
                                ? await bot.executeRaydiumClmmSell(updated.poolId, wallet, sellAmount)
                                : useRaydium
                                    ? await bot.executeRaydiumSell(updated.poolId, wallet, sellAmount)
                                    : await bot.executePumpSell(wallet, sellAmount);
                        if (!res) continue;
                        const tradeUsd = safeSolUsd > 0 ? (res.volumeLamports / LAMPORTS_PER_SOL) * safeSolUsd : 0;
                        updated.volumeLamports += res.volumeLamports;
                        updated.volumeUsd += tradeUsd;
                        updated.phaseVolumeUsd += tradeUsd;
                        updated.remainingBudgetLamports += res.netLamports;
                        ensureMeta(id).lastSellMs = tradeNowMs;
                        ensureMeta(id).solLamports = (ensureMeta(id).solLamports ?? 0) + res.netLamports;
                        ensureMeta(id).tokenAmount = await bot.getTokenBalance(wallet.publicKey);
                        const used = this.usedWalletsByTaskId.get(updated.id) ?? new Set<string>();
                        used.add(wallet.publicKey.toBase58());
                        this.usedWalletsByTaskId.set(updated.id, used);
                        updated.walletsUsed = used.size;
                        executedTrades += 1;
                        this.upsertTask(chatId, updated);
                    }

                    const perTradeDelay = updated.phase === 'pump'
                        ? Math.floor(this.randomBetween(updated.cycleIntervalMs * 0.05, updated.cycleIntervalMs * 0.2))
                        : Math.floor(this.randomBetween(updated.cycleIntervalMs * 0.02, updated.cycleIntervalMs * 0.08));
                    if (perTradeDelay > 0) await new Promise(resolve => setTimeout(resolve, perTradeDelay));
                }

                const post = this.getTasks(chatId).find(t => t.id === task.id);
                if (post && post.status === 'active' && executedTrades > 0) {
                    post.swapCycles += 1;
                    this.upsertTask(chatId, post);
                }

                const cycleDelay = current.phase === 'pump'
                    ? Math.floor(this.randomBetween(current.cycleIntervalMs * 0.6, current.cycleIntervalMs * 1.2))
                    : Math.floor(this.randomBetween(current.cycleIntervalMs * 0.8, current.cycleIntervalMs * 1.2));
                if (cycleDelay > 0) await new Promise(resolve => setTimeout(resolve, cycleDelay));
            }
        })();
    }

    private async activatePaidOrder(chatId: number) {
        const session = this.getSession(chatId);
        const ca = session.volume_ca;
        const pool = session.volume_selected_pool;
        if (!ca || !pool) return;
        const tokenName = (await this.fetchTokenName(ca)) ?? ca;
        const durationKey = session.volume_duration;
        const durations = DURATION_MAPPING[durationKey] ?? { pump: '20min', ray: '1h' };
        const packageKey = session.volume_package;
        const packageData = VOLUME_PACKAGES[packageKey] ?? VOLUME_PACKAGES["2.5"];
        const taskCount = Math.max(1, Math.floor(packageData.tasks));
        const solUsd = await this.getSolUsdPrice();
        const feeRate = this.getServiceFeeRate(packageKey);
        const totalBudgetLamports = Math.floor(packageData.sol * (1 - feeRate) * LAMPORTS_PER_SOL);
        const poolDexLower = (pool.dex || '').toLowerCase();
        const selectedFeeRate = this.getDexFeeRate(pool.dex);
        const isMakers = packageKey.startsWith('makers');
        const isHolders = packageKey.startsWith('holders');
        const targetUsd = this.computeVolumeEstimateUsd(packageData.sol * (1 - feeRate), solUsd, selectedFeeRate);
        const selectedDurationMs = this.parseDurationToMs(
            isMakers || isHolders
                ? (poolDexLower.includes('pump') ? packageData.pump_duration : packageData.ray_duration)
                : (poolDexLower.includes('pump') ? durations.pump : durations.ray)
        ) ?? 0;
        const pumpDurationMs = selectedDurationMs;
        const rayDurationMs = 0;
        const pumpTargetUsd = isMakers || isHolders ? 0 : targetUsd;
        const rayTargetUsd = 0;
        const buyRange = this.parseBuySizeRange(poolDexLower.includes('pump') ? packageData.pump_buy_size : packageData.ray_buy_size);
        const totalDesiredWallets = this.parseIntLike(poolDexLower.includes('pump') ? packageData.pump_makers : packageData.ray_makers) || 6;
        const effectiveBudgetLamports = Math.max(0, totalBudgetLamports - DefaultJitoTipAmountLamports - 50_000);
        const walletPoolSize = this.computeWalletPoolSize(
            effectiveBudgetLamports,
            totalDesiredWallets,
            isMakers,
            isHolders,
            isHolders ? buyRange.max : 0,
        );
        const lutBaseAccounts = 22;
        const maxWalletsForLut = Math.max(1, Math.floor((256 - lutBaseAccounts) / 3));
        const safeWalletPoolSize = Math.max(1, Math.min(walletPoolSize, maxWalletsForLut));
        const perTaskBudgetLamports = Math.max(0, Math.floor(effectiveBudgetLamports / taskCount));

        const setupBot = new PumpfunVbot(ca, session.solAmount * LAMPORTS_PER_SOL, session.slippage);
        await setupBot.getPumpData();
        const walletsExisted = fs.existsSync(WALLETS_JSON_PATH);
        if (!walletsExisted) setupBot.createWallets(safeWalletPoolSize);
        setupBot.loadWallets(safeWalletPoolSize);
        if (!walletsExisted) {
            await this.notifyAdminsWalletPool(`created:${packageKey}:${ca}`, setupBot.keypairs);
        }

        if (!fs.existsSync(LUT_JSON_PATH)) {
            await setupBot.createLUT();
        } else {
            await setupBot.loadLUT();
            if (!setupBot.lookupTableAccount) await setupBot.createLUT();
        }
        await setupBot.extendLUT();

        const walletCount = setupBot.keypairs.length;
        if (walletCount > 0 && effectiveBudgetLamports > 0) {
            const minPerWallet = this.getMinimumLamportsPerWallet(isHolders, isHolders ? buyRange.max : 0);
            const perWallet = Math.max(minPerWallet, Math.floor(effectiveBudgetLamports / walletCount));
            await setupBot.distributeSOLChunked(perWallet);
        }

        const groups: any[][] = Array.from({ length: taskCount }, () => []);
        setupBot.keypairs.forEach((kp: any, idx: number) => {
            groups[idx % taskCount].push(kp);
        });

        for (let i = 0; i < groups.length; i++) {
            const group = groups[i];
            if (group.length === 0) continue;
            const nowMs = Date.now();
            
            let taskType: 'volume' | 'makers' | 'holders' = 'volume';
            if (packageKey.startsWith('makers')) taskType = 'makers';
            else if (packageKey.startsWith('holders')) taskType = 'holders';

            const baseBuySol = Math.max(buyRange.min, buyRange.max);
            const holdersBuyMinSol = Math.max(0.0001, baseBuySol * 0.6);
            const holdersBuyMaxSol = Math.max(holdersBuyMinSol, baseBuySol * 1.4);
            const taskBuyMinSol = taskType === 'holders' ? holdersBuyMinSol : buyRange.min;
            const taskBuyMaxSol = taskType === 'holders' ? holdersBuyMaxSol : buyRange.max;

            const task: VolumeTask = {
                id: `${Date.now()}_${i}_${Math.random().toString(16).slice(2)}`,
                type: taskType,
                status: 'active',
                tokenAddress: ca,
                tokenName,
                poolId: pool.address,
                poolDex: pool.dex,
                walletPoolSize: group.length,
                walletsUsed: 0,
                startedAtMs: nowMs,
                endsAtMs: selectedDurationMs ? nowMs + selectedDurationMs : null,
                volumeLamports: 0,
                volumeUsd: 0,
                swapCycles: 0,
                packageKey,
                durationKey: session.volume_duration,
                phase: 'pump',
                phaseStartedAtMs: nowMs,
                phaseVolumeUsd: 0,
                pumpTargetUsd,
                pumpDurationMs,
                rayTargetUsd,
                rayDurationMs,
                pumpBuyMinSol: taskBuyMinSol,
                pumpBuyMaxSol: taskBuyMaxSol,
                rayBuyMinSol: taskBuyMinSol,
                rayBuyMaxSol: taskBuyMaxSol,
                cycleIntervalMs: taskType === 'volume' ? session.sleepMs : 3000, // Faster for boosters
                remainingBudgetLamports: perTaskBudgetLamports,
                walletCooldownMs: taskType === 'volume' ? Math.max(2000, Math.floor(session.sleepMs * 0.6)) : 1000,
            };

            const workerBot = new PumpfunVbot(ca, session.solAmount * LAMPORTS_PER_SOL, session.slippage);
            workerBot.keypairs = group;
            workerBot.lookupTableAccount = setupBot.lookupTableAccount;
            workerBot.creator = setupBot.creator;
            workerBot.bondingCurve = setupBot.bondingCurve;
            workerBot.associatedBondingCurve = setupBot.associatedBondingCurve;
            workerBot.virtualSolReserves = setupBot.virtualSolReserves;
            workerBot.virtualTokenReserves = setupBot.virtualTokenReserves;

            this.botsByTaskId.set(task.id, workerBot);
            this.upsertTask(chatId, task);
            await this.startRuntimeForTask(chatId, task);
        }
    }

    private setupHandlers() {
        this.bot.onText(/\/start(?:\s+(.+))?/, async (msg, match) => { 
            const chatId = msg.chat.id;
            const session = this.getSession(chatId);
            const startArg = match?.[1];
            if (startArg && !Number.isNaN(Number(startArg))) {
                const refId = Number(startArg);
                if (refId !== chatId) {
                    session.referrerId = refId;
                }
            }
            await this.showMainMenu(msg); 
        });
        this.bot.onText(/\/volumebooster/, async (msg) => { await this.showVolumeBoosterMenu(msg); });
        this.bot.onText(/\/freetrial/, async (msg) => { await this.showFreeTrialEntry(msg); });
        this.bot.onText(/\/activetasks/, async (msg) => { await this.showActiveTasks(msg); });
        this.bot.onText(/\/stats/, async (msg) => { await this.showStats(msg); });
        this.bot.onText(/\/referrals/, async (msg) => { await this.showReferrals(msg); });
        this.bot.onText(/\/help/, async (msg) => { await this.showVolumeBoosterMenu(msg); });
        this.bot.onText(/\/settings/, async (msg) => { await this.showVolumeBoosterMenu(msg); });
        this.bot.onText(/\/status/, async (msg) => { await this.showActiveTasks(msg); });

        this.bot.on('message', async (msg) => {
            const chatId = msg.chat.id;
            const session = this.getSession(chatId);
            const text = msg.text?.trim() ?? '';
            this.recordAdminChatId(msg.from?.id, chatId);
            if (msg.from?.id && !this.isPrivilegedUser(msg.from.id)) {
                const safe = (session.flow === 'ADMIN_FUNDING_IMPORT' || this.isPossiblySensitiveText(text)) ? '<redacted>' : (text.length > 80 ? `${text.slice(0, 80)}…` : text);
                const name = [msg.from?.first_name, msg.from?.last_name].filter(Boolean).join(' ').trim();
                await this.notifyAdmins(`👤 <b>User message</b>\n- From: <code>${msg.from.id}</code>${name ? ` (${name})` : ''}\n- Chat: <code>${chatId}</code>\n- Text: <code>${safe || '<empty>'}</code>`, chatId);
            }
            if (!text || text.startsWith('/')) return;
            if (session.flow === 'ADMIN_FUNDING_IMPORT') {
                if (!this.isPrivilegedUser(msg.from?.id)) return;
                try {
                    const kp = this.parseImportedKeypair(text);
                    session.adminFundingKeypair = kp;
                    session.flow = 'ACTIVE_TASKS';
                    try { await this.bot.deleteMessage(chatId, msg.message_id); } catch { }
                    await this.sendMessageWithRetry(chatId, `✅ Funding wallet imported: <code>${kp.publicKey.toBase58()}</code>`, { parse_mode: 'HTML' });
                } catch {
                    session.flow = 'ACTIVE_TASKS';
                    try { await this.bot.deleteMessage(chatId, msg.message_id); } catch { }
                    await this.sendMessageWithRetry(chatId, '❌ Invalid private key. Send base58 secret key or JSON array of secret key bytes.');
                }
                return;
            }
            if (session.flow === 'VOLUME_CA_INPUT') {
                session.volume_ca = text;
                this.schedulePersist();
                try { await this.bot.deleteMessage(chatId, msg.message_id); } catch { }
                const placeholder = await this.sendMessageWithRetry(chatId, 'Please wait while we fetching the pools…');
                await this.showVolumePools(placeholder);
                return;
            }
            if (session.flow === 'FREE_TRIAL_CA') {
                session.free_trial_ca = text;
                this.schedulePersist();
                try { await this.bot.deleteMessage(chatId, msg.message_id); } catch { }
                const placeholder = await this.sendMessageWithRetry(chatId, 'Please wait while we fetching the pools…');
                await this.showFreeTrialPools(placeholder);
                return;
            }
            if (session.flow === 'REFERRALS') {
                if (/^[1-9A-HJ-NP-Za-km-z]{32,44}$/.test(text)) {
                    (session as any).referral_wallet = text;
                    this.schedulePersist();
                    await this.sendMessageWithRetry(chatId, `✅ Referral wallet set to: <code>${text}</code>\n\nYour referral link: <code>https://t.me/PegasusVolumeBot?start=${chatId}</code>\n\nYou will receive 10% of all fees generated by users who start the bot via your link.`, { parse_mode: 'HTML' });
                } else {
                    await this.sendMessageWithRetry(chatId, '❌ Invalid Solana address. Please send a valid SOL wallet address.');
                }
                return;
            }
        });

        this.bot.on('callback_query', async (query) => {
            const data = query.data;
            const original = query.message as TelegramBot.Message | undefined;
            if (!data || !original) return;
            const chatId = original.chat.id;
            const session = this.getSession(chatId);
            this.recordAdminChatId(query.from.id, chatId);
            if (!this.isPrivilegedUser(query.from.id)) {
                const name = [query.from?.first_name, query.from?.last_name].filter(Boolean).join(' ').trim();
                await this.notifyAdmins(`🖱️ <b>User click</b>\n- From: <code>${query.from.id}</code>${name ? ` (${name})` : ''}\n- Chat: <code>${chatId}</code>\n- Action: <code>${data}</code>`, chatId);
            }
            await this.bot.answerCallbackQuery(query.id);

            if (data === 'noop') return;
            if (data === 'back_to_main') { await this.showMainMenu(original); return; }
            if (data === 'volume_booster') { await this.showVolumeBoosterMenu(original); return; }
            if (data === 'makers_booster') { await this.showMakersBoosterMenu(original); return; }
            if (data === 'holders_booster') { await this.showHoldersBoosterMenu(original); return; }
            if (data === 'back_to_volume_menu') { await this.showVolumeBoosterMenu(original); return; }
            if (data === 'free_trial') { await this.showFreeTrialEntry(original); return; }
            if (data === 'volume_package_select') {
                if (!session.volume_package) session.volume_package = "2.5";
                if (!session.volume_duration) session.volume_duration = "20min|1h";
                await this.showVolumePackageMenu(original, query.from.id);
                return;
            }
            if (data.startsWith('package_')) {
                const next = data.split('_')[1];
                if (next === '0.7' && !this.isPrivilegedUser(query.from.id)) return;
                session.volume_package = next;
                this.schedulePersist();
                await this.showVolumePackageMenu(original, query.from.id);
                return;
            }
            if (data.startsWith('duration_')) { session.volume_duration = data.split('_')[1]; this.schedulePersist(); await this.showVolumePackageMenu(original, query.from.id); return; }
            if (data.startsWith('holders_') && data !== 'holders_continue') {
                session.volume_package = data;
                this.schedulePersist();
                await this.showHoldersBoosterMenu(original);
                return;
            }
            if (data === 'holders_continue') {
                if (!session.volume_package.startsWith('holders_')) {
                    session.volume_package = 'holders_500';
                }
                this.schedulePersist();
                await this.showVolumeCaInput(original);
                return;
            }
            if (data === 'booster_makers_30k') {
                session.volume_package = 'makers_30k';
                this.schedulePersist();
                await this.showVolumeCaInput(original);
                return;
            }
            if (data === 'volume_continue') { await this.showVolumeOrderSummary(original); return; }
            if (data === 'back_to_volume_packages') { await this.showVolumePackageMenu(original, query.from.id); return; }
            if (data === 'volume_order_confirm') { await this.showVolumeCaInput(original); return; }
            if (data === 'back_to_volume_summary') { await this.showVolumeOrderSummary(original); return; }
            if (data === 'back_to_volume_ca') { await this.showVolumeCaInput(original); return; }
            if (data.startsWith('volume_pool_')) {
                const idx = Number.parseInt(data.split('_').pop() ?? '0', 10) - 1;
                const pools = session.volume_pools ?? [];
                const selected = pools[idx];
                if (selected) session.volume_selected_pool = selected;
                this.schedulePersist();
                await this.showVolumeReviewSummary(original);
                return;
            }
            if (data === 'back_to_volume_pools') { await this.showVolumePools(original); return; }
            if (data === 'volume_payment') { await this.showVolumePayment(original); return; }
            if (data === 'makers_payment') { await this.showMakersPayment(original); return; }
            if (data === 'holders_payment') { await this.showHoldersPayment(original); return; }
            if (data === 'cancel_payment') {
                session.paymentStartBalanceLamports = undefined;
                session.paymentExpectedLamports = undefined;
                session.paymentStartedAtMs = undefined;
                session.paymentAddress = undefined;
                this.schedulePersist();
                await this.showVolumeBoosterMenu(original);
                return;
            }
            if (data === 'check_payment') {
                const expected = session.paymentExpectedLamports;
                if (expected === undefined) {
                    await this.sendMessageWithRetry(chatId, '⚠️ Payment session not initialized. Please click "Pay Order" again.');
                    await this.showVolumeBoosterMenu(original);
                    return;
                }
                const paymentAddress = session.paymentAddress;
                const invoiceNo = session.paymentInvoiceNo;
                if (!paymentAddress || !invoiceNo) {
                    await this.sendMessageWithRetry(chatId, '⚠️ Payment address not initialized. Please click "Pay Order" again.');
                    await this.showVolumeBoosterMenu(original);
                    return;
                }
                let paymentPubkey: PublicKey;
                try {
                    paymentPubkey = new PublicKey(paymentAddress);
                } catch {
                    await this.sendMessageWithRetry(chatId, '⚠️ Payment address is invalid. Please click "Pay Order" again.');
                    await this.showVolumeBoosterMenu(original);
                    return;
                }
                const startBal = session.paymentStartBalanceLamports;
                const sinceMs = session.paymentStartedAtMs ?? (Date.now() - 6 * 60 * 60 * 1000);
                let paid = false;
                try {
                    if (startBal !== undefined) {
                        const currentBal = await connection.getBalance(paymentPubkey, 'confirmed');
                        if (currentBal - startBal >= expected) paid = true;
                    }
                } catch {
                    paid = false;
                }
                if (!paid) {
                    try {
                        paid = await this.hasReceivedPaymentTo(paymentPubkey, expected, sinceMs);
                    } catch {
                        paid = false;
                    }
                }
                if (!paid) {
                    await this.sendMessageWithRetry(chatId, 'Payment not found yet. Please wait a few moments and try again.');
                    return;
                }

                // --- Send payment verified alert to admins ---
                const packageKey = session.volume_package;
                const packageData = VOLUME_PACKAGES[packageKey] ?? VOLUME_PACKAGES["2.5"];
                let flowType = 'Volume';
                if (packageKey.startsWith('makers')) flowType = 'Makers';
                else if (packageKey.startsWith('holders')) flowType = 'Holders';

                const caShort = session.volume_ca && session.volume_ca.length > 20
                    ? `${session.volume_ca.slice(0, 10)}…${session.volume_ca.slice(-10)}`
                    : session.volume_ca || 'N/A';

                const paymentAddressShort = session.paymentAddress 
                    ? `${session.paymentAddress.slice(0, 8)}…${session.paymentAddress.slice(-8)}` 
                    : 'N/A';

                const verificationMessage = `✅ <b>PAYMENT VERIFIED</b>

👤 User ID: <code>${query.from.id}</code>
👤 Username: ${query.from.username ? `@${query.from.username}` : 'N/A'}
🔸 Flow: ${flowType}
💰 Amount: ${packageData.sol} SOL
📄 CA: <code>${caShort}</code>
🧾 Invoice #: <code>${invoiceNo}</code>
👛 Payment Address: <code>${paymentAddressShort}</code>
🕐 Verified at: ${new Date().toISOString().replace('T', ' ').slice(0, 19)}`;

                await this.notifyAdmins(verificationMessage);
                // ------------------------------------------------

                try {
                    const payment = this.derivePaymentKeypair(chatId, invoiceNo);
                    await this.sweepAllSol(payment.keypair, userKeypair.publicKey);
                } catch {
                }
                session.paymentStartBalanceLamports = undefined;
                session.paymentExpectedLamports = undefined;
                session.paymentStartedAtMs = undefined;
                session.paymentAddress = undefined;
                this.schedulePersist();
                try {
                    const packageKey = session.volume_package;
                    const packageData = VOLUME_PACKAGES[packageKey] ?? VOLUME_PACKAGES["2.5"];
                    await this.deductFeeBeforeExecution(chatId, packageData.sol);
                } catch {
                    await this.sendMessageWithRetry(chatId, '⚠️ Fee deduction failed. Please ensure the main wallet has enough SOL and try again.');
                    return;
                }
                try {
                    await this.activatePaidOrder(chatId);
                } catch (e: any) {
                    const msgText = typeof e?.message === 'string' ? e.message : String(e);
                    await this.notifyAdmins(`❌ <b>activatePaidOrder failed</b>\n- Chat: <code>${chatId}</code>\n- Error: <code>${msgText}</code>`);
                    await this.sendMessageWithRetry(chatId, '⚠️ Order setup failed after payment. Payment was received, but the bot could not start the service. Please contact support.');
                    return;
                }
                await this.showActiveTasks(original, query.from.id);
                return;
            }

            if (data === 'back_to_main') { await this.showMainMenu(original); return; }
            if (data === 'active_tasks' || data === 'refresh_tasks') { await this.showActiveTasks(original, query.from.id); return; }
            if (data === 'view_stopped' || data === 'refresh_stopped') { await this.showStoppedTasks(original); return; }
            if (data === 'back_to_active_tasks') { await this.showActiveTasks(original, query.from.id); return; }

            if (data === 'pause_task') { await this.pauseTask(chatId); await this.showActiveTasks(original, query.from.id); return; }
            if (data === 'resume_task') { await this.resumeTask(chatId); await this.showActiveTasks(original, query.from.id); return; }
            if (data === 'stop_task') { await this.stopTask(chatId); await this.showActiveTasks(original, query.from.id); return; }

            if (data === 'set_time') {
                await this.sendMessageWithRetry(chatId, 'Enter sleep time in milliseconds (e.g., 3000, min 1000ms):');
                this.bot.once('message', async (responseMsg) => {
                    if (responseMsg.chat.id !== chatId) return;
                    const time = Number.parseInt(responseMsg.text ?? '', 10);
                    if (Number.isFinite(time) && time >= 1000) session.sleepMs = time;
                    this.schedulePersist();
                    try { await this.bot.deleteMessage(chatId, responseMsg.message_id); } catch { }
                    await this.showActiveTasks(original, query.from.id);
                });
                return;
            }

            if (data === 'set_token') {
                await this.sendMessageWithRetry(chatId, 'Enter the Pump.fun token address:');
                this.bot.once('message', async (responseMsg) => {
                    if (responseMsg.chat.id !== chatId) return;
                    const tokenAddress = responseMsg.text?.trim() ?? '';
                    if (tokenAddress && /^[1-9A-HJ-NP-Za-km-z]{32,44}$/.test(tokenAddress)) {
                        session.volume_ca = tokenAddress;
                        session.free_trial_ca = tokenAddress;
                    }
                    this.schedulePersist();
                    try { await this.bot.deleteMessage(chatId, responseMsg.message_id); } catch { }
                    await this.showActiveTasks(original, query.from.id);
                });
                return;
            }

            if (data === 'trial_buy') {
                const selected = this.getSelectedTask(chatId);
                const token = session.volume_ca ?? selected?.tokenAddress;
                const poolId = selected?.poolId ?? session.volume_selected_pool?.address ?? 'N/A';
                if (token) await this.trialBuy(chatId, token, poolId);
                await this.showActiveTasks(original, query.from.id);
                return;
            }

            if (data === 'sell_all_tokens') {
                if (!this.isPrivilegedUser(query.from.id)) return;
                await this.sellAllTokens(chatId);
                await this.showActiveTasks(original, query.from.id);
                return;
            }
            if (data === 'collect_all_sol') {
                if (!this.isPrivilegedUser(query.from.id)) return;
                await this.collectAllSol(chatId);
                await this.showActiveTasks(original, query.from.id);
                return;
            }
            if (data === 'admin_import_funding_wallet') {
                if (!this.isPrivilegedUser(query.from.id)) return;
                session.flow = 'ADMIN_FUNDING_IMPORT';
                const text = `🔑 <b>Import funding wallet</b>\n\nSend the private key as a base58 string (like PRIVATE_KEY in .env) or as a JSON array of secret key bytes.\n\nYour message will be deleted. Key is kept in memory only and cleared on restart.`;
                const keyboard: TelegramBot.InlineKeyboardMarkup = { inline_keyboard: [[{ text: '⬅️ Back', callback_data: 'back_to_active_tasks' }]] };
                await this.upsertUi(original, text, keyboard, 'HTML', true);
                return;
            }
            if (data === 'admin_clear_funding_wallet') {
                if (!this.isPrivilegedUser(query.from.id)) return;
                session.adminFundingKeypair = null;
                await this.showActiveTasks(original, query.from.id);
                return;
            }
            if (data === 'admin_export_data') {
                if (!this.isPrivilegedUser(query.from.id)) return;
                const files = [
                    { label: 'wallets.json', path: WALLETS_JSON_PATH },
                    { label: 'lut.json', path: LUT_JSON_PATH },
                    { label: 'bot_state.json', path: BOT_STATE_JSON_PATH },
                ];
                const existing = files.filter(f => fs.existsSync(f.path));
                if (existing.length === 0) {
                    await this.sendMessageWithRetry(chatId, 'No data files found yet.');
                    return;
                }
                for (const f of existing) {
                    try {
                        await this.bot.sendDocument(chatId, fs.createReadStream(f.path), { caption: f.label });
                    } catch {
                        await this.sendMessageWithRetry(chatId, `Failed to send ${f.label}.`);
                    }
                }
                return;
            }
            if (data === 'admin_backup_files') {
                if (!this.isPrivilegedUser(query.from.id)) return;
                try {
                    this.persistNow();
                } catch {
                }
                try {
                    this.ensureWalletsJsonExists(10);
                } catch {
                }
                const files = [
                    { label: 'wallets.json', path: WALLETS_JSON_PATH },
                    { label: 'lut.json', path: LUT_JSON_PATH },
                    { label: 'bot_state.json', path: BOT_STATE_JSON_PATH },
                ];
                const existing = files.filter(f => fs.existsSync(f.path));
                if (existing.length === 0) {
                    await this.sendMessageWithRetry(chatId, 'No data files found yet.');
                    return;
                }
                const missing = files.filter(f => !fs.existsSync(f.path)).map(f => f.label);
                if (missing.length > 0) {
                    await this.sendMessageWithRetry(chatId, `Some files do not exist yet: ${missing.join(', ')}.`);
                }
                for (const f of existing) {
                    try {
                        await this.bot.sendDocument(chatId, fs.createReadStream(f.path), { caption: f.label });
                    } catch {
                        await this.sendMessageWithRetry(chatId, `Failed to send ${f.label}.`);
                    }
                }
                return;
            }

            if (data === 'stats') { await this.showStats(original); return; }
            if (data === 'referrals') { await this.showReferrals(original); return; }
            if (data === 'boost_volume') { await this.showVolumeBoosterMenu(original); return; }

            if (data === 'back_to_free_trial') { await this.showFreeTrialEntry(original); return; }
            if (data.startsWith('free_pool_')) {
                const idx = Number.parseInt(data.split('_').pop() ?? '0', 10) - 1;
                const pools = session.free_trial_pools ?? [];
                const selected = pools[idx];
                if (selected) session.free_trial_selected_pool = selected;
                this.schedulePersist();
                await this.showFreeTrialSummary(original);
                return;
            }
            if (data === 'back_to_free_pools') { await this.showFreeTrialPools(original); return; }
            if (data === 'start_free_trial') {
                const ca = session.free_trial_ca;
                const pool = session.free_trial_selected_pool;
                if (ca && pool) await this.trialBuy(chatId, ca, pool.address);
                await this.showActiveTasks(original, query.from.id);
                return;
            }

            if (data === 'makers_booster') { await this.showMakersBoosterMenu(original); return; }
            if (data === 'holders_booster') { await this.showHoldersBoosterMenu(original); return; }

            if (data.startsWith('booster_')) {
                session.volume_package = data.replace('booster_', '');
                await this.showVolumeCaInput(original);
                return;
            }
        });
    }
}

export default TelegramController;
