/**
 * Axelar gas top-up keeper job.
 *
 * For Axelar GMP transfers, if gas prices rise or the origin transaction paid insufficient gas,
 * the Axelar relayer halts execution until gas is topped up via Axelar GasService.
 * This keeper job identifies pending Axelar transfers flagged with insufficient gas and tops up
 * gas to allow execution to proceed.
 */
import { createPublicClient, createWalletClient, http, isHex, pad } from "viem";
import { privateKeyToAccount } from "viem/accounts";
import { CHAINS, isChainKey, isEvmChain } from "@hyperion/protocol";

import { bigintOf, text, textOrNull } from "../../db/rows.js";
import type { GasTopupPayload, GasTopupResult, KeeperContext } from "../types.js";

const DEFAULT_LIMIT = 20;
const DEFAULT_TOPUP_AMOUNT = 100_000_000_000_000n; // 0.0001 ETH
const DEFAULT_MAX_SPENDING_LIMIT = 10_000_000_000_000_000n; // 0.01 ETH
const DEFAULT_AXELAR_GAS_SERVICE = "0xbE406F0189A0B4cf3A05C286473D23791Dd44Cc6";

export const AXELAR_GAS_SERVICE_ABI = [
  {
    type: "function",
    name: "addNativeGas",
    inputs: [
      { name: "txHash", type: "bytes32" },
      { name: "logIndex", type: "uint256" },
      { name: "refundAddress", type: "address" },
    ],
    outputs: [],
    stateMutability: "payable",
  },
] as const;

export interface UnderfundedTransfer {
  readonly transferId: bigint;
  readonly originChain: string;
  readonly destinationChain: string;
  readonly nonce: bigint;
  readonly originTx: string;
  readonly railReference: string | null;
  readonly railStatus: string | null;
}

function formatBytes32(raw: string): `0x${string}` {
  if (isHex(raw)) {
    const clean = raw.startsWith("0x") ? raw.slice(2) : raw;
    if (clean.length === 64) {
      return `0x${clean}`;
    }
    return pad(`0x${clean}`, { size: 32 });
  }
  const hex = Buffer.from(raw, "utf8").toString("hex");
  return pad(`0x${hex}`, { size: 32 });
}

export async function processGasTopup(
  ctx: KeeperContext,
  payload: GasTopupPayload = {},
): Promise<GasTopupResult> {
  const { db, config, logger } = ctx;
  const limit = payload.limit ?? DEFAULT_LIMIT;

  const { rows } = await db.query(
    `SELECT t.id,
            t.origin_chain,
            t.destination_chain,
            t.nonce,
            t.origin_tx,
            a.rail_reference,
            a.rail_status
       FROM outbound_transfer t
       JOIN rail_attestation a ON a.transfer_id = t.id
      WHERE t.route = 1
        AND a.status = 'pending'
        AND (
          a.rail_status LIKE '%insufficient_gas%'
          OR a.rail_status LIKE '%is_insufficient_fee%'
          OR a.rail_status LIKE '%not_enough_gas%'
        )
      ORDER BY t.id ASC
      LIMIT $1`,
    [limit],
  );

  const underfunded: UnderfundedTransfer[] = rows.map((row) => ({
    transferId: bigintOf(row, "id"),
    originChain: text(row, "origin_chain"),
    destinationChain: text(row, "destination_chain"),
    nonce: bigintOf(row, "nonce"),
    originTx: text(row, "origin_tx"),
    railReference: textOrNull(row, "rail_reference"),
    railStatus: textOrNull(row, "rail_status"),
  }));

  const hasSigner = payload.evmRpcProvider !== undefined || config.keeper.evmPrivateKey !== null;
  const dryRun = !hasSigner;

  logger.info(
    { underfundedCount: underfunded.length, dryRun },
    "evaluating underfunded Axelar transfers for gas top-up",
  );

  if (dryRun) {
    for (const transfer of underfunded) {
      logger.debug(
        {
          transferId: transfer.transferId.toString(),
          originTx: transfer.originTx,
          status: transfer.railStatus,
        },
        "transfer flagged for gas top-up (dry-run)",
      );
    }
    return {
      toppedUp: 0,
      pending: underfunded.length,
      dryRun: true,
      detail: `dry-run: identified ${String(underfunded.length)} Axelar transfers needing gas top-up`,
    };
  }

  const topupAmount = payload.topupAmountPerTransfer ?? DEFAULT_TOPUP_AMOUNT;
  const maxSpendingLimit = payload.maxSpendingLimit ?? DEFAULT_MAX_SPENDING_LIMIT;
  const gasServiceAddress = (payload.gasServiceAddress ??
    DEFAULT_AXELAR_GAS_SERVICE) as `0x${string}`;

  let toppedUp = 0;
  let totalSpent = 0n;

  for (const transfer of underfunded) {
    if (totalSpent + topupAmount > maxSpendingLimit) {
      logger.warn(
        {
          transferId: transfer.transferId.toString(),
          totalSpent: totalSpent.toString(),
          maxSpendingLimit: maxSpendingLimit.toString(),
        },
        "per-job spending limit reached; halting gas top-ups",
      );
      break;
    }

    try {
      const txHash = formatBytes32(transfer.originTx);
      const logIndex = 0n;
      const refundAddress = (payload.refundAddress ??
        "0x0000000000000000000000000000000000000000") as `0x${string}`;

      if (payload.evmRpcProvider !== undefined) {
        const writeFn = payload.evmRpcProvider.writeContract;
        const waitFn = payload.evmRpcProvider.waitForTransactionReceipt;

        const hash =
          writeFn !== undefined
            ? await writeFn({
                address: gasServiceAddress,
                abi: AXELAR_GAS_SERVICE_ABI,
                functionName: "addNativeGas",
                args: [txHash, logIndex, refundAddress],
              })
            : (`0x${"2".repeat(64)}` as const);

        if (waitFn !== undefined) {
          const receipt = await waitFn({ hash });
          if (receipt.status !== "success") {
            throw new Error(`gas top-up transaction reverted: ${receipt.transactionHash}`);
          }
        }
      } else if (config.keeper.evmPrivateKey !== null) {
        const destination = transfer.destinationChain;
        const chainMeta = isChainKey(destination) ? CHAINS[destination] : null;
        const rpcUrl =
          config.indexer.evm.find((e) => e.chain === destination)?.rpcUrl ??
          (chainMeta !== null && isEvmChain(chainMeta)
            ? chainMeta.defaultRpcUrl
            : "http://127.0.0.1:8545");

        const account = privateKeyToAccount(config.keeper.evmPrivateKey as `0x${string}`);
        const publicClient = createPublicClient({ transport: http(rpcUrl) });
        const walletClient = createWalletClient({ account, transport: http(rpcUrl) });

        const hash = await walletClient.writeContract({
          address: gasServiceAddress,
          abi: AXELAR_GAS_SERVICE_ABI,
          functionName: "addNativeGas",
          args: [txHash, logIndex, account.address],
          value: topupAmount,
          chain: null,
        });

        const receipt = await publicClient.waitForTransactionReceipt({ hash });
        if (receipt.status !== "success") {
          throw new Error(`gas top-up transaction reverted: ${receipt.transactionHash}`);
        }
      }

      await db.query(
        `UPDATE rail_attestation
            SET rail_status = 'gas_topped_up',
                last_checked_at = now()
          WHERE transfer_id = $1`,
        [transfer.transferId],
      );

      toppedUp += 1;
      totalSpent += topupAmount;

      logger.info(
        {
          transferId: transfer.transferId.toString(),
          originTx: transfer.originTx,
          topupAmount: topupAmount.toString(),
        },
        "successfully submitted Axelar native gas top-up",
      );
    } catch (err: unknown) {
      const reason = err instanceof Error ? err.message : String(err);
      logger.error(
        { transferId: transfer.transferId.toString(), err: reason },
        "failed to submit Axelar native gas top-up",
      );
    }
  }

  const detail = `topped up gas for ${String(toppedUp)} of ${String(underfunded.length)} transfers`;

  return {
    toppedUp,
    pending: underfunded.length,
    dryRun: false,
    detail,
  };
}
