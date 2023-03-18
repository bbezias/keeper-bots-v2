import {
  BlockhashWithExpiryBlockHeight,
  Connection,
  PublicKey, RpcResponseAndContext, SignatureResult,
  Signer,
  TransactionInstruction,
  TransactionMessage, VersionedTransaction
} from '@solana/web3.js';

export async function sendTransactionV0(instructions: TransactionInstruction[], connection: Connection, signers: Signer[], payer: PublicKey): Promise<{ sig: string, blockhash: BlockhashWithExpiryBlockHeight }> {

  const latestBlockhash = await connection.getLatestBlockhash('confirmed');

  const message = new TransactionMessage({
    payerKey: payer,
    recentBlockhash: latestBlockhash.blockhash,
    instructions: instructions
  }).compileToV0Message();

  const transaction = new VersionedTransaction(message);
  transaction.sign(signers);
  return { sig: await connection.sendTransaction(transaction, { maxRetries: 5 }), blockhash: latestBlockhash };
}

export async function sendAndConfirmV0(
  instructions: TransactionInstruction[],
  connection: Connection,
  signers: Signer[],
  payer: PublicKey): Promise<{ sig: string, result: RpcResponseAndContext<SignatureResult> }> {
  const { sig, blockhash } = await sendTransactionV0(instructions, connection, signers, payer);
  return {
    sig, result: await connection.confirmTransaction({
      signature: sig,
      blockhash: blockhash.blockhash,
      lastValidBlockHeight: blockhash.lastValidBlockHeight
    })
  };
}
