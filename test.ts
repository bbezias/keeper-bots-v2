import { Connection } from '@solana/web3.js';
import { logger } from './src/logger';


const connection = new Connection("http://44.199.211.133:8545");

async function test() {
  const t1 = new Date().getTime();
  const latestBlockhash = await connection.getLatestBlockhash('confirmed');
  const t2 = new Date().getTime();
  logger.info(`Blockhash Ping: ${t2 - t1} ms, found: ${latestBlockhash}`);
}

test().then(_ => {});
