<div align="center">
  <img height="120x" src="https://uploads-ssl.webflow.com/611580035ad59b20437eb024/616f97a42f5637c4517d0193_Logo%20(1)%20(1).png" />

  <h1 style="margin-top:20px;">Keeper Bots for Drift Protocol v2</h1>

  <p>
    <a href="https://docs.drift.trade/tutorial-keeper-bots"><img alt="Docs" src="https://img.shields.io/badge/docs-tutorials-blueviolet" /></a>
    <a href="https://discord.com/channels/849494028176588802/878700556904980500"><img alt="Discord Chat" src="https://img.shields.io/discord/889577356681945098?color=blueviolet" /></a>
    <a href="https://opensource.org/licenses/Apache-2.0"><img alt="License" src="https://img.shields.io/github/license/project-serum/anchor?color=blueviolet" /></a>
  </p>
</div>

# Setting up
## Setup Environment
```shell
cp .env.example .env
```

Update values in `.env` accordingly

`KEEPER_PRIVATE_KEY`: can be a number array (as in example), or a path to a `keypair.json` as generated by `solana-keygen`. The address of this key requires SOL since it will be the signing authority of all transactions sent by this bot.

## Initialize User

A `ClearingHouseUser` must be created before interacting with the `ClearingHouse` program.

```shell
yarn run dev --init-user
```

## Depositing Collateral

Some bots (i.e. trading, liquidator and JIT makers) require collateral in order to keep positions open, a helper function is included to help with depositing collateral.
A user must be initialized first before collateral may be deposited.

```shell
# deposit 10,000 USDC
yarn run dev --force-deposit 10000
```

# Run Bots

By default, some [Prometheus](https://prometheus.io/) metrics are exposed on `localhost:9464/metrics`.

## Run Filler Bot
```shell
yarn
yarn run dev:filler
```

## Run Trigger Bot
```shell
yarn
yarn run dev:filler
```

## Run JIT Maker Bot

⚠ requires collateral (you dont want to run this as is)

```shell
yarn
yarn run dev:jitmaker
```

## Run Liquidator Bot

Read the docs: https://docs.drift.trade/liquidators

By default the liquidator will attempt to liqudate (inherit the risk of)
endangered positions in all markets. Pass `--perp-markets` or `--spot-markets`
flags to restrict which markets you want to liquidate.

You can also specify which subaccountId you want to use via the `--subaccount`
flag (the main subaccount 0 is default).


Example - default mode (liquidate all markets):
```shell
yarn run dev --liquidator
```

Example - default mode (liquidate SOL-PERP, BTC-PERP and SOL spot):
```shell
yarn run dev --liquidator --perp-markets 0,1 --spot-markets 0
```
```