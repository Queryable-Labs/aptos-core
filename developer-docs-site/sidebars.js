/**
 * Creating a sidebar enables you to:
 - create an ordered group of docs
 - render a sidebar for each doc of that group
 - provide next/previous navigation

 The sidebars can be generated from the filesystem, or explicitly defined here.

 Create as many sidebars as you want.
 */

// @ts-check

/** @type {import('@docusaurus/plugin-content-docs').SidebarsConfig} */
const sidebars = {
  // By default, Docusaurus generates a sidebar from the docs folder structure
  // defaultSidebar: [{type: 'autogenerated', dirName: '.', }],
  aptosSidebar: [
    "index",
    {
      type: "html",
      value: "Get Aptos",
      className: "sidebar-title",
    },
    {
      type: "category",
      label: "Start Aptos",
      link: { type: "doc", id: "guides/getting-started" },
      collapsible: true,
      collapsed: true,
      items: [
        "whats-new-in-docs",
        "guides/use-aptos-explorer",
        "cli-tools/aptos-cli-tool/install-aptos-cli",
        "cli-tools/build-aptos-cli",
        "cli-tools/aptos-cli-tool/use-aptos-cli",
        "cli-tools/install-move-prover",
        "nodes/local-testnet/using-cli-to-run-a-local-testnet",
        {
          type: "category",
          label: "Follow Token Standard",
          link: { type: "doc", id: "concepts/coin-and-token/index" },
          collapsible: true,
          collapsed: true,
          items: ["concepts/coin-and-token/aptos-coin", "concepts/coin-and-token/aptos-token"],
        },
        {
          type: "category",
          label: "Read White Paper",
          collapsible: true,
          collapsed: true,
          link: { type: "doc", id: "aptos-white-paper/index" },
          items: ["aptos-white-paper/in-korean"],
        },
        "guides/system-integrators-guide",
      ],
    },
    {
      type: "html",
      value: "Build Apps",
      className: "sidebar-title",
    },
    {
      type: "category",
      label: "Developer Tutorials",
      link: { type: "doc", id: "tutorials/index" },
      collapsible: true,
      collapsed: true,
      items: [
        "tutorials/first-transaction",
        "tutorials/your-first-nft",
        "tutorials/first-dapp",
        "tutorials/first-coin",
      ],
    },
    {
      type: "category",
      label: "Aptos SDKs",
      collapsible: true,
      collapsed: true,
      link: { type: "doc", id: "sdks/index" },
      items: [
        {
          type: "category",
          label: "TypeScript SDK",
          link: { type: "doc", id: "sdks/ts-sdk/index" },
          collapsible: true,
          collapsed: true,
          items: ["sdks/ts-sdk/typescript-sdk", "sdks/ts-sdk/typescript-sdk-overview"],
        },
        "sdks/python-sdk",
        "sdks/rust-sdk",
      ],
    },
    {
      type: "category",
      label: "Move Lang",
      link: { type: "doc", id: "guides/move-guides/index" },
      collapsible: true,
      collapsed: true,
      items: [
        "guides/move-guides/move-on-aptos",
        "tutorials/first-move-module",
        "guides/move-guides/upgrading-move-code"
      ],
    },
    {
      type: "category",
      label: "Developer Guides",
      link: { type: "doc", id: "guides/index" },
      collapsible: true,
      collapsed: true,
      items: [
        "guides/basics-life-of-txn",
        "guides/sign-a-transaction",
        "guides/interacting-with-the-blockchain",
        "guides/state-sync",
        "guides/data-pruning",
        "guides/indexing",
        "guides/resource-accounts",
        "guides/wallet-standard",
        "guides/install-petra-wallet",
        "guides/local-testnet-dev-flow",
        "guides/running-a-local-multi-node-network",
        "guides/handle-aptos-errors",
      ],
    },
    {
      type: "html",
      value: "Run Nodes",
      className: "sidebar-title",
    },
    {
      type: "category",
      label: "Node Overview",
      collapsible: true,
      collapsed: true,
      link: { type: "doc", id: "nodes/nodes-landing" },
      items: [
        "nodes/aptos-deployments",
        "nodes/leaderboard-metrics",
        "nodes/node-health-checker/index",
        "nodes/node-health-checker/node-health-checker-faq",
        "reference/telemetry",
        "nodes/identity-and-configuration",

      ],
    },
    {
      type: "category",
      label: "Node Files For Mainnet",
      collapsible: true,
      collapsed: true,
      link: { type: "doc", id: "nodes/node-files-all-networks/node-files" },
      items: ["nodes/node-files-all-networks/node-files-devnet", "nodes/node-files-all-networks/node-files-testnet"],
    },
    /** Delete during clean up
    {
      type: "category",
      label: "AIT-3",
      link: { type: "doc", id: "nodes/ait/index" },
      collapsible: true,
      collapsed: true,
      items: [
        "nodes/ait/whats-new-in-ait3",
        "nodes/ait/steps-in-ait3",

      ],
    },  */
    {
      type: "category",
      label: "Validators",
      link: { type: "doc", id: "nodes/validator-node/index" },
      collapsible: true,
      collapsed: true,
      items: [
        {
          type: "category",
          label: "Owner",
          collapsible: true,
          collapsed: true,
          link: { type: "doc", id: "nodes/validator-node/owner/index" },
          items: [],
        },
        {
          type: "category",
          label: "Operator",
          collapsible: true,
          collapsed: true,
          link: { type: "doc", id: "nodes/validator-node/operator/index" },
          items: [
            "nodes/validator-node/operator/node-requirements",
            {
              type: "category",
              label: "Running Validator Node",
              collapsible: true,
              collapsed: true,
              link: { type: "doc", id: "nodes/validator-node/operator/running-validator-node/index" },
              items: [
                "nodes/validator-node/operator/running-validator-node/using-aws",
                "nodes/validator-node/operator/running-validator-node/using-azure",
                "nodes/validator-node/operator/running-validator-node/using-gcp",
                "nodes/validator-node/operator/running-validator-node/using-docker",
                "nodes/validator-node/operator/running-validator-node/using-source-code",
              ],
            },
            "nodes/validator-node/operator/node-liveness-criteria",
            "nodes/validator-node/operator/connect-to-aptos-network",
            "nodes/validator-node/operator/staking-pool-operations",
            "nodes/validator-node/operator/shutting-down-nodes",
          ],
        },
        {
          type: "category",
          label: "Voter",
          collapsible: true,
          collapsed: true,
          link: { type: "doc", id: "nodes/validator-node/voter/index" },
          items: [],
        },
      ],
    },
    {
      type: "category",
      label: "Public Fullnode",
      link: { type: "doc", id: "nodes/full-node/index" },
      collapsible: true,
      collapsed: true,
      items: [
        "nodes/full-node/fullnode-source-code-or-docker",
        "nodes/full-node/update-fullnode-with-new-releases",
        "nodes/full-node/network-identity-fullnode",
        "nodes/full-node/fullnode-network-connections",
        "nodes/full-node/run-a-fullnode-on-gcp",
        "nodes/full-node/bootstrap-fullnode",
        "nodes/indexer-fullnode",
      ],
    },
        /** Delete after clean up
         * "nodes/local-testnet/index"
         *  "nodes/local-testnet/run-a-local-testnet"
         */
    {
      type: "html",
      value: "Reference",
      className: "sidebar-title",
    },
    "nodes/aptos-api-spec",
    "issues-and-workarounds",
    "reference/glossary",
    {
      type: "category",
      label: "Aptos Concepts",
      link: { type: "doc", id: "concepts/index" },
      collapsible: true,
      collapsed: true,
      items: [
        "concepts/basics-txns-states",
        "concepts/basics-accounts",
        "concepts/basics-events",
        "concepts/basics-gas-txn-fee",
        "concepts/base-gas",
        "concepts/basics-fullnodes",
        "concepts/basics-validator-nodes",
        "concepts/basics-node-networks-sync",
        "concepts/staking",
        "concepts/governance",
      ],
    },
  ],
};

module.exports = sidebars;
