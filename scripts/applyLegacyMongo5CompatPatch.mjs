#!/usr/bin/env node

import fs from "node:fs";
import path from "node:path";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);

function patchFile({ label, filePath, replacements }) {
  let content = fs.readFileSync(filePath, "utf8");
  let changed = false;

  for (const { before, after } of replacements) {
    if (content.includes(after)) {
      continue;
    }

    if (!content.includes(before)) {
      throw new Error(`Patch marker not found for ${label}: ${filePath}`);
    }

    content = content.replace(before, after);
    changed = true;
  }

  if (changed) {
    fs.writeFileSync(filePath, content);
  }

  return changed;
}

function resolvePatchedTargets() {
  const serviceProviderEntry = require.resolve("@mongosh/service-provider-node-driver");
  const serviceProviderDir = path.dirname(serviceProviderEntry);
  const devtoolsConnectEntry = require.resolve("@mongodb-js/devtools-connect", {
    paths: [serviceProviderDir],
  });
  const devtoolsConnectDir = path.dirname(devtoolsConnectEntry);

  return {
    serviceProviderFile: path.join(serviceProviderDir, "node-driver-service-provider.js"),
    devtoolsConnectFile: path.join(devtoolsConnectDir, "connect.js"),
  };
}

function main() {
  const { serviceProviderFile, devtoolsConnectFile } = resolvePatchedTargets();

  const changedFiles = [];

  if (
    patchFile({
      label: "@mongosh/service-provider-node-driver default options",
      filePath: serviceProviderFile,
      replacements: [
        {
          before: `const DEFAULT_DRIVER_OPTIONS = Object.freeze({
    __skipPingOnConnect: false,
});`,
          after: `const DEFAULT_DRIVER_OPTIONS = Object.freeze({});`,
        },
      ],
    })
  ) {
    changedFiles.push(serviceProviderFile);
  }

  if (
    patchFile({
      label: "@mongodb-js/devtools-connect client options merge",
      filePath: devtoolsConnectFile,
      replacements: [
        {
          before:
            "const mongoClientOptions = (0, lodash_merge_1.default)({ __skipPingOnConnect: true }, clientOptions, shouldAddOidcCallbacks ? state.oidcPlugin.mongoClientOptions : {}, { allowPartialTrustChain: true }, ca ? { ca } : {});",
          after:
            "const mongoClientOptions = (0, lodash_merge_1.default)({}, clientOptions, shouldAddOidcCallbacks ? state.oidcPlugin.mongoClientOptions : {}, ca ? { ca } : {});",
        },
      ],
    })
  ) {
    changedFiles.push(devtoolsConnectFile);
  }

  if (changedFiles.length === 0) {
    console.log("Legacy MongoDB 4.0 compatibility patch already applied.");
    return;
  }

  console.log("Applied legacy MongoDB 4.0 compatibility patch to:");
  for (const filePath of changedFiles) {
    console.log(`- ${filePath}`);
  }
}

try {
  main();
} catch (error) {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
}
