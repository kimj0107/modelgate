import { execFileSync } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { resolve, join } from "node:path";

// All consumer artifacts live inside the project and are removed, including on failure.
const npm = process.env.npm_execpath;
if (!npm) throw new Error("Run with npm run smoke:package");
const runNpm = (args, cwd) => execFileSync(process.execPath, [npm, ...args], {
  cwd, encoding: "utf8", timeout: 60_000,
});
const root = process.cwd();
const consumer = mkdtempSync(join(root, ".package-smoke-"));
let tarball;
try {
  const [pack] = JSON.parse(runNpm(["pack", "--ignore-scripts", "--json"], root));
  tarball = resolve(root, pack.filename);
  const forbidden = pack.files.filter(file =>
    !/^(package\.json|README\.md|LICENSE|dist\/src\/.+\.(js|d\.ts))$/.test(file.path));
  if (forbidden.length) throw new Error("Unexpected package contents");
  writeFileSync(join(consumer, "package.json"), '{"name":"modelgate-consumer-smoke","private":true,"type":"module"}');
  runNpm(["install", "--offline", "--ignore-scripts", "--no-audit", "--no-fund", tarball], consumer);
  const help = runNpm(["exec", "--offline", "--", "modelgate", "--help"], consumer);
  if (!help.includes("POST /v1/stream")) throw new Error("Installed CLI help missing");
  const smoke = runNpm(["exec", "--offline", "--", "modelgate", "--demo", "--smoke"], consumer);
  if (!smoke.includes("anthropic: start, deltas, done verified")) throw new Error("Installed demo failed");
  // Test package resolution through its public export, not a source-tree import.
  execFileSync(process.execPath, ["--input-type=module", "-e",
    'import { createGateway } from "modelgate"; if(typeof createGateway !== "function") process.exit(1);'],
  { cwd: consumer, timeout: 5000 });
  console.log(`Clean consumer: ${pack.files.length} allowlisted files; installed CLI help, demo, and public import passed.`);
} finally {
  rmSync(consumer, { recursive: true, force: true });
  if (tarball) rmSync(tarball, { force: true });
}
