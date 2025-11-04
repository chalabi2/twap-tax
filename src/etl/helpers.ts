import { tmpdir } from "os";
import { mkdtemp, readdir, stat } from "fs/promises";
import { join, resolve } from "path";
import { spawn } from "child_process";

export async function makeTempDir(prefix = "twap-tax-"): Promise<string> {
  const base = process.env["DATA_TMP_DIR"] ?? tmpdir();
  return await mkdtemp(join(base, prefix));
}

export async function execCmd(cmd: string, args: string[], opts?: { cwd?: string; env?: NodeJS.ProcessEnv }): Promise<{ code: number; stdout: string; stderr: string }> {
  return await new Promise((resolvePromise) => {
    const child = spawn(cmd, args, {
      cwd: opts?.cwd,
      env: { ...process.env, ...opts?.env },
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (d) => (stdout += String(d)));
    child.stderr.on("data", (d) => (stderr += String(d)));
    child.on("close", (code) => resolvePromise({ code: code ?? 0, stdout, stderr }));
  });
}

export async function awsS3CpRecursive(s3Prefix: string, outDir: string, requestPayer = "requester"): Promise<void> {
  const awsBin = process.env["AWS_CLI_PATH"] ?? "aws";
  const args = ["s3", "cp", s3Prefix, outDir, "--recursive", "--request-payer", requestPayer];
  const { code, stderr } = await execCmd(awsBin, args);
  if (code !== 0) {
    throw new Error(`aws s3 cp failed: ${stderr || `(exit ${code})`}`);
  }
}

export async function awsS3HasAny(s3Prefix: string, requestPayer = "requester"): Promise<boolean> {
  const awsBin = process.env["AWS_CLI_PATH"] ?? "aws";
  const { code, stdout } = await execCmd(awsBin, ["s3", "ls", s3Prefix, "--request-payer", requestPayer]);
  if (code !== 0) return false;
  return stdout.trim().length > 0;
}

export async function decompressLz4Recursive(dir: string): Promise<void> {
  const files = await listFilesRecursive(dir);
  const lz4Files = files.filter((f) => f.endsWith(".lz4"));
  for (const f of lz4Files) {
    const unlz4Bin = process.env["UNLZ4_PATH"] ?? "unlz4";
    const { code, stderr } = await execCmd(unlz4Bin, ["--rm", f]);
    if (code !== 0) {
      throw new Error(`unlz4 failed for ${f}: ${stderr}`);
    }
  }
}

export async function listFilesRecursive(dir: string): Promise<string[]> {
  const out: string[] = [];
  async function walk(d: string): Promise<void> {
    const items = await readdir(d);
    for (const name of items) {
      const full = resolve(d, name);
      const s = await stat(full);
      if (s.isDirectory()) await walk(full);
      else out.push(full);
    }
  }
  await walk(dir);
  return out.sort();
}

