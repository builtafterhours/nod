import { spawn } from 'node:child_process';
import { constants } from 'node:fs';
import { access, mkdtemp, open, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const rate = 16_000;
const maxPcmBytes = 300 * rate * 2;

export class LocalMediaDecoderMissing extends Error {}

export async function prepareSlackSpeech(
  input: { bytes: Uint8Array; mimeType: string; name: string },
  signal: AbortSignal,
) {
  const mime = input.mimeType.split(';', 1)[0].trim().toLowerCase();
  const video = mime.startsWith('video/');
  const mov = /^(audio\/(mp4|m4a|x-m4a)|video\/(mp4|quicktime))$/.test(mime);
  if (!mov && mime !== 'video/webm') return video ? null : { ...input, mimeType: mime };
  signal.throwIfAborted();
  let executable: string | undefined;
  for (const candidate of [
    '/opt/homebrew/bin/ffmpeg',
    '/usr/local/bin/ffmpeg',
    '/usr/bin/ffmpeg',
  ]) {
    if (
      await access(candidate, constants.X_OK).then(
        () => true,
        () => false,
      )
    ) {
      executable = candidate;
      break;
    }
  }
  if (!executable) throw new LocalMediaDecoderMissing();
  const operation = AbortSignal.any([signal, AbortSignal.timeout(30_000)]);
  const directory = await mkdtemp(join(tmpdir(), 'nod-slack-audio-'));
  try {
    const path = join(directory, 'clip');
    await writeFile(path, input.bytes, { mode: 0o600, signal: operation });
    const file = await open(path, 'r');
    try {
      operation.throwIfAborted();
      const chunks: Buffer[] = [];
      let size = 0;
      let stderr = '';
      const exit = await new Promise<number | null>((resolve, reject) => {
        const child = spawn(
          executable,
          [
            '-nostdin',
            '-hide_banner',
            '-loglevel',
            'error',
            '-xerror',
            '-max_alloc',
            '67108864',
            '-protocol_whitelist',
            'fd',
            '-fd',
            '0',
            '-f',
            mov ? 'mov' : 'matroska',
            ...(mov ? ['-enable_drefs', '0', '-use_absolute_path', '0'] : []),
            '-threads',
            '1',
            '-i',
            'fd:',
            '-map',
            '0:a:0',
            '-vn',
            '-ac',
            '1',
            '-ar',
            String(rate),
            '-t',
            '301',
            '-f',
            's16le',
            'pipe:1',
          ],
          {
            stdio: [file.fd, 'pipe', 'pipe'],
            env: { LANG: 'C' },
            signal: operation,
            killSignal: 'SIGKILL',
            windowsHide: true,
          },
        );
        let failure: Error | undefined;
        child.on('error', (error) => {
          failure = error;
        });
        child.once('close', (code) => (failure ? reject(failure) : resolve(code)));
        child.stdout!.on('data', (chunk: Buffer) => {
          size += chunk.length;
          if (size > maxPcmBytes) child.kill('SIGKILL');
          else chunks.push(chunk);
        });
        child.stderr!.setEncoding('utf8').on('data', (chunk: string) => {
          stderr += chunk.slice(0, Math.max(0, 4096 - stderr.length));
        });
      });
      operation.throwIfAborted();
      if (
        video &&
        exit !== 0 &&
        !size &&
        /^Stream map '(?:0:a:0)?' matches no streams\.$/m.test(stderr)
      )
        return null;
      if (exit !== 0 || size < 2 || size > maxPcmBytes || size % 2)
        throw new Error('Local media conversion failed.');
      const header = Buffer.alloc(44);
      header.write('RIFF', 0);
      header.writeUInt32LE(size + 36, 4);
      header.write('WAVEfmt ', 8);
      header.writeUInt32LE(16, 16);
      header.writeUInt16LE(1, 20);
      header.writeUInt16LE(1, 22);
      header.writeUInt32LE(rate, 24);
      header.writeUInt32LE(rate * 2, 28);
      header.writeUInt16LE(2, 32);
      header.writeUInt16LE(16, 34);
      header.write('data', 36);
      header.writeUInt32LE(size, 40);
      return {
        bytes: Buffer.concat([header, ...chunks], size + header.length),
        mimeType: 'audio/wav',
        name: 'clip.wav',
      };
    } finally {
      await file.close();
    }
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
}
