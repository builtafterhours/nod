import type { PoolClient } from 'pg';
import type { MessagingAttachment } from '../ports/messaging.ts';
import { MessagingAttachmentError, MessagingAttachmentReadError } from '../ports/messaging.ts';
import type { ArtifactService } from './artifacts.ts';
import type { SpeechService } from './speech.ts';
import { SpeechSetupError, TranscriptionLengthError } from './speech.ts';

export const channelMediaFailureReasons = [
  'too_large',
  'text_too_long',
  'unavailable',
  'metadata_unavailable',
  'download_url_rejected',
  'download_failed',
  'download_http_error',
  'download_timeout',
  'preparation_timeout',
  'attachment_read_failed',
  'attachment_empty',
  'attachment_size_mismatch',
  'voice_mime_invalid',
  'transcription_unavailable',
  'speech_setup_required',
  'local_speech_setup_required',
  'local_media_decoder_missing',
] as const;

export class ChannelMediaFailure extends Error {
  constructor(
    readonly reason: (typeof channelMediaFailureReasons)[number],
    readonly fileLimitMb?: number,
    readonly httpStatus?: number,
    readonly serviceCode?: MessagingAttachmentReadError['serviceCode'],
  ) {
    super(reason);
  }
}

export function createChannelMedia(options: {
  artifacts: Pick<ArtifactService, 'storeArtifact'>;
  speech: Pick<SpeechService, 'transcribe'>;
  slackTranscribe?: (
    input: { bytes: Buffer; mimeType: string; name: string },
    signal: AbortSignal,
    report: NonNullable<Parameters<SpeechService['transcribe']>[2]>,
  ) => Promise<string | null>;
}) {
  return {
    async prepare(
      attachments: MessagingAttachment[],
      signal: AbortSignal,
      beforeTranscribe?: () => Promise<void>,
      slack = false,
    ) {
      if (!attachments.length) throw new ChannelMediaFailure('unavailable');
      if (attachments.length > 10) throw new ChannelMediaFailure('too_large');
      const files: {
        bytes: Buffer;
        name: string;
        mimeType: string;
        source?: MessagingAttachment['source'];
      }[] = [];
      const transcripts: string[] = [];
      const usageIds = new Set<string>();
      const cleanup: Array<() => Promise<void>> = [];
      let total = 0;
      for (const attachment of attachments) {
        signal.throwIfAborted();
        if (attachment.size > 50 * 1024 * 1024) throw new ChannelMediaFailure('too_large');
        let bytes: Buffer;
        try {
          bytes = Buffer.from(await attachment.read(signal));
        } catch (error) {
          signal.throwIfAborted();
          if (error instanceof MessagingAttachmentError)
            throw new ChannelMediaFailure('too_large', error.maxBytes / 1024 / 1024);
          if (error instanceof MessagingAttachmentReadError)
            throw new ChannelMediaFailure(
              error.reason,
              undefined,
              error.httpStatus,
              error.serviceCode,
            );
          throw new ChannelMediaFailure('attachment_read_failed');
        }
        signal.throwIfAborted();
        total += bytes.length;
        if (!bytes.length) throw new ChannelMediaFailure('attachment_empty');
        if (attachment.size && bytes.length !== attachment.size)
          throw new ChannelMediaFailure('attachment_size_mismatch');
        if (total > 50 * 1024 * 1024) throw new ChannelMediaFailure('too_large');
        const video = slack && attachment.mimeType.toLowerCase().startsWith('video/');
        if (attachment.voice || video) {
          if (!video && !attachment.mimeType.startsWith('audio/'))
            throw new ChannelMediaFailure('voice_mime_invalid');
          if (bytes.length > 25 * 1024 * 1024) throw new ChannelMediaFailure('too_large');
          try {
            await beforeTranscribe?.();
            const report = (usage: { id: string }) => {
              usageIds.add(usage.id);
            };
            const input = { bytes, mimeType: attachment.mimeType, name: attachment.name };
            const transcribe = slack ? options.slackTranscribe : options.speech.transcribe;
            if (!transcribe) throw new SpeechSetupError();
            const transcript = await transcribe(input, signal, report);
            if (transcript) transcripts.push(transcript);
          } catch (error) {
            signal.throwIfAborted();
            if (error instanceof ChannelMediaFailure) throw error;
            if (error instanceof SpeechSetupError)
              throw new ChannelMediaFailure(
                slack ? 'local_speech_setup_required' : 'speech_setup_required',
              );
            if (error instanceof TranscriptionLengthError)
              throw new ChannelMediaFailure('text_too_long');
            throw new ChannelMediaFailure('transcription_unavailable');
          }
        }
        files.push({
          bytes,
          name: attachment.name,
          mimeType: attachment.mimeType,
          source: attachment.source,
        });
      }
      return {
        text: transcripts.join('\n\n'),
        usageIds: [...usageIds],
        async save(client: PoolClient, threadId?: string) {
          const saved = [];
          for (const file of files) {
            signal.throwIfAborted();
            const artifact = await options.artifacts.storeArtifact(
              file.bytes,
              file.name,
              file.mimeType,
              threadId,
              client,
              (remove) => cleanup.push(remove),
            );
            saved.push({ ...artifact, ...(file.source ? { source: file.source } : {}) });
          }
          return saved;
        },
        async dispose() {
          await Promise.all(cleanup.map((remove) => remove()));
        },
      };
    },
  };
}

export type ChannelMedia = ReturnType<typeof createChannelMedia>;
