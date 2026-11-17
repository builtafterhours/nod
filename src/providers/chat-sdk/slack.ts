import { createHash } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { SocketModeClient, LogLevel, UnrecoverableSocketModeStartError } from '@slack/socket-mode';
import {
  WebClient,
  WebAPIHTTPError,
  WebAPIRequestError,
  WebAPIPlatformError,
  WebAPIRateLimitedError,
  type ChatStopStreamArguments,
} from '@slack/web-api';
import { get as emojiForName, which as nameForEmoji } from 'node-emoji';
import type { Pool } from 'pg';
import { z } from 'zod';
import { parseMarkdown } from 'chat';
import type {
  InboundMessage,
  InboundReaction,
  MessagingAttachment,
  MessagingChannel,
  MessagingTransport,
  MessagingControl,
} from '../../ports/messaging.ts';
import {
  MessagingAttachmentError,
  MessagingConnectionError,
  MessagingDeliveryError,
} from '../../ports/messaging.ts';
import { isMessageReaction } from '../../shared/emoji.ts';
import { slackMessages } from './format.ts';

const id = /^[A-Z][A-Z0-9]{1,63}$/;
const timestamp = /^\d{10,16}\.\d{6}$/;
const fileLimit = 20 * 1024 * 1024;
function streamFragments(text: string, limit: number) {
  const parts: string[] = [];
  for (let offset = 0; offset < text.length;) {
    let end = Math.min(text.length, offset + limit);
    if (end < text.length && /[\uD800-\uDBFF]/.test(text[end - 1])) end--;
    parts.push(text.slice(offset, end));
    offset = end;
  }
  return parts;
}
function streamMessages(text: string) {
  if (text.length <= 9000) return [text];
  if (text.length > 100_000) return;
  const root = parseMarkdown(text);
  const pending: Array<{ type: string; children?: unknown[] }> = [root];
  while (pending.length) {
    const node = pending.pop()!;
    if (['definition', 'linkReference', 'imageReference'].includes(node.type)) return;
    pending.push(...((node.children ?? []) as typeof pending));
  }
  const parts: string[] = [];
  let current = '';
  let offset = 0;
  for (const node of root.children) {
    const end = node.position?.end.offset;
    if (end === undefined) return;
    const source = text.slice(offset, end);
    if (source.length > 9000) return;
    if (current.length + source.length > 9000) {
      parts.push(current);
      current = '';
    }
    current += source;
    offset = end;
  }
  const tail = text.slice(offset);
  if (current.length + tail.length > 9000) return;
  parts.push(current + tail);
  return parts;
}
const emojiAliases: Record<string, string> = {
  thinking_face: 'thinking',
  hugging_face: 'hugs',
  face_with_rolling_eyes: 'roll_eyes',
};

function reactionEmoji(name: string) {
  const parsed = /^([a-z0-9_+-]+)(?:::skin-tone-([2-6]))?$/.exec(name);
  if (!parsed) return undefined;
  const base = emojiForName(
    parsed[1] === 'thumbsup'
      ? '+1'
      : parsed[1] === 'thumbsdown'
        ? '-1'
        : (emojiAliases[parsed[1]] ?? parsed[1]),
  );
  return base && base + (parsed[2] ? String.fromCodePoint(0x1f3f9 + Number(parsed[2])) : '');
}

function reactionName(emoji: string) {
  const base = nameForEmoji(emoji);
  if (!base) return undefined;
  const modifier = /\p{Emoji_Modifier}$/u.exec(emoji)?.[0];
  const name =
    (Object.keys(emojiAliases).find((name) => emojiAliases[name] === base) ?? base) +
    (modifier ? `::skin-tone-${modifier.codePointAt(0)! - 0x1f3f9}` : '');
  return reactionEmoji(name)?.replaceAll('\uFE0F', '') === emoji.replaceAll('\uFE0F', '')
    ? name
    : undefined;
}
const logger = {
  debug() {},
  info() {},
  warn() {},
  error() {},
  setLevel() {},
  getLevel: () => LogLevel.ERROR,
  setName() {},
};
export const slackInput = z
  .object({
    botToken: z
      .string()
      .trim()
      .regex(/^xoxb-[A-Za-z0-9-]+$/)
      .max(4096),
    appToken: z
      .string()
      .trim()
      .regex(/^xapp-[A-Za-z0-9-]+$/)
      .max(4096),
    teamId: z
      .string()
      .trim()
      .regex(/^T[A-Z0-9]{1,63}$/),
    allowedUserId: z
      .string()
      .trim()
      .regex(/^[UW][A-Z0-9]{1,63}$/),
    richEnabled: z.boolean().optional(),
  })
  .strict();

function failure(error: unknown) {
  if (error instanceof MessagingConnectionError) return error;
  const reason =
    error instanceof WebAPIPlatformError && /^[a-z_]{1,80}$/.test(error.data.error)
      ? error.data.error
      : error instanceof WebAPIHTTPError
        ? `HTTP ${error.statusCode}`
        : error instanceof WebAPIRateLimitedError
          ? `rate_limited (retry after ${Math.min(86400, Math.max(0, error.retryAfter))} seconds)`
          : error instanceof WebAPIRequestError
            ? 'network_request_failed'
            : 'connection_failed';
  return new MessagingConnectionError(`Slack failed: ${reason}.`);
}

class SlackIdentityError extends MessagingConnectionError {}

function terminalFailure(error: unknown) {
  return (
    error instanceof SlackIdentityError ||
    (error instanceof WebAPIPlatformError &&
      [
        ...Object.values(UnrecoverableSocketModeStartError),
        'token_revoked',
        'token_expired',
        'missing_scope',
        'not_allowed_token_type',
        'invalid_arguments',
        'invalid_arg_name',
        'user_not_found',
        'bot_not_found',
        'channel_not_found',
      ].includes(error.data.error))
  );
}

function retryAfter(error: unknown) {
  const seconds =
    error instanceof WebAPIRateLimitedError
      ? error.retryAfter
      : error instanceof WebAPIHTTPError
        ? Number(error.headers['retry-after'])
        : 0;
  return Number.isFinite(seconds) ? Math.min(300, Math.max(0, seconds)) * 1000 : 0;
}

function client(token: string, signal: AbortSignal, request: typeof fetch = fetch) {
  return new WebClient(token, {
    logger,
    retryConfig: { retries: 0 },
    rejectRateLimitedCalls: true,
    timeout: 15_000,
    fetch: (url, init) =>
      request(url, {
        ...init,
        redirect: 'error',
        signal: AbortSignal.any([signal, ...(init?.signal ? [init.signal] : [])]),
      }),
  });
}

class SlackSocket extends SocketModeClient {
  constructor(
    options: ConstructorParameters<typeof SocketModeClient>[0],
    private appId: string,
  ) {
    super(options);
  }

  override async start() {
    try {
      return await super.start();
    } catch (error) {
      await this.disconnect().catch(() => {});
      this.emit('nod_start_failed', error);
      return { ok: false };
    }
  }

  protected override async onWebSocketMessage(
    ...args: Parameters<SocketModeClient['onWebSocketMessage']>
  ) {
    try {
      const value = JSON.parse(
        typeof args[0] === 'string' ? args[0] : new TextDecoder().decode(args[0]),
      );
      if (value.type === 'hello' && value.connection_info?.app_id !== this.appId) {
        this.emit('nod_identity_failed');
        await this.disconnect();
        return;
      }
    } catch {
      return;
    }
    try {
      await super.onWebSocketMessage(...args);
    } catch (error) {
      await this.disconnect().catch(() => {});
      this.emit('nod_start_failed', error);
    }
  }
}

function socket(
  appToken: string,
  appId: string,
  signal: AbortSignal,
  request: typeof fetch = fetch,
) {
  return new SlackSocket(
    {
      appToken,
      logger,
      autoReconnectEnabled: false,
      clientOptions: {
        retryConfig: { retries: 0 },
        rejectRateLimitedCalls: true,
        timeout: 15_000,
        fetch: (url, init) =>
          request(url, {
            ...init,
            redirect: 'error',
            signal: AbortSignal.any([signal, ...(init?.signal ? [init.signal] : [])]),
          }),
      },
    },
    appId,
  );
}

async function connect(connection: SlackSocket, signal: AbortSignal) {
  let cleanup = () => {};
  const connected = new Promise<void>((resolve, reject) => {
    const failed = (error: Error) => reject(error);
    const identityFailed = () =>
      reject(new SlackIdentityError('Slack failed: app_identity_mismatch.'));
    const aborted = () =>
      reject(new MessagingConnectionError('Slack failed: connection_cancelled.'));
    connection.once('connected', resolve);
    connection.once('nod_start_failed', failed);
    connection.once('nod_identity_failed', identityFailed);
    signal.addEventListener('abort', aborted, { once: true });
    cleanup = () => {
      connection.removeListener('connected', resolve);
      connection.removeListener('nod_start_failed', failed);
      connection.removeListener('nod_identity_failed', identityFailed);
      signal.removeEventListener('abort', aborted);
    };
    if (signal.aborted) aborted();
  });
  try {
    await Promise.all([connection.start(), connected]);
  } finally {
    cleanup();
  }
}

async function identity(web: WebClient, input: z.infer<typeof slackInput>) {
  const me = await web.auth.test();
  if (me.team_id !== input.teamId || !me.bot_id || !me.user_id)
    throw new SlackIdentityError('Slack failed: workspace_or_bot_mismatch.');
  const bot = await web.bots.info({ bot: me.bot_id });
  const appId = bot.bot?.app_id;
  if (!appId || !id.test(appId))
    throw new SlackIdentityError('Slack failed: bot_identity_unavailable.');
  const owner = (await web.users.info({ user: input.allowedUserId })).user;
  if (
    !owner ||
    owner.id !== input.allowedUserId ||
    owner.team_id !== input.teamId ||
    owner.is_bot ||
    owner.is_app_user ||
    owner.deleted ||
    owner.id === me.user_id
  )
    throw new SlackIdentityError('Slack failed: owner_mismatch.');
  const dm = (await web.conversations.open({ users: input.allowedUserId, return_im: true }))
    .channel;
  if (!dm?.id || !/^D[A-Z0-9]{1,63}$/.test(dm.id))
    throw new SlackIdentityError('Slack failed: direct_message_unavailable.');
  const info = (await web.conversations.info({ channel: dm.id })).channel;
  if (
    !info?.is_im ||
    info.is_mpim ||
    info.is_shared ||
    info.is_ext_shared ||
    z.object({ user: z.literal(input.allowedUserId) }).safeParse(info).success === false
  )
    throw new SlackIdentityError('Slack failed: direct_message_mismatch.');
  return { appId, botUserId: me.user_id, dmId: dm.id };
}

export async function verifySlack(input: unknown, request: typeof fetch = fetch) {
  const value = slackInput.parse(input);
  const signal = AbortSignal.timeout(20_000);
  let connection: SlackSocket | undefined;
  try {
    const verified = await identity(client(value.botToken, signal, request), value);
    connection = socket(value.appToken, verified.appId, signal, request);
    connection.on('error', () => {});
    await connect(connection, signal);
    return {
      environment: {
        SLACK_ENABLED: 'true',
        SLACK_BOT_TOKEN: value.botToken,
        SLACK_APP_TOKEN: value.appToken,
        SLACK_TEAM_ID: value.teamId,
        SLACK_ALLOWED_USER_ID: value.allowedUserId,
        SLACK_RICH_ENABLED: String(value.richEnabled === true),
      },
      summary: {
        provider: 'slack' as const,
        enabled: true,
        mode: 'socket' as const,
        allowedUserId: value.allowedUserId,
        accountName: `Workspace ${value.teamId}`,
        accountHandle: `Member ${value.allowedUserId}`,
        richEnabled: value.richEnabled === true,
      },
    };
  } catch (error) {
    throw failure(error);
  } finally {
    await connection?.disconnect();
  }
}

const fileSchema = z.object({
  id: z.string().regex(/^F[A-Z0-9]{1,63}$/),
  name: z.string().min(1).max(255),
  mimetype: z.string().min(1).max(200),
  size: z.number().int().nonnegative(),
  url_private_download: z.string().max(2048).optional(),
  url_private: z.string().max(2048).optional(),
  subtype: z.string().optional(),
  mode: z.string().optional(),
  user: z.string().optional(),
});
const eventSchema = z.object({
  team_id: z.string(),
  api_app_id: z.string(),
  event_id: z.string().min(1).max(200),
  event: z.object({
    type: z.string(),
    user: z.string().optional(),
    bot_id: z.string().optional(),
    subtype: z.string().optional(),
    channel: z.string().optional(),
    channel_type: z.string().optional(),
    ts: z.string().optional(),
    thread_ts: z.string().optional(),
    text: z.string().max(100_000).optional(),
    files: z.array(z.unknown()).max(10).optional(),
    reaction: z.string().max(100).optional(),
    event_ts: z.string().regex(timestamp).optional(),
    item: z
      .object({ type: z.string(), channel: z.string(), ts: z.string().regex(timestamp) })
      .optional(),
  }),
});

function fileUrl(value: string | undefined, upload = false) {
  let url: URL;
  try {
    url = new URL(value ?? '');
  } catch {
    throw new MessagingConnectionError('Slack failed: file_address_unavailable.');
  }
  if (
    url.protocol !== 'https:' ||
    url.username ||
    url.password ||
    url.port ||
    url.hash ||
    !(upload
      ? url.hostname === 'files.slack.com'
      : url.hostname === 'files.slack.com' || url.hostname === 'slack-files.com')
  )
    throw new MessagingConnectionError('Slack failed: unsafe_file_address.');
  return url.href;
}

export function createSlackTransport(
  pool: Pool,
  environment: Record<string, string | undefined>,
): MessagingTransport {
  const parsed = slackInput.safeParse({
    botToken: environment.SLACK_BOT_TOKEN,
    appToken: environment.SLACK_APP_TOKEN,
    teamId: environment.SLACK_TEAM_ID,
    allowedUserId: environment.SLACK_ALLOWED_USER_ID,
    richEnabled: environment.SLACK_RICH_ENABLED === 'true',
  });
  const enabled = environment.SLACK_ENABLED === 'true';
  const channel: MessagingChannel = {
    id: 'slack',
    status: !enabled ? 'disabled' : parsed.success ? 'configured' : 'setup_required',
    missing:
      enabled && !parsed.success
        ? ['SLACK_BOT_TOKEN', 'SLACK_APP_TOKEN', 'SLACK_TEAM_ID', 'SLACK_ALLOWED_USER_ID'].filter(
            (name) => !environment[name],
          )
        : [],
    accountId: environment.SLACK_TEAM_ID,
    allowedSenderIds: parsed.success ? [parsed.data.allowedUserId] : [],
    supportsAttachments: true,
    available: false,
    rich: environment.SLACK_RICH_ENABLED === 'true',
  };
  const lifetime = new AbortController();
  const web = parsed.success ? client(parsed.data.botToken, lifetime.signal) : undefined;
  let connection: SlackSocket | undefined;
  let verified: Awaited<ReturnType<typeof identity>> | undefined;
  let starting: Promise<void> | undefined;
  let replacementNeeded = false;
  let attempts = 0;
  let nextAttempt = 0;
  let terminal = false;
  let closed = false;
  let acceptControl: ((input: MessagingControl) => Promise<void>) | undefined;
  let controlCalls = 0;
  let richReady: Promise<unknown> | undefined;
  let streamUnsupported = false;
  let statusUnsupported = false;
  let cardsUnsupported = false;
  let nextViewAt = 0;
  const nextStreamCall = { start: 0, append: 0, stop: 0 };
  async function pace(kind: keyof typeof nextStreamCall, signal: AbortSignal) {
    const wait = Math.max(0, nextStreamCall[kind] - Date.now());
    nextStreamCall[kind] = Date.now() + wait + (kind === 'append' ? 650 : 3100);
    if (wait) await delay(wait, undefined, { signal });
    signal.throwIfAborted();
  }
  const richTable = () =>
    (richReady ??= pool
      .query(
        `
    CREATE TABLE IF NOT EXISTS channel_slack_views (
      installation text NOT NULL,session_id uuid NOT NULL,root_id text NOT NULL,provider_id text,state text NOT NULL,
      PRIMARY KEY(installation,session_id,root_id));
    CREATE TABLE IF NOT EXISTS channel_slack_reaction_names (
      installation text NOT NULL,message_id text NOT NULL,name text NOT NULL,event_at numeric NOT NULL,
      emoji text NOT NULL,present boolean NOT NULL,PRIMARY KEY(installation,message_id,name));
    CREATE TABLE IF NOT EXISTS channel_slack_files (
      installation text NOT NULL,message_id uuid NOT NULL,part integer NOT NULL,file_id text,state text NOT NULL,
      provider_ids text[] NOT NULL DEFAULT '{}',PRIMARY KEY(installation,message_id,part));
  `,
      )
      .catch((error) => {
        richReady = undefined;
        throw error;
      }));
  const installation = () =>
    createHash('sha256')
      .update(
        `${channel.accountId}:${parsed.success ? parsed.data.allowedUserId : ''}:${verified?.appId}`,
      )
      .digest('hex');

  async function control(type: string, body: unknown, ack: () => Promise<void>) {
    if (!channel.rich || !acceptControl || closed || !verified || !parsed.success) return;
    const basic = z
      .object({
        team_id: z.literal(parsed.data.teamId),
        api_app_id: z.literal(verified.appId),
        event_id: z.string().min(1).max(200),
        event: z.object({
          type: z.literal('agent_session_stopped'),
          user: z.literal(parsed.data.allowedUserId),
          channel: z.literal(verified.dmId),
          thread_ts: z.string().regex(timestamp),
          streaming_message_ts: z.array(z.string().regex(timestamp)).min(1).max(50),
          event_ts: z.string().regex(timestamp),
        }),
      })
      .safeParse(body);
    let input: MessagingControl | undefined;
    if (type === 'events_api' && basic.success) {
      const ids = basic.data.event.streaming_message_ts.map((ts) => `${verified!.dmId}:${ts}`);
      input = {
        channelId: 'slack',
        accountId: parsed.data.teamId,
        senderId: parsed.data.allowedUserId,
        threadId: verified.dmId,
        eventId: basic.data.event_id,
        rootMessageId: `${verified.dmId}:${basic.data.event.thread_ts}`,
        action: 'stop',
        streamIds: ids,
      };
    }
    if (type === 'interactive') {
      const action = z
        .object({
          type: z.literal('block_actions'),
          api_app_id: z.literal(verified.appId),
          team: z.object({ id: z.literal(parsed.data.teamId) }),
          user: z.object({ id: z.literal(parsed.data.allowedUserId) }),
          container: z.object({
            type: z.literal('message'),
            channel_id: z.literal(verified.dmId),
            message_ts: z.string().regex(timestamp),
          }),
          actions: z
            .array(
              z.object({
                action_id: z.enum(['nod_stop', 'nod_status']),
                value: z.uuid(),
                action_ts: z.string().regex(timestamp),
              }),
            )
            .length(1),
        })
        .safeParse(body);
      if (action.success) {
        await richTable();
        const message = `${verified.dmId}:${action.data.container.message_ts}`;
        const view = (
          await pool.query<{ root_id: string }>(
            "SELECT root_id FROM channel_slack_views WHERE installation=$1 AND provider_id=$2 AND state='sent'",
            [installation(), message],
          )
        ).rows[0];
        const selected = action.data.actions[0];
        if (view)
          input = {
            channelId: 'slack',
            accountId: parsed.data.teamId,
            senderId: parsed.data.allowedUserId,
            threadId: verified.dmId,
            eventId: `${message}:${selected.action_ts}:${selected.action_id}:${selected.value}`,
            rootMessageId: view.root_id,
            action: selected.action_id === 'nod_stop' ? 'stop' : 'status',
            nonce: selected.value,
            messageId: message,
          };
      }
    }
    if (input && !closed) {
      await acceptControl(input);
    }
    await ack().catch(() => {});
  }
  const incoming = new Map<
    string,
    { body: z.infer<typeof eventSchema>; ack: () => Promise<void> }
  >();
  const active = new Set<Promise<unknown>>();
  let ready: Promise<unknown> | undefined;
  const table = () =>
    (ready ??= pool
      .query(
        `CREATE TABLE IF NOT EXISTS channel_slack_reactions(account_id text NOT NULL,message_id text NOT NULL,event_at numeric NOT NULL,reaction text,PRIMARY KEY(account_id,message_id))`,
      )
      .catch((error) => {
        ready = undefined;
        throw error;
      }));

  function attachment(file: z.infer<typeof fileSchema>): MessagingAttachment {
    return {
      name: file.name,
      mimeType: file.mimetype,
      size: file.size,
      ...(file.mimetype.startsWith('audio/') ? { voice: true } : {}),
      async read(signal) {
        if (file.size > fileLimit) throw new MessagingAttachmentError(fileLimit);
        const operation = AbortSignal.any([
          lifetime.signal,
          AbortSignal.timeout(60_000),
          ...(signal ? [signal] : []),
        ]);
        const response = await fetch(fileUrl(file.url_private_download ?? file.url_private), {
          headers: { Authorization: `Bearer ${parsed.success ? parsed.data.botToken : ''}` },
          redirect: 'error',
          signal: operation,
        });
        if (!response.ok || !response.body)
          throw new MessagingConnectionError(
            `Slack file download failed: HTTP ${response.status}.`,
          );
        if (Number(response.headers.get('content-length') ?? 0) > fileLimit) {
          await response.body.cancel();
          throw new MessagingAttachmentError(fileLimit);
        }
        const reader = response.body.getReader();
        const chunks: Uint8Array[] = [];
        let size = 0;
        try {
          for (;;) {
            operation.throwIfAborted();
            const next = await reader.read();
            if (next.done) break;
            size += next.value.length;
            if (size > fileLimit) throw new MessagingAttachmentError(fileLimit);
            chunks.push(next.value);
          }
        } finally {
          await reader.cancel().catch(() => {});
          reader.releaseLock();
        }
        return Buffer.concat(chunks);
      },
    };
  }

  function incomingAttachment(value: unknown): MessagingAttachment {
    const file = fileSchema.safeParse(value);
    if (file.success) return attachment(file.data);
    return {
      name: 'Unavailable Slack file',
      mimeType: 'application/octet-stream',
      size: 0,
      async read() {
        throw new MessagingConnectionError('Slack failed: file_metadata_unavailable.');
      },
    };
  }

  function report(error: unknown) {
    if (closed) return;
    channel.available = false;
    channel.error = failure(error).message;
    replacementNeeded = true;
    terminal ||= terminalFailure(error);
    if (terminal) channel.error += ' Reconnect in Channels.';
    nextAttempt =
      Date.now() + Math.max(Math.min(30_000, 2000 * 2 ** (attempts - 1)), retryAfter(error));
  }

  function start() {
    if (closed || !parsed.success || !enabled || starting || terminal || Date.now() < nextAttempt)
      return;
    if (connection && !replacementNeeded) return;
    starting = Promise.resolve()
      .then(async () => {
        try {
          if (replacementNeeded) {
            await connection?.disconnect().catch(() => {});
            connection?.removeAllListeners();
            connection = undefined;
            replacementNeeded = false;
          }
          if (closed) return;
          attempts = Math.min(5, attempts + 1);
          const checked = await identity(web!, parsed.data);
          if (closed) return;
          verified = checked;
          const candidate = socket(parsed.data.appToken, checked.appId, lifetime.signal);
          connection = candidate;
          const current = () => !closed && connection === candidate;
          candidate.on('disconnected', () => {
            if (!current()) return;
            channel.available = false;
            replacementNeeded = true;
          });
          candidate.on('nod_start_failed', (error) => {
            if (!current()) return;
            report(error);
            replacementNeeded = true;
          });
          candidate.on('error', (error) => {
            if (current()) report(error);
          });
          candidate.on('nod_identity_failed', () => {
            if (current()) report(new SlackIdentityError('Slack failed: app_identity_mismatch.'));
          });
          candidate.on('slack_event', ({ type, body, ack }) => {
            if (!current() || terminal) return;
            if (
              channel.rich &&
              (type === 'interactive' ||
                z
                  .object({ event: z.object({ type: z.literal('agent_session_stopped') }) })
                  .safeParse(body).success)
            ) {
              if (controlCalls >= 8) return;
              controlCalls++;
              const pending = control(type, body, ack).catch(() => {});
              active.add(pending);
              void pending.finally(() => {
                active.delete(pending);
                controlCalls--;
              });
              return;
            }
            const value = eventSchema.safeParse(body);
            if (
              type !== 'events_api' ||
              !value.success ||
              value.data.team_id !== parsed.data.teamId ||
              value.data.api_app_id !== verified?.appId ||
              value.data.event.user !== parsed.data.allowedUserId ||
              value.data.event.bot_id ||
              value.data.event.user === verified.botUserId
            ) {
              void ack().catch(() => {});
              return;
            }
            const event = value.data.event;
            const dm = event.type === 'message' ? event.channel : event.item?.channel;
            if (
              dm !== verified.dmId ||
              (event.type === 'message' &&
                (event.channel_type !== 'im' ||
                  (event.subtype &&
                    !['file_share', 'thread_broadcast'].includes(event.subtype)))) ||
              !['message', 'reaction_added', 'reaction_removed'].includes(event.type)
            ) {
              void ack().catch(() => {});
              return;
            }
            if (incoming.size < 100 || incoming.has(value.data.event_id))
              incoming.set(value.data.event_id, { body: value.data, ack });
          });
          await connect(candidate, AbortSignal.any([lifetime.signal, AbortSignal.timeout(20_000)]));
          if (current() && !replacementNeeded && !terminal && verified === checked) {
            channel.available = true;
            channel.error = undefined;
            attempts = 0;
            nextAttempt = 0;
          }
        } catch (error) {
          report(error);
          await connection?.disconnect().catch(() => {});
          connection?.removeAllListeners();
          connection = undefined;
          replacementNeeded = false;
        }
      })
      .catch(report)
      .finally(() => {
        starting = undefined;
      });
  }

  function target(input: { accountId: string; threadId: string }) {
    if (
      closed ||
      !verified ||
      input.accountId !== channel.accountId ||
      input.threadId !== verified.dmId
    )
      throw new MessagingConnectionError(
        channel.error ?? 'Slack failed: direct_message_unavailable.',
      );
    return verified.dmId;
  }
  function messageId(value: string) {
    const [dm, ts, ...rest] = value.split(':');
    if (dm !== verified?.dmId || !timestamp.test(ts ?? '') || rest.length)
      throw new MessagingConnectionError('Slack failed: message_target_mismatch.');
    return ts;
  }
  async function operation<T>(action: () => Promise<T>): Promise<T> {
    lifetime.signal.throwIfAborted();
    const pending = action();
    active.add(pending);
    try {
      return await pending;
    } catch (error) {
      throw failure(error);
    } finally {
      active.delete(pending);
    }
  }

  async function streamReply(input: Parameters<MessagingTransport['post']>[0]) {
    if (!input.stream || !input.rootMessageId) return;
    const dm = target(input);
    const root = messageId(input.rootMessageId);
    const signal = AbortSignal.any([lifetime.signal, ...(input.signal ? [input.signal] : [])]);
    const streamWeb = client(parsed.success ? parsed.data.botToken : '', signal);
    const ids: string[] = [];
    const claimedParts: number[] = [];
    let openStreamId: string | undefined;
    let stoppingStream = false;
    if (!input.text) return;
    const parts = streamMessages(input.text);
    if (!parts) return;
    const warning = (value: unknown) => {
      const result = z
        .object({
          warning: z.string().optional(),
          response_metadata: z.object({ warnings: z.array(z.string()).optional() }).optional(),
        })
        .parse(value);
      for (const reason of [result.warning, ...(result.response_metadata?.warnings ?? [])].filter(
        Boolean,
      )) {
        const code = /^[a-z_]{1,80}$/.test(reason!) ? reason : 'stream_response_warning';
        if (code === 'missing_agent_session_stopped_event_subscription')
          channel.richError = `Slack native Stop is unavailable: ${code}.`;
        else throw new MessagingConnectionError(`Slack failed: ${code}.`);
      }
    };
    try {
      for (const [part, text] of parts.entries()) {
        signal.throwIfAborted();
        await input.stream.record({ part, state: 'starting' });
        claimedParts.push(part);
        let started;
        try {
          await pace('start', signal);
          await input.stream.record({ part, state: 'dispatching' });
          started = await streamWeb.chat.startStream({
            channel: dm,
            thread_ts: root,
            chunks: [{ type: 'markdown_text', text: streamFragments(text, 3000)[0] }],
          });
        } catch (error) {
          if (
            !ids.length &&
            error instanceof WebAPIPlatformError &&
            [
              'missing_scope',
              'not_allowed_token_type',
              'channel_type_not_supported',
              'invalid_thread_ts',
            ].includes(error.data.error)
          ) {
            streamUnsupported = true;
            await input.stream.record({ part, state: 'uncertain' });
            channel.richError = `Streaming unavailable. ${failure(error).message}`;
            return;
          }
          throw error;
        }
        if (!started.ts || !timestamp.test(started.ts))
          throw new MessagingConnectionError('Slack failed: stream_receipt_unavailable.');
        const providerId = `${dm}:${started.ts}`;
        openStreamId = providerId;
        ids.push(providerId);
        const fragments = streamFragments(text, 3000);
        let confirmedChars = fragments[0].length;
        await input.stream.record({ part, state: 'open', id: providerId, confirmedChars });
        warning(started);
        const deadline = Date.now() + 8 * 60_000;
        for (const fragment of fragments.slice(1)) {
          signal.throwIfAborted();
          if (Date.now() > deadline)
            throw new MessagingConnectionError('Slack failed: stream_deadline_exceeded.');
          await pace('append', signal);
          await input.stream.record({ part, state: 'appending' });
          const appended = await streamWeb.chat.appendStream({
            channel: dm,
            ts: started.ts,
            chunks: [{ type: 'markdown_text', text: fragment }],
          });
          warning(appended);
          confirmedChars += fragment.length;
          await input.stream.record({ part, state: 'open', id: providerId, confirmedChars });
        }
        signal.throwIfAborted();
        await pace('stop', signal);
        await input.stream.record({ part, state: 'finalizing' });
        stoppingStream = true;
        const stopped = await streamWeb.chat.stopStream({
          channel: dm,
          ts: started.ts,
          session_status: part < parts.length - 1 ? 'processing' : 'active',
        });
        openStreamId = undefined;
        warning(stopped);
        await input.stream.record({ part, state: 'finalized', id: providerId, confirmedChars });
        stoppingStream = false;
      }
      return { id: ids[0] ?? '', ids };
    } catch (error) {
      const hold = Date.now() + retryAfter(error);
      for (const kind of ['start', 'append', 'stop'] as const)
        nextStreamCall[kind] = Math.max(nextStreamCall[kind], hold);
      for (const part of claimedParts)
        await input.stream.record({ part, state: 'uncertain' }).catch(() => {});
      if (openStreamId && !stoppingStream) {
        const cleanup = client(
          parsed.success ? parsed.data.botToken : '',
          AbortSignal.any([lifetime.signal, AbortSignal.timeout(3000)]),
        );
        await cleanup.chat
          .stopStream({ channel: dm, ts: messageId(openStreamId), session_status: 'active' })
          .catch(() => {});
      }
      throw new MessagingDeliveryError(failure(error).message, ids);
    }
  }

  return {
    channels: [channel],
    controls(accept) {
      acceptControl = accept;
      return () => {
        if (acceptControl === accept) acceptControl = undefined;
      };
    },
    async conversation(input) {
      try {
        if (!channel.rich) return {};
        const dm = target(input);
        const root = messageId(input.rootMessageId);
        const signal = AbortSignal.any([lifetime.signal, input.signal]);
        signal.throwIfAborted();
        const viewWeb = client(parsed.success ? parsed.data.botToken : '', signal);
        const wait = Math.max(0, nextViewAt - Date.now());
        nextViewAt = Date.now() + wait + 1300;
        if (wait) await delay(wait, undefined, { signal });
        await richTable();
        if (!statusUnsupported) {
          try {
            const status = await viewWeb.agents.sessions.setStatus({
              channel_id: dm,
              thread_ts: root,
              status: input.status,
              title: input.title,
              initiator_user_id: parsed.success ? parsed.data.allowedUserId : undefined,
            });
            if (status.warning === 'missing_agent_session_stopped_event_subscription')
              channel.richError =
                'Slack native Stop is unavailable: missing_agent_session_stopped_event_subscription.';
          } catch (error) {
            if (
              error instanceof WebAPIPlatformError &&
              ['missing_scope', 'feature_disabled'].includes(error.data.error)
            ) {
              statusUnsupported = true;
              channel.richError = `Native status unavailable. ${failure(error).message}`;
            } else throw failure(error);
          }
        }
        const key = installation();
        const previous = (
          await pool.query<{ provider_id: string | null; state: string }>(
            'SELECT provider_id,state FROM channel_slack_views WHERE installation=$1 AND session_id=$2 AND root_id=$3',
            [key, input.sessionId, input.rootMessageId],
          )
        ).rows[0];
        if (previous && previous.state !== 'sent') return {};
        const text = [
          input.title,
          input.outcome === 'failed'
            ? 'Failed'
            : input.outcome === 'cancelled'
              ? 'Stopped'
              : input.status === 'processing'
                ? 'Working'
                : input.status === 'suspended'
                  ? 'Needs you'
                  : input.status === 'closed'
                    ? 'Closed'
                    : 'Ready',
          input.text,
          ...(input.task?.steps ?? []).map((title, index) => `${index + 1}. ${title}`),
        ]
          .filter(Boolean)
          .join('\n');
        const blocks: NonNullable<ChatStopStreamArguments['blocks']> = [
          { type: 'section', text: { type: 'plain_text', text: text.slice(0, 2900) } },
        ];
        if (input.task && !cardsUnsupported) {
          const status = input.outcome
            ? 'error'
            : input.task.state === 'done'
              ? 'complete'
              : input.task.state === 'open'
                ? 'in_progress'
                : 'error';
          blocks.push({
            type: 'task_card',
            task_id: 'task',
            title: input.title,
            status,
          });
        }
        if (input.control)
          blocks.push({
            type: 'actions',
            elements: [
              {
                type: 'button',
                text: { type: 'plain_text', text: 'Stop' },
                action_id: 'nod_stop',
                value: input.control.stop,
              },
              {
                type: 'button',
                text: { type: 'plain_text', text: 'Status' },
                action_id: 'nod_status',
                value: input.control.status,
              },
            ],
          });
        const render = async () => {
          const send = () => {
            if (previous?.provider_id) {
              return viewWeb.chat.update({
                channel: dm,
                ts: messageId(previous.provider_id),
                text,
                blocks,
              });
            }
            return viewWeb.chat.postMessage({
              channel: dm,
              thread_ts: root,
              text,
              blocks,
              unfurl_links: false,
              unfurl_media: false,
            });
          };
          try {
            return await send();
          } catch (error) {
            const card = blocks.findIndex((block) => block.type === 'task_card');
            if (
              !(error instanceof WebAPIPlatformError) ||
              error.data.error !== 'invalid_blocks' ||
              card < 0
            )
              throw error;
            cardsUnsupported = true;
            channel.richError = 'Slack task cards unavailable: invalid_blocks.';
            blocks.splice(card, 1);
            return send();
          }
        };
        if (previous?.provider_id) {
          await render();
          return { id: previous.provider_id };
        }
        const claim = await pool.query(
          "INSERT INTO channel_slack_views(installation,session_id,root_id,state) VALUES($1,$2,$3,'sending') ON CONFLICT DO NOTHING RETURNING session_id",
          [key, input.sessionId, input.rootMessageId],
        );
        if (!claim.rowCount) return {};
        const sent = await render();
        if (!sent.ts || !timestamp.test(sent.ts))
          throw new MessagingConnectionError('Slack failed: control_receipt_unavailable.');
        const providerId = `${dm}:${sent.ts}`;
        await pool.query(
          "UPDATE channel_slack_views SET state='sent',provider_id=$3 WHERE installation=$1 AND session_id=$2 AND root_id=$4",
          [key, input.sessionId, providerId, input.rootMessageId],
        );
        return { id: providerId };
      } catch (error) {
        nextViewAt = Math.max(nextViewAt, Date.now() + retryAfter(error));
        channel.richError = failure(error).message;
        throw failure(error);
      }
    },
    async poll(accept, _pair, react) {
      if (!enabled || !parsed.success || closed) return;
      start();
      for (const [key, item] of incoming) {
        if (closed) return;
        const event = item.body.event;
        if (
          event.type === 'message' &&
          timestamp.test(event.ts ?? '') &&
          (!event.thread_ts || timestamp.test(event.thread_ts))
        ) {
          const text = (event.text ?? '')
            .replace(/<((?:https?:\/\/|mailto:)[^>|]+)(?:\|([^>]+))?>/g, (_, url, label) =>
              label ? `[${label}](${url})` : url,
            )
            .replaceAll('&lt;', '<')
            .replaceAll('&gt;', '>')
            .replaceAll('&amp;', '&');
          const command = /^nod\s+(usage|memory|tasks|status|stop)$/i.exec(text.trim());
          await accept({
            channelId: 'slack',
            accountId: parsed.data.teamId,
            senderId: parsed.data.allowedUserId,
            messageId: `${verified!.dmId}:${event.ts}`,
            threadId: verified!.dmId,
            text: command ? `/${command[1].toLowerCase()}` : text,
            sentAt: new Date(Number(event.ts) * 1000),
            ...(event.thread_ts && event.thread_ts !== event.ts
              ? { replyToMessageId: `${verified!.dmId}:${event.thread_ts}` }
              : {}),
            ...(event.files?.length ? { attachments: event.files.map(incomingAttachment) } : {}),
          } satisfies InboundMessage);
        } else if (react && event.item?.type === 'message' && event.reaction && event.event_ts) {
          const emoji = reactionEmoji(event.reaction);
          if (isMessageReaction(emoji)) {
            if (channel.rich) {
              await richTable();
              const targetId = `${verified!.dmId}:${event.item.ts}`;
              const claimed = await pool.query(
                `INSERT INTO channel_slack_reaction_names(installation,message_id,name,event_at,emoji,present) VALUES($1,$2,$3,$4,$5,$6)
                ON CONFLICT(installation,message_id,name) DO UPDATE SET event_at=EXCLUDED.event_at,present=EXCLUDED.present
                WHERE channel_slack_reaction_names.event_at<EXCLUDED.event_at RETURNING name`,
                [
                  installation(),
                  targetId,
                  event.reaction,
                  event.event_ts,
                  emoji,
                  event.type === 'reaction_added',
                ],
              );
              if (claimed.rowCount) {
                const latest = (
                  await pool.query<{ emoji: string }>(
                    'SELECT emoji FROM channel_slack_reaction_names WHERE installation=$1 AND message_id=$2 AND present ORDER BY event_at DESC,name LIMIT 1',
                    [installation(), targetId],
                  )
                ).rows[0];
                await react({
                  channelId: 'slack',
                  accountId: parsed.data.teamId,
                  senderId: parsed.data.allowedUserId,
                  messageId: targetId,
                  reaction: latest?.emoji ?? null,
                  displayOnly: event.type === 'reaction_removed' || latest?.emoji !== emoji,
                });
              }
              await item.ack().catch(() => {});
              incoming.delete(key);
              continue;
            }
            await table();
            const targetId = `${verified!.dmId}:${event.item.ts}`;
            const claimed = await pool.query(
              `INSERT INTO channel_slack_reactions(account_id,message_id,event_at,reaction) VALUES($1,$2,$3,$4)
              ON CONFLICT(account_id,message_id) DO UPDATE SET event_at=EXCLUDED.event_at,reaction=CASE WHEN $5 AND channel_slack_reactions.reaction IS DISTINCT FROM $6 THEN channel_slack_reactions.reaction ELSE EXCLUDED.reaction END
              WHERE channel_slack_reactions.event_at < EXCLUDED.event_at RETURNING reaction`,
              [
                createHash('sha256')
                  .update(`${parsed.data.teamId}:${parsed.data.allowedUserId}`)
                  .digest('hex'),
                targetId,
                event.event_ts,
                event.type === 'reaction_removed' ? null : emoji,
                event.type === 'reaction_removed',
                emoji,
              ],
            );
            if (
              claimed.rowCount &&
              (event.type === 'reaction_added' || claimed.rows[0].reaction === null)
            )
              await react({
                channelId: 'slack',
                accountId: parsed.data.teamId,
                senderId: parsed.data.allowedUserId,
                messageId: targetId,
                reaction: claimed.rows[0].reaction,
              } satisfies InboundReaction);
          }
        }
        await item.ack().catch(() => {});
        incoming.delete(key);
      }
    },
    async receive() {
      return { status: 404, body: { error: 'Slack Socket Mode does not use a webhook.' } };
    },
    async post(input) {
      const dm = target(input);
      if (input.replyToMessageId) messageId(input.replyToMessageId);
      if (input.rootMessageId) messageId(input.rootMessageId);
      input.signal?.throwIfAborted();
      const sendSignal = AbortSignal.any([
        lifetime.signal,
        ...(input.signal ? [input.signal] : []),
      ]);
      const postWeb = client(parsed.success ? parsed.data.botToken : '', sendSignal);
      if (channel.rich && input.stream && !streamUnsupported) {
        const result = await streamReply(input);
        if (result) return result;
      }
      const ids: string[] = [];
      try {
        const parts = slackMessages(
          input.text || input.attachments?.map((file) => file.name).join('\n') || '',
        );
        for (const text of parts) {
          sendSignal.throwIfAborted();
          const result = await operation(() =>
            postWeb.chat.postMessage({
              channel: dm,
              ...(input.rootMessageId ? { thread_ts: messageId(input.rootMessageId) } : {}),
              text,
              unfurl_links: false,
              unfurl_media: false,
              parse: 'none',
              link_names: false,
            }),
          );
          if (!result.ts || !timestamp.test(result.ts))
            throw new MessagingConnectionError('Slack failed: message_receipt_unavailable.');
          ids.push(`${dm}:${result.ts}`);
        }
        const unsent: string[] = [];
        let uncertain: MessagingConnectionError | undefined;
        for (const [part, file] of (input.attachments ?? []).entries()) {
          let bytes: Uint8Array;
          try {
            if (!file.size || file.size > fileLimit) throw new MessagingAttachmentError(fileLimit);
            bytes = await file.read(sendSignal);
            if (bytes.length !== file.size || bytes.length > fileLimit)
              throw new MessagingAttachmentError(fileLimit);
          } catch {
            unsent.push(file.name);
            continue;
          }
          let fileClaim = false;
          const recordFile = channel.rich && input.deliveryId;
          try {
            if (recordFile) {
              await richTable();
              const claimed = await pool.query(
                "INSERT INTO channel_slack_files(installation,message_id,part,state) VALUES($1,$2,$3,'preparing') ON CONFLICT DO NOTHING RETURNING part",
                [installation(), input.deliveryId, part],
              );
              if (!claimed.rowCount)
                throw new MessagingConnectionError('Slack failed: file_delivery_already_claimed.');
              fileClaim = true;
            }
            const upload = await operation(() =>
              postWeb.files.getUploadURLExternal({ filename: file.name, length: bytes.length }),
            );
            if (!upload.file_id)
              throw new MessagingConnectionError('Slack failed: upload_receipt_unavailable.');
            if (fileClaim)
              await pool.query(
                "UPDATE channel_slack_files SET file_id=$4,state='uploading' WHERE installation=$1 AND message_id=$2 AND part=$3",
                [installation(), input.deliveryId, part, upload.file_id],
              );
            const response = await operation(() =>
              fetch(fileUrl(upload.upload_url, true), {
                method: 'POST',
                body: Buffer.from(bytes),
                redirect: 'error',
                signal: AbortSignal.any([sendSignal, AbortSignal.timeout(60_000)]),
              }),
            );
            if (!response.ok)
              throw new MessagingConnectionError(
                `Slack file upload failed: HTTP ${response.status}.`,
              );
            if (fileClaim)
              await pool.query(
                "UPDATE channel_slack_files SET state='completing' WHERE installation=$1 AND message_id=$2 AND part=$3",
                [installation(), input.deliveryId, part],
              );
            await operation(() =>
              postWeb.files.completeUploadExternal({
                files: [{ id: upload.file_id!, title: file.name }],
                channel_id: dm,
                ...(input.rootMessageId ? { thread_ts: messageId(input.rootMessageId) } : {}),
              }),
            );
            const shared = await operation(() => postWeb.files.info({ file: upload.file_id! }));
            const shares = z
              .object({
                shares: z
                  .object({
                    private: z
                      .record(
                        z.string(),
                        z.array(
                          z.object({
                            ts: z.string().regex(timestamp),
                            thread_ts: z.string().regex(timestamp).optional(),
                          }),
                        ),
                      )
                      .optional(),
                  })
                  .optional(),
              })
              .safeParse(shared.file);
            const receipts = shares.success
              ? (shares.data.shares?.private?.[dm] ?? []).filter(
                  (share) =>
                    !input.rootMessageId || share.thread_ts === messageId(input.rootMessageId),
                )
              : [];
            if (!receipts.length)
              throw new MessagingConnectionError('Slack failed: file_message_receipt_unavailable.');
            for (const share of receipts) ids.push(`${dm}:${share.ts}`);
            if (fileClaim)
              await pool.query(
                "UPDATE channel_slack_files SET state='confirmed',provider_ids=$4 WHERE installation=$1 AND message_id=$2 AND part=$3",
                [
                  installation(),
                  input.deliveryId,
                  part,
                  receipts.map((share) => `${dm}:${share.ts}`),
                ],
              );
          } catch (error) {
            if (fileClaim)
              await pool
                .query(
                  "UPDATE channel_slack_files SET state='uncertain' WHERE installation=$1 AND message_id=$2 AND part=$3 AND state<>'confirmed'",
                  [installation(), input.deliveryId, part],
                )
                .catch(() => {});
            uncertain ??= failure(error);
            if (closed) throw uncertain;
          }
        }
        if (unsent.length) {
          const notice = await input.attachmentFailure?.(unsent);
          if (!notice) throw new MessagingConnectionError('Slack failed: attachment_unavailable.');
          for (const text of slackMessages(notice)) {
            const sent = await operation(() =>
              postWeb.chat.postMessage({
                channel: dm,
                ...(input.rootMessageId ? { thread_ts: messageId(input.rootMessageId) } : {}),
                text,
                unfurl_links: false,
                unfurl_media: false,
                parse: 'none',
              }),
            );
            if (!sent.ts || !timestamp.test(sent.ts))
              throw new MessagingConnectionError('Slack failed: message_receipt_unavailable.');
            ids.push(`${dm}:${sent.ts}`);
          }
        }
        if (uncertain) throw uncertain;
        return { id: ids[0] ?? '', ids };
      } catch (error) {
        throw new MessagingDeliveryError(failure(error).message, ids);
      }
    },
    async react(input) {
      const dm = target(input);
      input.signal?.throwIfAborted();
      const ts = messageId(input.messageId);
      const desired = input.reaction ? reactionName(input.reaction) : undefined;
      if (input.reaction && !desired) return 'unsupported';
      const reactionWeb = client(
        parsed.success ? parsed.data.botToken : '',
        AbortSignal.any([lifetime.signal, ...(input.signal ? [input.signal] : [])]),
      );
      const current = await operation(() =>
        reactionWeb.reactions.get({ channel: dm, timestamp: ts, full: true }),
      );
      const previous = (current.message?.reactions ?? [])
        .filter((reaction) => reaction.users?.includes(verified!.botUserId))
        .map((reaction) => reaction.name)
        .filter((name): name is string => Boolean(name && /^[a-z0-9_+:-]{1,100}$/.test(name)));
      const change = async (name: string, remove: boolean) => {
        input.signal?.throwIfAborted();
        try {
          await operation(() =>
            remove
              ? reactionWeb.reactions.remove({ channel: dm, timestamp: ts, name })
              : reactionWeb.reactions.add({ channel: dm, timestamp: ts, name }),
          );
        } catch (error) {
          if (
            error instanceof MessagingConnectionError &&
            error.message === 'Slack failed: invalid_name.'
          )
            return 'unsupported' as const;
          if (!(
            error instanceof MessagingConnectionError &&
            (remove
              ? error.message === 'Slack failed: no_reaction.'
              : error.message === 'Slack failed: already_reacted.')
          ))
            throw error;
        }
      };
      for (const name of previous) {
        if (name !== desired && (await change(name, true)) === 'unsupported') return 'unsupported';
      }
      if (desired && !previous.includes(desired)) {
        if ((await change(desired, false)) === 'unsupported') return 'unsupported';
      }
      return 'sent';
    },
    async close() {
      closed = true;
      channel.available = false;
      lifetime.abort();
      incoming.clear();
      await connection?.disconnect().catch(() => {});
      await starting?.catch(() => {});
      await Promise.allSettled(active);
      connection?.removeAllListeners();
    },
  };
}
