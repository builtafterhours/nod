import { randomUUID } from 'node:crypto';
import type { Pool, PoolClient } from 'pg';
import type { MessagingControl, MessagingTransport } from '../ports/messaging.ts';
import { transaction } from './transaction.ts';
import { logFailure } from './failure-log.ts';
import { workOriginSql } from './turn-chain.ts';
import { z } from 'zod';

interface Root {
  owner_id: string;
  account_id: string;
  sender_id: string;
  dm: string;
  session_id: string;
  thread_id: string;
  turn_id: string;
  channel: string;
  root_id: string | null;
  target_id: string | null;
  nonce: string | null;
  status_nonce: string | null;
  expires_at: Date | null;
  control_message_id: string | null;
  state: string;
  last_status: string | null;
  lifecycle_revision: string;
}

export async function createChannelRich(
  pool: Pool,
  options: {
    ownerId: string;
    transport(): MessagingTransport;
    permitted(identity: { provider: string; account_id: string; sender_id: string }): boolean;
    safe(text: string): { text: string; secureAction: boolean; artifact: boolean };
    lock?(client: PoolClient): Promise<unknown>;
    cancel?<T>(work: (client: PoolClient) => Promise<T>): Promise<T>;
    control?(
      client: PoolClient,
      target: { threadId: string; sessionId: string; turnId: string; channel: string },
    ): Promise<() => Promise<void>>;
  },
) {
  await pool.query(`CREATE TABLE IF NOT EXISTS channel_roots (
    owner_id text NOT NULL,account_id text NOT NULL,sender_id text NOT NULL,dm text NOT NULL,
    session_id uuid NOT NULL,thread_id uuid NOT NULL,turn_id uuid NOT NULL,channel text NOT NULL,
    root_id text,target_id uuid,nonce uuid,status_nonce uuid,expires_at timestamptz,control_message_id text,state text NOT NULL DEFAULT 'preparing',last_status text,lifecycle_revision bigint NOT NULL DEFAULT 0,
    PRIMARY KEY(owner_id,account_id,sender_id,dm,session_id));
    CREATE TABLE IF NOT EXISTS channel_controls(owner_id text NOT NULL,event_id text NOT NULL,PRIMARY KEY(owner_id,event_id));
    CREATE TABLE IF NOT EXISTS channel_streams(owner_id text NOT NULL,message_id uuid NOT NULL,part integer NOT NULL,
      account_id text NOT NULL,sender_id text NOT NULL,dm text NOT NULL,session_id uuid NOT NULL,turn_id uuid NOT NULL,
      root_id text NOT NULL,revision uuid NOT NULL,provider_id text,state text NOT NULL,confirmed_chars integer NOT NULL DEFAULT 0,
      PRIMARY KEY(owner_id,message_id,part));`);
  await pool.query(
    "UPDATE channel_roots SET state='uncertain',nonce=NULL WHERE owner_id=$1 AND state='preparing'",
    [options.ownerId],
  );
  await pool.query('UPDATE channel_roots SET nonce=NULL,status_nonce=NULL WHERE owner_id=$1', [
    options.ownerId,
  ]);
  let lifetime = new AbortController();
  let revision = randomUUID();
  await pool.query(
    "UPDATE channel_streams SET state='uncertain' WHERE owner_id=$1 AND state NOT IN ('finalized','uncertain')",
    [options.ownerId],
  );
  let closed = false;
  let used = false;
  let running: Promise<void> | undefined;
  let release: (() => void) | undefined;
  const shown = new Map<string, string>();
  const permitted = (row: Root) =>
    options.permitted({ provider: 'slack', account_id: row.account_id, sender_id: row.sender_id });
  const rich = () =>
    options
      .transport()
      .channels.some((channel) => channel.id === 'slack' && channel.rich && channel.available);

  async function accept(input: MessagingControl) {
    if (
      !z
        .object({
          channelId: z.literal('slack'),
          accountId: z.string().min(1).max(100),
          senderId: z.string().min(1).max(100),
          threadId: z.string().min(1).max(100),
          eventId: z.string().min(1).max(500),
          rootMessageId: z.string().min(1).max(100),
          action: z.enum(['stop', 'status']),
          nonce: z.uuid().optional(),
          messageId: z.string().max(100).optional(),
          streamIds: z.array(z.string().min(1).max(100)).max(50).optional(),
        })
        .safeParse(input).success
    )
      return;
    if (closed || lifetime.signal.aborted || !rich() || input.channelId !== 'slack') return;
    const signal = lifetime.signal;
    const after = await (options.cancel ?? ((work) => transaction(work, pool)))(async (client) => {
      await options.lock?.(client);
      const row = (
        await client.query<Root>(
          `SELECT r.* FROM channel_roots r JOIN threads th ON th.id=r.thread_id AND th.owner_id=r.owner_id
         WHERE r.owner_id=$1 AND r.account_id=$2 AND r.sender_id=$3 AND r.dm=$4 AND r.root_id=$5
           AND r.state='open' AND th.archived_at IS NULL
           AND (r.session_id=r.thread_id OR EXISTS(SELECT 1 FROM tasks t WHERE t.owner_id=r.owner_id AND t.session_id=r.session_id AND t.state='open')) FOR UPDATE OF r`,
          [options.ownerId, input.accountId, input.senderId, input.threadId, input.rootMessageId],
        )
      ).rows[0];
      if (!row || !permitted(row) || closed || lifetime.signal.aborted) return;
      if (
        input.nonce
          ? input.nonce !== (input.action === 'stop' ? row.nonce : row.status_nonce) ||
            input.messageId !== row.control_message_id ||
            !row.expires_at ||
            row.expires_at.getTime() <= Date.now()
          : !input.streamIds?.length || input.action !== 'stop'
      )
        return;
      if (!input.nonce) {
        const streams = await client.query(
          `SELECT provider_id FROM channel_streams WHERE owner_id=$1 AND account_id=$2 AND sender_id=$3 AND dm=$4 AND root_id=$5
          AND session_id=$6 AND turn_id=$7 AND revision=$8 AND state='open' AND provider_id=ANY($9::text[]) FOR UPDATE`,
          [
            options.ownerId,
            row.account_id,
            row.sender_id,
            row.dm,
            row.root_id,
            row.session_id,
            row.turn_id,
            revision,
            input.streamIds,
          ],
        );
        if (streams.rowCount !== new Set(input.streamIds).size) return;
      }
      const current = await client.query(
        "SELECT 1 FROM turns WHERE id=$1 AND owner_id=$2 AND session_id=$3 AND thread_id=$4 AND channel=$5 AND status='running'",
        [row.turn_id, options.ownerId, row.session_id, row.thread_id, row.channel],
      );
      if (!current.rowCount || signal.aborted) return;
      const claim = await client.query(
        'INSERT INTO channel_controls(owner_id,event_id) VALUES($1,$2) ON CONFLICT DO NOTHING RETURNING event_id',
        [options.ownerId, input.eventId],
      );
      if (!claim.rowCount) return;
      if (input.action === 'status') {
        shown.delete(row.session_id);
        return;
      }
      if (!options.control) return;
      signal.throwIfAborted();
      const finish = await options.control(client, {
        threadId: row.thread_id,
        sessionId: row.session_id,
        turnId: row.turn_id,
        channel: row.channel,
      });
      await client.query(
        "UPDATE channel_streams SET state='uncertain' WHERE owner_id=$1 AND turn_id=$2 AND state<>'finalized'",
        [options.ownerId, row.turn_id],
      );
      await client.query(
        'UPDATE channel_roots SET nonce=NULL,status_nonce=NULL WHERE owner_id=$1 AND session_id=$2',
        [options.ownerId, row.session_id],
      );
      return finish;
    });
    await after?.();
  }

  function listen() {
    release?.();
    release = options.transport().controls?.((input) =>
      accept(input).catch((cause) => {
        logFailure('channel_control_failed', cause);
        throw cause;
      }),
    );
  }

  async function step(targetSessionId?: string) {
    if (closed || !rich() || lifetime.signal.aborted) return;
    used = true;
    const signal = lifetime.signal;
    const retired = await pool.query<Root>(
      `SELECT r.* FROM channel_roots r JOIN threads th ON th.id=r.thread_id AND th.owner_id=r.owner_id AND th.archived_at IS NULL
      WHERE r.owner_id=$1 AND r.state='open' AND (r.last_status IS NULL OR r.last_status IN ('processing','suspended'))
      AND NOT EXISTS(SELECT 1 FROM turns live WHERE live.owner_id=r.owner_id AND live.session_id=r.session_id AND live.channel=r.channel AND live.status='running')
      AND EXISTS(SELECT 1 FROM turns n JOIN turns old ON old.id=r.turn_id AND old.owner_id=r.owner_id
        WHERE n.owner_id=r.owner_id AND n.session_id=r.session_id AND n.channel<>r.channel AND (n.created_at,n.id)>(old.created_at,old.id)) LIMIT 100`,
      [options.ownerId],
    );
    for (const row of retired.rows) {
      if (signal.aborted || !permitted(row) || !row.root_id) continue;
      await pool.query(
        'UPDATE channel_roots SET nonce=NULL,status_nonce=NULL WHERE owner_id=$1 AND session_id=$2 AND dm=$3',
        [options.ownerId, row.session_id, row.dm],
      );
      try {
        await options.transport().conversation?.({
          ownerId: options.ownerId,
          channelId: 'slack',
          accountId: row.account_id,
          threadId: row.dm,
          sessionId: row.session_id,
          turnId: row.turn_id,
          rootMessageId: row.root_id,
          status: 'active',
          title: 'Nod',
          text: '',
          signal,
        });
        if (!signal.aborted)
          await pool.query(
            "UPDATE channel_roots SET last_status='active' WHERE owner_id=$1 AND session_id=$2 AND dm=$3 AND lifecycle_revision=$4",
            [options.ownerId, row.session_id, row.dm, row.lifecycle_revision],
          );
        shown.delete(row.session_id);
      } catch (cause) {
        if (!signal.aborted) logFailure('channel_session_failed', cause);
      }
    }
    const { rows } = await pool.query<
      Root & {
        title: string;
        text: string;
        status: string;
        task_state: string | null;
        steps: string[];
        step_index: number;
      }
    >(
      `SELECT * FROM (SELECT DISTINCT ON (u.session_id) $1::text AS owner_id,i.account_id,i.sender_id,i.thread_id AS dm,
       u.session_id,u.thread_id,u.id AS turn_id,u.channel,COALESCE(t.message_id,i.message_id) AS target_id,
       COALESCE(t.title,'Nod') AS title,COALESCE(t.now_text,'') AS text,u.status,t.state AS task_state,t.steps,t.step_index,
       u.created_at AS at,bound.root_id AS bound_root_id,bound.last_status,bound.state AS binding_state,
       CASE WHEN t.id IS NULL THEN substring(i.source_id from length('channel:slack:'||i.account_id||':')+1) END AS root_id
       FROM turns u JOIN threads th ON th.id=u.thread_id AND th.owner_id=u.owner_id AND th.archived_at IS NULL
       JOIN LATERAL (
         WITH RECURSIVE lineage AS (
           SELECT u.id,u.parent_turn_id,0 AS depth
           UNION ALL SELECT p.id,p.parent_turn_id,c.depth+1 FROM lineage c JOIN turns p ON p.id=c.parent_turn_id
             AND p.owner_id=u.owner_id AND p.thread_id=u.thread_id AND p.channel=u.channel WHERE c.depth<200)
         SELECT receipt.* FROM lineage c JOIN channel_inbox receipt ON receipt.owner_id=u.owner_id AND receipt.channel=u.channel AND receipt.state='submitted'
           AND CASE WHEN u.channel_source_id IS NOT NULL THEN receipt.source_id=u.channel_source_id ELSE receipt.turn_id=c.id END
         ORDER BY c.depth LIMIT 1
       ) i ON true
       LEFT JOIN tasks t ON t.owner_id=u.owner_id AND t.session_id=u.session_id
       LEFT JOIN channel_roots bound ON bound.owner_id=u.owner_id AND bound.session_id=u.session_id AND bound.account_id=i.account_id AND bound.sender_id=i.sender_id AND bound.dm=i.thread_id
       ${workOriginSql('u')}
       WHERE u.owner_id=$1 AND i.provider='slack' AND u.source IN ('user','task','resume') AND NOT u.proactive
         AND (u.session_id=u.thread_id OR t.id IS NOT NULL)
         AND work_origin.source='user' AND work_task.activity_key IS NULL
         AND ($2::uuid IS NULL OR u.session_id=$2)
         AND NOT EXISTS(SELECT 1 FROM turns n WHERE n.owner_id=u.owner_id AND n.session_id=u.session_id AND n.id<>u.id
           AND ((n.status='running' AND u.status<>'running') OR ((n.status='running')=(u.status='running') AND (n.created_at,n.id)>(u.created_at,u.id))))
       ORDER BY u.session_id,(u.status='running') DESC,u.created_at DESC,u.id DESC,i.received_at DESC) current
       WHERE $2::uuid IS NOT NULL OR status IN ('queued','running','waiting') OR (binding_state='open' AND (last_status IS NULL OR last_status IN ('processing','suspended')))
       ORDER BY (status='running') DESC,(status='queued') DESC,(status='waiting') DESC,at DESC LIMIT 100`,
      [options.ownerId, targetSessionId ?? null],
    );
    for (const row of rows) {
      try {
        if (closed || signal.aborted || !permitted(row)) continue;
        const existing = (
          await pool.query<Root>(
            'SELECT * FROM channel_roots WHERE owner_id=$1 AND account_id=$2 AND sender_id=$3 AND session_id=$4 AND dm=$5',
            [row.owner_id, row.account_id, row.sender_id, row.session_id, row.dm],
          )
        ).rows[0];
        if (existing && ['uncertain', 'preparing'].includes(existing.state)) continue;
        if (existing && (existing.thread_id !== row.thread_id || existing.dm !== row.dm)) continue;
        const status =
          row.status === 'running' || row.status === 'queued'
            ? 'processing'
            : row.status === 'waiting'
              ? 'suspended'
              : row.task_state && row.task_state !== 'open'
                ? 'closed'
                : 'active';
        if (!existing && status !== 'processing' && !targetSessionId) continue;
        if (!existing && ['failed', 'cancelled'].includes(row.status)) continue;
        const root = existing?.root_id ?? row.root_id;
        const claim = randomUUID();
        if (!existing) {
          const created = await pool.query(
            `INSERT INTO channel_roots(owner_id,account_id,sender_id,dm,session_id,thread_id,turn_id,channel,root_id,target_id,nonce,state)
          VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'preparing') ON CONFLICT DO NOTHING RETURNING session_id`,
            [
              row.owner_id,
              row.account_id,
              row.sender_id,
              row.dm,
              row.session_id,
              row.thread_id,
              row.turn_id,
              row.channel,
              root,
              row.target_id,
              claim,
            ],
          );
          if (!created.rowCount) continue;
          try {
            const sent = root
              ? { id: root }
              : await options.transport().post({
                  channelId: 'slack',
                  accountId: row.account_id,
                  threadId: row.dm,
                  text: options.safe(row.title).text.slice(0, 200) || 'Nod',
                  signal,
                });
            await pool.query(
              "UPDATE channel_roots SET root_id=$3,state='open' WHERE owner_id=$1 AND session_id=$2 AND nonce=$4",
              [row.owner_id, row.session_id, sent.id, claim],
            );
            row.root_id = sent.id;
          } catch (cause) {
            await pool.query(
              "UPDATE channel_roots SET state='uncertain',nonce=NULL WHERE owner_id=$1 AND session_id=$2 AND dm=$3",
              [row.owner_id, row.session_id, row.dm],
            );
            throw cause;
          }
        } else row.root_id = existing.root_id;
        if (!row.root_id || signal.aborted) continue;
        const nonce =
          row.status === 'running'
            ? existing?.turn_id === row.turn_id &&
              existing.nonce &&
              existing.expires_at &&
              existing.expires_at.getTime() > Date.now()
              ? existing.nonce
              : randomUUID()
            : null;
        const binding = await pool.query<{ status_nonce: string }>(
          `UPDATE channel_roots SET turn_id=$3,nonce=$4,status_nonce=CASE WHEN nonce IS NOT DISTINCT FROM $4::uuid THEN status_nonce ELSE $6::uuid END,
         expires_at=CASE WHEN nonce IS NOT DISTINCT FROM $4::uuid THEN expires_at ELSE now()+interval '15 minutes' END,target_id=COALESCE(target_id,$5) WHERE owner_id=$1 AND session_id=$2 AND dm=$7 RETURNING status_nonce`,
          [row.owner_id, row.session_id, row.turn_id, nonce, row.target_id, randomUUID(), row.dm],
        );
        const safe = options.safe(row.text);
        const text = safe.secureAction || safe.artifact ? '' : safe.text.slice(0, 1000);
        const task = row.task_state
          ? {
              state: row.task_state,
              steps: (row.steps ?? [])
                .slice(0, 50)
                .map((step) => options.safe(step).text.slice(0, 200)),
              stepIndex: row.step_index ?? 0,
            }
          : undefined;
        const signature = JSON.stringify([
          row.turn_id,
          status,
          text,
          nonce,
          task,
          status === 'processing' ? Math.floor(Date.now() / (45 * 60_000)) : 0,
        ]);
        if (shown.get(row.session_id) === signature && existing?.last_status === status) continue;
        const view = await options.transport().conversation?.({
          ownerId: row.owner_id,
          channelId: 'slack',
          accountId: row.account_id,
          threadId: row.dm,
          sessionId: row.session_id,
          turnId: row.turn_id,
          rootMessageId: row.root_id,
          status,
          title: options.safe(row.title).text.slice(0, 200) || 'Nod',
          text,
          control:
            nonce && binding.rows[0]?.status_nonce
              ? {
                  stop: nonce,
                  status: binding.rows[0].status_nonce,
                }
              : undefined,
          task,
          outcome: row.status === 'failed' || row.status === 'cancelled' ? row.status : undefined,
          signal,
        });
        if (view?.id && !signal.aborted)
          await pool.query(
            'UPDATE channel_roots SET control_message_id=$3 WHERE owner_id=$1 AND session_id=$2 AND turn_id=$4 AND dm=$5',
            [row.owner_id, row.session_id, view.id, row.turn_id, row.dm],
          );
        if (!signal.aborted) {
          const saved = await pool.query(
            'UPDATE channel_roots SET last_status=$4 WHERE owner_id=$1 AND session_id=$2 AND dm=$3 AND lifecycle_revision=$5',
            [options.ownerId, row.session_id, row.dm, status, existing?.lifecycle_revision ?? '0'],
          );
          if (saved.rowCount) shown.set(row.session_id, signature);
        }
      } catch (cause) {
        if (!signal.aborted) logFailure('channel_session_failed', cause);
      }
    }
  }
  function refresh() {
    if (running || closed) return;
    running = step()
      .catch((cause) => logFailure('channel_session_failed', cause))
      .finally(() => {
        running = undefined;
      });
  }
  listen();
  const timer = setInterval(refresh, 2000);
  timer.unref();
  return {
    refresh,
    async prepare(sessionId: string) {
      await running;
      if (closed || lifetime.signal.aborted) return;
      running = step(sessionId)
        .catch((cause) => logFailure('channel_session_failed', cause))
        .finally(() => {
          running = undefined;
        });
      await running;
    },
    invalidate() {
      shown.clear();
      refresh();
    },
    async root(
      identity: { account_id: string; sender_id: string; thread_id: string },
      sessionId: string,
    ) {
      return (
        await pool.query<Root>(
          "SELECT * FROM channel_roots WHERE owner_id=$1 AND account_id=$2 AND sender_id=$3 AND session_id=$4 AND dm=$5 AND state='open'",
          [options.ownerId, identity.account_id, identity.sender_id, sessionId, identity.thread_id],
        )
      ).rows[0];
    },
    async resolve(identity: { account_id: string; sender_id: string }, dm: string, root: string) {
      const row = (
        await pool.query<{ target_id: string; allowed: boolean }>(
          `SELECT r.target_id,(r.account_id=$2 AND r.sender_id=$3 AND r.dm=$4 AND r.state='open' AND th.archived_at IS NULL
            AND (r.session_id=r.thread_id OR t.state='open')) AS allowed
           FROM channel_roots r JOIN threads th ON th.id=r.thread_id AND th.owner_id=r.owner_id
           LEFT JOIN tasks t ON t.session_id=r.session_id AND t.owner_id=r.owner_id
           WHERE r.owner_id=$1 AND r.root_id=$5 ORDER BY (r.account_id=$2 AND r.sender_id=$3 AND r.dm=$4) DESC LIMIT 1`,
          [options.ownerId, identity.account_id, identity.sender_id, dm, root],
        )
      ).rows[0];
      return { known: Boolean(row), target: row?.allowed ? row.target_id : undefined };
    },
    record(
      identity: { account_id: string; sender_id: string; thread_id: string },
      context: {
        messageId: string;
        sessionId: string;
        turnId: string;
        rootMessageId: string;
        channel: string;
      },
    ): NonNullable<Parameters<MessagingTransport['post']>[0]['stream']>['record'] {
      used = true;
      const signal = lifetime.signal;
      const generation = revision;
      return async (update) => {
        signal.throwIfAborted();
        if (
          closed ||
          revision !== generation ||
          !options.permitted({ provider: 'slack', ...identity })
        )
          throw new Error('The stream connection has changed.');
        if (update.state === 'starting') {
          const inserted = await pool.query(
            `INSERT INTO channel_streams(owner_id,message_id,part,account_id,sender_id,dm,session_id,turn_id,root_id,revision,state)
            SELECT $1,m.id,$3,$4,$5,$6,u.session_id,u.id,$9,$10,'starting' FROM messages m JOIN turns u ON u.id=m.turn_id AND u.owner_id=m.owner_id
            JOIN threads th ON th.id=m.thread_id AND th.owner_id=m.owner_id AND th.archived_at IS NULL
            WHERE m.owner_id=$1 AND m.id=$2 AND u.session_id=$7 AND u.id=$8 AND u.channel=$11 AND m.hidden_at IS NULL AND u.status NOT IN ('cancelled','failed')
              AND NOT EXISTS(SELECT 1 FROM approvals a WHERE a.owner_id=m.owner_id AND a.confirmation_message_id=m.id)
            ON CONFLICT DO NOTHING RETURNING part`,
            [
              options.ownerId,
              context.messageId,
              update.part,
              identity.account_id,
              identity.sender_id,
              identity.thread_id,
              context.sessionId,
              context.turnId,
              context.rootMessageId,
              generation,
              context.channel,
            ],
          );
          if (!inserted.rowCount)
            throw new Error('The stream message is unavailable or already claimed.');
        } else {
          const expected =
            update.state === 'dispatching'
              ? ['starting']
              : update.state === 'open'
                ? ['dispatching', 'appending']
                : update.state === 'appending' || update.state === 'finalizing'
                  ? ['open']
                  : update.state === 'finalized'
                    ? ['finalizing']
                    : ['starting', 'dispatching', 'open', 'appending', 'finalizing'];
          const changed = await pool.query(
            `UPDATE channel_streams SET state=$4,provider_id=COALESCE($5,provider_id),confirmed_chars=COALESCE($6,confirmed_chars)
            WHERE owner_id=$1 AND message_id=$2 AND part=$3 AND revision=$7 AND state=ANY($8::text[])
              AND ($9 OR EXISTS(SELECT 1 FROM turns u JOIN threads th ON th.id=u.thread_id AND th.owner_id=u.owner_id AND th.archived_at IS NULL
                WHERE u.id=channel_streams.turn_id AND u.owner_id=channel_streams.owner_id AND u.status NOT IN ('cancelled','failed')))
            RETURNING part`,
            [
              options.ownerId,
              context.messageId,
              update.part,
              update.state,
              update.id ?? null,
              update.confirmedChars ?? null,
              generation,
              expected,
              !['dispatching', 'appending'].includes(update.state),
            ],
          );
          if (!changed.rowCount && update.state !== 'uncertain')
            throw new Error('The stream is no longer active.');
        }
        signal.throwIfAborted();
        if (
          update.state === 'finalized' ||
          update.state === 'uncertain' ||
          (update.state === 'open' && (update.confirmedChars ?? 0) <= 3000)
        ) {
          await pool.query(
            'UPDATE channel_roots SET last_status=NULL,lifecycle_revision=lifecycle_revision+1 WHERE owner_id=$1 AND session_id=$2 AND dm=$3',
            [options.ownerId, context.sessionId, identity.thread_id],
          );
        }
      };
    },
    async pause() {
      lifetime.abort();
      release?.();
      await running;
      shown.clear();
      if (!used) return;
      await pool.query('UPDATE channel_roots SET nonce=NULL,status_nonce=NULL WHERE owner_id=$1', [
        options.ownerId,
      ]);
      await pool.query(
        "UPDATE channel_streams SET state='uncertain' WHERE owner_id=$1 AND revision=$2 AND state<>'finalized'",
        [options.ownerId, revision],
      );
    },
    resume() {
      lifetime = new AbortController();
      revision = randomUUID();
      listen();
      refresh();
    },
    async close() {
      closed = true;
      clearInterval(timer);
      lifetime.abort();
      release?.();
      await running;
      if (!used) return;
      await pool.query('UPDATE channel_roots SET nonce=NULL,status_nonce=NULL WHERE owner_id=$1', [
        options.ownerId,
      ]);
      await pool.query(
        "UPDATE channel_streams SET state='uncertain' WHERE owner_id=$1 AND revision=$2 AND state<>'finalized'",
        [options.ownerId, revision],
      );
    },
  };
}
