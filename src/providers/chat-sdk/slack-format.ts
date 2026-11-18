import { parseMarkdown, type Root } from 'chat';
import type { KnownBlock, RichTextText, RichTextLink } from '@slack/types';

type Node = Root | Root['children'][number];
type Style = { bold?: boolean; italic?: boolean; strike?: boolean; code?: boolean };
type Span = { text: string; style?: Style; url?: string; from?: number; end: number };
type Frame = {
  type: 'section' | 'quote' | 'code' | 'list';
  indent?: number;
  number?: number;
  language?: string;
};
export type SlackUnit = { block: KnownBlock; text: string; from: number; end: number };
export type SlackPart = { blocks?: KnownBlock[]; text: string; from: number; end: number };
export type SlackPlan = { units: SlackUnit[]; parts: SlackPart[]; plain: SlackPart[] };

const escape = (value: string) =>
  value.replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;');
const bytes = (value: unknown) => Buffer.byteLength(JSON.stringify(value));
const children = (node: Node) => ('children' in node ? (node.children as Node[]) : []);
const start = (node: Node) => node.position!.start.offset!;
const end = (node: Node) => node.position!.end.offset!;

export function validateSlackSource(source: string) {
  if (source.includes('\0')) throw new Error('Slack rendering failed: NUL is not representable.');
  for (const point of source)
    if (point.length === 1 && /[\uD800-\uDFFF]/.test(point))
      throw new Error('Slack rendering failed: ill-formed UTF16.');
}

function cut(value: string, limit: number) {
  let offset = 0;
  let size = 0;
  let word = 0;
  let line = 0;
  for (const point of value) {
    const length = escape(point).length;
    if (size + length > limit) break;
    offset += point.length;
    size += length;
    if (/\s/u.test(point)) word = offset;
    if (point === '\n') line = offset;
  }
  return offset === value.length ? offset : line || word || offset;
}

function literal(source: string): SlackPart[] {
  const parts: SlackPart[] = [];
  let from = 0;
  while (from < source.length) {
    const to = from + cut(source.slice(from, from + 3002), 3000);
    parts.push({ text: escape(source.slice(from, to)), from, end: to });
    from = to;
  }
  return parts;
}

function witnessed(root: Root, source: string) {
  const parents = new Set([
    'root',
    'paragraph',
    'heading',
    'strong',
    'emphasis',
    'delete',
    'blockquote',
    'list',
    'listItem',
    'table',
    'tableRow',
    'tableCell',
  ]);
  let count = 0;
  const walk = (node: Node, from: number, to: number, depth: number): boolean => {
    if (++count > 20_000 || depth > 64) return false;
    const a = node.position?.start.offset;
    const b = node.position?.end.offset;
    if (
      a === undefined ||
      b === undefined ||
      !Number.isInteger(a) ||
      !Number.isInteger(b) ||
      a < from ||
      b > to ||
      a > b
    )
      return false;
    const raw = source.slice(a, b);
    if (node.type === 'text') return node.value === raw;
    if (node.type === 'inlineCode') {
      const match = /^(`+)([^\n]*?)\1$/.exec(raw);
      return Boolean(match && match[2] === node.value);
    }
    if (node.type === 'code') {
      const match = /^(`{3,}|~{3,})([\w#+.-]*)\n([\s\S]*?)\n\1$/.exec(raw);
      return Boolean(
        match &&
        node.value &&
        match[3] === node.value &&
        (node.lang ?? '').length <= 80 &&
        (node.lang ?? '') === match[2] &&
        !node.meta,
      );
    }
    if (node.type === 'link') {
      if (
        !/^(https?|mailto|tel):/i.test(node.url) ||
        node.title ||
        /\s/.test(node.url) ||
        node.children.some((child) => child.type !== 'text') ||
        !raw.startsWith('[') ||
        !raw.endsWith(`](${node.url})`)
      )
        return false;
    } else if (node.type === 'list' && node.ordered) {
      if ((node.start ?? 1) < 1) return false;
      if (
        !node.children.every(
          (item, index) =>
            /^(\d+)[.)] /.exec(source.slice(start(item), end(item)))?.[1] ===
            String((node.start ?? 1) + index),
        )
      )
        return false;
    } else if (node.type === 'break') return /^ {2,}\n$/.test(raw);
    else if (node.type === 'thematicBreak') return /^\s*([-*_])(?:\s*\1){2,}\s*$/.test(raw);
    else if (!parents.has(node.type)) return false;
    if (node.type === 'heading' && !node.children.length) return false;
    if (
      node.type === 'blockquote' &&
      (node.children.length !== 1 || node.children[0].type !== 'paragraph')
    )
      return false;
    if (
      node.type === 'listItem' &&
      (node.children[0]?.type !== 'paragraph' ||
        node.children.slice(1).some((child) => child.type !== 'list'))
    )
      return false;
    let previous = a;
    for (const child of children(node)) {
      if ((child.position?.start.offset ?? -1) < previous || !walk(child, a, b, depth + 1))
        return false;
      previous = end(child);
    }
    return true;
  };
  return (
    root.position?.start.offset === 0 &&
    root.position.end.offset === source.length &&
    walk(root, 0, source.length, 0)
  );
}

function pack(units: SlackUnit[], native: boolean) {
  const parts: SlackPart[] = [];
  let current: SlackPart | undefined;
  for (const unit of units) {
    const next: SlackPart = {
      from: current?.from ?? unit.from,
      end: unit.end,
      text: (current?.text ?? '') + unit.text,
      ...(native ? { blocks: [...(current?.blocks ?? []), unit.block] } : {}),
    };
    if (
      current &&
      (next.text.length > 3000 || (next.blocks?.length ?? 0) > 40 || bytes(next) > 30_000)
    ) {
      parts.push(current);
      current = undefined;
    }
    current = current
      ? next
      : {
          from: unit.from,
          end: unit.end,
          text: unit.text,
          ...(native ? { blocks: [unit.block] } : {}),
        };
  }
  if (current) parts.push(current);
  return parts;
}

export function slackPlan(source: string, metadata = false): SlackPlan {
  validateSlackSource(source);
  const fallback = (): SlackPlan => {
    const parts = literal(source);
    return { units: [], parts, plain: parts };
  };
  if (!source || metadata || source.length > 100_000 || /[&\\<%\r\t\uFEFF]/u.test(source))
    return fallback();
  try {
    const root = parseMarkdown(source);
    if (!witnessed(root, source)) return fallback();
    const units: SlackUnit[] = [];
    let covered = 0;
    const inline = (node: Node, style: Style = {}): Span[] => {
      if (node.type === 'text')
        return [{ text: node.value, style, from: start(node), end: end(node) }];
      if (node.type === 'inlineCode') {
        const raw = source.slice(start(node), end(node));
        return [
          {
            text: node.value,
            style: { ...style, code: true },
            from: start(node) + /^`+/.exec(raw)![0].length,
            end: end(node),
          },
        ];
      }
      if (node.type === 'break') return [{ text: '\n', end: end(node) }];
      if (['strong', 'emphasis', 'delete'].includes(node.type)) {
        if (/[\p{L}\p{N}]/u.test(`${source[start(node) - 1] ?? ''}${source[end(node)] ?? ''}`))
          return [
            {
              text: source.slice(start(node), end(node)),
              style,
              from: start(node),
              end: end(node),
            },
          ];
        style = {
          ...style,
          [node.type === 'strong' ? 'bold' : node.type === 'emphasis' ? 'italic' : 'strike']: true,
        };
      }
      if (node.type === 'link')
        return [
          {
            text: children(node)
              .flatMap((n) => inline(n))
              .map((s) => s.text)
              .join(''),
            url: node.url,
            style,
            end: end(node),
          },
        ];
      return children(node).flatMap((n) => inline(n, style));
    };
    const emit = (
      spans: Span[],
      frame: Frame,
      finish: number,
      separator: string,
      atomic = false,
    ) => {
      let pending: Span[] = [];
      let length = 0;
      let plain = separator;
      let first = true;
      const flush = () => {
        if (!pending.length) return;
        const elements = pending.map((span): RichTextText | RichTextLink =>
          span.url
            ? {
                type: 'link',
                url: span.url,
                text: span.text,
                ...(span.style ? { style: span.style } : {}),
              }
            : { type: 'text', text: span.text, ...(span.style ? { style: span.style } : {}) },
        );
        const section = { type: 'rich_text_section' as const, elements };
        const element =
          frame.type === 'list'
            ? {
                type: 'rich_text_list' as const,
                style: frame.number === undefined ? ('bullet' as const) : ('ordered' as const),
                indent: frame.indent ?? 0,
                ...(frame.number === undefined ? {} : { offset: frame.number - 1 }),
                elements: [section],
              }
            : frame.type === 'quote'
              ? { type: 'rich_text_quote' as const, elements }
              : frame.type === 'code'
                ? {
                    type: 'rich_text_preformatted' as const,
                    elements,
                    ...(frame.language ? { language: frame.language } : {}),
                  }
                : section;
        const block: KnownBlock = { type: 'rich_text', elements: [element] };
        const next = atomic ? covered : Math.max(covered, ...pending.map((s) => s.end));
        const unit = { block, text: escape(plain), from: covered, end: next };
        if (unit.text.length > 3000 || bytes(unit) > 18_000) throw new Error('native_budget');
        units.push(unit);
        covered = next;
        pending = [];
        length = 0;
        plain = '';
        first = false;
      };
      if (frame.type === 'list')
        plain += `${'  '.repeat(frame.indent ?? 0)}${frame.number === undefined ? '-' : `${frame.number}.`} `;
      if (frame.type === 'code' && frame.language) plain += `[${frame.language}]\n`;
      for (const span of spans) {
        if (!span.text) continue;
        if (span.url) {
          const projection = `${span.text} (${span.url})`;
          if (span.text.length > 2800 || escape(projection).length > 2800 || bytes(span) > 12_000)
            throw new Error('native_budget');
          if (
            length + span.text.length > 2800 ||
            pending.length >= 200 ||
            escape(plain + projection).length > 2900
          )
            flush();
          pending.push(span);
          plain += projection;
          length += span.text.length;
          continue;
        }
        let offset = 0;
        while (offset < span.text.length) {
          if (length >= 2800 || pending.length >= 200 || escape(plain).length >= 2900) flush();
          const size = cut(
            span.text.slice(offset, offset + 2802),
            Math.min(2800 - length, 2900 - escape(plain).length),
          );
          if (!size) {
            flush();
            continue;
          }
          const value = span.text.slice(offset, offset + size);
          const complete = offset + size === span.text.length;
          pending.push({
            ...span,
            text: value,
            end: complete
              ? span.end
              : span.from === undefined
                ? covered
                : span.from + offset + size,
          });
          length += value.length;
          plain += value;
          offset += size;
        }
      }
      flush();
      if (first) throw new Error('empty_native');
      units[units.length - 1].end = Math.max(covered, finish);
      covered = units[units.length - 1].end;
    };
    const block = (node: Node, indent = 0, separator = '\n\n') => {
      if (node.type === 'heading') {
        const spans = inline(node);
        const value = spans.map((s) => s.text).join('');
        if (
          value &&
          value.length <= 150 &&
          spans.every((s) => !s.url && !Object.values(s.style ?? {}).some(Boolean))
        ) {
          units.push({
            block: { type: 'header', text: { type: 'plain_text', text: value, emoji: false } },
            text: escape((units.length ? separator : '') + value),
            from: covered,
            end: end(node),
          });
          covered = end(node);
        } else
          emit(
            inline(node, { bold: true }),
            { type: 'section' },
            end(node),
            units.length ? separator : '',
          );
      } else if (node.type === 'paragraph')
        emit(inline(node), { type: 'section' }, end(node), units.length ? separator : '');
      else if (node.type === 'code') {
        const bodyFrom = source.indexOf('\n', start(node)) + 1;
        emit(
          [{ text: node.value, from: bodyFrom, end: bodyFrom + node.value.length }],
          { type: 'code', ...(node.lang ? { language: node.lang } : {}) },
          end(node),
          units.length ? separator : '',
        );
      } else if (node.type === 'blockquote')
        emit(inline(node), { type: 'quote' }, end(node), units.length ? separator : '');
      else if (node.type === 'list') {
        if (indent > 8) throw new Error('native_indent');
        for (const [index, item] of node.children.entries()) {
          const number = node.ordered ? (node.start ?? 1) + index : undefined;
          for (const child of item.children) {
            if (child.type === 'paragraph') {
              const spans = inline(child);
              if (item.checked !== null && item.checked !== undefined)
                spans.unshift({ text: item.checked ? '[x] ' : '[ ] ', end: start(child) });
              emit(spans, { type: 'list', indent, number }, end(child), units.length ? '\n' : '');
            } else block(child, indent + 1, '\n');
          }
          units[units.length - 1].end = end(item);
          covered = end(item);
        }
      } else if (node.type === 'table') {
        const [head, ...rows] = node.children;
        const labels = head.children.map((cell) =>
          inline(cell)
            .map((s) => s.text)
            .join(''),
        );
        if (!rows.length) {
          emit(
            head.children.flatMap((cell, index) => [
              ...(index ? [{ text: '\n', end: start(cell) }] : []),
              ...inline(cell),
            ]),
            { type: 'section' },
            end(head),
            units.length ? '\n\n' : '',
          );
        }
        for (const row of rows) {
          const spans: Span[] = [];
          for (let index = 0; index < Math.max(labels.length, row.children.length); index++) {
            spans.push({
              text: `${index ? '\n' : ''}${labels[index] ?? `Column ${index + 1}`}: `,
              end: start(row),
            });
            if (row.children[index])
              spans.push(...inline(row.children[index], index ? {} : { bold: true }));
          }
          emit(spans, { type: 'section' }, end(row), units.length ? '\n\n' : '', true);
        }
      } else if (node.type === 'thematicBreak')
        emit(
          [{ text: '---', end: end(node) }],
          { type: 'section' },
          end(node),
          units.length ? separator : '',
        );
      else throw new Error('native_shape');
    };
    for (const node of root.children) block(node);
    if (!units.length) return fallback();
    units[units.length - 1].end = source.length;
    const parts = pack(units, true);
    const plain = pack(units, false);
    return { units, parts, plain };
  } catch {
    return fallback();
  }
}

export function slackStreamParts(plan: SlackPlan) {
  if (!plan.units.length) return;
  const parts: Array<{
    from: number;
    end: number;
    calls: Array<{ blocks: KnownBlock[]; end: number }>;
  }> = [];
  let part: (typeof parts)[number] | undefined;
  let projected = 0;
  let count = 0;
  for (let index = 0; index < plan.units.length;) {
    const group: SlackUnit[] = [];
    let size = 0;
    do {
      const unit = plan.units[index++];
      group.push(unit);
      size += unit.text.length;
    } while (
      index < plan.units.length &&
      group[group.length - 1].end === group[group.length - 1].from
    );
    if (group.length > 40 || size > 9000) return;
    if (!part || count + group.length > 40 || projected + size > 9000) {
      part = { from: group[0].from, end: group[group.length - 1].end, calls: [] };
      parts.push(part);
      count = 0;
      projected = 0;
    }
    for (const unit of group) part.calls.push({ blocks: [unit.block], end: unit.end });
    part.end = group[group.length - 1].end;
    projected += size;
    count += group.length;
  }
  return parts;
}
