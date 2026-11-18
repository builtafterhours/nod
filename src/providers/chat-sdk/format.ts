import { parseMarkdown, type Root } from 'chat';
import { MessagingConnectionError } from '../../ports/messaging.ts';

type Node = Root | Root['children'][number];
type Parent = Extract<Node, { children: unknown }>;

interface Syntax {
  text(value: string): string;
  code(value: string): string;
  block(value: string, language?: string | null): string;
  link(label: string, url: string): string;
  bold: string;
  italic: string;
  strike: string;
  bullet: string;
  number(value: number): string;
  quote: string;
  literal: RegExp;
}

const telegram: Syntax = {
  text: (value) => value.replace(/[_*[\]()~`>#+\-=|{}.!\\]/g, '\\$&'),
  code: (value) => `\`${value.replace(/[`\\]/g, '\\$&')}\``,
  block: (value, language) =>
    `\`\`\`${language && /^[\w#+.-]+$/.test(language) ? language : ''}\n${value.replace(/[`\\]/g, '\\$&')}\n\`\`\``,
  link: (label, url) => `[${label}](${url.replace(/[)\\]/g, '\\$&')})`,
  bold: '*',
  italic: '_',
  strike: '~',
  bullet: '•',
  number: (value) => `${value}\\.`,
  quote: '>',
  literal: /(?<![\w@:/.~-])[\w~./-]*[\w-]\.(?:md|py|sh|java|zip|mov)(?![\w-]|\.\w)/gi,
};

const whatsapp: Syntax = {
  text: (value) => value,
  code: (value) => `\`${value}\``,
  block: (value) => `\`\`\`\n${value}\n\`\`\``,
  link: (label, url) => `${label} (${url})`,
  bold: '*',
  italic: '_',
  strike: '~',
  bullet: '-',
  number: (value) => `${value}.`,
  quote: '> ',
  literal: /(?<![\p{L}\p{N}])([*_~])(?=\S)[^\n`]*?(?<=\S)\1(?![\p{L}\p{N}])/gu,
};

function plain(node: Node): string {
  if ('value' in node) return String(node.value);
  return 'children' in node ? node.children.map((child) => plain(child as Node)).join('') : '';
}

function prose(value: string, syntax: Syntax) {
  let result = '';
  let last = 0;
  for (const match of value.matchAll(syntax.literal)) {
    result += syntax.text(value.slice(last, match.index)) + syntax.code(match[0]);
    last = match.index + match[0].length;
  }
  return result + syntax.text(value.slice(last));
}

function render(root: Root, syntax: Syntax, source?: string) {
  const inline = (nodes: Node[], open: ReadonlySet<string>) =>
    nodes.map((node) => phrase(node, open)).join('');

  const mark = (marker: string, node: Parent, open: ReadonlySet<string>) => {
    const from = node.position?.start.offset;
    const to = node.position?.end.offset;
    if (
      source !== undefined &&
      from !== undefined &&
      to !== undefined &&
      /[\p{L}\p{N}]/u.test(`${source[from - 1] ?? ''}${source[to] ?? ''}`)
    )
      return syntax.text(source.slice(from, to));
    const content = inline(node.children as Node[], new Set([...open, marker]));
    const core = content.trim();
    if (open.has(marker) || !core) return content;
    const start = content.indexOf(core);
    return `${content.slice(0, start)}${marker}${core}${marker}${content.slice(start + core.length)}`;
  };

  const phrase = (node: Node, open: ReadonlySet<string>): string => {
    switch (node.type) {
      case 'text':
        return open.has('link') ? syntax.text(node.value) : prose(node.value, syntax);
      case 'html':
        return syntax.text(node.value);
      case 'strong':
        return mark(syntax.bold, node, open);
      case 'emphasis':
        return mark(syntax.italic, node, open);
      case 'delete':
        return mark(syntax.strike, node, open);
      case 'inlineCode':
        return syntax.code(node.value);
      case 'break':
        return '\n';
      case 'link':
      case 'image': {
        const label = node.type === 'image' ? node.alt || 'Image' : plain(node);
        const rendered =
          node.type === 'image'
            ? syntax.text(label)
            : inline(node.children as Node[], new Set([...open, 'link']));
        if (
          !/^(?:https?|mailto|tel):/i.test(node.url) ||
          [label, `mailto:${label}`].includes(node.url)
        )
          return rendered;
        return syntax.link(rendered, node.url);
      }
      case 'footnoteReference':
        return syntax.text(`[${node.label ?? node.identifier}]`);
      default:
        return 'children' in node ? inline(node.children as Node[], open) : '';
    }
  };

  const none = new Set<string>();
  const blocks = (nodes: Node[], separator = '\n\n') =>
    nodes.map(block).filter(Boolean).join(separator);

  const block = (node: Node): string => {
    switch (node.type) {
      case 'paragraph':
        return inline(node.children, none);
      case 'heading':
        return mark(syntax.bold, node, none);
      case 'thematicBreak':
        return syntax.text('———');
      case 'code':
        return syntax.block(node.value, node.lang);
      case 'blockquote':
        return blocks(node.children)
          .split('\n')
          .map((line) =>
            `${syntax.quote}${line.startsWith(syntax.quote) ? line.slice(syntax.quote.length) : line}`.trimEnd(),
          )
          .join('\n');
      case 'list':
        return node.children
          .map((item, index) => {
            const box = item.checked === true ? '☑' : item.checked === false ? '☐' : '';
            const marker = node.ordered
              ? `${syntax.number((node.start ?? 1) + index)}${box && ` ${box}`}`
              : box || syntax.bullet;
            const content = blocks(item.children, item.spread ? '\n\n' : '\n')
              .split('\n')
              .map((line, position) => (position && line ? `  ${line}` : line))
              .join('\n');
            return `${marker} ${content}`;
          })
          .join('\n');
      case 'table': {
        const [head, ...rows] = node.children.map((row) => row.children);
        if (!head) return '';
        const labels = head.map((cell) => inline(cell.children, none).trim());
        if (!rows.length || labels.length < 2)
          return (rows.length ? rows.map((row) => row[0]) : head)
            .map((cell) => cell && inline(cell.children, none).trim())
            .filter(Boolean)
            .map((cell) => `${syntax.bullet} ${cell}`)
            .join('\n');
        return rows
          .map((row) =>
            [
              row[0] && mark(syntax.bold, row[0], none).trim(),
              ...row.slice(1).map((cell, index) => {
                const value = inline(cell.children, none).trim();
                const label = labels[index + 1];
                return value && `${syntax.bullet} ${label ? `${label}: ` : ''}${value}`;
              }),
            ]
              .filter(Boolean)
              .join('\n'),
          )
          .join('\n\n');
      }
      case 'definition':
        return '';
      case 'footnoteDefinition':
        return `${syntax.text(`[${node.label ?? node.identifier}]`)} ${blocks(node.children, '\n')}`;
      default:
        return 'children' in node ? blocks(node.children as Node[]) : phrase(node, none);
    }
  };

  return blocks(root.children).trim();
}

export function telegramText(markdown: string | Root) {
  return typeof markdown === 'string'
    ? render(parseMarkdown(markdown), telegram, markdown)
    : render(markdown, telegram);
}

export function whatsappText(markdown: string) {
  return render(parseMarkdown(markdown), whatsapp, markdown);
}

export function slackText(markdown: string | Root) {
  const escape = (value: string) =>
    value.replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;');
  return render(
    typeof markdown === 'string' ? parseMarkdown(markdown) : markdown,
    {
      ...whatsapp,
      text: escape,
      code: (value) => `\`${escape(value).replaceAll('`', "'")}\``,
      block: (value) => `\`\`\`\n${escape(value).replaceAll('```', "'''")}\n\`\`\``,
      link: (label, url) => `<${escape(url).replaceAll('|', '%7C')}|${label.replaceAll('|', ' ')}>`,
    },
    typeof markdown === 'string' ? markdown : undefined,
  );
}

export function slackMessages(markdown: string) {
  const limit = 3000;
  const text = slackText(markdown);
  if (text.length <= limit) return text ? [text] : [];
  const root = parseMarkdown(markdown);
  const split = (node: Node, wrap: (part: Node) => Root): Node[] => {
    const length = (part: Node) => slackText(wrap(part)).length;
    if (length(node) <= limit) return [node];
    if (
      node.type === 'image' ||
      (node.type === 'link' && length({ ...node, children: [] }) > limit - 100)
    ) {
      const label = node.type === 'image' ? node.alt || 'Image' : plain(node);
      return split(
        {
          type: 'text',
          value: [label, `mailto:${label}`].includes(node.url) ? label : `${label} (${node.url})`,
        } as Node,
        wrap,
      );
    }
    if ('children' in node) {
      const parts: Node[] = [];
      let children: Node[] = [];
      const withChildren = (values: Node[]) => ({ ...node, children: values }) as Node;
      for (const child of node.children as Node[]) {
        for (const part of split(child, (value) => wrap(withChildren([value])))) {
          if (children.length && length(withChildren([...children, part])) > limit) {
            parts.push(withChildren(children));
            children = [];
          }
          children.push(part);
        }
      }
      if (children.length) parts.push(withChildren(children));
      return parts;
    }
    if ('value' in node && typeof node.value === 'string') {
      const points = Array.from(node.value);
      const parts: Node[] = [];
      let offset = 0;
      while (offset < points.length) {
        const part = (count: number) =>
          ({
            ...node,
            value: points.slice(offset, offset + count).join(''),
          }) as Node;
        let low = 0;
        let high = Math.min(points.length - offset, limit);
        while (low < high) {
          const mid = Math.ceil((low + high) / 2);
          if (length(part(mid)) <= limit) low = mid;
          else high = mid - 1;
        }
        if (!low) throw new MessagingConnectionError('Slack failed: text_format_limit.');
        if (offset + low < points.length) {
          const candidate = points.slice(offset, offset + low);
          let boundary = candidate.lastIndexOf('\n');
          if (boundary < low / 2) boundary = candidate.lastIndexOf(' ');
          if (boundary >= low / 2) low = boundary + 1;
        }
        parts.push(part(low));
        offset += low;
      }
      return parts;
    }
    throw new MessagingConnectionError('Slack failed: text_format_limit.');
  };
  return split(root, (part) => part as Root)
    .map((part) => slackText(part as Root))
    .filter(Boolean);
}
