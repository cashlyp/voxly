const escapeMarkdown = (input = '') => {
    if (typeof input !== 'string') return input;
    // Telegram Markdown (legacy) only needs escaping for a small subset of characters.
    return input.replace(/([_*[\]`])/g, '\\$1');
};

const buildLine = (emoji, label, value) => `${emoji} ${label}: ${value}`;

const section = (title, entries = []) => {
    const body = entries.filter(Boolean).join('\n');
    return `*${title}*\n${body}`;
};

const emphasize = (text = '') => {
    if (!text) return '';
    return `✨ ${text}`;
};

const tipLine = (emoji, text = '') => `${emoji} ${text}`;

module.exports = {
    escapeMarkdown,
    buildLine,
    section,
    emphasize,
    tipLine
};
