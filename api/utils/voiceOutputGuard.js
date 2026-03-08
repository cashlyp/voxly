const EMOJI_REGEX = /[\p{Extended_Pictographic}\uFE0F\u200D]/gu;
const URL_REGEX = /\bhttps?:\/\/[^\s]+/gi;
const MARKDOWN_LINK_REGEX = /\[([^\]]+)\]\(([^)]+)\)/g;

function normalizeWhitespace(value = "") {
  return String(value || "")
    .replace(/\r/g, "")
    .replace(/[ \t]+/g, " ")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

function truncateAtSentenceBoundary(text, maxChars) {
  if (!Number.isFinite(maxChars) || maxChars <= 0) return text;
  if (text.length <= maxChars) return text;
  const slice = text.slice(0, maxChars);
  const lastBreak = Math.max(
    slice.lastIndexOf(". "),
    slice.lastIndexOf("? "),
    slice.lastIndexOf("! "),
    slice.lastIndexOf(", "),
  );
  if (lastBreak >= Math.floor(maxChars * 0.55)) {
    return slice.slice(0, lastBreak + 1).trim();
  }
  return `${slice.trimEnd()}.`;
}

function applyNaturalSpeechPacing(value = "") {
  let text = String(value || "");
  if (!text) return text;

  text = text
    .replace(/\s*[|]+\s*/g, ", ")
    .replace(/\s*;\s*/g, ", ")
    .replace(/\s*:\s*/g, ", ")
    .replace(/\s*\/\s*/g, " ")
    .replace(/\.{4,}/g, "...")
    .replace(/,{2,}/g, ",");

  // Slow down spoken long numbers for telephony clarity.
  text = text.replace(/\b(\d{3})(\d{3})(\d{4})\b/g, "$1. $2. $3");

  // If a long response has no punctuation, add one natural break.
  if (!/[.!?]/.test(text) && text.length > 120) {
    const mid = Math.floor(text.length / 2);
    const leftWindow = Math.max(0, mid - 28);
    const rightWindow = Math.min(text.length - 1, mid + 28);
    const windowText = text.slice(leftWindow, rightWindow + 1);
    const commaPos = windowText.lastIndexOf(",");
    if (commaPos >= 0) {
      const absolute = leftWindow + commaPos;
      text = `${text.slice(0, absolute + 1)} ${text.slice(absolute + 1)}`;
    } else {
      const breakPos = windowText.search(/\s(and|but|so|then)\s/i);
      if (breakPos >= 0) {
        const absolute = leftWindow + breakPos;
        text = `${text.slice(0, absolute).trimEnd()}. ${text.slice(absolute).trimStart()}`;
      }
    }
  }
  return text;
}

function sanitizeVoiceOutputText(rawText = "", options = {}) {
  const fallbackText = String(options.fallbackText || "Let me help you with that.")
    .trim();
  const maxChars = Number(options.maxChars || 260);
  const reasons = [];
  let text = String(rawText || "");
  if (!text.trim()) {
    return {
      text: fallbackText,
      changed: true,
      reasons: ["empty"],
    };
  }

  const original = text;
  const pushReason = (reason) => {
    if (!reasons.includes(reason)) reasons.push(reason);
  };

  if (MARKDOWN_LINK_REGEX.test(text)) {
    text = text.replace(MARKDOWN_LINK_REGEX, "$1");
    pushReason("markdown_link");
  }
  MARKDOWN_LINK_REGEX.lastIndex = 0;

  if (/```/.test(text)) {
    text = text.replace(/```/g, "");
    pushReason("markdown_fence");
  }
  if (/[`*_~#>]/.test(text)) {
    text = text.replace(/[`*_~#>]/g, "");
    pushReason("markdown_tokens");
  }
  if (URL_REGEX.test(text)) {
    text = text.replace(URL_REGEX, "this call");
    pushReason("url");
  }
  URL_REGEX.lastIndex = 0;

  if (EMOJI_REGEX.test(text)) {
    text = text.replace(EMOJI_REGEX, "");
    pushReason("emoji");
  }
  EMOJI_REGEX.lastIndex = 0;

  const channelRules = [
    [/\b(text|dm|message|chat|inbox|ping)\s+me\b/gi, "talk with me on this call"],
    [/\b(send|drop)\s+(me\s+)?(a\s+)?(dm|text|message)\b/gi, "tell me now on this call"],
    [/\b(on|via)\s+(whatsapp|instagram|ig|imessage|textnow|sms|telegram|x|tiktok)\b/gi, "on this call"],
    [/\bmove this to (chat|text|dm|messages?)\b/gi, "continue on this call"],
    [/\b(in|via)\s+(dm|text|chat)\b/gi, "on this call"],
    [/\b(dms?|texts?|chats?)\b/gi, "this call"],
  ];
  for (const [pattern, replacement] of channelRules) {
    if (pattern.test(text)) {
      text = text.replace(pattern, replacement);
      pushReason("chat_channel_reference");
    }
  }

  if (/•/.test(text)) {
    text = text.replace(/•+/g, ", ");
    pushReason("bullet_symbol");
  }

  text = text
    .replace(/\(\s*\)/g, " ")
    .replace(/\s+([,.!?;:])/g, "$1")
    .replace(/([,.!?;:]){2,}/g, "$1")
    .replace(/\s{2,}/g, " ");

  text = applyNaturalSpeechPacing(text);

  text = normalizeWhitespace(text);
  text = truncateAtSentenceBoundary(text, maxChars);
  text = normalizeWhitespace(text);
  if (!text) {
    text = fallbackText;
    pushReason("fallback");
  }
  if (!/[.!?]$/.test(text)) {
    text = `${text}.`;
  }

  return {
    text,
    changed: text !== String(original || ""),
    reasons,
  };
}

module.exports = {
  sanitizeVoiceOutputText,
};
