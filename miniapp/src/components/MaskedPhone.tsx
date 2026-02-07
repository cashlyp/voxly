import { useCallback, useEffect, useMemo, useState } from "react";

import { confirmAction } from "../lib/ux";

const DEFAULT_REVEAL_MS = 15000;

function isLikelyPhone(value: string) {
  if (!value) return false;
  if (/[a-z]/i.test(value)) return false;
  const digits = value.replace(/\D/g, "");
  return digits.length >= 7;
}

function maskPhone(value: string) {
  const digits = value.replace(/\D/g, "");
  if (digits.length < 4) return "***";
  const last4 = digits.slice(-4);
  const prefix = value.trim().startsWith("+") ? "+" : "";
  return `${prefix}*** *** ${last4}`;
}

type MaskedPhoneProps = {
  value?: string | null;
  className?: string;
  revealForMs?: number;
};

export function MaskedPhone({
  value,
  className,
  revealForMs = DEFAULT_REVEAL_MS,
}: MaskedPhoneProps) {
  const [revealed, setRevealed] = useState(false);
  const text = useMemo(() => {
    if (value === null || value === undefined || value === "") return "-";
    if (!isLikelyPhone(value)) return value;
    return revealed ? value : maskPhone(value);
  }, [value, revealed]);

  useEffect(() => {
    if (!revealed) return undefined;
    const timer = window.setTimeout(() => setRevealed(false), revealForMs);
    return () => window.clearTimeout(timer);
  }, [revealed, revealForMs]);

  const handleToggle = useCallback(async () => {
    if (value === null || value === undefined || value === "") return;
    if (!isLikelyPhone(value)) return;
    if (revealed) {
      setRevealed(false);
      return;
    }
    const confirmed = await confirmAction({
      title: "Reveal phone number?",
      message: "This may contain sensitive information.",
      confirmText: "Reveal",
      destructive: false,
    });
    if (confirmed) {
      setRevealed(true);
    }
  }, [revealed, value]);

  if (value === null || value === undefined || value === "" || !isLikelyPhone(value)) {
    return <span className={className}>{text}</span>;
  }

  return (
    <button
      type="button"
      className={["masked-value", revealed ? "revealed" : "", className]
        .filter(Boolean)
        .join(" ")}
      onClick={() => void handleToggle()}
      aria-label={revealed ? "Hide phone number" : "Reveal phone number"}
      title={revealed ? "Tap to hide" : "Tap to reveal"}
    >
      {text}
    </button>
  );
}
