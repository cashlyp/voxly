function getDb(ctx = {}) {
  return typeof ctx.getDb === "function" ? ctx.getDb() : ctx.db;
}

function getDigitService(ctx = {}) {
  return typeof ctx.getDigitService === "function" ? ctx.getDigitService() : ctx.digitService;
}

async function dedupeProviderEvent(asyncFn, syncFn, source, payload, options = {}) {
  const fn = typeof asyncFn === "function"
    ? asyncFn
    : typeof syncFn === "function"
      ? syncFn
      : null;
  if (!fn) return true;
  try {
    const result = await fn(source, payload, options);
    return result !== false;
  } catch (error) {
    console.error("provider_event_dedupe_error", {
      source: source || null,
      error: error?.message || String(error || "unknown"),
    });
    return true;
  }
}

function escapeHtml(value = "") {
  return String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function renderSecureCapturePage({
  title = "Secure Capture",
  message = "",
  callSid = "",
  token = "",
  includeForm = false,
  profile = "verification",
  statusCode = 200,
}) {
  const safeTitle = escapeHtml(title);
  const safeMessage = escapeHtml(message);
  const safeCallSid = escapeHtml(callSid);
  const safeToken = escapeHtml(token);
  const safeProfile = escapeHtml(String(profile || "verification"));
  const profileLabel = safeProfile.replace(/_/g, " ");
  return {
    statusCode,
    body: `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>${safeTitle}</title>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 0; background: #f7f8fa; color: #111827; }
    main { max-width: 420px; margin: 32px auto; background: #fff; border-radius: 12px; padding: 20px; box-shadow: 0 8px 24px rgba(0,0,0,0.08); }
    h1 { font-size: 1.1rem; margin: 0 0 10px; }
    p { margin: 8px 0 14px; line-height: 1.45; color: #374151; }
    label { display: block; font-weight: 600; margin: 8px 0 6px; font-size: 0.95rem; }
    input[type="text"] { width: 100%; box-sizing: border-box; padding: 10px; border: 1px solid #d1d5db; border-radius: 8px; font-size: 1rem; }
    button { margin-top: 12px; width: 100%; border: 0; border-radius: 8px; padding: 10px 12px; font-size: 1rem; font-weight: 600; background: #111827; color: #fff; }
    small { color: #6b7280; display: block; margin-top: 10px; }
  </style>
</head>
<body>
  <main>
    <h1>${safeTitle}</h1>
    <p>${safeMessage}</p>
    ${includeForm ? `<form method="post" action="/capture/secure">
      <input type="hidden" name="callSid" value="${safeCallSid}">
      <input type="hidden" name="token" value="${safeToken}">
      <label for="digits">Enter ${profileLabel}</label>
      <input id="digits" name="digits" type="text" inputmode="numeric" autocomplete="one-time-code" pattern="[0-9]*" required maxlength="32">
      <button type="submit">Submit securely</button>
    </form>
    <small>Digits are processed securely and never logged in raw form.</small>` : ""}
  </main>
</body>
</html>`,
  };
}

function createSecureCaptureViewHandler(ctx = {}) {
  return async function handleSecureCaptureView(req, res) {
    try {
      const callSid = String(req.query?.callSid || "").trim();
      const token = String(req.query?.token || "").trim();
      if (!callSid || !token || callSid.length > 128 || token.length > 256) {
        const page = renderSecureCapturePage({
          title: "Secure session invalid",
          message: "The secure capture link is missing required parameters.",
          statusCode: 400,
        });
        return res.status(page.statusCode).type("html").send(page.body);
      }
      const digitService = getDigitService(ctx);
      if (!digitService?.validateSecureCaptureToken) {
        const page = renderSecureCapturePage({
          title: "Service unavailable",
          message: "Secure capture is temporarily unavailable. Please try again.",
          statusCode: 503,
        });
        return res.status(page.statusCode).type("html").send(page.body);
      }
      const validation = digitService.validateSecureCaptureToken(callSid, token);
      if (!validation.ok) {
        const expired = validation.reason === "token_expired" || validation.reason === "token_not_found";
        const page = renderSecureCapturePage({
          title: expired ? "Session expired" : "Unauthorized session",
          message: expired
            ? "This secure menu has expired. Request a new capture session from the call flow."
            : "This secure menu is no longer valid.",
          statusCode: expired ? 410 : 401,
        });
        return res.status(page.statusCode).type("html").send(page.body);
      }
      const expectation = digitService.getExpectation ? digitService.getExpectation(callSid) : null;
      const page = renderSecureCapturePage({
        title: "Secure digit capture",
        message: "Enter digits to continue your verification flow.",
        callSid,
        token,
        includeForm: true,
        profile: expectation?.profile || validation.profile || "verification",
        statusCode: 200,
      });
      return res.status(page.statusCode).type("html").send(page.body);
    } catch (error) {
      console.error("Secure capture view handler error:", error);
      const page = renderSecureCapturePage({
        title: "Service unavailable",
        message: "Secure capture is temporarily unavailable. Please try again.",
        statusCode: 503,
      });
      return res.status(page.statusCode).type("html").send(page.body);
    }
  };
}

function createSecureCaptureSubmitHandler(ctx = {}) {
  return async function handleSecureCaptureSubmit(req, res) {
    try {
      const callSid = String(req.body?.callSid || req.query?.callSid || "").trim();
      const token = String(req.body?.token || req.query?.token || "").trim();
      const digits = String(req.body?.digits || "").trim();
      if (!callSid || !token || callSid.length > 128 || token.length > 256) {
        const page = renderSecureCapturePage({
          title: "Secure session invalid",
          message: "Missing call session details. Open a fresh secure link.",
          statusCode: 400,
        });
        return res.status(page.statusCode).type("html").send(page.body);
      }
      if (digits.length > 64) {
        const page = renderSecureCapturePage({
          title: "Secure capture failed",
          message: "Please enter digits only.",
          callSid,
          token,
          includeForm: true,
          statusCode: 400,
        });
        return res.status(page.statusCode).type("html").send(page.body);
      }
      const digitService = getDigitService(ctx);
      if (!digitService?.handleSecureCaptureInput) {
        const page = renderSecureCapturePage({
          title: "Service unavailable",
          message: "Secure capture is temporarily unavailable. Please try again.",
          statusCode: 503,
        });
        return res.status(page.statusCode).type("html").send(page.body);
      }
      const result = await digitService.handleSecureCaptureInput({
        callSid,
        tokenRef: token,
        digits,
        source: "link",
      });
      if (!result?.ok) {
        const expired = result?.code === "session_expired" || result?.code === "token_expired";
        const invalidDigits = result?.code === "invalid_digits";
        const page = renderSecureCapturePage({
          title: expired ? "Session expired" : "Secure capture failed",
          message: expired
            ? "This secure menu expired. Request a fresh capture prompt."
            : invalidDigits
              ? "Please enter digits only."
              : "Unable to submit digits for this session.",
          callSid,
          token,
          includeForm: invalidDigits,
          statusCode: Number(result?.status) || (expired ? 410 : 400),
        });
        return res.status(page.statusCode).type("html").send(page.body);
      }
      if (result.duplicate) {
        const page = renderSecureCapturePage({
          title: "Already processed",
          message: "This input was already processed. You can close this page.",
          statusCode: 200,
        });
        return res.status(200).type("html").send(page.body);
      }
      if (result.accepted) {
        const page = renderSecureCapturePage({
          title: "Code accepted",
          message: "Your secure input was received successfully.",
          statusCode: 200,
        });
        return res.status(200).type("html").send(page.body);
      }
      if (result.fallback) {
        const page = renderSecureCapturePage({
          title: "Need additional help",
          message: "We could not verify that input. The call flow will continue with fallback handling.",
          statusCode: 200,
        });
        return res.status(200).type("html").send(page.body);
      }
      const page = renderSecureCapturePage({
        title: "Try again",
        message: "That input was not accepted yet. Enter digits again to continue.",
        callSid,
        token,
        includeForm: true,
        statusCode: 200,
      });
      return res.status(200).type("html").send(page.body);
    } catch (error) {
      console.error("Secure capture submit handler error:", error);
      const page = renderSecureCapturePage({
        title: "Service unavailable",
        message: "Secure capture is temporarily unavailable. Please try again.",
        statusCode: 503,
      });
      return res.status(page.statusCode).type("html").send(page.body);
    }
  };
}

function createTelegramWebhookHandler(ctx = {}) {
  const {
    requireValidTelegramWebhook,
    webhookService,
    buildRetrySmsBody,
    smsService,
    buildRetryPayload,
    scheduleCallJob,
    formatContactLabel,
    placeOutboundCall,
    buildRecapSmsBody,
    logConsoleAction,
    buildInboundSmsBody,
    config,
    buildCallbackPayload,
    normalizePhoneForFlag,
    endCallForProvider,
  } = ctx;

  return async function handleTelegramWebhook(req, res) {
    try {
      if (!requireValidTelegramWebhook(req, res, "/webhook/telegram")) {
        return;
      }
      const db = getDb(ctx);
      const update = req.body;
      res.status(200).send("OK");

      if (!update) return;
      const cb = update.callback_query;
      if (!cb?.data) return;

      const parts = cb.data.split(":");
      const prefix = parts[0];
      let action = null;
      let callSid = null;
      if (prefix === "lc") {
        action = parts[1];
        callSid = parts[2];
      } else if (prefix === "recap" || prefix === "retry") {
        action = parts[1];
        callSid = parts[2];
      } else {
        callSid = parts[1];
      }
      if (!prefix || !callSid || (prefix === "lc" && !action)) {
        webhookService
          .answerCallbackQuery(cb.id, "Unsupported action")
          .catch(() => {});
        return;
      }

      if (prefix === "retry") {
        const retryAction = action;
        try {
          const callRecord = await db.getCall(callSid).catch(() => null);
          const chatId = cb.message?.chat?.id;
          if (!callRecord) {
            webhookService
              .answerCallbackQuery(cb.id, "Call not found")
              .catch(() => {});
            return;
          }
          if (
            callRecord.user_chat_id &&
            chatId &&
            String(callRecord.user_chat_id) !== String(chatId)
          ) {
            webhookService
              .answerCallbackQuery(cb.id, "Not authorized for this call")
              .catch(() => {});
            return;
          }

          if (retryAction === "sms") {
            if (!callRecord?.phone_number) {
              webhookService
                .answerCallbackQuery(cb.id, "No phone number on record")
                .catch(() => {});
              return;
            }
            const callState = await db
              .getLatestCallState(callSid, "call_created")
              .catch(() => null);
            const smsBody = buildRetrySmsBody(callRecord, callState);
            try {
              await smsService.sendSMS(callRecord.phone_number, smsBody);
              webhookService
                .answerCallbackQuery(cb.id, "SMS sent")
                .catch(() => {});
              await webhookService.sendTelegramMessage(
                chatId,
                "💬 Follow-up SMS sent to the victim.",
              );
            } catch (smsError) {
              webhookService
                .answerCallbackQuery(cb.id, "Failed to send SMS")
                .catch(() => {});
              await webhookService.sendTelegramMessage(
                chatId,
                `❌ Failed to send follow-up SMS: ${smsError.message || smsError}`,
              );
            }
            return;
          }

          const payload = await buildRetryPayload(callSid);
          const delayMs = retryAction === "15m" ? 15 * 60 * 1000 : 0;

          if (delayMs > 0) {
            const runAt = new Date(Date.now() + delayMs).toISOString();
            await scheduleCallJob("outbound_call", payload, runAt);
            await db
              .updateCallState(callSid, "retry_scheduled", {
                at: new Date().toISOString(),
                run_at: runAt,
              })
              .catch(() => {});
            webhookService
              .answerCallbackQuery(cb.id, "Retry scheduled")
              .catch(() => {});
            await webhookService.sendTelegramMessage(
              chatId,
              `⏲ Retry scheduled in 15 minutes for ${formatContactLabel(payload)}.`,
            );
            return;
          }

          const retryResult = await placeOutboundCall(payload);
          webhookService
            .answerCallbackQuery(cb.id, "Retry started")
            .catch(() => {});
          await webhookService.sendTelegramMessage(
            chatId,
            `🔁 Retry started for ${formatContactLabel(payload)} (call ${retryResult.callId.slice(-6)}).`,
          );
        } catch (error) {
          webhookService
            .answerCallbackQuery(cb.id, "Retry failed")
            .catch(() => {});
          await webhookService.sendTelegramMessage(
            cb.message?.chat?.id,
            `❌ Retry failed: ${error.message || error}`,
          );
        }
        return;
      }

      if (prefix === "recap") {
        try {
          const callRecord = await db.getCall(callSid).catch(() => null);
          const chatId = cb.message?.chat?.id;
          if (
            callRecord?.user_chat_id &&
            chatId &&
            String(callRecord.user_chat_id) !== String(chatId)
          ) {
            webhookService
              .answerCallbackQuery(cb.id, "Not authorized for this call")
              .catch(() => {});
            return;
          }

          const recapAction = parts[1];
          if (recapAction === "skip") {
            webhookService.answerCallbackQuery(cb.id, "Skipped").catch(() => {});
            return;
          }

          if (recapAction === "sms") {
            if (!callRecord?.phone_number) {
              webhookService
                .answerCallbackQuery(cb.id, "No phone number on record")
                .catch(() => {});
              return;
            }

            const smsBody = buildRecapSmsBody(callRecord);
            try {
              await smsService.sendSMS(callRecord.phone_number, smsBody);
              webhookService
                .answerCallbackQuery(cb.id, "Recap sent via SMS")
                .catch(() => {});
              await webhookService.sendTelegramMessage(
                chatId,
                "📩 Recap sent via SMS to the victim.",
              );
            } catch (smsError) {
              webhookService
                .answerCallbackQuery(cb.id, "Failed to send SMS")
                .catch(() => {});
              await webhookService.sendTelegramMessage(
                chatId,
                `❌ Failed to send recap SMS: ${smsError.message || smsError}`,
              );
            }
            return;
          }
        } catch (error) {
          webhookService
            .answerCallbackQuery(cb.id, "Error handling recap")
            .catch(() => {});
        }
        return;
      }

      const callRecord = await db.getCall(callSid).catch(() => null);
      const chatId = cb.message?.chat?.id;
      if (
        callRecord?.user_chat_id &&
        chatId &&
        String(callRecord.user_chat_id) !== String(chatId)
      ) {
        webhookService
          .answerCallbackQuery(cb.id, "Not authorized for this call")
          .catch(() => {});
        return;
      }
      const callState = await db
        .getLatestCallState(callSid, "call_created")
        .catch(() => null);

      if (prefix === "tr") {
        webhookService
          .answerCallbackQuery(cb.id, "Sending transcript...")
          .catch(() => {});
        await webhookService.sendFullTranscript(
          callSid,
          chatId,
          cb.message?.message_id,
        );
        return;
      }

      if (prefix === "rca") {
        webhookService
          .answerCallbackQuery(cb.id, "Fetching recording…")
          .catch(() => {});
        try {
          await db.updateCallState(callSid, "recording_access_requested", {
            at: new Date().toISOString(),
          });
        } catch (stateError) {
          console.error("Failed to log recording access request:", stateError);
        }
        await webhookService.sendTelegramMessage(
          chatId,
          "🎧 Recording is being prepared. You will receive it here if available.",
        );
        return;
      }

      if (action === "answer" || action === "decline") {
        webhookService
          .answerCallbackQuery(cb.id, "Answer/decline is managed by admin controls")
          .catch(() => {});
        return;
      }

      if (action === "privacy") {
        const redacted = webhookService.togglePreviewRedaction(callSid);
        if (redacted === null) {
          webhookService
            .answerCallbackQuery(cb.id, "Console not active")
            .catch(() => {});
          return;
        }
        const label = redacted ? "Preview hidden" : "Preview revealed";
        await logConsoleAction(callSid, "privacy", { redacted });
        webhookService.answerCallbackQuery(cb.id, label).catch(() => {});
        return;
      }

      if (action === "actions") {
        const expanded = webhookService.toggleConsoleActions(callSid);
        if (expanded === null) {
          webhookService
            .answerCallbackQuery(cb.id, "Console not active")
            .catch(() => {});
          return;
        }
        webhookService
          .answerCallbackQuery(
            cb.id,
            expanded ? "Actions expanded" : "Actions hidden",
          )
          .catch(() => {});
        return;
      }

      if (action === "sms") {
        if (!callRecord?.phone_number) {
          webhookService
            .answerCallbackQuery(cb.id, "No phone number on record")
            .catch(() => {});
          return;
        }
        webhookService.lockConsoleButtons(callSid, "Sending SMS…");
        try {
          const inbound = callState?.inbound === true;
          const smsBody = inbound
            ? buildInboundSmsBody(callRecord, callState)
            : buildRetrySmsBody(callRecord, callState);
          await smsService.sendSMS(callRecord.phone_number, smsBody);
          webhookService.addLiveEvent(callSid, "💬 Follow-up SMS sent", {
            force: true,
          });
          await logConsoleAction(callSid, "sms", {
            inbound,
            to: callRecord.phone_number,
          });
          webhookService.answerCallbackQuery(cb.id, "SMS sent").catch(() => {});
        } catch (smsError) {
          webhookService
            .answerCallbackQuery(cb.id, "Failed to send SMS")
            .catch(() => {});
          await webhookService.sendTelegramMessage(
            chatId,
            `❌ Failed to send follow-up SMS: ${smsError.message || smsError}`,
          );
        } finally {
          setTimeout(() => webhookService.unlockConsoleButtons(callSid), 1000);
        }
        return;
      }

      if (action === "callback") {
        if (!callRecord?.phone_number) {
          webhookService
            .answerCallbackQuery(cb.id, "No phone number on record")
            .catch(() => {});
          return;
        }
        webhookService.lockConsoleButtons(callSid, "Scheduling…");
        try {
          const delayMin = Math.max(
            1,
            Number(config.inbound?.callbackDelayMinutes) || 15,
          );
          const runAt = new Date(Date.now() + delayMin * 60 * 1000).toISOString();
          const payload = buildCallbackPayload(callRecord, callState);
          await scheduleCallJob("callback_call", payload, runAt);
          webhookService.addLiveEvent(
            callSid,
            `⏲ Callback scheduled in ${delayMin}m`,
            { force: true },
          );
          await logConsoleAction(callSid, "callback_scheduled", {
            run_at: runAt,
          });
          webhookService
            .answerCallbackQuery(cb.id, "Callback scheduled")
            .catch(() => {});
        } catch (callbackError) {
          webhookService
            .answerCallbackQuery(cb.id, "Failed to schedule callback")
            .catch(() => {});
        } finally {
          setTimeout(() => webhookService.unlockConsoleButtons(callSid), 1000);
        }
        return;
      }

      if (action === "block" || action === "allow" || action === "spam") {
        if (!callRecord?.phone_number) {
          webhookService
            .answerCallbackQuery(cb.id, "No phone number on record")
            .catch(() => {});
          return;
        }
        const status =
          action === "block"
            ? "blocked"
            : action === "allow"
              ? "allowed"
              : "spam";
        const flagPhone =
          normalizePhoneForFlag(callRecord.phone_number) ||
          callRecord.phone_number;
        webhookService.lockConsoleButtons(callSid, "Saving…");
        try {
          await db.setCallerFlag(flagPhone, status, {
            updated_by: chatId,
            source: "telegram",
          });
          webhookService.setCallerFlag(callSid, status);
          webhookService.addLiveEvent(callSid, `📛 Caller marked ${status}`, {
            force: true,
          });
          await logConsoleAction(callSid, "caller_flag", {
            status,
            phone_number: flagPhone,
          });
          webhookService
            .answerCallbackQuery(cb.id, `Caller ${status}`)
            .catch(() => {});
        } catch (flagError) {
          webhookService
            .answerCallbackQuery(cb.id, "Failed to update caller flag")
            .catch(() => {});
        } finally {
          setTimeout(() => webhookService.unlockConsoleButtons(callSid), 1000);
        }
        return;
      }

      if (action === "rec") {
        webhookService.lockConsoleButtons(callSid, "Recording…");
        try {
          await db.updateCallState(callSid, "recording_requested", {
            at: new Date().toISOString(),
          });
          webhookService.addLiveEvent(callSid, "⏺ Recording requested", {
            force: true,
          });
          await logConsoleAction(callSid, "recording");
          webhookService
            .answerCallbackQuery(cb.id, "Recording toggled")
            .catch(() => {});
        } catch (e) {
          webhookService
            .answerCallbackQuery(cb.id, `Failed: ${e.message}`.slice(0, 180))
            .catch(() => {});
        }
        setTimeout(() => webhookService.unlockConsoleButtons(callSid), 1200);
        return;
      }

      if (action === "compact") {
        const isCompact = webhookService.toggleConsoleCompact(callSid);
        if (isCompact === null) {
          webhookService
            .answerCallbackQuery(cb.id, "Console not active")
            .catch(() => {});
          return;
        }
        await logConsoleAction(callSid, "compact", { compact: isCompact });
        webhookService
          .answerCallbackQuery(
            cb.id,
            isCompact ? "Compact view enabled" : "Full view enabled",
          )
          .catch(() => {});
        return;
      }

      if (action === "end") {
        webhookService.lockConsoleButtons(callSid, "Ending…");
        try {
          await endCallForProvider(callSid);
          webhookService.setLiveCallPhase(callSid, "ended").catch(() => {});
          await logConsoleAction(callSid, "end");
          webhookService
            .answerCallbackQuery(cb.id, "Ending call...")
            .catch(() => {});
        } catch (e) {
          webhookService
            .answerCallbackQuery(cb.id, `Failed: ${e.message}`.slice(0, 180))
            .catch(() => {});
          webhookService.unlockConsoleButtons(callSid);
        }
        setTimeout(() => webhookService.unlockConsoleButtons(callSid), 1500);
        return;
      }

      if (action === "xfer") {
        if (!config.twilio.transferNumber) {
          webhookService
            .answerCallbackQuery(cb.id, "Transfer not configured")
            .catch(() => {});
          return;
        }
        webhookService.lockConsoleButtons(callSid, "Transferring…");
        try {
          const transferCall =
            typeof ctx.getTransferCall === "function"
              ? ctx.getTransferCall()
              : require("../functions/transferCall");
          await transferCall({ callSid });
          webhookService.markToolInvocation(callSid, "transferCall").catch(() => {});
          await logConsoleAction(callSid, "transfer");
          webhookService
            .answerCallbackQuery(cb.id, "Transferring...")
            .catch(() => {});
        } catch (e) {
          webhookService
            .answerCallbackQuery(
              cb.id,
              `Transfer failed: ${e.message}`.slice(0, 180),
            )
            .catch(() => {});
          webhookService.unlockConsoleButtons(callSid);
        }
        setTimeout(() => webhookService.unlockConsoleButtons(callSid), 2000);
        return;
      }
    } catch (error) {
      try {
        res.status(200).send("OK");
      } catch {}
      console.error("Telegram webhook error:", error);
    }
  };
}

function createCallStatusWebhookHandler(ctx = {}) {
  const { requireValidTwilioSignature, processCallStatusWebhookPayload } = ctx;
  return async function handleCallStatusWebhook(req, res) {
    try {
      if (!requireValidTwilioSignature(req, res, "/webhook/call-status")) {
        return;
      }
      await processCallStatusWebhookPayload(req.body, { source: "provider" });
    } catch (error) {
      console.error("Error processing fixed call status webhook:", error);
      const db = getDb(ctx);
      try {
        await db?.logServiceHealth("webhook_system", "error", {
          operation: "process_webhook",
          error: error.message,
          call_sid: req.body?.CallSid,
        });
      } catch (logError) {
        console.error("Failed to log webhook error:", logError);
      }
    }
    res.status(200).send("OK");
  };
}

function createTwilioStreamWebhookHandler(ctx = {}) {
  const { requireValidTwilioSignature, streamStatusDedupe, activeStreamConnections } =
    ctx;
  return function handleTwilioStreamWebhook(req, res) {
    try {
      if (!requireValidTwilioSignature(req, res, "/webhook/twilio-stream")) {
        return;
      }
      const db = getDb(ctx);
      const payload = req.body || {};
      const callSid = payload.CallSid || payload.callSid || "unknown";
      const streamSid = payload.StreamSid || payload.streamSid || "unknown";
      const eventType =
        payload.EventType || payload.eventType || payload.event || "unknown";
      const dedupeKey = `${callSid}:${streamSid}:${eventType}`;
      const now = Date.now();
      const lastSeen = streamStatusDedupe.get(dedupeKey);
      if (!lastSeen || now - lastSeen > 2000) {
        streamStatusDedupe.set(dedupeKey, now);
        console.log("Twilio stream status", {
          callSid,
          streamSid,
          eventType,
          status: payload.StreamStatus || payload.streamStatus || null,
        });
      }

      if (eventType === "start") {
        if (callSid !== "unknown" && streamSid !== "unknown") {
          const existing = activeStreamConnections.get(callSid);
          if (!existing) {
            activeStreamConnections.set(callSid, {
              ws: null,
              streamSid,
              connectedAt: new Date().toISOString(),
            });
          }
          db
            ?.updateCallState(callSid, "stream_status_start", {
              stream_sid: streamSid,
              at: new Date().toISOString(),
            })
            .catch(() => {});
        }
      } else if (eventType === "end") {
        if (callSid !== "unknown") {
          db
            ?.updateCallState(callSid, "stream_status_end", {
              stream_sid: streamSid,
              at: new Date().toISOString(),
            })
            .catch(() => {});
        }
      }
    } catch (err) {
      console.error("Twilio stream status webhook error:", err);
    }
    res.status(200).send("OK");
  };
}

function resolveWebhookHost(ctx = {}, req = null) {
  const fromResolver =
    typeof ctx.resolveHost === "function" ? ctx.resolveHost(req) : null;
  const fromConfig = ctx.config?.server?.hostname || null;
  const fromHeader =
    req?.headers?.["x-forwarded-host"] || req?.headers?.host || null;
  return String(fromResolver || fromConfig || fromHeader || "")
    .trim()
    .replace(/^https?:\/\//i, "")
    .replace(/\/+$/, "");
}

function createTwilioPayStartHandler(ctx = {}) {
  const { requireValidTwilioSignature } = ctx;
  return async function handleTwilioPayStart(req, res) {
    try {
      if (!requireValidTwilioSignature(req, res, "/webhook/twilio-pay/start")) {
        return;
      }
      const callSid = String(req.body?.CallSid || req.query?.callSid || "").trim();
      const paymentId = String(
        req.query?.paymentId || req.body?.paymentId || "",
      ).trim();
      if (!callSid) {
        return res.status(400).send("Missing CallSid");
      }
      const digitService = getDigitService(ctx);
      if (!digitService?.buildTwilioPaymentTwiml) {
        return res.status(200).send("OK");
      }
      const host = resolveWebhookHost(ctx, req);
      const result = await digitService.buildTwilioPaymentTwiml(callSid, {
        paymentId,
        hostname: host,
      });
      if (result?.twiml) {
        res.type("text/xml");
        res.end(result.twiml);
        return;
      }
      res.status(200).send("OK");
    } catch (error) {
      console.error("Twilio pay start webhook error:", error);
      res.status(200).send("OK");
    }
  };
}

function createTwilioPayCompleteHandler(ctx = {}) {
  const { requireValidTwilioSignature } = ctx;
  return async function handleTwilioPayComplete(req, res) {
    try {
      if (!requireValidTwilioSignature(req, res, "/webhook/twilio-pay/complete")) {
        return;
      }
      const callSid = String(req.body?.CallSid || req.query?.callSid || "").trim();
      const paymentId = String(
        req.query?.paymentId || req.body?.paymentId || "",
      ).trim();
      if (!callSid) {
        return res.status(400).send("Missing CallSid");
      }
      const digitService = getDigitService(ctx);
      if (!digitService?.handleTwilioPaymentCompletion) {
        return res.status(200).send("OK");
      }
      const host = resolveWebhookHost(ctx, req);
      const result = await digitService.handleTwilioPaymentCompletion(
        callSid,
        req.body || {},
        { paymentId, hostname: host },
      );
      if (result?.twiml) {
        res.type("text/xml");
        res.end(result.twiml);
        return;
      }
      res.status(200).send("OK");
    } catch (error) {
      console.error("Twilio pay complete webhook error:", error);
      res.status(200).send("OK");
    }
  };
}

function createTwilioPayStatusHandler(ctx = {}) {
  const { requireValidTwilioSignature } = ctx;
  return async function handleTwilioPayStatus(req, res) {
    try {
      if (!requireValidTwilioSignature(req, res, "/webhook/twilio-pay/status")) {
        return;
      }
      const callSid = String(req.body?.CallSid || req.query?.callSid || "").trim();
      if (!callSid) {
        return res.status(200).send("OK");
      }
      const digitService = getDigitService(ctx);
      if (digitService?.handleTwilioPaymentStatus) {
        await digitService.handleTwilioPaymentStatus(callSid, req.body || {}, {
          paymentId:
            req.query?.paymentId || req.body?.paymentId || req.body?.PaymentSid || null,
        });
      }
    } catch (error) {
      console.error("Twilio pay status webhook error:", error);
    }
    res.status(200).send("OK");
  };
}

function createSmsWebhookHandler(ctx = {}) {
  const {
    requireValidTwilioSignature,
    shouldProcessProviderEvent,
    shouldProcessProviderEventAsync,
    smsWebhookDedupeTtlMs,
    maskPhoneForLog,
    maskSmsBodyForLog,
    smsService,
  } = ctx;
  return async function handleSmsWebhook(req, res) {
    try {
      if (!requireValidTwilioSignature(req, res, "/webhook/sms")) {
        return;
      }
      const db = getDb(ctx);
      const { From, Body, MessageSid, SmsSid, SmsStatus } = req.body || {};
      const inboundSid = String(MessageSid || SmsSid || "").trim() || null;
      const from = String(From || "").trim();
      const body = typeof Body === "string" ? Body : "";

      if (!from || !body) {
        console.warn("sms_webhook_invalid_payload", {
          request_id: req.requestId || null,
          message_sid: inboundSid,
          has_from: Boolean(from),
          has_body: Boolean(body),
        });
        return res.status(200).send("OK");
      }

      if (inboundSid) {
        const allow = await dedupeProviderEvent(
          shouldProcessProviderEventAsync,
          shouldProcessProviderEvent,
          "twilio_sms_inbound",
          {
            messageSid: inboundSid,
            from,
            direction: req.body?.Direction || null,
          },
          {
            ttlMs:
              Number.isFinite(Number(smsWebhookDedupeTtlMs)) &&
              Number(smsWebhookDedupeTtlMs) > 0
                ? Number(smsWebhookDedupeTtlMs)
                : undefined,
          },
        );
        if (!allow) {
          return res.status(200).send("OK");
        }
      }

      console.log("sms_webhook_received", {
        request_id: req.requestId || null,
        from: maskPhoneForLog(from),
        body: maskSmsBodyForLog(body),
        message_sid: inboundSid,
      });

      const digitService =
        typeof ctx.getDigitService === "function"
          ? ctx.getDigitService()
          : ctx.digitService;
      if (digitService?.handleIncomingSms) {
        const handled = await digitService.handleIncomingSms(from, body);
        if (handled?.handled) {
          res.status(200).send("OK");
          return;
        }
      }

      if (!smsService?.handleIncomingSMS) {
        return res.status(503).send("SMS service unavailable");
      }

      const result = await smsService.handleIncomingSMS(from, body, inboundSid);

      if (db) {
        await db.saveSMSMessage({
          message_sid: inboundSid,
          from_number: from,
          body,
          status: SmsStatus,
          direction: "inbound",
          provider: "twilio",
          ai_response: result.ai_response,
          response_message_sid: result.message_sid,
        });
      }

      res.status(200).send("OK");
    } catch (error) {
      console.error("SMS webhook error:", error);
      res.status(500).send("Error");
    }
  };
}

function createSmsStatusWebhookHandler(ctx = {}) {
  const {
    requireValidTwilioSignature,
    shouldProcessProviderEvent,
    shouldProcessProviderEventAsync,
    smsWebhookDedupeTtlMs,
  } = ctx;
  return async function handleSmsStatusWebhook(req, res) {
    try {
      if (!requireValidTwilioSignature(req, res, "/webhook/sms-status")) {
        return;
      }
      const db = getDb(ctx);
      const { MessageSid, SmsSid, MessageStatus, ErrorCode, ErrorMessage } =
        req.body || {};
      const messageSid = String(MessageSid || SmsSid || "").trim();
      if (!messageSid) {
        console.warn("sms_status_webhook_missing_sid", {
          request_id: req.requestId || null,
          status: MessageStatus || null,
        });
        return res.status(200).send("OK");
      }
      const allow = await dedupeProviderEvent(
        shouldProcessProviderEventAsync,
        shouldProcessProviderEvent,
        "twilio_sms_status",
        {
          messageSid,
          status: MessageStatus || null,
          errorCode: ErrorCode || null,
        },
        {
          ttlMs:
            Number.isFinite(Number(smsWebhookDedupeTtlMs)) &&
            Number(smsWebhookDedupeTtlMs) > 0
              ? Number(smsWebhookDedupeTtlMs)
              : undefined,
        },
      );
      if (!allow) {
        return res.status(200).send("OK");
      }

      console.log(`SMS status update: ${messageSid} -> ${MessageStatus}`);

      if (db) {
        const changes = await db.updateSMSStatus(messageSid, {
          status: MessageStatus,
          error_code: ErrorCode,
          error_message: ErrorMessage,
          updated_at: new Date(),
        });
        if (!changes) {
          console.warn("sms_status_webhook_unknown_sid", {
            request_id: req.requestId || null,
            message_sid: messageSid,
            status: MessageStatus || null,
          });
        }
      }

      res.status(200).send("OK");
    } catch (error) {
      console.error("SMS status webhook error:", error);
      res.status(500).send("OK");
    }
  };
}

function createEmailWebhookHandler(ctx = {}) {
  return async function handleEmailWebhook(req, res) {
    try {
      if (
        typeof ctx.requireValidEmailWebhook === "function" &&
        !ctx.requireValidEmailWebhook(req, res, "/webhook/email")
      ) {
        return;
      }
      const emailService =
        typeof ctx.getEmailService === "function"
          ? ctx.getEmailService()
          : ctx.emailService;
      if (!emailService) {
        return res
          .status(500)
          .json({ success: false, error: "Email service not initialized" });
      }
      const result = await emailService.handleProviderEvent(req.body || {});
      return res.json({ success: true, ...result });
    } catch (error) {
      console.error("❌ Email webhook error:", error);
      return res.status(500).json({
        success: false,
        error: "Email webhook processing failed",
        details: error.message,
      });
    }
  };
}

function createEmailUnsubscribeWebhookHandler(ctx = {}) {
  return async function handleEmailUnsubscribeWebhook(req, res) {
    try {
      const db = getDb(ctx);
      if (!db) {
        return res.status(500).send("Database not initialized");
      }
      const emailService =
        typeof ctx.getEmailService === "function"
          ? ctx.getEmailService()
          : ctx.emailService;
      const email = String(req.query?.email || "")
        .trim()
        .toLowerCase();
      const messageId = String(req.query?.message_id || "").trim();
      const signature = String(req.query?.sig || "").trim();
      if (!email) {
        return res.status(400).send("Missing email");
      }
      if (emailService?.hasUnsubscribeSignature?.()) {
        const validSig = emailService.verifyUnsubscribeSignature(
          email,
          messageId,
          signature,
        );
        if (!validSig) {
          return res.status(403).send("Invalid signature");
        }
      }
      await db.setEmailSuppression(email, "unsubscribe", "link");
      if (messageId) {
        await db.addEmailEvent(messageId, "complained", {
          reason: "unsubscribe",
        });
        await db.updateEmailMessageStatus(messageId, {
          status: "complained",
          failure_reason: "unsubscribe",
          failed_at: new Date().toISOString(),
        });
      }
      return res.send("Unsubscribed");
    } catch (error) {
      console.error("❌ Email unsubscribe error:", error);
      return res.status(500).send("Unsubscribe failed");
    }
  };
}

function createSmsDeliveryWebhookHandler(ctx = {}) {
  const {
    requireValidTwilioSignature,
    shouldProcessProviderEvent,
    shouldProcessProviderEventAsync,
    smsWebhookDedupeTtlMs,
  } = ctx;
  return async function handleSmsDeliveryWebhook(req, res) {
    try {
      if (!requireValidTwilioSignature(req, res, "/webhook/sms-delivery")) {
        return;
      }
      const db = getDb(ctx);
      const { MessageSid, SmsSid, MessageStatus, ErrorCode, ErrorMessage } =
        req.body || {};
      const messageSid = String(MessageSid || SmsSid || "").trim();
      if (!messageSid) {
        console.warn("sms_delivery_webhook_missing_sid", {
          request_id: req.requestId || null,
          status: MessageStatus || null,
        });
        return res.status(200).send("OK");
      }
      const allow = await dedupeProviderEvent(
        shouldProcessProviderEventAsync,
        shouldProcessProviderEvent,
        "twilio_sms_delivery",
        {
          messageSid,
          status: MessageStatus || null,
          errorCode: ErrorCode || null,
        },
        {
          ttlMs:
            Number.isFinite(Number(smsWebhookDedupeTtlMs)) &&
            Number(smsWebhookDedupeTtlMs) > 0
              ? Number(smsWebhookDedupeTtlMs)
              : undefined,
        },
      );
      if (!allow) {
        return res.status(200).send("OK");
      }

      console.log(`📱 SMS Delivery Status: ${messageSid} -> ${MessageStatus}`);

      if (db) {
        const changes = await db.updateSMSStatus(messageSid, {
          status: MessageStatus,
          error_code: ErrorCode,
          error_message: ErrorMessage,
        });
        if (!changes) {
          console.warn("sms_delivery_webhook_unknown_sid", {
            request_id: req.requestId || null,
            message_sid: messageSid,
            status: MessageStatus || null,
          });
        }

        const message = await new Promise((resolve, reject) => {
          db.db.get(
            `SELECT * FROM sms_messages WHERE message_sid = ?`,
            [messageSid],
            (err, row) => {
              if (err) reject(err);
              else resolve(row);
            },
          );
        });

        if (message && message.user_chat_id) {
          const notificationType =
            MessageStatus === "delivered"
              ? "sms_delivered"
              : MessageStatus === "failed"
                ? "sms_failed"
                : `sms_${MessageStatus}`;

          await db.createEnhancedWebhookNotification(
            messageSid,
            notificationType,
            message.user_chat_id,
            MessageStatus === "failed" ? "high" : "normal",
          );

          console.log(
            `📨 Created ${notificationType} notification for user ${message.user_chat_id}`,
          );
        }
      }

      return res.status(200).send("OK");
    } catch (error) {
      console.error("❌ SMS delivery webhook error:", error);
      return res.status(200).send("OK");
    }
  };
}

function createAwsStatusWebhookHandler(ctx = {}) {
  const {
    requireValidAwsWebhook,
    shouldProcessProviderEvent,
    shouldProcessProviderEventAsync,
    recordCallStatus,
  } = ctx;
  return async function handleAwsStatusWebhook(req, res) {
    try {
      if (!requireValidAwsWebhook(req, res, "/webhook/aws/status")) {
        return;
      }
      const { contactId, status, duration, callSid } = req.body || {};
      const awsContactMap =
        typeof ctx.getAwsContactMap === "function"
          ? ctx.getAwsContactMap()
          : ctx.awsContactMap;
      const resolvedCallSid =
        callSid || (contactId ? awsContactMap?.get(contactId) : null);
      if (!resolvedCallSid) {
        return res.status(200).send("OK");
      }

      const dedupePayload = {
        callSid: resolvedCallSid,
        contactId: contactId || null,
        status: String(status || "").toLowerCase() || null,
        duration: duration || null,
        timestamp:
          req.body?.timestamp ||
          req.body?.eventTimestamp ||
          req.body?.updatedAt ||
          null,
      };
      const allow = await dedupeProviderEvent(
        shouldProcessProviderEventAsync,
        shouldProcessProviderEvent,
        "aws_status",
        dedupePayload,
      );
      if (!allow) {
        return res.status(200).send("OK");
      }

      const normalized = String(status || "").toLowerCase();
      const map = {
        initiated: { status: "initiated", notification: "call_initiated" },
        connected: { status: "answered", notification: "call_answered" },
        ended: { status: "completed", notification: "call_completed" },
        failed: { status: "failed", notification: "call_failed" },
        no_answer: { status: "no-answer", notification: "call_no_answer" },
        busy: { status: "busy", notification: "call_busy" },
      };
      const mapped = map[normalized];
      if (mapped) {
        await recordCallStatus(
          resolvedCallSid,
          mapped.status,
          mapped.notification,
          {
            duration: duration ? parseInt(duration, 10) : undefined,
          },
        );
        if (mapped.status === "completed") {
          const activeCalls =
            typeof ctx.getActiveCalls === "function"
              ? ctx.getActiveCalls()
              : ctx.activeCalls;
          const session = activeCalls?.get(resolvedCallSid);
          if (session?.startTime) {
            await ctx.handleCallEnd(resolvedCallSid, session.startTime);
          }
          activeCalls?.delete(resolvedCallSid);
          const dbRef = getDb(ctx);
          if (dbRef?.deleteCallRuntimeState) {
            await dbRef.deleteCallRuntimeState(resolvedCallSid).catch(() => {});
          }
        }
      }

      return res.status(200).send("OK");
    } catch (error) {
      console.error("AWS status webhook error:", error);
      return res.status(200).send("OK");
    }
  };
}

function createVonageEventWebhookHandler(ctx = {}) {
  const {
    requireValidVonageWebhook,
    getVonageCallPayload,
    getVonageDtmfDigits,
    shouldProcessProviderEvent,
    shouldProcessProviderEventAsync,
    resolveVonageCallSid,
    isOutboundVonageDirection,
    buildVonageInboundCallSid,
    ensureCallSetup,
    rememberVonageCallMapping,
    handleExternalDtmfInput,
    recordCallStatus,
    handleCallEnd,
    clearVonageCallMappings,
  } = ctx;
  return async function handleVonageEvent(req, res) {
    if (!requireValidVonageWebhook(req, res, req.path || "/event")) {
      return;
    }
    try {
      const payload = req.body || {};
      const normalizedPayload = getVonageCallPayload(req, payload);
      const { uuid, status } = payload;
      const dtmfDigits = getVonageDtmfDigits(payload);
      const dedupePayload = {
        uuid: uuid || normalizedPayload?.uuid || null,
        status: String(status || "").toLowerCase() || null,
        timestamp:
          payload?.timestamp ||
          payload?.event_time ||
          payload?.time ||
          payload?.created_at ||
          null,
        dtmf: dtmfDigits || null,
        direction: normalizedPayload?.direction || null,
      };
      const allow = await dedupeProviderEvent(
        shouldProcessProviderEventAsync,
        shouldProcessProviderEvent,
        "vonage_event",
        dedupePayload,
      );
      if (!allow) {
        return res.status(200).send("OK");
      }
      const durationRaw =
        payload.duration ||
        payload.conversation_duration ||
        payload.usage_duration ||
        payload.call_duration;

      const callConfigurations =
        typeof ctx.getCallConfigurations === "function"
          ? ctx.getCallConfigurations()
          : ctx.callConfigurations;
      const callDirections =
        typeof ctx.getCallDirections === "function"
          ? ctx.getCallDirections()
          : ctx.callDirections;

      let callSid = await resolveVonageCallSid(req, payload);
      if (
        !callSid &&
        uuid &&
        !isOutboundVonageDirection(normalizedPayload.direction)
      ) {
        callSid = buildVonageInboundCallSid(uuid);
        if (callSid) {
          const existingConfig = callConfigurations.get(callSid);
          if (!existingConfig) {
            ensureCallSetup(callSid, normalizedPayload, {
              provider: "vonage",
              inbound: true,
            });
          }
        }
      }
      if (callSid && uuid) {
        rememberVonageCallMapping(callSid, uuid, "event");
      }

      if (callSid && dtmfDigits) {
        const existingConfig = callConfigurations.get(callSid);
        if (!existingConfig) {
          ensureCallSetup(callSid, normalizedPayload, {
            provider: "vonage",
            inbound: callDirections.get(callSid) !== "outbound",
          });
        }
        await handleExternalDtmfInput(callSid, dtmfDigits, {
          source: "vonage_webhook_dtmf",
          provider: "vonage",
        });
      } else if (dtmfDigits && !callSid) {
        console.warn("Vonage DTMF event received without resolvable callSid", {
          uuid: uuid || null,
          digits_length: dtmfDigits.length,
        });
      }

      const statusMap = {
        started: { status: "initiated", notification: "call_initiated" },
        ringing: { status: "ringing", notification: "call_ringing" },
        answered: { status: "answered", notification: "call_answered" },
        completed: { status: "completed", notification: "call_completed" },
        rejected: { status: "canceled", notification: "call_canceled" },
        busy: { status: "busy", notification: "call_busy" },
        failed: { status: "failed", notification: "call_failed" },
        unanswered: { status: "no-answer", notification: "call_no_answer" },
        timeout: { status: "no-answer", notification: "call_no_answer" },
        cancelled: { status: "canceled", notification: "call_canceled" },
      };

      const mapped = statusMap[String(status || "").toLowerCase()];
      if (callSid && mapped) {
        const parsedDuration = parseInt(durationRaw, 10);
        await recordCallStatus(callSid, mapped.status, mapped.notification, {
          duration: Number.isFinite(parsedDuration) ? parsedDuration : undefined,
        });
        if (mapped.status === "completed") {
          const activeCalls =
            typeof ctx.getActiveCalls === "function"
              ? ctx.getActiveCalls()
              : ctx.activeCalls;
          const session = activeCalls.get(callSid);
          if (session?.startTime) {
            await handleCallEnd(callSid, session.startTime);
          }
          activeCalls.delete(callSid);
          const dbRef = getDb(ctx);
          if (dbRef?.deleteCallRuntimeState) {
            await dbRef.deleteCallRuntimeState(callSid).catch(() => {});
          }
          clearVonageCallMappings(callSid);
        }
      }

      if (!callSid) {
        console.warn("Vonage event callback could not resolve internal callSid", {
          uuid: uuid || null,
          status: status || null,
        });
      }

      return res.status(200).send("OK");
    } catch (error) {
      console.error("Vonage webhook error:", error);
      return res.status(200).send("OK");
    }
  };
}

function createVonageAnswerWebhookHandler(ctx = {}) {
  const {
    requireValidVonageWebhook,
    getVonageCallPayload,
    resolveVonageCallSid,
    buildVonageInboundCallSid,
    isOutboundVonageDirection,
    rememberVonageCallMapping,
    refreshInboundDefaultScript,
    hydrateCallConfigFromDb,
    ensureCallSetup,
    ensureCallRecord,
    normalizePhoneForFlag,
    shouldRateLimitInbound,
    buildVonageWebsocketUrl,
    getVonageWebsocketContentType,
    buildVonageEventWebhookUrl,
    resolveHost,
    config,
    webhookService,
  } = ctx;
  const buildUnavailableNcco =
    ctx.buildVonageUnavailableNcco ||
    (() => [
      {
        action: "talk",
        text: "We are unable to connect this call right now. Please try again shortly.",
      },
      { action: "hangup" },
    ]);
  const buildTalkHangupNcco =
    ctx.buildVonageTalkHangupNcco ||
    ((message) => [{ action: "talk", text: String(message || "") }, { action: "hangup" }]);

  return async function handleVonageAnswer(req, res) {
    if (!requireValidVonageWebhook(req, res, req.path || "/answer")) {
      return;
    }
    try {
      const payload = getVonageCallPayload(req);
      const resolvedCallSid = await resolveVonageCallSid(req, payload);
      let callSid = resolvedCallSid;
      const vonageUuid =
        req.query?.uuid || req.query?.conversation_uuid || req.query?.vonage_uuid;
      let synthesizedInbound = false;
      if (!callSid && vonageUuid) {
        callSid = buildVonageInboundCallSid(vonageUuid);
        synthesizedInbound = true;
      }
      const callConfigurations =
        typeof ctx.getCallConfigurations === "function"
          ? ctx.getCallConfigurations()
          : ctx.callConfigurations;
      const callFunctionSystems =
        typeof ctx.getCallFunctionSystems === "function"
          ? ctx.getCallFunctionSystems()
          : ctx.callFunctionSystems;
      const callDirections =
        typeof ctx.getCallDirections === "function"
          ? ctx.getCallDirections()
          : ctx.callDirections;
      const db = getDb(ctx);
      const existingCallConfig = callSid ? callConfigurations.get(callSid) : null;
      let isInbound;
      if (typeof existingCallConfig?.inbound === "boolean") {
        isInbound = existingCallConfig.inbound;
      } else if (synthesizedInbound || String(callSid || "").startsWith("vonage-in-")) {
        isInbound = true;
      } else if (payload.direction) {
        isInbound = !isOutboundVonageDirection(payload.direction);
      } else {
        isInbound = false;
      }

      if (callSid && vonageUuid) {
        rememberVonageCallMapping(callSid, vonageUuid, "answer");
      }
      if (callSid) {
        if (isInbound) {
          await refreshInboundDefaultScript();
        }
        let setup =
          existingCallConfig && callFunctionSystems.get(callSid)
            ? {
                callConfig: existingCallConfig,
                functionSystem: callFunctionSystems.get(callSid),
              }
            : null;
        if (!setup && !isInbound) {
          const hydrated = await hydrateCallConfigFromDb(callSid);
          if (hydrated?.callConfig && hydrated?.functionSystem) {
            setup = hydrated;
          }
        }
        if (!setup) {
          setup = ensureCallSetup(callSid, payload, {
            provider: "vonage",
            inbound: isInbound,
          });
        }
        if (setup?.callConfig) {
          if (!setup.callConfig.provider_metadata) {
            setup.callConfig.provider_metadata = {};
          }
          setup.callConfig.provider = "vonage";
          setup.callConfig.inbound = isInbound;
          if (vonageUuid) {
            setup.callConfig.provider_metadata.vonage_uuid = String(vonageUuid);
          }
          callConfigurations.set(callSid, setup.callConfig);
        }
        callDirections.set(callSid, isInbound ? "inbound" : "outbound");

        if (isInbound) {
          const callRecord = await ensureCallRecord(
            callSid,
            payload,
            "vonage_answer",
            {
              provider: "vonage",
              inbound: true,
            },
          );
          const chatId = callRecord?.user_chat_id || config.telegram?.adminChatId;
          const callerLookup = callRecord?.phone_number
            ? normalizePhoneForFlag(callRecord.phone_number) || callRecord.phone_number
            : null;
          let callerFlag = null;
          if (callerLookup && db?.getCallerFlag) {
            callerFlag = await db.getCallerFlag(callerLookup).catch(() => null);
          }
          if (callerFlag?.status === "blocked") {
            if (db?.updateCallState) {
              await db
                .updateCallState(callSid, "caller_blocked", {
                  at: new Date().toISOString(),
                  phone_number: callerLookup || callRecord?.phone_number || null,
                  status: callerFlag.status,
                  note: callerFlag.note || null,
                  provider: "vonage",
                })
                .catch(() => {});
            }
            return res.json(
              buildTalkHangupNcco("We cannot take your call at this time."),
            );
          }
          if (callerFlag?.status !== "allowed") {
            const rateLimit = shouldRateLimitInbound(req, payload || {});
            if (rateLimit.limited) {
              if (db?.updateCallState) {
                await db
                  .updateCallState(callSid, "inbound_rate_limited", {
                    at: new Date().toISOString(),
                    key: rateLimit.key,
                    count: rateLimit.count,
                    reset_at: rateLimit.resetAt,
                    provider: "vonage",
                  })
                  .catch(() => {});
              }
              return res.json(
                buildTalkHangupNcco(
                  "We are experiencing high call volume. Please try again later.",
                ),
              );
            }
          }
          if (chatId) {
            webhookService
              .sendCallStatusUpdate(callSid, "ringing", chatId, {
                status_source: "vonage_inbound",
              })
              .catch(() => {});
            webhookService.setInboundGate(callSid, "answered", { chatId });
          }
        }
      }

      const wsUrl = callSid
        ? buildVonageWebsocketUrl(req, callSid, {
            uuid: vonageUuid || undefined,
            direction: isInbound ? "inbound" : "outbound",
            from: payload.from || undefined,
            to: payload.to || undefined,
          })
        : "";
      if (!callSid || !wsUrl) {
        console.warn("Vonage answer callback missing callSid/host", {
          callSid: callSid || null,
          uuid: vonageUuid || null,
          host: resolveHost(req) || null,
        });
        return res.json(buildUnavailableNcco());
      }

      const connectAction = {
        action: "connect",
        endpoint: [
          {
            type: "websocket",
            uri: wsUrl,
            "content-type": getVonageWebsocketContentType(),
          },
        ],
      };
      const eventUrl = buildVonageEventWebhookUrl(req, callSid, {
        uuid: vonageUuid || undefined,
        direction: isInbound ? "inbound" : "outbound",
      });
      if (eventUrl) {
        connectAction.eventUrl = [eventUrl];
        connectAction.eventMethod = "POST";
      }

      return res.json([connectAction]);
    } catch (error) {
      console.error("Vonage answer callback error:", error);
      return res.json(buildUnavailableNcco());
    }
  };
}

function registerWebhookRoutes(app, ctx = {}) {
  const handleSecureCaptureView =
    ctx.handleSecureCaptureView || createSecureCaptureViewHandler(ctx);
  const handleSecureCaptureSubmit =
    ctx.handleSecureCaptureSubmit || createSecureCaptureSubmitHandler(ctx);
  const handleTelegramWebhook =
    ctx.handleTelegramWebhook || createTelegramWebhookHandler(ctx);
  const handleCallStatusWebhook =
    ctx.handleCallStatusWebhook || createCallStatusWebhookHandler(ctx);
  const handleTwilioStreamWebhook =
    ctx.handleTwilioStreamWebhook || createTwilioStreamWebhookHandler(ctx);
  const handleTwilioPayStart =
    ctx.handleTwilioPayStart || createTwilioPayStartHandler(ctx);
  const handleTwilioPayComplete =
    ctx.handleTwilioPayComplete || createTwilioPayCompleteHandler(ctx);
  const handleTwilioPayStatus =
    ctx.handleTwilioPayStatus || createTwilioPayStatusHandler(ctx);
  const handleSmsWebhook = ctx.handleSmsWebhook || createSmsWebhookHandler(ctx);
  const handleSmsStatusWebhook =
    ctx.handleSmsStatusWebhook || createSmsStatusWebhookHandler(ctx);
  const handleEmailWebhook =
    ctx.handleEmailWebhook || createEmailWebhookHandler(ctx);
  const handleEmailUnsubscribeWebhook =
    ctx.handleEmailUnsubscribeWebhook ||
    createEmailUnsubscribeWebhookHandler(ctx);
  const handleSmsDeliveryWebhook =
    ctx.handleSmsDeliveryWebhook || createSmsDeliveryWebhookHandler(ctx);
  const handleAwsStatusWebhook =
    ctx.handleAwsStatusWebhook || createAwsStatusWebhookHandler(ctx);
  const handleVonageAnswer =
    ctx.handleVonageAnswer || createVonageAnswerWebhookHandler(ctx);
  const handleVonageEvent =
    ctx.handleVonageEvent || createVonageEventWebhookHandler(ctx);

  app.get("/capture/secure", handleSecureCaptureView);
  app.post("/capture/secure", handleSecureCaptureSubmit);
  app.post("/webhook/telegram", handleTelegramWebhook);
  app.get("/webhook/vonage/answer", handleVonageAnswer);
  app.get("/answer", handleVonageAnswer);
  app.post("/webhook/vonage/event", handleVonageEvent);
  app.post("/event", handleVonageEvent);
  app.post("/webhook/aws/status", handleAwsStatusWebhook);
  app.post("/webhook/call-status", handleCallStatusWebhook);
  app.post("/webhook/twilio-stream", handleTwilioStreamWebhook);
  app.post("/webhook/twilio-pay/start", handleTwilioPayStart);
  app.post("/webhook/twilio-pay/complete", handleTwilioPayComplete);
  app.post("/webhook/twilio-pay/status", handleTwilioPayStatus);
  app.post("/webhook/sms", handleSmsWebhook);
  app.post("/webhook/sms-status", handleSmsStatusWebhook);
  app.post("/webhook/email", handleEmailWebhook);
  app.get("/webhook/email-unsubscribe", handleEmailUnsubscribeWebhook);
  app.post("/webhook/twilio-gather", ctx.handleTwilioGatherWebhook);
  app.post("/webhook/sms-delivery", handleSmsDeliveryWebhook);
}

module.exports = { registerWebhookRoutes };
