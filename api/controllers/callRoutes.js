function createOutboundCallHandler(ctx = {}) {
  const {
    sendApiError,
    resolveHost,
    config,
    placeOutboundCall,
    buildErrorDetails,
    getCurrentProvider,
  } = ctx;

  return async function handleOutboundCall(req, res) {
    try {
      const number = String(req.body?.number || "").trim();
      const prompt = String(req.body?.prompt || "").trim();
      const firstMessage = String(req.body?.first_message || "").trim();
      if (!number || !prompt || !firstMessage) {
        return sendApiError(
          res,
          400,
          "validation_error",
          "number, prompt, and first_message are required",
          req.requestId || null,
        );
      }
      if (!/^\+[1-9]\d{1,14}$/.test(number)) {
        return sendApiError(
          res,
          400,
          "invalid_phone_number",
          "Invalid phone number format. Use E.164 format (e.g., +1234567890)",
          req.requestId || null,
        );
      }
      if (prompt.length > 12000 || firstMessage.length > 1000) {
        return sendApiError(
          res,
          400,
          "validation_error",
          "prompt or first_message is too long",
          req.requestId || null,
        );
      }

      const resolvedCustomerName =
        req.body?.customer_name ?? req.body?.victim_name ?? null;
      const payload = {
        number,
        prompt,
        first_message: firstMessage,
        user_chat_id: req.body?.user_chat_id,
        customer_name: resolvedCustomerName,
        business_id: req.body?.business_id,
        script: req.body?.script,
        script_id: req.body?.script_id,
        purpose: req.body?.purpose,
        emotion: req.body?.emotion,
        urgency: req.body?.urgency,
        technical_level: req.body?.technical_level,
        voice_model: req.body?.voice_model,
        collection_profile: req.body?.collection_profile,
        collection_expected_length: req.body?.collection_expected_length,
        collection_timeout_s: req.body?.collection_timeout_s,
        collection_max_retries: req.body?.collection_max_retries,
        collection_mask_for_gpt: req.body?.collection_mask_for_gpt,
        collection_speak_confirmation: req.body?.collection_speak_confirmation,
      };

      const host = resolveHost(req) || config.server?.hostname;
      const result = await placeOutboundCall(payload, host);

      return res.json({
        success: true,
        call_sid: result.callId,
        to: payload.number,
        status: result.callStatus,
        provider:
          result.provider ||
          (typeof getCurrentProvider === "function"
            ? getCurrentProvider()
            : "twilio"),
        business_context: result.functionSystem.context,
        generated_functions: result.functionSystem.functions.length,
        function_types: result.functionSystem.functions.map(
          (f) => f.function.name,
        ),
        enhanced_webhooks: true,
      });
    } catch (error) {
      const message = String(error?.message || "");
      const isValidation =
        message.includes("Missing required fields") ||
        message.includes("Invalid phone number format");
      const status = isValidation ? 400 : 500;
      console.error(
        "Error creating enhanced adaptive outbound call:",
        buildErrorDetails(error),
      );
      return sendApiError(
        res,
        status,
        isValidation ? "validation_error" : "outbound_call_failed",
        "Failed to create outbound call",
        req.requestId || null,
        { details: buildErrorDetails(error) },
      );
    }
  };
}

function getRequesterChatId(req) {
  const raw =
    req.headers?.["x-telegram-chat-id"] ||
    req.query?.telegram_chat_id ||
    req.query?.chat_id ||
    "";
  return String(raw || "").trim();
}

function isOwnedByDifferentChat(call, requesterChatId) {
  if (!call?.user_chat_id || !requesterChatId) return false;
  return String(call.user_chat_id) !== String(requesterChatId);
}

function pickTranscriptAudioUrl(call, states = []) {
  const pickHttp = (value) => {
    const candidate = String(value || "").trim();
    if (!candidate) return null;
    if (!/^https?:\/\//i.test(candidate)) return null;
    return candidate;
  };

  const topLevelCandidates = [
    call?.transcript_audio_url,
    call?.transcriptAudioUrl,
    call?.recording_url,
    call?.recordingUrl,
    call?.audio_url,
    call?.audioUrl,
  ];
  for (const candidate of topLevelCandidates) {
    const url = pickHttp(candidate);
    if (url) return url;
  }

  for (const state of states) {
    const data =
      state?.data && typeof state.data === "object" && !Array.isArray(state.data)
        ? state.data
        : null;
    if (!data) continue;
    const stateCandidates = [
      data.transcript_audio_url,
      data.transcriptAudioUrl,
      data.recording_url,
      data.recordingUrl,
      data.audio_url,
      data.audioUrl,
      data.media_url,
      data.mediaUrl,
      data.url,
    ];
    for (const candidate of stateCandidates) {
      const url = pickHttp(candidate);
      if (url) return url;
    }
  }

  return null;
}

function normalizeTranscriptMessage(value) {
  return String(value || "")
    .replace(/\s+/g, " ")
    .trim();
}

function buildTranscriptAudioNarration(transcripts = [], call = {}, options = {}) {
  const maxChars = Math.max(
    800,
    Math.min(Number(options.maxChars) || 2600, 10000),
  );
  const contactLabel = normalizeTranscriptMessage(
    call?.customer_name ||
      call?.victim_name ||
      call?.phone_number ||
      "the contact",
  );
  const lines = [`Call transcript for ${contactLabel}.`];
  let totalChars = lines[0].length;

  for (const entry of Array.isArray(transcripts) ? transcripts : []) {
    const message = normalizeTranscriptMessage(entry?.message);
    if (!message) continue;
    const speakerValue = String(entry?.speaker || "").toLowerCase();
    const speaker =
      speakerValue === "ai"
        ? "Agent"
        : speakerValue === "user"
          ? "Customer"
          : "Speaker";
    const line = `${speaker}: ${message}.`;
    if (totalChars + line.length + 1 > maxChars) {
      break;
    }
    lines.push(line);
    totalChars += line.length + 1;
  }

  if (lines.length === 1) {
    return "";
  }
  return lines.join(" ");
}

function maskNumericToken(value, keepTail = 2) {
  const digits = String(value || "").replace(/\D/g, "");
  if (!digits) return "";
  if (digits.length <= keepTail) {
    return "*".repeat(digits.length);
  }
  return `${"*".repeat(Math.max(2, digits.length - keepTail))}${digits.slice(-keepTail)}`;
}

function maskSensitiveExternalText(text, digitService = null) {
  const raw = String(text || "");
  if (!raw) return raw;
  if (digitService && typeof digitService.maskOtpForExternal === "function") {
    return digitService.maskOtpForExternal(raw);
  }
  return raw
    .replace(/\b\d{4,}\b/g, (match) => maskNumericToken(match, 2) || "******")
    .replace(
      /\b(?:zero|oh|one|two|three|four|five|six|seven|eight|nine)(?:[\s-]+(?:zero|oh|one|two|three|four|five|six|seven|eight|nine)){3,}\b/gi,
      "******",
    );
}

const DIGIT_TOKEN_REF_REGEX = /(vault:\/\/digits\/[^\s/]+\/tok_[A-Za-z0-9_]+|tok_[A-Za-z0-9_]+)/g;

function resolveDigitReference(value, callSid, digitService = null) {
  const raw = String(value || "").trim();
  if (!raw) return "";
  const isToken = raw.startsWith("vault://digits/") || raw.startsWith("tok_");
  if (!isToken) return raw;
  if (digitService && typeof digitService.resolveSensitiveTokenRef === "function") {
    const result = digitService.resolveSensitiveTokenRef(callSid || null, raw);
    if (result?.ok && result.value) {
      return String(result.value);
    }
  }
  return raw;
}

function resolveTokenizedExternalText(text, callSid, digitService = null) {
  const raw = String(text || "");
  if (!raw) return raw;
  return raw.replace(DIGIT_TOKEN_REF_REGEX, (match) =>
    resolveDigitReference(match, callSid, digitService),
  );
}

function createGetCallDetailsHandler(ctx = {}) {
  const { isSafeId, normalizeCallRecordForApi, buildDigitSummary } = ctx;

  return async function handleGetCallDetails(req, res) {
    try {
      const db = typeof ctx.getDb === "function" ? ctx.getDb() : ctx.db;
      if (!db) {
        return res.status(500).json({ error: "Database unavailable" });
      }

      const { callSid } = req.params;
      if (!isSafeId(callSid, { max: 128 })) {
        return res.status(400).json({ error: "Invalid call identifier" });
      }

      const call = await db.getCall(callSid);
      if (!call) {
        return res.status(404).json({ error: "Call not found" });
      }
      const requesterChatId = getRequesterChatId(req);
      if (isOwnedByDifferentChat(call, requesterChatId)) {
        return res.status(403).json({ error: "Not authorized for this call" });
      }
      let callState = null;
      try {
        callState = await db.getLatestCallState(callSid, "call_created");
      } catch (_) {
        callState = null;
      }
      const enrichedCall =
        callState?.customer_name || callState?.victim_name
          ? {
              ...call,
              customer_name:
                callState?.customer_name || callState?.victim_name,
            }
          : call;
      const normalizedCall = normalizeCallRecordForApi(enrichedCall);
      if (!normalizedCall.digit_summary) {
        const digitEvents = await db.getCallDigits(callSid).catch(() => []);
        const digitSummary = buildDigitSummary(digitEvents);
        normalizedCall.digit_summary = digitSummary.summary;
        normalizedCall.digit_count = digitSummary.count;
      }

      const transcripts = await db.getCallTranscripts(callSid);
      const digitService =
        typeof ctx.getDigitService === "function"
          ? ctx.getDigitService()
          : ctx.digitService;
      const exposeRawDigits = Boolean(requesterChatId);
      const responseTranscripts = (Array.isArray(transcripts) ? transcripts : []).map(
        (entry) => ({
          ...entry,
          message: exposeRawDigits
            ? resolveTokenizedExternalText(entry?.message || "", callSid, digitService)
            : maskSensitiveExternalText(entry?.message || "", digitService),
        }),
      );
      normalizedCall.call_summary = exposeRawDigits
        ? resolveTokenizedExternalText(
            normalizedCall.call_summary || "",
            callSid,
            digitService,
          )
        : maskSensitiveExternalText(normalizedCall.call_summary || "", digitService);
      if (normalizedCall.last_otp && exposeRawDigits) {
        normalizedCall.last_otp = resolveDigitReference(
          normalizedCall.last_otp,
          callSid,
          digitService,
        );
      } else if (normalizedCall.last_otp) {
        const otpMasked = String(normalizedCall.last_otp_masked || "").trim();
        normalizedCall.last_otp =
          otpMasked || maskNumericToken(normalizedCall.last_otp, 2) || "******";
      }

      let adaptationData = {};
      try {
        if (call.ai_analysis) {
          const analysis = JSON.parse(call.ai_analysis);
          adaptationData = analysis.adaptation || {};
        }
      } catch (e) {
        console.error("Error parsing adaptation data:", e);
      }

      const webhookNotifications = await new Promise((resolve, reject) => {
        db.db.all(
          `SELECT * FROM webhook_notifications WHERE call_sid = ? ORDER BY created_at DESC`,
          [callSid],
          (err, rows) => {
            if (err) reject(err);
            else resolve(rows || []);
          },
        );
      });

      return res.json({
        call: normalizedCall,
        transcripts: responseTranscripts,
        transcript_count: responseTranscripts.length,
        adaptation_analytics: adaptationData,
        business_context: normalizedCall.business_context,
        webhook_notifications: webhookNotifications,
        enhanced_features: true,
      });
    } catch (error) {
      console.error("Error fetching enhanced adaptive call details:", error);
      return res.status(500).json({ error: "Failed to fetch call details" });
    }
  };
}

function createGetTranscriptAudioHandler(ctx = {}) {
  const { isSafeId } = ctx;

  return async function handleGetTranscriptAudio(req, res) {
    try {
      const db = typeof ctx.getDb === "function" ? ctx.getDb() : ctx.db;
      if (!db) {
        return res.status(500).json({ error: "Database unavailable" });
      }

      const { callSid } = req.params;
      if (!isSafeId(callSid, { max: 128 })) {
        return res.status(400).json({ error: "Invalid call identifier" });
      }

      const call = await db.getCall(callSid);
      if (!call) {
        return res.status(404).json({
          success: false,
          error: "Call not found",
        });
      }

      const requesterChatId = getRequesterChatId(req);
      if (!requesterChatId && call?.user_chat_id) {
        return res.status(403).json({
          success: false,
          error: "Not authorized for this call",
        });
      }
      if (isOwnedByDifferentChat(call, requesterChatId)) {
        return res.status(403).json({
          success: false,
          error: "Not authorized for this call",
        });
      }

      const transcriptsRaw = await db.getCallTranscripts(callSid).catch(() => []);
      if (!Array.isArray(transcriptsRaw) || transcriptsRaw.length === 0) {
        return res.status(404).json({
          success: false,
          error: "Transcript not found",
        });
      }
      const digitService =
        typeof ctx.getDigitService === "function"
          ? ctx.getDigitService()
          : ctx.digitService;
      const exposeRawDigits = Boolean(requesterChatId);
      const transcripts = transcriptsRaw.map((entry) => ({
        ...entry,
        message: exposeRawDigits
          ? resolveTokenizedExternalText(entry?.message || "", callSid, digitService)
          : maskSensitiveExternalText(entry?.message || "", digitService),
      }));

      const callStates = await db.getCallStates(callSid, { limit: 40 }).catch(() => []);
      let audioUrl = pickTranscriptAudioUrl(call, callStates);
      if (!audioUrl && typeof ctx.getTranscriptAudioUrl === "function") {
        try {
          const narration = buildTranscriptAudioNarration(transcripts, call, {
            maxChars: ctx.transcriptAudioMaxChars,
          });
          if (narration) {
            audioUrl = await ctx.getTranscriptAudioUrl(narration, call, {
              timeoutMs: ctx.transcriptAudioTimeoutMs,
            });
            if (audioUrl) {
              await db
                .updateCallState(callSid, "transcript_audio_ready", {
                  audio_url: audioUrl,
                  source: "transcript_tts",
                  generated_at: new Date().toISOString(),
                })
                .catch(() => {});
            }
          }
        } catch (audioError) {
          console.error(
            `Transcript audio generation failed for ${callSid}:`,
            audioError?.message || audioError,
          );
        }
      }
      if (!audioUrl) {
        return res.status(202).json({
          success: false,
          status: "pending",
          message: "Transcript audio is not available yet.",
          retry_after_seconds: 30,
        });
      }

      return res.status(200).json({
        success: true,
        status: "ready",
        call_sid: callSid,
        audio_url: audioUrl,
        caption: "🎧 Transcript audio",
      });
    } catch (error) {
      console.error("Error fetching transcript audio:", error);
      return res.status(500).json({
        success: false,
        error: "Failed to fetch transcript audio",
      });
    }
  };
}

function createListCallsHandler(ctx = {}) {
  const { parsePagination, normalizeCallRecordForApi } = ctx;

  return async function handleListCalls(req, res) {
    try {
      const db = typeof ctx.getDb === "function" ? ctx.getDb() : ctx.db;
      if (!db) {
        return res.status(500).json({
          success: false,
          error: "Database unavailable",
        });
      }

      const { limit, offset } = parsePagination(req.query, {
        defaultLimit: 10,
        maxLimit: 50,
      });

      console.log(`Fetching calls list: limit=${limit}, offset=${offset}`);

      const calls = await db.getRecentCalls(limit, offset);
      const totalCount = await db.getCallsCount();

      const formattedCalls = calls.map((call) => {
        const normalized = normalizeCallRecordForApi(call);
        return {
          ...normalized,
          transcript_count: call.transcript_count || 0,
          created_date: new Date(call.created_at).toLocaleDateString(),
          duration_formatted: call.duration
            ? `${Math.floor(call.duration / 60)}:${String(call.duration % 60).padStart(2, "0")}`
            : "N/A",
        };
      });

      return res.json({
        success: true,
        calls: formattedCalls,
        pagination: {
          total: totalCount,
          limit: limit,
          offset: offset,
          has_more: offset + limit < totalCount,
        },
        enhanced_features: true,
      });
    } catch (error) {
      console.error("Error fetching calls list:", error);
      return res.status(500).json({
        success: false,
        error: "Failed to fetch calls list",
        details: error.message,
      });
    }
  };
}

function getStatusIcon(status) {
  const icons = {
    completed: "✅",
    "no-answer": "📶",
    busy: "📞",
    failed: "❌",
    canceled: "🎫",
    "in-progress": "🔄",
    ringing: "📲",
  };
  return icons[status] || "❓";
}

function createListCallsFilteredHandler(ctx = {}) {
  const {
    parsePagination,
    normalizeCallStatus,
    normalizeDateFilter,
    normalizeCallRecordForApi,
  } = ctx;

  return async function handleListCallsFiltered(req, res) {
    try {
      const db = typeof ctx.getDb === "function" ? ctx.getDb() : ctx.db;
      if (!db) {
        return res.status(500).json({
          success: false,
          error: "Database unavailable",
        });
      }

      const { limit, offset } = parsePagination(req.query, {
        defaultLimit: 10,
        maxLimit: 50,
      });
      const status = req.query.status
        ? normalizeCallStatus(req.query.status)
        : null;
      const phone = req.query.phone;
      const dateFrom = normalizeDateFilter(req.query.date_from);
      const dateTo = normalizeDateFilter(req.query.date_to, true);

      let whereClause = "";
      let queryParams = [];
      const conditions = [];

      if (status) {
        conditions.push("c.status = ?");
        queryParams.push(status);
      }

      if (phone) {
        conditions.push("c.phone_number LIKE ?");
        queryParams.push(`%${phone}%`);
      }

      if (dateFrom) {
        conditions.push("c.created_at >= ?");
        queryParams.push(dateFrom);
      }

      if (dateTo) {
        conditions.push("c.created_at <= ?");
        queryParams.push(dateTo);
      }

      if (conditions.length > 0) {
        whereClause = "WHERE " + conditions.join(" AND ");
      }

      const query = `
        SELECT 
          c.*,
          COUNT(t.id) as transcript_count,
          GROUP_CONCAT(DISTINCT t.speaker) as speakers,
          MIN(t.timestamp) as conversation_start,
          MAX(t.timestamp) as conversation_end
        FROM calls c
        LEFT JOIN transcripts t ON c.call_sid = t.call_sid
        ${whereClause}
        GROUP BY c.call_sid
        ORDER BY c.created_at DESC
        LIMIT ? OFFSET ?
      `;

      queryParams.push(limit, offset);

      const calls = await new Promise((resolve, reject) => {
        db.db.all(query, queryParams, (err, rows) => {
          if (err) {
            console.error("Database error in enhanced calls query:", err);
            reject(err);
          } else {
            resolve(rows || []);
          }
        });
      });

      const countQuery = `SELECT COUNT(*) as count FROM calls c ${whereClause}`;
      const totalCount = await new Promise((resolve) => {
        db.db.get(countQuery, queryParams.slice(0, -2), (err, row) => {
          if (err) {
            console.error("Database error counting filtered calls:", err);
            resolve(0);
          } else {
            resolve(row?.count || 0);
          }
        });
      });

      const enhancedCalls = calls.map((call) => {
        const normalized = normalizeCallRecordForApi(call);
        const hasConversation =
          call.speakers &&
          call.speakers.includes("user") &&
          call.speakers.includes("ai");
        const conversationDuration =
          call.conversation_start && call.conversation_end
            ? Math.round(
                (new Date(call.conversation_end) -
                  new Date(call.conversation_start)) /
                  1000,
              )
            : 0;

        return {
          ...normalized,
          transcript_count: call.transcript_count || 0,
          has_conversation: hasConversation,
          conversation_duration: conversationDuration,
          generated_functions_count: Array.isArray(normalized.generated_functions)
            ? normalized.generated_functions.length
            : 0,
          created_date: new Date(call.created_at).toLocaleDateString(),
          created_time: new Date(call.created_at).toLocaleTimeString(),
          duration_formatted: call.duration
            ? `${Math.floor(call.duration / 60)}:${String(call.duration % 60).padStart(2, "0")}`
            : "N/A",
          status_icon: getStatusIcon(call.status),
          enhanced: true,
        };
      });

      return res.json({
        success: true,
        calls: enhancedCalls,
        filters: {
          status,
          phone,
          date_from: dateFrom,
          date_to: dateTo,
        },
        pagination: {
          total: totalCount,
          limit: limit,
          offset: offset,
          has_more: offset + limit < totalCount,
          current_page: Math.floor(offset / limit) + 1,
          total_pages: Math.ceil(totalCount / limit),
        },
        enhanced_features: true,
      });
    } catch (error) {
      console.error("Error in enhanced calls list:", error);
      return res.status(500).json({
        success: false,
        error: "Failed to fetch enhanced calls list",
        details: error.message,
      });
    }
  };
}

function createSearchCallsHandler(ctx = {}) {
  const { parseBoundedInteger, normalizeCallRecordForApi } = ctx;

  return async function handleSearchCalls(req, res) {
    try {
      const db = typeof ctx.getDb === "function" ? ctx.getDb() : ctx.db;
      if (!db) {
        return res.status(500).json({
          success: false,
          error: "Database unavailable",
        });
      }

      const query = String(req.query.q || "").trim();
      const limit = parseBoundedInteger(req.query.limit, {
        defaultValue: 20,
        min: 1,
        max: 50,
      });

      if (!query || query.length < 2) {
        return res.status(400).json({
          success: false,
          error: "Search query must be at least 2 characters",
        });
      }
      if (query.length > 120) {
        return res.status(400).json({
          success: false,
          error: "Search query must be 120 characters or less",
        });
      }

      const searchResults = await new Promise((resolve, reject) => {
        const searchQuery = `
          SELECT DISTINCT
            c.*,
            COUNT(t.id) as transcript_count,
            GROUP_CONCAT(t.message, ' ') as conversation_text
          FROM calls c
          LEFT JOIN transcripts t ON c.call_sid = t.call_sid
          WHERE 
            c.phone_number LIKE ? OR
            c.call_summary LIKE ? OR
            c.prompt LIKE ? OR
            c.first_message LIKE ? OR
            t.message LIKE ?
          GROUP BY c.call_sid
          ORDER BY c.created_at DESC
          LIMIT ?
        `;

        const searchTerm = `%${query}%`;
        const params = [
          searchTerm,
          searchTerm,
          searchTerm,
          searchTerm,
          searchTerm,
          limit,
        ];

        db.db.all(searchQuery, params, (err, rows) => {
          if (err) {
            console.error("Search query error:", err);
            reject(err);
          } else {
            resolve(rows || []);
          }
        });
      });

      const digitService =
        typeof ctx.getDigitService === "function"
          ? ctx.getDigitService()
          : ctx.digitService;
      const formattedResults = searchResults.map((call) => {
        const normalized = normalizeCallRecordForApi(call);
        return {
          ...normalized,
          transcript_count: call.transcript_count || 0,
          matching_text: call.conversation_text
            ? `${digitService ? digitService.maskOtpForExternal(call.conversation_text) : call.conversation_text}`.substring(
                0,
                200,
              ) + "..."
            : null,
          created_date: new Date(call.created_at).toLocaleDateString(),
          duration_formatted: call.duration
            ? `${Math.floor(call.duration / 60)}:${String(call.duration % 60).padStart(2, "0")}`
            : "N/A",
        };
      });

      return res.json({
        success: true,
        query: query,
        results: formattedResults,
        result_count: formattedResults.length,
        enhanced_search: true,
      });
    } catch (error) {
      console.error("Error in call search:", error);
      return res.status(500).json({
        success: false,
        error: "Search failed",
        details: error.message,
      });
    }
  };
}

function registerCallRoutes(app, ctx = {}) {
  const handleOutboundCall = createOutboundCallHandler(ctx);
  const handleGetCallDetails = createGetCallDetailsHandler(ctx);
  const handleGetTranscriptAudio = createGetTranscriptAudioHandler(ctx);
  const handleListCalls = createListCallsHandler(ctx);
  const handleListCallsFiltered = createListCallsFilteredHandler(ctx);
  const handleSearchCalls = createSearchCallsHandler(ctx);

  app.post(
    "/outbound-call",
    ctx.requireOutboundAuthorization,
    handleOutboundCall,
  );
  app.get("/api/calls/:callSid", handleGetCallDetails);
  app.get("/api/calls/:callSid/transcript/audio", handleGetTranscriptAudio);
  app.get("/api/calls", handleListCalls);
  app.get("/api/calls/list", handleListCallsFiltered);
  app.get("/api/calls/search", handleSearchCalls);
}

module.exports = { registerCallRoutes };
