function createGetCallStatusHandler(ctx = {}) {
  const {
    isSafeId,
    normalizeCallRecordForApi,
    buildDigitSummary,
    webhookService,
  } = ctx;

  return async function handleGetCallStatus(req, res) {
    try {
      const db =
        typeof ctx.getDb === "function"
          ? ctx.getDb()
          : ctx.db;
      const { callSid } = req.params;
      if (!isSafeId(callSid, { max: 128 })) {
        return res.status(400).json({ error: "Invalid call identifier" });
      }
      if (!db) {
        return res.status(500).json({ error: "Database unavailable" });
      }

      const call = await db.getCall(callSid);
      if (!call) {
        return res.status(404).json({ error: "Call not found" });
      }
      const normalizedCall = normalizeCallRecordForApi(call);
      if (!normalizedCall.digit_summary) {
        const digitEvents = await db.getCallDigits(callSid).catch(() => []);
        const digitSummary = buildDigitSummary(digitEvents);
        normalizedCall.digit_summary = digitSummary.summary;
        normalizedCall.digit_count = digitSummary.count;
      }

      const recentStates = await db.getCallStates(callSid, { limit: 15 });

      const notificationStatus = await new Promise((resolve, reject) => {
        db.db.all(
          `SELECT notification_type, status, created_at, sent_at, delivery_time_ms, error_message 
           FROM webhook_notifications 
           WHERE call_sid = ? 
           ORDER BY created_at DESC`,
          [callSid],
          (err, rows) => {
            if (err) reject(err);
            else resolve(rows || []);
          },
        );
      });

      let timingMetrics = {};
      if (normalizedCall.created_at) {
        const now = new Date();
        const created = new Date(normalizedCall.created_at);
        timingMetrics.total_elapsed = Math.round((now - created) / 1000);

        if (normalizedCall.started_at) {
          const started = new Date(normalizedCall.started_at);
          timingMetrics.time_to_answer = Math.round((started - created) / 1000);
        }

        if (normalizedCall.ended_at) {
          const ended = new Date(normalizedCall.ended_at);
          timingMetrics.call_duration =
            normalizedCall.duration ||
            Math.round(
              (ended -
                new Date(
                  normalizedCall.started_at || normalizedCall.created_at,
                )) /
                1000,
            );
        }

        if (normalizedCall.ring_duration) {
          timingMetrics.ring_duration = normalizedCall.ring_duration;
        }
      }

      return res.json({
        call: {
          ...normalizedCall,
          timing_metrics: timingMetrics,
        },
        recent_states: recentStates,
        notification_status: notificationStatus,
        webhook_service_status: webhookService.getCallStatusStats(),
        enhanced_tracking: true,
      });
    } catch (error) {
      console.error("Error fetching enhanced call status:", error);
      return res.status(500).json({ error: "Failed to fetch call status" });
    }
  };
}

function createSystemStatusHandler(ctx = {}) {
  const {
    getProviderReadiness,
    appVersion,
    getCurrentProvider,
    getCurrentSmsProvider,
    getCurrentEmailProvider,
    callConfigurations,
  } = ctx;

  return async function handleSystemStatus(req, res) {
    try {
      const readiness = getProviderReadiness();
      return res.json({
        status: "ok",
        timestamp: new Date().toISOString(),
        version: appVersion,
        active_provider: {
          call: getCurrentProvider(),
          sms: getCurrentSmsProvider(),
          email: getCurrentEmailProvider(),
        },
        providers: readiness,
        active_calls: callConfigurations.size,
      });
    } catch (error) {
      return res.status(500).json({
        status: "error",
        timestamp: new Date().toISOString(),
        error: "Failed to compute status",
      });
    }
  };
}

function createHealthHandler(ctx = {}) {
  const {
    config,
    verifyHmacSignature,
    hasAdminToken,
    webhookService,
    refreshInboundDefaultScript,
    getInboundHealthContext,
    supportedProviders,
    providerHealth,
    getProviderReadiness,
    isProviderDegraded,
    pruneExpiredKeypadProviderOverrides,
    keypadProviderOverrides,
    callConfigurations,
    functionEngine,
    callFunctionSystems,
  } = ctx;

  return async function handleHealth(req, res) {
    try {
      const db =
        typeof ctx.getDb === "function"
          ? ctx.getDb()
          : ctx.db;
      const hmacSecret = config.apiAuth?.hmacSecret;
      const hmacOk = hmacSecret ? verifyHmacSignature(req).ok : false;
      const adminOk = hasAdminToken(req);
      if (!hmacOk && !adminOk) {
        return res.json({
          status: "healthy",
          timestamp: new Date().toISOString(),
          public: true,
        });
      }
      if (!db) {
        return res.status(500).json({
          status: "unhealthy",
          timestamp: new Date().toISOString(),
          enhanced_features: true,
          error: "Database unavailable",
        });
      }

      const calls = await db.getCallsWithTranscripts(1);
      const webhookHealth = await webhookService.healthCheck();
      const callStats = webhookService.getCallStatusStats();
      const notificationMetrics = await db.getNotificationAnalytics(1);
      await refreshInboundDefaultScript();
      const { inboundDefaultSummary, inboundEnvSummary } =
        getInboundHealthContext();
      const providerHealthSummary = supportedProviders.reduce((acc, provider) => {
        const health = providerHealth.get(provider) || {};
        acc[provider] = {
          configured: Boolean(getProviderReadiness()[provider]),
          degraded: isProviderDegraded(provider),
          last_error_at: health.lastErrorAt || null,
          last_success_at: health.lastSuccessAt || null,
        };
        return acc;
      }, {});
      pruneExpiredKeypadProviderOverrides();
      const keypadOverrideSummary = [...keypadProviderOverrides.entries()].map(
        ([scopeKey, override]) => ({
          scope_key: scopeKey,
          provider: override?.provider || null,
          expires_at: override?.expiresAt
            ? new Date(override.expiresAt).toISOString()
            : null,
        }),
      );

      const recentHealthLogs = await new Promise((resolve, reject) => {
        db.db.all(
          `
          SELECT service_name, status, COUNT(*) as count
          FROM service_health_logs 
          WHERE timestamp >= datetime('now', '-1 hour')
          GROUP BY service_name, status
          ORDER BY service_name
        `,
          (err, rows) => {
            if (err) reject(err);
            else resolve(rows || []);
          },
        );
      });

      return res.json({
        status: "healthy",
        timestamp: new Date().toISOString(),
        enhanced_features: true,
        services: {
          database: {
            connected: true,
            recent_calls: calls.length,
          },
          webhook_service: webhookHealth,
          call_tracking: callStats,
          notification_system: {
            total_today: notificationMetrics.total_notifications,
            success_rate: notificationMetrics.overall_success_rate + "%",
            avg_delivery_time:
              notificationMetrics.breakdown.length > 0
                ? notificationMetrics.breakdown[0].avg_delivery_time + "ms"
                : "N/A",
          },
          provider_failover: providerHealthSummary,
          keypad_guard: {
            enabled: config.keypadGuard?.enabled === true,
            active_overrides: keypadOverrideSummary.length,
            overrides: keypadOverrideSummary,
          },
        },
        active_calls: callConfigurations.size,
        adaptation_engine: {
          available_scripts: functionEngine
            ? functionEngine.getBusinessAnalysis().availableTemplates.length
            : 0,
          active_function_systems: callFunctionSystems.size,
        },
        inbound_defaults: inboundDefaultSummary,
        inbound_env_defaults: inboundEnvSummary,
        system_health: recentHealthLogs,
      });
    } catch (error) {
      console.error("Enhanced health check error:", error);
      return res.status(500).json({
        status: "unhealthy",
        timestamp: new Date().toISOString(),
        enhanced_features: true,
        error: error.message,
        services: {
          database: {
            connected: false,
            error: error.message,
          },
          webhook_service: {
            status: "error",
            reason: "Database connection failed",
          },
        },
      });
    }
  };
}

function registerStatusRoutes(app, ctx = {}) {
  const handleGetCallStatus = createGetCallStatusHandler(ctx);
  const handleSystemStatus = createSystemStatusHandler(ctx);
  const handleHealth = createHealthHandler(ctx);

  app.get("/api/calls/:callSid/status", handleGetCallStatus);
  app.get("/status", handleSystemStatus);
  app.get("/health", handleHealth);
}

module.exports = { registerStatusRoutes };
