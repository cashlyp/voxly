const RELATIONSHIP_PROFILE_TYPES = new Set([
  "dating",
  "celebrity",
  "fan",
  "creator",
  "friendship",
  "networking",
  "community",
  "marketplace_seller",
  "real_estate_agent",
]);

const CONNECTOR_REGISTRY = Object.freeze({
  confirm_identity: {
    id: "identity_verification",
    class: "side_effect",
    risk_class: "medium",
    capability_tags: ["identity", "verification", "compliance"],
    default_policy: { timeout_ms: 9000, retry_limit: 0, circuit_group: "identity" },
    approval: { required: false, mode: "none" },
  },
  route_to_agent: {
    id: "human_handoff",
    class: "side_effect",
    risk_class: "medium",
    capability_tags: ["handoff", "escalation", "support"],
    default_policy: { timeout_ms: 8000, retry_limit: 0, circuit_group: "handoff" },
    approval: { required: false, mode: "none" },
  },
  collect_digits: {
    id: "digit_capture",
    class: "capture",
    risk_class: "medium",
    capability_tags: ["capture", "identity", "verification", "payment"],
    default_policy: { timeout_ms: 7000, retry_limit: 0, circuit_group: "capture" },
    approval: { required: false, mode: "none" },
  },
  collect_multiple_digits: {
    id: "digit_capture_plan",
    class: "capture",
    risk_class: "medium",
    capability_tags: ["capture", "identity", "verification", "payment"],
    default_policy: { timeout_ms: 9000, retry_limit: 0, circuit_group: "capture" },
    approval: { required: false, mode: "none" },
  },
  play_disclosure: {
    id: "policy_disclosure",
    class: "read",
    risk_class: "low",
    capability_tags: ["disclosure", "compliance", "general"],
    default_policy: { timeout_ms: 6000, retry_limit: 0, circuit_group: "compliance" },
    approval: { required: false, mode: "none" },
  },
  start_payment: {
    id: "payment_session",
    class: "side_effect",
    risk_class: "high",
    capability_tags: ["payment", "billing", "capture"],
    default_policy: { timeout_ms: 14000, retry_limit: 0, circuit_group: "payment" },
    approval: { required: true, mode: "explicit" },
  },
  web_search: {
    id: "deep_research_web_search",
    class: "read",
    risk_class: "low",
    capability_tags: ["research", "web", "knowledge"],
    default_policy: { timeout_ms: 9000, retry_limit: 1, circuit_group: "research" },
    approval: { required: false, mode: "none" },
  },
  docs_lookup: {
    id: "deep_research_docs_lookup",
    class: "read",
    risk_class: "low",
    capability_tags: ["research", "knowledge", "docs"],
    default_policy: { timeout_ms: 9000, retry_limit: 1, circuit_group: "research" },
    approval: { required: false, mode: "none" },
  },
  news_lookup: {
    id: "deep_research_news_lookup",
    class: "read",
    risk_class: "low",
    capability_tags: ["research", "news", "knowledge"],
    default_policy: { timeout_ms: 9000, retry_limit: 1, circuit_group: "research" },
    approval: { required: false, mode: "none" },
  },
  site_crawl_summary: {
    id: "deep_research_site_crawl",
    class: "read",
    risk_class: "low",
    capability_tags: ["research", "web", "knowledge"],
    default_policy: { timeout_ms: 10000, retry_limit: 1, circuit_group: "research" },
    approval: { required: false, mode: "none" },
  },
  crm_lookup_account: {
    id: "business_crm_lookup",
    class: "read",
    risk_class: "medium",
    capability_tags: ["crm", "business_ops", "support"],
    default_policy: { timeout_ms: 9000, retry_limit: 0, circuit_group: "business_ops" },
    approval: { required: false, mode: "none" },
  },
  crm_create_lead: {
    id: "business_crm_create_lead",
    class: "side_effect",
    risk_class: "medium",
    capability_tags: ["crm", "business_ops", "sales"],
    default_policy: { timeout_ms: 10000, retry_limit: 0, circuit_group: "business_ops" },
    approval: { required: false, mode: "none" },
  },
  ticket_create: {
    id: "business_ticket_create",
    class: "side_effect",
    risk_class: "medium",
    capability_tags: ["ticketing", "business_ops", "support"],
    default_policy: { timeout_ms: 10000, retry_limit: 0, circuit_group: "business_ops" },
    approval: { required: false, mode: "none" },
  },
  calendar_check_availability: {
    id: "business_calendar_availability",
    class: "read",
    risk_class: "low",
    capability_tags: ["calendar", "scheduling", "business_ops"],
    default_policy: { timeout_ms: 9000, retry_limit: 0, circuit_group: "business_ops" },
    approval: { required: false, mode: "none" },
  },
  order_lookup_status: {
    id: "business_order_lookup",
    class: "read",
    risk_class: "low",
    capability_tags: ["erp", "orders", "business_ops", "support"],
    default_policy: { timeout_ms: 9000, retry_limit: 0, circuit_group: "business_ops" },
    approval: { required: false, mode: "none" },
  },
  payment_link_generate: {
    id: "billing_payment_link",
    class: "side_effect",
    risk_class: "high",
    capability_tags: ["payment", "billing", "business_ops"],
    default_policy: { timeout_ms: 14000, retry_limit: 0, circuit_group: "payment" },
    approval: { required: true, mode: "explicit" },
  },
  invoice_create: {
    id: "billing_invoice_create",
    class: "side_effect",
    risk_class: "high",
    capability_tags: ["payment", "billing", "business_ops"],
    default_policy: { timeout_ms: 14000, retry_limit: 0, circuit_group: "payment" },
    approval: { required: true, mode: "explicit" },
  },
  payment_intent_status: {
    id: "billing_payment_intent_status",
    class: "read",
    risk_class: "medium",
    capability_tags: ["payment", "billing", "business_ops"],
    default_policy: { timeout_ms: 10000, retry_limit: 0, circuit_group: "payment" },
    approval: { required: false, mode: "none" },
  },
  refund_request_initiate: {
    id: "billing_refund_request",
    class: "side_effect",
    risk_class: "high",
    capability_tags: ["payment", "billing", "risk", "business_ops"],
    default_policy: { timeout_ms: 14000, retry_limit: 0, circuit_group: "payment" },
    approval: { required: true, mode: "explicit" },
  },
  kyc_verify_identity: {
    id: "risk_kyc_verify",
    class: "read",
    risk_class: "medium",
    capability_tags: ["risk", "identity", "verification"],
    default_policy: { timeout_ms: 10000, retry_limit: 0, circuit_group: "risk" },
    approval: { required: false, mode: "none" },
  },
  fraud_score_assess: {
    id: "risk_fraud_score",
    class: "read",
    risk_class: "high",
    capability_tags: ["risk", "fraud", "verification"],
    default_policy: { timeout_ms: 10000, retry_limit: 0, circuit_group: "risk" },
    approval: { required: true, mode: "explicit" },
  },
  velocity_check: {
    id: "risk_velocity_check",
    class: "read",
    risk_class: "medium",
    capability_tags: ["risk", "fraud", "verification"],
    default_policy: { timeout_ms: 9000, retry_limit: 0, circuit_group: "risk" },
    approval: { required: false, mode: "none" },
  },
  risk_list_check: {
    id: "risk_list_check",
    class: "read",
    risk_class: "medium",
    capability_tags: ["risk", "fraud", "verification"],
    default_policy: { timeout_ms: 9000, retry_limit: 0, circuit_group: "risk" },
    approval: { required: false, mode: "none" },
  },
  kb_search: {
    id: "knowledge_kb_search",
    class: "read",
    risk_class: "low",
    capability_tags: ["knowledge", "kb", "support"],
    default_policy: { timeout_ms: 9000, retry_limit: 0, circuit_group: "knowledge" },
    approval: { required: false, mode: "none" },
  },
  runbook_lookup: {
    id: "knowledge_runbook_lookup",
    class: "read",
    risk_class: "low",
    capability_tags: ["knowledge", "runbook", "support"],
    default_policy: { timeout_ms: 9000, retry_limit: 0, circuit_group: "knowledge" },
    approval: { required: false, mode: "none" },
  },
  policy_lookup: {
    id: "knowledge_policy_lookup",
    class: "read",
    risk_class: "medium",
    capability_tags: ["knowledge", "policy", "compliance"],
    default_policy: { timeout_ms: 9000, retry_limit: 0, circuit_group: "knowledge" },
    approval: { required: false, mode: "none" },
  },
  product_doc_lookup: {
    id: "knowledge_product_docs",
    class: "read",
    risk_class: "low",
    capability_tags: ["knowledge", "docs", "support"],
    default_policy: { timeout_ms: 9000, retry_limit: 0, circuit_group: "knowledge" },
    approval: { required: false, mode: "none" },
  },
  contract_lookup: {
    id: "knowledge_contract_lookup",
    class: "read",
    risk_class: "medium",
    capability_tags: ["knowledge", "contract", "compliance"],
    default_policy: { timeout_ms: 9000, retry_limit: 0, circuit_group: "knowledge" },
    approval: { required: false, mode: "none" },
  },
});

function normalizeToolName(value = "") {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_]/g, "_");
}

function normalizeCapabilityTag(value = "") {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_]/g, "_");
}

function normalizeApprovalMode(value = "disabled") {
  const normalized = String(value || "").trim().toLowerCase();
  if (["disabled", "high_risk", "required"].includes(normalized)) {
    return normalized;
  }
  return "disabled";
}

function deriveDescriptorFromPattern(toolName = "", fallbackClass = "read") {
  const normalized = normalizeToolName(toolName);
  if (!normalized) {
    return null;
  }

  if (normalized.startsWith("set_") && normalized.endsWith("_context")) {
    return {
      id: `${normalized}_context_writer`,
      class: "side_effect",
      risk_class: "low",
      capability_tags: ["relationship_context", "profile", "general"],
      default_policy: { timeout_ms: 7000, retry_limit: 0, circuit_group: "profile_context" },
      approval: { required: false, mode: "none" },
    };
  }
  if (normalized.startsWith("get_") && normalized.endsWith("_context")) {
    return {
      id: `${normalized}_context_reader`,
      class: "read",
      risk_class: "low",
      capability_tags: ["relationship_context", "profile", "general"],
      default_policy: { timeout_ms: 6000, retry_limit: 1, circuit_group: "profile_context" },
      approval: { required: false, mode: "none" },
    };
  }
  if (
    normalized.includes("payment") ||
    normalized.includes("charge") ||
    normalized.includes("invoice") ||
    normalized.includes("refund")
  ) {
    return {
      id: `${normalized}_payment`,
      class: "side_effect",
      risk_class: "high",
      capability_tags: ["payment", "billing"],
      default_policy: { timeout_ms: 14000, retry_limit: 0, circuit_group: "payment" },
      approval: { required: true, mode: "explicit" },
    };
  }
  if (
    normalized.includes("search") ||
    normalized.includes("lookup") ||
    normalized.includes("research")
  ) {
    return {
      id: `${normalized}_research`,
      class: "read",
      risk_class: "low",
      capability_tags: ["research", "knowledge"],
      default_policy: { timeout_ms: 9000, retry_limit: 1, circuit_group: "research" },
      approval: { required: false, mode: "none" },
    };
  }
  if (
    normalized.includes("crm") ||
    normalized.includes("ticket") ||
    normalized.includes("calendar") ||
    normalized.includes("order")
  ) {
    return {
      id: `${normalized}_business`,
      class: normalized.includes("create") || normalized.includes("update")
        ? "side_effect"
        : "read",
      risk_class: normalized.includes("create") || normalized.includes("update")
        ? "medium"
        : "low",
      capability_tags: ["business_ops", "crm", "support"],
      default_policy: { timeout_ms: 10000, retry_limit: 0, circuit_group: "business_ops" },
      approval: { required: false, mode: "none" },
    };
  }
  if (
    normalized.includes("kyc") ||
    normalized.includes("fraud") ||
    normalized.includes("velocity") ||
    normalized.includes("risk")
  ) {
    return {
      id: `${normalized}_risk`,
      class: "read",
      risk_class: normalized.includes("fraud") ? "high" : "medium",
      capability_tags: ["risk", "fraud", "verification"],
      default_policy: { timeout_ms: 10000, retry_limit: 0, circuit_group: "risk" },
      approval: { required: normalized.includes("fraud"), mode: "explicit" },
    };
  }
  if (
    normalized.includes("kb") ||
    normalized.includes("runbook") ||
    normalized.includes("policy") ||
    normalized.includes("contract") ||
    normalized.includes("doc")
  ) {
    return {
      id: `${normalized}_knowledge`,
      class: "read",
      risk_class: normalized.includes("contract") || normalized.includes("policy")
        ? "medium"
        : "low",
      capability_tags: ["knowledge", "docs", "support"],
      default_policy: { timeout_ms: 9000, retry_limit: 0, circuit_group: "knowledge" },
      approval: { required: false, mode: "none" },
    };
  }
  if (
    normalized.includes("route") ||
    normalized.includes("transfer") ||
    normalized.includes("handoff")
  ) {
    return {
      id: `${normalized}_handoff`,
      class: "side_effect",
      risk_class: "medium",
      capability_tags: ["handoff", "escalation", "support"],
      default_policy: { timeout_ms: 8000, retry_limit: 0, circuit_group: "handoff" },
      approval: { required: false, mode: "none" },
    };
  }

  return {
    id: `${normalized}_runtime`,
    class: fallbackClass === "side_effect" ? "side_effect" : "read",
    risk_class: fallbackClass === "side_effect" ? "medium" : "low",
    capability_tags: ["general"],
    default_policy: {
      timeout_ms: fallbackClass === "side_effect" ? 9000 : 7000,
      retry_limit: fallbackClass === "side_effect" ? 0 : 1,
      circuit_group: "runtime",
    },
    approval: { required: false, mode: "none" },
  };
}

function cloneDescriptor(descriptor = {}) {
  return {
    ...descriptor,
    capability_tags: Array.isArray(descriptor.capability_tags)
      ? descriptor.capability_tags.map((entry) => normalizeCapabilityTag(entry)).filter(Boolean)
      : [],
    default_policy:
      descriptor.default_policy && typeof descriptor.default_policy === "object"
        ? { ...descriptor.default_policy }
        : {},
    approval:
      descriptor.approval && typeof descriptor.approval === "object"
        ? { ...descriptor.approval }
        : { required: false, mode: "none" },
  };
}

function getConnectorDescriptor(toolName = "", fallbackClass = "read") {
  const normalized = normalizeToolName(toolName);
  if (!normalized) return null;
  if (CONNECTOR_REGISTRY[normalized]) {
    return cloneDescriptor(CONNECTOR_REGISTRY[normalized]);
  }
  return cloneDescriptor(deriveDescriptorFromPattern(normalized, fallbackClass));
}

function mergeConnectorMetadata(existing = {}, descriptor = {}, toolName = "") {
  const normalizedToolName = normalizeToolName(toolName);
  const merged = {
    id: String(existing.id || descriptor.id || normalizedToolName || "runtime").trim(),
    class: String(existing.class || descriptor.class || "read").trim().toLowerCase(),
    risk_class: String(existing.risk_class || descriptor.risk_class || "low")
      .trim()
      .toLowerCase(),
    capability_tags: Array.from(
      new Set([
        ...(Array.isArray(descriptor.capability_tags) ? descriptor.capability_tags : []),
        ...(Array.isArray(existing.capability_tags) ? existing.capability_tags : []),
      ]),
    )
      .map((entry) => normalizeCapabilityTag(entry))
      .filter(Boolean),
    timeout_ms:
      Number(existing.timeout_ms ?? existing.timeoutMs) ||
      Number(descriptor.default_policy?.timeout_ms) ||
      null,
    retry_limit:
      Number(existing.retry_limit ?? existing.retryLimit) ||
      Number(descriptor.default_policy?.retry_limit) ||
      null,
    circuit_group:
      String(existing.circuit_group || existing.circuitGroup || descriptor.default_policy?.circuit_group || "runtime")
        .trim()
        .toLowerCase(),
    approval:
      existing.approval && typeof existing.approval === "object"
        ? {
            required: existing.approval.required === true,
            mode: String(existing.approval.mode || descriptor.approval?.mode || "none")
              .trim()
              .toLowerCase(),
          }
        : {
            required: descriptor.approval?.required === true,
            mode: String(descriptor.approval?.mode || "none").trim().toLowerCase(),
          },
  };

  if (!["read", "capture", "side_effect"].includes(merged.class)) {
    merged.class = descriptor.class || "read";
  }
  if (!["low", "medium", "high"].includes(merged.risk_class)) {
    merged.risk_class = "low";
  }
  return merged;
}

function attachConnectorMetadataToTools(tools = []) {
  const input = Array.isArray(tools) ? tools : [];
  return input
    .filter((tool) => tool && tool.type === "function" && tool.function?.name)
    .map((tool) => {
      const fn = { ...tool.function };
      const descriptor = getConnectorDescriptor(
        fn.name,
        fn.permission === "write" ? "side_effect" : "read",
      );
      const existingConnector =
        (fn.connector && typeof fn.connector === "object" ? fn.connector : null) ||
        (tool.connector && typeof tool.connector === "object" ? tool.connector : null) ||
        {};
      const connector = mergeConnectorMetadata(existingConnector, descriptor || {}, fn.name);
      return {
        ...tool,
        connector,
        function: {
          ...fn,
          connector,
        },
      };
    });
}

function deriveIntentEnvelope(callConfig = {}, options = {}) {
  const normalizedConfig =
    callConfig && typeof callConfig === "object" ? callConfig : {};
  const profile = normalizeToolName(
    options.conversationProfile ||
      normalizedConfig.conversation_profile ||
      normalizedConfig.call_profile ||
      normalizedConfig.purpose ||
      "general",
  );
  const purposeText = [
    normalizedConfig.purpose,
    normalizedConfig.call_profile,
    normalizedConfig.conversation_profile,
    normalizedConfig.script,
    normalizedConfig.script_name,
  ]
    .map((entry) => String(entry || "").trim().toLowerCase())
    .filter(Boolean)
    .join(" ");
  const runtimeSignalText = [
    normalizedConfig?.last_user_transcript,
    normalizedConfig?.last_user_text,
    options?.runtimeSignalText,
  ]
    .map((entry) => String(entry || "").trim().toLowerCase())
    .filter(Boolean)
    .join(" ");
  const combinedIntentText = `${purposeText} ${runtimeSignalText}`.trim();

  const allowedCapabilities = new Set(["general", "handoff", "disclosure"]);
  let confidence = "low";
  let intent = "general_assistance";

  const hasDigitIntent =
    normalizedConfig?.digit_intent?.mode === "dtmf" ||
    ["capture_pending", "capture_active"].includes(
      String(normalizedConfig.flow_state || "").trim().toLowerCase(),
    );
  if (hasDigitIntent) {
    intent = "digit_capture";
    confidence = "high";
    allowedCapabilities.add("capture");
    allowedCapabilities.add("identity");
    allowedCapabilities.add("verification");
  }

  const hasPaymentIntent =
    normalizedConfig.payment_in_progress === true ||
    normalizedConfig.payment_enabled === true ||
    String(normalizedConfig.script_policy?.payment_enabled || "")
      .trim()
      .toLowerCase() === "true" ||
    /(payment|billing|invoice|charge|collect|refund|outstanding balance)/i.test(
      combinedIntentText,
    );
  if (hasPaymentIntent) {
    intent = "payment_flow";
    confidence = hasDigitIntent ? "high" : "medium";
    allowedCapabilities.add("payment");
    allowedCapabilities.add("billing");
    allowedCapabilities.add("capture");
    allowedCapabilities.add("identity");
  }

  const isRelationshipProfile = RELATIONSHIP_PROFILE_TYPES.has(profile);
  if (isRelationshipProfile) {
    if (confidence === "low") {
      confidence = "medium";
    }
    if (intent === "general_assistance") {
      intent = "relationship_flow";
    }
    allowedCapabilities.add("relationship_context");
    allowedCapabilities.add("profile");
  }

  if (/(support|ticket|case|incident|create ticket)/i.test(combinedIntentText)) {
    allowedCapabilities.add("ticketing");
    allowedCapabilities.add("support");
    if (confidence === "low") {
      confidence = "medium";
    }
  }
  if (
    /(sales|lead|crm|followup|appointment|schedule|account status|order update|calendar)/i.test(
      combinedIntentText,
    )
  ) {
    allowedCapabilities.add("crm");
    allowedCapabilities.add("scheduling");
    allowedCapabilities.add("business_ops");
    if (confidence === "low") {
      confidence = "medium";
    }
  }
  if (
    /(research|compare|latest|what changed|best option|news|search|web|crawl|competitor)/i.test(
      combinedIntentText,
    )
  ) {
    allowedCapabilities.add("research");
    allowedCapabilities.add("knowledge");
    allowedCapabilities.add("docs");
    if (confidence === "low") {
      confidence = "medium";
    }
  }
  if (/(suspicious|high-value|account recovery|kyc|fraud|velocity|blacklist|allowlist)/i.test(combinedIntentText)) {
    allowedCapabilities.add("risk");
    allowedCapabilities.add("fraud");
    allowedCapabilities.add("verification");
    if (confidence === "low") {
      confidence = "medium";
    }
  }
  if (/(how to|runbook|policy|contract|documentation|knowledge base|kb)/i.test(combinedIntentText)) {
    allowedCapabilities.add("knowledge");
    allowedCapabilities.add("docs");
    if (confidence === "low") {
      confidence = "medium";
    }
  }

  return {
    intent,
    confidence,
    profile,
    allowed_capabilities: Array.from(allowedCapabilities),
  };
}

function routeToolsByIntent(tools = [], callConfig = {}, options = {}) {
  const enrichedTools = attachConnectorMetadataToTools(tools);
  const envelope = deriveIntentEnvelope(callConfig, options);
  const allowedCapabilities = new Set(
    envelope.allowed_capabilities.map((entry) => normalizeCapabilityTag(entry)),
  );
  const getConnectorForTool = (tool) =>
    tool?.function?.connector && typeof tool.function.connector === "object"
      ? tool.function.connector
      : {};
  const getCapabilitySet = (connector = {}) =>
    new Set(
      Array.isArray(connector.capability_tags)
        ? connector.capability_tags.map((entry) => normalizeCapabilityTag(entry))
        : [],
    );

  const suppressed = [];
  const routed = enrichedTools.filter((tool) => {
    const name = normalizeToolName(tool?.function?.name);
    const connector = getConnectorForTool(tool);
    const capabilities = getCapabilitySet(connector);
    const hasAllowedCapability =
      capabilities.size === 0 ||
      [...capabilities].some((tag) => allowedCapabilities.has(tag));
    const isHighRisk = String(connector.risk_class || "").toLowerCase() === "high";
    const isPaymentConnector =
      capabilities.has("payment") || capabilities.has("billing");
    const isCaptureConnector =
      name === "collect_digits" || name === "collect_multiple_digits";
    const paymentIntentAllowed =
      allowedCapabilities.has("payment") || allowedCapabilities.has("billing");
    const isRiskConnector =
      capabilities.has("risk") || capabilities.has("fraud");
    const riskIntentAllowed =
      allowedCapabilities.has("risk") || allowedCapabilities.has("fraud");

    if (!hasAllowedCapability) {
      suppressed.push(name);
      return false;
    }
    if (isPaymentConnector && !paymentIntentAllowed && !isCaptureConnector) {
      suppressed.push(name);
      return false;
    }
    if (isRiskConnector && !riskIntentAllowed) {
      suppressed.push(name);
      return false;
    }
    if (isHighRisk && envelope.confidence === "low") {
      suppressed.push(name);
      return false;
    }
    return true;
  });

  const safeFallback = enrichedTools.filter((tool) => {
    const connector = getConnectorForTool(tool);
    const capabilities = getCapabilitySet(connector);
    const connectorClass = String(connector.class || "").toLowerCase();
    const connectorRisk = String(connector.risk_class || "").toLowerCase();
    if (connectorClass === "side_effect" || connectorRisk === "high") {
      return false;
    }
    if (capabilities.size === 0) {
      return true;
    }
    return (
      capabilities.has("general") ||
      capabilities.has("disclosure") ||
      capabilities.has("handoff")
    );
  });
  const finalTools = routed.length > 0 ? routed : safeFallback;
  return {
    tools: finalTools,
    decision: {
      source: "connector_router",
      ...envelope,
      total_in: enrichedTools.length,
      total_out: finalTools.length,
      suppressed_tools: suppressed,
    },
  };
}

function getToolApprovalConfig(callConfig = {}) {
  const configValue =
    callConfig && typeof callConfig === "object" ? callConfig : {};
  const mode = normalizeApprovalMode(
    configValue.tool_approval_mode ||
      configValue.script_policy?.tool_approval_mode ||
      configValue.relationship_profile?.tool_approval_mode ||
      "disabled",
  );
  const approvedTools =
    configValue.tool_approvals &&
    typeof configValue.tool_approvals === "object" &&
    !Array.isArray(configValue.tool_approvals)
      ? configValue.tool_approvals
      : {};
  const expiresAt = Number(configValue.tool_approval_expires_at || 0);
  return {
    mode,
    approved_tools: approvedTools,
    expires_at: Number.isFinite(expiresAt) ? expiresAt : 0,
  };
}

function evaluateConnectorApprovalPolicy(request = {}, context = {}) {
  const toolName = normalizeToolName(
    request.toolName || request.tool_name || request.functionName || "",
  );
  const callConfig =
    context.callConfig && typeof context.callConfig === "object"
      ? context.callConfig
      : {};
  const approvalConfig = getToolApprovalConfig(callConfig);
  if (approvalConfig.mode === "disabled") {
    return {
      allowed: true,
      action: "allow",
      reason: "approval_disabled",
      blocked: [],
      metadata: {
        approval_mode: approvalConfig.mode,
      },
    };
  }

  const connectorFromRequest =
    request.registryEntry &&
    request.registryEntry.connector &&
    typeof request.registryEntry.connector === "object"
      ? request.registryEntry.connector
      : null;
  const descriptor = connectorFromRequest || getConnectorDescriptor(toolName);
  const connector = descriptor && typeof descriptor === "object" ? descriptor : {};
  const connectorRisk = String(connector.risk_class || "low").trim().toLowerCase();
  const connectorClass = String(connector.class || "read").trim().toLowerCase();
  const connectorNeedsApproval = connector.approval?.required === true;
  const isSideEffect = connectorClass === "side_effect";
  const requiresByMode =
    approvalConfig.mode === "required"
      ? isSideEffect
      : connectorRisk === "high";
  const requiresApproval = connectorNeedsApproval || requiresByMode;

  if (!requiresApproval) {
    return {
      allowed: true,
      action: "allow",
      reason: "approval_not_required",
      blocked: [],
      metadata: {
        approval_mode: approvalConfig.mode,
        risk_class: connectorRisk,
      },
    };
  }

  const now = Date.now();
  const approvalExpired =
    approvalConfig.expires_at > 0 && now > Number(approvalConfig.expires_at);
  const allHighRiskApproved =
    approvalConfig.approved_tools.all_high_risk === true &&
    connectorRisk === "high";
  const explicitlyApproved = approvalConfig.approved_tools[toolName] === true;
  const approved = !approvalExpired && (allHighRiskApproved || explicitlyApproved);

  if (approved) {
    return {
      allowed: true,
      action: "allow",
      reason: "approval_present",
      blocked: [],
      metadata: {
        approval_mode: approvalConfig.mode,
        risk_class: connectorRisk,
      },
    };
  }

  return {
    allowed: false,
    action: "deny",
    code: "tool_approval_required",
    reason: "approval_required",
    message: `Tool ${toolName || "requested action"} requires operator approval before execution.`,
    blocked: ["approval_required"],
    metadata: {
      approval_mode: approvalConfig.mode,
      risk_class: connectorRisk,
      tool: toolName || null,
      approval_expired: approvalExpired,
    },
  };
}

module.exports = {
  CONNECTOR_REGISTRY,
  getConnectorDescriptor,
  attachConnectorMetadataToTools,
  deriveIntentEnvelope,
  routeToolsByIntent,
  getToolApprovalConfig,
  evaluateConnectorApprovalPolicy,
};
