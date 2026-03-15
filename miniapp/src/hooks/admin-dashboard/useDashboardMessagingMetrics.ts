import { useMemo } from 'react';

import {
  estimateSmsSegments,
  isLikelyEmail,
  isValidE164,
  parseEmailList,
  parsePhoneList,
  renderTemplateString,
  textBar,
  toInt,
  toText,
} from '@/services/admin-dashboard/dashboardPrimitives';

type SmsSummary = {
  totalRecipients?: unknown;
  totalSuccessful?: unknown;
  totalFailed?: unknown;
};

type EmailStats = {
  total_recipients?: unknown;
  sent?: unknown;
  failed?: unknown;
  delivered?: unknown;
  bounced?: unknown;
  complained?: unknown;
  suppressed?: unknown;
};

type EmailJob = {
  total?: unknown;
  delivered?: unknown;
  status?: unknown;
};

type UseDashboardMessagingMetricsOptions = {
  smsSummary: SmsSummary;
  emailStats: EmailStats;
  emailJobs: EmailJob[];
  smsRecipientsInput: string;
  smsMessageInput: string;
  smsCostPerSegment: string;
  smsDefaultCostPerSegment: number;
  mailerRecipientsInput: string;
  mailerSubjectInput: string;
  mailerHtmlInput: string;
  mailerTextInput: string;
  mailerVariablesInput: string;
};

type UseDashboardMessagingMetricsResult = {
  smsTotalRecipients: number;
  smsSuccess: number;
  smsFailed: number;
  smsProcessedPercent: number;
  emailTotalRecipients: number;
  emailSent: number;
  emailFailed: number;
  emailDelivered: number;
  emailBounced: number;
  emailComplained: number;
  emailSuppressed: number;
  emailProcessedPercent: number;
  emailDeliveredPercent: number;
  emailBouncePercent: number;
  emailComplaintPercent: number;
  smsRecipientsParsed: string[];
  smsInvalidRecipients: string[];
  smsDuplicateCount: number;
  smsSegmentEstimate: { segments: number; perSegment: number };
  smsValidationCategories: {
    valid: number;
    invalid: number;
    duplicate: number;
    likelyLandline: number;
  };
  smsEstimatedCost: number;
  mailerRecipientsParsed: string[];
  mailerInvalidRecipients: string[];
  mailerDuplicateCount: number;
  mailerVariableKeys: string[];
  mailerTemplatePreviewSubject: string;
  mailerTemplatePreviewBody: string;
  mailerTemplatePreviewError: string;
  mailerDomainHealthStatus: 'Healthy' | 'Watch' | 'Critical';
  mailerDomainHealthDetail: string;
  mailerTrendBars: string[];
};

export function useDashboardMessagingMetrics({
  smsSummary,
  emailStats,
  emailJobs,
  smsRecipientsInput,
  smsMessageInput,
  smsCostPerSegment,
  smsDefaultCostPerSegment,
  mailerRecipientsInput,
  mailerSubjectInput,
  mailerHtmlInput,
  mailerTextInput,
  mailerVariablesInput,
}: UseDashboardMessagingMetricsOptions): UseDashboardMessagingMetricsResult {
  return useMemo(() => {
    const smsTotalRecipients = toInt(smsSummary.totalRecipients);
    const smsSuccess = toInt(smsSummary.totalSuccessful);
    const smsFailed = toInt(smsSummary.totalFailed);
    const smsProcessedPercent = smsTotalRecipients > 0
      ? Math.round(((smsSuccess + smsFailed) / smsTotalRecipients) * 100)
      : 0;

    const emailTotalRecipients = toInt(emailStats.total_recipients);
    const emailSent = toInt(emailStats.sent);
    const emailFailed = toInt(emailStats.failed);
    const emailDelivered = toInt(emailStats.delivered);
    const emailBounced = toInt(emailStats.bounced);
    const emailComplained = toInt(emailStats.complained);
    const emailSuppressed = toInt(emailStats.suppressed);
    const emailProcessedPercent = emailTotalRecipients > 0
      ? Math.round(((emailSent + emailFailed) / emailTotalRecipients) * 100)
      : 0;
    const emailDeliveredPercent = emailTotalRecipients > 0
      ? Math.round((emailDelivered / emailTotalRecipients) * 100)
      : 0;
    const emailBouncePercent = emailTotalRecipients > 0
      ? Math.round((emailBounced / emailTotalRecipients) * 100)
      : 0;
    const emailComplaintPercent = emailTotalRecipients > 0
      ? Math.round((emailComplained / emailTotalRecipients) * 100)
      : 0;

    const smsRecipientsParsed = parsePhoneList(smsRecipientsInput);
    const smsInvalidRecipients = smsRecipientsParsed.filter((phone) => !isValidE164(phone));
    const smsDuplicateCount = Math.max(
      0,
      String(smsRecipientsInput || '')
        .split(/[\n,;\t ]+/g)
        .filter(Boolean).length - smsRecipientsParsed.length,
    );
    const smsSegmentEstimate = estimateSmsSegments(smsMessageInput);
    const smsValidRecipients = smsRecipientsParsed.length - smsInvalidRecipients.length;
    const smsLikelyLandlineRecipients = smsRecipientsParsed.filter((phone) => {
      const digits = phone.replace(/\D/g, '');
      return digits.length < 11 || /0000$/.test(digits);
    }).length;
    const smsValidationCategories = {
      valid: smsValidRecipients,
      invalid: smsInvalidRecipients.length,
      duplicate: smsDuplicateCount,
      likelyLandline: smsLikelyLandlineRecipients,
    };

    const smsCostPerSegmentNumber = Number(smsCostPerSegment);
    const smsCostPerSegmentResolved = Number.isFinite(smsCostPerSegmentNumber) && smsCostPerSegmentNumber >= 0
      ? smsCostPerSegmentNumber
      : smsDefaultCostPerSegment;
    const smsEstimatedCost = Number(
      (smsValidRecipients * Math.max(1, smsSegmentEstimate.segments) * smsCostPerSegmentResolved).toFixed(4),
    );

    const mailerRecipientsParsed = parseEmailList(mailerRecipientsInput);
    const mailerInvalidRecipients = mailerRecipientsParsed.filter((email) => !isLikelyEmail(email));
    const mailerDuplicateCount = Math.max(
      0,
      String(mailerRecipientsInput || '')
        .split(/[\n,;\t ]+/g)
        .filter(Boolean).length - mailerRecipientsParsed.length,
    );

    const mailerVariableKeys = Array.from(
      new Set([
        ...Array.from(String(mailerSubjectInput || '').matchAll(/{{\s*([\w.-]+)\s*}}/g)).map((m) => m[1]),
        ...Array.from(String(mailerHtmlInput || '').matchAll(/{{\s*([\w.-]+)\s*}}/g)).map((m) => m[1]),
        ...Array.from(String(mailerTextInput || '').matchAll(/{{\s*([\w.-]+)\s*}}/g)).map((m) => m[1]),
      ]),
    );

    let mailerTemplatePreviewContext: Record<string, unknown> = {};
    let mailerTemplatePreviewError = '';
    if (mailerVariablesInput.trim()) {
      try {
        const parsed: unknown = JSON.parse(mailerVariablesInput);
        if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
          mailerTemplatePreviewContext = parsed as Record<string, unknown>;
        }
      } catch {
        mailerTemplatePreviewError = 'Preview variables JSON is invalid.';
      }
    }

    const mailerTemplatePreviewSubject = renderTemplateString(
      mailerSubjectInput || '(no subject)',
      mailerTemplatePreviewContext,
    );
    const mailerTemplatePreviewBody = renderTemplateString(
      mailerTextInput || mailerHtmlInput || '(no body content)',
      mailerTemplatePreviewContext,
    );

    const mailerDomainHealthStatus = emailBouncePercent <= 2 && emailComplaintPercent <= 1
      ? 'Healthy'
      : emailBouncePercent <= 5 && emailComplaintPercent <= 2
        ? 'Watch'
        : 'Critical';
    const mailerDomainHealthDetail = `Bounce ${emailBouncePercent}% · Complaint ${emailComplaintPercent}%`;
    const mailerTrendBars = emailJobs.slice(0, 5).map((job) => {
      const total = Math.max(1, toInt(job.total));
      const delivered = Math.max(0, toInt(job.delivered));
      const deliveryRate = Math.round((delivered / total) * 100);
      return `${toText(job.status, 'job')} ${textBar(deliveryRate, 12)}`;
    });

    return {
      smsTotalRecipients,
      smsSuccess,
      smsFailed,
      smsProcessedPercent,
      emailTotalRecipients,
      emailSent,
      emailFailed,
      emailDelivered,
      emailBounced,
      emailComplained,
      emailSuppressed,
      emailProcessedPercent,
      emailDeliveredPercent,
      emailBouncePercent,
      emailComplaintPercent,
      smsRecipientsParsed,
      smsInvalidRecipients,
      smsDuplicateCount,
      smsSegmentEstimate,
      smsValidationCategories,
      smsEstimatedCost,
      mailerRecipientsParsed,
      mailerInvalidRecipients,
      mailerDuplicateCount,
      mailerVariableKeys,
      mailerTemplatePreviewSubject,
      mailerTemplatePreviewBody,
      mailerTemplatePreviewError,
      mailerDomainHealthStatus,
      mailerDomainHealthDetail,
      mailerTrendBars,
    };
  }, [
    emailJobs,
    emailStats.bounced,
    emailStats.complained,
    emailStats.delivered,
    emailStats.failed,
    emailStats.sent,
    emailStats.suppressed,
    emailStats.total_recipients,
    mailerHtmlInput,
    mailerRecipientsInput,
    mailerSubjectInput,
    mailerTextInput,
    mailerVariablesInput,
    smsCostPerSegment,
    smsDefaultCostPerSegment,
    smsMessageInput,
    smsRecipientsInput,
    smsSummary.totalFailed,
    smsSummary.totalRecipients,
    smsSummary.totalSuccessful,
  ]);
}
