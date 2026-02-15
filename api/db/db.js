const sqlite3 = require('sqlite3').verbose();
const path = require('path');

class EnhancedDatabase {
    constructor() {
        this.db = null;
        this.isInitialized = false;
        this.dbPath = path.join(__dirname, 'data.db');
        this.emailDlqColumnsEnsured = false;
        this.emailQueueColumnsEnsured = false;
        this.outboundRateLastCleanupMs = 0;
    }

    async initialize() {
        return new Promise((resolve, reject) => {
            this.db = new sqlite3.Database(this.dbPath, (err) => {
                if (err) {
                    console.error('Error opening enhanced database:', err);
                    reject(err);
                    return;
                }
                console.log('Connected to enhanced SQLite database');
                this.createEnhancedTables().then(() => {
                    this.initializeSMSTables().then(() => {
                        this.initializeEmailTables().then(() => {
                            this.ensureEmailDlqColumns().then(() => {
                                this.ensureEmailQueueColumns().then(() => {
                                    this.isInitialized = true;
                                    console.log('✅ Enhanced database initialization complete');
                                    resolve();
                                }).catch(reject);
                            }).catch(reject);
                        }).catch(reject);
                    }).catch(reject);
                }).catch(reject);
            });
        });
    }

    async createEnhancedTables() {
        const tables = [
            // Enhanced calls table with comprehensive tracking
            `CREATE TABLE IF NOT EXISTS calls (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                call_sid TEXT UNIQUE NOT NULL,
                phone_number TEXT NOT NULL,
                prompt TEXT,
                first_message TEXT,
                user_chat_id TEXT,
                status TEXT DEFAULT 'initiated',
                twilio_status TEXT,
                direction TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                started_at DATETIME,
                ended_at DATETIME,
                duration INTEGER,
                call_summary TEXT,
                ai_analysis TEXT,
                business_context TEXT,
                generated_functions TEXT,
                answered_by TEXT,
                error_code TEXT,
                error_message TEXT,
                ring_duration INTEGER,
                answer_delay INTEGER
            )`,

            // Enhanced call transcripts table with personality tracking
            `CREATE TABLE IF NOT EXISTS call_transcripts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                call_sid TEXT NOT NULL,
                speaker TEXT NOT NULL CHECK(speaker IN ('user', 'ai')),
                message TEXT NOT NULL,
                interaction_count INTEGER,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                personality_used TEXT,
                adaptation_data TEXT,
                confidence_score REAL,
                FOREIGN KEY(call_sid) REFERENCES calls(call_sid)
            )`,

            // Add backward compatibility table name
            `CREATE TABLE IF NOT EXISTS transcripts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                call_sid TEXT NOT NULL,
                speaker TEXT NOT NULL CHECK(speaker IN ('user', 'ai')),
                message TEXT NOT NULL,
                interaction_count INTEGER,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                personality_used TEXT,
                adaptation_data TEXT,
                confidence_score REAL,
                FOREIGN KEY(call_sid) REFERENCES calls(call_sid)
            )`,

            // Enhanced call states for comprehensive real-time tracking
            `CREATE TABLE IF NOT EXISTS call_states (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                call_sid TEXT NOT NULL,
                state TEXT NOT NULL,
                data TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                sequence_number INTEGER,
                FOREIGN KEY(call_sid) REFERENCES calls(call_sid)
            )`,

            // Caller flags for inbound allow/block/spam decisions
            `CREATE TABLE IF NOT EXISTS caller_flags (
                phone_number TEXT PRIMARY KEY,
                status TEXT NOT NULL CHECK(status IN ('blocked', 'allowed', 'spam')),
                note TEXT,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_by TEXT,
                source TEXT
            )`,

            // Digit capture events (DTMF, spoken, gather)
            `CREATE TABLE IF NOT EXISTS call_digits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                call_sid TEXT NOT NULL,
                source TEXT NOT NULL,
                profile TEXT NOT NULL,
                digits TEXT,
                len INTEGER,
                accepted INTEGER DEFAULT 0,
                reason TEXT,
                metadata TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(call_sid) REFERENCES calls(call_sid)
            )`,

            // Call templates for outbound call presets
            `CREATE TABLE IF NOT EXISTS call_templates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                description TEXT,
                prompt TEXT,
                first_message TEXT,
                business_id TEXT,
                voice_model TEXT,
                requires_otp INTEGER DEFAULT 0,
                default_profile TEXT,
                expected_length INTEGER,
                allow_terminator INTEGER DEFAULT 0,
                terminator_char TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )`,

            // Enhanced webhook notifications table with delivery metrics
            `CREATE TABLE IF NOT EXISTS webhook_notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                call_sid TEXT NOT NULL,
                notification_type TEXT NOT NULL,
                telegram_chat_id TEXT NOT NULL,
                status TEXT DEFAULT 'pending' CHECK(status IN ('pending', 'sent', 'failed', 'retrying')),
                error_message TEXT,
                retry_count INTEGER DEFAULT 0,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                last_attempt_at DATETIME,
                next_attempt_at DATETIME,
                sent_at DATETIME,
                delivery_time_ms INTEGER,
                telegram_message_id INTEGER,
                priority TEXT DEFAULT 'normal' CHECK(priority IN ('low', 'normal', 'high', 'urgent')),
                FOREIGN KEY(call_sid) REFERENCES calls(call_sid)
            )`,

            // Notification delivery metrics for analytics - FIXED: Added UNIQUE constraint
            `CREATE TABLE IF NOT EXISTS notification_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                notification_type TEXT NOT NULL,
                total_count INTEGER DEFAULT 0,
                success_count INTEGER DEFAULT 0,
                failure_count INTEGER DEFAULT 0,
                avg_delivery_time_ms REAL DEFAULT 0,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(date, notification_type)
            )`,

            // Service health monitoring logs
            `CREATE TABLE IF NOT EXISTS service_health_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                service_name TEXT NOT NULL,
                status TEXT NOT NULL,
                details TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )`,

            // Application settings
            `CREATE TABLE IF NOT EXISTS app_settings (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )`,

            // Call performance metrics
            `CREATE TABLE IF NOT EXISTS call_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                call_sid TEXT NOT NULL,
                metric_type TEXT NOT NULL,
                metric_value REAL,
                metric_data TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(call_sid) REFERENCES calls(call_sid)
            )`,

            // Durable call job queue (outbound retries, callbacks)
            `CREATE TABLE IF NOT EXISTS call_jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_type TEXT NOT NULL,
                payload TEXT,
                status TEXT DEFAULT 'pending' CHECK(status IN ('pending', 'running', 'completed', 'failed')),
                run_at DATETIME NOT NULL,
                attempts INTEGER DEFAULT 0,
                last_error TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                locked_at DATETIME,
                completed_at DATETIME
            )`,

            // Durable call-job dead-letter queue
            `CREATE TABLE IF NOT EXISTS call_job_dlq (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id INTEGER NOT NULL UNIQUE,
                job_type TEXT NOT NULL,
                payload TEXT,
                attempts INTEGER DEFAULT 0,
                last_error TEXT,
                dead_letter_reason TEXT,
                status TEXT DEFAULT 'open' CHECK(status IN ('open', 'replayed')),
                replay_count INTEGER DEFAULT 0,
                last_replay_job_id INTEGER,
                replayed_at DATETIME,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )`,

            // Outbound API rate-limit counters (shared coordination across app instances)
            `CREATE TABLE IF NOT EXISTS outbound_rate_limits (
                scope TEXT NOT NULL,
                actor_key TEXT NOT NULL,
                window_start INTEGER NOT NULL,
                count INTEGER NOT NULL DEFAULT 0,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (scope, actor_key)
            )`,

            // Enhanced user sessions tracking - FIXED: Added UNIQUE constraint
            `CREATE TABLE IF NOT EXISTS user_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_chat_id TEXT NOT NULL UNIQUE,
                session_start DATETIME DEFAULT CURRENT_TIMESTAMP,
                session_end DATETIME,
                total_calls INTEGER DEFAULT 0,
                successful_calls INTEGER DEFAULT 0,
                failed_calls INTEGER DEFAULT 0,
                total_duration INTEGER DEFAULT 0,
                last_activity DATETIME DEFAULT CURRENT_TIMESTAMP
            )`,

            // Hierarchical GPT memory (session-level summary per call)
            `CREATE TABLE IF NOT EXISTS gpt_call_memory (
                call_sid TEXT PRIMARY KEY,
                summary TEXT,
                summary_turns INTEGER DEFAULT 0,
                summary_updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                metadata TEXT
            )`,

            // Long-term facts extracted from conversation
            `CREATE TABLE IF NOT EXISTS gpt_memory_facts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                call_sid TEXT NOT NULL,
                fact_key TEXT NOT NULL,
                fact_text TEXT NOT NULL,
                confidence REAL DEFAULT 0.5,
                source TEXT DEFAULT 'derived',
                last_seen_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(call_sid, fact_key)
            )`,

            // Tool audit + idempotency record
            `CREATE TABLE IF NOT EXISTS gpt_tool_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                call_sid TEXT,
                trace_id TEXT,
                tool_name TEXT NOT NULL,
                idempotency_key TEXT,
                input_hash TEXT,
                request_payload TEXT,
                response_payload TEXT,
                status TEXT NOT NULL,
                error_message TEXT,
                duration_ms INTEGER,
                metadata TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )`,

            // Durable idempotency lock/result store for side-effect GPT tools
            `CREATE TABLE IF NOT EXISTS gpt_tool_idempotency (
                idempotency_key TEXT PRIMARY KEY,
                call_sid TEXT,
                trace_id TEXT,
                tool_name TEXT NOT NULL,
                input_hash TEXT,
                status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending', 'ok', 'failed')),
                response_payload TEXT,
                error_message TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )`,

            // Durable provider webhook idempotency guard
            `CREATE TABLE IF NOT EXISTS provider_event_idempotency (
                event_key TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                payload_hash TEXT NOT NULL,
                expires_at DATETIME NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )`,

            // Persisted runtime snapshot for active calls
            `CREATE TABLE IF NOT EXISTS call_runtime_state (
                call_sid TEXT PRIMARY KEY,
                provider TEXT,
                interaction_count INTEGER DEFAULT 0,
                flow_state TEXT,
                call_mode TEXT,
                digit_capture_active INTEGER DEFAULT 0,
                snapshot TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )`,

        ];

        for (const table of tables) {
            await new Promise((resolve, reject) => {
                this.db.run(table, (err) => {
                    if (err) {
                        console.error('Error creating enhanced table:', err);
                        reject(err);
                    } else {
                        resolve();
                    }
                });
            });
        }

        await this.ensureCallColumns(['digit_summary', 'digit_count', 'last_otp', 'last_otp_masked', 'direction']);
        await this.ensureTemplateColumns(['requires_otp', 'default_profile', 'expected_length', 'allow_terminator', 'terminator_char']);
        await this.ensureNotificationColumns(['last_attempt_at', 'next_attempt_at']);

        // Create comprehensive indexes for optimal performance
        const indexes = [
            // Call indexes
            'CREATE INDEX IF NOT EXISTS idx_calls_call_sid ON calls(call_sid)',
            'CREATE INDEX IF NOT EXISTS idx_calls_user_chat_id ON calls(user_chat_id)',
            'CREATE INDEX IF NOT EXISTS idx_calls_status ON calls(status)',
            'CREATE INDEX IF NOT EXISTS idx_calls_created_at ON calls(created_at)',
            'CREATE INDEX IF NOT EXISTS idx_calls_twilio_status ON calls(twilio_status)',
            'CREATE INDEX IF NOT EXISTS idx_calls_direction ON calls(direction)',
            'CREATE INDEX IF NOT EXISTS idx_calls_phone_number ON calls(phone_number)',
            
            // Transcript indexes for both table names
            'CREATE INDEX IF NOT EXISTS idx_transcripts_call_sid ON call_transcripts(call_sid)',
            'CREATE INDEX IF NOT EXISTS idx_transcripts_timestamp ON call_transcripts(timestamp)',
            'CREATE INDEX IF NOT EXISTS idx_transcripts_speaker ON call_transcripts(speaker)',
            'CREATE INDEX IF NOT EXISTS idx_transcripts_personality ON call_transcripts(personality_used)',
            'CREATE INDEX IF NOT EXISTS idx_legacy_transcripts_call_sid ON transcripts(call_sid)',
            'CREATE INDEX IF NOT EXISTS idx_legacy_transcripts_timestamp ON transcripts(timestamp)',
            'CREATE INDEX IF NOT EXISTS idx_legacy_transcripts_speaker ON transcripts(speaker)',
            
            // State indexes
            'CREATE INDEX IF NOT EXISTS idx_states_call_sid ON call_states(call_sid)',
            'CREATE INDEX IF NOT EXISTS idx_states_timestamp ON call_states(timestamp)',
            'CREATE INDEX IF NOT EXISTS idx_states_state ON call_states(state)',
            'CREATE INDEX IF NOT EXISTS idx_caller_flags_status ON caller_flags(status)',
            'CREATE INDEX IF NOT EXISTS idx_call_digits_call_sid ON call_digits(call_sid)',
            'CREATE INDEX IF NOT EXISTS idx_call_digits_profile ON call_digits(profile)',
            'CREATE INDEX IF NOT EXISTS idx_call_digits_created_at ON call_digits(created_at)',
            'CREATE INDEX IF NOT EXISTS idx_call_templates_name ON call_templates(name)',
            
            // Notification indexes
            'CREATE INDEX IF NOT EXISTS idx_notifications_status ON webhook_notifications(status)',
            'CREATE INDEX IF NOT EXISTS idx_notifications_call_sid ON webhook_notifications(call_sid)',
            'CREATE INDEX IF NOT EXISTS idx_notifications_type ON webhook_notifications(notification_type)',
            'CREATE INDEX IF NOT EXISTS idx_notifications_created_at ON webhook_notifications(created_at)',
            'CREATE INDEX IF NOT EXISTS idx_notifications_chat_id ON webhook_notifications(telegram_chat_id)',
            'CREATE INDEX IF NOT EXISTS idx_notifications_priority ON webhook_notifications(priority)',
            'CREATE INDEX IF NOT EXISTS idx_notifications_next_attempt ON webhook_notifications(next_attempt_at)',
            
            // Metrics indexes
            'CREATE INDEX IF NOT EXISTS idx_metrics_date ON notification_metrics(date)',
            'CREATE INDEX IF NOT EXISTS idx_metrics_type ON notification_metrics(notification_type)',
            'CREATE INDEX IF NOT EXISTS idx_call_metrics_call_sid ON call_metrics(call_sid)',
            'CREATE INDEX IF NOT EXISTS idx_call_metrics_type ON call_metrics(metric_type)',
            'CREATE INDEX IF NOT EXISTS idx_call_jobs_status ON call_jobs(status)',
            'CREATE INDEX IF NOT EXISTS idx_call_jobs_run_at ON call_jobs(run_at)',
            'CREATE INDEX IF NOT EXISTS idx_call_job_dlq_status ON call_job_dlq(status)',
            'CREATE INDEX IF NOT EXISTS idx_call_job_dlq_created_at ON call_job_dlq(created_at)',
            'CREATE INDEX IF NOT EXISTS idx_outbound_rate_limits_updated_at ON outbound_rate_limits(updated_at)',
            
            // Health indexes
            'CREATE INDEX IF NOT EXISTS idx_health_service ON service_health_logs(service_name)',
            'CREATE INDEX IF NOT EXISTS idx_health_timestamp ON service_health_logs(timestamp)',
            'CREATE INDEX IF NOT EXISTS idx_health_status ON service_health_logs(status)',
            
            // Session indexes
            'CREATE INDEX IF NOT EXISTS idx_sessions_chat_id ON user_sessions(telegram_chat_id)',
            'CREATE INDEX IF NOT EXISTS idx_sessions_start ON user_sessions(session_start)',
            'CREATE INDEX IF NOT EXISTS idx_sessions_activity ON user_sessions(last_activity)',
            'CREATE INDEX IF NOT EXISTS idx_gpt_call_memory_updated ON gpt_call_memory(summary_updated_at)',
            'CREATE INDEX IF NOT EXISTS idx_gpt_memory_facts_call_sid ON gpt_memory_facts(call_sid)',
            'CREATE INDEX IF NOT EXISTS idx_gpt_memory_facts_last_seen ON gpt_memory_facts(last_seen_at)',
            'CREATE INDEX IF NOT EXISTS idx_gpt_tool_audit_call_sid ON gpt_tool_audit(call_sid)',
            'CREATE INDEX IF NOT EXISTS idx_gpt_tool_audit_idempotency ON gpt_tool_audit(idempotency_key)',
            'CREATE INDEX IF NOT EXISTS idx_gpt_tool_audit_created ON gpt_tool_audit(created_at)',
            'CREATE INDEX IF NOT EXISTS idx_gpt_tool_idem_status ON gpt_tool_idempotency(status)',
            'CREATE INDEX IF NOT EXISTS idx_gpt_tool_idem_updated ON gpt_tool_idempotency(updated_at)',
            'CREATE INDEX IF NOT EXISTS idx_provider_event_idem_source ON provider_event_idempotency(source)',
            'CREATE INDEX IF NOT EXISTS idx_provider_event_idem_expires ON provider_event_idempotency(expires_at)',
            'CREATE INDEX IF NOT EXISTS idx_call_runtime_state_updated ON call_runtime_state(updated_at)',
        ];

        for (const index of indexes) {
            await new Promise((resolve, reject) => {
                this.db.run(index, (err) => {
                    if (err && !err.message.includes('already exists')) {
                        console.error('Error creating enhanced index:', err);
                        reject(err);
                    } else {
                        resolve();
                    }
                });
            });
        }

        console.log('✅ Enhanced database tables and indexes created successfully');
    }

    async ensureCallColumns(columns = []) {
        if (!columns.length) return;
        const existing = await new Promise((resolve, reject) => {
            this.db.all('PRAGMA table_info(calls)', (err, rows) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(rows || []);
                }
            });
        });

        const existingNames = new Set(existing.map((row) => row.name));
        const addColumn = (name, definition) => {
            return new Promise((resolve, reject) => {
                this.db.run(`ALTER TABLE calls ADD COLUMN ${name} ${definition}`, (err) => {
                    if (err) {
                        if (String(err.message || '').includes('duplicate')) {
                            resolve();
                        } else {
                            reject(err);
                        }
                    } else {
                        resolve();
                    }
                });
            });
        };

        for (const column of columns) {
            if (existingNames.has(column)) continue;
            if (column === 'digit_summary') {
                await addColumn('digit_summary', 'TEXT');
            } else if (column === 'digit_count') {
                await addColumn('digit_count', 'INTEGER DEFAULT 0');
            } else if (column === 'last_otp') {
                await addColumn('last_otp', 'TEXT');
            } else if (column === 'last_otp_masked') {
                await addColumn('last_otp_masked', 'TEXT');
            } else if (column === 'direction') {
                await addColumn('direction', 'TEXT');
            }
        }
    }

    async ensureTemplateColumns(columns = []) {
        if (!columns.length) return;
        const existing = await new Promise((resolve, reject) => {
            this.db.all('PRAGMA table_info(call_templates)', (err, rows) => {
                if (err) reject(err);
                else resolve(rows || []);
            });
        });
        const existingNames = new Set(existing.map((row) => row.name));
        const addColumn = (name, definition) => new Promise((resolve, reject) => {
            this.db.run(`ALTER TABLE call_templates ADD COLUMN ${name} ${definition}`, (err) => {
                if (err) {
                    if (String(err.message || '').includes('duplicate')) resolve();
                    else reject(err);
                } else resolve();
            });
        });
        for (const column of columns) {
            if (existingNames.has(column)) continue;
            if (column === 'requires_otp') {
                await addColumn('requires_otp', 'INTEGER DEFAULT 0');
            } else if (column === 'default_profile') {
                await addColumn('default_profile', 'TEXT');
            } else if (column === 'expected_length') {
                await addColumn('expected_length', 'INTEGER');
            } else if (column === 'allow_terminator') {
                await addColumn('allow_terminator', 'INTEGER DEFAULT 0');
            } else if (column === 'terminator_char') {
                await addColumn('terminator_char', 'TEXT');
            }
        }
    }

    // Enhanced call creation with comprehensive metadata
    async createCall(callData) {
        const { 
            call_sid, 
            phone_number, 
            prompt, 
            first_message, 
            user_chat_id, 
            business_context = null,
            generated_functions = null,
            direction = null
        } = callData;
        
        return new Promise((resolve, reject) => {
            const stmt = this.db.prepare(`
                INSERT INTO calls (
                    call_sid, phone_number, prompt, first_message, 
                    user_chat_id, status, business_context, generated_functions, direction
                )
                VALUES (?, ?, ?, ?, ?, 'initiated', ?, ?, ?)
            `);
            
            stmt.run([
                call_sid, 
                phone_number, 
                prompt, 
                first_message, 
                user_chat_id, 
                business_context,
                generated_functions,
                direction
            ], function(err) {
                if (err) {
                    reject(err);
                } else {
                    resolve(this.lastID);
                }
            });
            stmt.finalize();
        });
    }

    async ensureNotificationColumns(columns = []) {
        if (!columns.length) return;
        const existing = await new Promise((resolve, reject) => {
            this.db.all('PRAGMA table_info(webhook_notifications)', (err, rows) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(rows || []);
                }
            });
        });
        const existingNames = new Set(existing.map((row) => row.name));
        const addColumn = (name, definition) => new Promise((resolve, reject) => {
            this.db.run(`ALTER TABLE webhook_notifications ADD COLUMN ${name} ${definition}`, (err) => {
                if (err) {
                    if (String(err.message || '').includes('duplicate')) resolve();
                    else reject(err);
                } else {
                    resolve();
                }
            });
        });

        for (const column of columns) {
            if (existingNames.has(column)) continue;
            if (column === 'last_attempt_at') {
                await addColumn('last_attempt_at', 'DATETIME');
            } else if (column === 'next_attempt_at') {
                await addColumn('next_attempt_at', 'DATETIME');
            }
        }
    }

    // Enhanced status update with comprehensive tracking
    async updateCallStatus(call_sid, status, additionalData = {}) {
        return new Promise((resolve, reject) => {
            let updateFields = ['status = ?'];
            let values = [status];

            // Handle all possible additional data fields
            const fieldMappings = {
                'started_at': 'started_at',
                'ended_at': 'ended_at', 
                'duration': 'duration',
                'call_summary': 'call_summary',
                'ai_analysis': 'ai_analysis',
                'twilio_status': 'twilio_status',
                'answered_by': 'answered_by',
                'error_code': 'error_code',
                'error_message': 'error_message',
                'ring_duration': 'ring_duration',
                'answer_delay': 'answer_delay',
                'digit_summary': 'digit_summary',
                'digit_count': 'digit_count',
                'last_otp': 'last_otp',
                'last_otp_masked': 'last_otp_masked'
            };

            Object.entries(fieldMappings).forEach(([key, field]) => {
                if (additionalData[key] !== undefined) {
                    updateFields.push(`${field} = ?`);
                    values.push(additionalData[key]);
                }
            });

            values.push(call_sid);

            const sql = `UPDATE calls SET ${updateFields.join(', ')} WHERE call_sid = ?`;
            
            this.db.run(sql, values, function(err) {
                if (err) {
                    reject(err);
                } else {
                    resolve(this.changes);
                }
            });
        });
    }

    // Enhanced call state tracking
    async updateCallState(call_sid, state, data = null) {
        return new Promise((resolve, reject) => {
            const stmt = this.db.prepare(`
                INSERT INTO call_states (call_sid, state, data, sequence_number)
                VALUES (?, ?, ?, (
                    SELECT COALESCE(MAX(sequence_number), 0) + 1 
                    FROM call_states 
                    WHERE call_sid = ?
                ))
            `);
            
            stmt.run([call_sid, state, data ? JSON.stringify(data) : null, call_sid], function(err) {
                if (err) {
                    reject(err);
                } else {
                    resolve(this.lastID);
                }
            });
            stmt.finalize();
        });
    }

    async getLatestCallState(call_sid, state) {
        return new Promise((resolve, reject) => {
            const sql = `
                SELECT data
                FROM call_states
                WHERE call_sid = ? AND state = ?
                ORDER BY sequence_number DESC
                LIMIT 1
            `;
            this.db.get(sql, [call_sid, state], (err, row) => {
                if (err) {
                    reject(err);
                } else if (!row?.data) {
                    resolve(null);
                } else {
                    try {
                        resolve(JSON.parse(row.data));
                    } catch (parseError) {
                        resolve(null);
                    }
                }
            });
        });
    }

    async getCallStates(call_sid, options = {}) {
        if (!call_sid) return [];
        const limit = Math.max(1, Math.min(Number(options.limit) || 20, 100));
        return new Promise((resolve, reject) => {
            const sql = `
                SELECT state, data, timestamp, sequence_number
                FROM call_states
                WHERE call_sid = ?
                ORDER BY sequence_number DESC
                LIMIT ?
            `;
            this.db.all(sql, [call_sid, limit], (err, rows) => {
                if (err) {
                    reject(err);
                    return;
                }
                const parsed = (rows || []).map((row) => {
                    let data = row.data;
                    if (typeof data === 'string' && data) {
                        try {
                            data = JSON.parse(data);
                        } catch (_) {
                            // leave as string
                        }
                    }
                    return {
                        state: row.state,
                        data,
                        timestamp: row.timestamp,
                        sequence_number: row.sequence_number
                    };
                });
                resolve(parsed);
            });
        });
    }

    async getCallStatesAfter(call_sid, after = 0, limit = 100) {
        if (!call_sid) return [];
        const safeAfter = Number.isFinite(Number(after)) ? Number(after) : 0;
        const safeLimit = Math.max(1, Math.min(Number(limit) || 100, 200));
        return new Promise((resolve, reject) => {
            const sql = `
                SELECT state, data, timestamp, sequence_number
                FROM call_states
                WHERE call_sid = ? AND sequence_number > ?
                ORDER BY sequence_number ASC
                LIMIT ?
            `;
            this.db.all(sql, [call_sid, safeAfter, safeLimit], (err, rows) => {
                if (err) {
                    reject(err);
                    return;
                }
                const parsed = (rows || []).map((row) => {
                    let data = row.data;
                    if (typeof data === 'string' && data) {
                        try {
                            data = JSON.parse(data);
                        } catch (_) {
                            // leave as string
                        }
                    }
                    return {
                        state: row.state,
                        data,
                        timestamp: row.timestamp,
                        sequence_number: row.sequence_number
                    };
                });
                resolve(parsed);
            });
        });
    }

    async getCallerFlag(phone_number) {
        if (!phone_number) return null;
        return new Promise((resolve, reject) => {
            const sql = `
                SELECT phone_number, status, note, updated_at, updated_by, source
                FROM caller_flags
                WHERE phone_number = ?
                LIMIT 1
            `;
            this.db.get(sql, [phone_number], (err, row) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(row || null);
                }
            });
        });
    }

    async setCallerFlag(phone_number, status, meta = {}) {
        if (!phone_number || !status) return null;
        const note = meta.note || null;
        const updatedBy = meta.updated_by || meta.updatedBy || null;
        const source = meta.source || null;
        const updatedAt = new Date().toISOString();
        return new Promise((resolve, reject) => {
            const sql = `
                INSERT INTO caller_flags (phone_number, status, note, updated_at, updated_by, source)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(phone_number) DO UPDATE SET
                    status = excluded.status,
                    note = excluded.note,
                    updated_at = excluded.updated_at,
                    updated_by = excluded.updated_by,
                    source = excluded.source
            `;
            this.db.run(sql, [phone_number, status, note, updatedAt, updatedBy, source], function(err) {
                if (err) {
                    reject(err);
                } else {
                    resolve({ phone_number, status, note, updated_at: updatedAt, updated_by: updatedBy, source });
                }
            });
        });
    }

    async clearCallerFlag(phone_number) {
        if (!phone_number) return 0;
        return new Promise((resolve, reject) => {
            this.db.run('DELETE FROM caller_flags WHERE phone_number = ?', [phone_number], function(err) {
                if (err) {
                    reject(err);
                } else {
                    resolve(this.changes);
                }
            });
        });
    }

    async listCallerFlags(filters = {}) {
        const status = filters.status || null;
        const limit = Number.isFinite(Number(filters.limit)) ? Math.max(1, Math.min(500, Number(filters.limit))) : 100;
        const sql = status
            ? `SELECT phone_number, status, note, updated_at, updated_by, source FROM caller_flags WHERE status = ? ORDER BY updated_at DESC LIMIT ?`
            : `SELECT phone_number, status, note, updated_at, updated_by, source FROM caller_flags ORDER BY updated_at DESC LIMIT ?`;
        const params = status ? [status, limit] : [limit];
        return new Promise((resolve, reject) => {
            this.db.all(sql, params, (err, rows) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(rows || []);
                }
            });
        });
    }

    async getCallTemplates() {
        return new Promise((resolve, reject) => {
            const sql = `
                SELECT id, name, description, prompt, first_message, business_id, voice_model,
                       requires_otp, default_profile, expected_length, allow_terminator, terminator_char,
                       created_at, updated_at
                FROM call_templates
                ORDER BY id DESC
            `;
            this.db.all(sql, [], (err, rows) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(rows || []);
                }
            });
        });
    }

    async getCallTemplateById(id) {
        return new Promise((resolve, reject) => {
            const sql = `
                SELECT id, name, description, prompt, first_message, business_id, voice_model,
                       requires_otp, default_profile, expected_length, allow_terminator, terminator_char,
                       created_at, updated_at
                FROM call_templates
                WHERE id = ?
            `;
            this.db.get(sql, [id], (err, row) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(row || null);
                }
            });
        });
    }

    async createCallTemplate(payload) {
        const {
            name,
            description = null,
            prompt = null,
            first_message,
            business_id = null,
            voice_model = null
        } = payload || {};
        return new Promise((resolve, reject) => {
            const sql = `
                INSERT INTO call_templates (
                    name, description, prompt, first_message, business_id, voice_model, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            `;
            this.db.run(
                sql,
                [name, description, prompt, first_message, business_id, voice_model],
                function(err) {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(this.lastID);
                    }
                }
            );
        });
    }

    async updateCallTemplate(id, payload) {
        const fields = [];
        const values = [];
        const mapping = {
            name: 'name',
            description: 'description',
            prompt: 'prompt',
            first_message: 'first_message',
            business_id: 'business_id',
            voice_model: 'voice_model',
            requires_otp: 'requires_otp',
            default_profile: 'default_profile',
            expected_length: 'expected_length',
            allow_terminator: 'allow_terminator',
            terminator_char: 'terminator_char'
        };
        Object.entries(mapping).forEach(([key, column]) => {
            if (payload[key] !== undefined) {
                fields.push(`${column} = ?`);
                values.push(payload[key]);
            }
        });
        if (!fields.length) {
            return 0;
        }
        fields.push('updated_at = CURRENT_TIMESTAMP');
        values.push(id);
        return new Promise((resolve, reject) => {
            const sql = `UPDATE call_templates SET ${fields.join(', ')} WHERE id = ?`;
            this.db.run(sql, values, function(err) {
                if (err) {
                    reject(err);
                } else {
                    resolve(this.changes);
                }
            });
        });
    }

    async deleteCallTemplate(id) {
        return new Promise((resolve, reject) => {
            this.db.run('DELETE FROM call_templates WHERE id = ?', [id], function(err) {
                if (err) {
                    reject(err);
                } else {
                    resolve(this.changes);
                }
            });
        });
    }

    // Enhanced transcript with personality tracking (supports both table names)
    async addTranscript(transcriptData) {
        const { 
            call_sid, 
            speaker, 
            message, 
            interaction_count,
            personality_used = null,
            adaptation_data = null,
            confidence_score = null
        } = transcriptData;
        
        return new Promise((resolve, reject) => {
            // Insert into both tables for backward compatibility
            const insertIntoTable = (tableName) => {
                return new Promise((resolve, reject) => {
                    const stmt = this.db.prepare(`
                        INSERT INTO ${tableName} (
                            call_sid, speaker, message, interaction_count, 
                            personality_used, adaptation_data, confidence_score
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    `);
                    
                    stmt.run([
                        call_sid, 
                        speaker, 
                        message, 
                        interaction_count,
                        personality_used,
                        adaptation_data,
                        confidence_score
                    ], function(err) {
                        if (err) {
                            reject(err);
                        } else {
                            resolve(this.lastID);
                        }
                    });
                    stmt.finalize();
                });
            };

            // Insert into both tables
            Promise.all([
                insertIntoTable('call_transcripts'),
                insertIntoTable('transcripts')
            ]).then((results) => {
                resolve(results[0]); // Return the first table's lastID
            }).catch(reject);
        });
    }

    async addCallDigitEvent(payload = {}) {
        const {
            call_sid,
            source = 'unknown',
            profile = 'generic',
            digits = null,
            len = digits ? String(digits).length : null,
            accepted = false,
            reason = null,
            metadata = null
        } = payload;

        return new Promise((resolve, reject) => {
            const stmt = this.db.prepare(`
                INSERT INTO call_digits (
                    call_sid, source, profile, digits, len, accepted, reason, metadata
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            `);

            stmt.run([
                call_sid,
                source,
                profile,
                digits,
                len,
                accepted ? 1 : 0,
                reason,
                metadata ? JSON.stringify(metadata) : null
            ], function(err) {
                if (err) {
                    reject(err);
                } else {
                    resolve(this.lastID);
                }
            });
            stmt.finalize();
        });
    }

    async getCallDigits(call_sid) {
        return new Promise((resolve, reject) => {
            this.db.all(
                `SELECT * FROM call_digits WHERE call_sid = ? ORDER BY created_at ASC, id ASC`,
                [call_sid],
                (err, rows) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(rows || []);
                    }
                }
            );
        });
    }

    async getCallMemory(call_sid) {
        return new Promise((resolve, reject) => {
            this.db.get(
                `SELECT * FROM gpt_call_memory WHERE call_sid = ?`,
                [call_sid],
                (err, row) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(row || null);
                    }
                }
            );
        });
    }

    async upsertCallMemory(call_sid, payload = {}) {
        const summary = typeof payload.summary === 'string' ? payload.summary : '';
        const summaryTurns = Number.isFinite(Number(payload.summary_turns))
            ? Number(payload.summary_turns)
            : 0;
        const metadata = payload.metadata && typeof payload.metadata === 'object'
            ? JSON.stringify(payload.metadata)
            : null;

        return new Promise((resolve, reject) => {
            const sql = `
                INSERT INTO gpt_call_memory (call_sid, summary, summary_turns, metadata, summary_updated_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(call_sid) DO UPDATE SET
                    summary = excluded.summary,
                    summary_turns = excluded.summary_turns,
                    metadata = excluded.metadata,
                    summary_updated_at = CURRENT_TIMESTAMP
            `;
            this.db.run(sql, [call_sid, summary, summaryTurns, metadata], function(err) {
                if (err) {
                    reject(err);
                } else {
                    resolve(this.changes || 0);
                }
            });
        });
    }

    async listCallMemoryFacts(call_sid, limit = 20, maxAgeDays = 14) {
        const safeLimit = Math.min(Math.max(Number(limit) || 20, 1), 100);
        const safeMaxAgeDays = Math.min(Math.max(Number(maxAgeDays) || 14, 1), 90);
        return new Promise((resolve, reject) => {
            const sql = `
                SELECT id, call_sid, fact_key, fact_text, confidence, source, last_seen_at, created_at
                FROM gpt_memory_facts
                WHERE call_sid = ?
                  AND last_seen_at >= datetime('now', ?)
                ORDER BY confidence DESC, last_seen_at DESC, id DESC
                LIMIT ?
            `;
            this.db.all(sql, [call_sid, `-${safeMaxAgeDays} days`, safeLimit], (err, rows) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(rows || []);
                }
            });
        });
    }

    async upsertCallMemoryFact(payload = {}) {
        const {
            call_sid,
            fact_key,
            fact_text,
            confidence = 0.5,
            source = 'derived'
        } = payload;
        if (!call_sid || !fact_key || !fact_text) {
            return 0;
        }
        const safeConfidence = Math.max(0, Math.min(1, Number(confidence) || 0.5));

        return new Promise((resolve, reject) => {
            const sql = `
                INSERT INTO gpt_memory_facts (
                    call_sid, fact_key, fact_text, confidence, source, last_seen_at
                )
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(call_sid, fact_key) DO UPDATE SET
                    fact_text = excluded.fact_text,
                    confidence = excluded.confidence,
                    source = excluded.source,
                    last_seen_at = CURRENT_TIMESTAMP
            `;
            this.db.run(
                sql,
                [call_sid, fact_key, fact_text, safeConfidence, String(source || 'derived')],
                function(err) {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(this.changes || 0);
                    }
                }
            );
        });
    }

    async addGptToolAudit(payload = {}) {
        const {
            call_sid = null,
            trace_id = null,
            tool_name = null,
            idempotency_key = null,
            input_hash = null,
            request_payload = null,
            response_payload = null,
            status = 'unknown',
            error_message = null,
            duration_ms = null,
            metadata = null
        } = payload;
        if (!tool_name) {
            return 0;
        }

        return new Promise((resolve, reject) => {
            const sql = `
                INSERT INTO gpt_tool_audit (
                    call_sid, trace_id, tool_name, idempotency_key, input_hash,
                    request_payload, response_payload, status, error_message, duration_ms, metadata
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            `;
            this.db.run(
                sql,
                [
                    call_sid,
                    trace_id,
                    tool_name,
                    idempotency_key,
                    input_hash,
                    request_payload ? JSON.stringify(request_payload) : null,
                    response_payload ? JSON.stringify(response_payload) : null,
                    String(status || 'unknown'),
                    error_message,
                    Number.isFinite(Number(duration_ms)) ? Number(duration_ms) : null,
                    metadata ? JSON.stringify(metadata) : null
                ],
                function(err) {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(this.lastID || 0);
                    }
                }
            );
        });
    }

    async getGptToolAuditByIdempotency(idempotency_key) {
        if (!idempotency_key) return null;
        return new Promise((resolve, reject) => {
            this.db.get(
                `SELECT * FROM gpt_tool_audit
                 WHERE idempotency_key = ? AND status IN ('ok', 'cached')
                 ORDER BY id DESC LIMIT 1`,
                [idempotency_key],
                (err, row) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(row || null);
                    }
                }
            );
        });
    }

    async reserveGptToolIdempotency(payload = {}) {
        const {
            idempotency_key,
            call_sid = null,
            trace_id = null,
            tool_name = null,
            input_hash = null,
        } = payload;
        if (!idempotency_key || !tool_name) {
            return { reserved: false };
        }
        return new Promise((resolve, reject) => {
            const sql = `
                INSERT INTO gpt_tool_idempotency (
                    idempotency_key, call_sid, trace_id, tool_name, input_hash, status, updated_at
                )
                VALUES (?, ?, ?, ?, ?, 'pending', CURRENT_TIMESTAMP)
                ON CONFLICT(idempotency_key) DO NOTHING
            `;
            this.db.run(
                sql,
                [idempotency_key, call_sid, trace_id, tool_name, input_hash],
                function(err) {
                    if (err) {
                        reject(err);
                    } else {
                        resolve({ reserved: (this.changes || 0) > 0 });
                    }
                }
            );
        });
    }

    async getGptToolIdempotency(idempotency_key) {
        if (!idempotency_key) return null;
        return new Promise((resolve, reject) => {
            const sql = `
                SELECT *
                FROM gpt_tool_idempotency
                WHERE idempotency_key = ?
                LIMIT 1
            `;
            this.db.get(sql, [idempotency_key], (err, row) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(row || null);
                }
            });
        });
    }

    async completeGptToolIdempotency(payload = {}) {
        const {
            idempotency_key,
            call_sid = null,
            trace_id = null,
            tool_name = null,
            input_hash = null,
            status = 'failed',
            response_payload = null,
            error_message = null,
        } = payload;
        if (!idempotency_key || !tool_name) return 0;
        const safeStatus = ['pending', 'ok', 'failed'].includes(String(status))
            ? String(status)
            : 'failed';
        return new Promise((resolve, reject) => {
            const sql = `
                INSERT INTO gpt_tool_idempotency (
                    idempotency_key, call_sid, trace_id, tool_name, input_hash,
                    status, response_payload, error_message, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(idempotency_key) DO UPDATE SET
                    call_sid = COALESCE(excluded.call_sid, gpt_tool_idempotency.call_sid),
                    trace_id = COALESCE(excluded.trace_id, gpt_tool_idempotency.trace_id),
                    tool_name = COALESCE(excluded.tool_name, gpt_tool_idempotency.tool_name),
                    input_hash = COALESCE(excluded.input_hash, gpt_tool_idempotency.input_hash),
                    status = excluded.status,
                    response_payload = excluded.response_payload,
                    error_message = excluded.error_message,
                    updated_at = CURRENT_TIMESTAMP
            `;
            this.db.run(
                sql,
                [
                    idempotency_key,
                    call_sid,
                    trace_id,
                    tool_name,
                    input_hash,
                    safeStatus,
                    response_payload != null ? JSON.stringify(response_payload) : null,
                    error_message,
                ],
                function(err) {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(this.changes || 0);
                    }
                }
            );
        });
    }

    async reserveProviderEventIdempotency(payload = {}) {
        const source = String(payload.source || '').trim();
        const payloadHash = String(payload.payload_hash || '').trim();
        const eventKey = String(payload.event_key || `${source}:${payloadHash}`).trim();
        if (!source || !payloadHash || !eventKey) {
            return { reserved: false };
        }
        const ttlMs = Number(payload.ttl_ms);
        const ttlSecondsRaw = Number.isFinite(ttlMs) && ttlMs > 0
            ? Math.ceil(ttlMs / 1000)
            : 300;
        const ttlSeconds = Math.max(1, Math.min(24 * 60 * 60, ttlSecondsRaw));

        return new Promise((resolve, reject) => {
            const cleanupSql = `
                DELETE FROM provider_event_idempotency
                WHERE expires_at <= CURRENT_TIMESTAMP
            `;
            const insertSql = `
                INSERT OR IGNORE INTO provider_event_idempotency (
                    event_key, source, payload_hash, expires_at
                )
                VALUES (?, ?, ?, datetime('now', '+' || ? || ' seconds'))
            `;

            this.db.run(cleanupSql, (cleanupErr) => {
                if (cleanupErr) {
                    reject(cleanupErr);
                    return;
                }
                this.db.run(
                    insertSql,
                    [eventKey, source, payloadHash, ttlSeconds],
                    function(err) {
                        if (err) {
                            reject(err);
                        } else {
                            resolve({ reserved: (this.changes || 0) > 0 });
                        }
                    }
                );
            });
        });
    }

    async pruneProviderEventIdempotency() {
        return new Promise((resolve, reject) => {
            const sql = `
                DELETE FROM provider_event_idempotency
                WHERE expires_at <= CURRENT_TIMESTAMP
            `;
            this.db.run(sql, function(err) {
                if (err) {
                    reject(err);
                } else {
                    resolve(this.changes || 0);
                }
            });
        });
    }

    async getCallRuntimeState(call_sid) {
        if (!call_sid) return null;
        return new Promise((resolve, reject) => {
            const sql = `
                SELECT *
                FROM call_runtime_state
                WHERE call_sid = ?
                LIMIT 1
            `;
            this.db.get(sql, [call_sid], (err, row) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(row || null);
                }
            });
        });
    }

    async upsertCallRuntimeState(payload = {}) {
        const callSid = String(payload.call_sid || '').trim();
        if (!callSid) return 0;
        const interactionCount = Number(payload.interaction_count);
        const digitCaptureActive = payload.digit_capture_active ? 1 : 0;
        const snapshot = payload.snapshot != null
            ? JSON.stringify(payload.snapshot)
            : null;

        return new Promise((resolve, reject) => {
            const sql = `
                INSERT INTO call_runtime_state (
                    call_sid,
                    provider,
                    interaction_count,
                    flow_state,
                    call_mode,
                    digit_capture_active,
                    snapshot,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(call_sid) DO UPDATE SET
                    provider = COALESCE(excluded.provider, call_runtime_state.provider),
                    interaction_count = COALESCE(excluded.interaction_count, call_runtime_state.interaction_count),
                    flow_state = COALESCE(excluded.flow_state, call_runtime_state.flow_state),
                    call_mode = COALESCE(excluded.call_mode, call_runtime_state.call_mode),
                    digit_capture_active = excluded.digit_capture_active,
                    snapshot = COALESCE(excluded.snapshot, call_runtime_state.snapshot),
                    updated_at = CURRENT_TIMESTAMP
            `;
            this.db.run(
                sql,
                [
                    callSid,
                    payload.provider || null,
                    Number.isFinite(interactionCount)
                        ? Math.max(0, Math.floor(interactionCount))
                        : 0,
                    payload.flow_state || null,
                    payload.call_mode || null,
                    digitCaptureActive,
                    snapshot,
                ],
                function(err) {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(this.changes || 0);
                    }
                }
            );
        });
    }

    async deleteCallRuntimeState(call_sid) {
        const callSid = String(call_sid || '').trim();
        if (!callSid) return 0;
        return new Promise((resolve, reject) => {
            this.db.run(
                'DELETE FROM call_runtime_state WHERE call_sid = ?',
                [callSid],
                function(err) {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(this.changes || 0);
                    }
                }
            );
        });
    }

    async cleanupCallRuntimeState(hoursToKeep = 24) {
        const safeHours = Math.max(1, Number(hoursToKeep) || 24);
        return new Promise((resolve, reject) => {
            const sql = `
                DELETE FROM call_runtime_state
                WHERE updated_at < datetime('now', '-' || ? || ' hours')
            `;
            this.db.run(sql, [safeHours], function(err) {
                if (err) {
                    reject(err);
                } else {
                    resolve(this.changes || 0);
                }
            });
        });
    }

    // NEW: Get recent calls with transcripts count (REQUIRED FOR API ENDPOINTS)
    async getRecentCalls(limitOrOptions = 10, offset = 0) {
        let limit = 10;
        let actualOffset = 0;
        let status = null;
        let direction = null;
        let search = null;
        let start = null;
        let end = null;

        if (typeof limitOrOptions === 'object' && limitOrOptions !== null) {
            limit = Number(limitOrOptions.limit ?? 10);
            actualOffset = Number(limitOrOptions.offset ?? 0);
            status = limitOrOptions.status ?? null;
            direction = limitOrOptions.direction ?? null;
            search = limitOrOptions.query ?? null;
            start = limitOrOptions.start ?? null;
            end = limitOrOptions.end ?? null;
        } else {
            limit = Number(limitOrOptions ?? 10);
            actualOffset = Number(offset ?? 0);
        }

        const where = [];
        const params = [];

        if (status) {
            const statuses = String(status)
                .split(',')
                .map((value) => value.trim().toLowerCase())
                .filter(Boolean);
            if (statuses.length === 1) {
                where.push('LOWER(c.status) = ?');
                params.push(statuses[0]);
            } else if (statuses.length > 1) {
                where.push(`LOWER(c.status) IN (${statuses.map(() => '?').join(',')})`);
                params.push(...statuses);
            }
        }

        if (direction) {
            const directions = String(direction)
                .split(',')
                .map((value) => value.trim().toLowerCase())
                .filter(Boolean);
            if (directions.length === 1) {
                where.push('LOWER(c.direction) = ?');
                params.push(directions[0]);
            } else if (directions.length > 1) {
                where.push(`LOWER(c.direction) IN (${directions.map(() => '?').join(',')})`);
                params.push(...directions);
            }
        }

        if (search) {
            const needle = `%${String(search).trim()}%`;
            where.push('(c.phone_number LIKE ? OR c.call_sid LIKE ?)');
            params.push(needle, needle);
        }

        if (start) {
            where.push('c.created_at >= ?');
            params.push(start);
        }

        if (end) {
            where.push('c.created_at <= ?');
            params.push(end);
        }

        const whereClause = where.length ? `WHERE ${where.join(' AND ')}` : '';

        return new Promise((resolve, reject) => {
            const query = `
                SELECT 
                    c.*,
                    COUNT(t.id) as transcript_count
                FROM calls c
                LEFT JOIN transcripts t ON c.call_sid = t.call_sid
                ${whereClause}
                GROUP BY c.call_sid
                ORDER BY c.created_at DESC
                LIMIT ? OFFSET ?
            `;

            params.push(limit, actualOffset);
            this.db.all(query, params, (err, rows) => {
                if (err) {
                    console.error('Database error in getRecentCalls:', err);
                    reject(err);
                } else {
                    resolve(rows || []);
                }
            });
        });
    }

    // NEW: Get total calls count (REQUIRED FOR API ENDPOINTS)
    async getCallsCount() {
        return new Promise((resolve, reject) => {
            this.db.get('SELECT COUNT(*) as count FROM calls', (err, row) => {
                if (err) {
                    console.error('Database error in getCallsCount:', err);
                    reject(err);
                } else {
                    resolve(row?.count || 0);
                }
            });
        });
    }

    async getRecentCallCountByNumber(number, hours = 24) {
        if (!number) return 0;
        const window = Math.max(1, Number(hours) || 24);
        const sql = `
            SELECT COUNT(*) as count
            FROM calls
            WHERE phone_number = ?
              AND created_at >= datetime('now', '-${window} hours')
        `;
        return new Promise((resolve, reject) => {
            this.db.get(sql, [number], (err, row) => {
                if (err) {
                    console.error('Database error in getRecentCallCountByNumber:', err);
                    reject(err);
                } else {
                    resolve(row?.count || 0);
                }
            });
        });
    }

    async getSetting(key) {
        return new Promise((resolve, reject) => {
            this.db.get('SELECT value FROM app_settings WHERE key = ?', [key], (err, row) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(row?.value ?? null);
                }
            });
        });
    }

    async setSetting(key, value) {
        return new Promise((resolve, reject) => {
            const stmt = this.db.prepare(`
                INSERT OR REPLACE INTO app_settings (key, value, updated_at)
                VALUES (?, ?, datetime('now'))
            `);
            stmt.run([key, value], function(err) {
                if (err) {
                    reject(err);
                } else {
                    resolve(this.changes);
                }
            });
            stmt.finalize();
        });
    }

    async tableExists(tableName) {
        return new Promise((resolve, reject) => {
            this.db.get(
                `SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?`,
                [tableName],
                (err, row) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(Boolean(row?.name));
                    }
                }
            );
        });
    }

    async indexExists(indexName) {
        return new Promise((resolve, reject) => {
            this.db.get(
                `SELECT name FROM sqlite_master WHERE type = 'index' AND name = ?`,
                [indexName],
                (err, row) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(Boolean(row?.name));
                    }
                }
            );
        });
    }

    async ensureSchemaGuardrails(options = {}) {
        const expectedVersion = Math.max(1, Number(options.expectedVersion) || 1);
        const strict = options.strict !== false;
        const requiredTables = Array.isArray(options.requiredTables) ? options.requiredTables : [];
        const requiredIndexes = Array.isArray(options.requiredIndexes) ? options.requiredIndexes : [];
        const rawVersion = await this.getSetting('schema_version');
        let currentVersion = Number.parseInt(rawVersion, 10);
        if (!Number.isFinite(currentVersion)) {
            currentVersion = 0;
        }

        if (currentVersion > expectedVersion) {
            const error = new Error(`Database schema version ${currentVersion} is newer than supported ${expectedVersion}`);
            if (strict) throw error;
            console.warn(error.message);
        } else if (currentVersion < expectedVersion) {
            await this.setSetting('schema_version', String(expectedVersion));
            currentVersion = expectedVersion;
        }

        const missingTables = [];
        for (const tableName of requiredTables) {
            if (!tableName) continue;
            const exists = await this.tableExists(tableName);
            if (!exists) missingTables.push(tableName);
        }

        const missingIndexes = [];
        for (const indexName of requiredIndexes) {
            if (!indexName) continue;
            const exists = await this.indexExists(indexName);
            if (!exists) missingIndexes.push(indexName);
        }

        if (missingTables.length || missingIndexes.length) {
            const error = new Error(
                `Database schema artifacts missing. tables=${missingTables.join(',') || 'none'} indexes=${missingIndexes.join(',') || 'none'}`
            );
            if (strict) throw error;
            console.warn(error.message);
        }

        return {
            schema_version: currentVersion,
            expected_version: expectedVersion,
            strict,
            missing_tables: missingTables,
            missing_indexes: missingIndexes,
            ok: missingTables.length === 0 && missingIndexes.length === 0
        };
    }

    // Enhanced webhook notification creation with priority
    async createEnhancedWebhookNotification(call_sid, notification_type, telegram_chat_id, priority = 'normal') {
        return new Promise((resolve, reject) => {
            const stmt = this.db.prepare(`
                INSERT INTO webhook_notifications (call_sid, notification_type, telegram_chat_id, priority, retry_count)
                VALUES (?, ?, ?, ?, 0)
            `);
            
            stmt.run([call_sid, notification_type, telegram_chat_id, priority], function(err) {
                if (err) {
                    reject(err);
                } else {
                    resolve(this.lastID);
                }
            });
            stmt.finalize();
        });
    }

    // Backward compatibility method
    async createWebhookNotification(call_sid, notification_type, telegram_chat_id) {
        return this.createEnhancedWebhookNotification(call_sid, notification_type, telegram_chat_id, 'normal');
    }

    // Enhanced webhook notification update with delivery metrics
    async updateEnhancedWebhookNotification(id, status, error_message = null, telegram_message_id = null, options = {}) {
        return new Promise((resolve, reject) => {
            const sent_at = status === 'sent' ? new Date().toISOString() : null;
            const last_attempt_at = options.lastAttemptAt || new Date().toISOString();
            const next_attempt_at = options.nextAttemptAt || null;
            
            // Calculate delivery time if we're marking as sent
            if (status === 'sent') {
                this.db.get('SELECT created_at FROM webhook_notifications WHERE id = ?', [id], (err, row) => {
                    if (err) {
                        reject(err);
                        return;
                    }
                    
                    let delivery_time_ms = null;
                    if (row) {
                        const created = new Date(row.created_at);
                        delivery_time_ms = new Date() - created;
                    }
                    
                    const stmt = this.db.prepare(`
                        UPDATE webhook_notifications 
                        SET status = ?, error_message = ?, sent_at = ?, 
                            telegram_message_id = ?, delivery_time_ms = ?, 
                            last_attempt_at = ?, next_attempt_at = ?
                        WHERE id = ?
                    `);
                    
                    stmt.run([status, error_message, sent_at, telegram_message_id, delivery_time_ms, last_attempt_at, null, id], function(err) {
                        if (err) {
                            reject(err);
                        } else {
                            resolve(this.changes);
                        }
                    });
                    stmt.finalize();
                });
            } else {
                const stmt = this.db.prepare(`
                    UPDATE webhook_notifications 
                    SET status = ?, error_message = ?, retry_count = retry_count + 1,
                        last_attempt_at = ?, next_attempt_at = ?
                    WHERE id = ?
                `);
                
                stmt.run([status, error_message, last_attempt_at, next_attempt_at, id], function(err) {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(this.changes);
                    }
                });
                stmt.finalize();
            }
        });
    }

    // Backward compatibility method
    async updateWebhookNotification(id, status, error_message = null, sent_at = null) {
        return this.updateEnhancedWebhookNotification(id, status, error_message, null);
    }

    // Enhanced pending notifications with priority and retry logic
    async getEnhancedPendingWebhookNotifications(limit = 50, maxRetries = 3) {
        return new Promise((resolve, reject) => {
            const sql = `
                SELECT 
                    wn.*,
                    c.phone_number, 
                    c.call_summary, 
                    c.ai_analysis,
                    c.status as call_status,
                    c.duration as call_duration,
                    c.twilio_status
                FROM webhook_notifications wn
                JOIN calls c ON wn.call_sid = c.call_sid
                WHERE wn.status IN ('pending', 'retrying')
                    AND wn.retry_count < ?
                    AND (wn.next_attempt_at IS NULL OR wn.next_attempt_at <= datetime('now'))
                ORDER BY 
                    CASE wn.priority
                        WHEN 'urgent' THEN 1
                        WHEN 'high' THEN 2
                        WHEN 'normal' THEN 3
                        WHEN 'low' THEN 4
                        ELSE 5
                    END,
                    CASE wn.notification_type
                        WHEN 'call_failed' THEN 1
                        WHEN 'call_completed' THEN 2
                        WHEN 'call_transcript' THEN 3
                        ELSE 4
                    END,
                    wn.created_at ASC
                LIMIT ?
            `;
            
            this.db.all(sql, [maxRetries, limit], (err, rows) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(rows || []);
                }
            });
        });
    }

    // Backward compatibility method
    async getPendingWebhookNotifications() {
        return this.getEnhancedPendingWebhookNotifications(50);
    }

    // FIXED: Enhanced notification metrics logging - Using INSERT OR REPLACE instead of ON CONFLICT
    async logNotificationMetric(notification_type, success, delivery_time_ms = null) {
        const today = new Date().toISOString().split('T')[0];
        
        return new Promise((resolve, reject) => {
            // First try to get existing record
            this.db.get(
                'SELECT * FROM notification_metrics WHERE date = ? AND notification_type = ?',
                [today, notification_type],
                (err, existingRow) => {
                    if (err) {
                        reject(err);
                        return;
                    }

                    const success_increment = success ? 1 : 0;
                    const failure_increment = success ? 0 : 1;
                    const delivery_time = delivery_time_ms || 0;

                    if (existingRow) {
                        // Update existing record
                        const new_total = existingRow.total_count + 1;
                        const new_success = existingRow.success_count + success_increment;
                        const new_failure = existingRow.failure_count + failure_increment;
                        const new_avg_delivery = ((existingRow.avg_delivery_time_ms * existingRow.total_count) + delivery_time) / new_total;

                        const stmt = this.db.prepare(`
                            UPDATE notification_metrics 
                            SET total_count = ?, success_count = ?, failure_count = ?, 
                                avg_delivery_time_ms = ?, updated_at = datetime('now')
                            WHERE id = ?
                        `);
                        
                        stmt.run([new_total, new_success, new_failure, new_avg_delivery, existingRow.id], function(err) {
                            if (err) {
                                reject(err);
                            } else {
                                resolve(this.changes);
                            }
                        });
                        stmt.finalize();
                    } else {
                        // Insert new record
                        const stmt = this.db.prepare(`
                            INSERT INTO notification_metrics 
                            (date, notification_type, total_count, success_count, failure_count, avg_delivery_time_ms)
                            VALUES (?, ?, 1, ?, ?, ?)
                        `);
                        
                        stmt.run([today, notification_type, success_increment, failure_increment, delivery_time], function(err) {
                            if (err) {
                                reject(err);
                            } else {
                                resolve(this.lastID);
                            }
                        });
                        stmt.finalize();
                    }
                }
            );
        });
    }

    // Enhanced service health logging
    async logServiceHealth(service_name, status, details = null) {
        return new Promise((resolve, reject) => {
            const stmt = this.db.prepare(`
                INSERT INTO service_health_logs (service_name, status, details)
                VALUES (?, ?, ?)
            `);
            
            stmt.run([service_name, status, JSON.stringify(details)], function(err) {
                if (err) {
                    reject(err);
                } else {
                    resolve(this.lastID);
                }
            });
            stmt.finalize();
        });
    }

    // Call metrics tracking
    async addCallMetric(call_sid, metric_type, metric_value, metric_data = null) {
        return new Promise((resolve, reject) => {
            const stmt = this.db.prepare(`
                INSERT INTO call_metrics (call_sid, metric_type, metric_value, metric_data)
                VALUES (?, ?, ?, ?)
            `);
            
            stmt.run([call_sid, metric_type, metric_value, JSON.stringify(metric_data)], function(err) {
                if (err) {
                    reject(err);
                } else {
                    resolve(this.lastID);
                }
            });
            stmt.finalize();
        });
    }

    async createCallJob(job_type, payload = {}, run_at = null) {
        const scheduled = run_at || new Date().toISOString();
        return new Promise((resolve, reject) => {
            const stmt = this.db.prepare(`
                INSERT INTO call_jobs (job_type, payload, run_at)
                VALUES (?, ?, ?)
            `);
            stmt.run([job_type, JSON.stringify(payload || {}), scheduled], function(err) {
                if (err) {
                    reject(err);
                } else {
                    resolve(this.lastID);
                }
            });
            stmt.finalize();
        });
    }

    async listCallJobs({ job_type = null, status = null, limit = 20 } = {}) {
        const clauses = [];
        const params = [];
        if (job_type) {
            clauses.push('job_type = ?');
            params.push(job_type);
        }
        if (status) {
            clauses.push('status = ?');
            params.push(status);
        }
        const where = clauses.length ? `WHERE ${clauses.join(' AND ')}` : '';
        const sql = `
            SELECT * FROM call_jobs
            ${where}
            ORDER BY run_at ASC
            LIMIT ?
        `;
        params.push(Math.min(100, Math.max(1, Number(limit) || 20)));
        return new Promise((resolve, reject) => {
            this.db.all(sql, params, (err, rows) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(rows || []);
                }
            });
        });
    }

    async claimDueCallJobs(limit = 10) {
        const now = new Date().toISOString();
        const rows = await new Promise((resolve, reject) => {
            const sql = `
                SELECT * FROM call_jobs
                WHERE status = 'pending' AND run_at <= ?
                ORDER BY run_at ASC
                LIMIT ?
            `;
            this.db.all(sql, [now, limit], (err, result) => {
                if (err) reject(err);
                else resolve(result || []);
            });
        });

        const claimed = [];
        for (const row of rows) {
            const updated = await new Promise((resolve, reject) => {
                const sql = `
                    UPDATE call_jobs
                    SET status = 'running', attempts = attempts + 1, locked_at = ?, updated_at = ?
                    WHERE id = ? AND status = 'pending'
                `;
                this.db.run(sql, [now, now, row.id], function(err) {
                    if (err) reject(err);
                    else resolve(this.changes);
                });
            });
            if (updated) {
                claimed.push({ ...row, attempts: (row.attempts || 0) + 1 });
            }
        }
        return claimed;
    }

    async rescheduleCallJob(job_id, run_at, error = null) {
        const updatedAt = new Date().toISOString();
        return new Promise((resolve, reject) => {
            const sql = `
                UPDATE call_jobs
                SET status = 'pending',
                    run_at = ?,
                    last_error = ?,
                    updated_at = ?,
                    locked_at = NULL
                WHERE id = ?
            `;
            this.db.run(sql, [run_at, error, updatedAt, job_id], function(err) {
                if (err) {
                    reject(err);
                } else {
                    resolve(this.changes);
                }
            });
        });
    }

    async completeCallJob(job_id, status = 'completed', error = null) {
        const updatedAt = new Date().toISOString();
        const completedAt = (status === 'completed' || status === 'failed') ? updatedAt : null;
        return new Promise((resolve, reject) => {
            const sql = `
                UPDATE call_jobs
                SET status = ?,
                    last_error = ?,
                    updated_at = ?,
                    completed_at = COALESCE(?, completed_at),
                    locked_at = NULL
                WHERE id = ?
            `;
            this.db.run(sql, [status, error, updatedAt, completedAt, job_id], function(err) {
                if (err) {
                    reject(err);
                } else {
                    resolve(this.changes);
                }
            });
        });
    }

    async moveCallJobToDlq(job = {}, reason = 'max_attempts_exceeded', error = null) {
        const now = new Date().toISOString();
        const jobId = Number(job?.id);
        if (!Number.isFinite(jobId) || jobId <= 0) {
            throw new Error('Invalid call job id for DLQ');
        }
        const payload = typeof job?.payload === 'string'
            ? job.payload
            : JSON.stringify(job?.payload || {});
        const attempts = Math.max(0, Number(job?.attempts) || 0);
        const sql = `
            INSERT INTO call_job_dlq (
                job_id,
                job_type,
                payload,
                attempts,
                last_error,
                dead_letter_reason,
                status,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, 'open', ?)
            ON CONFLICT(job_id) DO UPDATE SET
                job_type = excluded.job_type,
                payload = excluded.payload,
                attempts = excluded.attempts,
                last_error = excluded.last_error,
                dead_letter_reason = excluded.dead_letter_reason,
                status = 'open',
                updated_at = excluded.updated_at
        `;
        return new Promise((resolve, reject) => {
            this.db.run(sql, [
                jobId,
                String(job?.job_type || 'unknown'),
                payload,
                attempts,
                error || job?.last_error || null,
                reason || null,
                now,
            ], function(err) {
                if (err) {
                    reject(err);
                } else {
                    resolve(this.lastID || jobId);
                }
            });
        });
    }

    async listCallJobDlq({ status = null, limit = 20, offset = 0 } = {}) {
        const params = [];
        const clauses = [];
        if (status && String(status).toLowerCase() !== 'all') {
            clauses.push('status = ?');
            params.push(String(status).toLowerCase());
        }
        const where = clauses.length ? `WHERE ${clauses.join(' AND ')}` : '';
        const sql = `
            SELECT *
            FROM call_job_dlq
            ${where}
            ORDER BY created_at DESC
            LIMIT ? OFFSET ?
        `;
        params.push(Math.min(100, Math.max(1, Number(limit) || 20)));
        params.push(Math.max(0, Number(offset) || 0));
        return new Promise((resolve, reject) => {
            this.db.all(sql, params, (err, rows) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(rows || []);
                }
            });
        });
    }

    async getCallJobDlqEntry(dlqId) {
        return new Promise((resolve, reject) => {
            const sql = 'SELECT * FROM call_job_dlq WHERE id = ?';
            this.db.get(sql, [dlqId], (err, row) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(row || null);
                }
            });
        });
    }

    async countOpenCallJobDlq() {
        return new Promise((resolve, reject) => {
            const sql = `SELECT COUNT(*) AS total FROM call_job_dlq WHERE status = 'open'`;
            this.db.get(sql, [], (err, row) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(Number(row?.total) || 0);
                }
            });
        });
    }

    async markCallJobDlqReplayed(dlqId, replayJobId) {
        const now = new Date().toISOString();
        return new Promise((resolve, reject) => {
            const sql = `
                UPDATE call_job_dlq
                SET replay_count = replay_count + 1,
                    status = 'replayed',
                    last_replay_job_id = ?,
                    replayed_at = ?,
                    updated_at = ?
                WHERE id = ?
            `;
            this.db.run(sql, [replayJobId || null, now, now, dlqId], function(err) {
                if (err) {
                    reject(err);
                } else {
                    resolve(this.changes || 0);
                }
            });
        });
    }

    // FIXED: User session tracking - Using INSERT OR REPLACE instead of ON CONFLICT
    async updateUserSession(telegram_chat_id, call_outcome = null) {
        return new Promise((resolve, reject) => {
            // First try to get existing session
            this.db.get(
                'SELECT * FROM user_sessions WHERE telegram_chat_id = ?',
                [telegram_chat_id],
                (err, existingSession) => {
                    if (err) {
                        reject(err);
                        return;
                    }

                    const success_increment = (call_outcome === 'completed') ? 1 : 0;
                    const failure_increment = (call_outcome && call_outcome !== 'completed') ? 1 : 0;

                    if (existingSession) {
                        // Update existing session
                        const stmt = this.db.prepare(`
                            UPDATE user_sessions 
                            SET total_calls = total_calls + 1,
                                successful_calls = successful_calls + ?,
                                failed_calls = failed_calls + ?,
                                last_activity = datetime('now')
                            WHERE telegram_chat_id = ?
                        `);
                        
                        stmt.run([success_increment, failure_increment, telegram_chat_id], function(err) {
                            if (err) {
                                reject(err);
                            } else {
                                resolve(this.changes);
                            }
                        });
                        stmt.finalize();
                    } else {
                        // Insert new session
                        const stmt = this.db.prepare(`
                            INSERT INTO user_sessions 
                            (telegram_chat_id, total_calls, successful_calls, failed_calls, last_activity)
                            VALUES (?, 1, ?, ?, datetime('now'))
                        `);
                        
                        stmt.run([telegram_chat_id, success_increment, failure_increment], function(err) {
                            if (err) {
                                reject(err);
                            } else {
                                resolve(this.lastID);
                            }
                        });
                        stmt.finalize();
                    }
                }
            );
        });
    }

    // Get enhanced call details
    async getCall(call_sid) {
        return new Promise((resolve, reject) => {
            const sql = `SELECT * FROM calls WHERE call_sid = ?`;
            
            this.db.get(sql, [call_sid], (err, row) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(row);
                }
            });
        });
    }

    // Get enhanced call transcripts (supports both table names)
    async getCallTranscripts(call_sid) {
        return new Promise((resolve, reject) => {
            // Try the legacy table first for backward compatibility
            const sql = `
                SELECT * FROM transcripts 
                WHERE call_sid = ? 
                ORDER BY interaction_count ASC, timestamp ASC
            `;
            
            this.db.all(sql, [call_sid], (err, rows) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(rows || []);
                }
            });
        });
    }

    // Get enhanced calls with comprehensive metrics
    async getCallsWithTranscripts(limit = 50) {
        return new Promise((resolve, reject) => {
            const sql = `
                SELECT c.*, 
                       COUNT(ct.id) as transcript_count,
                       COUNT(CASE WHEN ct.personality_used IS NOT NULL THEN 1 END) as personality_adaptations,
                       GROUP_CONCAT(DISTINCT ct.personality_used) as personalities_used
                FROM calls c
                LEFT JOIN transcripts ct ON c.call_sid = ct.call_sid
                GROUP BY c.call_sid
                ORDER BY c.created_at DESC
                LIMIT ?
            `;
            
            this.db.all(sql, [limit], (err, rows) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(rows || []);
                }
            });
        });
    }

    // Get enhanced notification analytics
    async getNotificationAnalytics(days = 7) {
        return new Promise((resolve, reject) => {
            const sql = `
                SELECT 
                    notification_type,
                    SUM(total_count) as total,
                    SUM(success_count) as successful,
                    SUM(failure_count) as failed,
                    AVG(avg_delivery_time_ms) as avg_delivery_time,
                    COUNT(*) as days_active,
                    MAX(updated_at) as last_updated
                FROM notification_metrics 
                WHERE date >= date('now', '-${days} days')
                GROUP BY notification_type
                ORDER BY total DESC
            `;
            
            this.db.all(sql, [], (err, rows) => {
                if (err) {
                    reject(err);
                } else {
                    const analytics = {
                        period_days: days,
                        total_notifications: 0,
                        total_successful: 0,
                        total_failed: 0,
                        overall_success_rate: 0,
                        avg_delivery_time_ms: 0,
                        breakdown: rows || []
                    };
                    
                    let totalDeliveryTime = 0;
                    let deliveryTimeCount = 0;
                    
                    analytics.breakdown.forEach(row => {
                        analytics.total_notifications += row.total;
                        analytics.total_successful += row.successful;
                        analytics.total_failed += row.failed;
                        
                        if (row.avg_delivery_time && row.total > 0) {
                            totalDeliveryTime += row.avg_delivery_time * row.total;
                            deliveryTimeCount += row.total;
                        }
                    });
                    
                    if (analytics.total_notifications > 0) {
                        analytics.overall_success_rate = 
                            ((analytics.total_successful / analytics.total_notifications) * 100).toFixed(2);
                    }
                    
                    if (deliveryTimeCount > 0) {
                       analytics.avg_delivery_time_ms = (totalDeliveryTime / deliveryTimeCount).toFixed(2);
                   }
                   
                   resolve(analytics);
               }
           });
       });
   }

   // Get comprehensive call statistics
   async getEnhancedCallStats(hours = 24) {
       return new Promise((resolve, reject) => {
           const sql = `
               SELECT 
                   COUNT(*) as total_calls,
                   COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed_calls,
                   COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed_calls,
                   COUNT(CASE WHEN status = 'busy' THEN 1 END) as busy_calls,
                   COUNT(CASE WHEN status = 'no-answer' THEN 1 END) as no_answer_calls,
                   AVG(duration) as avg_duration,
                   AVG(answer_delay) as avg_answer_delay,
                   AVG(ring_duration) as avg_ring_duration,
                   COUNT(CASE WHEN created_at >= datetime('now', '-${hours} hours') THEN 1 END) as recent_calls,
                   COUNT(DISTINCT user_chat_id) as unique_users
               FROM calls
           `;
           
           this.db.get(sql, [], (err, row) => {
               if (err) {
                   reject(err);
               } else {
                   // Calculate success rate
                   const successRate = row.total_calls > 0 ? 
                       ((row.completed_calls / row.total_calls) * 100).toFixed(2) : 0;
                   
                   resolve({
                       ...row,
                       success_rate: successRate,
                       period_hours: hours
                   });
               }
           });
       });
   }

   // Get service health summary
   async getServiceHealthSummary(hours = 24) {
       return new Promise((resolve, reject) => {
           const sql = `
               SELECT 
                   service_name,
                   status,
                   COUNT(*) as count,
                   MAX(timestamp) as last_occurrence
               FROM service_health_logs 
               WHERE timestamp >= datetime('now', '-${hours} hours')
               GROUP BY service_name, status
               ORDER BY service_name, status
           `;
           
           this.db.all(sql, [], (err, rows) => {
               if (err) {
                   reject(err);
               } else {
                   const summary = {
                       period_hours: hours,
                       services: {},
                       total_events: 0
                   };
                   
                   rows.forEach(row => {
                       if (!summary.services[row.service_name]) {
                           summary.services[row.service_name] = {};
                       }
                       summary.services[row.service_name][row.status] = {
                           count: row.count,
                           last_occurrence: row.last_occurrence
                       };
                       summary.total_events += row.count;
                   });
                   
                   resolve(summary);
               }
           });
       });
   }

   async getGptObservabilitySummary(windowMinutes = 60) {
       const safeWindow = Math.max(1, Math.min(1440, Number(windowMinutes) || 60));
       const windowExpr = `-${safeWindow} minutes`;

       const totals = await new Promise((resolve, reject) => {
           const sql = `
               SELECT
                   COUNT(*) as total,
                   SUM(CASE WHEN status = 'ok' THEN 1 ELSE 0 END) as ok_count,
                   SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_count,
                   SUM(CASE WHEN status = 'cached' THEN 1 ELSE 0 END) as cached_count,
                   AVG(CASE WHEN duration_ms IS NOT NULL THEN duration_ms END) as avg_duration_ms
               FROM gpt_tool_audit
               WHERE created_at >= datetime('now', ?)
           `;
           this.db.get(sql, [windowExpr], (err, row) => {
               if (err) reject(err);
               else resolve(row || {});
           });
       });

       const byToolRows = await new Promise((resolve, reject) => {
           const sql = `
               SELECT tool_name, status, COUNT(*) as count
               FROM gpt_tool_audit
               WHERE created_at >= datetime('now', ?)
               GROUP BY tool_name, status
               ORDER BY tool_name ASC, status ASC
           `;
           this.db.all(sql, [windowExpr], (err, rows) => {
               if (err) reject(err);
               else resolve(rows || []);
           });
       });

       const sloRows = await new Promise((resolve, reject) => {
           const sql = `
               SELECT status, COUNT(*) as count
               FROM service_health_logs
               WHERE service_name = 'gpt_slo'
                 AND timestamp >= datetime('now', ?)
               GROUP BY status
           `;
           this.db.all(sql, [windowExpr], (err, rows) => {
               if (err) reject(err);
               else resolve(rows || []);
           });
       });

       const circuitRows = await new Promise((resolve, reject) => {
           const sql = `
               SELECT status, COUNT(*) as count
               FROM service_health_logs
               WHERE service_name = 'gpt_tool_circuit'
                 AND timestamp >= datetime('now', ?)
               GROUP BY status
           `;
           this.db.all(sql, [windowExpr], (err, rows) => {
               if (err) reject(err);
               else resolve(rows || []);
           });
       });

       const idemRows = await new Promise((resolve, reject) => {
           const sql = `
               SELECT status, COUNT(*) as count
               FROM gpt_tool_idempotency
               WHERE updated_at >= datetime('now', ?)
               GROUP BY status
           `;
           this.db.all(sql, [windowExpr], (err, rows) => {
               if (err) reject(err);
               else resolve(rows || []);
           });
       });

       const total = Number(totals?.total) || 0;
       const failed = Number(totals?.failed_count) || 0;
       const failureRate = total > 0 ? failed / total : 0;
       const byTool = {};
       byToolRows.forEach((row) => {
           const toolName = String(row.tool_name || 'unknown');
           if (!byTool[toolName]) {
               byTool[toolName] = { ok: 0, failed: 0, cached: 0, other: 0, total: 0 };
           }
           const status = String(row.status || 'other');
           const count = Number(row.count) || 0;
           if (status === 'ok') byTool[toolName].ok += count;
           else if (status === 'failed') byTool[toolName].failed += count;
           else if (status === 'cached') byTool[toolName].cached += count;
           else byTool[toolName].other += count;
           byTool[toolName].total += count;
       });

       const toMap = (rows = []) => rows.reduce((acc, row) => {
           const key = String(row.status || 'unknown');
           acc[key] = Number(row.count) || 0;
           return acc;
       }, {});

       return {
           window_minutes: safeWindow,
           tool_execution: {
               total,
               ok: Number(totals?.ok_count) || 0,
               failed,
               cached: Number(totals?.cached_count) || 0,
               failure_rate: Number(failureRate.toFixed(4)),
               avg_duration_ms: Math.round(Number(totals?.avg_duration_ms) || 0),
               by_tool: byTool
           },
           slo: toMap(sloRows),
           circuits: toMap(circuitRows),
           idempotency: toMap(idemRows)
       };
   }

   // Create SMS messages table
   async initializeSMSTables() {
       return new Promise((resolve, reject) => {
           const createSMSTable = `CREATE TABLE IF NOT EXISTS sms_messages (
               id INTEGER PRIMARY KEY AUTOINCREMENT,
               message_sid TEXT UNIQUE NOT NULL,
               to_number TEXT,
               from_number TEXT,
               body TEXT NOT NULL,
               status TEXT DEFAULT 'queued',
               direction TEXT NOT NULL CHECK(direction IN ('inbound', 'outbound')),
               provider TEXT,
               error_code TEXT,
               error_message TEXT,
               ai_response TEXT,
               response_message_sid TEXT,
               user_chat_id TEXT,
               created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
               updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
           )`;

           const createBulkSMSTable = `CREATE TABLE IF NOT EXISTS bulk_sms_operations (
               id INTEGER PRIMARY KEY AUTOINCREMENT,
               total_recipients INTEGER NOT NULL,
               successful INTEGER DEFAULT 0,
               failed INTEGER DEFAULT 0,
               message TEXT NOT NULL,
               user_chat_id TEXT,
               created_at DATETIME DEFAULT CURRENT_TIMESTAMP
           )`;

           const createSmsOptOutTable = `CREATE TABLE IF NOT EXISTS sms_opt_outs (
               phone_number TEXT PRIMARY KEY,
               reason TEXT,
               opted_out INTEGER DEFAULT 1,
               updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
           )`;

           const createSmsIdempotencyTable = `CREATE TABLE IF NOT EXISTS sms_idempotency (
               idempotency_key TEXT PRIMARY KEY,
               message_sid TEXT,
               to_number TEXT,
               body_hash TEXT,
               created_at DATETIME DEFAULT CURRENT_TIMESTAMP
           )`;

           this.db.serialize(() => {
               this.db.run(createSMSTable, (err) => {
                   if (err) {
                       console.error('Error creating SMS table:', err);
                       reject(err);
                       return;
                   }
               });
               
               this.db.run(createBulkSMSTable, (err) => {
                   if (err) {
                       console.error('Error creating bulk SMS table:', err);
                       reject(err);
                       return;
                   }
                   this.db.run(createSmsOptOutTable, (optErr) => {
                       if (optErr) {
                           console.error('Error creating sms_opt_outs table:', optErr);
                           reject(optErr);
                           return;
                       }
                       this.db.run(createSmsIdempotencyTable, (idemErr) => {
                           if (idemErr) {
                               console.error('Error creating sms_idempotency table:', idemErr);
                               reject(idemErr);
                               return;
                           }
                           this.ensureSmsColumns(['provider']).then(() => {
                               const smsIndexes = [
                                   'CREATE INDEX IF NOT EXISTS idx_sms_messages_status_updated_at ON sms_messages(status, updated_at)',
                                   'CREATE INDEX IF NOT EXISTS idx_sms_messages_direction_updated_at ON sms_messages(direction, updated_at)',
                                   'CREATE INDEX IF NOT EXISTS idx_sms_messages_provider ON sms_messages(provider)',
                               ];
                               Promise.all(
                                   smsIndexes.map((sql) => new Promise((indexResolve, indexReject) => {
                                       this.db.run(sql, (indexErr) => {
                                           if (indexErr) indexReject(indexErr);
                                           else indexResolve();
                                       });
                                   })),
                               ).then(() => {
                                   console.log('✅ SMS tables created successfully');
                                   resolve();
                               }).catch(reject);
                           }).catch(reject);
                       });
                   });
               });
           });
       });
   }

   async ensureSmsColumns(columns = []) {
       if (!columns.length) return;
       const existing = await new Promise((resolve, reject) => {
           this.db.all('PRAGMA table_info(sms_messages)', (err, rows) => {
               if (err) {
                   reject(err);
               } else {
                   resolve(rows || []);
               }
           });
       });
       const existingNames = new Set(existing.map((row) => row.name));
       const addColumn = (name, definition) => new Promise((resolve, reject) => {
           this.db.run(`ALTER TABLE sms_messages ADD COLUMN ${name} ${definition}`, (err) => {
               if (err) {
                   if (String(err.message || '').includes('duplicate')) resolve();
                   else reject(err);
               } else {
                   resolve();
               }
           });
       });
       for (const column of columns) {
           if (existingNames.has(column)) continue;
           if (column === 'provider') {
               await addColumn('provider', 'TEXT');
           }
       }
   }

   // Create Email tables
   async initializeEmailTables() {
       return new Promise((resolve, reject) => {
           const createEmailMessagesTable = `CREATE TABLE IF NOT EXISTS email_messages (
               id INTEGER PRIMARY KEY AUTOINCREMENT,
               message_id TEXT UNIQUE NOT NULL,
               to_email TEXT NOT NULL,
               from_email TEXT,
               subject TEXT,
               html TEXT,
               text TEXT,
               template_id TEXT,
               variables_json TEXT,
               variables_hash TEXT,
               metadata_json TEXT,
               status TEXT DEFAULT 'queued',
               provider TEXT,
               provider_message_id TEXT,
               provider_response TEXT,
               failure_reason TEXT,
               tenant_id TEXT,
               bulk_job_id TEXT,
               scheduled_at DATETIME,
               last_attempt_at DATETIME,
               next_attempt_at DATETIME,
               retry_count INTEGER DEFAULT 0,
               max_retries INTEGER DEFAULT 5,
               queue_lock_token TEXT,
               queue_lock_expires_at_ms INTEGER,
               created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
               updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
               sent_at DATETIME,
               delivered_at DATETIME,
               failed_at DATETIME,
               suppressed_reason TEXT
           )`;

           const createEmailEventsTable = `CREATE TABLE IF NOT EXISTS email_events (
               id INTEGER PRIMARY KEY AUTOINCREMENT,
               message_id TEXT NOT NULL,
               event_type TEXT NOT NULL,
               provider TEXT,
               timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
               metadata TEXT
           )`;

           const createEmailBulkJobsTable = `CREATE TABLE IF NOT EXISTS email_bulk_jobs (
               job_id TEXT PRIMARY KEY,
               status TEXT DEFAULT 'queued',
               total INTEGER DEFAULT 0,
               queued INTEGER DEFAULT 0,
               sending INTEGER DEFAULT 0,
               sent INTEGER DEFAULT 0,
               failed INTEGER DEFAULT 0,
               delivered INTEGER DEFAULT 0,
               bounced INTEGER DEFAULT 0,
               complained INTEGER DEFAULT 0,
               suppressed INTEGER DEFAULT 0,
               tenant_id TEXT,
               template_id TEXT,
               created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
               updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
               completed_at DATETIME
           )`;

           const createEmailSuppressionTable = `CREATE TABLE IF NOT EXISTS email_suppression (
               email TEXT PRIMARY KEY,
               reason TEXT,
               source TEXT,
               created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
               updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
           )`;

           const createEmailIdempotencyTable = `CREATE TABLE IF NOT EXISTS email_idempotency (
               idempotency_key TEXT PRIMARY KEY,
               message_id TEXT,
               bulk_job_id TEXT,
               request_hash TEXT,
               created_at DATETIME DEFAULT CURRENT_TIMESTAMP
           )`;

           const createEmailProviderEventsTable = `CREATE TABLE IF NOT EXISTS email_provider_events (
               id INTEGER PRIMARY KEY AUTOINCREMENT,
               event_key TEXT UNIQUE NOT NULL,
               message_id TEXT,
               provider TEXT,
               event_type TEXT,
               reason TEXT,
               payload_json TEXT,
               created_at DATETIME DEFAULT CURRENT_TIMESTAMP
           )`;

           const createEmailDlqTable = `CREATE TABLE IF NOT EXISTS email_dlq (
               id INTEGER PRIMARY KEY AUTOINCREMENT,
               message_id TEXT NOT NULL,
               reason TEXT,
               payload TEXT,
               status TEXT DEFAULT 'open',
               replay_count INTEGER DEFAULT 0,
               replayed_at DATETIME,
               updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
               last_replay_error TEXT,
               created_at DATETIME DEFAULT CURRENT_TIMESTAMP
           )`;

           const createEmailTemplatesTable = `CREATE TABLE IF NOT EXISTS email_templates (
               template_id TEXT PRIMARY KEY,
               subject TEXT,
               html TEXT,
               text TEXT,
               required_vars TEXT,
               created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
               updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
           )`;

           const createEmailMetricsTable = `CREATE TABLE IF NOT EXISTS email_metrics (
               id INTEGER PRIMARY KEY AUTOINCREMENT,
               date TEXT NOT NULL,
               metric_type TEXT NOT NULL,
               total_count INTEGER DEFAULT 0,
               created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
               updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
               UNIQUE(date, metric_type)
           )`;

           const indexes = [
               'CREATE INDEX IF NOT EXISTS idx_email_messages_status ON email_messages(status)',
               'CREATE INDEX IF NOT EXISTS idx_email_messages_to_email ON email_messages(to_email)',
               'CREATE INDEX IF NOT EXISTS idx_email_messages_bulk_job_id ON email_messages(bulk_job_id)',
               'CREATE INDEX IF NOT EXISTS idx_email_messages_provider_message_id ON email_messages(provider_message_id)',
               'CREATE INDEX IF NOT EXISTS idx_email_messages_created_at ON email_messages(created_at)',
               'CREATE INDEX IF NOT EXISTS idx_email_events_message_id ON email_events(message_id)',
               'CREATE INDEX IF NOT EXISTS idx_email_events_type ON email_events(event_type)',
               'CREATE INDEX IF NOT EXISTS idx_email_events_timestamp ON email_events(timestamp)',
               'CREATE INDEX IF NOT EXISTS idx_email_bulk_status ON email_bulk_jobs(status)',
               'CREATE INDEX IF NOT EXISTS idx_email_bulk_tenant ON email_bulk_jobs(tenant_id)',
               'CREATE INDEX IF NOT EXISTS idx_email_suppression_email ON email_suppression(email)',
               'CREATE INDEX IF NOT EXISTS idx_email_provider_events_key ON email_provider_events(event_key)',
               'CREATE INDEX IF NOT EXISTS idx_email_provider_events_message_id ON email_provider_events(message_id)',
               'CREATE INDEX IF NOT EXISTS idx_email_provider_events_created_at ON email_provider_events(created_at)'
           ];

           this.db.serialize(() => {
               this.db.run(createEmailMessagesTable, (err) => {
                   if (err) {
                       console.error('Error creating email_messages table:', err);
                       reject(err);
                       return;
                   }
               });
               this.db.run(createEmailEventsTable, (err) => {
                   if (err) {
                       console.error('Error creating email_events table:', err);
                       reject(err);
                       return;
                   }
               });
               this.db.run(createEmailBulkJobsTable, (err) => {
                   if (err) {
                       console.error('Error creating email_bulk_jobs table:', err);
                       reject(err);
                       return;
                   }
               });
               this.db.run(createEmailSuppressionTable, (err) => {
                   if (err) {
                       console.error('Error creating email_suppression table:', err);
                       reject(err);
                       return;
                   }
               });
               this.db.run(createEmailIdempotencyTable, (err) => {
                   if (err) {
                       console.error('Error creating email_idempotency table:', err);
                       reject(err);
                       return;
                   }
               });
               this.db.run(createEmailProviderEventsTable, (err) => {
                   if (err) {
                       console.error('Error creating email_provider_events table:', err);
                       reject(err);
                       return;
                   }
               });
               this.db.run(createEmailDlqTable, (err) => {
                   if (err) {
                       console.error('Error creating email_dlq table:', err);
                       reject(err);
                       return;
                   }
               });
               this.db.run(createEmailTemplatesTable, (err) => {
                   if (err) {
                       console.error('Error creating email_templates table:', err);
                       reject(err);
                       return;
                   }
               });
               this.db.run(createEmailMetricsTable, (err) => {
                   if (err) {
                       console.error('Error creating email_metrics table:', err);
                       reject(err);
                       return;
                   }
               });

               let indexErrors = null;
               indexes.forEach((stmt) => {
                   this.db.run(stmt, (err) => {
                       if (err) {
                           indexErrors = err;
                           console.error('Error creating email index:', err);
                       }
                   });
               });
               if (indexErrors) {
                   reject(indexErrors);
                   return;
               }
               console.log('✅ Email tables created successfully');
               resolve();
           });
       });
   }

   async ensureEmailDlqColumns() {
       if (this.emailDlqColumnsEnsured) {
           return;
       }
       const existing = await new Promise((resolve, reject) => {
           this.db.all('PRAGMA table_info(email_dlq)', (err, rows) => {
               if (err) {
                   reject(err);
               } else {
                   resolve(rows || []);
               }
           });
       });
       const names = new Set(existing.map((row) => row.name));
       const alterStatements = [];
       if (!names.has('status')) {
           alterStatements.push(`ALTER TABLE email_dlq ADD COLUMN status TEXT DEFAULT 'open'`);
       }
       if (!names.has('replay_count')) {
           alterStatements.push(`ALTER TABLE email_dlq ADD COLUMN replay_count INTEGER DEFAULT 0`);
       }
       if (!names.has('updated_at')) {
           alterStatements.push(`ALTER TABLE email_dlq ADD COLUMN updated_at DATETIME DEFAULT CURRENT_TIMESTAMP`);
       }
       if (!names.has('replayed_at')) {
           alterStatements.push(`ALTER TABLE email_dlq ADD COLUMN replayed_at DATETIME`);
       }
       if (!names.has('last_replay_error')) {
           alterStatements.push(`ALTER TABLE email_dlq ADD COLUMN last_replay_error TEXT`);
       }
       for (const stmt of alterStatements) {
           await new Promise((resolve, reject) => {
               this.db.run(stmt, (err) => {
                   if (err) {
                       const message = String(err.message || '').toLowerCase();
                       if (message.includes('duplicate column name')) {
                           resolve();
                           return;
                       }
                       reject(err);
                   } else {
                       resolve();
                   }
               });
           });
       }
       await new Promise((resolve, reject) => {
           this.db.run(
               `CREATE INDEX IF NOT EXISTS idx_email_dlq_status ON email_dlq(status)`,
               (err) => {
                   if (err) reject(err);
                   else resolve();
               },
           );
       });
       await new Promise((resolve, reject) => {
           this.db.run(
               `CREATE INDEX IF NOT EXISTS idx_email_dlq_created_at ON email_dlq(created_at)`,
               (err) => {
                   if (err) reject(err);
                   else resolve();
               },
           );
       });
       this.emailDlqColumnsEnsured = true;
   }

   async ensureEmailQueueColumns() {
       if (this.emailQueueColumnsEnsured) {
           return;
       }
       const existing = await new Promise((resolve, reject) => {
           this.db.all('PRAGMA table_info(email_messages)', (err, rows) => {
               if (err) {
                   reject(err);
               } else {
                   resolve(rows || []);
               }
           });
       });
       const names = new Set(existing.map((row) => row.name));
       const alterStatements = [];
       if (!names.has('queue_lock_token')) {
           alterStatements.push('ALTER TABLE email_messages ADD COLUMN queue_lock_token TEXT');
       }
       if (!names.has('queue_lock_expires_at_ms')) {
           alterStatements.push('ALTER TABLE email_messages ADD COLUMN queue_lock_expires_at_ms INTEGER');
       }
       for (const stmt of alterStatements) {
           await new Promise((resolve, reject) => {
               this.db.run(stmt, (err) => {
                   if (err) {
                       const message = String(err.message || '').toLowerCase();
                       if (message.includes('duplicate column name')) {
                           resolve();
                           return;
                       }
                       reject(err);
                   } else {
                       resolve();
                   }
               });
           });
       }
       await new Promise((resolve, reject) => {
           this.db.run(
               'CREATE INDEX IF NOT EXISTS idx_email_messages_queue_lock ON email_messages(queue_lock_expires_at_ms)',
               (err) => {
                   if (err) reject(err);
                   else resolve();
               },
           );
       });
       this.emailQueueColumnsEnsured = true;
   }

   // Save SMS message
   async saveSMSMessage(messageData) {
       return new Promise((resolve, reject) => {
           const sql = `INSERT INTO sms_messages (
               message_sid, to_number, from_number, body, status, 
               direction, provider, ai_response, response_message_sid, user_chat_id
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;

           this.db.run(sql, [
               messageData.message_sid,
               messageData.to_number || null,
               messageData.from_number || null,
               messageData.body,
               messageData.status || 'queued',
               messageData.direction,
               messageData.provider || null,
               messageData.ai_response || null,
               messageData.response_message_sid || null,
               messageData.user_chat_id || null
           ], function (err) {
               if (err) {
                   console.error('Error saving SMS message:', err);
                   reject(err);
               } else {
                   resolve(this.lastID);
               }
           });
       });
   }

   // Update SMS status
   async updateSMSStatus(messageSid, statusData) {
       return new Promise((resolve, reject) => {
           const sql = `UPDATE sms_messages 
               SET status = ?, error_code = ?, error_message = ?, updated_at = CURRENT_TIMESTAMP 
               WHERE message_sid = ?`;

           this.db.run(sql, [
               statusData.status,
               statusData.error_code || null,
               statusData.error_message || null,
               messageSid
           ], function (err) {
               if (err) {
                   console.error('Error updating SMS status:', err);
                   reject(err);
               } else {
                   resolve(this.changes);
               }
           });
       });
   }

   async setSmsOptOut(phoneNumber, reason = null) {
       return new Promise((resolve, reject) => {
           const sql = `INSERT INTO sms_opt_outs (phone_number, reason, opted_out, updated_at)
               VALUES (?, ?, 1, CURRENT_TIMESTAMP)
               ON CONFLICT(phone_number) DO UPDATE SET
               reason = excluded.reason,
               opted_out = 1,
               updated_at = CURRENT_TIMESTAMP`;
           this.db.run(sql, [phoneNumber, reason], function (err) {
               if (err) {
                   console.error('Error setting SMS opt-out:', err);
                   reject(err);
               } else {
                   resolve(true);
               }
           });
       });
   }

   async clearSmsOptOut(phoneNumber) {
       return new Promise((resolve, reject) => {
           const sql = `UPDATE sms_opt_outs SET opted_out = 0, updated_at = CURRENT_TIMESTAMP WHERE phone_number = ?`;
           this.db.run(sql, [phoneNumber], function (err) {
               if (err) {
                   console.error('Error clearing SMS opt-out:', err);
                   reject(err);
               } else {
                   resolve(true);
               }
           });
       });
   }

   async isSmsOptedOut(phoneNumber) {
       return new Promise((resolve, reject) => {
           const sql = `SELECT opted_out FROM sms_opt_outs WHERE phone_number = ?`;
           this.db.get(sql, [phoneNumber], (err, row) => {
               if (err) {
                   console.error('Error checking SMS opt-out:', err);
                   reject(err);
               } else {
                   resolve(row ? row.opted_out === 1 : false);
               }
           });
       });
   }

   async saveSmsIdempotency(idempotencyKey, messageSid, toNumber, bodyHash) {
       return new Promise((resolve, reject) => {
           const sql = `INSERT OR IGNORE INTO sms_idempotency (idempotency_key, message_sid, to_number, body_hash)
               VALUES (?, ?, ?, ?)`;
           this.db.run(sql, [idempotencyKey, messageSid, toNumber, bodyHash], function (err) {
               if (err) {
                   console.error('Error saving SMS idempotency:', err);
                   reject(err);
               } else {
                   resolve(true);
               }
           });
       });
   }

   async checkAndConsumeOutboundRateLimit({
       scope,
       key,
       limit,
       windowMs,
       nowMs = Date.now(),
   } = {}) {
       const safeLimit = Number(limit);
       const safeWindowMs = Number(windowMs);
       if (!Number.isFinite(safeLimit) || safeLimit <= 0) {
           return { allowed: true, retryAfterMs: 0, count: 0 };
       }
       if (!Number.isFinite(safeWindowMs) || safeWindowMs <= 0) {
           return { allowed: true, retryAfterMs: 0, count: 0 };
       }

       const resolvedScope = String(scope || 'outbound');
       const resolvedKey = String(key || 'anonymous');
       const bucketStart = Math.floor(Number(nowMs) / safeWindowMs) * safeWindowMs;

       const upsertSql = `
           INSERT INTO outbound_rate_limits (scope, actor_key, window_start, count, updated_at)
           VALUES (?, ?, ?, 1, CURRENT_TIMESTAMP)
           ON CONFLICT(scope, actor_key) DO UPDATE SET
               count = CASE
                   WHEN outbound_rate_limits.window_start = excluded.window_start THEN
                       outbound_rate_limits.count + 1
                   ELSE 1
               END,
               window_start = excluded.window_start,
               updated_at = CURRENT_TIMESTAMP
       `;

       await new Promise((resolve, reject) => {
           this.db.run(
               upsertSql,
               [resolvedScope, resolvedKey, bucketStart],
               (err) => {
                   if (err) reject(err);
                   else resolve();
               },
           );
       });

       const row = await new Promise((resolve, reject) => {
           this.db.get(
               `SELECT count, window_start FROM outbound_rate_limits WHERE scope = ? AND actor_key = ?`,
               [resolvedScope, resolvedKey],
               (err, result) => {
                   if (err) reject(err);
                   else resolve(result || null);
               },
           );
       });

       const count = Number(row?.count || 0);
       const windowStart = Number(row?.window_start || bucketStart);
       const retryAfterMs = Math.max(0, windowStart + safeWindowMs - Number(nowMs));
       const allowed = count <= safeLimit;

       const cleanupIntervalMs = Math.max(60000, safeWindowMs * 10);
       if (Number(nowMs) - this.outboundRateLastCleanupMs >= cleanupIntervalMs) {
           this.outboundRateLastCleanupMs = Number(nowMs);
           const staleBeforeMs = Number(nowMs) - Math.max(cleanupIntervalMs, safeWindowMs);
           this.cleanupOutboundRateLimits(staleBeforeMs).catch(() => {});
       }

       return {
           allowed,
           retryAfterMs: allowed ? 0 : retryAfterMs,
           count,
           limit: safeLimit,
           windowStart,
       };
   }

   async cleanupOutboundRateLimits(staleBeforeMs) {
       const threshold = Number(staleBeforeMs);
       if (!Number.isFinite(threshold)) return 0;
       return new Promise((resolve, reject) => {
           this.db.run(
               `DELETE FROM outbound_rate_limits WHERE window_start < ?`,
               [threshold],
               function (err) {
                   if (err) reject(err);
                   else resolve(this.changes || 0);
               },
           );
       });
   }

   async getSmsIdempotency(idempotencyKey) {
       return new Promise((resolve, reject) => {
           const sql = `SELECT * FROM sms_idempotency WHERE idempotency_key = ?`;
           this.db.get(sql, [idempotencyKey], (err, row) => {
               if (err) {
                   console.error('Error fetching SMS idempotency:', err);
                   reject(err);
               } else {
                   resolve(row || null);
               }
           });
       });
   }

   // Log bulk SMS operation
   async logBulkSMSOperation(operationData) {
       return new Promise((resolve, reject) => {
           const sql = `INSERT INTO bulk_sms_operations (
               total_recipients, successful, failed, message, user_chat_id
           ) VALUES (?, ?, ?, ?, ?)`;

           this.db.run(sql, [
               operationData.total_recipients,
               operationData.successful,
               operationData.failed,
               operationData.message,
               operationData.user_chat_id || null
           ], function (err) {
               if (err) {
                   console.error('Error logging bulk SMS operation:', err);
                   reject(err);
               } else {
                   resolve(this.lastID);
               }
           });
       });
   }

   // Get SMS messages
   async getSMSMessages(limit = 50, offset = 0) {
       return new Promise((resolve, reject) => {
           const sql = `SELECT * FROM sms_messages 
               ORDER BY created_at DESC 
               LIMIT ? OFFSET ?`;

           this.db.all(sql, [limit, offset], (err, rows) => {
               if (err) {
                   console.error('Error getting SMS messages:', err);
                   reject(err);
               } else {
                   resolve(rows || []);
               }
           });
       });
   }

   async getSmsMessagesForReconcile({ statuses = [], olderThanMinutes = 15, limit = 50 } = {}) {
       const normalizedStatuses = Array.isArray(statuses)
           ? statuses
               .map((item) => String(item || '').trim().toLowerCase())
               .filter(Boolean)
           : [];
       if (!normalizedStatuses.length) {
           return [];
       }
       const safeLimit = Math.max(1, Math.min(500, Number(limit) || 50));
       const staleMinutes = Math.max(1, Number(olderThanMinutes) || 15);
       const placeholders = normalizedStatuses.map(() => '?').join(', ');
       const sql = `
           SELECT message_sid, status, provider, error_code, error_message, updated_at
           FROM sms_messages
           WHERE direction = 'outbound'
             AND message_sid IS NOT NULL
             AND LOWER(COALESCE(status, '')) IN (${placeholders})
             AND datetime(updated_at) <= datetime('now', ?)
           ORDER BY datetime(updated_at) ASC
           LIMIT ?
       `;
       const staleExpr = `-${staleMinutes} minutes`;
       return new Promise((resolve, reject) => {
           this.db.all(
               sql,
               [...normalizedStatuses, staleExpr, safeLimit],
               (err, rows) => {
                   if (err) {
                       console.error('Error getting stale SMS messages for reconcile:', err);
                       reject(err);
                   } else {
                       resolve(rows || []);
                   }
               },
           );
       });
   }

   // Get SMS conversation
   async getSMSConversation(phoneNumber, limit = 50) {
       return new Promise((resolve, reject) => {
           const sql = `SELECT * FROM sms_messages 
               WHERE to_number = ? OR from_number = ? 
               ORDER BY created_at ASC 
               LIMIT ?`;

           this.db.all(sql, [phoneNumber, phoneNumber, limit], (err, rows) => {
               if (err) {
                   console.error('Error getting SMS conversation:', err);
                   reject(err);
               } else {
                   resolve(rows || []);
               }
           });
       });
   }

   // Save Email message
   async saveEmailMessage(messageData) {
       return new Promise((resolve, reject) => {
           const sql = `INSERT INTO email_messages (
               message_id, to_email, from_email, subject, html, text,
               template_id, variables_json, variables_hash, metadata_json,
               status, provider, tenant_id, bulk_job_id, scheduled_at, max_retries
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;
           this.db.run(sql, [
               messageData.message_id,
               messageData.to_email,
               messageData.from_email || null,
               messageData.subject || null,
               messageData.html || null,
               messageData.text || null,
               messageData.template_id || null,
               messageData.variables_json || null,
               messageData.variables_hash || null,
               messageData.metadata_json || null,
               messageData.status || 'queued',
               messageData.provider || null,
               messageData.tenant_id || null,
               messageData.bulk_job_id || null,
               messageData.scheduled_at || null,
               messageData.max_retries || 5
           ], function (err) {
               if (err) {
                   console.error('Error saving email message:', err);
                   reject(err);
               } else {
                   resolve(this.lastID);
               }
           });
       });
   }

   async getEmailMessage(messageId) {
       return new Promise((resolve, reject) => {
           const sql = `SELECT * FROM email_messages WHERE message_id = ?`;
           this.db.get(sql, [messageId], (err, row) => {
               if (err) {
                   console.error('Error fetching email message:', err);
                   reject(err);
               } else {
                   resolve(row || null);
               }
           });
       });
   }

   async getEmailMessageByProviderId(providerMessageId) {
       return new Promise((resolve, reject) => {
           const sql = `SELECT * FROM email_messages WHERE provider_message_id = ?`;
           this.db.get(sql, [providerMessageId], (err, row) => {
               if (err) {
                   console.error('Error fetching email message by provider id:', err);
                   reject(err);
               } else {
                   resolve(row || null);
               }
           });
       });
   }

   async getEmailMessageByProviderId(providerMessageId) {
       return new Promise((resolve, reject) => {
           const sql = `SELECT * FROM email_messages WHERE provider_message_id = ?`;
           this.db.get(sql, [providerMessageId], (err, row) => {
               if (err) {
                   console.error('Error fetching email message by provider id:', err);
                   reject(err);
               } else {
                   resolve(row || null);
               }
           });
       });
   }

   async listEmailEvents(messageId) {
       return new Promise((resolve, reject) => {
           const sql = `SELECT * FROM email_events WHERE message_id = ? ORDER BY timestamp ASC`;
           this.db.all(sql, [messageId], (err, rows) => {
               if (err) {
                   console.error('Error fetching email events:', err);
                   reject(err);
               } else {
                   resolve(rows || []);
               }
           });
       });
   }

   async addEmailEvent(messageId, eventType, metadata = null, provider = null) {
       return new Promise((resolve, reject) => {
           const sql = `INSERT INTO email_events (message_id, event_type, provider, metadata)
               VALUES (?, ?, ?, ?)`;
           this.db.run(sql, [
               messageId,
               eventType,
               provider || null,
               metadata ? JSON.stringify(metadata) : null
           ], function (err) {
               if (err) {
                   console.error('Error adding email event:', err);
                   reject(err);
               } else {
                   resolve(this.lastID);
               }
           });
       });
   }

   async saveEmailProviderEvent(event = {}) {
       const eventKey = String(event.event_key || event.eventKey || '').trim();
       if (!eventKey) {
           return false;
       }
       return new Promise((resolve, reject) => {
           const sql = `INSERT OR IGNORE INTO email_provider_events (
               event_key, message_id, provider, event_type, reason, payload_json
           ) VALUES (?, ?, ?, ?, ?, ?)`;
           this.db.run(
               sql,
               [
                   eventKey,
                   event.message_id || null,
                   event.provider || null,
                   event.event_type || null,
                   event.reason || null,
                   event.payload_json || null,
               ],
               function (err) {
                   if (err) {
                       console.error('Error saving email provider event:', err);
                       reject(err);
                   } else {
                       resolve((this.changes || 0) > 0);
                   }
               },
           );
       });
   }

   async cleanupExpiredEmailProviderEvents(daysToKeep = 14) {
       const safeDays = Math.max(1, Number(daysToKeep) || 14);
       return new Promise((resolve, reject) => {
           const sql = `DELETE FROM email_provider_events WHERE created_at < datetime('now', ?)`;
           this.db.run(sql, [`-${safeDays} days`], function (err) {
               if (err) {
                   console.error('Error cleaning email provider events:', err);
                   reject(err);
               } else {
                   resolve(this.changes || 0);
               }
           });
       });
   }

   async updateEmailMessageStatus(messageId, updates = {}) {
       return new Promise((resolve, reject) => {
           const fields = [];
           const params = [];
           const setField = (name, value) => {
               fields.push(`${name} = ?`);
               params.push(value);
           };
           if (updates.status) setField('status', updates.status);
           if (Object.prototype.hasOwnProperty.call(updates, 'failure_reason')) setField('failure_reason', updates.failure_reason);
           if (Object.prototype.hasOwnProperty.call(updates, 'provider_message_id')) setField('provider_message_id', updates.provider_message_id);
           if (Object.prototype.hasOwnProperty.call(updates, 'provider_response')) setField('provider_response', updates.provider_response);
           if (Object.prototype.hasOwnProperty.call(updates, 'last_attempt_at')) setField('last_attempt_at', updates.last_attempt_at);
           if (Object.prototype.hasOwnProperty.call(updates, 'next_attempt_at')) setField('next_attempt_at', updates.next_attempt_at);
           if (Object.prototype.hasOwnProperty.call(updates, 'retry_count')) setField('retry_count', updates.retry_count);
           if (Object.prototype.hasOwnProperty.call(updates, 'sent_at')) setField('sent_at', updates.sent_at);
           if (Object.prototype.hasOwnProperty.call(updates, 'delivered_at')) setField('delivered_at', updates.delivered_at);
           if (Object.prototype.hasOwnProperty.call(updates, 'failed_at')) setField('failed_at', updates.failed_at);
           if (Object.prototype.hasOwnProperty.call(updates, 'suppressed_reason')) setField('suppressed_reason', updates.suppressed_reason);
           if (Object.prototype.hasOwnProperty.call(updates, 'queue_lock_token')) setField('queue_lock_token', updates.queue_lock_token);
           if (Object.prototype.hasOwnProperty.call(updates, 'queue_lock_expires_at_ms')) setField('queue_lock_expires_at_ms', updates.queue_lock_expires_at_ms);
           fields.push('updated_at = CURRENT_TIMESTAMP');
           params.push(messageId);
           const sql = `UPDATE email_messages SET ${fields.join(', ')} WHERE message_id = ?`;
           this.db.run(sql, params, function (err) {
               if (err) {
                   console.error('Error updating email message:', err);
                   reject(err);
               } else {
                   resolve(this.changes);
               }
           });
       });
   }

   async getPendingEmailMessages(limit = 10) {
       return new Promise((resolve, reject) => {
           const sql = `SELECT * FROM email_messages
               WHERE status IN ('queued', 'retry')
               AND (scheduled_at IS NULL OR scheduled_at <= CURRENT_TIMESTAMP)
               AND (next_attempt_at IS NULL OR next_attempt_at <= CURRENT_TIMESTAMP)
               ORDER BY created_at ASC
               LIMIT ?`;
           this.db.all(sql, [limit], (err, rows) => {
               if (err) {
                   console.error('Error fetching pending email messages:', err);
                   reject(err);
               } else {
                   resolve(rows || []);
               }
           });
       });
   }

   async claimPendingEmailMessages(limit = 10, options = {}) {
       await this.ensureEmailQueueColumns();
       const safeLimit = Math.max(1, Math.min(100, Number(limit) || 10));
       const leaseMs = Math.max(1000, Number(options.leaseMs) || 60000);
       const staleSendingMs = Math.max(15000, Number(options.staleSendingMs) || leaseMs * 2);
       const nowMs = Date.now();
       const expiresAtMs = nowMs + leaseMs;
       const token = `emailq_${nowMs}_${Math.random().toString(36).slice(2, 10)}`;

       const sqlClaim = `
           UPDATE email_messages
           SET queue_lock_token = ?, queue_lock_expires_at_ms = ?, updated_at = CURRENT_TIMESTAMP
           WHERE id IN (
               SELECT id
               FROM email_messages
               WHERE (
                   (
                       status IN ('queued', 'retry')
                       AND (scheduled_at IS NULL OR scheduled_at <= CURRENT_TIMESTAMP)
                       AND (next_attempt_at IS NULL OR next_attempt_at <= CURRENT_TIMESTAMP)
                   )
                   OR (
                       status = 'sending'
                       AND last_attempt_at IS NOT NULL
                       AND (strftime('%s', 'now') - strftime('%s', last_attempt_at)) * 1000 >= ?
                   )
               )
               AND (queue_lock_expires_at_ms IS NULL OR queue_lock_expires_at_ms <= ?)
               ORDER BY created_at ASC
               LIMIT ?
           )
       `;

       await new Promise((resolve, reject) => {
           this.db.run(sqlClaim, [token, expiresAtMs, staleSendingMs, nowMs, safeLimit], (err) => {
               if (err) {
                   console.error('Error claiming pending email messages:', err);
                   reject(err);
               } else {
                   resolve();
               }
           });
       });

       return new Promise((resolve, reject) => {
           const sql = `SELECT * FROM email_messages WHERE queue_lock_token = ? ORDER BY created_at ASC`;
           this.db.all(sql, [token], (err, rows) => {
               if (err) {
                   console.error('Error fetching claimed email messages:', err);
                   reject(err);
               } else {
                   resolve(rows || []);
               }
           });
       });
   }

   async releaseEmailMessageClaim(messageId, lockToken = null) {
       await this.ensureEmailQueueColumns();
       if (!messageId) {
           return 0;
       }
       return new Promise((resolve, reject) => {
           const hasToken = typeof lockToken === 'string' && lockToken.length > 0;
           const sql = hasToken
               ? `UPDATE email_messages
                   SET queue_lock_token = NULL, queue_lock_expires_at_ms = NULL, updated_at = CURRENT_TIMESTAMP
                   WHERE message_id = ? AND queue_lock_token = ?`
               : `UPDATE email_messages
                   SET queue_lock_token = NULL, queue_lock_expires_at_ms = NULL, updated_at = CURRENT_TIMESTAMP
                   WHERE message_id = ?`;
           const params = hasToken ? [messageId, lockToken] : [messageId];
           this.db.run(sql, params, function (err) {
               if (err) {
                   console.error('Error releasing email message claim:', err);
                   reject(err);
               } else {
                   resolve(this.changes || 0);
               }
           });
       });
   }

   async createEmailBulkJob(jobData) {
       return new Promise((resolve, reject) => {
           const sql = `INSERT INTO email_bulk_jobs (
               job_id, status, total, queued, sending, sent, failed, delivered, bounced, complained, suppressed, tenant_id, template_id
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;
           this.db.run(sql, [
               jobData.job_id,
               jobData.status || 'queued',
               jobData.total || 0,
               jobData.queued || 0,
               jobData.sending || 0,
               jobData.sent || 0,
               jobData.failed || 0,
               jobData.delivered || 0,
               jobData.bounced || 0,
               jobData.complained || 0,
               jobData.suppressed || 0,
               jobData.tenant_id || null,
               jobData.template_id || null
           ], function (err) {
               if (err) {
                   console.error('Error creating email bulk job:', err);
                   reject(err);
               } else {
                   resolve(this.lastID);
               }
           });
       });
   }

   async updateEmailBulkJob(jobId, updates = {}) {
       return new Promise((resolve, reject) => {
           const fields = [];
           const params = [];
           const setField = (name, value) => {
               fields.push(`${name} = ?`);
               params.push(value);
           };
           Object.entries(updates).forEach(([key, value]) => {
               setField(key, value);
           });
           fields.push('updated_at = CURRENT_TIMESTAMP');
           params.push(jobId);
           const sql = `UPDATE email_bulk_jobs SET ${fields.join(', ')} WHERE job_id = ?`;
           this.db.run(sql, params, function (err) {
               if (err) {
                   console.error('Error updating email bulk job:', err);
                   reject(err);
               } else {
                   resolve(this.changes);
               }
           });
       });
   }

  async getEmailBulkJob(jobId) {
      return new Promise((resolve, reject) => {
          const sql = `SELECT * FROM email_bulk_jobs WHERE job_id = ?`;
          this.db.get(sql, [jobId], (err, row) => {
              if (err) {
                  console.error('Error fetching email bulk job:', err);
                  reject(err);
              } else {
                  resolve(row || null);
              }
          });
      });
  }

  async getEmailBulkJobs({ limit = 10, offset = 0 } = {}) {
      return new Promise((resolve, reject) => {
          const sql = `SELECT job_id, status, total, queued, sending, sent, failed, delivered, bounced, complained, suppressed, template_id, created_at, updated_at, completed_at
                       FROM email_bulk_jobs
                       ORDER BY created_at DESC
                       LIMIT ? OFFSET ?`;
          this.db.all(sql, [limit, offset], (err, rows) => {
              if (err) {
                  console.error('Error fetching email bulk jobs:', err);
                  reject(err);
              } else {
                  resolve(rows || []);
              }
          });
      });
  }

  async getEmailBulkStats({ hours = 24 } = {}) {
      return new Promise((resolve, reject) => {
          const window = `-${Math.max(hours, 1)} hours`;
          const sql = `SELECT
                           COUNT(*) as total_jobs,
                           COALESCE(SUM(total), 0) as total_recipients,
                           COALESCE(SUM(sent), 0) as sent,
                           COALESCE(SUM(failed), 0) as failed,
                           COALESCE(SUM(delivered), 0) as delivered,
                           COALESCE(SUM(bounced), 0) as bounced,
                           COALESCE(SUM(complained), 0) as complained,
                           COALESCE(SUM(suppressed), 0) as suppressed
                       FROM email_bulk_jobs
                       WHERE created_at >= datetime('now', ?)`;
          this.db.get(sql, [window], (err, row) => {
              if (err) {
                  console.error('Error fetching email bulk stats:', err);
                  reject(err);
              } else {
                  resolve(row || null);
              }
          });
      });
  }

   async getEmailTemplate(templateId) {
       return new Promise((resolve, reject) => {
           const sql = `SELECT * FROM email_templates WHERE template_id = ?`;
           this.db.get(sql, [templateId], (err, row) => {
               if (err) {
                   console.error('Error fetching email template:', err);
                   reject(err);
               } else {
                   resolve(row || null);
               }
           });
       });
   }

   async listEmailTemplates(limit = 50) {
       return new Promise((resolve, reject) => {
           const sql = `
               SELECT template_id, subject, required_vars, created_at, updated_at
               FROM email_templates
               ORDER BY updated_at DESC
               LIMIT ?
           `;
           this.db.all(sql, [limit], (err, rows) => {
               if (err) {
                   console.error('Error listing email templates:', err);
                   reject(err);
               } else {
                   resolve(rows || []);
               }
           });
       });
   }

   async createEmailTemplate(payload) {
       const {
           template_id,
           subject = '',
           html = '',
           text = '',
           required_vars = null
       } = payload || {};
       return new Promise((resolve, reject) => {
           const sql = `
               INSERT INTO email_templates (
                   template_id, subject, html, text, required_vars, created_at, updated_at
               )
               VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
           `;
           this.db.run(
               sql,
               [template_id, subject, html, text, required_vars],
               function (err) {
                   if (err) {
                       console.error('Error creating email template:', err);
                       reject(err);
                   } else {
                       resolve(this.changes);
                   }
               }
           );
       });
   }

   async updateEmailTemplate(templateId, payload) {
       const fields = [];
       const values = [];
       const mapping = {
           subject: 'subject',
           html: 'html',
           text: 'text',
           required_vars: 'required_vars'
       };
       Object.entries(mapping).forEach(([key, column]) => {
           if (payload[key] !== undefined) {
               fields.push(`${column} = ?`);
               values.push(payload[key]);
           }
       });
       if (!fields.length) {
           return 0;
       }
       fields.push('updated_at = CURRENT_TIMESTAMP');
       values.push(templateId);
       return new Promise((resolve, reject) => {
           const sql = `UPDATE email_templates SET ${fields.join(', ')} WHERE template_id = ?`;
           this.db.run(sql, values, function (err) {
               if (err) {
                   console.error('Error updating email template:', err);
                   reject(err);
               } else {
                   resolve(this.changes);
               }
           });
       });
   }

   async deleteEmailTemplate(templateId) {
       return new Promise((resolve, reject) => {
           this.db.run('DELETE FROM email_templates WHERE template_id = ?', [templateId], function (err) {
               if (err) {
                   console.error('Error deleting email template:', err);
                   reject(err);
               } else {
                   resolve(this.changes);
               }
           });
       });
   }

   async reserveEmailIdempotency(idempotencyKey, requestHash, bulkJobId = null) {
       const key = String(idempotencyKey || '').trim();
       if (!key) {
           throw new Error('Idempotency key is required');
       }
       const hash = requestHash || null;
       const dbInstance = this;
       return new Promise((resolve, reject) => {
           const sql = `INSERT OR IGNORE INTO email_idempotency (
               idempotency_key, message_id, bulk_job_id, request_hash
           ) VALUES (?, NULL, ?, ?)`;
           this.db.run(sql, [key, bulkJobId || null, hash], function (insertErr) {
               if (insertErr) {
                   console.error('Error reserving email idempotency key:', insertErr);
                   reject(insertErr);
                   return;
               }
               const inserted = (this.changes || 0) > 0;
               dbInstance.getEmailIdempotency(key)
                   .then((record) => {
                       resolve({ inserted, record });
                   })
                   .catch(reject);
           });
       });
   }

   async finalizeEmailIdempotency(idempotencyKey, messageId = null, bulkJobId = null, requestHash = null) {
       const key = String(idempotencyKey || '').trim();
       if (!key) {
           throw new Error('Idempotency key is required');
       }
       return new Promise((resolve, reject) => {
           const sql = `
               INSERT INTO email_idempotency (idempotency_key, message_id, bulk_job_id, request_hash)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(idempotency_key) DO UPDATE SET
                   message_id = COALESCE(email_idempotency.message_id, excluded.message_id),
                   bulk_job_id = COALESCE(email_idempotency.bulk_job_id, excluded.bulk_job_id),
                   request_hash = COALESCE(email_idempotency.request_hash, excluded.request_hash)
           `;
           this.db.run(
               sql,
               [key, messageId || null, bulkJobId || null, requestHash || null],
               function (err) {
                   if (err) {
                       console.error('Error finalizing email idempotency key:', err);
                       reject(err);
                   } else {
                       resolve((this.changes || 0) > 0);
                   }
               },
           );
       });
   }

   async clearPendingEmailIdempotency(idempotencyKey) {
       const key = String(idempotencyKey || '').trim();
       if (!key) {
           return 0;
       }
       return new Promise((resolve, reject) => {
           const sql = `
               DELETE FROM email_idempotency
               WHERE idempotency_key = ?
                 AND message_id IS NULL
           `;
           this.db.run(sql, [key], function (err) {
               if (err) {
                   console.error('Error clearing pending email idempotency:', err);
                   reject(err);
               } else {
                   resolve(this.changes || 0);
               }
           });
       });
   }

   async saveEmailIdempotency(idempotencyKey, messageId, bulkJobId, requestHash) {
       return this.finalizeEmailIdempotency(
           idempotencyKey,
           messageId || null,
           bulkJobId || null,
           requestHash || null,
       );
   }

   async getEmailIdempotency(idempotencyKey) {
       return new Promise((resolve, reject) => {
           const sql = `SELECT * FROM email_idempotency WHERE idempotency_key = ?`;
           this.db.get(sql, [idempotencyKey], (err, row) => {
               if (err) {
                   console.error('Error fetching email idempotency:', err);
                   reject(err);
               } else {
                   resolve(row || null);
               }
           });
       });
   }

   async isEmailSuppressed(email) {
       return new Promise((resolve, reject) => {
           const sql = `SELECT reason FROM email_suppression WHERE email = ?`;
           this.db.get(sql, [email], (err, row) => {
               if (err) {
                   console.error('Error checking email suppression:', err);
                   reject(err);
               } else {
                   resolve(row || null);
               }
           });
       });
   }

   async setEmailSuppression(email, reason = null, source = null) {
       return new Promise((resolve, reject) => {
           const sql = `INSERT INTO email_suppression (email, reason, source, created_at, updated_at)
               VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
               ON CONFLICT(email) DO UPDATE SET
               reason = excluded.reason,
               source = excluded.source,
               updated_at = CURRENT_TIMESTAMP`;
           this.db.run(sql, [email, reason, source], function (err) {
               if (err) {
                   console.error('Error setting email suppression:', err);
                   reject(err);
               } else {
                   resolve(true);
               }
           });
       });
   }

   async clearEmailSuppression(email) {
       return new Promise((resolve, reject) => {
           const sql = `DELETE FROM email_suppression WHERE email = ?`;
           this.db.run(sql, [email], function (err) {
               if (err) {
                   console.error('Error clearing email suppression:', err);
                   reject(err);
               } else {
                   resolve(true);
               }
           });
       });
   }

   async insertEmailDlq(messageId, reason, payload = null) {
       await this.ensureEmailDlqColumns();
       return new Promise((resolve, reject) => {
           const sql = `INSERT INTO email_dlq (message_id, reason, payload, status, updated_at)
               VALUES (?, ?, ?, 'open', CURRENT_TIMESTAMP)`;
           this.db.run(sql, [messageId, reason || null, payload ? JSON.stringify(payload) : null], function (err) {
               if (err) {
                   console.error('Error inserting email DLQ:', err);
                   reject(err);
               } else {
                   resolve(this.lastID);
               }
           });
       });
   }

   async listEmailDlq({ status = null, limit = 20, offset = 0 } = {}) {
       await this.ensureEmailDlqColumns();
       const clauses = [];
       const params = [];
       if (status && String(status).toLowerCase() !== 'all') {
           clauses.push('d.status = ?');
           params.push(String(status).toLowerCase());
       }
       const where = clauses.length ? `WHERE ${clauses.join(' AND ')}` : '';
       const sql = `
           SELECT
               d.*,
               m.to_email,
               m.subject,
               m.provider,
               m.status AS message_status
           FROM email_dlq d
           LEFT JOIN email_messages m ON m.message_id = d.message_id
           ${where}
           ORDER BY d.created_at DESC
           LIMIT ? OFFSET ?
       `;
       params.push(Math.min(100, Math.max(1, Number(limit) || 20)));
       params.push(Math.max(0, Number(offset) || 0));
       return new Promise((resolve, reject) => {
           this.db.all(sql, params, (err, rows) => {
               if (err) {
                   console.error('Error listing email DLQ:', err);
                   reject(err);
               } else {
                   resolve(rows || []);
               }
           });
       });
   }

   async getEmailDlqEntry(dlqId) {
       await this.ensureEmailDlqColumns();
       return new Promise((resolve, reject) => {
           const sql = `
               SELECT
                   d.*,
                   m.to_email,
                   m.subject,
                   m.provider,
                   m.status AS message_status
               FROM email_dlq d
               LEFT JOIN email_messages m ON m.message_id = d.message_id
               WHERE d.id = ?
           `;
           this.db.get(sql, [dlqId], (err, row) => {
               if (err) {
                   console.error('Error fetching email DLQ entry:', err);
                   reject(err);
               } else {
                   resolve(row || null);
               }
           });
       });
   }

   async countOpenEmailDlq() {
       await this.ensureEmailDlqColumns();
       return new Promise((resolve, reject) => {
           const sql = `SELECT COUNT(*) AS total FROM email_dlq WHERE status = 'open'`;
           this.db.get(sql, [], (err, row) => {
               if (err) {
                   console.error('Error counting open email DLQ rows:', err);
                   reject(err);
               } else {
                   resolve(Number(row?.total) || 0);
               }
           });
       });
   }

   async markEmailDlqReplayed(dlqId, replayError = null) {
       await this.ensureEmailDlqColumns();
       const now = new Date().toISOString();
       const status = replayError ? 'open' : 'replayed';
       return new Promise((resolve, reject) => {
           const sql = `
               UPDATE email_dlq
               SET replay_count = replay_count + 1,
                   status = ?,
                   replayed_at = ?,
                   last_replay_error = ?,
                   updated_at = ?
               WHERE id = ?
           `;
           this.db.run(sql, [status, now, replayError || null, now, dlqId], function(err) {
               if (err) {
                   console.error('Error marking email DLQ replay:', err);
                   reject(err);
               } else {
                   resolve(this.changes || 0);
               }
           });
       });
   }

   async incrementEmailMetric(metricType) {
       return new Promise((resolve, reject) => {
           const date = new Date().toISOString().slice(0, 10);
           const sqlSelect = 'SELECT total_count FROM email_metrics WHERE date = ? AND metric_type = ?';
           this.db.get(sqlSelect, [date, metricType], (err, row) => {
               if (err) {
                   console.error('Email metrics select error:', err);
                   reject(err);
                   return;
               }
               if (row) {
                   const sqlUpdate = `UPDATE email_metrics 
                       SET total_count = total_count + 1, updated_at = CURRENT_TIMESTAMP
                       WHERE date = ? AND metric_type = ?`;
                   this.db.run(sqlUpdate, [date, metricType], function (updateErr) {
                       if (updateErr) {
                           console.error('Email metrics update error:', updateErr);
                           reject(updateErr);
                       } else {
                           resolve(true);
                       }
                   });
               } else {
                   const sqlInsert = `INSERT INTO email_metrics (date, metric_type, total_count)
                       VALUES (?, ?, 1)`;
                   this.db.run(sqlInsert, [date, metricType], function (insertErr) {
                       if (insertErr) {
                           console.error('Email metrics insert error:', insertErr);
                           reject(insertErr);
                       } else {
                           resolve(true);
                       }
                   });
               }
           });
       });
   }

   async getEmailMetricCount(metricType) {
       return new Promise((resolve, reject) => {
           const date = new Date().toISOString().slice(0, 10);
           const sql = `SELECT total_count FROM email_metrics WHERE date = ? AND metric_type = ?`;
           this.db.get(sql, [date, metricType], (err, row) => {
               if (err) {
                   console.error('Email metrics fetch error:', err);
                   reject(err);
               } else {
                   resolve(row ? row.total_count : 0);
               }
           });
       });
   }

   // Comprehensive cleanup with enhanced metrics
   async cleanupOldRecords(daysToKeep = 30) {
       const tables = [
           { name: 'call_states', dateField: 'timestamp' },
           { name: 'service_health_logs', dateField: 'timestamp' },
           { name: 'call_metrics', dateField: 'timestamp' },
           { name: 'notification_metrics', dateField: 'created_at' },
           { name: 'email_provider_events', dateField: 'created_at' },
           { name: 'email_idempotency', dateField: 'created_at' },
           { name: 'gpt_tool_audit', dateField: 'created_at' },
           { name: 'gpt_tool_idempotency', dateField: 'updated_at' },
           { name: 'gpt_memory_facts', dateField: 'last_seen_at' },
           { name: 'provider_event_idempotency', dateField: 'created_at' },
           { name: 'call_runtime_state', dateField: 'updated_at' }
       ];
       
       let totalCleaned = 0;
       const cleanupResults = {};
       
       for (const table of tables) {
           const cleaned = await new Promise((resolve, reject) => {
               const sql = `DELETE FROM ${table.name} 
                   WHERE ${table.dateField} < datetime('now', '-${daysToKeep} days')`;
               
               this.db.run(sql, function(err) {
                   if (err) {
                       reject(err);
                   } else {
                       resolve(this.changes);
                   }
               });
           });
           
           cleanupResults[table.name] = cleaned;
           totalCleaned += cleaned;
           
           if (cleaned > 0) {
               console.log(`🧹 Cleaned ${cleaned} old records from ${table.name}`);
           }
       }

       const jobsCleaned = await new Promise((resolve, reject) => {
           const sql = `
               DELETE FROM call_jobs
               WHERE status IN ('completed', 'failed')
                 AND updated_at < datetime('now', '-${daysToKeep} days')
           `;
           this.db.run(sql, function(err) {
               if (err) {
                   reject(err);
               } else {
                   resolve(this.changes);
               }
           });
       });
       cleanupResults.call_jobs = jobsCleaned;
       totalCleaned += jobsCleaned;

       const callDlqCleaned = await new Promise((resolve, reject) => {
           const sql = `
               DELETE FROM call_job_dlq
               WHERE status = 'replayed'
                 AND updated_at < datetime('now', '-${daysToKeep} days')
           `;
           this.db.run(sql, function(err) {
               if (err) {
                   reject(err);
               } else {
                   resolve(this.changes);
               }
           });
       });
       cleanupResults.call_job_dlq = callDlqCleaned;
       totalCleaned += callDlqCleaned;

       const emailDlqCleaned = await new Promise((resolve, reject) => {
           const sql = `
               DELETE FROM email_dlq
               WHERE status = 'replayed'
                 AND updated_at < datetime('now', '-${daysToKeep} days')
           `;
           this.db.run(sql, function(err) {
               if (err) {
                   reject(err);
               } else {
                   resolve(this.changes);
               }
           });
       });
       cleanupResults.email_dlq = emailDlqCleaned;
       totalCleaned += emailDlqCleaned;
       
       // Clean up old successful webhook notifications (keep for 7 days)
       const webhooksCleaned = await new Promise((resolve, reject) => {
           const sql = `DELETE FROM webhook_notifications 
               WHERE status = 'sent' 
               AND created_at < datetime('now', '-7 days')`;
           
           this.db.run(sql, function(err) {
               if (err) {
                   reject(err);
               } else {
                   resolve(this.changes);
               }
           });
       });
       
       cleanupResults.webhook_notifications = webhooksCleaned;
       totalCleaned += webhooksCleaned;
       
       if (webhooksCleaned > 0) {
           console.log(`🧹 Cleaned ${webhooksCleaned} old successful webhook notifications`);
       }
       
       // Clean up old user sessions (keep for 90 days)
       const sessionsCleaned = await new Promise((resolve, reject) => {
           const sql = `DELETE FROM user_sessions 
               WHERE last_activity < datetime('now', '-90 days')`;
           
           this.db.run(sql, function(err) {
               if (err) {
                   reject(err);
               } else {
                   resolve(this.changes);
               }
           });
       });
       
       cleanupResults.user_sessions = sessionsCleaned;
       totalCleaned += sessionsCleaned;
       
       if (sessionsCleaned > 0) {
           console.log(`🧹 Cleaned ${sessionsCleaned} old user sessions`);
       }

       const memoryCleaned = await new Promise((resolve, reject) => {
           const sql = `DELETE FROM gpt_call_memory
               WHERE summary_updated_at < datetime('now', '-${daysToKeep} days')`;
           this.db.run(sql, function(err) {
               if (err) {
                   reject(err);
               } else {
                   resolve(this.changes);
               }
           });
       });
       cleanupResults.gpt_call_memory = memoryCleaned;
       totalCleaned += memoryCleaned;
       
       // Log cleanup operation
       await this.logServiceHealth('database', 'cleanup_completed', {
           total_cleaned: totalCleaned,
           days_kept: daysToKeep,
           breakdown: cleanupResults
       });
       
       console.log(`✅ Enhanced cleanup completed: ${totalCleaned} total records cleaned`);
       
       return {
           total_cleaned: totalCleaned,
           breakdown: cleanupResults,
           days_kept: daysToKeep
       };
   }

   // Database maintenance and optimization
   async optimizeDatabase() {
       return new Promise((resolve, reject) => {
           console.log('🔧 Running database optimization...');
           
           // Run VACUUM to reclaim space and defragment
           this.db.run('VACUUM', (err) => {
               if (err) {
                   console.error('❌ Database VACUUM failed:', err);
                   reject(err);
               } else {
                   // Run ANALYZE to update query planner statistics
                   this.db.run('ANALYZE', (analyzeErr) => {
                       if (analyzeErr) {
                           console.error('❌ Database ANALYZE failed:', analyzeErr);
                           reject(analyzeErr);
                       } else {
                           console.log('✅ Database optimization completed');
                           resolve(true);
                       }
                   });
               }
           });
       });
   }

   // Get database size and performance metrics
   async getDatabaseMetrics() {
       return new Promise((resolve, reject) => {
           const fs = require('fs');
           
           // Get file size
           let fileSize = 0;
           try {
               const stats = fs.statSync(this.dbPath);
               fileSize = stats.size;
           } catch (e) {
               console.warn('Could not get database file size:', e.message);
           }
           
           // Get table counts
           const sql = `
               SELECT 
                   'calls' as table_name,
                   COUNT(*) as row_count
               FROM calls
               UNION ALL
               SELECT 'call_transcripts', COUNT(*) FROM call_transcripts
               UNION ALL
               SELECT 'transcripts', COUNT(*) FROM transcripts
               UNION ALL
               SELECT 'call_states', COUNT(*) FROM call_states
               UNION ALL
               SELECT 'webhook_notifications', COUNT(*) FROM webhook_notifications
               UNION ALL
               SELECT 'notification_metrics', COUNT(*) FROM notification_metrics
               UNION ALL
               SELECT 'service_health_logs', COUNT(*) FROM service_health_logs
               UNION ALL
               SELECT 'call_metrics', COUNT(*) FROM call_metrics
               UNION ALL
               SELECT 'user_sessions', COUNT(*) FROM user_sessions
               UNION ALL
               SELECT 'sms_messages', COUNT(*) FROM sms_messages
               UNION ALL
               SELECT 'bulk_sms_operations', COUNT(*) FROM bulk_sms_operations
           `;
           
           this.db.all(sql, [], (err, rows) => {
               if (err) {
                   reject(err);
               } else {
                   const metrics = {
                       file_size_bytes: fileSize,
                       file_size_mb: (fileSize / (1024 * 1024)).toFixed(2),
                       table_counts: {},
                       total_rows: 0
                   };
                   
                   rows.forEach(row => {
                       metrics.table_counts[row.table_name] = row.row_count;
                       metrics.total_rows += row.row_count;
                   });
                   
                   resolve(metrics);
               }
           });
       });
   }

   // Enhanced close method with cleanup
   async close() {
       if (this.db) {
           return new Promise((resolve) => {
               // Log database shutdown
               this.logServiceHealth('database', 'shutdown_initiated', {
                   timestamp: new Date().toISOString()
               }).then(() => {
                   this.db.close((err) => {
                       if (err) {
                           console.error('Error closing enhanced database:', err);
                       } else {
                           console.log('✅ Enhanced database connection closed');
                       }
                       resolve();
                   });
               }).catch(() => {
                   // If logging fails, still close the database
                   this.db.close((err) => {
                       if (err) {
                           console.error('Error closing enhanced database:', err);
                       } else {
                           console.log('✅ Enhanced database connection closed');
                       }
                       resolve();
                   });
               });
           });
       }
   }

   // Health check method
   async healthCheck() {
       return new Promise((resolve, reject) => {
           if (!this.isInitialized) {
               reject(new Error('Database not initialized'));
               return;
           }
           
           // Simple query to test database connectivity
           this.db.get('SELECT 1 as test', [], (err, row) => {
               if (err) {
                   reject(err);
               } else {
                   resolve({
                       status: 'healthy',
                       initialized: this.isInitialized,
                       timestamp: new Date().toISOString()
                   });
               }
           });
       });
   }
}

module.exports = EnhancedDatabase;
