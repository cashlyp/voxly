const sqlite3 = require('sqlite3').verbose();
const path = require('path');

class EnhancedDatabase {
    constructor() {
        this.db = null;
        this.isInitialized = false;
        this.dbPath = path.join(__dirname, 'data.db');
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
                        this.isInitialized = true;
                        console.log('✅ Enhanced database initialization complete');
                        resolve();
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
            )`
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

        await this.ensureCallColumns(['digit_summary', 'digit_count', 'last_otp', 'last_otp_masked']);
        await this.ensureTemplateColumns(['requires_otp', 'default_profile', 'expected_length', 'allow_terminator', 'terminator_char']);

        // Create comprehensive indexes for optimal performance
        const indexes = [
            // Call indexes
            'CREATE INDEX IF NOT EXISTS idx_calls_call_sid ON calls(call_sid)',
            'CREATE INDEX IF NOT EXISTS idx_calls_user_chat_id ON calls(user_chat_id)',
            'CREATE INDEX IF NOT EXISTS idx_calls_status ON calls(status)',
            'CREATE INDEX IF NOT EXISTS idx_calls_created_at ON calls(created_at)',
            'CREATE INDEX IF NOT EXISTS idx_calls_twilio_status ON calls(twilio_status)',
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
            
            // Metrics indexes
            'CREATE INDEX IF NOT EXISTS idx_metrics_date ON notification_metrics(date)',
            'CREATE INDEX IF NOT EXISTS idx_metrics_type ON notification_metrics(notification_type)',
            'CREATE INDEX IF NOT EXISTS idx_call_metrics_call_sid ON call_metrics(call_sid)',
            'CREATE INDEX IF NOT EXISTS idx_call_metrics_type ON call_metrics(metric_type)',
            
            // Health indexes
            'CREATE INDEX IF NOT EXISTS idx_health_service ON service_health_logs(service_name)',
            'CREATE INDEX IF NOT EXISTS idx_health_timestamp ON service_health_logs(timestamp)',
            'CREATE INDEX IF NOT EXISTS idx_health_status ON service_health_logs(status)',
            
            // Session indexes
            'CREATE INDEX IF NOT EXISTS idx_sessions_chat_id ON user_sessions(telegram_chat_id)',
            'CREATE INDEX IF NOT EXISTS idx_sessions_start ON user_sessions(session_start)',
            'CREATE INDEX IF NOT EXISTS idx_sessions_activity ON user_sessions(last_activity)'
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
            generated_functions = null 
        } = callData;
        
        return new Promise((resolve, reject) => {
            const stmt = this.db.prepare(`
                INSERT INTO calls (
                    call_sid, phone_number, prompt, first_message, 
                    user_chat_id, status, business_context, generated_functions
                )
                VALUES (?, ?, ?, ?, ?, 'initiated', ?, ?)
            `);
            
            stmt.run([
                call_sid, 
                phone_number, 
                prompt, 
                first_message, 
                user_chat_id, 
                business_context,
                generated_functions
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

    // NEW: Get recent calls with transcripts count (REQUIRED FOR API ENDPOINTS)
    async getRecentCalls(limit = 10, offset = 0) {
        return new Promise((resolve, reject) => {
            const query = `
                SELECT 
                    c.*,
                    COUNT(t.id) as transcript_count
                FROM calls c
                LEFT JOIN transcripts t ON c.call_sid = t.call_sid
                GROUP BY c.call_sid
                ORDER BY c.created_at DESC
                LIMIT ? OFFSET ?
            `;
            
            this.db.all(query, [limit, offset], (err, rows) => {
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
    async updateEnhancedWebhookNotification(id, status, error_message = null, telegram_message_id = null) {
        return new Promise((resolve, reject) => {
            const sent_at = status === 'sent' ? new Date().toISOString() : null;
            
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
                            telegram_message_id = ?, delivery_time_ms = ?
                        WHERE id = ?
                    `);
                    
                    stmt.run([status, error_message, sent_at, telegram_message_id, delivery_time_ms, id], function(err) {
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
                    SET status = ?, error_message = ?, retry_count = retry_count + 1
                    WHERE id = ?
                `);
                
                stmt.run([status, error_message, id], function(err) {
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
    async getEnhancedPendingWebhookNotifications(limit = 50) {
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
                    AND wn.retry_count < 3
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
            
            this.db.all(sql, [limit], (err, rows) => {
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
                   } else {
                       console.log('✅ SMS tables created successfully');
                       resolve();
                   }
               });
           });
       });
   }

   // Save SMS message
   async saveSMSMessage(messageData) {
       return new Promise((resolve, reject) => {
           const sql = `INSERT INTO sms_messages (
               message_sid, to_number, from_number, body, status, 
               direction, ai_response, response_message_sid, user_chat_id
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`;

           this.db.run(sql, [
               messageData.message_sid,
               messageData.to_number || null,
               messageData.from_number || null,
               messageData.body,
               messageData.status || 'queued',
               messageData.direction,
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

   // Comprehensive cleanup with enhanced metrics
   async cleanupOldRecords(daysToKeep = 30) {
       const tables = [
           { name: 'call_states', dateField: 'timestamp' },
           { name: 'service_health_logs', dateField: 'timestamp' },
           { name: 'call_metrics', dateField: 'timestamp' },
           { name: 'notification_metrics', dateField: 'created_at' }
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
