/**
 * ============================================================
 * email-ui.js
 * Drop-in Email Notification Widget — Works on ANY HTML page
 *
 * PURPOSE:
 *   Inject a self-contained "Email Notifications" card onto
 *   any page with a single <script> tag. Reads auth state
 *   from window.AppDB (already loaded via app-backend.js),
 *   reads/writes subscription via window.EmailDB
 *   (loaded via email-backend.js), and renders its own UI.
 *
 * ZERO DEPENDENCIES beyond app-backend.js + email-backend.js.
 * NO localStorage. NO sessionStorage. State lives in Firestore.
 *
 * TO ADD TO ANY HTML PAGE:
 *   <!-- after firebase-config.js, app-backend.js, email-backend.js -->
 *   <script type="module" src="email-ui.js"></script>
 *
 *   Then place a mount point anywhere in your HTML:
 *   <div id="email-notif-widget"></div>
 *
 *   If no mount point exists the widget appends itself to <body>.
 *
 * CUSTOMISATION:
 *   Pass data attributes on the mount div:
 *     data-title="Email Reminders"
 *     data-accent="#4f46e5"
 * ============================================================
 */

/* ── Wait helpers ──────────────────────────────────────────── */

function _waitFor(getter, label, timeoutMs = 10000) {
    return new Promise((resolve, reject) => {
        const val = getter();
        if (val) { resolve(val); return; }
        const deadline = Date.now() + timeoutMs;
        const tid = setInterval(() => {
            const v = getter();
            if (v) { clearInterval(tid); resolve(v); }
            else if (Date.now() > deadline) {
                clearInterval(tid);
                reject(new Error(`email-ui.js: ${label} not ready after ${timeoutMs}ms`));
            }
        }, 80);
    });
}

/* ── CSS injection (scoped to .enotif-*) ─────────────────── */

function _injectStyles(accent) {
    if (document.getElementById('_enotif_styles')) return;
    const s = document.createElement('style');
    s.id = '_enotif_styles';
    s.textContent = `
.enotif-card {
    background: var(--card-bg, #fff);
    border: 1.5px solid var(--border, #e5e7eb);

    padding: 20px 22px;
    max-width: 420px;
    font-family: inherit;
    box-shadow: 0 2px 8px rgba(0,0,0,.06);
    margin: 16px 0;
}
.enotif-card h3 {
    margin: 0 0 4px;
    font-size: 1rem;
    font-weight: 700;
    color: var(--text, #111);
    display: flex;
    align-items: center;
    gap: 8px;
}
.enotif-card p.enotif-sub {
    margin: 0 0 16px;
    font-size: .82rem;
    color: var(--text-muted, #6b7280);
}
.enotif-badge {
    display: inline-flex;
    align-items: center;
    gap: 5px;
    padding: 2px 9px;
    border-radius: 99px;
    font-size: .72rem;
    font-weight: 600;
    background: #dcfce7;
    color: #15803d;
}
.enotif-badge.off { background: #f3f4f6; color: #9ca3af; }
.enotif-toggle-row {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 12px;
}
.enotif-label {
    font-size: .88rem;
    color: var(--text, #374151);
    display: flex;
    align-items: center;
    gap: 6px;
}
/* Toggle switch */
.enotif-switch {
    position: relative;
    display: inline-block;
    width: 42px;
    height: 24px;
    flex-shrink: 0;
}
.enotif-switch input { opacity: 0; width: 0; height: 0; }
.enotif-slider {
    position: absolute;
    inset: 0;
    background: #d1d5db;
    border-radius: 24px;
    cursor: pointer;
    transition: background .2s;
}
.enotif-slider::before {
    content: '';
    position: absolute;
    left: 3px; top: 3px;
    width: 18px; height: 18px;
    background: #fff;
    border-radius: 50%;
    transition: transform .2s;
    box-shadow: 0 1px 3px rgba(0,0,0,.2);
}
.enotif-switch input:checked + .enotif-slider { background: #251577; }
.enotif-switch input:checked + .enotif-slider::before { transform: translateX(18px); }
.enotif-divider {
    border: none;
    border-top: 1px solid var(--border, #e5e7eb);
    margin: 14px 0;
}
.enotif-btn {
    display: inline-block;
    padding: 9px 20px;
    border-radius: 8px;
    font-size: .88rem;
    font-weight: 600;
    cursor: pointer;
    border: none;
    transition: opacity .15s, transform .1s;
}
.enotif-btn:active { transform: scale(.97); }
.enotif-btn-primary {
    background: #251577;
    color: #fff;
}
.enotif-btn-primary:hover { background: #251577; }
.enotif-btn-ghost {
    background: #dc3545;
    color: #ffff;
    border: 1.5px solid #dc3545;
    margin-left: 8px;
}
.enotif-btn-ghost:hover { #dc3545; }
.enotif-msg {
    margin-top: 12px;
    padding: 8px 12px;
    border-radius: 8px;
    font-size: .83rem;
    display: none;
}
.enotif-msg.ok  { background: #f0fdf4; color: #166534; display: block; }
.enotif-msg.err { background: #fff5f5; color: #dc3545; display: block; }
.enotif-loading { color: var(--text-muted, #9ca3af); font-size: .85rem; padding: 6px 0; }
    `;
    document.head.appendChild(s);
}

/* ── Toggle helper ───────────────────────────────────────── */

function _makeToggle(id, label, icon, checked) {
    return `
<div class="enotif-toggle-row">
    <span class="enotif-label">${icon} ${label}</span>
    <label class="enotif-switch">
        <input type="checkbox" id="${id}" ${checked ? 'checked' : ''}>
        <span class="enotif-slider"></span>
    </label>
</div>`;
}

/* ── Render ──────────────────────────────────────────────── */

function _render(container, { title, accent, sub, isSubscribed, email, prefs }) {
    const badge = isSubscribed
        ? `<span class="enotif-badge">● সক্রিয়</span>`
        : `<span class="enotif-badge off">○ নিষ্ক্রিয়</span>`;

    container.innerHTML = `
<div class="enotif-card">
    <h3>📧 ${title} ${badge}</h3>
    <p class="enotif-sub">${sub}</p>

    ${_makeToggle('enotif-morning', 'সকাল ৮টায় ডাইজেস্ট', '🌅', prefs.morning)}
    ${_makeToggle('enotif-evening', 'সন্ধ্যা ৮টায় ডাইজেস্ট', '🌆', prefs.evening)}
    ${_makeToggle('enotif-tasks',   'মুলতবি টাস্ক অন্তর্ভুক্ত', '📋', prefs.tasks)}

    <hr class="enotif-divider">

    <div class="enotif-toggle-row" style="align-items:flex-end">
        <div style="flex:1;margin-right:10px">
            <label style="font-size:.8rem;color:var(--text-muted,#6b7280);display:block;margin-bottom:4px">
                ইমেইল ঠিকানা
            </label>
            <input
                id="enotif-email"
                type="email"
                value="${email || ''}"
                placeholder="you@example.com"
                style="
                    width:100%;padding:7px 10px;border-radius:7px;
                    border:1.5px solid var(--border,#e5e7eb);
                    font-size:.88rem;box-sizing:border-box;
                    background:var(--input-bg,#f9fafb);
                    color:var(--text,#111);
                "
            >
        </div>
    </div>

    <div style="margin-top:14px">
        <button class="enotif-btn enotif-btn-primary" id="enotif-save">
            ${isSubscribed ? 'আপডেট করুন' : 'সাবস্ক্রাইব করুন'}
        </button>
        ${isSubscribed ? `<button class="enotif-btn enotif-btn-ghost" id="enotif-unsub">আনসাবস্ক্রাইব</button>` : ''}
    </div>

    <div class="enotif-msg" id="enotif-msg"></div>
</div>`;
}

/* ── Message helper ──────────────────────────────────────── */

function _showMsg(msg, type = 'ok') {
    const el = document.getElementById('enotif-msg');
    if (!el) return;
    el.className = `enotif-msg ${type}`;
    el.textContent = msg;
    setTimeout(() => { if (el) el.className = 'enotif-msg'; }, 4000);
}

/* ── Wire events ─────────────────────────────────────────── */

function _wireEvents(container, uid, currentEmail) {
    const saveBtn  = container.querySelector('#enotif-save');
    const unsubBtn = container.querySelector('#enotif-unsub');

    if (saveBtn) {
        saveBtn.addEventListener('click', async () => {
            const emailInput = container.querySelector('#enotif-email');
            const email      = (emailInput ? emailInput.value : currentEmail || '').trim();

            if (!email || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
                _showMsg('⚠️ একটি বৈধ ইমেইল ঠিকানা লিখুন।', 'err');
                return;
            }

            const prefs = {
                morning: !!(container.querySelector('#enotif-morning') || {}).checked,
                evening: !!(container.querySelector('#enotif-evening') || {}).checked,
                tasks:   !!(container.querySelector('#enotif-tasks')   || {}).checked,
            };

            // Read checked state properly
            const morningEl = container.querySelector('#enotif-morning');
            const eveningEl = container.querySelector('#enotif-evening');
            const tasksEl   = container.querySelector('#enotif-tasks');
            prefs.morning = morningEl ? morningEl.checked : true;
            prefs.evening = eveningEl ? eveningEl.checked : true;
            prefs.tasks   = tasksEl   ? tasksEl.checked   : true;

            saveBtn.disabled = true;
            saveBtn.textContent = '⏳ সেভ হচ্ছে...';

            try {
                await window.EmailDB.saveEmailSubscription(uid, email, prefs);
                _showMsg('✅ সাবস্ক্রিপশন সেভ হয়েছে! আপনি এখন ইমেইল পাবেন।', 'ok');
                // Re-render with fresh state
                await _mount(container.closest('[id]') || container, uid);
            } catch (err) {
                console.error('EmailUI save error:', err);
                _showMsg('❌ সেভ করতে সমস্যা হয়েছে। আবার চেষ্টা করুন।', 'err');
                saveBtn.disabled = false;
                saveBtn.textContent = '✅ সাবস্ক্রাইব করুন';
            }
        });
    }

    if (unsubBtn) {
        unsubBtn.addEventListener('click', async () => {
            if (!confirm('ইমেইল নোটিফিকেশন বন্ধ করতে চান?')) return;
            unsubBtn.disabled = true;
            try {
                await window.EmailDB.removeEmailSubscription(uid);
                _showMsg('🔕 সাবস্ক্রিপশন বাতিল হয়েছে।', 'ok');
                await _mount(container.closest('[id]') || container, uid);
            } catch (err) {
                console.error('EmailUI unsub error:', err);
                _showMsg('❌ বাতিল করতে সমস্যা হয়েছে।', 'err');
                unsubBtn.disabled = false;
            }
        });
    }
}

/* ── Mount ───────────────────────────────────────────────── */

async function _mount(el, uid) {
    const mountPoint = typeof el === 'string'
        ? document.getElementById(el)
        : el;

    if (!mountPoint) return;

    // Derive config from data attributes
    const title  = mountPoint.dataset.title  || 'ইমেইল নোটিফিকেশন';
    const accent  = mountPoint.dataset.accent || '#251577';
    const sub     = mountPoint.dataset.subtitle || '';

    _injectStyles(accent);

    // Show loading state
    mountPoint.innerHTML = `<div class="enotif-loading">লোড হচ্ছে...</div>`;

    const existing     = await window.EmailDB.getEmailSubscription(uid);
    const isSubscribed = !!(existing && existing.active);
    const email        = (existing && existing.email) || '';
    const prefs        = (existing && existing.prefs) || { morning: true, evening: true, tasks: true };

    _render(mountPoint, { title, accent, sub, isSubscribed, email, prefs });
    _wireEvents(mountPoint, uid, email);
}

/* ── Bootstrap ───────────────────────────────────────────── */

(async function init() {
    try {
        // Wait for both AppDB and EmailDB to be ready
        await _waitFor(() => window.AppDB && typeof window.AppDB.onAuthStateChanged === 'function', 'AppDB');
        await _waitFor(() => window.EmailDB && typeof window.EmailDB.saveEmailSubscription === 'function', 'EmailDB');

        window.AppDB.onAuthStateChanged(async (user) => {
            const mountPoint =
                document.getElementById('email-notif-widget') ||
                (() => {
                    const d = document.createElement('div');
                    d.id = 'email-notif-widget';
                    document.body.appendChild(d);
                    return d;
                })();

            if (!user) {
                mountPoint.innerHTML = '';
                return;
            }

            await _mount(mountPoint, user.uid);
        });

    } catch (err) {
        console.error('email-ui.js init error:', err);
    }
})();

// Public API for manual mounting
window.EmailUI = {
    mount: _mount
};
