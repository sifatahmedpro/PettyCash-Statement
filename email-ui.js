/**
 * ============================================================
 * email-ui.js  —  v3.0 (PROFESSIONAL BRANDED WIDGET)
 * Drop-in Email Notification Widget — Works on ANY HTML page
 *
 * WHAT CHANGED vs v2:
 *   Complete visual redesign of the subscription widget:
 *   - Brand colour #251577 throughout (matches app theme)
 *   - SolaimanLipi / Kalpurush Bengali fonts
 *   - Richer header with icon, gradient background
 *   - Summary badges showing active slot count
 *   - Per-page toggle section with module icons & labels
 *   - Styled email input with floating label
 *   - Animated save / unsub buttons
 *   - Responsive — works on mobile and desktop
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

/* ── Page definitions (mirrors send-email.js) ──────────────── */

const PAGE_DEFS = [
    { key: 'advance_payment', label: 'অগ্রিম পরিশোধ',    icon: '💵', color: '#0f766e' },
    { key: 'business_stats',  label: 'ব্যবসা পরিসংখ্যান', icon: '📊', color: '#251577' },
    { key: 'donation',        label: 'অনুদান',             icon: '🤝', color: '#7c3aed' },
    { key: 'help',            label: 'সহায়তা',             icon: '📋', color: '#b45309' },
    { key: 'office_issue',    label: 'সমস্যা ও সমাধান',   icon: '⚠️',  color: '#dc2626' },
    { key: 'premium_submit',  label: 'প্রিমিয়াম জমা',    icon: '🏦',  color: '#0369a1' },
];

/* ── CSS injection ─────────────────────────────────────────── */

function _injectStyles() {
    if (document.getElementById('_enotif_styles_v3')) return;
    const s = document.createElement('style');
    s.id = '_enotif_styles_v3';
    s.textContent = `
/* === Email Widget v3 — Brand-aligned design === */

#email-notif-widget {
    display: block;
    width: 100%;
    font-family: 'SolaimanLipi', 'Kalpurush', 'Noto Sans Bengali', sans-serif !important;
}

.en3-card {
    background: #fff;
    border: 1.5px solid #dde0f8;
    border-radius: 14px;
    overflow: hidden;
    max-width: 480px;
    width: 100%;
    margin: 20px 0;
    box-shadow: 0 4px 20px rgba(37,21,119,.10);
    font-family: 'SolaimanLipi','Kalpurush','Noto Sans Bengali',sans-serif !important;
}

/* Header */
.en3-header {
    background: linear-gradient(135deg, #251577 0%, #1d4ed8 100%);
    padding: 0;
    position: relative;
    overflow: hidden;
}
.en3-header::before {
    content: '';
    position: absolute;
    top: -30px; right: -30px;
    width: 120px; height: 120px;
    background: rgba(255,255,255,.07);
    border-radius: 50%;
}
.en3-header::after {
    content: '';
    position: absolute;
    bottom: -20px; left: 20px;
    width: 80px; height: 80px;
    background: rgba(255,193,0,.1);
    border-radius: 50%;
}
.en3-header-gold-bar {
    height: 3px;
    background: linear-gradient(90deg, #FFC400, #FFE066, #FFC400);
}
.en3-header-inner {
    padding: 18px 22px 14px;
    position: relative; z-index: 1;
}
.en3-header-top {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 8px;
}
.en3-header-title {
    display: flex;
    align-items: center;
    gap: 10px;
}
.en3-icon-circle {
    width: 40px; height: 40px;
    background: rgba(255,255,255,.2);
    border-radius: 50%;
    display: flex; align-items: center; justify-content: center;
    font-size: 1.2rem;
    flex-shrink: 0;
}
.en3-header h3 {
    margin: 0;
    color: #fff;
    font-size: 1rem;
    font-weight: 700;
    font-family: 'SolaimanLipi','Kalpurush','Noto Sans Bengali',sans-serif !important;
}
.en3-header p {
    margin: 0;
    color: rgba(255,255,255,.72);
    font-size: .78rem;
    font-family: 'SolaimanLipi','Kalpurush','Noto Sans Bengali',sans-serif !important;
}
.en3-status-badge {
    display: inline-flex;
    align-items: center;
    gap: 5px;
    padding: 3px 11px;
    border-radius: 20px;
    font-size: .72rem;
    font-weight: 700;
    font-family: 'SolaimanLipi','Kalpurush','Noto Sans Bengali',sans-serif !important;
}
.en3-status-badge.active {
    background: #22c55e;
    color: #fff;
}
.en3-status-badge.inactive {
    background: rgba(255,255,255,.2);
    color: rgba(255,255,255,.8);
    border: 1px solid rgba(255,255,255,.3);
}

/* Slot pills */
.en3-slot-pills {
    display: flex;
    gap: 6px;
    margin-top: 10px;
}
.en3-slot-pill {
    display: inline-flex;
    align-items: center;
    gap: 4px;
    padding: 3px 10px;
    border-radius: 20px;
    font-size: .73rem;
    background: rgba(255,255,255,.15);
    color: rgba(255,255,255,.9);
    border: 1px solid rgba(255,255,255,.25);
    font-family: 'SolaimanLipi','Kalpurush',sans-serif !important;
}
.en3-slot-pill.enabled {
    background: rgba(255,193,0,.25);
    border-color: rgba(255,193,0,.5);
    color: #FFE066;
}

/* Body */
.en3-body {
    padding: 20px 22px 22px;
}

/* Section heading */
.en3-section-head {
    display: flex;
    align-items: center;
    gap: 8px;
    margin: 18px 0 10px;
}
.en3-section-head:first-child {
    margin-top: 0;
}
.en3-section-head-line {
    width: 3px; height: 18px;
    background: #251577;
    border-radius: 2px;
}
.en3-section-head span {
    font-size: .78rem;
    font-weight: 700;
    color: #251577;
    text-transform: uppercase;
    letter-spacing: .05em;
    font-family: 'SolaimanLipi','Kalpurush',sans-serif !important;
}

/* Toggle rows */
.en3-toggle-row {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 8px 12px;
    border-radius: 8px;
    margin-bottom: 5px;
    background: #f8f8ff;
    border: 1px solid #ebebfc;
    transition: background .15s;
}
.en3-toggle-row:hover {
    background: #f0f0fd;
}
.en3-toggle-label {
    display: flex;
    align-items: center;
    gap: 8px;
    font-size: .86rem;
    color: #251577;
    font-weight: 600;
    font-family: 'SolaimanLipi','Kalpurush',sans-serif !important;
}
.en3-toggle-icon {
    width: 28px; height: 28px;
    border-radius: 7px;
    display: flex; align-items: center; justify-content: center;
    font-size: .9rem;
    background: rgba(37,21,119,.08);
}

/* Switch */
.en3-switch {
    position: relative;
    display: inline-block;
    width: 44px;
    height: 24px;
    flex-shrink: 0;
}
.en3-switch input { opacity: 0; width: 0; height: 0; }
.en3-slider {
    position: absolute;
    inset: 0;
    background: #d1d5db;
    border-radius: 24px;
    cursor: pointer;
    transition: background .2s;
}
.en3-slider::before {
    content: '';
    position: absolute;
    left: 3px; top: 3px;
    width: 18px; height: 18px;
    background: #fff;
    border-radius: 50%;
    transition: transform .2s;
    box-shadow: 0 1px 4px rgba(0,0,0,.2);
}
.en3-switch input:checked + .en3-slider { background: #251577; }
.en3-switch input:checked + .en3-slider::before { transform: translateX(20px); }

/* Email input */
.en3-input-wrap {
    position: relative;
    margin: 16px 0 0;
}
.en3-input-wrap label {
    display: block;
    font-size: .78rem;
    font-weight: 700;
    color: #251577;
    margin-bottom: 6px;
    font-family: 'SolaimanLipi','Kalpurush',sans-serif !important;
}
.en3-email-input {
    width: 100%;
    padding: 10px 14px 10px 38px;
    border: 1.5px solid #dde0f8;
    border-radius: 9px;
    font-size: .88rem;
    background: #f8f8ff;
    color: #251577;
    box-sizing: border-box;
    font-family: 'SolaimanLipi','Kalpurush',sans-serif !important;
    transition: border-color .2s, box-shadow .2s;
    outline: none;
}
.en3-email-input:focus {
    border-color: #251577;
    box-shadow: 0 0 0 3px rgba(37,21,119,.1);
    background: #fff;
}
.en3-email-icon {
    position: absolute;
    left: 11px;
    bottom: 11px;
    font-size: .9rem;
    pointer-events: none;
}

/* Buttons */
.en3-btn-row {
    display: flex;
    gap: 8px;
    margin-top: 16px;
    flex-wrap: wrap;
}
.en3-btn {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 10px 22px;
    border-radius: 9px;
    font-size: .88rem;
    font-weight: 700;
    cursor: pointer;
    border: none;
    transition: opacity .15s, transform .1s, box-shadow .15s;
    font-family: 'SolaimanLipi','Kalpurush',sans-serif !important;
}
.en3-btn:active { transform: scale(.97); }
.en3-btn:disabled { opacity: .6; cursor: not-allowed; }

.en3-btn-primary {
    background: linear-gradient(135deg, #251577, #1d4ed8);
    color: #fff;
    box-shadow: 0 3px 10px rgba(37,21,119,.25);
    flex: 1;
    justify-content: center;
}
.en3-btn-primary:hover:not(:disabled) {
    box-shadow: 0 5px 16px rgba(37,21,119,.35);
}
.en3-btn-danger {
    background: #fff;
    color: #dc2626;
    border: 1.5px solid #fca5a5;
}
.en3-btn-danger:hover:not(:disabled) {
    background: #fee2e2;
}

/* Toast / message */
.en3-toast {
    display: none;
    margin-top: 12px;
    padding: 10px 14px;
    border-radius: 9px;
    font-size: .83rem;
    font-weight: 600;
    font-family: 'SolaimanLipi','Kalpurush',sans-serif !important;
    align-items: center;
    gap: 8px;
}
.en3-toast.show { display: flex; }
.en3-toast.ok  { background: #f0fdf4; color: #15803d; border: 1px solid #bbf7d0; }
.en3-toast.err { background: #fff5f5; color: #dc2626; border: 1px solid #fca5a5; }

/* Loading */
.en3-loading {
    padding: 18px;
    color: #9ca3af;
    font-size: .85rem;
    text-align: center;
    font-family: 'SolaimanLipi','Kalpurush',sans-serif !important;
}

/* Divider */
.en3-divider {
    border: none;
    border-top: 1px solid #ebebfc;
    margin: 14px 0;
}

/* Page toggles grid on wider screens */
.en3-pages-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 5px;
}
@media (max-width: 420px) {
    .en3-pages-grid { grid-template-columns: 1fr; }
    .en3-btn-primary { flex: unset; width: 100%; }
}
    `;
    document.head.appendChild(s);
}

/* ── Toggle builder ──────────────────────────────────────────── */

function _makeToggle({ id, icon, label, checked, iconBg = 'rgba(37,21,119,.08)' }) {
    return `
<div class="en3-toggle-row">
    <span class="en3-toggle-label">
        <span class="en3-toggle-icon" style="background:${iconBg}">${icon}</span>
        ${label}
    </span>
    <label class="en3-switch">
        <input type="checkbox" id="${id}" ${checked ? 'checked' : ''}>
        <span class="en3-slider"></span>
    </label>
</div>`;
}

/* ── Render ──────────────────────────────────────────────────── */

function _render(container, { title, sub, isSubscribed, email, prefs }) {
    const pagePrefs = prefs.pages || {};

    const slotPillMorning = `<span class="en3-slot-pill ${prefs.morning ? 'enabled' : ''}">🌅 সকাল ৮টা</span>`;
    const slotPillEvening = `<span class="en3-slot-pill ${prefs.evening ? 'enabled' : ''}">🌆 সন্ধ্যা ৮টা</span>`;

    const activePagesCount = PAGE_DEFS.filter(p => pagePrefs[p.key] !== false).length;

    container.innerHTML = `
<div class="en3-card">

    <!-- Header -->
    <div class="en3-header">
        <div class="en3-header-gold-bar"></div>
        <div class="en3-header-inner">
            <div class="en3-header-top">
                <div class="en3-header-title">
                    <div class="en3-icon-circle">📧</div>
                    <div>
                        <h3>${title}</h3>
                        <p>${sub || 'স্বয়ংক্রিয় ইমেইল ডাইজেস্ট পরিষেবা'}</p>
                    </div>
                </div>
                <span class="en3-status-badge ${isSubscribed ? 'active' : 'inactive'}">
                    ${isSubscribed ? '● সক্রিয়' : '○ নিষ্ক্রিয়'}
                </span>
            </div>
            ${isSubscribed ? `
            <div class="en3-slot-pills">
                ${slotPillMorning}
                ${slotPillEvening}
                <span class="en3-slot-pill enabled">📌 ${activePagesCount}/৬ পেজ</span>
            </div>` : ''}
        </div>
    </div>

    <!-- Body -->
    <div class="en3-body">

        <!-- Digest time slots -->
        <div class="en3-section-head">
            <div class="en3-section-head-line"></div>
            <span>📅 ডাইজেস্ট সময়</span>
        </div>

        <div style="display:flex;gap:8px;flex-wrap:wrap">
            ${_makeToggle({ id: 'en3-morning', icon: '🌅', label: 'সকাল ৮টায় ডাইজেস্ট', checked: prefs.morning })}
            ${_makeToggle({ id: 'en3-evening', icon: '🌆', label: 'সন্ধ্যা ৮টায় ডাইজেস্ট', checked: prefs.evening })}
        </div>

        <hr class="en3-divider">

        <!-- Per-page toggles -->
        <div class="en3-section-head">
            <div class="en3-section-head-line"></div>
            <span>📌 পেজ ভিত্তিক ইমেইল</span>
        </div>

        <div class="en3-pages-grid">
            ${PAGE_DEFS.map(p => _makeToggle({
                id:    `en3-page-${p.key}`,
                icon:  p.icon,
                label: p.label,
                checked: pagePrefs[p.key] !== false,
                iconBg: p.color + '18',
            })).join('')}
        </div>

        <hr class="en3-divider">

        <!-- Email input -->
        <div class="en3-input-wrap">
            <label for="en3-email">✉️ ইমেইল ঠিকানা</label>
            <span class="en3-email-icon">📬</span>
            <input
                class="en3-email-input"
                id="en3-email"
                type="email"
                value="${email || ''}"
                placeholder="example@gmail.com"
                autocomplete="email"
            >
        </div>

        <!-- Action buttons -->
        <div class="en3-btn-row">
            <button class="en3-btn en3-btn-primary" id="en3-save">
                ${isSubscribed ? '✅ আপডেট করুন' : '📨 সাবস্ক্রাইব করুন'}
            </button>
            ${isSubscribed
                ? `<button class="en3-btn en3-btn-danger" id="en3-unsub">🔕 আনসাবস্ক্রাইব</button>`
                : ''}
        </div>

        <!-- Toast message -->
        <div class="en3-toast" id="en3-toast"></div>

    </div>
</div>`;

    // Fix: slot toggles need separate wrappers (not inside a flex row together)
    // Re-render the slot section properly — remove the flex wrapper added above
    const slotGrid = container.querySelector('.en3-body > div[style*="flex"]');
    if (slotGrid) {
        slotGrid.style.cssText = 'display:grid;grid-template-columns:1fr 1fr;gap:5px';
    }
}

/* ── Toast helper ────────────────────────────────────────────── */

function _showToast(msg, type = 'ok') {
    const el = document.getElementById('en3-toast');
    if (!el) return;
    el.className = `en3-toast show ${type}`;
    el.textContent = msg;
    setTimeout(() => { if (el) el.className = 'en3-toast'; }, 4500);
}

/* ── Wire events ─────────────────────────────────────────────── */

function _wireEvents(container, uid, currentEmail) {
    const saveBtn  = container.querySelector('#en3-save');
    const unsubBtn = container.querySelector('#en3-unsub');

    if (saveBtn) {
        saveBtn.addEventListener('click', async () => {
            const emailInput = container.querySelector('#en3-email');
            const email      = (emailInput ? emailInput.value : currentEmail || '').trim();

            if (!email || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
                _showToast('⚠️ একটি বৈধ ইমেইল ঠিকানা লিখুন।', 'err');
                emailInput && emailInput.focus();
                return;
            }

            const morningEl = container.querySelector('#en3-morning');
            const eveningEl = container.querySelector('#en3-evening');

            const pages = {};
            PAGE_DEFS.forEach(p => {
                const el = container.querySelector(`#en3-page-${p.key}`);
                pages[p.key] = el ? el.checked : true;
            });

            const prefs = {
                morning: morningEl ? morningEl.checked : true,
                evening: eveningEl ? eveningEl.checked : true,
                tasks:   true,  // legacy key
                pages:   pages
            };

            saveBtn.disabled    = true;
            saveBtn.textContent = '⏳ সেভ হচ্ছে...';

            try {
                await window.EmailDB.saveEmailSubscription(uid, email, prefs);
                _showToast('✅ সাবস্ক্রিপশন সেভ হয়েছে! আপনি ইমেইল পাবেন।', 'ok');
                await _mount(document.getElementById('email-notif-widget') || container, uid);
            } catch (err) {
                console.error('EmailUI save error:', err);
                _showToast('❌ সেভ করতে সমস্যা হয়েছে। আবার চেষ্টা করুন।', 'err');
                saveBtn.disabled    = false;
                saveBtn.textContent = '📨 সাবস্ক্রাইব করুন';
            }
        });
    }

    if (unsubBtn) {
        unsubBtn.addEventListener('click', async () => {
            if (!confirm('ইমেইল নোটিফিকেশন বন্ধ করতে চান?')) return;
            unsubBtn.disabled    = true;
            unsubBtn.textContent = '⏳...';
            try {
                await window.EmailDB.removeEmailSubscription(uid);
                _showToast('🔕 সাবস্ক্রিপশন বাতিল হয়েছে।', 'ok');
                await _mount(document.getElementById('email-notif-widget') || container, uid);
            } catch (err) {
                console.error('EmailUI unsub error:', err);
                _showToast('❌ বাতিল করতে সমস্যা হয়েছে।', 'err');
                unsubBtn.disabled    = false;
                unsubBtn.textContent = '🔕 আনসাবস্ক্রাইব';
            }
        });
    }
}

/* ── Mount ───────────────────────────────────────────────────── */

async function _mount(el, uid) {
    const mountPoint = typeof el === 'string' ? document.getElementById(el) : el;
    if (!mountPoint) return;

    const title = mountPoint.dataset.title    || 'ইমেইল নোটিফিকেশন';
    const sub   = mountPoint.dataset.subtitle || '';

    _injectStyles();
    mountPoint.innerHTML = `<div class="en3-loading">⏳ লোড হচ্ছে...</div>`;

    const existing     = await window.EmailDB.getEmailSubscription(uid);
    const isSubscribed = !!(existing && existing.active);
    const email        = (existing && existing.email) || '';
    const prefs        = (existing && existing.prefs) || {
        morning: true, evening: true, tasks: true, pages: {}
    };

    _render(mountPoint, { title, sub, isSubscribed, email, prefs });
    _wireEvents(mountPoint, uid, email);
}

/* ── Bootstrap ───────────────────────────────────────────────── */

(async function init() {
    try {
        await _waitFor(() => window.AppDB   && typeof window.AppDB.onAuthStateChanged      === 'function', 'AppDB');
        await _waitFor(() => window.EmailDB && typeof window.EmailDB.saveEmailSubscription === 'function', 'EmailDB');

        window.AppDB.onAuthStateChanged(async (user) => {
            const mountPoint = document.getElementById('email-notif-widget');
            if (!mountPoint) return;
            if (!user) { mountPoint.innerHTML = ''; return; }
            await _mount(mountPoint, user.uid);
        });

    } catch (err) {
        console.error('email-ui.js init error:', err);
    }
})();

// Public API for manual mounting
window.EmailUI = { mount: _mount };
