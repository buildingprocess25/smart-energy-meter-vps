
let _mqttClient = null;
let _subscribedTopic = null;
let _firstSnap = true;

const MAP_METRIC_JS = {
    'Voltage_V': 'Voltage (V)',
    'Current_A': 'Current (A)',
    'Power_W': 'Power (W)',
    'Apparent_Power_kVA': 'Apparent Power (kVA)',
    'Reactive_Power_kVAR': 'Reactive Power (kVAR)',
    'Power_Factor': 'Power Factor',
    'Phase_Angle_deg': 'Sensor Angle (°)',
    'Frequency_Hz': 'Frequency (Hz)',
    'Active_Energy_kWh': 'Active Energy (kWh)',
    'Apparent_Energy_kVAh': 'Apparent Energy (kVAh)',
    'Reactive_Energy_kVARh': 'Reactive Energy (kVARh)'
};
function showGlobalLoader() { const g = document.getElementById('globalLoader'); if (g) { g.style.display = 'flex'; void g.offsetWidth; g.classList.remove('hidden'); } }
function hideGlobalLoader() { const g = document.getElementById('globalLoader'); if (g) { g.classList.add('hidden'); setTimeout(() => g.classList.contains('hidden') && (g.style.display = 'none'), 500); } }
let realtimeData = null;
let rawRealtimeData = null;
let isConnected = false;
let lastDataTimestamp = 0;
let connectionStartTime = 0;
let connectionCheckInterval = null;
let selectedDeviceId = '';
let selectedDeviceName = '';
let lastSensorValues = null;
// Per-phase last-seen: tracks when each phase last received an MQTT message
let _phaseLastSeen = {}; // { 'L1': <timestamp ms>, 'L2': <timestamp ms>, ... }
const PHASE_DATA_TIMEOUT_MS = 90000; // 90 detik — sesuai delta-publishing ESP32 (hanya kirim saat ada perubahan)
let _phaseTimeoutCheckId = null;
// Cache data phase terakhir yang valid — agar chart tidak anjlok ke 0 saat data stale sesaat
let _lastKnownPhaseData = {}; // { 'L1': { 'Voltage (V)': ..., ... }, 'L2': {...}, ... }
let _deviceListCache = [];
let _prevDeviceId = '';
let currentSessionId = null;
let sessionsData = {};
let _renamingSessionId = null;
let dbSearchQuery = '';
let _renamingDeviceId = null;
let selectedPhase = '';
let hourlyHistoryData = {};
let _hourlyListenerAttached = null;
let dailyHistoryData = {};
let _dayListenerAttached = null;
let realtimeChart = null;
let selectedParameter = 'current';
let timeFilter = 'all';
let chartTargetDate = null;
let _hourlyListenerDate = null;
let sessionChartData = [];
let sessionMeta = null;
let _userIsZoomed = false;
function _updateResetZoomUI() {
    const btn = document.getElementById('resetZoomBtn');
    if (btn) {
        if (_userIsZoomed) {
            btn.classList.remove('hidden');
        } else {
            btn.classList.add('hidden');
        }
    }
}
function resetChartZoom() {
    _userIsZoomed = false;
    if (realtimeChart) {
        if (realtimeChart.scales?.x?.options) {
            delete realtimeChart.scales.x.options.min;
            delete realtimeChart.scales.x.options.max;
        }
        if (realtimeChart.scales?.y?.options) {
            delete realtimeChart.scales.y.options.min;
            delete realtimeChart.scales.y.options.max;
        }
        try { realtimeChart.resetZoom('easeOutQuart'); } catch (_) { }
    }
    _updateResetZoomUI();
    _rebuildChart(false);
}
let _visiblePoints = 600;
let activeOfflineSessionData = null;
let activeOfflineChart = null;
let activeOfflineSelectedPhase = '';
let activeOfflineSelectedPage = 1;
const MAX_DATA_POINTS = 600;
const PARAM_KEYS = ['voltage', 'current', 'power', 'frequency', 'energy', 'powerFactor'];
let phaseChartData = {};
let chartLabels = [];
let chartTimestamps = [];
let _rafId = null;
let _rafDirty = false;
let _pageVisible = !document.hidden;
document.addEventListener('visibilitychange', () => {
    _pageVisible = !document.hidden;
    if (_pageVisible && _rafDirty && timeFilter === 'all') _scheduleRender();
});
function _scheduleRender() {
    if (_rafId) return;
    _rafId = requestAnimationFrame(_doRender);
}
function _showChartSpinner() {
    const s = document.getElementById('chartSpinner');
    if (s) s.classList.add('active');
}
function _hideChartSpinner() {
    const s = document.getElementById('chartSpinner');
    if (s) s.classList.remove('active');
}
function _fadeChartIn() {
    _hideChartSpinner();
    const canvas = document.getElementById('realtimeChart');
    if (canvas && canvas.style.opacity === '0') canvas.style.opacity = '1';
}
function _doRender() {
    _fadeChartIn();
    _rafId = null;
    _rafDirty = false;
    if (!realtimeChart || !_pageVisible || timeFilter !== 'all' || _userIsZoomed) return;
    const enabledKeys = _getEnabledPhaseKeys();
    const phases = enabledKeys.slice().sort((a, b) => parseInt(a.slice(1)) - parseInt(b.slice(1)));
    if (!phases.length) return;
    const total = chartLabels.length;
    if (total === 0) return;
    const visible = Math.min(_visiblePoints, total);
    const start = total - visible;
    realtimeChart.data.labels = chartLabels.slice(start);
    const slicedDatasets = [];
    let mismatch = false;
    phases.forEach((phase, i) => {
        let values = phaseChartData[phase]?.[selectedParameter] || [];
        if (!values.length) values = Array(total).fill(0);
        else if (values.length < total) {
            const firstValid = values.find(v => v != null && v > 0) ?? 0;
            values = [...Array(total - values.length).fill(firstValid), ...values];
        }
        const sliced = values.slice(start);
        const labelName = getPhaseLabel(phase);
        const ds = realtimeChart.data.datasets.find(d => d.label === labelName);
        if (ds) {
            ds.data = sliced;
            slicedDatasets.push({ data: sliced });
        } else {
            mismatch = true;
        }
    });
    if (mismatch || phases.length !== realtimeChart.data.datasets.length) {
        _rebuildChart();
        return;
    }
    // Use actual chart datasets (has hidden state, real data values) for Y bounds
    const { yMin, yMax } = getYBoundsMulti(realtimeChart.data.datasets, selectedParameter);
    realtimeChart.options.scales.y.min = yMin;
    realtimeChart.options.scales.y.max = yMax;
    realtimeChart.update('none');
}
let _rebuildTimer = null;
let _chartEntryAnimate = false;
let _clipPathCleanupId = null;
function _rebuildChart(animate = false) {
    if (_rebuildTimer) {
        clearTimeout(_rebuildTimer);
        _rebuildTimer = null;
    }
    _chartEntryAnimate = animate;
    if (!realtimeChart) {
        initChart();
    } else {
        _morphChartStructure(animate);
    }
    _startAggRebuild();
}
let _lastChartMinute = -1;
let _lastChartHour = -1;
let _lastChartDay = -1;
let _timeWindowCheckId = null;
let _aggRebuildId = null;
const PHASE_COLORS = [
    { line: '#00A651', bar: 'rgba(0,166,81,0.85)', light: 'rgba(0,166,81,0.15)' },    // L1: Vivid Green
    { line: '#1E90FF', bar: 'rgba(30,144,255,0.85)', light: 'rgba(30,144,255,0.15)' },  // L2: Dodger Blue
    { line: '#FF8C00', bar: 'rgba(255,140,0,0.85)', light: 'rgba(255,140,0,0.15)' },   // L3: Dark Orange
    { line: '#8A2BE2', bar: 'rgba(138,43,226,0.85)', light: 'rgba(138,43,226,0.15)' },  // L4: Blue Violet
    { line: '#FF1493', bar: 'rgba(255,20,147,0.85)', light: 'rgba(255,20,147,0.15)' },  // L5: Deep Pink
];

function hexToRgba(hex, alpha) {
    let c = (hex || '').replace('#', '');
    if (c.length === 3) c = c.split('').map(x => x + x).join('');
    const num = parseInt(c, 16);
    if (isNaN(num)) return `rgba(22, 119, 255, ${alpha})`;
    const r = (num >> 16) & 255;
    const g = (num >> 8) & 255;
    const b = num & 255;
    return `rgba(${r}, ${g}, ${b}, ${alpha})`;
}

function hslToHex(h, s, l) {
    l /= 100;
    const a = s * Math.min(l, 1 - l) / 100;
    const f = n => {
        const k = (n + h / 30) % 12;
        const color = l - a * Math.max(Math.min(k - 3, 9 - k, 1), -1);
        return `#${Math.round(255 * color).toString(16).padStart(2, '0')}`;
    };
    return `${f(0)}${f(4)}${f(8)}`;
}

function getPhaseColors(phase) {
    const dev = _deviceListCache.find(d => d.id === selectedDeviceId);
    const sensor = dev?.phases?.find(p => p.phase === phase);
    let customColor = sensor?.color;

    if (customColor && /^#[0-9A-Fa-f]{6}$/.test(customColor)) {
        return {
            line: customColor,
            bar: hexToRgba(customColor, 0.85),
            light: hexToRgba(customColor, 0.15)
        };
    }

    const idx = parseInt((phase || '').slice(1)) - 1;
    if (!isNaN(idx) && idx >= 0) {
        if (idx < 5) return PHASE_COLORS[idx];
        // Dynamic Golden Angle generator for L6..L99 so L11+ gets unique vivid colors automatically!
        const hue = (idx * 137.5) % 360;
        const hex = hslToHex(hue, 75, 45);
        return {
            line: hex,
            bar: hexToRgba(hex, 0.85),
            light: hexToRgba(hex, 0.15)
        };
    }
    return PHASE_COLORS[0];
}

async function updateSensorColor(deviceId, phase, color) {
    if (!deviceId || !phase || !color) return;
    try {
        const res = await fetch(`/api/devices/${encodeURIComponent(deviceId)}/sensors/${encodeURIComponent(phase)}/color`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ color })
        });
        const json = await res.json();
        if (json.ok) {
            const dev = _deviceListCache.find(d => d.id === deviceId);
            if (dev && dev.phases) {
                const s = dev.phases.find(p => p.phase === phase);
                if (s) s.color = color;
                else dev.phases.push({ phase, name: phase, enabled: true, color });
            }
            if (typeof renderPhaseToggles === 'function') renderPhaseToggles();
            _rebuildChart(false);
            if (dev) renderDeviceList([dev]);
        }
    } catch (e) {
        console.error("Error updating sensor color:", e);
    }
}

const PRESET_COLORS = [
    '#00A651', '#1E90FF', '#FF8C00', '#8A2BE2', '#FF1493', '#00CED1',
    '#FF3344', '#F59E0B', '#0D9488', '#6366F1', '#84CC16', '#EC4899'
];

let _activeColorTarget = { deviceId: null, phase: null };

function openSensorColorPicker(deviceId, phase, currentColor) {
    _activeColorTarget = { deviceId, phase };
    const modal = document.getElementById('sensorColorModal');
    const grid = document.getElementById('presetColorGrid');
    const title = document.getElementById('colorModalTitle');
    const customInput = document.getElementById('customColorPickerInput');

    if (title) title.textContent = `Warna Grafik Sensor ${phase}`;
    if (customInput) customInput.value = currentColor || '#1677ff';

    if (grid) {
        grid.innerHTML = PRESET_COLORS.map(c => `
            <div class="preset-color-box${(currentColor || '').toLowerCase() === c.toLowerCase() ? ' selected' : ''}" 
                 style="background: ${c};" 
                 title="Pilih warna ${c}" 
                 onclick="selectPresetColor('${c}')"></div>
        `).join('');
    }

    if (modal) {
        modal.classList.add('active');
        document.body.style.overflow = 'hidden';
    }
}

function closeSensorColorModal() {
    const modal = document.getElementById('sensorColorModal');
    if (modal) modal.classList.remove('active');
    document.body.style.overflow = '';
}

function selectPresetColor(color) {
    if (_activeColorTarget.deviceId && _activeColorTarget.phase) {
        updateSensorColor(_activeColorTarget.deviceId, _activeColorTarget.phase, color);
    }
    closeSensorColorModal();
}

function onCustomColorPicked(color) {
    selectPresetColor(color);
}
function createAreaGradient(ctx, chartArea, color) {
    const gradient = ctx.createLinearGradient(0, chartArea.top, 0, chartArea.bottom);
    const base = color.replace('rgba(', '').replace(')', '').split(',');
    const r = base[0].trim(), g = base[1].trim(), b = base[2].trim();
    gradient.addColorStop(0, `rgba(${r},${g},${b},0.32)`);
    gradient.addColorStop(0.5, `rgba(${r},${g},${b},0.10)`);
    gradient.addColorStop(1, `rgba(${r},${g},${b},0)`);
    return gradient;
}
let _mouseChartPos = { x: null, y: null, active: false, chart: null };

const crosshairPlugin = {
    id: 'isolarCrosshair',
    afterDraw(chart) {
        if (!_mouseChartPos.active || _mouseChartPos.x == null || _mouseChartPos.y == null) return;
        if (_mouseChartPos.chart && _mouseChartPos.chart !== chart && _mouseChartPos.chart.canvas !== chart.canvas) return;

        const ctx = chart.ctx;
        const { top, bottom, left, right } = chart.chartArea;
        const mx = _mouseChartPos.x;
        const my = _mouseChartPos.y;

        if (mx < left || mx > right || my < top || my > bottom) return;

        ctx.save();
        ctx.beginPath();
        // Garis Vertikal (+)
        ctx.moveTo(mx, top);
        ctx.lineTo(mx, bottom);
        // Garis Horizontal (+) tepat di posisi kursor mouse!
        ctx.moveTo(left, my);
        ctx.lineTo(right, my);

        ctx.lineWidth = 1;
        ctx.strokeStyle = 'rgba(100, 116, 139, 0.6)';
        ctx.setLineDash([4, 4]);
        ctx.stroke();

        // Gambar badge angka Y di sumbu Y (TradingView style)
        const yScale = chart.scales?.y;
        if (yScale) {
            const yVal = yScale.getValueForPixel(my);
            if (yVal != null && !isNaN(yVal)) {
                const info = (chart === realtimeChart ? PARAM_INFO[selectedParameter] : null) || {};
                const unit = info.unit || (yScale.options?.title?.text || '');
                const valStr = yVal.toFixed(2) + (unit ? ' ' + unit : '');
                ctx.font = 'bold 10px "Outfit", "Segoe UI", sans-serif';
                const textWidth = ctx.measureText(valStr).width;
                const badgeW = textWidth + 10;
                const badgeH = 18;
                const badgeX = left - badgeW - 4;
                const badgeY = my - badgeH / 2;

                ctx.fillStyle = 'rgba(15, 23, 42, 0.88)';
                ctx.fillRect(badgeX, badgeY, badgeW, badgeH);
                ctx.fillStyle = '#ffffff';
                ctx.textAlign = 'center';
                ctx.textBaseline = 'middle';
                ctx.fillText(valStr, badgeX + badgeW / 2, my);
            }
        }

        // Highlight titik data terdekat dari tooltip
        const active = chart.tooltip?._active;
        if (active?.length) {
            active.forEach(pt => {
                ctx.beginPath();
                ctx.arc(pt.element.x, pt.element.y, 4, 0, Math.PI * 2);
                ctx.fillStyle = pt.element.options?.borderColor || '#1677FF';
                ctx.strokeStyle = '#fff';
                ctx.lineWidth = 2;
                ctx.setLineDash([]);
                ctx.fill();
                ctx.stroke();
            });
        }

        ctx.restore();
    },
};
Chart.register(crosshairPlugin);

function _zoomChartFocal(chart, mouseX, factor) {
    const xScale = chart.scales?.x;
    if (!xScale) return;

    let minIdx = typeof xScale.min === 'number' ? xScale.min : (xScale.options.min ?? 0);
    let maxIdx = typeof xScale.max === 'number' ? xScale.max : (xScale.options.max ?? ((chart.data?.labels?.length || 1) - 1));

    const xVal = xScale.getValueForPixel(mouseX);
    if (xVal == null || isNaN(xVal)) return;

    let newXMin = xVal - (xVal - minIdx) * factor;
    let newXMax = xVal + (maxIdx - xVal) * factor;

    const totalPoints = chart.data?.labels?.length || 600;
    if ((newXMax - newXMin) < 1.5) return;
    if ((newXMax - newXMin) > totalPoints) {
        newXMin = 0;
        newXMax = totalPoints - 1;
    }

    newXMin = Math.max(0, newXMin);
    newXMax = Math.min(totalPoints - 1, newXMax);

    xScale.options.min = newXMin;
    xScale.options.max = newXMax;

    if (chart === realtimeChart) {
        _userIsZoomed = true;
        _updateResetZoomUI();
    }
    chart.update('none');
}

function _panChartPixels(chart, dx, dy = 0) {
    const xScale = chart.scales?.x;
    const yScale = chart.scales?.y;
    if (!xScale) return;

    let minIdx = typeof xScale.min === 'number' ? xScale.min : (xScale.options.min ?? 0);
    let maxIdx = typeof xScale.max === 'number' ? xScale.max : (xScale.options.max ?? ((chart.data?.labels?.length || 1) - 1));

    // Geser Sumbu X (Waktu)
    if (dx !== 0) {
        const xRange = maxIdx - minIdx;
        const xPixelWidth = xScale.width || 1;
        const xDelta = (dx / xPixelWidth) * xRange;

        const totalPoints = chart.data?.labels?.length || 600;
        let newMin = minIdx - xDelta;
        let newMax = maxIdx - xDelta;

        if (newMin < 0) {
            newMin = 0;
            newMax = xRange;
        }
        if (newMax > totalPoints - 1) {
            newMax = totalPoints - 1;
            newMin = newMax - xRange;
        }

        xScale.options.min = Math.max(0, newMin);
        xScale.options.max = Math.min(totalPoints - 1, newMax);
    }

    // Geser Sumbu Y (Nilai Parameter) secara Manual
    if (dy !== 0 && yScale) {
        let yMin = typeof yScale.min === 'number' ? yScale.min : (yScale.options.min ?? 0);
        let yMax = typeof yScale.max === 'number' ? yScale.max : (yScale.options.max ?? 10);
        const yRange = yMax - yMin;
        const yPixelHeight = yScale.height || 1;
        const yDelta = (dy / yPixelHeight) * yRange;

        yScale.options.min = yMin + yDelta;
        yScale.options.max = yMax + yDelta;
    }

    if (chart === realtimeChart) {
        _userIsZoomed = true;
        _updateResetZoomUI();
    }
    chart.update('none');
}

function _initChartGestures(chart) {
    if (!chart || !chart.canvas) return;
    const canvas = chart.canvas;
    canvas._activeChart = chart;

    if (canvas._gestureInit) return;
    canvas._gestureInit = true;

    let isDragging = false;
    let dragStartX = 0;
    let dragStartY = 0;

    const getActiveChart = () => canvas._activeChart || chart;

    canvas.addEventListener('mousemove', (e) => {
        const targetChart = getActiveChart();
        const rect = canvas.getBoundingClientRect();
        _mouseChartPos.x = e.clientX - rect.left;
        _mouseChartPos.y = e.clientY - rect.top;
        _mouseChartPos.active = true;
        _mouseChartPos.chart = targetChart;

        if (isDragging && targetChart) {
            const dx = e.clientX - dragStartX;
            const dy = e.clientY - dragStartY;
            dragStartX = e.clientX;
            dragStartY = e.clientY;
            _panChartPixels(targetChart, dx, dy);
        } else if (targetChart) {
            targetChart.render();
        }
    });

    canvas.addEventListener('mouseleave', () => {
        const targetChart = getActiveChart();
        if (_mouseChartPos.chart === targetChart || _mouseChartPos.chart?.canvas === canvas) {
            _mouseChartPos.active = false;
            _mouseChartPos.chart = null;
        }
        if (isDragging) {
            isDragging = false;
            canvas.style.cursor = 'crosshair';
        }
        if (targetChart) targetChart.render();
    });

    canvas.addEventListener('mousedown', (e) => {
        if (e.button !== 0) return;
        e.preventDefault();
        isDragging = true;
        dragStartX = e.clientX;
        dragStartY = e.clientY;
        canvas.style.cursor = 'grabbing';
    });

    window.addEventListener('mouseup', () => {
        if (isDragging) {
            isDragging = false;
            if (canvas) canvas.style.cursor = 'crosshair';
        }
    });

    // Double-click pada grafik untuk reset zoom & auto-fit sumbu Y
    canvas.addEventListener('dblclick', (e) => {
        const targetChart = getActiveChart();
        e.preventDefault();
        if (targetChart === realtimeChart) {
            resetChartZoom();
        } else if (targetChart) {
            if (targetChart.scales?.x?.options) {
                delete targetChart.scales.x.options.min;
                delete targetChart.scales.x.options.max;
            }
            if (targetChart.scales?.y?.options) {
                delete targetChart.scales.y.options.min;
                delete targetChart.scales.y.options.max;
            }
            targetChart.update('none');
        }
    });

    canvas.addEventListener('wheel', (e) => {
        const targetChart = getActiveChart();
        if (!targetChart) return;
        // Jika tidak menahan tombol Ctrl/Meta, biarkan halaman web di-scroll ke atas/bawah secara alami
        const isZoom = e.ctrlKey || e.metaKey;
        if (!isZoom) return;

        e.preventDefault();
        const rect = canvas.getBoundingClientRect();
        const mouseX = e.clientX - rect.left;
        const zoomFactor = Math.pow(1.0015, e.deltaY);
        _zoomChartFocal(targetChart, mouseX, zoomFactor);
    }, { passive: false });
}
const _ttDrag = { active: false, offX: 0, offY: 0, pinned: false };
function _initTooltipDrag(el) {
    if (el._dragInit) return;
    el._dragInit = true;
    el.style.cursor = 'grab';
    el.title = 'Drag untuk memindahkan · Klik 2x untuk reset posisi';
    el.addEventListener('dblclick', () => {
        _ttDrag.pinned = false;
        el.style.transition = '';
    });
    el.addEventListener('mousedown', e => {
        if (e.button !== 0) return;
        _ttDrag.active = true;
        _ttDrag.pinned = false;
        _ttDrag.offX = e.clientX - parseFloat(el.style.left || 0);
        _ttDrag.offY = e.clientY - parseFloat(el.style.top || 0);
        el.style.cursor = 'grabbing';
        el.style.transition = 'none';
        el.style.userSelect = 'none';
        e.stopPropagation();
        e.preventDefault();
    });
    document.addEventListener('mousemove', e => {
        if (!_ttDrag.active) return;
        _ttDrag.pinned = true;
        el.style.left = (e.clientX - _ttDrag.offX) + 'px';
        el.style.top = (e.clientY - _ttDrag.offY) + 'px';
    });
    document.addEventListener('mouseup', () => {
        if (!_ttDrag.active) return;
        _ttDrag.active = false;
        el.style.cursor = 'grab';
    });
}
function iSolarTooltipHandler(context) {
    const { chart, tooltip } = context;
    let el = document.getElementById('isc-tooltip');
    if (!el) {
        el = document.createElement('div');
        el.id = 'isc-tooltip';
        el.className = 'isc-tooltip';
        document.body.appendChild(el);
        _initTooltipDrag(el);
    }
    if (tooltip.opacity === 0) {
        if (_ttDrag.active || _ttDrag.pinned) return;
        el.style.opacity = '0';
        el.style.pointerEvents = 'none';
        _ttDrag.pinned = false;
        return;
    }
    const info = PARAM_INFO[selectedParameter] || {};
    const title = tooltip.title?.[0] || '';
    let html = `<div class="isc-tt-header"><span class="isc-tt-clock">🕐</span><span>${title}</span></div><div class="isc-tt-rows">`;
    (tooltip.dataPoints || []).forEach(dp => {
        const val = dp.parsed.y;
        const color = dp.dataset.borderColor || dp.dataset.backgroundColor;
        const label = dp.dataset.label;
        const disp = val != null ? val.toFixed(2) : '—';
        const unit = info.unit || (dp.chart.scales?.y?.options?.title?.text || '');
        html += `<div class="isc-tt-row">
            <span class="isc-tt-dot" style="background:${color}"></span>
            <span class="isc-tt-label">${label}</span>
            <span class="isc-tt-val">${disp}<span class="isc-tt-unit">${unit ? ' ' + unit : ''}</span></span>
        </div>`;
    });
    html += '</div>';
    el.innerHTML = html;
    const rect = chart.canvas.getBoundingClientRect();
    const cx = tooltip.caretX;
    const cy = tooltip.caretY;
    const ttW = el.offsetWidth || 190;
    const ttH = el.offsetHeight || 80;
    let left, top;
    const spaceR = rect.width - cx;
    const leftBase = spaceR > ttW + 24 ? cx + 16 : cx - ttW - 16;
    let topBase = cy - ttH / 2;
    topBase = Math.max(4, Math.min(topBase, rect.height - ttH - 4));
    left = rect.left + window.scrollX + leftBase;
    top = rect.top + window.scrollY + topBase;
    el.style.opacity = '1';
    el.style.transform = 'translateY(0)';
    el.style.pointerEvents = 'auto';
    if (!_ttDrag.pinned) {
        el.style.left = left + 'px';
        el.style.top = top + 'px';
    }
}
function hideIscTooltip() {
    const el = document.getElementById('isc-tooltip');
    if (!el) return;
    if (_ttDrag.active || _ttDrag.pinned) return;
    el.style.opacity = '0';
    el.style.pointerEvents = 'none';
}
function getPhaseLabel(phase) {
    const dev = _deviceListCache.find(d => d.id === selectedDeviceId);
    const phaseObj = dev?.phases?.find(p => p.phase === phase);
    return phaseObj?.name && phaseObj.name !== phase ? `${phase} (${phaseObj.name})` : phase;
}
const $ = id => document.getElementById(id);
const DOM = {
    get statusDot() { return $('statusDot'); },
    get statusText() { return $('statusText'); },
    get lastUpdate() { return $('lastUpdate'); },
    get captureBtn() { return $('captureBtn'); },
    get historyBody() { return $('historyTableBody'); },
    get historyCount() { return $('historyCount'); },
    get deviceList() { return $('deviceList'); },
    get deviceSelect() { return $('deviceSelect'); },
    get summaryDeviceSelect() { return $('summaryDeviceSelect'); },
    get paramSelect() { return $('parameterSelect'); },
    get intervalDisplay() { return $('intervalDisplay'); },
};
function resetChartData() { phaseChartData = {}; chartLabels = []; chartTimestamps = []; resetAggData(); }
function _detectPhaseKeys(raw) {
    if (!raw || typeof raw !== 'object') return [];
    return Object.keys(raw)
        .filter(k => /^L\d+$/.test(k))
        .sort((a, b) => parseInt(a.slice(1)) - parseInt(b.slice(1)));
}
function _getEnabledPhaseKeys() {
    const dev = _deviceListCache.find(d => d.id === selectedDeviceId);
    const registered = [];
    if (dev && dev.phases && dev.phases.length > 0) {
        dev.phases.forEach(p => {
            if (p.enabled !== false) registered.push(p.phase);
        });
    }
    // Dynamically merge any newly detected phases from chart data or raw live data
    const chartKeys = Object.keys(phaseChartData).filter(k => /^L\d+$/.test(k));
    const rawKeys = rawRealtimeData ? Object.keys(rawRealtimeData).filter(k => /^L\d+$/.test(k)) : [];
    const merged = new Set([...registered, ...chartKeys, ...rawKeys]);
    if (!merged.size) return ['L1'];
    return Array.from(merged).sort((a, b) => parseInt(a.slice(1)) - parseInt(b.slice(1)));
}
function normalizeHistoryData(raw) {
    if (!raw) return null;
    const phases = _detectPhaseKeys(raw);
    if (!phases.length) return null;
    const getVal = (phase, key) =>
        raw[phase] && raw[phase][key] ? parseFloat(raw[phase][key]) || 0 : 0;
    const voltages = phases.map(p => getVal(p, 'Voltage (V)'));
    const currents = phases.map(p => getVal(p, 'Current (A)'));
    const powers = phases.map(p => getVal(p, 'Power (W)'));
    const freqs = phases.map(p => getVal(p, 'Frequency (Hz)'));
    const energies = phases.map(p => getVal(p, 'Active Energy (kWh)'));
    const pfs = phases.map(p => getVal(p, 'Power Factor'));
    const sum = arr => arr.reduce((a, b) => a + b, 0);
    const activePhases = voltages.filter(v => v > 0).length;
    const denom = activePhases > 0 ? activePhases : 1;
    return {
        Voltage: sum(voltages) / denom,
        Current: sum(currents),
        Power: sum(powers),
        Frequency: sum(freqs) / denom,
        Apparent: phases.reduce((s, p) => s + getVal(p, 'Apparent Power (kVA)'), 0),
        Reactive: phases.reduce((s, p) => s + getVal(p, 'Reactive Power (kVAR)'), 0),
        Energy: sum(energies),
        PowerFactor: sum(pfs) / denom,
        Phase1: getVal(phases[0], 'Sensor Angle (°)'),
        EnergyApparent: phases.reduce((s, p) => s + getVal(p, 'Apparent Energy (kVAh)'), 0),
        EnergyReactive: phases.reduce((s, p) => s + getVal(p, 'Reactive Energy (kVARh)'), 0),
        DeviceTimestamp: raw.Timestamp || '',
        _phases: phases,
    };
}
function getPhaseDisplayData(raw, phase) {
    if (!raw) return null;
    const phaseData = raw[phase];
    if (!phaseData || typeof phaseData !== 'object') return null;
    const f = key => { try { return parseFloat(phaseData[key] || 0) || 0; } catch (_) { return 0; } };
    return {
        Voltage: f('Voltage (V)'),
        Current: f('Current (A)'),
        Power: f('Power (W)'),
        Frequency: f('Frequency (Hz)'),
        Energy: f('Active Energy (kWh)'),
        PowerFactor: f('Power Factor'),
        Apparent: f('Apparent Power (kVA)'),
        Reactive: f('Reactive Power (kVAR)'),
        Phase1: f('Sensor Angle (°)'),
        EnergyApparent: f('Apparent Energy (kVAh)'),
        EnergyReactive: f('Reactive Energy (kVARh)'),
        _phases: [phase],
    };
}
function setPhase(phase, resetToggles = true) {
    selectedPhase = phase;
    document.querySelectorAll('.phase-btn').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.phase === phase);
    });
    if (rawRealtimeData) {
        const data = getPhaseDisplayData(rawRealtimeData, phase);
        if (data) updateDisplayCards(data);
        else updateDisplayCardsBlank();
    }

    if (resetToggles) {
        // Sembunyikan semua sensor di chart kecuali yang sedang aktif dipilih
        _hiddenPhases.clear();
        const enabledKeys = _getEnabledPhaseKeys();
        enabledKeys.forEach(k => {
            if (k !== phase) {
                _hiddenPhases.add(k);
            }
        });
    }

    if (typeof renderPhaseToggles === 'function') {
        renderPhaseToggles();
    }

    if (realtimeChart && realtimeChart.data && realtimeChart.data.datasets) {
        realtimeChart.data.datasets.forEach(ds => {
            if (ds._phaseKey) {
                ds.hidden = _hiddenPhases.has(ds._phaseKey);
            }
        });
        const { yMin, yMax } = getYBoundsMulti(realtimeChart.data.datasets, selectedParameter);
        realtimeChart.options.scales.y.min = yMin;
        realtimeChart.options.scales.y.max = yMax;
        realtimeChart.update('none');
    }
}
function updatePhaseSelector(phases) {
    const container = $('phaseSelectorBtns');
    if (!container) return;
    const dev = _deviceListCache.find(d => d.id === selectedDeviceId);
    const enabledKeys = _getEnabledPhaseKeys();
    const mergedPhases = Array.from(new Set([...(phases || []), ...enabledKeys])).sort((a, b) => parseInt(a.slice(1)) - parseInt(b.slice(1)));

    const enabledPhases = mergedPhases.filter(p => {
        const po = dev?.phases?.find(ph => ph.phase === p);
        return !po || po.enabled !== false;
    });
    if (!selectedPhase || !enabledPhases.includes(selectedPhase)) {
        selectedPhase = enabledPhases[0] || '';
    }
    container.innerHTML = enabledPhases.map(p => {
        const phaseObj = dev?.phases?.find(ph => ph.phase === p);
        const label = phaseObj?.name && phaseObj.name !== p
            ? `${p} <span class="phase-btn-sub">${phaseObj.name}</span>`
            : p;
        return `<button class="phase-btn${selectedPhase === p ? ' active' : ''}" data-phase="${p}" onclick="setPhase('${p}')">${label}</button>`;
    }).join('');
    if (selectedPhase) {
        setPhase(selectedPhase, false);
    } else {
        if (typeof renderPhaseToggles === 'function') renderPhaseToggles();
    }
}
const _p2 = v => String(v).padStart(2, '0');
function _minKey(ts) { const d = new Date(ts); return `${d.getFullYear()}-${_p2(d.getMonth() + 1)}-${_p2(d.getDate())}T${_p2(d.getHours())}:${_p2(d.getMinutes())}`; }
function _hourKey(ts) { const d = new Date(ts); return `${d.getFullYear()}-${_p2(d.getMonth() + 1)}-${_p2(d.getDate())}T${_p2(d.getHours())}`; }
function _dayKey(ts) { const d = new Date(ts); return `${d.getFullYear()}-${_p2(d.getMonth() + 1)}-${_p2(d.getDate())}`; }
let phaseMinAgg = {};
let phaseHourAgg = {};
let phaseDayAgg = {};
let _prevMinKey = '';
let _prevHourKey = '';
let _prevDayKey = '';
function _ensureAgg(agg, phase, param, key) {
    if (!agg[phase]) agg[phase] = {};
    if (!agg[phase][param]) agg[phase][param] = {};
    if (!agg[phase][param][key]) agg[phase][param][key] = { sum: 0, count: 0 };
}
function _avgOf(agg, phase, param, key) {
    const e = agg[phase]?.[param]?.[key];
    return (e && e.count > 0) ? e.sum / e.count : null;
}
function accumulatePoint(raw) {
    if (!raw) return;
    const now = Date.now();
    const minKey = _minKey(now);
    const hourKey = _hourKey(now);
    const dayKey = _dayKey(now);
    const phases = _detectPhaseKeys(raw);
    if (_prevMinKey && _prevMinKey !== minKey) {
        phases.forEach(phase => {
            PARAM_KEYS.forEach(param => {
                const avg = _avgOf(phaseMinAgg, phase, param, _prevMinKey);
                if (avg === null) return;
                const hk = _prevHourKey || hourKey;
                _ensureAgg(phaseHourAgg, phase, param, hk);
                phaseHourAgg[phase][param][hk].sum += avg;
                phaseHourAgg[phase][param][hk].count += 1;
            });
        });
    }
    if (_prevHourKey && _prevHourKey !== hourKey) {
        phases.forEach(phase => {
            PARAM_KEYS.forEach(param => {
                const avg = _avgOf(phaseHourAgg, phase, param, _prevHourKey);
                if (avg === null) return;
                const dk = _prevDayKey || dayKey;
                _ensureAgg(phaseDayAgg, phase, param, dk);
                phaseDayAgg[phase][param][dk].sum += avg;
                phaseDayAgg[phase][param][dk].count += 1;
            });
        });
        _pruneOldMinAgg();
    }
    _prevMinKey = minKey;
    _prevHourKey = hourKey;
    _prevDayKey = dayKey;
    phases.forEach(phase => {
        const pd = raw[phase] || {};
        const fv = k => { try { return parseFloat(pd[k] || 0) || 0; } catch (_) { return 0; } };
        const vals = {
            voltage: fv('Voltage (V)'),
            current: fv('Current (A)'),
            power: fv('Power (W)'),
            frequency: fv('Frequency (Hz)'),
            energy: fv('Active Energy (kWh)'),
            powerFactor: fv('Power Factor'),
        };
        PARAM_KEYS.forEach(param => {
            _ensureAgg(phaseMinAgg, phase, param, minKey);
            phaseMinAgg[phase][param][minKey].sum += vals[param];
            phaseMinAgg[phase][param][minKey].count += 1;
        });
    });
}
function _pruneOldMinAgg() {
    const cutoff = _minKey(Date.now() - 2 * 3_600_000);
    Object.values(phaseMinAgg).forEach(phaseData =>
        Object.values(phaseData).forEach(paramData =>
            Object.keys(paramData).forEach(k => { if (k < cutoff) delete paramData[k]; })
        )
    );
}
function resetAggData() {
    phaseMinAgg = {};
    phaseHourAgg = {};
    phaseDayAgg = {};
    _prevMinKey = '';
    _prevHourKey = '';
    _prevDayKey = '';
}
function rebuildCascadeFromRaw() {
    resetAggData();
    if (!chartTimestamps.length) return;
    const phases = Object.keys(phaseChartData);
    if (!phases.length) return;
    for (let i = 0; i < chartTimestamps.length; i++) {
        const ts = chartTimestamps[i];
        const mk = _minKey(ts);
        phases.forEach(phase => {
            PARAM_KEYS.forEach(param => {
                const v = phaseChartData[phase]?.[param]?.[i];
                if (v == null || isNaN(v)) return;
                _ensureAgg(phaseMinAgg, phase, param, mk);
                phaseMinAgg[phase][param][mk].sum += v;
                phaseMinAgg[phase][param][mk].count += 1;
            });
        });
    }
    const liveMinKey = _minKey(Date.now());
    phases.forEach(phase => {
        PARAM_KEYS.forEach(param => {
            const minKeys = Object.keys(phaseMinAgg[phase]?.[param] || {});
            minKeys.forEach(mk => {
                if (mk === liveMinKey) return;
                const e = phaseMinAgg[phase][param][mk];
                if (!e || e.count === 0) return;
                const hk = mk.slice(0, 13);
                _ensureAgg(phaseHourAgg, phase, param, hk);
                phaseHourAgg[phase][param][hk].sum += e.sum / e.count;
                phaseHourAgg[phase][param][hk].count += 1;
            });
        });
    });
    const liveHourKey = _hourKey(Date.now());
    phases.forEach(phase => {
        PARAM_KEYS.forEach(param => {
            const hourKeys = Object.keys(phaseHourAgg[phase]?.[param] || {});
            hourKeys.forEach(hk => {
                if (hk === liveHourKey) return;
                const e = phaseHourAgg[phase][param][hk];
                if (!e || e.count === 0) return;
                const dk = hk.slice(0, 10);
                _ensureAgg(phaseDayAgg, phase, param, dk);
                phaseDayAgg[phase][param][dk].sum += e.sum / e.count;
                phaseDayAgg[phase][param][dk].count += 1;
            });
        });
    });
    const now = Date.now();
    _prevMinKey = _minKey(now);
    _prevHourKey = _hourKey(now);
    _prevDayKey = _dayKey(now);
}
function getHourlyHistoryData(phase, param) {
    const targetDateObj = chartTargetDate ? new Date(chartTargetDate + 'T00:00:00') : new Date();
    const todayDate = new Date();
    todayDate.setHours(0, 0, 0, 0);
    const isToday = targetDateObj.getTime() === todayDate.getTime();

    const targetStr = `${targetDateObj.getFullYear()}-${_p2(targetDateObj.getMonth() + 1)}-${_p2(targetDateObj.getDate())}`;

    const fieldMap = {
        voltage: 'Voltage', current: 'Current', power: 'Power',
        frequency: 'Frequency', energy: 'Energy', powerFactor: 'PowerFactor',
    };
    const field = fieldMap[param] || 'Voltage';
    const labels = [], values = [];

    const now = new Date();
    // Batas akhir: jika hari ini, sampai slot 15 menit terakhir yang sudah lewat
    const endHour = isToday ? now.getHours() : 23;
    const endMin = isToday ? Math.floor(now.getMinutes() / 15) * 15 : 45;

    const liveParamKeys = {
        Voltage: 'Voltage (V)', Current: 'Current (A)', Power: 'Power (W)',
        Frequency: 'Frequency (Hz)', Energy: 'Active Energy (kWh)', PowerFactor: 'Power Factor'
    };

    for (let h = 0; h <= endHour; h++) {
        const maxMin = (isToday && h === endHour) ? endMin : 45;
        for (let m = 0; m <= maxMin; m += 15) {
            labels.push(`${_p2(h)}:${_p2(m)}`);
            const key = `${_p2(h)}${_p2(m)}`;
            const rec = hourlyHistoryData[phase]?.[key];

            if (rec && rec.date === targetStr) {
                if (rec.offline) {
                    // Slot offline → null agar chart tampilkan gap, bukan garis ke 0
                    values.push(null);
                } else if (rec[field] != null) {
                    values.push(parseFloat(parseFloat(rec[field]).toFixed(4)));
                } else {
                    values.push(null);
                }
            } else if (isToday && h === endHour && m === endMin
                && isConnected && rawRealtimeData?.[phase]) {
                // Slot sekarang: pakai data live jika belum ada snapshot tersimpan
                const liveVal = parseFloat(rawRealtimeData[phase][liveParamKeys[field] || 'Voltage (V)']) || 0;
                values.push(parseFloat(liveVal.toFixed(4)));
            } else {
                // Belum ada data untuk slot ini
                values.push(null);
            }
        }
    }
    return { labels, values };
}
async function fetchChartDataFromServer() {
    if (!selectedDeviceId) return;
    const range = timeFilter;
    if (range === 'all') return;

    _showChartSpinner();
    try {
        const res = await fetch(`/api/devices/${selectedDeviceId}/chart-data?date=${chartTargetDate}&range=${range}`);
        const list = await res.json();

        if (range === 'day') {
            hourlyHistoryData = {};
            list.forEach(item => {
                const d = new Date(item.timestamp);
                const hh = String(d.getHours()).padStart(2, '0');
                const mm = String(d.getMinutes()).padStart(2, '0');
                const key = `${hh}${mm}`;
                const datePart = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;

                Object.keys(item.data).forEach(phase => {
                    if (!hourlyHistoryData[phase]) hourlyHistoryData[phase] = {};
                    hourlyHistoryData[phase][key] = {
                        Voltage: item.data[phase].Voltage,
                        Current: item.data[phase].Current,
                        Power: item.data[phase].Power,
                        Frequency: item.data[phase].Frequency,
                        Energy: item.data[phase].Energy,
                        PowerFactor: item.data[phase].PowerFactor,
                        offline: item.data[phase].offline || false,
                        date: datePart
                    };
                });
            });
            _refreshDayChartFromDB();
        } else if (range === 'week') {
            dailyHistoryData = {};
            list.forEach(item => {
                const d = new Date(item.timestamp);
                const dateKey = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;

                Object.keys(item.data).forEach(phase => {
                    if (!dailyHistoryData[phase]) dailyHistoryData[phase] = {};
                    dailyHistoryData[phase][dateKey] = {
                        Voltage: item.data[phase].Voltage,
                        Current: item.data[phase].Current,
                        Power: item.data[phase].Power,
                        Frequency: item.data[phase].Frequency,
                        Energy: item.data[phase].Energy,
                        PowerFactor: item.data[phase].PowerFactor
                    };
                });
            });
            _refreshWeekChartFromDB();
        }
    } catch (err) {
        console.error("Error fetching chart data:", err);
        _fadeChartIn();
    }
}
function _attachHourlyListener(deviceId) {
    if (_hourlyListenerAttached === deviceId && _hourlyListenerDate === chartTargetDate) {
        if (timeFilter === 'day') fetchChartDataFromServer();
        return;
    }
    _hourlyListenerAttached = deviceId;
    _hourlyListenerDate = chartTargetDate;
    fetchChartDataFromServer();
}
function _refreshDayChartFromDB() {
    if (!realtimeChart || timeFilter !== 'day') return;
    const { labels, datasets } = getAllPhaseDatasets();
    realtimeChart.data.labels = labels;
    realtimeChart.data.datasets = datasets;
    const { yMin, yMax } = getYBoundsMulti(datasets, selectedParameter);
    realtimeChart.options.scales.y.min = yMin;
    realtimeChart.options.scales.y.max = yMax;
    realtimeChart.update('none');
    _fadeChartIn();
}
function _attachDayListener(deviceId) {
    if (_dayListenerAttached === deviceId) {
        if (timeFilter === 'week') fetchChartDataFromServer();
        return;
    }
    _dayListenerAttached = deviceId;
    fetchChartDataFromServer();
}
function _refreshWeekChartFromDB() {
    if (!realtimeChart || timeFilter !== 'week') return;
    const { labels, datasets } = getAllPhaseDatasets();
    realtimeChart.data.labels = labels;
    realtimeChart.data.datasets = datasets;
    const { yMin, yMax } = getYBoundsMulti(datasets, selectedParameter);
    realtimeChart.options.scales.y.min = yMin;
    realtimeChart.options.scales.y.max = yMax;
    realtimeChart.update('none');
    _fadeChartIn();
}
function getDayViewData(phase, param) {
    const fieldMap = {
        voltage: 'Voltage', current: 'Current', power: 'Power',
        frequency: 'Frequency', energy: 'Energy', powerFactor: 'PowerFactor',
    };
    const field = fieldMap[param] || 'Voltage';
    const phaseRec = dailyHistoryData[phase] || {};

    const targetDateObj = chartTargetDate ? new Date(chartTargetDate + 'T00:00:00') : new Date();
    const daysArr = [];
    for (let i = 6; i >= 0; i--) {
        const d = new Date(targetDateObj);
        d.setDate(d.getDate() - i);
        daysArr.push(`${d.getFullYear()}-${_p2(d.getMonth() + 1)}-${_p2(d.getDate())}`);
    }

    const labels = [], values = [];
    daysArr.forEach(dateKey => {
        const [y, m, dStr] = dateKey.split('-');
        const _MON = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
        labels.push(`${parseInt(dStr)} ${_MON[parseInt(m) - 1]}`);
        const rec = phaseRec[dateKey];
        if (rec && rec[field] != null) {
            values.push(parseFloat(parseFloat(rec[field]).toFixed(4)));
        } else if (dateKey === new Date().toLocaleDateString('en-CA') && isConnected && rawRealtimeData && rawRealtimeData[phase]) {
            const liveParamKeys = {
                Voltage: 'Voltage (V)', Current: 'Current (A)', Power: 'Power (W)',
                Frequency: 'Frequency (Hz)', Energy: 'Active Energy (kWh)', PowerFactor: 'Power Factor'
            };
            const liveField = liveParamKeys[field] || 'Voltage (V)';
            const liveVal = parseFloat(rawRealtimeData[phase][liveField]) || 0;
            values.push(parseFloat(liveVal.toFixed(4)));
        } else {
            values.push(0);
        }
    });
    return { labels, values };
}
function getAggregatedDataForPhase(phase, param) {
    if (timeFilter === 'all') return { labels: chartLabels, values: phaseChartData[phase]?.[param] || [] };
    if (timeFilter === 'day') return getHourlyHistoryData(phase, param);
    return getDayViewData(phase, param);
}
const MODAL_ICONS = {
    success: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3"><polyline points="20 6 9 17 4 12"></polyline></svg>',
    warning: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"></path><line x1="12" y1="9" x2="12" y2="13"></line><line x1="12" y1="17" x2="12.01" y2="17"></line></svg>',
    error: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"></circle><line x1="15" y1="9" x2="9" y2="15"></line><line x1="9" y1="9" x2="15" y2="15"></line></svg>',
    info: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"></circle><line x1="12" y1="16" x2="12" y2="12"></line><line x1="12" y1="8" x2="12.01" y2="8"></line></svg>',
};
function showModal(title, message, type = 'info', buttons = ['ok']) {
    return new Promise(resolve => {
        $('modalTitle').textContent = title;
        $('modalMessage').textContent = message;
        const iconEl = $('modalIcon');
        iconEl.className = 'modal-icon ' + type;
        iconEl.innerHTML = MODAL_ICONS[type] || MODAL_ICONS.info;
        const btnsEl = $('modalButtons');
        btnsEl.innerHTML = '';
        if (buttons.includes('confirm')) {
            const cancelBtn = document.createElement('button');
            cancelBtn.className = 'modal-btn modal-btn-secondary';
            cancelBtn.textContent = 'BATAL';
            cancelBtn.onclick = () => { closeModal(); resolve(false); };
            btnsEl.appendChild(cancelBtn);
        }
        const primaryBtn = document.createElement('button');
        primaryBtn.className = 'modal-btn modal-btn-primary';
        primaryBtn.textContent = buttons.includes('confirm') ? 'YA, LANJUTKAN' : 'OK';
        primaryBtn.onclick = () => { closeModal(); resolve(true); };
        btnsEl.appendChild(primaryBtn);
        $('customModal').classList.add('active');
        document.body.style.overflow = 'hidden';
    });
}
function closeModal() {
    $('customModal').classList.remove('active');
    document.body.style.overflow = '';
}
document.addEventListener('click', e => { if (e.target === $('customModal')) closeModal(); });
document.addEventListener('keydown', e => { if (e.key === 'Escape') closeModal(); });
const CARD_IDS = ['voltage', 'current', 'power', 'frequency', 'energy', 'powerFactor'];
const CARD_DEC = { voltage: 1, current: 2, power: 1, frequency: 1, energy: 3, powerFactor: 3 };
function updateDisplayCards(data) {
    const fmt = (v, d) => (v != null && !isNaN(v)) ? parseFloat(v).toFixed(d) : '---';
    CARD_IDS.forEach(id => {
        const el = $(id);
        if (el) el.textContent = fmt(data[id.charAt(0).toUpperCase() + id.slice(1)], CARD_DEC[id]);
    });
    if (DOM.lastUpdate) {
        DOM.lastUpdate.textContent = 'Last update: ' + new Date().toLocaleTimeString('id-ID')
            + (selectedPhase ? ` · ${selectedPhase}` : '');
        DOM.lastUpdate.classList.add('online');
        DOM.lastUpdate.classList.remove('offline');
    }
}
function updateDisplayCardsBlank(state = 'offline') {
    CARD_IDS.forEach(id => { const el = $(id); if (el) el.textContent = '---'; });
    if (DOM.lastUpdate) {
        DOM.lastUpdate.classList.remove('online', 'offline', 'connecting');
        if (state === 'connecting') {
            DOM.lastUpdate.textContent = 'Waiting for data...';
            DOM.lastUpdate.classList.add('connecting');
        } else {
            DOM.lastUpdate.textContent = 'Device offline';
            DOM.lastUpdate.classList.add('offline');
        }
    }
}
const PARAM_INFO = {
    voltage: { label: 'Voltage', unit: 'V', color: '#FFA500', border: '#FF8C00' },
    current: { label: 'Current', unit: 'A', color: '#0066CC', border: '#0052A3' },
    power: { label: 'Power', unit: 'W', color: '#00A651', border: '#008040' },
    frequency: { label: 'Frequency', unit: 'Hz', color: '#6B46C1', border: '#5A3AA0' },
    energy: { label: 'Energy', unit: 'kWh', color: '#00A651', border: '#008040' },
    powerFactor: { label: 'Power Factor', unit: '', color: '#6B46C1', border: '#5A3AA0' },
};
function updateDateNavigatorUI() {
    const nav = document.getElementById('chartDateNav');
    const label = document.getElementById('isolarDateLabel');
    const hiddenDate = document.getElementById('chartHiddenDate');
    if (!nav || !label || !hiddenDate) return;

    if (timeFilter === 'all' || timeFilter === 'session') {
        nav.style.display = 'none';
        return;
    }
    nav.style.display = 'flex';

    if (!chartTargetDate) {
        chartTargetDate = new Date().toLocaleDateString('en-CA');
    }

    const todayDate = new Date();
    const minDate = new Date();
    minDate.setDate(todayDate.getDate() - 30);
    hiddenDate.min = minDate.toLocaleDateString('en-CA');
    hiddenDate.max = todayDate.toLocaleDateString('en-CA');
    hiddenDate.value = chartTargetDate;

    const targetDateObj = new Date(chartTargetDate + 'T00:00:00');

    if (timeFilter === 'day') {
        const d = String(targetDateObj.getDate()).padStart(2, '0');
        const m = String(targetDateObj.getMonth() + 1).padStart(2, '0');
        const y = targetDateObj.getFullYear();
        label.innerText = `${d}/${m}/${y}`;
    } else if (timeFilter === 'week') {
        const startDateObj = new Date(targetDateObj);
        startDateObj.setDate(startDateObj.getDate() - 6);
        const sd = String(startDateObj.getDate()).padStart(2, '0');
        const sm = String(startDateObj.getMonth() + 1).padStart(2, '0');
        const sy = startDateObj.getFullYear();
        const ed = String(targetDateObj.getDate()).padStart(2, '0');
        const em = String(targetDateObj.getMonth() + 1).padStart(2, '0');
        const ey = targetDateObj.getFullYear();
        label.innerText = `${sd}/${sm}/${sy} - ${ed}/${em}/${ey}`;
    }
}

function openNativeDatePicker() {
    const hiddenDate = document.getElementById('chartHiddenDate');
    if (hiddenDate && hiddenDate.showPicker) {
        hiddenDate.showPicker();
    }
}

function onHiddenDateChange() {
    const hiddenDate = document.getElementById('chartHiddenDate');
    if (!hiddenDate || !hiddenDate.value) return;
    chartTargetDate = hiddenDate.value;
    updateDateNavigatorUI();

    if (timeFilter === 'day' && selectedDeviceId) _attachHourlyListener(selectedDeviceId);
    if (timeFilter === 'week' && selectedDeviceId && realtimeChart) _refreshWeekChartFromDB();
}

function shiftChartDate(daysDirection) {
    if (!chartTargetDate) return;
    const targetDateObj = new Date(chartTargetDate + 'T00:00:00');

    let shiftAmount = daysDirection;
    if (timeFilter === 'week') {
        shiftAmount = daysDirection * 7;
    }

    targetDateObj.setDate(targetDateObj.getDate() + shiftAmount);

    const todayDate = new Date();
    todayDate.setHours(0, 0, 0, 0);
    if (targetDateObj > todayDate) return;

    const minDate = new Date();
    minDate.setDate(todayDate.getDate() - 30);
    minDate.setHours(0, 0, 0, 0);
    if (targetDateObj < minDate) return;

    chartTargetDate = targetDateObj.toLocaleDateString('en-CA');
    updateDateNavigatorUI();

    if (timeFilter === 'day' && selectedDeviceId) _attachHourlyListener(selectedDeviceId);
    if (timeFilter === 'week' && selectedDeviceId && realtimeChart) _refreshWeekChartFromDB();
}

function setTimeFilter(filter) {
    if (timeFilter === filter) return;
    timeFilter = filter;

    const canvas = document.getElementById('realtimeChart');
    if (canvas) {
        canvas.style.transition = 'none';
        canvas.style.opacity = '0';
        _showChartSpinner();
        void canvas.offsetWidth;
        canvas.style.transition = 'opacity 0.25s ease';
    }

    const sessionSel = document.getElementById('chartSessionSelect');
    if (sessionSel) {
        sessionSel.style.display = filter === 'session' ? 'inline-block' : 'none';
    }

    const dashBatchNav = document.getElementById('dashChartBatchNav');
    if (dashBatchNav && filter !== 'session') {
        dashBatchNav.style.display = 'none';
    }

    updateDateNavigatorUI();

    _userIsZoomed = false;
    _visiblePoints = filter === 'day' ? 24 : (filter === 'week' ? 7 : 600);
    document.querySelectorAll('.time-filter-btn').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.filter === filter);
    });
    const now = new Date();
    _lastChartMinute = now.getMinutes();
    _lastChartHour = now.getHours();
    _lastChartDay = now.getDate();
    if (filter === 'day' && selectedDeviceId) _attachHourlyListener(selectedDeviceId);
    if (filter === 'week' && selectedDeviceId) _attachDayListener(selectedDeviceId);
    if (filter === 'session' && selectedDeviceId) loadChartSessionsDropdown();

    _rebuildChart(false);
}

async function loadChartSessionsDropdown() {
    const sel = document.getElementById('chartSessionSelect');
    if (!sel || !selectedDeviceId) return;
    sel.innerHTML = '<option value="">Memuat Sesi...</option>';
    try {
        const res = await fetch(`/api/devices/${selectedDeviceId}/sessions`);
        const list = await res.json();
        if (!Array.isArray(list) || list.length === 0) {
            sel.innerHTML = '<option value="">(Belum Ada Sesi Record)</option>';
            sessionChartData = [];
            sessionMeta = null;
            if (realtimeChart) {
                realtimeChart.data.labels = [];
                realtimeChart.data.datasets = [];
                realtimeChart.update('none');
            }
            return;
        }

        sel.innerHTML = list.map(s => {
            const cnt = s.recordCount || 0;
            const sName = s.name || s.id;
            const sTime = s.startTime || '';
            return `<option value="${s.id}">${sName} (${cnt} recs - ${sTime})</option>`;
        }).join('');

        const firstId = list[0].id;
        sel.value = firstId;
        await onChartSessionChange(firstId);
    } catch (e) {
        console.error("Error loading chart sessions dropdown:", e);
        sel.innerHTML = '<option value="">Gagal Memuat Sesi</option>';
    }
}

async function onChartSessionChange(sessionId, page = 'last') {
    if (!sessionId) return;
    _showChartSpinner();
    try {
        const res = await fetch(`/api/history/session-chart-data/${sessionId}?page_size=3000&page=${page}`);
        const json = await res.json();
        sessionMeta = json.meta || {};
        sessionChartData = json.data || [];

        if (timeFilter === 'session' && realtimeChart) {
            const { labels, datasets } = getAllPhaseDatasets();
            realtimeChart.data.labels = labels;
            realtimeChart.data.datasets = datasets;
            const { yMin, yMax } = getYBoundsMulti(datasets, selectedParameter);
            realtimeChart.options.scales.y.min = yMin;
            realtimeChart.options.scales.y.max = yMax;
            realtimeChart.update('none');

            const totalSlots = sessionMeta.totalTimeSlots || sessionChartData.length;
            const totalPages = sessionMeta.totalPages || 1;
            const currentPage = sessionMeta.page || page;
            const pageSize = sessionMeta.pageSize || 3000;
            const startNum = (currentPage - 1) * pageSize + 1;
            const endNum = Math.min(currentPage * pageSize, totalSlots);

            _renderBatchNav('dashChartBatchNav', currentPage, totalPages, startNum, endNum, totalSlots, (newPage) => {
                onChartSessionChange(sessionId, newPage);
            });
        }
    } catch (e) {
        console.error("Error loading session chart data:", e);
    } finally {
        _fadeChartIn();
    }
}
function _startAggRebuild() {
    if (_aggRebuildId) { clearInterval(_aggRebuildId); _aggRebuildId = null; }
    if (timeFilter === 'day') {
        setTimeout(() => fetchChartDataFromServer(), 150);
        _aggRebuildId = setInterval(() => {
            fetchChartDataFromServer();
        }, 30_000);
        return;
    }
    if (timeFilter === 'week') {
        setTimeout(() => fetchChartDataFromServer(), 150);
        _aggRebuildId = setInterval(() => {
            fetchChartDataFromServer();
        }, 300_000);
    }
}
function _checkTimeWindowChange() {
    const now = new Date();
    const m = now.getMinutes();
    const h = now.getHours();
    const day = now.getDate();
    if (timeFilter === 'day') {
        if (_lastChartDay !== -1 && day !== _lastChartDay) {
            hourlyHistoryData = {};
            _rebuildChart();
        }
        else if (_lastChartMinute !== -1 &&
            Math.floor(m / 5) !== Math.floor(_lastChartMinute / 5)) {
            fetchChartDataFromServer();
        }
    } else if (timeFilter === 'week') {
        if (_lastChartDay !== -1 && day !== _lastChartDay) {
            fetchChartDataFromServer();
        }
    }
    _lastChartMinute = m;
    _lastChartHour = h;
    _lastChartDay = day;
}
function startTimeWindowMonitoring() {
    if (_timeWindowCheckId) clearInterval(_timeWindowCheckId);
    const now = new Date();
    _lastChartMinute = now.getMinutes();
    _lastChartHour = now.getHours();
    _lastChartDay = now.getDate();
    _startAggRebuild();
    _timeWindowCheckId = setInterval(_checkTimeWindowChange, 5_000);
}

function _renderBatchNav(containerId, page, totalPages, startNum, endNum, totalNum, onPageChangeFn) {
    const el = document.getElementById(containerId);
    if (!el) return;
    if (!totalNum || totalNum === 0) {
        el.style.display = 'none';
        el.innerHTML = '';
        return;
    }
    el.style.display = 'flex';
    el.className = 'batch-nav-container';
    el.innerHTML = `
        <button class="batch-nav-btn" ${page <= 1 ? 'disabled' : ''} id="${containerId}_prev">
            &#9668; Prev Batch
        </button>
        <div class="batch-nav-info">
            Halaman <strong>${page}</strong> dari <strong>${totalPages}</strong> &middot; (Data <strong>${startNum.toLocaleString('id-ID')}</strong> - <strong>${endNum.toLocaleString('id-ID')}</strong> dari <strong>${totalNum.toLocaleString('id-ID')}</strong>)
        </div>
        <button class="batch-nav-btn" ${page >= totalPages ? 'disabled' : ''} id="${containerId}_next">
            Next Batch &#9658;
        </button>
    `;
    const prevBtn = document.getElementById(`${containerId}_prev`);
    const nextBtn = document.getElementById(`${containerId}_next`);
    if (prevBtn) prevBtn.onclick = (e) => { e.stopPropagation(); onPageChangeFn(page - 1); };
    if (nextBtn) nextBtn.onclick = (e) => { e.stopPropagation(); onPageChangeFn(page + 1); };
}

function parseTimestampToDate(ts) {
    if (!ts) return new Date();
    if (ts instanceof Date) return ts;
    if (typeof ts === 'number') return new Date(ts);
    if (/^\d+$/.test(ts)) return new Date(parseInt(ts));

    const str = String(ts).trim();

    // 1. Format: DD/MM/YYYY HH:mm:ss or DD/MM/YYYY or DD-MM-YYYY
    let m = str.match(/^(\d{1,2})[/.-](\d{1,2})[/.-](\d{4})(?:\s+(\d{1,2}):(\d{1,2})(?::(\d{1,2}))?)?/);
    if (m) {
        const day = parseInt(m[1], 10);
        const month = parseInt(m[2], 10) - 1;
        const year = parseInt(m[3], 10);
        const hh = parseInt(m[4] || 0, 10);
        const mm = parseInt(m[5] || 0, 10);
        const ss = parseInt(m[6] || 0, 10);
        return new Date(year, month, day, hh, mm, ss);
    }

    // 2. Format: HH:mm:ss DD/MM/YYYY or HH:mm:ss DD-MM-YYYY
    m = str.match(/^(\d{1,2}):(\d{1,2})(?::(\d{1,2}))?\s+(\d{1,2})[/.-](\d{1,2})[/.-](\d{4})/);
    if (m) {
        const hh = parseInt(m[1], 10);
        const mm = parseInt(m[2], 10);
        const ss = parseInt(m[3] || 0, 10);
        const day = parseInt(m[4], 10);
        const month = parseInt(m[5], 10) - 1;
        const year = parseInt(m[6], 10);
        return new Date(year, month, day, hh, mm, ss);
    }

    // 3. Format: YYYY-MM-DD HH:mm:ss
    m = str.match(/^(\d{4})[/.-](\d{1,2})[/.-](\d{1,2})(?:[T\s]+(\d{1,2}):(\d{1,2})(?::(\d{1,2}))?)?/);
    if (m) {
        const year = parseInt(m[1], 10);
        const month = parseInt(m[2], 10) - 1;
        const day = parseInt(m[3], 10);
        const hh = parseInt(m[4] || 0, 10);
        const mm = parseInt(m[5] || 0, 10);
        const ss = parseInt(m[6] || 0, 10);
        return new Date(year, month, day, hh, mm, ss);
    }

    const fallback = new Date(str);
    return isNaN(fallback.getTime()) ? new Date() : fallback;
}

function _formatTimestampForChart(ts) {
    if (!ts) return '';
    try {
        const d = parseTimestampToDate(ts);
        if (isNaN(d.getTime())) return String(ts);
        const _today = new Date();
        const _isToday = d.getFullYear() === _today.getFullYear() &&
            d.getMonth() === _today.getMonth() &&
            d.getDate() === _today.getDate();
        const _MON = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
        const _p = v => String(v).padStart(2, '0');
        const timeStr = `${_p(d.getHours())}:${_p(d.getMinutes())}:${_p(d.getSeconds())}`;
        return _isToday ? timeStr : `${d.getDate()} ${_MON[d.getMonth()]}, ${timeStr}`;
    } catch (_) {
        return String(ts);
    }
}

function getAllPhaseDatasets() {
    const enabledKeys = _getEnabledPhaseKeys();
    const allPhases = enabledKeys.slice().sort((a, b) => parseInt(a.slice(1)) - parseInt(b.slice(1)));
    if (!allPhases.length) return { labels: [], datasets: [] };

    if (timeFilter === 'session') {
        if (!sessionChartData || !sessionChartData.length) {
            return { labels: [], datasets: [] };
        }
        const labels = sessionChartData.map(item => _formatTimestampForChart(item.timestamp));
        const fieldMap = { voltage: 'Voltage', current: 'Current', power: 'Power', frequency: 'Frequency', energy: 'Energy', powerFactor: 'PowerFactor' };
        const field = fieldMap[selectedParameter] || 'Voltage';

        const sessionPhases = (sessionMeta && sessionMeta.phases && sessionMeta.phases.length > 0)
            ? sessionMeta.phases
            : allPhases;

        const datasets = sessionPhases.map(phase => {
            const colors = getPhaseColors(phase);
            const phaseCustomName = (sessionMeta && sessionMeta.phaseNames && sessionMeta.phaseNames[phase])
                ? sessionMeta.phaseNames[phase]
                : getPhaseLabel(phase);
            const values = sessionChartData.map(item => {
                const pd = item.data?.[phase];
                if (!pd || pd.offline) return null;
                return pd[field] != null ? parseFloat(pd[field]) : null;
            });
            return {
                label: (phaseCustomName && phaseCustomName !== phase) ? `${phase} (${phaseCustomName})` : phase,
                data: values,
                borderColor: colors.line,
                backgroundColor: colors.light,
                borderWidth: 2,
                tension: 0.2,
                spanGaps: true,
                fill: false,
                pointRadius: sessionChartData.length > 150 ? 0 : 2,
                pointHoverRadius: 5,
                hidden: _hiddenPhases.has(phase),
                type: 'line',
                _phaseKey: phase,
            };
        });
        return { labels, datasets };
    }

    let labels;
    let getValues;

    if (timeFilter === 'week') {
        // WEEK mode: canonical 7-day date array
        const targetDateObj = chartTargetDate ? new Date(chartTargetDate + 'T00:00:00') : new Date();
        const canonicalDates = [];
        for (let i = 6; i >= 0; i--) {
            const d = new Date(targetDateObj);
            d.setDate(d.getDate() - i);
            canonicalDates.push(`${d.getFullYear()}-${_p2(d.getMonth() + 1)}-${_p2(d.getDate())}`);
        }
        const _MON2 = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
        labels = canonicalDates.map(dk => { const [, m, day] = dk.split('-'); return `${parseInt(day)} ${_MON2[parseInt(m) - 1]}`; });
        const fieldMap = { voltage: 'Voltage', current: 'Current', power: 'Power', frequency: 'Frequency', energy: 'Energy', powerFactor: 'PowerFactor' };
        const field = fieldMap[selectedParameter] || 'Voltage';
        getValues = (phase) => {
            const phaseRec = dailyHistoryData[phase] || {};
            return canonicalDates.map(dk => {
                const rec = phaseRec[dk];
                return (rec && rec[field] != null) ? parseFloat(parseFloat(rec[field]).toFixed(4)) : null;
            });
        };
    } else {
        // DAY / ALL mode
        let maxLabels = [];
        for (const ph of allPhases) {
            const ag = getAggregatedDataForPhase(ph, selectedParameter);
            if (ag.labels && ag.labels.length > maxLabels.length) maxLabels = ag.labels;
        }
        labels = maxLabels;
        getValues = (phase) => {
            let { values } = getAggregatedDataForPhase(phase, selectedParameter);
            if (!values || values.length === 0) return Array(labels.length).fill(0);
            if (values.length < labels.length) {
                const firstValid = values.find(v => v != null && v > 0) ?? 0;
                return [...Array(labels.length - values.length).fill(firstValid), ...values];
            }
            return values;
        };
    }

    const datasets = allPhases.map(phase => {
        const colors = getPhaseColors(phase);
        const values = getValues(phase);
        const isDayBar = timeFilter === 'day';
        const isWeekBar = timeFilter === 'week';
        const isBarChart = isDayBar || isWeekBar;
        const bgFn = isBarChart
            ? colors.bar
            : (context) => {
                const ch = context.chart;
                if (!ch.chartArea) return colors.light;
                return createAreaGradient(ch.ctx, ch.chartArea, colors.light);
            };
        return {
            label: getPhaseLabel(phase),
            data: values,
            borderColor: colors.line,
            backgroundColor: bgFn,
            borderWidth: 0,
            tension: 0.38,
            cubicInterpolationMode: 'monotone',
            spanGaps: false,
            fill: !isBarChart,
            pointRadius: 0,
            pointHoverRadius: 0,
            pointBackgroundColor: colors.line,
            pointBorderColor: '#fff',
            pointBorderWidth: 0,
            borderRadius: isBarChart ? [6, 6, 0, 0] : 0,
            borderSkipped: false,
            ...(isBarChart ? {
                type: 'bar',
                barPercentage: isDayBar ? 0.6 : 0.55,
                categoryPercentage: isDayBar ? 0.75 : 0.7,
                grouped: true,
            } : {
                type: 'line',
            }),
            _phaseKey: phase,
            hidden: typeof _hiddenPhases !== 'undefined' ? _hiddenPhases.has(phase) : false,
        };
    });
    return { labels, datasets };
}
function getYBoundsMulti(datasets, param) {
    const padMap = { voltage: 3, current: 0.2, power: 10, frequency: 0.2, energy: 0.05, powerFactor: 0.02 };
    const pad = padMap[param] ?? 2;
    const allValues = datasets.filter(ds => !ds.hidden).flatMap(ds => ds.data).filter(v => v != null && isFinite(v));
    if (!allValues.length) return { yMin: 0, yMax: pad * 2 };
    // Use non-zero values for max to avoid being dominated by zero-filled history
    const nonZeroValues = allValues.filter(v => v > 0);
    const dataMax = nonZeroValues.length > 0 ? Math.max(...nonZeroValues) : Math.max(...allValues);
    const spread = dataMax;
    const actualPad = spread < pad ? pad : spread * 0.08;
    return { yMin: 0, yMax: parseFloat((dataMax + actualPad).toFixed(4)) };
}
function initChart() {
    const ctx = $('realtimeChart');
    if (!ctx) return;
    // Always destroy any existing chart to prevent orphaned Chart.js canvas contexts
    let existingChart = Chart.getChart(ctx);
    if (existingChart) {
        try { existingChart.destroy(); } catch (_) { }
    }
    if (realtimeChart) {
        try { realtimeChart.destroy(); } catch (_) { }
        realtimeChart = null;
    }
    const info = PARAM_INFO[selectedParameter];
    const isBar = timeFilter === 'week';
    const now = new Date();
    const xTitles = {
        all: '',
        day: `Hari ini — ${now.toLocaleDateString('id-ID', { weekday: 'long', day: '2-digit', month: 'long', year: 'numeric' })}`,
        week: '7 Hari Terakhir',
    };
    const unitLabel = info.unit ? `${info.label} (${info.unit})` : info.label;
    let initLabels, initDatasets;
    if (timeFilter === 'all') {
        const total = chartLabels.length;
        const visible = Math.min(_visiblePoints, total);
        const start = Math.max(0, total - visible);
        const enabledKeys = _getEnabledPhaseKeys();
        const phases = enabledKeys.slice().sort((a, b) => parseInt(a.slice(1)) - parseInt(b.slice(1)));
        initLabels = chartLabels.slice(start);
        initDatasets = phases.map(phase => {
            const colors = getPhaseColors(phase);
            let values = phaseChartData[phase]?.[selectedParameter] || [];
            if (!values.length) values = Array(total).fill(0);
            else if (values.length < total) values = [...Array(total - values.length).fill(0), ...values];
            values = values.slice(start);
            return {
                _phaseKey: phase,
                label: getPhaseLabel(phase),
                data: values,
                borderColor: colors.line,
                backgroundColor: (context) => {
                    const ch = context.chart;
                    if (!ch.chartArea) return colors.light;
                    return createAreaGradient(ch.ctx, ch.chartArea, colors.light);
                },
                borderWidth: 2.5,
                tension: 0.38,
                cubicInterpolationMode: 'monotone',
                spanGaps: false,
                fill: true,
                pointRadius: 0,
                pointHoverRadius: 0,
                hidden: typeof _hiddenPhases !== 'undefined' ? _hiddenPhases.has(phase) : false,
            };
        });
    } else {
        const built = getAllPhaseDatasets();
        initLabels = built.labels;
        initDatasets = built.datasets;
    }
    const { yMin, yMax } = getYBoundsMulti(initDatasets, selectedParameter);
    const maxTicksMap = { all: 30, day: 18, week: 7 };
    realtimeChart = new Chart(ctx, {
        type: (timeFilter === 'week' || timeFilter === 'day') ? 'bar' : 'line',
        data: { labels: initLabels, datasets: initDatasets },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: { intersect: false, mode: 'index' },
            animation: _chartEntryAnimate ? {
                duration: 800,
                easing: 'easeOutQuint',
                onComplete({ chart }) {
                    if (timeFilter === 'all') {
                        chart.options.animation = false;
                        chart.options.animations = {};
                    }
                },
            } : false,
            animations: _chartEntryAnimate ? {
                y: {
                    duration: 800,
                    easing: 'easeOutQuint',
                    from(ctx) {
                        return ctx.chart.scales?.y?.getPixelForValue(0) ?? 0;
                    },
                },
            } : {},
            layout: { padding: { top: 8, right: 16, bottom: 2, left: 4 } },
            plugins: {
                legend: {
                    display: false,
                    position: 'top',
                    align: 'start',
                    labels: {
                        font: { family: "'Outfit','Segoe UI',sans-serif", size: 11.5, weight: '600' },
                        color: '#374151',
                        usePointStyle: true,
                        pointStyle: isBar ? 'rectRounded' : 'circle',
                        pointStyleWidth: 10,
                        padding: 20,
                        boxHeight: 8,
                        generateLabels(chart) {
                            const orig = Chart.defaults.plugins.legend.labels.generateLabels(chart);
                            return orig.map(item => ({
                                ...item,
                                fillStyle: chart.data.datasets[item.datasetIndex]?.borderColor || item.fillStyle,
                                strokeStyle: 'transparent',
                            }));
                        },
                    },
                },
                tooltip: {
                    enabled: false,
                    external: iSolarTooltipHandler,
                    mode: 'index',
                    intersect: false,
                },
                zoom: {
                    zoom: {
                        wheel: { enabled: true, speed: 0.08 },
                        pinch: { enabled: true },
                        mode: 'x',
                        onZoom: ({ chart }) => {
                            _userIsZoomed = true;
                            _updateResetZoomUI();
                        },
                    },
                    pan: {
                        enabled: true,
                        mode: 'x',
                        onPan: ({ chart }) => {
                            _userIsZoomed = true;
                            _updateResetZoomUI();
                        },
                    },
                    limits: { x: { minRange: 2 } },
                },
            },
            scales: {
                x: {
                    display: true,
                    border: { display: false },
                    title: {
                        display: !!(xTitles[timeFilter]),
                        text: xTitles[timeFilter] || '',
                        font: { size: 10, weight: '600', family: "'Outfit','Segoe UI',sans-serif" },
                        color: document.documentElement.classList.contains('dark') ? '#94a3b8' : '#9CA3AF',
                        padding: { top: 6 },
                    },
                    grid: {
                        color: document.documentElement.classList.contains('dark') ? 'rgba(255, 255, 255, 0.04)' : 'rgba(226, 232, 240, 0.7)',
                        drawTicks: false,
                        lineWidth: 1,
                        borderDash: [4, 4],
                    },
                    ticks: {
                        maxRotation: 45,
                        minRotation: 45,
                        font: { size: 10.5, weight: '600', family: "'Outfit','Segoe UI',sans-serif" },
                        color: document.documentElement.classList.contains('dark') ? '#94a3b8' : '#64748B',
                        maxTicksLimit: maxTicksMap[timeFilter] ?? 10,
                        padding: 8,
                        autoSkip: true,
                        autoSkipPadding: timeFilter === 'day' ? 6 : 8,
                    },
                    offset: (timeFilter === 'week' || timeFilter === 'day'),
                },
                y: {
                    display: true,
                    position: 'left',
                    border: { display: false },
                    title: {
                        display: true,
                        text: unitLabel,
                        font: { size: 10, weight: '600', family: "'Outfit','Segoe UI',sans-serif" },
                        color: document.documentElement.classList.contains('dark') ? '#94a3b8' : '#9CA3AF',
                        padding: { bottom: 8 },
                    },
                    grid: {
                        color: document.documentElement.classList.contains('dark') ? 'rgba(255, 255, 255, 0.04)' : 'rgba(226, 232, 240, 0.7)',
                        drawTicks: false,
                        lineWidth: 1,
                        borderDash: [4, 4],
                    },
                    ticks: {
                        font: { size: 10.5, weight: '600', family: "'Outfit','Segoe UI',sans-serif" },
                        color: document.documentElement.classList.contains('dark') ? '#94a3b8' : '#64748B',
                        padding: 12,
                        maxTicksLimit: 6,
                        callback: v => {
                            if (v == null) return '';
                            if (Math.abs(v) >= 1000) return (v / 1000).toFixed(1) + 'k';
                            return parseFloat(v.toFixed(3)).toString();
                        },
                    },
                    min: yMin,
                    max: yMax,
                },
            },
        },
    });
    _initChartGestures(realtimeChart);
    if (_clipPathCleanupId) {
        clearTimeout(_clipPathCleanupId);
        _clipPathCleanupId = null;
    }
    if (!isBar && _chartEntryAnimate) {
        _chartEntryAnimate = false;
        const container = ctx.parentElement;
        container.style.transition = 'none';
        container.style.clipPath = '';
        void container.offsetWidth;
        container.style.clipPath = 'inset(0 100% 0 0)';
        void container.offsetWidth;
        container.style.transition = 'clip-path 0.9s cubic-bezier(0.4, 0, 0.2, 1)';
        container.style.clipPath = 'inset(0 0% 0 0)';
        _clipPathCleanupId = setTimeout(() => {
            container.style.transition = '';
            container.style.clipPath = '';
            _clipPathCleanupId = null;
        }, 950);
    }
    ctx.addEventListener('mouseleave', () => {
        if (!_ttDrag.active && !_ttDrag.pinned) hideIscTooltip();
    });
    _initChartGestures(realtimeChart);
}
const CHART_INTERVAL_MS = 5000;
let _chartTimer = null;
function _chartZeroPoint() {
    return {
        'Voltage (V)': 0, 'Current (A)': 0, 'Power (W)': 0,
        'Frequency (Hz)': 0, 'Active Energy (kWh)': 0, 'Power Factor': 0,
        'Apparent Power (kVA)': 0, 'Reactive Power (kVAR)': 0,
        'Sensor Angle (°)': 0, 'Apparent Energy (kVAh)': 0, 'Reactive Energy (kVARh)': 0,
    };
}
function _chartPush() {
    const ts = Date.now();
    const online = isConnected && !!rawRealtimeData;
    // Kumpulkan semua phase dari live data + last known cache
    const livePhasesSet = new Set(online ? _detectPhaseKeys(rawRealtimeData) : []);
    const cachedPhasesSet = new Set(Object.keys(_lastKnownPhaseData).filter(k => /^L\d+$/.test(k)));
    const enabledPhases = new Set(_getEnabledPhaseKeys());
    const allPhases = new Set([...livePhasesSet, ...cachedPhasesSet, ...enabledPhases]);
    let phases = [...allPhases].sort((a, b) => parseInt(a.slice(1)) - parseInt(b.slice(1)));
    if (!phases.length) phases = ['L1'];
    const point = { ts };
    phases.forEach(ph => {
        if (online && rawRealtimeData[ph]) {
            // Data segar dari live: update cache dan gunakan
            _lastKnownPhaseData[ph] = rawRealtimeData[ph];
            point[ph] = rawRealtimeData[ph];
        } else if (_lastKnownPhaseData[ph]) {
            // Data stale (ESP32 tidak kirim karena tidak ada perubahan): pakai cache terakhir
            // Ini mencegah grafik anjlok ke 0 saat nilai listrik stabil
            point[ph] = _lastKnownPhaseData[ph];
        } else {
            // Belum pernah ada data sama sekali: isi 0 sebagai fallback awal
            point[ph] = _chartZeroPoint();
        }
    });
    _appendChartPoint(point);
    const raw = _rebuildRawFromPoint(point);
    if (raw) accumulatePoint(raw);
    _rafDirty = true;
    if (_pageVisible && timeFilter === 'all') _scheduleRender();
}
// ── Chart localStorage cache ────────────────────────────────────────────────
const _CHART_CACHE_VERSION = 2;
const _CHART_CACHE_MAX_MS = 60 * 60 * 1000; // 1 jam
function _chartCacheKey(deviceId) { return `chart_cache_v${_CHART_CACHE_VERSION}_${deviceId}`; }
function _saveChartCache() {
    if (!selectedDeviceId || !chartLabels.length) return;
    try {
        const payload = JSON.stringify({
            savedAt: Date.now(),
            chartLabels,
            chartTimestamps,
            phaseChartData,
        });
        localStorage.setItem(_chartCacheKey(selectedDeviceId), payload);
    } catch (_) { /* quota exceeded – ignore */ }
}
function _loadChartCache(deviceId) {
    try {
        const raw = localStorage.getItem(_chartCacheKey(deviceId));
        if (!raw) return null;
        const cache = JSON.parse(raw);
        if (!cache || !Array.isArray(cache.chartLabels) || !cache.chartLabels.length) return null;
        if (Date.now() - cache.savedAt > _CHART_CACHE_MAX_MS) {
            localStorage.removeItem(_chartCacheKey(deviceId));
            return null;
        }
        return cache;
    } catch (_) { return null; }
}
// ────────────────────────────────────────────────────────────────────────────

let _chartInitSeq = 0; // sequence number untuk guard race condition

async function _chartInit(deviceId) {
    const seq = ++_chartInitSeq; // setiap panggilan dapat nomor unik
    if (_chartTimer) { clearInterval(_chartTimer); _chartTimer = null; }
    if (_rafId) { cancelAnimationFrame(_rafId); _rafId = null; }
    // Reset cache phase saat ganti device agar tidak ada sisa data device sebelumnya
    _lastKnownPhaseData = {};
    _phaseLastSeen = {};

    const canvas = document.getElementById('realtimeChart');
    if (canvas) {
        canvas.style.transition = 'none';
        canvas.style.opacity = '0';
        _showChartSpinner();
        void canvas.offsetWidth;
        canvas.style.transition = 'opacity 0.25s ease';
    }

    resetChartData();
    const now = Date.now();
    let phases = _getEnabledPhaseKeys();
    if (!phases.length) {
        const dev = _deviceListCache.find(d => d.id === deviceId);
        phases = (dev?.phases || []).filter(p => p.enabled !== false).map(p => p.phase);
    }
    if (!phases.length) phases = ['L1'];

    // ── Restore dari localStorage (data sebelum refresh / server restart) ──
    const cache = _loadChartCache(deviceId);
    if (cache) {
        chartLabels = [...cache.chartLabels];
        chartTimestamps = [...cache.chartTimestamps];
        // deep-copy tiap phase
        Object.keys(cache.phaseChartData).forEach(ph => {
            phaseChartData[ph] = {};
            Object.keys(cache.phaseChartData[ph]).forEach(param => {
                phaseChartData[ph][param] = [...cache.phaseChartData[ph][param]];
            });
        });
        // tambahkan phase dari cache ke daftar phase aktif
        Object.keys(cache.phaseChartData).forEach(ph => {
            if (/^L\d+$/.test(ph) && !phases.includes(ph)) phases.push(ph);
        });
        phases.sort((a, b) => parseInt(a.slice(1)) - parseInt(b.slice(1)));
    }
    // ────────────────────────────────────────────────────────────────────────

    // Timestamp terakhir yang sudah ada di cache (hindari duplikat)
    const cachedLastTs = chartTimestamps.length ? chartTimestamps[chartTimestamps.length - 1] : 0;

    try {
        // Fetch kedua sumber secara paralel
        const [liveRes, histRes] = await Promise.all([
            fetch(`/api/live-buffer/${deviceId}`),
            fetch(`/api/history-buffer/${deviceId}?hours=6`),
        ]);
        const [dataList, histList] = await Promise.all([liveRes.json(), histRes.json()]);

        // Abort: ada _chartInit lebih baru yang sudah jalan saat kita fetch
        if (seq !== _chartInitSeq) return;

        // ── Pre-populate rawRealtimeData from latest live-buffer data ──
        if (Array.isArray(dataList) && dataList.length > 0) {
            const latestItem = dataList[dataList.length - 1];
            if (latestItem.data && latestItem.data.offline) {
                isConnected = false;
                updateDisplayCardsBlank('offline');
            } else {
                const lastValidItem = dataList.slice().reverse().find(item => item.data && !item.data.offline);
                if (lastValidItem) {
                    rawRealtimeData = JSON.parse(JSON.stringify(lastValidItem.data));
                    isConnected = true;
                    lastDataTimestamp = lastValidItem.timestamp;
                    const normalized = normalizeHistoryData(rawRealtimeData);
                    if (normalized) {
                        if (selectedPhase) {
                            const phaseData = getPhaseDisplayData(rawRealtimeData, selectedPhase);
                            if (phaseData) updateDisplayCards(phaseData);
                        } else {
                            updateDisplayCards(normalized);
                        }
                    }
                }
            }

            // ── Bangun _phaseLastSeen & _lastKnownPhaseData dari live-buffer ──
            // Iterasi mundur agar phase yang lebih baru menimpa yang lama
            for (let i = dataList.length - 1; i >= 0; i--) {
                const item = dataList[i];
                if (!item?.data || item.data.offline) continue;
                const ts = item.timestamp;
                Object.keys(item.data).forEach(ph => {
                    if (!/^L\d+$/.test(ph)) return;
                    // Update _phaseLastSeen dengan timestamp terbaru yang ada data untuk phase ini
                    if (!_phaseLastSeen[ph] || ts > _phaseLastSeen[ph]) {
                        _phaseLastSeen[ph] = ts;
                    }
                    // Update _lastKnownPhaseData sekali saja (dari item terbaru)
                    if (!_lastKnownPhaseData[ph] && item.data[ph]) {
                        _lastKnownPhaseData[ph] = item.data[ph];
                    }
                });
            }
        }

        // ── Build live-data map ──
        const liveData = {};
        if (Array.isArray(dataList)) {
            dataList.forEach(item => { if (item?.timestamp) liveData[item.timestamp] = item.data; });
        }
        const liveKeys = Object.keys(liveData).sort((a, b) => +a - +b);

        // ── Build history-data map (5-menit dari HourlyCapture) ──
        const histData = {};
        if (Array.isArray(histList)) {
            histList.forEach(item => { if (item?.timestamp) histData[item.timestamp] = item.data; });
        }
        const histKeys = Object.keys(histData).sort((a, b) => +a - +b);

        // Kumpulkan semua phase dari kedua sumber
        [...liveKeys, ...histKeys].forEach(k => {
            const d = liveData[k] || histData[k];
            if (d && typeof d === 'object') {
                Object.keys(d).forEach(ph => {
                    if (/^L\d+$/.test(ph) && !phases.includes(ph)) phases.push(ph);
                });
            }
        });
        phases.sort((a, b) => parseInt(a.slice(1)) - parseInt(b.slice(1)));

        // Waktu pertama live-buffer tersedia
        const liveStart = liveKeys.length ? +liveKeys[0] : now;

        // ── Phase 1: tambahkan titik history (5-menit) sebelum live-buffer ──
        histKeys.forEach(k => {
            const ts = +k;
            if (ts >= liveStart) return; // live-buffer lebih akurat untuk range ini
            if (ts <= cachedLastTs) return; // sudah ada di cache
            const d = histData[k];
            const point = { ts };
            phases.forEach(ph => { point[ph] = d?.[ph] || _chartZeroPoint(); });
            _appendChartPoint(point);
        });

        // ── Phase 2: tambahkan titik live-buffer (5-detik) ──
        const interval = typeof CHART_INTERVAL_MS !== 'undefined' ? CHART_INTERVAL_MS : 5000;
        const liveWindowStart = Math.max(
            liveStart,
            cachedLastTs > 0 ? cachedLastTs + interval : 0
        );
        let kIdx = 0;
        for (let ts = liveWindowStart; ts <= now; ts += interval) {
            const point = { ts };
            while (kIdx < liveKeys.length - 1 && +liveKeys[kIdx + 1] <= ts) kIdx++;
            const curK = liveKeys[kIdx];
            const rawAtCur = (curK && +curK <= ts) ? liveData[curK] : null;
            phases.forEach(ph => { point[ph] = rawAtCur?.[ph] || _chartZeroPoint(); });
            _appendChartPoint(point);
        }
    } catch (e) {
        console.error("Error in _chartInit:", e);
        if (seq !== _chartInitSeq) return;
        if (!cache) {
            const interval = typeof CHART_INTERVAL_MS !== 'undefined' ? CHART_INTERVAL_MS : 5000;
            for (let ts = now - _visiblePoints * interval; ts <= now; ts += interval) {
                const point = { ts };
                phases.forEach(ph => { point[ph] = _chartZeroPoint(); });
                _appendChartPoint(point);
            }
        }
    }

    // Buang data yang lebih tua dari MAX_DATA_POINTS setelah merge
    while (chartLabels.length > MAX_DATA_POINTS) {
        chartLabels.shift();
        chartTimestamps.shift();
        Object.keys(phaseChartData).forEach(ph => {
            PARAM_KEYS.forEach(k => { phaseChartData[ph]?.[k]?.shift(); });
        });
    }

    // Abort jika ada _chartInit lebih baru yang sudah jalan
    if (seq !== _chartInitSeq) return;

    rebuildCascadeFromRaw();
    _rebuildChart();
    _fadeChartIn();
    _chartTimer = setInterval(() => { if (selectedDeviceId === deviceId) _chartPush(); }, typeof CHART_INTERVAL_MS !== 'undefined' ? CHART_INTERVAL_MS : 5000);
}
function _rebuildRawFromPoint(point) {
    if (!point) return null;
    const phases = Object.keys(point).filter(k => /^L\d+$/.test(k));
    if (!phases.length) return null;
    const raw = {};
    phases.forEach(ph => { raw[ph] = point[ph]; });
    return raw;
}
function _appendChartPoint(point) {
    if (!point || !point.ts) return;
    const ts = point.ts;
    const phases = Object.keys(point).filter(k => /^L\d+$/.test(k));
    if (!phases.length) return;
    const _d = new Date(ts);
    const _today = new Date();
    const _isToday = _d.getFullYear() === _today.getFullYear() &&
        _d.getMonth() === _today.getMonth() &&
        _d.getDate() === _today.getDate();
    const _MON = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
    const label = _isToday
        ? _d.toLocaleTimeString('id-ID', { hour: '2-digit', minute: '2-digit', second: '2-digit' })
        : `${_MON[_d.getMonth()]} ${_d.getDate()}, ${String(_d.getHours()).padStart(2, '0')}:${String(_d.getMinutes()).padStart(2, '0')}`;
    chartLabels.push(label);
    chartTimestamps.push(ts);
    const SHORT_MAP = {
        'Voltage (V)': 'V', 'Current (A)': 'A', 'Power (W)': 'W',
        'Frequency (Hz)': 'Hz', 'Active Energy (kWh)': 'kWh', 'Power Factor': 'pf',
    };
    const fv = (pd, k) => {
        if (!pd) return 0;
        const val = pd[k] ?? pd[SHORT_MAP[k]];
        if (val == null) return 0;
        try { return parseFloat(val) || 0; } catch (_) { return 0; }
    };
    phases.forEach(phase => {
        if (!phaseChartData[phase]) {
            phaseChartData[phase] = Object.fromEntries(PARAM_KEYS.map(k => [k, []]));
        }
        const pd = point[phase] || {};
        const vV  = fv(pd, 'Voltage (V)');
        const vA  = fv(pd, 'Current (A)');
        const vW  = fv(pd, 'Power (W)');
        const vHz = fv(pd, 'Frequency (Hz)');
        const vE  = fv(pd, 'Active Energy (kWh)');
        const vPF = fv(pd, 'Power Factor');

        const currentArr = phaseChartData[phase].current;
        const targetLen = chartLabels.length - 1;
        if (currentArr.length < targetLen) {
            const padCount = targetLen - currentArr.length;
            const fillV  = vV  > 0 ? vV  : 0;
            const fillA  = vA  > 0 ? vA  : 0;
            const fillW  = vW  > 0 ? vW  : 0;
            const fillHz = vHz > 0 ? vHz : 0;
            const fillE  = vE  > 0 ? vE  : 0;
            const fillPF = vPF > 0 ? vPF : 0;

            for (let i = 0; i < padCount; i++) {
                phaseChartData[phase].voltage.push(fillV);
                phaseChartData[phase].current.push(fillA);
                phaseChartData[phase].power.push(fillW);
                phaseChartData[phase].frequency.push(fillHz);
                phaseChartData[phase].energy.push(fillE);
                phaseChartData[phase].powerFactor.push(fillPF);
            }
        }

        phaseChartData[phase].voltage.push(vV);
        phaseChartData[phase].current.push(vA);
        phaseChartData[phase].power.push(vW);
        phaseChartData[phase].frequency.push(vHz);
        phaseChartData[phase].energy.push(vE);
        phaseChartData[phase].powerFactor.push(vPF);
    });
    if (chartLabels.length > MAX_DATA_POINTS) {
        chartLabels.shift();
        chartTimestamps.shift();
        phases.forEach(ph => {
            PARAM_KEYS.forEach(k => { phaseChartData[ph]?.[k]?.shift(); });
        });
    }
}
function updateChart(raw) { }
function changeParameter() { _switchParameter(DOM.paramSelect?.value); }
function _switchParameter(param) {
    if (!param) return;
    selectedParameter = param;
    _userIsZoomed = false;
    _visiblePoints = timeFilter === 'day' ? 24 : (timeFilter === 'week' ? 7 : 600);
    if (DOM.paramSelect) DOM.paramSelect.value = param;
    document.querySelectorAll('.metric-card-compact').forEach(card => {
        card.classList.toggle('card-active', card.dataset.param === param);
    });
    _rebuildChart(false);
}
function _morphChartStructure(animate = true) {
    if (!realtimeChart) { initChart(); return; }
    const isBar = timeFilter === 'week' || timeFilter === 'day';
    const targetType = isBar ? 'bar' : 'line';
    if (realtimeChart.config.type !== targetType) {
        initChart();
        return;
    }
    realtimeChart.config.type = targetType;
    if (timeFilter === 'all') {
        const total = chartLabels.length;
        const visible = Math.min(_visiblePoints, total);
        const start = Math.max(0, total - visible);
        const enabledKeys = _getEnabledPhaseKeys();
        const phases = enabledKeys.slice().sort((a, b) => parseInt(a.slice(1)) - parseInt(b.slice(1)));
        realtimeChart.data.labels = chartLabels.slice(start);
        realtimeChart.data.datasets = phases.map(phase => {
            const colors = getPhaseColors(phase);
            let values = phaseChartData[phase]?.[selectedParameter] || [];
            if (!values.length) values = Array(total).fill(0);
            else if (values.length < total) values = [...Array(total - values.length).fill(0), ...values];
            values = values.slice(start);
            return {
                _phaseKey: phase,
                label: getPhaseLabel(phase),
                data: values,
                borderColor: colors.line,
                backgroundColor: (context) => {
                    const ch = context.chart;
                    if (!ch.chartArea) return colors.light;
                    return createAreaGradient(ch.ctx, ch.chartArea, colors.light);
                },
                borderWidth: 2.5,
                tension: 0.38,
                cubicInterpolationMode: 'monotone',
                spanGaps: false,
                fill: true,
                pointRadius: 0,
                pointHoverRadius: 0,
                hidden: typeof _hiddenPhases !== 'undefined' ? _hiddenPhases.has(phase) : false,
            };
        });
    } else {
        const built = getAllPhaseDatasets();
        realtimeChart.data.labels = built.labels;
        realtimeChart.data.datasets = built.datasets;
    }
    const { yMin, yMax } = getYBoundsMulti(realtimeChart.data.datasets, selectedParameter);
    realtimeChart.options.scales.y.min = yMin;
    realtimeChart.options.scales.y.max = yMax;

    // Update Y-axis title
    const info = PARAM_INFO[selectedParameter];
    if (info && realtimeChart.options.scales.y.title) {
        realtimeChart.options.scales.y.title.text = info.unit ? `${info.label} (${info.unit})` : info.label;
    }

    // Ensure grouping/stacking is correctly applied during transition
    if (realtimeChart.options.scales.x) {
        realtimeChart.options.scales.x.stacked = false;
        realtimeChart.options.scales.x.offset = isBar;
    }
    if (realtimeChart.options.scales.y) {
        realtimeChart.options.scales.y.stacked = false;
    }
    if (animate) {
        realtimeChart.options.animation = { duration: 750, easing: 'easeOutQuart' };
    } else {
        realtimeChart.options.animation = false;
    }
    realtimeChart.update();
    if (animate && timeFilter === 'all') {
        setTimeout(() => {
            if (realtimeChart) realtimeChart.options.animation = false;
        }, 800);
    }
}
function switchTab(tabName) {
    document.querySelectorAll('.tab-btn').forEach(btn => btn.classList.remove('active'));
    document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
    $(`${tabName}Tab`)?.classList.add('active');
    $(`${tabName}Content`)?.classList.add('active');
    if (tabName === 'history') loadDevices().then(() => buildSessionUI());
    if (tabName === 'tools' && typeof activeOfflineChart !== 'undefined' && activeOfflineChart) {
        setTimeout(() => activeOfflineChart.resize(), 50);
    }
}
async function loadDevices() {
    try {
        const devices = await fetch('/api/devices').then(r => r.json());
        devices.forEach(d => {
            const cached = _deviceListCache.find(c => c.id === d.id);
            if (cached?.phases) {
                d.phases.forEach(p => {
                    const cp = cached.phases.find(c => c.phase === p.phase);
                    if (cp !== undefined) p.enabled = cp.enabled !== false;
                });
            }
        });
        _deviceListCache = devices;
        const visible = devices;
        
        _populateDeviceSelect(visible);
        if (!visible.length) {
            selectedDeviceId = '';
            selectedDeviceName = '';
            renderDeviceList([]);
            updatePhaseSelector([]);
            updateConnectionStatus(false);
            updateDisplayCardsBlank();
            return;
        }

        const initialLoad = !selectedDeviceId;
        if (initialLoad) {
            selectedDeviceId = visible[0].id;
            selectedDeviceName = visible[0].name && visible[0].name !== visible[0].id ? `${visible[0].id} ${visible[0].name}` : visible[0].id;
        }
        const activeDev = visible.find(d => d.id === selectedDeviceId);
        if (!initialLoad && activeDev) {
            selectedDeviceName = activeDev.name && activeDev.name !== activeDev.id ? `${activeDev.id} ${activeDev.name}` : activeDev.id;
        }

        if (initialLoad && activeDev) {
            connectionStartTime = Date.now();
            _attachRealtimeListener(selectedDeviceId);
            _attachHistoryListener(selectedDeviceId);
            _attachHourlyListener(selectedDeviceId);
            _attachDayListener(selectedDeviceId);
        }

        if (activeDev) {
            const isEditing = Array.from(document.querySelectorAll('.device-edit-mode, .device-phase-edit'))
                .some(el => el.style.display === 'flex' || el.style.display === 'block');
            if (!isEditing) {
                renderDeviceList([activeDev]);
                if (activeDev.phases?.length) {
                    const enabledPhases = activeDev.phases.filter(p => p.enabled !== false).map(p => p.phase);
                    updatePhaseSelector(enabledPhases);
                }
            }
        } else {
            renderDeviceList([]);
        }
    } catch (e) { }
}
function _populateDeviceSelect(devices) {
    const selects = [DOM.deviceSelect, DOM.summaryDeviceSelect].filter(Boolean);
    if (!selects.length) return;
    const currentVal = DOM.deviceSelect?.value || DOM.summaryDeviceSelect?.value || selectedDeviceId;
    const html = devices.map(d => {
        const displayName = d.name && d.name !== d.id ? `${d.id} ${d.name}` : d.id;
        return `<option value="${d.id}"${d.id === currentVal ? ' selected' : ''}>${displayName}</option>`;
    }).join('');
    selects.forEach(sel => sel.innerHTML = html);
}
function renderDeviceList(devices) {
    const container = DOM.deviceList;
    if (!container) return;
    if (!devices.length) {
        container.innerHTML = '<p style="color:var(--text-tertiary);font-size:12px;padding:8px 0">Belum ada device terdaftar</p>';
        return;
    }
    const editSVG = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"/><path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"/></svg>`;
    const deleteSVG = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="3 6 5 6 21 6"/><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/><line x1="10" y1="11" x2="10" y2="17"/><line x1="14" y1="11" x2="14" y2="17"/></svg>`;
    const checkSVG = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3"><polyline points="20 6 9 17 4 12"/></svg>`;
    const closeSVG = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>`;
    container.innerHTML = devices.map(d => {
        const dotClass = d.online ? 'online' : 'offline';
        const phasesHTML = (d.phases && d.phases.length > 0)
            ? d.phases.map(p => {
                const isEnabled = p.enabled !== false;
                const phaseColor = getPhaseColors(p.phase).line;
                const phaseSeen = _phaseLastSeen[p.phase];
                const isLive = phaseSeen && (Date.now() - phaseSeen <= 45000);
                const statusHTML = !isEnabled 
                    ? '<span class="sensor-status-badge disabled"><span class="status-dot-pulse"></span> Nonaktif</span>'
                    : (isLive 
                        ? '<span class="sensor-status-badge live"><span class="status-dot-pulse"></span> Terkoneksi</span>' 
                        : '<span class="sensor-status-badge idle"><span class="status-dot-pulse"></span> Terputus</span>');
                return `
            <div class="device-phase-item${isEnabled ? '' : ' phase-disabled'}" id="phase-item_${d.id}_${p.phase}">
                <div class="device-phase-view" id="phase-view_${d.id}_${p.phase}">
                    <label class="phase-toggle-wrap" title="${isEnabled ? 'Nonaktifkan' : 'Aktifkan'} sensor ini" onclick="event.stopPropagation()">
                        <input type="checkbox" class="phase-toggle-cb" ${isEnabled ? 'checked' : ''}
                            onchange="togglePhaseEnabled('${d.id}','${p.phase}',this.checked)">
                        <span class="phase-toggle-track"></span>
                    </label>
                    <div class="device-phase-badge" style="background:${phaseColor};${isEnabled ? '' : 'opacity:.4'}">${p.phase}</div>
                    <div class="device-phase-info">
                        <p class="device-phase-name" id="phase-label_${d.id}_${p.phase}" style="${isEnabled ? '' : 'opacity:.45;text-decoration:line-through'}">${p.name || p.phase}</p>
                        <p class="device-phase-status">${statusHTML}</p>
                    </div>
                    <button class="sensor-color-picker" style="background:${phaseColor}"
                        title="Pilih Warna Grafik untuk Sensor ${p.phase}"
                        onclick="openSensorColorPicker('${d.id}','${p.phase}','${phaseColor}')"></button>
                    <button class="device-phase-edit-btn" onclick="startRenamePhase('${d.id}','${p.phase}')" title="Ubah nama sensor">${editSVG}</button>
                    <button class="device-delete-btn" onclick="deleteSensor('${d.id}','${p.phase}')" title="Hapus sensor ${p.phase} permanen" style="width:26px;height:26px;border-radius:.25rem 0 .25rem 0">${deleteSVG}</button>
                </div>
                <div class="device-phase-edit" id="phase-edit_${d.id}_${p.phase}" style="display:none">
                    <div class="device-phase-info">
                        <div class="device-phase-edit-field">
                            <input type="text" class="device-phase-rename-input" id="phase-rename_${d.id}_${p.phase}"
                                value="${p.name || p.phase}" maxlength="40" autocomplete="off"
                                onkeydown="if(event.key==='Enter') savePhaseRename('${d.id}','${p.phase}'); else if(event.key==='Escape') cancelRenamePhase('${d.id}','${p.phase}')">
                        </div>
                    </div>
                    <div class="device-phase-actions">
                        <button class="device-confirm-btn" onclick="savePhaseRename('${d.id}','${p.phase}')" title="Simpan">${checkSVG}</button>
                        <button class="device-cancel-btn"  onclick="cancelRenamePhase('${d.id}','${p.phase}')" title="Batal">${closeSVG}</button>
                    </div>
                </div>
            </div>`;
            }).join('')
            : '<p style="font-size:11px;color:var(--text-tertiary);padding:4px 0">Mendeteksi phase…</p>';
        const displayName = d.name && d.name !== d.id ? `${d.id} ${d.name}` : d.id;
        const renameValue = d.name === d.id ? '' : d.name;
        return `
        <div class="device-item" id="device-item_${d.id}">
            <div class="device-view-mode" id="view_${d.id}">
                <div class="device-item-info">
                    <span class="device-online-dot ${dotClass}"></span>
                    <div>
                        <p class="device-item-name" id="label_${d.id}">${displayName}</p>
                        <p class="device-item-id">${d.phaseCount || 0} Sensor · Last seen: ${d.lastSeen || '---'}</p>
                    </div>
                </div>
                <div style="display:flex;gap:4px">
                    <button class="device-edit-btn" onclick="startRenameDevice('${d.id}')" title="Ubah nama">${editSVG}</button>
                    <button class="device-delete-btn" onclick="deleteDevice('${d.id}', '${_escapeAttr(displayName)}')" title="Hapus device">${deleteSVG}</button>
                </div>
            </div>
            <div class="device-edit-mode" id="edit_${d.id}" style="display:none">
                <div class="device-item-info">
                    <span class="device-online-dot ${dotClass}"></span>
                    <div class="device-edit-field">
                        <input type="text" class="device-rename-input-inline" id="rename_${d.id}"
                            value="${renameValue}" placeholder="Nama Lokasi / Toko..." maxlength="40" autocomplete="off"
                            onkeydown="if(event.key==='Enter') saveDeviceName('${d.id}'); else if(event.key==='Escape') cancelRenameDevice('${d.id}')">
                        <p class="device-edit-hint"><kbd>Enter</kbd> simpan &nbsp;·&nbsp; <kbd>Esc</kbd> batal</p>
                    </div>
                </div>
                <div class="device-edit-actions">
                    <button class="device-confirm-btn" onclick="saveDeviceName('${d.id}')"     title="Simpan">${checkSVG}</button>
                    <button class="device-cancel-btn"  onclick="cancelRenameDevice('${d.id}')" title="Batal">${closeSVG}</button>
                </div>
            </div>
            <div class="device-phases-container">${phasesHTML}</div>
        </div>`;
    }).join('');
}
function startRenameDevice(deviceId) {
    _renamingDeviceId = deviceId;
    $(`view_${deviceId}`).style.display = 'none';
    $(`edit_${deviceId}`).style.display = 'flex';
    const input = $(`rename_${deviceId}`);
    input.focus(); input.select();
}
function cancelRenameDevice(deviceId) {
    _renamingDeviceId = null;
    const label = $(`label_${deviceId}`), input = $(`rename_${deviceId}`);
    if (label && input) input.value = label.textContent;
    $(`edit_${deviceId}`).style.display = 'none';
    $(`view_${deviceId}`).style.display = 'flex';
    input.disabled = false;
}
async function saveDeviceName(deviceId) {
    const input = $(`rename_${deviceId}`);
    const newName = input?.value.trim();
    if (!newName) { await showModal('Error', 'Nama tidak boleh kosong', 'warning'); return; }
    if (newName.length < 2) { await showModal('Error', 'Nama minimal 2 karakter', 'warning'); return; }
    if (newName.length > 100) { await showModal('Error', 'Nama maksimal 100 karakter', 'warning'); return; }
    if (/[\/\.\$\#\[\]]/.test(newName)) { await showModal('Error', 'Karakter tidak diizinkan: / . $ # [ ]', 'warning'); return; }
    const oldName = _deviceListCache.find(d => d.id === deviceId)?.name || deviceId;
    const dev = _deviceListCache.find(d => d.id === deviceId);
    if (dev) dev.name = newName;
    
    const label = $(`label_${deviceId}`);
    const displayNewName = newName && newName !== deviceId ? `${deviceId} ${newName}` : deviceId;
    if (label) label.textContent = displayNewName;
    
    if (deviceId === selectedDeviceId) {
        selectedDeviceName = displayNewName;
        [DOM.deviceSelect, DOM.summaryDeviceSelect].forEach(sel => {
            if (sel) Array.from(sel.options).forEach(opt => { if (opt.value === deviceId) opt.text = displayNewName; });
        });
    }
    _renamingDeviceId = null;
    cancelRenameDevice(deviceId);
    showModal('Berhasil', `Nama device diubah menjadi:\n"${newName}"`, 'success');
    try {
        const controller = new AbortController();
        const tid = setTimeout(() => controller.abort(), 8000);
        const response = await fetch(`/api/devices/${deviceId}/rename`, {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ name: newName }), signal: controller.signal,
        });
        clearTimeout(tid);
        if (!response.ok) { const e = await response.json().catch(() => ({})); throw new Error(e.error || `HTTP ${response.status}`); }
        const json = await response.json();
        if (!json.ok) throw new Error(json.error || 'Gagal menyimpan ke database');
    } catch (e) {
        if (dev) dev.name = oldName;
        const displayOldName = oldName && oldName !== deviceId ? `${deviceId} ${oldName}` : deviceId;
        if (label) label.textContent = displayOldName;
        if (deviceId === selectedDeviceId) {
            selectedDeviceName = displayOldName;
            [DOM.deviceSelect, DOM.summaryDeviceSelect].forEach(sel => {
                if (sel) Array.from(sel.options).forEach(opt => { if (opt.value === displayOldName) opt.text = displayOldName; });
            });
        }
        closeModal();
        await showModal('Error', e.name === 'AbortError' ? 'Request timeout (8s) - Periksa koneksi internet' : 'Gagal menyimpan: ' + e.message, 'error');
    }
}
async function deleteDevice(deviceId, deviceName) {
    const confirmed = await showModal('Hapus Device Permanen?', `Apakah Anda yakin ingin menghapus device:\n"${deviceName}"?\n\nSeluruh data histori dan konfigurasi sensor terkait akan dihapus secara permanen dari server.`, 'warning', ['confirm']);
    if (!confirmed) return;
    
    showGlobalLoader();
    try {
        const response = await fetch(`/api/devices/${deviceId}`, {
            method: 'DELETE'
        });
        if (!response.ok) {
            const e = await response.json().catch(() => ({}));
            throw new Error(e.error || `HTTP ${response.status}`);
        }
        const json = await response.json();
        if (!json.ok) throw new Error(json.error || 'Gagal menghapus device');
        
        hideGlobalLoader();
        await showModal('Berhasil', `Device "${deviceName}" berhasil dihapus.`, 'success');
        
        _deviceListCache = _deviceListCache.filter(d => d.id !== deviceId);
        
        if (selectedDeviceId === deviceId) {
            selectedDeviceId = '';
            selectedDeviceName = '';
            if (_mqttClient && _subscribedTopic) {
                _mqttClient.unsubscribe(_subscribedTopic);
                _subscribedTopic = null;
            }
        }
        
        await loadDevices();
    } catch (e) {
        hideGlobalLoader();
        await showModal('Error', 'Gagal menghapus device: ' + e.message, 'error');
    }
}

let _hiddenPhases = new Set();
function toggleChartPhase(phase) {
    if (_hiddenPhases.has(phase)) _hiddenPhases.delete(phase);
    else _hiddenPhases.add(phase);
    renderPhaseToggles();

    if (realtimeChart && realtimeChart.data && realtimeChart.data.datasets) {
        realtimeChart.data.datasets.forEach(ds => {
            if (ds._phaseKey === phase) {
                ds.hidden = _hiddenPhases.has(phase);
            }
        });
        const { yMin, yMax } = getYBoundsMulti(realtimeChart.data.datasets, selectedParameter);
        realtimeChart.options.scales.y.min = yMin;
        realtimeChart.options.scales.y.max = yMax;
        realtimeChart.update('none');
    }
}
function renderPhaseToggles() {
    const container = document.getElementById('chartPhaseToggles');
    if (!container) return;
    const phases = _getEnabledPhaseKeys().slice().sort((a, b) => parseInt(a.slice(1)) - parseInt(b.slice(1)));
    if (phases.length <= 1) {
        container.style.display = 'none';
        return;
    }
    container.style.display = 'flex';
    container.innerHTML = '';
    phases.forEach(ph => {
        const isActive = !_hiddenPhases.has(ph);
        const colors = getPhaseColors(ph);

        const btn = document.createElement('button');
        btn.className = 'phase-toggle-btn' + (isActive ? ' active' : '');
        btn.onclick = () => toggleChartPhase(ph);

        const dot = document.createElement('span');
        dot.className = 'phase-toggle-dot';
        dot.style.background = colors.border || colors.line;

        const txt = document.createTextNode(' ' + getPhaseLabel(ph));

        btn.appendChild(dot);
        btn.appendChild(txt);
        container.appendChild(btn);
    });
}

async function onDeviceChange(deviceId) {
    if (!deviceId || deviceId === selectedDeviceId) return;
    [DOM.deviceSelect, DOM.summaryDeviceSelect].forEach(sel => {
        if (sel && sel.value !== deviceId) sel.value = deviceId;
    });
    if (_prevDeviceId) {
        _hourlyListenerAttached = null;
        _dayListenerAttached = null;
        hourlyHistoryData = {};
        dailyHistoryData = {};
    }
    if (_chartTimer) { clearInterval(_chartTimer); _chartTimer = null; }
    if (_liveBufferPollTimer) { clearInterval(_liveBufferPollTimer); _liveBufferPollTimer = null; }
    selectedDeviceId = deviceId;
    const activeDev = _deviceListCache.find(d => d.id === deviceId);
    selectedDeviceName = activeDev && activeDev.name && activeDev.name !== activeDev.id ? `${activeDev.id} ${activeDev.name}` : deviceId;
    selectedPhase = '';
    updatePhaseSelector([]);
    resetChartData();
    rawRealtimeData = null;
    _phaseLastSeen = {}; // reset per-phase timestamps on device change
    _lastKnownPhaseData = {}; // reset cached phase data on device change

    const canvas = document.getElementById('realtimeChart');
    if (canvas) {
        canvas.style.transition = 'none';
        canvas.style.opacity = '0';
        _showChartSpinner();
        void canvas.offsetWidth;
        canvas.style.transition = 'opacity 0.3s ease';
    }

    initChart();
    updateConnectionStatus('connecting');
    updateDisplayCardsBlank('connecting');
    lastDataTimestamp = 0;
    connectionStartTime = Date.now();
    lastSensorValues = null;
    historyData = []; recordsBySession = {}; sessionsData = {};
    buildSessionUI();
    fetch(`/api/devices/${deviceId}/init-sensors`, { method: 'POST' })
        .then(r => r.json()).then(json => { if (json.phases) loadDevices(); }).catch(() => { });
    _attachRealtimeListener(deviceId);
    _attachHistoryListener(deviceId);
    _attachDeviceNameListener(deviceId);
    _attachPhasesListener(deviceId);
    _attachHourlyListener(deviceId);
    _attachDayListener(deviceId);
    if (activeDev) renderDeviceList([activeDev]);
}
function startRenamePhase(deviceId, phase) {
    const id = `${deviceId}_${phase}`;
    $(`phase-view_${id}`).style.display = 'none';
    $(`phase-edit_${id}`).style.display = 'flex';
    const input = $(`phase-rename_${id}`);
    input.focus(); input.select();
}
function cancelRenamePhase(deviceId, phase) {
    const id = `${deviceId}_${phase}`;
    const label = $(`phase-label_${id}`), input = $(`phase-rename_${id}`);
    if (label && input) input.value = label.textContent;
    $(`phase-edit_${id}`).style.display = 'none';
    $(`phase-view_${id}`).style.display = 'flex';
    input.disabled = false;
}
async function savePhaseRename(deviceId, phase) {
    const id = `${deviceId}_${phase}`;
    const input = $(`phase-rename_${id}`);
    const newName = input?.value.trim();
    if (!newName) { await showModal('Error', 'Nama tidak boleh kosong', 'warning'); return; }
    if (newName.length < 2) { await showModal('Error', 'Nama minimal 2 karakter', 'warning'); return; }
    if (newName.length > 40) { await showModal('Error', 'Nama maksimal 40 karakter', 'warning'); return; }
    if (/[\/\.\$\#\[\]]/.test(newName)) { await showModal('Error', 'Karakter tidak diizinkan: / . $ # [ ]', 'warning'); return; }
    const dev = _deviceListCache.find(d => d.id === deviceId);
    const phaseObj = dev?.phases?.find(p => p.phase === phase);
    const oldName = phaseObj?.name || phase;
    if (phaseObj) phaseObj.name = newName;
    const label = $(`phase-label_${id}`);
    if (label) label.textContent = newName;
    cancelRenamePhase(deviceId, phase);
    if (dev?.phases) updatePhaseSelector(dev.phases.map(p => p.phase));
    if ($('historyContent')?.classList.contains('active')) buildSessionUI();
    showModal('Berhasil', `Sensor ${phase} diubah menjadi:\n"${newName}"`, 'success');
    try {
        const controller = new AbortController();
        const tid = setTimeout(() => controller.abort(), 8000);
        const response = await fetch(`/api/devices/${deviceId}/sensors/${phase}/rename`, {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ name: newName }), signal: controller.signal,
        });
        clearTimeout(tid);
        if (!response.ok) { const e = await response.json().catch(() => ({})); throw new Error(e.error || `HTTP ${response.status}`); }
        const json = await response.json();
        if (!json.ok) throw new Error(json.error || 'Gagal menyimpan');
    } catch (e) {
        if (phaseObj) phaseObj.name = oldName;
        if (label) label.textContent = oldName;
        if (dev?.phases) updatePhaseSelector(dev.phases.map(p => p.phase));
        if ($('historyContent')?.classList.contains('active')) buildSessionUI();
        closeModal();
        await showModal('Error', e.name === 'AbortError' ? 'Request timeout (8s) - Periksa koneksi' : 'Gagal menyimpan: ' + e.message, 'error');
    }
}
async function deleteSensor(deviceId, phase) {
    const confirmed = await showModal('Hapus Sensor Permanen?', `Apakah Anda yakin ingin menghapus sensor ${phase}?\n\nKonfigurasi nama dan warna untuk sensor ini akan dihapus dari device.`, 'warning', ['confirm']);
    if (!confirmed) return;
    
    showGlobalLoader();
    try {
        const res = await fetch(`/api/devices/${encodeURIComponent(deviceId)}/sensors/${encodeURIComponent(phase)}`, {
            method: 'DELETE'
        });
        const json = await res.json();
        if (!json.ok) throw new Error(json.error || 'Gagal menghapus sensor');

        const dev = _deviceListCache.find(d => d.id === deviceId);
        if (dev && dev.phases) {
            dev.phases = dev.phases.filter(p => p.phase !== phase);
        }
        delete _phaseLastSeen[phase];
        if (rawRealtimeData) delete rawRealtimeData[phase];

        hideGlobalLoader();
        await showModal('Berhasil', `Sensor ${phase} berhasil dihapus dari device ${deviceId}.`, 'success');
        
        if (selectedDeviceId === deviceId) {
            const enabledPhases = (dev?.phases || []).filter(p => p.enabled !== false).map(p => p.phase);
            updatePhaseSelector(enabledPhases);
            _rebuildChart();
        }
        await loadDevices();
    } catch (e) {
        hideGlobalLoader();
        await showModal('Error', 'Gagal menghapus sensor: ' + e.message, 'error');
    }
}
async function togglePhaseEnabled(deviceId, phase, enabled) {
    const dev = _deviceListCache.find(d => d.id === deviceId);
    const phaseObj = dev?.phases?.find(p => p.phase === phase);
    if (!enabled && dev?.phases) {
        const stillEnabled = dev.phases.filter(p => p.phase !== phase && p.enabled !== false);
        if (stillEnabled.length === 0) {
            const cb = document.querySelector(`#phase-item_${deviceId}_${phase} .phase-toggle-cb`);
            if (cb) cb.checked = true;
            await showModal('Tidak Diizinkan', 'Minimal satu sensor harus tetap aktif.', 'warning');
            return;
        }
    }
    if (phaseObj) phaseObj.enabled = enabled;
    const item = $(`phase-item_${deviceId}_${phase}`);
    if (item) item.classList.toggle('phase-disabled', !enabled);
    const badge = item?.querySelector('.device-phase-badge');
    if (badge) badge.style.opacity = enabled ? '' : '0.4';
    const nameEl = $(`phase-label_${deviceId}_${phase}`);
    if (nameEl) {
        nameEl.style.opacity = enabled ? '' : '0.45';
        nameEl.style.textDecoration = enabled ? '' : 'line-through';
    }
    const phaseSeen = _phaseLastSeen[phase];
    const isLive = phaseSeen && (Date.now() - phaseSeen <= 45000);
    const statusHTML = !enabled 
        ? '<span class="sensor-status-badge disabled"><span class="status-dot-pulse"></span> Nonaktif</span>'
        : (isLive 
            ? '<span class="sensor-status-badge live"><span class="status-dot-pulse"></span> Terkoneksi</span>' 
            : '<span class="sensor-status-badge idle"><span class="status-dot-pulse"></span> Terputus</span>');
    const statusEl = item?.querySelector('.device-phase-status');
    if (statusEl) statusEl.innerHTML = statusHTML;
    if (deviceId === selectedDeviceId && dev?.phases) {
        const enabledPhases = dev.phases.filter(p => p.enabled !== false).map(p => p.phase);
        updatePhaseSelector(enabledPhases);
    }
    if (deviceId === selectedDeviceId) {
        _rebuildChart();
    }
    try {
        await fetch(`/api/devices/${deviceId}/sensors/${phase}/enabled`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ enabled }),
        });
    } catch (e) {
        if (phaseObj) phaseObj.enabled = !enabled;
        const cb = document.querySelector(`#phase-item_${deviceId}_${phase} .phase-toggle-cb`);
        if (cb) cb.checked = !enabled;
        if (deviceId === selectedDeviceId) _rebuildChart();
    }
}
let _liveBufferPollTimer = null;

function startLiveBufferPolling(deviceId) {
    if (_liveBufferPollTimer) {
        clearInterval(_liveBufferPollTimer);
        _liveBufferPollTimer = null;
    }

    const pollFn = async () => {
        if (!selectedDeviceId || selectedDeviceId !== deviceId) return;
        try {
            const res = await fetch(`/api/live-buffer/${encodeURIComponent(deviceId)}`);
            if (!res.ok) return;
            const dataList = await res.json();
            if (!Array.isArray(dataList) || dataList.length === 0) return;

            const latestItem = dataList[dataList.length - 1];
            if (latestItem.data && latestItem.data.offline) {
                isConnected = false;
                updateConnectionStatus(false);
            } else {
                const lastValidItem = dataList.slice().reverse().find(item => item.data && !item.data.offline);
                if (lastValidItem) {
                    rawRealtimeData = JSON.parse(JSON.stringify(lastValidItem.data));
                    isConnected = true;
                    lastDataTimestamp = Date.now();

                    // Update per-phase last seen timestamp & last known data untuk status realtime
                    const nowMs = Date.now();
                    Object.keys(lastValidItem.data).forEach(ph => {
                        if (/^L\d+$/.test(ph)) {
                            _phaseLastSeen[ph] = nowMs;
                            if (lastValidItem.data[ph]) {
                                _lastKnownPhaseData[ph] = lastValidItem.data[ph];
                            }
                        }
                    });

                    _processIncomingMQTTData();
                    updateConnectionStatus(true);

                    // Panggil loadDevices jika terdeteksi phase baru (misal L17) yang belum terdaftar di UI
                    const dev = _deviceListCache.find(d => d.id === selectedDeviceId);
                    const knownPhases = (dev?.phases || []).map(p => p.phase);
                    const incomingPhases = Object.keys(lastValidItem.data).filter(k => /^L\d+$/.test(k));
                    if (incomingPhases.some(p => !knownPhases.includes(p))) {
                        loadDevices();
                    }
                }
            }
        } catch (e) {
            console.error('[LiveBuffer] Polling error:', e);
        }
    };

    pollFn();
    _liveBufferPollTimer = setInterval(pollFn, 3000);
}

function _attachRealtimeListener(deviceId) {
    _prevDeviceId = deviceId;
    _chartInit(deviceId);
    _firstSnap = true;

    startLiveBufferPolling(deviceId);
    initMQTT();
    _subscribeToDevice(deviceId);
}

function initMQTT() {
    if (_mqttClient) return;

    if (!isConnected) {
        updateConnectionStatus('connecting');
    }
    const brokerUrl = 'wss://166f9507c83945a4a1c4be54fccdb9a9.s1.eu.hivemq.cloud:8884/mqtt';
    const options = {
        username: 'EnergyMeter',
        password: '8nxbqv6WJn@VkuJ',
        clientId: 'web_client_' + Math.random().toString(16).substr(2, 8),
        keepalive: 60,
        reconnectPeriod: 5000,
        connectTimeout: 30 * 1000,
        clean: true
    };

    _mqttClient = mqtt.connect(brokerUrl, options);

    _mqttClient.on('connect', () => {
        console.log('[MQTT] Connected to broker');
        if (selectedDeviceId) {
            _subscribeToDevice(selectedDeviceId);
        }
    });

    // Mapping kunci JSON singkat dari ESP32 → nama field standar
    const _ESP32_JSON_MAP_JS = {
        'V': 'Voltage (V)', 'A': 'Current (A)', 'W': 'Power (W)',
        'Hz': 'Frequency (Hz)', 'kWh': 'Active Energy (kWh)', 'pf': 'Power Factor',
    };

    _mqttClient.on('message', (topic, message) => {
        try {
            const payload = message.toString();
            const parts = topic.split('/');
            if (parts.length >= 2 && parts[0] === 'energymeter' && parts[1] === selectedDeviceId) {
                if (!rawRealtimeData) {
                    rawRealtimeData = {};
                }

                // [FIX] Update timestamp segera saat MQTT message manapun diterima dari device
                // Ini mencegah false-offline saat bacaan stabil (ESP32 delta filter suppress publish)
                lastDataTimestamp = Date.now();

                if (parts.length === 3 && parts[2] === 'Timestamp') {
                    // energymeter/alat1/Timestamp
                    rawRealtimeData.Timestamp = payload;
                    _processIncomingMQTTData();

                } else if (parts.length === 3 && /^L\d+$/.test(parts[2])) {
                    // [FORMAT BARU] energymeter/alat1/L1
                    // ESP32 kirim 1 JSON per channel:
                    // {"V":220.1,"A":1.234,"W":270.1,"Hz":50.01,"kWh":0.123,"pf":0.987}
                    const phase = parts[2];
                    // Track per-phase last-seen timestamp
                    _phaseLastSeen[phase] = Date.now();
                    try {
                        const data = JSON.parse(payload);
                        if (!rawRealtimeData[phase]) rawRealtimeData[phase] = {};
                        Object.entries(data).forEach(([k, v]) => {
                            const mapped = _ESP32_JSON_MAP_JS[k] || k;
                            rawRealtimeData[phase][mapped] = parseFloat(v) || 0;
                        });
                        _processIncomingMQTTData();
                    } catch (parseErr) {
                        console.error('[MQTT] JSON parse error:', parseErr);
                    }

                } else if (parts.length === 4) {
                    // [FORMAT LAMA] energymeter/alat1/L1/Voltage_V → float tunggal
                    const phase = parts[2];
                    // Track per-phase last-seen timestamp (format lama)
                    _phaseLastSeen[phase] = Date.now();
                    const metric = parts[3];
                    if (!rawRealtimeData[phase]) {
                        rawRealtimeData[phase] = {};
                    }
                    const mappedMetric = MAP_METRIC_JS[metric] || metric;
                    rawRealtimeData[phase][mappedMetric] = parseFloat(payload) || 0;
                }
            }
        } catch (e) {
            console.error('[MQTT] Message parsing error:', e);
        }
    });

    _mqttClient.on('close', () => {
        console.log('[MQTT] Connection closed');
        updateConnectionStatus(false);
    });

    _mqttClient.on('error', (err) => {
        console.error('[MQTT] Connection error:', err);
        updateConnectionStatus(false);
    });
}

function _subscribeToDevice(deviceId) {
    if (!_mqttClient || !_mqttClient.connected) return;

    if (_subscribedTopic) {
        _mqttClient.unsubscribe(_subscribedTopic);
        console.log(`[MQTT] Unsubscribed from: ${_subscribedTopic}`);
    }

    _subscribedTopic = `energymeter/${deviceId}/#`;
    _mqttClient.subscribe(_subscribedTopic, (err) => {
        if (err) {
            console.error(`[MQTT] Subscription error for ${deviceId}:`, err);
        } else {
            console.log(`[MQTT] Subscribed to topic: ${_subscribedTopic}`);
        }
    });
}

function _processIncomingMQTTData() {
    if (!rawRealtimeData) return;
    const data = normalizeHistoryData(rawRealtimeData);
    if (!data) { updateConnectionStatus(false); return; }

    // [FIX] Hapus _firstSnap — data pertama langsung diproses, tidak dibuang
    // Dulu: data pertama selalu dibuang dan tidak pernah set isConnected=true
    _firstSnap = false;



    // [FIX] Selalu update isConnected dan lastSensorValues untuk data valid,
    // terlepas dari apakah nilai berubah atau tidak.
    // Dulu: hasChanged=false → lastDataTimestamp tidak diupdate → timeout 30s → offline
    lastSensorValues = {
        Voltage: data.Voltage,
        Current: data.Current,
        Power: data.Power,
        Frequency: data.Frequency,
        Energy: data.Energy,
        PowerFactor: data.PowerFactor
    };
    isConnected = true;

    const dev = _deviceListCache.find(d => d.id === selectedDeviceId);
    const knownPhases = (dev?.phases || []).map(p => p.phase);
    const hasNewPhase = (data._phases || []).some(p => !knownPhases.includes(p));

    if (hasNewPhase && (data._phases || []).length > 0) {
        fetch(`/api/devices/${selectedDeviceId}/init-sensors`, { method: 'POST' })
            .then(r => r.json()).then(() => loadDevices()).catch(() => { });
    }

    realtimeData = data;

    if (selectedPhase) {
        // Check if the selected phase has timed out (no MQTT data for 30s)
        const phaseSeen = _phaseLastSeen[selectedPhase];
        const phaseTimedOut = !phaseSeen || (Date.now() - phaseSeen > PHASE_DATA_TIMEOUT_MS);
        if (phaseTimedOut) {
            updateDisplayCardsBlank();
        } else {
            const displayData = getPhaseDisplayData(rawRealtimeData, selectedPhase);
            if (displayData) updateDisplayCards(displayData);
            else updateDisplayCardsBlank();
        }
    }
    updateConnectionStatus(true);
}

function _updateSettingsSensorStatuses() {
    const activeDev = _deviceListCache.find(d => d.id === selectedDeviceId);
    if (!activeDev || !activeDev.phases) return;

    activeDev.phases.forEach(p => {
        const item = document.getElementById(`phase-item_${selectedDeviceId}_${p.phase}`);
        if (!item) return;
        const statusEl = item.querySelector('.device-phase-status');
        if (!statusEl) return;

        const isEnabled = p.enabled !== false;
        const phaseSeen = _phaseLastSeen[p.phase];
        const isLive = phaseSeen && (Date.now() - phaseSeen <= 45000);
        const statusHTML = !isEnabled 
            ? '<span class="sensor-status-badge disabled"><span class="status-dot-pulse"></span> Nonaktif</span>'
            : (isLive 
                ? '<span class="sensor-status-badge live"><span class="status-dot-pulse"></span> Terkoneksi</span>' 
                : '<span class="sensor-status-badge idle"><span class="status-dot-pulse"></span> Terputus</span>');

        if (statusEl.innerHTML !== statusHTML) {
            statusEl.innerHTML = statusHTML;
        }
    });
}

// Checks if the currently-selected phase has stopped publishing and blanks the cards
function _checkPhaseDataFreshness() {
    _updateSettingsSensorStatuses();
    if (!isConnected || !selectedPhase) return;
    const phaseSeen = _phaseLastSeen[selectedPhase];
    const phaseTimedOut = !phaseSeen || (Date.now() - phaseSeen > PHASE_DATA_TIMEOUT_MS);
    if (phaseTimedOut) {
        // Only blank if currently showing real data (prevent double-update)
        const firstCard = document.getElementById('voltage');
        if (firstCard && firstCard.textContent !== '---') {
            updateDisplayCardsBlank();
        }
    }
}

function startPhaseTimeoutMonitoring() {
    if (_phaseTimeoutCheckId) clearInterval(_phaseTimeoutCheckId);
    _phaseTimeoutCheckId = setInterval(_checkPhaseDataFreshness, 2000);
}
function updateConnectionStatus(connected) {
    const dot = DOM.statusDot, txt = DOM.statusText;
    if (!dot || !txt) return;
    if (connected === 'connecting') {
        dot.className = 'status-dot connecting';
        txt.textContent = 'CONNECTING';
        return;
    }
    dot.className = 'status-dot ' + (connected ? 'online' : 'offline');
    txt.textContent = connected ? 'ONLINE' : 'OFFLINE';
    if (!connected) updateDisplayCardsBlank();
}
function checkDataFreshness() {
    const now = Date.now();
    // Timeout 300 detik agar sinkron dengan backend (300s = threshold offline backend).
    // ESP32 delta-filter: saat bacaan listrik stabil, bisa tidak ada publish selama menit-menit.
    // Backend heartbeat (30s) memastikan lastDataTimestamp selalu diperbarui walau nilai tidak berubah.
    if (lastDataTimestamp > 0 && (now - lastDataTimestamp > 300000)) {
        if (isConnected) {
            isConnected = false; realtimeData = null; rawRealtimeData = null;
            updateConnectionStatus(false);
        }
    } else if (lastDataTimestamp === 0 && (now - connectionStartTime > 10000)) {
        isConnected = false;
        updateConnectionStatus(false);
    }
}
function startConnectionMonitoring() {
    if (connectionCheckInterval) clearInterval(connectionCheckInterval);
    connectionCheckInterval = setInterval(checkDataFreshness, 2000);
}
function onDbSearchInput(value) {
    dbSearchQuery = value.trim().toLowerCase();
    $('dbSearchClear')?.classList.toggle('visible', dbSearchQuery.length > 0);
    buildSessionUI();
}
function clearDbSearch() {
    const input = $('dbSearchInput');
    if (input) input.value = '';
    dbSearchQuery = '';
    $('dbSearchClear')?.classList.remove('visible');
    buildSessionUI();
    input?.focus();
}
let historyData = [], recordsBySession = {};
async function _attachHistoryListener(deviceId, isAutoPoll = false) {
    try {
        const res = await fetch(`/api/devices/${deviceId}/sessions`);
        const list = await res.json();

        let totalCount = 0;
        historyData = [];
        
        const oldSessions = { ...sessionsData };
        sessionsData = {};
        
        // Preserve offline backups
        Object.values(oldSessions).forEach(s => {
            if (s.isOfflineBackup) {
                sessionsData[s.id] = s;
                totalCount += s.recordCount || 0;
            }
        });

        list.forEach(meta => {
            if (oldSessions[meta.id] && oldSessions[meta.id].computedPhases) {
                meta.computedPhases = oldSessions[meta.id].computedPhases;
            }
            sessionsData[meta.id] = meta;
            totalCount += meta.recordCount || 0;
        });
        historyData = Array(totalCount).fill(1);
        buildSessionUI(isAutoPoll);
    } catch (err) {
        console.error("Error loading sessions:", err);
    }
}
function _attachDeviceNameListener(deviceId) {
    // No-op: handled via REST device list reloading
}
function _attachPhasesListener(deviceId) {
    // No-op: handled via REST device list reloading
}
function parseTimestamp(ts) {
    try {
        const [time, date] = ts.split(' ');
        const [h, m, s] = time.split(':').map(Number);
        const [d, mo, y] = date.split('/').map(Number);
        const dt = new Date(y, mo - 1, d, h, m, s);
        if (isNaN(dt.getTime())) return new Date();
        return dt;
    } catch (_) { return new Date(); }
}
function sortByEpochDesc(a, b) {
    const ea = a.epoch ?? parseTimestamp(a.timestamp).getTime();
    const eb = b.epoch ?? parseTimestamp(b.timestamp).getTime();
    return eb - ea;
}
function sortByEpochAsc(a, b) {
    const ea = a.epoch ?? parseTimestamp(a.timestamp).getTime();
    const eb = b.epoch ?? parseTimestamp(b.timestamp).getTime();
    return ea - eb;
}
function _escapeAttr(s) { return (s || '').replace(/'/g, "\\'"); }
function _highlight(text, query = dbSearchQuery) {
    if (!query) return text;
    return text.replace(new RegExp(`(${query.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')})`, 'gi'), '<mark class="search-highlight">$1</mark>');
}
function buildSessionUI(isAutoPoll = false) {
    // 1. Jika auto-poll berkala dan user sedang membuka menu dropdown 3 titik: tunda refresh UI agar menu tidak tertutup!
    const isAnyDropdownOpen = Array.from(document.querySelectorAll('.session-dropdown-menu')).some(el => el.style.display === 'block');
    if (isAutoPoll && isAnyDropdownOpen) return;

    const allSessions = Object.values(sessionsData).sort((a, b) => (b.startTimestamp || 0) - (a.startTimestamp || 0));
    const filtered = dbSearchQuery ? allSessions.filter(s => (s.name || s.id || '').toLowerCase().includes(dbSearchQuery)) : allSessions;
    if (DOM.historyCount) {
        DOM.historyCount.textContent = dbSearchQuery
            ? `${filtered.length} dari ${allSessions.length} sesi · ${historyData.length} total record`
            : `${allSessions.length} sesi · ${historyData.length} total record`;
    }
    const tbody = DOM.historyBody;
    if (!tbody) return;
    if (!filtered.length) {
        tbody.innerHTML = `<tr><td colspan="5" class="loading-cell">${dbSearchQuery ? 'Tidak ada sesi yang cocok.' : 'Belum ada data rekaman'}</td></tr>`;
        return;
    }
    
    // 2. Hitung jumlah baris sesi (bukan detail row) yang ada di DOM saat ini
    const existingRowCount = tbody.querySelectorAll('.session-row').length;

    // 3. Jika ini auto-poll berkala, dan jumlah baris di DOM SAMA dengan jumlah sesi yang ada:
    //    Cukup perbarui angka recordCount di tempat agar DOM tidak di-wipe & zoom level grafik tidak ter-reset!
    //    Jika ada sesi baru (jumlah baris beda / 0), maka jalankan render ulang lengkap agar sesi baru langsung muncul!
    if (isAutoPoll && existingRowCount === filtered.length && tbody.children.length > 0) {
        filtered.forEach(session => {
            const row = document.getElementById(`detail_${session.id}`)?.previousElementSibling;
            if (row) {
                const countBadge = row.querySelector('.record-count-badge');
                if (countBadge) {
                    countBadge.textContent = `${session.recordCount || 0} record`;
                }
            }
        });
        return;
    }

    const openSessions = new Set();
    document.querySelectorAll('.session-detail-row').forEach(row => { if (row.style.display !== 'none') openSessions.add(row.id.replace('detail_', '')); });
    
    tbody.innerHTML = filtered.map(session => {
        const dev2 = _deviceListCache.find(d => d.id === (session.deviceId || selectedDeviceId));
        const liveDeviceName = dev2?.name || session.deviceName || session.deviceId;
        const isActive = session.id === currentSessionId && captureActive;
        let actionBtns = '';
        if (isActive) {
            actionBtns += `
            <div class="session-dropdown-wrap" onclick="event.stopPropagation()">
                <button class="session-more-btn" onclick="toggleSessionDropdown('${session.id}', event)" title="Menu Aksi">
                    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><circle cx="12" cy="5" r="1.5"></circle><circle cx="12" cy="12" r="1.5"></circle><circle cx="12" cy="19" r="1.5"></circle></svg>
                </button>
                <div class="session-dropdown-menu" id="dropdown_${session.id}">
                    <button class="session-dropdown-item" onclick="openChangeTimeModal('${session.id}','${session.startTime}','${_escapeAttr(session.name)}',event)">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"></circle><polyline points="12 6 12 12 16 14"></polyline></svg>
                        Ubah Waktu
                    </button>
                </div>
            </div>`;
        } else {
            actionBtns += `
            <button class="session-export-btn" onclick="exportSession('${session.id}','${_escapeAttr(session.name)}',event)" title="Export Excel">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"></path><polyline points="7 10 12 15 17 10"></polyline><line x1="12" y1="15" x2="12" y2="3"></line></svg>
            </button>
            <div class="session-dropdown-wrap" onclick="event.stopPropagation()">
                <button class="session-more-btn" onclick="toggleSessionDropdown('${session.id}', event)" title="Menu Aksi">
                    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><circle cx="12" cy="5" r="1.5"></circle><circle cx="12" cy="12" r="1.5"></circle><circle cx="12" cy="19" r="1.5"></circle></svg>
                </button>
                <div class="session-dropdown-menu" id="dropdown_${session.id}">
                    <button class="session-dropdown-item" onclick="openRenameModal('${session.id}','${_escapeAttr(session.name)}',event)">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"></path><path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"></path></svg>
                        Rename Sesi
                    </button>
                    <button class="session-dropdown-item" onclick="openChangeTimeModal('${session.id}','${session.startTime}','${_escapeAttr(session.name)}',event)">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"></circle><polyline points="12 6 12 12 16 14"></polyline></svg>
                        Ubah Waktu
                    </button>
                    ${!session.isOfflineBackup ? `
                    <button class="session-dropdown-item" onclick="backupSessionJSON('${session.id}','${_escapeAttr(session.name)}',event)">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"></path><polyline points="17 8 12 3 7 8"></polyline><line x1="12" y1="3" x2="12" y2="15"></line></svg>
                        Backup JSON
                    </button>
                    ` : ''}
                    <div style="border-top:1px solid var(--border);margin:4px 0"></div>
                    <button class="session-dropdown-item danger" onclick="deleteSession('${session.id}','${_escapeAttr(session.name)}',event)">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="3 6 5 6 21 6"></polyline><path d="M19 6l-1 14H6L5 6"></path><path d="M10 11v6m4-6v6"></path></svg>
                        Hapus Sesi
                    </button>
                </div>
            </div>`;
        }

        return `
        <tr class="session-row${isActive ? ' session-active' : ''}" onclick="toggleSessionDetail('${session.id}')">
            <td class="session-toggle-cell">
                <span class="session-chevron" id="chevron_${session.id}">&#9658;</span>
                <span class="session-id-badge" title="${session.id}">${session.id.replace('session_', '')}</span>
            </td>
            <td class="session-name-cell">
                <span class="session-name">${_highlight(session.name || 'Tanpa nama')}</span>
                ${(session.deviceName || liveDeviceName) && (session.deviceName || liveDeviceName) !== session.deviceId ? `<span style="font-size:10.5px;color:var(--text-tertiary);margin-left:6px;font-weight:600">· ${session.deviceName || liveDeviceName}</span>` : ''}
                ${isActive ? '<span class="session-live-badge">&#9679; LIVE</span>' : ''}
                ${session.isOfflineBackup ? '<span class="session-live-badge" style="background:rgba(147,51,234,0.1);color:#9333ea;border-color:rgba(147,51,234,0.25)">&#9679; OFFLINE VIEW</span>' : ''}
            </td>
            <td>${session.startTime || '---'}</td>
            <td>${isActive ? '<span style="color:#00A651;font-weight:700">Sedang berlangsung...</span>' : (session.endTime || '---')}</td>
            <td style="text-align:right;padding-right:16px">
                <div class="session-actions">
                    <span class="record-count-badge">${session.recordCount || 0} record</span>
                    ${actionBtns}
                </div>
            </td>
        </tr>
        <tr class="session-detail-row" id="detail_${session.id}" style="display:none">
            <td colspan="5" style="padding:16px; background:var(--surface-2);">
                <div class="session-dashboard" id="session-dash_${session.id}"></div>
            </td>
        </tr>`;
    }).join('');

    openSessions.forEach(sid => {
        const detail = $(`detail_${sid}`), chevron = $(`chevron_${sid}`);
        if (detail) detail.style.display = 'table-row';
        if (chevron) chevron.textContent = '\u25BC';
        
        if (!_sessionSelectedParam[sid]) _sessionSelectedParam[sid] = 'Power';
        const session = sessionsData[sid];
        if (session) {
            const devObj = _deviceListCache.find(d => d.id === (session.deviceId || selectedDeviceId));
            const frozenNames = session.phaseNames || {};
            const recordedPhaseKeys = Object.keys(recordsBySession[sid] || {}).filter(k => /^L\d+$/.test(k));
            const backendPhaseKeys = (session.phases || []).filter(k => /^L\d+$/.test(k));
            const frozenPhaseKeys = Object.keys(frozenNames).filter(k => /^L\d+$/.test(k));
            const allKeysSet = new Set([...recordedPhaseKeys, ...backendPhaseKeys, ...frozenPhaseKeys]);
            if (allKeysSet.size === 0 && devObj && devObj.phases) {
                devObj.phases.filter(p => p.enabled !== false).forEach(p => allKeysSet.add(p.phase));
            }
            const phaseSourceKeys = Array.from(allKeysSet).sort((a, b) => parseInt(a.slice(1)) - parseInt(b.slice(1)));
            const phases = phaseSourceKeys.map(ph => {
                const displayName = frozenNames[ph] || devObj?.phases?.find(p => p.phase === ph)?.name || ph;
                return { phase: ph, name: displayName };
            });
            session.computedPhases = phases;
            
            if (!_sessionSelectedPhase[sid]) _sessionSelectedPhase[sid] = phases[0]?.phase || 'L1';
            if (!_sessionSelectedPage[sid]) _sessionSelectedPage[sid] = 1;
            
            renderSessionDashboard(sid);
        }
    });
}
function toggleSessionDetail(sessionId) {
    const detail = $(`detail_${sessionId}`), chevron = $(`chevron_${sessionId}`);
    if (!detail) return;
    const isOpen = detail.style.display !== 'none';
    detail.style.display = isOpen ? 'none' : 'table-row';
    if (chevron) chevron.textContent = isOpen ? '\u25B6' : '\u25BC';

    if (!isOpen) {
        if (!_sessionSelectedParam[sessionId]) _sessionSelectedParam[sessionId] = 'Power';
        const session = sessionsData[sessionId];
        if (session) {
            const devObj = _deviceListCache.find(d => d.id === (session.deviceId || selectedDeviceId));
            const frozenNames = session.phaseNames || {};
            const recordedPhaseKeys = Object.keys(recordsBySession[sessionId] || {}).filter(k => /^L\d+$/.test(k));
            const backendPhaseKeys = (session.phases || []).filter(k => /^L\d+$/.test(k));
            const frozenPhaseKeys = Object.keys(frozenNames).filter(k => /^L\d+$/.test(k));
            const allKeysSet = new Set([...recordedPhaseKeys, ...backendPhaseKeys, ...frozenPhaseKeys]);
            if (allKeysSet.size === 0 && devObj && devObj.phases) {
                devObj.phases.filter(p => p.enabled !== false).forEach(p => allKeysSet.add(p.phase));
            }
            const phaseSourceKeys = Array.from(allKeysSet).sort((a, b) => parseInt(a.slice(1)) - parseInt(b.slice(1)));
            const phases = phaseSourceKeys.map(ph => {
                const displayName = frozenNames[ph] || devObj?.phases?.find(p => p.phase === ph)?.name || ph;
                return { phase: ph, name: displayName };
            });
            session.computedPhases = phases;
            
            if (!_sessionSelectedPhase[sessionId]) _sessionSelectedPhase[sessionId] = phases[0]?.phase || 'L1';
            if (!_sessionSelectedPage[sessionId]) _sessionSelectedPage[sessionId] = 1;

            const phaseKeys = phases.map(p => p.phase);
            
            const container = $(`session-dash_${sessionId}`);
            if (container) {
                container.innerHTML = `
                <div style="display:flex; flex-direction:column; align-items:center; justify-content:center; padding:48px 24px; color:var(--text-secondary); background:var(--surface); border:1px solid var(--border); border-radius:var(--radius-md) 0 var(--radius-md) 0;">
                    <div style="width:24px; height:24px; border:2.5px solid var(--border); border-top-color:var(--brand); border-radius:50%; animation: spin 0.8s linear infinite; margin-bottom:12px;"></div>
                    <span style="font-size:12px; font-weight:600; letter-spacing:0.3px">Memuat data sensor sesi...</span>
                </div>`;
            }
            
            Promise.all(phaseKeys.map(phase => {
                return new Promise((resolve) => {
                    _fetchPhaseHistory(sessionId, phase, (data) => {
                        resolve(data);
                    });
                });
            })).then(() => {
                renderSessionDashboard(sessionId);
            });
        }
    } else {
        if (_sessionCharts[sessionId]) {
            _sessionCharts[sessionId].destroy();
            delete _sessionCharts[sessionId];
        }
    }
}

// ==========================================
// UNIFIED SESSION DASHBOARD LOGIC (HISTORY TAB)
// ==========================================
const _sessionSelectedParam = {};
const _sessionSelectedPhase = {};
const _sessionSelectedPage = {};
const _sessionTimeFilter = {};
const _sessionCharts = {};

function setSessionTimeFilter(sessionId, filter) {
    _sessionTimeFilter[sessionId] = filter;
    _sessionSelectedPage[sessionId] = 1;
    const container = $(`session-dash_${sessionId}`);
    if (container) {
        const btnSesi = container.querySelector(`[onclick*="'session'"]`);
        const btnDay = container.querySelector(`[onclick*="'day'"]`);
        const btnWeek = container.querySelector(`[onclick*="'week'"]`);
        if (btnSesi) btnSesi.classList.toggle('active', filter === 'session');
        if (btnDay) btnDay.classList.toggle('active', filter === 'day');
        if (btnWeek) btnWeek.classList.toggle('active', filter === 'week');
    }
    renderSessionChart(sessionId);
}

let activeOfflineTimeFilter = 'session';

function setOwTimeFilter(filter) {
    activeOfflineTimeFilter = filter;
    activeOfflineSelectedPage = 1;
    ['session', 'day', 'week'].forEach(f => {
        const btn = document.getElementById(`owFilter_${f}`);
        if (btn) btn.classList.toggle('active', f === filter);
    });
    renderOfflineChart();
}

function aggregateRecordsByTime(recordsMap, phases, param, mode) {
    const allTimestampsSet = new Set();
    phases.forEach(p => {
        const records = recordsMap[p.phase] || [];
        records.forEach(r => { if (r.timestamp) allTimestampsSet.add(r.timestamp); });
    });
    const sortedTimestamps = Array.from(allTimestampsSet).sort((a, b) => parseTimestampToEpoch(a) - parseTimestampToEpoch(b));

    if (mode === 'session' || !mode) {
        return { sortedTimestamps, recordsMap };
    }

    const _p = v => String(v).padStart(2, '0');
    const getBucketKey = (ts) => {
        const d = parseTimestampToDate(ts);
        if (isNaN(d.getTime())) return null;

        if (mode === 'day') {
            const m = Math.floor(d.getMinutes() / 15) * 15;
            return `${d.getFullYear()}-${_p(d.getMonth() + 1)}-${_p(d.getDate())} ${_p(d.getHours())}:${_p(m)}`;
        } else if (mode === 'week') {
            return `${d.getFullYear()}-${_p(d.getMonth() + 1)}-${_p(d.getDate())}`;
        }
        return null;
    };

    const bucketSet = new Set();
    const aggData = {};

    phases.forEach(p => {
        const ph = p.phase;
        aggData[ph] = {};
        const records = recordsMap[ph] || [];
        records.forEach(r => {
            if (!r.timestamp) return;
            const bk = getBucketKey(r.timestamp);
            if (!bk) return;
            bucketSet.add(bk);
            if (!aggData[ph][bk]) aggData[ph][bk] = { sum: 0, count: 0 };
            const v = r[param];
            if (v != null && !isNaN(v)) {
                aggData[ph][bk].sum += parseFloat(v);
                aggData[ph][bk].count += 1;
            }
        });
    });

    const sortedBuckets = Array.from(bucketSet).sort((a, b) => {
        const epA = Date.parse(a.replace(/-/g, '/')) || 0;
        const epB = Date.parse(b.replace(/-/g, '/')) || 0;
        return epA - epB;
    });

    const aggregatedRecordsMap = {};
    phases.forEach(p => {
        const ph = p.phase;
        aggregatedRecordsMap[ph] = sortedBuckets.map(bk => {
            const entry = aggData[ph]?.[bk];
            const avgVal = (entry && entry.count > 0) ? (entry.sum / entry.count) : null;
            const obj = { timestamp: bk };
            obj[param] = avgVal != null ? parseFloat(avgVal.toFixed(4)) : null;
            return obj;
        });
    });

    return { sortedTimestamps: sortedBuckets, recordsMap: aggregatedRecordsMap };
}

const _activeHistoryFetches = new Set();
const _activeHistoryCallbacks = {};

function _fetchPhaseHistory(sessionId, phase, cb) {
    if (recordsBySession[sessionId]?.[phase]) {
        if (cb) cb(recordsBySession[sessionId][phase]);
        return;
    }
    const key = `${sessionId}__${phase}`;
    if (_activeHistoryFetches.has(key)) {
        if (cb) {
            if (!_activeHistoryCallbacks[key]) _activeHistoryCallbacks[key] = [];
            _activeHistoryCallbacks[key].push(cb);
        }
        return;
    }
    _activeHistoryFetches.add(key);
    if (cb) {
        if (!_activeHistoryCallbacks[key]) _activeHistoryCallbacks[key] = [];
        _activeHistoryCallbacks[key].push(cb);
    }
    fetch(`/api/devices/${selectedDeviceId}/history/${sessionId}/${phase}`)
        .then(res => res.json())
        .then(historyMap => {
            if (!recordsBySession[sessionId]) recordsBySession[sessionId] = {};
            const arr = [];
            Object.entries(historyMap).forEach(([k, val]) => {
                if (k !== '_meta') arr.push(val);
            });
            recordsBySession[sessionId][phase] = arr;
            _activeHistoryFetches.delete(key);
            const cbs = _activeHistoryCallbacks[key] || [];
            delete _activeHistoryCallbacks[key];
            cbs.forEach(callback => callback(arr));
        })
        .catch(err => {
            console.error("Error loading phase history details:", err);
            _activeHistoryFetches.delete(key);
            const cbs = _activeHistoryCallbacks[key] || [];
            delete _activeHistoryCallbacks[key];
            cbs.forEach(callback => callback(null));
        });
}

function renderSessionDashboard(sessionId) {
    const session = sessionsData[sessionId];
    if (!session) return;

    const phases = session.computedPhases || [];
    const activePhase = _sessionSelectedPhase[sessionId] || 'L1';
    const activeParam = _sessionSelectedParam[sessionId] || 'Power';

    const container = $(`session-dash_${sessionId}`);
    if (!container) return;

    let hasSkeleton = container.querySelector('.session-dashboard-card');
    if (!hasSkeleton) {
        container.innerHTML = `
        <div class="session-dashboard-card" style="margin-bottom:12px; background:var(--surface); border:1px solid var(--border); border-radius:var(--radius-md) 0 var(--radius-md) 0; padding:16px;">
            <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:12px;">
                <div style="font-family:var(--font-ui)">
                    <h5 style="margin:0; font-size:13px; font-weight:800; color:var(--text-primary)">Grafik Telemetri Sesi</h5>
                    <p style="margin:2px 0 0; font-size:11px; color:var(--text-tertiary)">Visualisasi perbandingan seluruh sensor</p>
                </div>
                <div style="display:flex; gap:8px; align-items:center; flex-wrap:wrap">
                    <div class="time-filter-group" style="scale:0.85; transform-origin:right center;">
                        <button class="time-filter-btn ${(_sessionTimeFilter[sessionId] || 'session') === 'session' ? 'active' : ''}" onclick="setSessionTimeFilter('${sessionId}', 'session')">Sesi</button>
                        <button class="time-filter-btn ${(_sessionTimeFilter[sessionId] || 'session') === 'day' ? 'active' : ''}" onclick="setSessionTimeFilter('${sessionId}', 'day')">Day (15m)</button>
                        <button class="time-filter-btn ${(_sessionTimeFilter[sessionId] || 'session') === 'week' ? 'active' : ''}" onclick="setSessionTimeFilter('${sessionId}', 'week')">Week (1d)</button>
                    </div>
                    <select id="paramSelect_${sessionId}" class="param-select" style="width:130px" onchange="onSessionParamChange('${sessionId}')">
                        <option value="Power">Power (W)</option>
                        <option value="Voltage">Voltage (V)</option>
                        <option value="Current">Current (A)</option>
                        <option value="Frequency">Frequency (Hz)</option>
                        <option value="Energy">Energy (kWh)</option>
                        <option value="PowerFactor">Power Factor</option>
                    </select>
                </div>
            </div>
            <div style="height:220px; position:relative; width:100%">
                <canvas id="chart_${sessionId}"></canvas>
            </div>
            <div id="sessionBatchNav_${sessionId}" style="display:none"></div>
        </div>

        <div class="session-dashboard-card" style="background:var(--surface); border:1px solid var(--border); border-radius:var(--radius-md) 0 var(--radius-md) 0; padding:16px;">
            <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:12px; flex-wrap:wrap; gap:8px;">
                <div>
                    <h5 style="margin:0; font-size:13px; font-weight:800; color:var(--text-primary)">Tabel Data Telemetri</h5>
                </div>
                <div style="display:flex; gap:6px; align-items:center">
                    <div id="tabs_${sessionId}" style="display:flex; gap:4px"></div>
                    <button onclick="event.stopPropagation(); startRenameSessionPhase('${sessionId}')" title="Ubah nama sensor" style="width:26px; height:26px; border-radius:4px; border:1px solid var(--border); background:transparent; color:var(--text-tertiary); cursor:pointer; display:flex; align-items:center; justify-content:center;">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="12" height="12"><path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"/><path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"/></svg>
                    </button>
                </div>
            </div>
            <div id="rename-phase-wrap_${sessionId}" style="display:none; margin-bottom:10px; padding:8px 12px; background:var(--surface-3); border-radius:4px; align-items:center; gap:8px;">
                <span style="font-size:11px; font-weight:700; color:var(--text-secondary)" id="rename-phase-label_${sessionId}">Sensor:</span>
                <input id="rename-phase-input_${sessionId}" type="text" style="flex:1; height:28px; font-size:12px; font-weight:600; padding:0 8px; border:1px solid var(--border); border-radius:4px; outline:none;">
                <button onclick="saveRenameSessionPhase('${sessionId}')" style="padding:4px 8px; font-size:11px; background:var(--green); color:white; border:none; border-radius:4px; cursor:pointer;">Simpan</button>
                <button onclick="cancelRenameSessionPhase('${sessionId}')" style="padding:4px 8px; font-size:11px; background:var(--border); color:var(--text-secondary); border:none; border-radius:4px; cursor:pointer;">Batal</button>
            </div>
            <div class="table-container" style="margin:0; border:none; box-shadow:none;">
                <table class="data-table inner-table" style="width:100%;">
                    <thead>
                        <tr>
                            <th>Timestamp</th>
                            <th style="text-align:right">Voltage (V)</th>
                            <th style="text-align:right">Current (A)</th>
                            <th style="text-align:right">Power (W)</th>
                            <th style="text-align:right">Frequency (Hz)</th>
                            <th style="text-align:right">Energy (kWh)</th>
                            <th style="text-align:right">PF</th>
                        </tr>
                    </thead>
                    <tbody id="tbody_${sessionId}"></tbody>
                </table>
                <div id="pag_${sessionId}" style="margin-top:10px"></div>
            </div>
        </div>`;
    }

    const tabsWrap = $(`tabs_${sessionId}`);
    if (tabsWrap) {
        tabsWrap.innerHTML = phases.map(p => {
            return `<button class="sensor-tab-btn ${p.phase === activePhase ? 'active' : ''}" onclick="event.stopPropagation(); switchSessionSensorTab('${sessionId}', '${p.phase}')">${p.name}</button>`;
        }).join('');
    }

    const paramSelect = $(`paramSelect_${sessionId}`);
    if (paramSelect) paramSelect.value = activeParam;

    renderSessionChart(sessionId);
    renderSessionTable(sessionId);
}

function switchSessionSensorTab(sessionId, phase) {
    _sessionSelectedPhase[sessionId] = phase;
    _sessionSelectedPage[sessionId] = 1;
    renderSessionDashboard(sessionId);
}

function onSessionParamChange(sessionId) {
    const dropdown = $(`paramSelect_${sessionId}`);
    if (dropdown) _sessionSelectedParam[sessionId] = dropdown.value;
    renderSessionChart(sessionId);
}

function renderSessionChart(sessionId) {
    const canvasId = `chart_${sessionId}`;
    const ctx = document.getElementById(canvasId);
    if (!ctx) return;

    if (_sessionCharts[sessionId]) {
        _sessionCharts[sessionId].destroy();
        delete _sessionCharts[sessionId];
    }

    const session = sessionsData[sessionId];
    if (!session) return;

    const phases = session.computedPhases || [];
    const activeParam = _sessionSelectedParam[sessionId] || 'Power';
    const filterMode = _sessionTimeFilter[sessionId] || 'session';

    const rawRecordsMap = recordsBySession[sessionId] || {};
    const { sortedTimestamps, recordsMap } = aggregateRecordsByTime(rawRecordsMap, phases, activeParam, filterMode);

    const totalSlots = sortedTimestamps.length;
    const batchSize = 3000;
    const totalPages = Math.ceil(totalSlots / batchSize) || 1;
    if (!_sessionSelectedPage[sessionId]) _sessionSelectedPage[sessionId] = totalPages;
    const currentPage = Math.min(Math.max(1, _sessionSelectedPage[sessionId]), totalPages);
    _sessionSelectedPage[sessionId] = currentPage;

    const startIdx = (currentPage - 1) * batchSize;
    const endIdx = startIdx + batchSize;
    const chartLabels = sortedTimestamps.slice(startIdx, endIdx);

    _renderBatchNav(`sessionBatchNav_${sessionId}`, currentPage, totalPages, startIdx + 1, Math.min(endIdx, totalSlots), totalSlots, (newPage) => {
        _sessionSelectedPage[sessionId] = newPage;
        renderSessionChart(sessionId);
    });

    const datasets = phases.map((p, idx) => {
        const records = recordsMap[p.phase] || [];
        const dataMap = {};
        records.forEach(r => {
            if (r.timestamp) dataMap[r.timestamp] = r;
        });

        const dataPoints = chartLabels.map(ts => {
            const rec = dataMap[ts];
            return rec ? (rec[activeParam] != null ? rec[activeParam] : 0) : null;
        });

        const phaseColors = ['#00A651', '#1E90FF', '#FF8C00', '#8A2BE2', '#FF1493', '#00CED1'];
        const color = phaseColors[idx % phaseColors.length];

        return {
            label: p.name,
            data: dataPoints,
            borderColor: color,
            backgroundColor: color + '0a',
            borderWidth: 1.5,
            pointRadius: chartLabels.length > 60 ? 0 : 2.5,
            fill: false,
            tension: 0.15,
            spanGaps: true
        };
    });

    _sessionCharts[sessionId] = new Chart(ctx, {
        type: 'line',
        data: {
            labels: chartLabels.map(ts => _formatTimestampForChart(ts)),
            datasets: datasets
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: true,
                    position: 'top',
                    labels: {
                        boxWidth: 12,
                        font: { family: 'var(--font-ui)', weight: 'bold', size: 10 },
                        color: 'var(--text-secondary)'
                    }
                },
                tooltip: {
                    enabled: false,
                    external: iSolarTooltipHandler,
                    mode: 'index',
                    intersect: false
                }
            },
            scales: {
                x: {
                    grid: { display: false },
                    ticks: {
                        maxTicksLimit: 8,
                        font: { family: 'var(--font-ui)', size: 9 },
                        color: 'var(--text-tertiary)'
                    }
                },
                y: {
                    grid: { color: 'rgba(0,0,0,0.03)' },
                    title: {
                        display: true,
                        text: activeParam,
                        font: { family: 'var(--font-ui)', weight: 'bold', size: 10 },
                        color: 'var(--text-secondary)'
                    },
                    ticks: {
                        font: { family: 'var(--font-ui)', size: 9 },
                        color: 'var(--text-tertiary)'
                    }
                }
            }
        }
    });
    _initChartGestures(_sessionCharts[sessionId]);
}

function renderSessionTable(sessionId) {
    const tbody = $(`tbody_${sessionId}`);
    const activePhase = _sessionSelectedPhase[sessionId];
    if (!tbody || !activePhase) return;

    const records = recordsBySession[sessionId]?.[activePhase] || [];
    const sorted = records.slice().sort((a, b) => parseTimestampToEpoch(b.timestamp) - parseTimestampToEpoch(a.timestamp));

    const pageSize = 20;
    const totalPages = Math.ceil(sorted.length / pageSize);
    const page = Math.max(1, Math.min(_sessionSelectedPage[sessionId] || 1, totalPages));
    _sessionSelectedPage[sessionId] = page;

    const slice = sorted.slice((page - 1) * pageSize, page * pageSize);

    if (!slice.length) {
        tbody.innerHTML = `<tr><td colspan="7" style="text-align:center;padding:16px;color:var(--text-tertiary)">Tidak ada data.</td></tr>`;
        $(`pag_${sessionId}`).innerHTML = '';
        return;
    }

    tbody.innerHTML = slice.map(r => {
        const isOff = r.offline;
        return `<tr class="inner-record-row ${isOff ? 'record-offline' : ''}">
            <td>${r.timestamp || '---'}</td>
            <td style="text-align:right">${r.Voltage != null ? r.Voltage.toFixed(2) : '---'}</td>
            <td style="text-align:right">${r.Current != null ? r.Current.toFixed(2) : '---'}</td>
            <td style="text-align:right; font-weight:700; color:${isOff ? 'inherit' : 'var(--brand)'}">${r.Power != null ? r.Power.toFixed(2) : '---'}</td>
            <td style="text-align:right">${r.Frequency != null ? r.Frequency.toFixed(1) : '---'}</td>
            <td style="text-align:right">${r.Energy != null ? r.Energy.toFixed(4) : '---'}</td>
            <td style="text-align:right">${r.PowerFactor != null ? r.PowerFactor.toFixed(4) : '---'}</td>
        </tr>`;
    }).join('');

    if (totalPages > 1) {
        $(`pag_${sessionId}`).innerHTML = _renderSessionPaginationBar(sessionId, page, totalPages, sorted.length);
    } else {
        $(`pag_${sessionId}`).innerHTML = '';
    }
}

function _renderSessionPaginationBar(sessionId, page, totalPages, totalCount) {
    return `<div style="display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:12px;">
        <span style="font-size:11px; color:var(--text-tertiary); font-weight:600">Menampilkan ${Math.min(totalCount, (page - 1) * 20 + 1)}-${Math.min(totalCount, page * 20)} dari ${totalCount} record</span>
        <div style="display:flex; gap:4px">
            <button class="time-filter-btn" ${page <= 1 ? 'disabled style="opacity:0.4;cursor:not-allowed"' : ''} onclick="goSessionTablePage('${sessionId}', ${page - 1})">Sebelumnya</button>
            <span style="display:flex; align-items:center; padding:0 8px; font-size:11.5px; font-weight:700; color:var(--text-secondary)">Halaman ${page} / ${totalPages}</span>
            <button class="time-filter-btn" ${page >= totalPages ? 'disabled style="opacity:0.4;cursor:not-allowed"' : ''} onclick="goSessionTablePage('${sessionId}', ${page + 1})">Berikutnya</button>
        </div>
    </div>`;
}

function goSessionTablePage(sessionId, page) {
    _sessionSelectedPage[sessionId] = page;
    renderSessionTable(sessionId);
}
(function _injectSphHoverStyle() {
    if (document.getElementById('sph-hover-style')) return;
    const s = document.createElement('style');
    s.id = 'sph-hover-style';
    s.textContent = `
        .session-phase-header:hover .sph-edit-btn {
            opacity: 1 !important;
            background: var(--blue-muted) !important;
            border-color: rgba(22,119,255,0.3) !important;
            color: var(--blue) !important;
        }
        .sph-edit-btn:hover {
            background: rgba(22,119,255,0.18) !important;
            border-color: var(--blue) !important;
        }
    `;
    document.head.appendChild(s);
})();
function startRenameSessionPhase(sessionId) {
    const activePhase = _sessionSelectedPhase[sessionId];
    if (!activePhase) return;
    
    const inputWrap = $(`rename-phase-wrap_${sessionId}`);
    const inputEl = $(`rename-phase-input_${sessionId}`);
    const labelEl = $(`rename-phase-label_${sessionId}`);
    if (!inputWrap || !inputEl || !labelEl) return;
    
    // Find current phase name from sessionsData
    const session = sessionsData[sessionId];
    const phaseNames = session?.phaseNames || {};
    const currentName = phaseNames[activePhase] || activePhase;
    
    labelEl.textContent = `Sensor ${activePhase}:`;
    inputEl.value = currentName;
    inputWrap.style.display = 'flex';
    setTimeout(() => { inputEl.focus(); inputEl.select(); }, 50);
}

function cancelRenameSessionPhase(sessionId) {
    const inputWrap = $(`rename-phase-wrap_${sessionId}`);
    if (inputWrap) inputWrap.style.display = 'none';
}

async function saveRenameSessionPhase(sessionId) {
    const activePhase = _sessionSelectedPhase[sessionId];
    if (!activePhase) return;
    
    const inputEl = $(`rename-phase-input_${sessionId}`);
    const newName = inputEl?.value.trim();
    if (!newName) { await showModal('Input Kosong', 'Nama sensor tidak boleh kosong.', 'warning'); return; }
    if (newName.length > 40) { await showModal('Terlalu Panjang', 'Nama maksimal 40 karakter.', 'warning'); return; }

    cancelRenameSessionPhase(sessionId);
    showGlobalLoader();
    
    try {
        const res = await fetch(`/api/capture/rename-session-sensor`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ sessionId: sessionId, phase: activePhase, name: newName })
        });
        const json = await res.json();
        if (!json.ok) throw new Error(json.error || 'Server error');
        
        // Update cache
        if (sessionsData[sessionId]) {
            if (!sessionsData[sessionId].phaseNames) sessionsData[sessionId].phaseNames = {};
            sessionsData[sessionId].phaseNames[activePhase] = newName;
        }
        
        // Re-render dashboard to show updated tab names
        renderSessionDashboard(sessionId);
        await showModal('Berhasil', `Nama sensor ${activePhase} pada sesi ini diubah menjadi:\n"${newName}"`, 'success');
    } catch (e) {
        await showModal('Gagal Mengubah Nama', 'Error: ' + e.message, 'error');
    } finally {
        hideGlobalLoader();
    }
}
function getDevicePhasesWithNames() {
    if (!selectedDeviceId) return [];
    const dev = _deviceListCache.find(d => d.id === selectedDeviceId);
    if (!dev?.phases?.length) return [];
    return dev.phases.filter(p => p.enabled !== false).map(p => ({ phase: p.phase, name: p.name }));
}
const COL_WIDTHS = [
    { wch: 20 }, { wch: 20 }, { wch: 10 }, { wch: 13 }, { wch: 13 }, { wch: 13 }, { wch: 15 }, { wch: 20 }, { wch: 15 }
];
function _buildExcelRow(entry, deviceName) {
    const row = {};
    row['Device Name'] = deviceName;
    row['Timestamp'] = entry.timestamp ?? '';
    row['Status'] = entry.offline ? 'OFFLINE' : 'online';
    row['Voltage (V)'] = entry.Voltage != null ? +entry.Voltage.toFixed(2) : '';
    row['Current (A)'] = entry.Current != null ? +entry.Current.toFixed(2) : '';
    row['Power (W)'] = entry.Power != null ? +entry.Power.toFixed(2) : '';
    row['Frequency (Hz)'] = entry.Frequency != null ? +entry.Frequency.toFixed(1) : '';
    row['Active Energy (kWh)'] = entry.Energy != null ? +entry.Energy.toFixed(4) : '';
    row['Power Factor'] = entry.PowerFactor != null ? +entry.PowerFactor.toFixed(4) : '';
    return row;
}
async function exportSession(sessionId, sessionName, event) {
    event.stopPropagation();
    
    const btn = event.currentTarget;
    if (!btn || btn.classList.contains('loading')) return;
    
    btn.classList.add('loading');
    btn.disabled = true;
    const originalHTML = btn.innerHTML;
    
    // Set rotating spinner SVG
    btn.innerHTML = `<svg class="spinner-icon rotate-spin" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3" style="width:13px;height:13px;display:inline-block;vertical-align:middle;"><circle cx="12" cy="12" r="10" stroke-dasharray="30 10"></circle></svg>`;

    try {
        // Ensure all phase data is loaded before exporting
        const sessionMeta = sessionsData[sessionId];
        if (sessionMeta) {
            const phaseNames = sessionMeta.phaseNames || {};
            const phases = Object.keys(phaseNames).filter(k => /^L\d+$/.test(k));
            if (!recordsBySession[sessionId]) recordsBySession[sessionId] = {};

            let needsFetch = false;
            for (const ph of phases) {
                if (!recordsBySession[sessionId][ph]) {
                    needsFetch = true;
                    break;
                }
            }

            if (needsFetch) {
                await Promise.all(phases.map(async (ph) => {
                    if (!recordsBySession[sessionId][ph]) {
                        const res = await fetch(`/api/devices/${selectedDeviceId}/history/${sessionId}/${ph}`);
                        if (!res.ok) throw new Error(`HTTP ${res.status} gagal memuat data sensor ${ph}`);
                        const historyMap = await res.json();
                        const arr = [];
                        Object.entries(historyMap).forEach(([key, val]) => {
                            if (key !== '_meta') arr.push(val);
                        });
                        recordsBySession[sessionId][ph] = arr;
                    }
                }));
                buildSessionUI(); // Refresh UI to show the fetched records
            }
        }

        const phaseData = recordsBySession[sessionId] || {};
        const phaseKeys = Object.keys(phaseData).filter(k => /^L\d+$/.test(k)).sort((a, b) => parseInt(a.slice(1)) - parseInt(b.slice(1)));
        const totalRecords = phaseKeys.reduce((s, ph) => s + (phaseData[ph]?.length || 0), 0);
        if (!phaseKeys.length || totalRecords === 0) { 
            await showModal('Tidak Ada Data', `Sesi "${sessionName}" belum memiliki record.`, 'warning'); 
            return; 
        }

        const confirmed = await showModal('Ekspor Data Sesi ke Excel',
            `Unduh ${totalRecords.toLocaleString('id-ID')} record (${phaseKeys.length} sensor) dari sesi:\n"${sessionName}"?\n\nProses ini hanya mengunduh berkas Excel dan tidak akan mengubah atau menghapus data di database.`, 'info', ['confirm']);
        if (!confirmed) return;

        const session = sessionsData[sessionId];
        const deviceName = session?.deviceName || _deviceListCache.find(d => d.id === (session?.deviceId || selectedDeviceId))?.name || selectedDeviceId;
        const wb = XLSX.utils.book_new();
        const frozenNames = session.phaseNames || {};
        const devObj = _deviceListCache.find(d => d.id === (session?.deviceId || selectedDeviceId));
        for (const phase of phaseKeys) {
            const phaseRecs = (phaseData[phase] || []).slice().sort(sortByEpochAsc);
            const cachedName = devObj?.phases?.find(p => p.phase === phase)?.name;
            const phaseDevName = (frozenNames[phase] && frozenNames[phase] !== phase) ? frozenNames[phase] : (cachedName || phase);
            const ws = XLSX.utils.json_to_sheet(phaseRecs.map(e => _buildExcelRow(e, deviceName)));
            ws['!cols'] = COL_WIDTHS;
            XLSX.utils.book_append_sheet(wb, ws, phaseDevName);
        }
        const allRecords = Object.values(phaseData).flat();
        const onlineRows = allRecords.filter(e => !e.offline);
        const avg = f => onlineRows.length ? onlineRows.reduce((s, e) => s + (e[f] || 0), 0) / onlineRows.length : 0;
        const sum = f => onlineRows.reduce((s, e) => s + (e[f] || 0), 0);
        const wsMeta = XLSX.utils.aoa_to_sheet([
            ['Smart Energy Monitor - Session Export'], [''],
            ['Nama Sesi', sessionName], ['Export Date', new Date().toLocaleString('id-ID')],
            ['Device Name', deviceName], ['Sensors', phaseKeys.map(ph => {
                const cachedName = devObj?.phases?.find(p => p.phase === ph)?.name;
                return (frozenNames[ph] && frozenNames[ph] !== ph) ? frozenNames[ph] : (cachedName || ph);
            }).join(', ')],
            ['Waktu Mulai', session?.startTime || '---'], ['Waktu Selesai', session?.endTime || 'Berlangsung'],
            ['Total Records', totalRecords], ['Records Online', onlineRows.length], ['Records Offline', allRecords.length - onlineRows.length], [''],
            ['Summary Statistics (semua sensor, online saja)'], [''],
            ['Parameter', 'Rata-rata', 'Satuan'],
            ['Voltage', avg('Voltage').toFixed(2), 'V'],
            ['Current', avg('Current').toFixed(2), 'A'],
            ['Power', avg('Power').toFixed(2), 'W'],
            ['Frequency', avg('Frequency').toFixed(1), 'Hz'],
            ['Power Factor', avg('PowerFactor').toFixed(4), ''],
            ['Total Active Energy', sum('Energy').toFixed(4), 'kWh'],
        ]);
        wsMeta['!cols'] = [{ wch: 28 }, { wch: 28 }, { wch: 10 }];
        XLSX.utils.book_append_sheet(wb, wsMeta, 'Summary');
        XLSX.writeFile(wb, `${sessionName.replace(/[\\/:*?"<>|]/g, '_')}.xlsx`);
        await showModal('Export Berhasil!', `${totalRecords} record dari "${sessionName}" berhasil diekspor.`, 'success');
    } catch (e) {
        await showModal('Export Gagal', 'Error: ' + e.message, 'error');
    } finally {
        btn.classList.remove('loading');
        btn.disabled = false;
        btn.innerHTML = originalHTML;
    }
}

async function backupSessionJSON(sessionId, sessionName, event) {
    event.stopPropagation();
    
    // Close dropdowns
    document.querySelectorAll('.session-dropdown-menu').forEach(el => el.style.display = 'none');

    try {
        // Ensure all phase data is loaded before backing up
        const sessionMeta = sessionsData[sessionId];
        if (sessionMeta) {
            const phaseNames = sessionMeta.phaseNames || {};
            const phases = Object.keys(phaseNames).filter(k => /^L\d+$/.test(k));
            if (!recordsBySession[sessionId]) recordsBySession[sessionId] = {};

            let needsFetch = false;
            for (const ph of phases) {
                if (!recordsBySession[sessionId][ph]) {
                    needsFetch = true;
                    break;
                }
            }

            if (needsFetch) {
                showGlobalLoader();
                await Promise.all(phases.map(async (ph) => {
                    if (!recordsBySession[sessionId][ph]) {
                        const res = await fetch(`/api/devices/${selectedDeviceId}/history/${sessionId}/${ph}`);
                        if (!res.ok) throw new Error(`HTTP ${res.status} gagal memuat data sensor ${ph}`);
                        const historyMap = await res.json();
                        const arr = [];
                        Object.entries(historyMap).forEach(([key, val]) => {
                            if (key !== '_meta') arr.push(val);
                        });
                        recordsBySession[sessionId][ph] = arr;
                    }
                }));
                hideGlobalLoader();
                buildSessionUI();
            }
        }

        const phaseData = recordsBySession[sessionId] || {};
        const phaseKeys = Object.keys(phaseData).filter(k => /^L\d+$/.test(k)).sort((a, b) => parseInt(a.slice(1)) - parseInt(b.slice(1)));
        const totalRecords = phaseKeys.reduce((s, ph) => s + (phaseData[ph]?.length || 0), 0);
        if (!phaseKeys.length || totalRecords === 0) { 
            await showModal('Tidak Ada Data', `Sesi "${sessionName}" belum memiliki record.`, 'warning'); 
            return; 
        }

        const confirmed = await showModal('Unduh Backup JSON Sesi',
            `Unduh berkas cadangan ${totalRecords.toLocaleString('id-ID')} record (${phaseKeys.length} sensor) dari sesi:\n"${sessionName}"?\n\nBerkas ini dapat Anda simpan dan dibuka kembali kapan saja lewat menu Visualisasi Data.`, 'info', ['confirm']);
        if (!confirmed) return;

        // Construct standard backup schema
        const backupPayload = {
            schema: "smart-energy-meter-backup",
            version: "1.0",
            exportedAt: new Date().toISOString(),
            session: {
                id: sessionId,
                name: sessionName,
                deviceId: sessionMeta?.deviceId || selectedDeviceId,
                deviceName: sessionMeta?.deviceName || selectedDeviceName,
                startTime: sessionMeta?.startTime || '---',
                endTime: sessionMeta?.endTime || '---',
                recordCount: totalRecords,
                phaseNames: sessionMeta?.phaseNames || {}
            },
            records: phaseData
        };

        const jsonString = JSON.stringify(backupPayload, null, 2);
        const blob = new Blob([jsonString], { type: "application/json" });
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = `${sessionName.replace(/[\\/:*?"<>|]/g, '_')}_backup.json`;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);

        await showModal('Backup Berhasil!', `File backup JSON berhasil diunduh.`, 'success');
    } catch (e) {
        hideGlobalLoader();
        await showModal('Backup Gagal', 'Error: ' + e.message, 'error');
    }
}



async function clearRecords() {
    if (!historyData.length && !Object.keys(sessionsData).length) { await showModal('Tidak Ada Data', 'Tidak ada data histori yang perlu dihapus.', 'info'); return; }
    const confirmed = await showModal('Hapus Semua Rekaman Device?', `Apakah Anda yakin ingin menghapus SEMUA sesi & data rekaman device:\n"${selectedDeviceName}"?\n\nData yang telah dihapus TIDAK DAPAT dikembalikan.`, 'warning', ['confirm']);
    if (!confirmed) return;
    try {
        const res = await fetch('/api/capture/clear-all', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ deviceId: selectedDeviceId })
        });
        const json = await res.json();
        if (!json.ok) throw new Error(json.error || 'Server error');

        historyData = []; recordsBySession = {}; sessionsData = {};
        buildSessionUI();
        await showModal('Berhasil Dihapus', 'Semua data rekaman telah dihapus.', 'success');
    } catch (e) { await showModal('Error', 'Gagal menghapus! Error: ' + e.message, 'error'); }
}
let captureActive = false;
let captureInterval = 15000;
let _captureTransitioning = false;
let _captureStatusPollId = null;
let _intervalUserEdited = false;
let _lastHistoryRefresh = 0;
const _HISTORY_REFRESH_MS = 8000; // refresh DB tiap 8 detik saat capture aktif

async function _refreshActiveSessionRecords() {
    if (!captureActive || !currentSessionId || !selectedDeviceId) return;
    const dev = _deviceListCache.find(d => d.id === selectedDeviceId);
    if (!dev) return;
    const enabledPhases = dev.phases ? dev.phases.filter(p => p.enabled !== false).map(p => p.phase) : ['L1', 'L2', 'L3', 'L4', 'L5'];
    
    const openPhasesOfActiveSession = [];
    enabledPhases.forEach(ph => {
        const el = document.getElementById(`phase-detail_${currentSessionId}_${ph}`);
        if (el && el.style.display !== 'none') {
            openPhasesOfActiveSession.push(ph);
        }
    });
    
    if (openPhasesOfActiveSession.length === 0) return;
    
    try {
        await Promise.all(openPhasesOfActiveSession.map(async (phase) => {
            const res = await fetch(`/api/devices/${selectedDeviceId}/history/${currentSessionId}/${phase}`);
            const historyMap = await res.json();
            if (!recordsBySession[currentSessionId]) recordsBySession[currentSessionId] = {};
            const arr = [];
            Object.entries(historyMap).forEach(([key, val]) => {
                if (key !== '_meta') arr.push(val);
            });
            recordsBySession[currentSessionId][phase] = arr;

            const tbody = document.getElementById(`inner-tbody_${currentSessionId}_${phase}`);
            if (tbody) {
                const pr = arr.slice().sort(sortByEpochDesc);
                tbody.innerHTML = pr.length ? pr.map(e => {
                    const pfColor = e.offline ? '#9CA3AF' : (e.PowerFactor >= 0.85 ? '#00A651' : '#ED1C24');
                    const offTag = e.offline ? ' <span style="color:#9CA3AF;font-size:9px;font-weight:700">[offline]</span>' : '';
                    return '<tr class="inner-record-row"' + (e.offline ? ' style="opacity:0.5;font-style:italic"' : '') + '>'
                        + '<td>' + e.timestamp + offTag + '</td>'
                        + '<td>' + (e.Voltage != null ? e.Voltage.toFixed(2) : '---') + '</td>'
                        + '<td>' + (e.Current != null ? e.Current.toFixed(2) : '---') + '</td>'
                        + '<td>' + (e.Power != null ? e.Power.toFixed(2) : '---') + '</td>'
                        + '<td>' + (e.Frequency != null ? e.Frequency.toFixed(2) : '---') + '</td>'
                        + '<td>' + (e.Energy != null ? e.Energy.toFixed(3) : '---') + '</td>'
                        + '<td style="color:' + pfColor + '">' + (e.PowerFactor != null ? e.PowerFactor.toFixed(3) : '---') + '</td>'
                        + '</tr>';
                }).join('') : '';

            }
        }));
    } catch (e) {
        console.error("Error auto-refreshing active session records:", e);
    }
}

async function syncCaptureStatus() {
    try {
        const devParam = selectedDeviceId ? `?deviceId=${encodeURIComponent(selectedDeviceId)}` : '';
        const status = await fetch(`/api/capture/status${devParam}`).then(r => r.json());
        
        // Status khusus untuk device yang sedang dipilih di UI
        const currentDevStatus = (status.devices && selectedDeviceId && status.devices[selectedDeviceId])
            ? status.devices[selectedDeviceId]
            : status;

        const isDevActive = Boolean(currentDevStatus && currentDevStatus.active && currentDevStatus.device_id === selectedDeviceId);

        if (!_captureTransitioning) {
            if (isDevActive) {
                captureActive = true;
                currentSessionId = currentDevStatus.session_id || null;
                _updateCaptureButtonUI(true);

                // Tampilkan sesi aktif SEGERA tanpa menunggu record pertama masuk DB
                if (currentSessionId && selectedDeviceId) {
                    if (!sessionsData[currentSessionId]) {
                        const dev = _deviceListCache.find(d => d.id === selectedDeviceId);
                        const devPhases = dev && dev.phases 
                            ? dev.phases.filter(p => p.enabled !== false).map(p => p.phase)
                            : ['L1', 'L2', 'L3', 'L4', 'L5'];
                        // Sesi baru: inject langsung ke sessionsData agar muncul di tabel
                        sessionsData[currentSessionId] = {
                            id: currentSessionId,
                            name: currentDevStatus.session_name || 'Rekaman',
                            startTime: currentDevStatus.started_at || '---',
                            endTime: null,
                            recordCount: 0,
                            startTimestamp: Date.now(),
                            deviceId: selectedDeviceId,
                            deviceName: selectedDeviceName || selectedDeviceId,
                            phases: devPhases,
                            phaseNames: {},
                        };
                    }
                    // Update record count dari status (akurat tanpa perlu query DB)
                    if (sessionsData[currentSessionId]) {
                        sessionsData[currentSessionId].recordCount = currentDevStatus.count || sessionsData[currentSessionId].recordCount;
                    }
                }
            } else if (!currentDevStatus.finalizing) {
                const wasActive = captureActive;
                captureActive = false;
                currentSessionId = null;
                _updateCaptureButtonUI(false);
                if (wasActive && selectedDeviceId) {
                    _attachHistoryListener(selectedDeviceId);
                }
            }
        }
        if (!_captureTransitioning && !_intervalUserEdited) {
            const serverSec = currentDevStatus.interval || 15;
            captureInterval = serverSec * 1000;
            const inputEl = $('intervalInput'), unitEl = $('intervalUnit');
            if (inputEl && unitEl) {
                inputEl.value = serverSec;
                unitEl.value = '1';
            }
            if (DOM.intervalDisplay)
                DOM.intervalDisplay.textContent = `Current: ${serverSec} seconds`;
        }
        // Refresh dari DB setiap 8 detik (untuk phases & count akurat)
        const now = Date.now();
        if (captureActive && selectedDeviceId) {
            if (now - _lastHistoryRefresh > _HISTORY_REFRESH_MS) {
                _lastHistoryRefresh = now;
                await _attachHistoryListener(selectedDeviceId, true);
            } else {
                buildSessionUI(true); // render ulang halus dengan isAutoPoll = true
            }
        }
        const recBadge = $('recordingBadge');
        const recInfo = $('recBadgeInfo');
        const activeDevIds = status.active_device_ids || (status.active && status.device_id ? [status.device_id] : []);

        if (recBadge) {
            if (activeDevIds.length > 0) {
                recBadge.style.display = 'flex';
                if (recInfo) {
                    if (isDevActive) {
                        const ivStr = currentDevStatus.interval ? `${currentDevStatus.interval}s` : '15s';
                        const cntStr = currentDevStatus.count != null ? `${currentDevStatus.count} recs` : '';
                        const sName = currentDevStatus.session_name || 'Rekaman';
                        recInfo.textContent = `${sName} (${ivStr} · ${cntStr})`;
                    } else {
                        const activeDevName = (status.devices && status.devices[activeDevIds[0]])
                            ? (status.devices[activeDevIds[0]].device_name || activeDevIds[0])
                            : activeDevIds[0];
                        recInfo.textContent = `Background: ${activeDevName} (${activeDevIds.length} aktif)`;
                    }
                }
            } else {
                recBadge.style.display = 'none';
            }
        }
    } catch (e) { }
}

async function quickStopCapture() {
    const confirmed = await showModal('Hentikan Rekaman Sesi?', 'Apakah Anda yakin ingin menghentikan sesi rekaman yang sedang berlangsung?\n\nData yang sudah terekam tetap tersimpan aman di database.', 'warning', ['confirm']);
    if (confirmed) await _apiStopCapture();
}
function _startStatusPolling() {
    if (_captureStatusPollId) return;
    _captureStatusPollId = setInterval(syncCaptureStatus, 4000);
}
const CAPTURE_START_HTML = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"></circle><polygon points="10 8 16 12 10 16 10 8"></polygon></svg> Start Capture`;
const CAPTURE_STOP_HTML = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="6" y="4" width="4" height="16"></rect><rect x="14" y="4" width="4" height="16"></rect></svg> Stop Capture`;
function _updateCaptureButtonUI(active) {
    const btn = DOM.captureBtn;
    if (!btn) return;
    btn.classList.toggle('active', active);
    btn.innerHTML = active ? CAPTURE_STOP_HTML : CAPTURE_START_HTML;

    const clearBtn = document.querySelector('.btn-reset');
    const intervalInput = document.getElementById('intervalInput');
    const intervalUnit = document.getElementById('intervalUnit');
    const intervalSetBtn = document.querySelector('.btn-set');

    if (clearBtn) clearBtn.disabled = active;
    if (intervalInput) intervalInput.disabled = active;
    if (intervalUnit) intervalUnit.disabled = active;
    if (intervalSetBtn) intervalSetBtn.disabled = active;
}
async function toggleCapture() {
    if (!captureActive) {
        if (!selectedDeviceId) { await showModal('Pilih Device', 'Pilih device terlebih dahulu.', 'warning'); return; }
        if (!isConnected) { await showModal('Device Offline', 'Tidak dapat memulai capture.\nPastikan device menyala.', 'error'); return; }
        openSessionNameModal();
    } else {
        const confirmed = await showModal('Hentikan Rekaman Sesi?', 'Apakah Anda yakin ingin menghentikan sesi rekaman yang sedang berlangsung?\n\nData yang sudah terekam tetap tersimpan aman di database.', 'warning', ['confirm']);
        if (confirmed) await _apiStopCapture();
    }
}
async function _apiStopCapture() {
    captureActive = false;
    currentSessionId = null;
    _captureTransitioning = true;
    _updateCaptureButtonUI(false);
    buildSessionUI();
    try {
        const json = await fetch('/api/capture/stop', { method: 'POST' })
            .then(r => r.json());
        if (!json.ok) {
            await showModal('Error', 'Gagal menghentikan: ' + json.error, 'error');
        }
        if (selectedDeviceId) {
            await _attachHistoryListener(selectedDeviceId);
        }
    } catch (e) {
        await showModal('Error', 'Network error: ' + e.message, 'error');
    } finally {
        _captureTransitioning = false;
    }
}
function openSessionNameModal() {
    const input = $('sessionNameInput');
    const now = new Date(), pad = v => String(v).padStart(2, '0');
    input.value = `Rekaman ${pad(now.getDate())}/${pad(now.getMonth() + 1)}/${now.getFullYear()} ${pad(now.getHours())}:${pad(now.getMinutes())}`;
    _resetSessionModal();
    $('sessionNameModal').classList.add('active');
    document.body.style.overflow = 'hidden';
    setTimeout(() => { input.focus(); input.select(); }, 120);
}
function closeSessionNameModal() {
    $('sessionNameModal').classList.remove('active');
    document.body.style.overflow = '';
    _renamingSessionId = null;
    _resetSessionModal();
}
function _resetSessionModal() {
    const modal = $('sessionNameModal');
    if (!modal) return;
    modal.querySelector('.modal-title').textContent = 'Mulai Rekaman Baru';
    modal.querySelector('.modal-message').textContent = 'Beri nama sesi rekaman ini sebelum memulai.';
    const btn = modal.querySelector('.modal-btn-primary');
    btn.innerHTML = '&#9654; MULAI REKAM'; btn.onclick = confirmStartCapture;
}
function openRenameModal(sessionId, currentName, event) {
    event.stopPropagation();
    _renamingSessionId = sessionId;
    const modal = $('sessionNameModal');
    modal.querySelector('.modal-title').textContent = 'Rename Sesi';
    modal.querySelector('.modal-message').textContent = 'Ubah nama sesi rekaman ini.';
    const btn = modal.querySelector('.modal-btn-primary');
    btn.innerHTML = 'SIMPAN NAMA'; btn.onclick = confirmRenameSession;
    $('sessionNameInput').value = currentName;
    modal.classList.add('active');
    document.body.style.overflow = 'hidden';
    setTimeout(() => { $('sessionNameInput').focus(); $('sessionNameInput').select(); }, 120);
}
async function confirmRenameSession() {
    const newName = $('sessionNameInput')?.value.trim();
    const targetId = _renamingSessionId;
    if (!newName) { await showModal('Nama Kosong', 'Nama sesi tidak boleh kosong.', 'warning'); return; }
    closeSessionNameModal();
    if (!targetId) return;
    try {
        const res = await fetch('/api/capture/rename-session', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ sessionId: targetId, name: newName })
        });
        const json = await res.json();
        if (!json.ok) throw new Error(json.error || 'Server error');

        if (sessionsData[targetId]) sessionsData[targetId].name = newName;
        buildSessionUI();
        await showModal('Berhasil', `Nama sesi diubah menjadi:\n"${newName}"`, 'success');
    } catch (e) { await showModal('Error', 'Gagal mengubah nama! Error: ' + e.message, 'error'); }
}
async function confirmStartCapture() {
    const sessionName = $('sessionNameInput')?.value.trim()
        || `Rekaman ${new Date().toLocaleTimeString('id-ID')}`;
    const intervalSec = Math.round(captureInterval / 1000) || 3;
    closeSessionNameModal();
    const activeDev = _deviceListCache.find(d => d.id === selectedDeviceId);
    const phasesHint = (activeDev?.phases || []).filter(p => p.enabled !== false).map(p => p.phase);
    captureActive = true;
    currentSessionId = null;
    _captureTransitioning = true;
    _updateCaptureButtonUI(true);
    buildSessionUI();
    showModal(
        'Capture Diaktifkan',
        `Sesi: "${sessionName}"\nDevice: ${selectedDeviceName}\n\nRekaman berjalan di server.\nInterval: ${intervalSec}s`,
        'success'
    );
    try {
        const json = await fetch('/api/capture/start', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                sessionName,
                interval: intervalSec,
                deviceId: selectedDeviceId,
                deviceName: selectedDeviceName,
                phases: phasesHint,
            }),
        }).then(r => r.json());
        if (!json.ok) {
            captureActive = false;
            currentSessionId = null;
            _updateCaptureButtonUI(false);
            closeModal();
            buildSessionUI();
            await showModal('Error', 'Gagal memulai capture: ' + (json.error || ''), 'error');
            return;
        }
        currentSessionId = json.session_id;
        buildSessionUI();
    } catch (e) {
        captureActive = false;
        currentSessionId = null;
        _updateCaptureButtonUI(false);
        closeModal();
        buildSessionUI();
        await showModal('Error', 'Network error: ' + e.message, 'error');
    } finally {
        _captureTransitioning = false;
    }
}

let _timeEditSessionId = null;

function openChangeTimeModal(sessionId, currentStartTime, sessionName, event) {
    if (event) event.stopPropagation();
    _timeEditSessionId = sessionId;
    $('oldTimeInput').value = currentStartTime || '---';
    $('newTimeInput').value = currentStartTime || '';
    $('changeTimeModal').classList.add('active');
    document.body.style.overflow = 'hidden';
}

function closeChangeTimeModal() {
    _timeEditSessionId = null;
    $('changeTimeModal').classList.remove('active');
    document.body.style.overflow = '';
}

async function confirmChangeTime() {
    const sessionId = _timeEditSessionId;
    if (!sessionId) return;
    const oldTimeStr = $('oldTimeInput').value;
    const newTimeStr = $('newTimeInput').value.trim();
    if (!newTimeStr) {
        showModal('Error', 'Waktu baru tidak boleh kosong', 'warning');
        return;
    }
    if (oldTimeStr === newTimeStr) {
        closeChangeTimeModal();
        return;
    }

    const formatRegex = /^\d{2}:\d{2}:\d{2} \d{2}\/\d{2}\/\d{4}$/;
    if (!formatRegex.test(newTimeStr)) {
        closeChangeTimeModal();
        showModal('Format Tidak Valid', 'Mohon masukkan waktu sesuai format:\nHH:MM:SS DD/MM/YYYY\nContoh: 14:30:00 08/04/2026', 'warning');
        return;
    }

    const oldDate = parseTimestamp(oldTimeStr);
    const newDate = parseTimestamp(newTimeStr);
    if (isNaN(oldDate.getTime()) || isNaN(newDate.getTime())) {
        closeChangeTimeModal();
        showModal('Error', 'Format waktu tidak valid! Gunakan: HH:MM:SS DD/MM/YYYY', 'warning');
        return;
    }

    const deltaMs = newDate.getTime() - oldDate.getTime();

    try {
        closeChangeTimeModal();
        showGlobalLoader();

        const res = await fetch('/api/capture/shift_time', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ sessionId, deltaMs })
        });
        const json = await res.json();
        if (!json.ok) throw new Error(json.error || 'Server error');

        await _attachHistoryListener(selectedDeviceId);

        hideGlobalLoader();
        showModal('Sukses', 'Waktu sesi berhasil diubah dan disinkronkan.', 'success');
    } catch (e) {
        hideGlobalLoader();
        showModal('Error', 'Gagal mengubah waktu! Error: ' + e.message, 'error');
    }
}

function toggleSessionDropdown(sessionId, event) {
    event.stopPropagation();
    document.querySelectorAll('.session-dropdown-menu').forEach(el => {
        if (el.id !== `dropdown_${sessionId}`) el.style.display = 'none';
    });
    const dropdown = document.getElementById(`dropdown_${sessionId}`);
    if (dropdown) {
        const isShown = dropdown.style.display === 'block';
        dropdown.style.display = isShown ? 'none' : 'block';
    }
}

async function deleteSession(sessionId, sessionName, event) {
    event.stopPropagation();
    
    // Close dropdowns
    document.querySelectorAll('.session-dropdown-menu').forEach(el => el.style.display = 'none');

    const session = sessionsData[sessionId];
    const isOffline = session && session.isOfflineBackup;

    const confirmed = await showModal('Hapus Sesi Rekaman?', `Apakah Anda yakin ingin menghapus sesi:\n"${sessionName}"?\n\nSeluruh data pengukuran dalam sesi ini akan dihapus secara permanen.`, 'warning', ['confirm']);
    if (!confirmed) return;

    if (isOffline) {
        delete sessionsData[sessionId];
        delete recordsBySession[sessionId];
        buildSessionUI();
        await showModal('Sesi Dihapus', `Sesi backup "${sessionName}" berhasil dihapus dari memori.`, 'success');
        return;
    }

    try {
        const res = await fetch('/api/capture/delete-session', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ sessionId })
        });
        const json = await res.json();
        if (!json.ok) throw new Error(json.error || 'Server error');

        delete sessionsData[sessionId]; delete recordsBySession[sessionId];
        await _attachHistoryListener(selectedDeviceId);
        await showModal('Sesi Dihapus', `Sesi "${sessionName}" berhasil dihapus.`, 'success');
    } catch (e) { await showModal('Error', 'Gagal menghapus! Error: ' + e.message, 'error'); }
}

async function setCaptureInterval() {
    const val = parseInt($('intervalInput')?.value);
    const multiplier = parseInt($('intervalUnit')?.value);
    const totalSec = val * multiplier;
    if (isNaN(totalSec) || totalSec < 15) { await showModal('Input Tidak Valid', 'Interval minimal adalah 15 detik!', 'warning'); return; }
    captureInterval = totalSec * 1000;
    _intervalUserEdited = true;
    const unitLabel = $('intervalUnit')?.options[$('intervalUnit').selectedIndex]?.text.toLowerCase() || 'seconds';
    if (DOM.intervalDisplay) DOM.intervalDisplay.textContent = multiplier === 1 ? `Current: ${val} seconds` : `Current: ${val} ${unitLabel} (${totalSec}s)`;
    try {
        await fetch('/api/capture/interval', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ interval: totalSec }),
        });
    } catch (e) { }
    await showModal('Interval Diperbarui', `Interval diubah menjadi ${val} ${unitLabel}.`, 'success');
}

// ==========================================
// OFFLINE VISUALIZER LOGIC (TOOLS TAB)
// ==========================================

function initOfflineDropZone() {
    const dz = $('offlineDropZone');
    if (!dz) return;
    
    ['dragenter', 'dragover'].forEach(eventName => {
        dz.addEventListener(eventName, (e) => {
            e.preventDefault();
            e.stopPropagation();
            dz.style.borderColor = 'var(--brand)';
            dz.style.background = 'color-mix(in srgb, var(--brand) 2%, var(--surface))';
        }, false);
    });
    
    ['dragleave', 'drop'].forEach(eventName => {
        dz.addEventListener(eventName, (e) => {
            e.preventDefault();
            e.stopPropagation();
            dz.style.borderColor = 'var(--border)';
            dz.style.background = 'var(--surface)';
        }, false);
    });
    
    dz.addEventListener('drop', (e) => {
        const dt = e.dataTransfer;
        const file = dt.files[0];
        if (file) {
            handleOfflineFile(file);
        }
    }, false);
}

async function handleOfflineUpload(event) {
    const file = event.target.files[0];
    if (file) {
        handleOfflineFile(file);
    }
    event.target.value = '';
}

function parseTimestampToEpoch(ts) {
    if (!ts) return Date.now();
    try {
        const d = parseTimestampToDate(ts);
        return d.getTime();
    } catch (e) {
        return Date.now();
    }
}

async function handleOfflineFile(file) {
    showGlobalLoader();
    const isXlsx = file.name.endsWith('.xlsx');
    const isJson = file.name.endsWith('.json');
    
    if (!isXlsx && !isJson) {
        hideGlobalLoader();
        await showModal('Format Tidak Didukung', 'Hanya berkas berekstensi .xlsx atau .json yang didukung.', 'warning');
        return;
    }
    
    if (isJson) {
        const reader = new FileReader();
        reader.onload = async function(e) {
            try {
                const data = JSON.parse(e.target.result);
                if (data.schema !== 'smart-energy-meter-backup') {
                    throw new Error("Format file JSON bukan merupakan backup Smart Energy Meter yang valid.");
                }
                
                const session = data.session;
                const records = data.records;
                
                if (!session || !session.id || !records) {
                    throw new Error("File backup kekurangan data sesi atau data telemetri.");
                }
                
                // Construct standard UI records where keys are capitalized
                const cleanRecords = {};
                Object.entries(records).forEach(([ph, arr]) => {
                    cleanRecords[ph] = arr.map(r => ({
                        timestamp: r.timestamp || '',
                        epoch: r.epoch || (r.timestamp ? parseTimestampToEpoch(r.timestamp) : Date.now()),
                        offline: !!r.offline,
                        Voltage: r.Voltage != null ? +r.Voltage : (r.voltage != null ? +r.voltage : 0),
                        Current: r.Current != null ? +r.Current : (r.current != null ? +r.current : 0),
                        Power: r.Power != null ? +r.Power : (r.power != null ? +r.power : 0),
                        Frequency: r.Frequency != null ? +r.Frequency : (r.frequency != null ? +r.frequency : 0),
                        Energy: r.Energy != null ? +r.Energy : (r.energy != null ? +r.energy : 0),
                        PowerFactor: r.PowerFactor != null ? +r.PowerFactor : (r.powerFactor != null ? +r.powerFactor : (r.Power_Factor != null ? +r.Power_Factor : 1))
                    }));
                });

                if (!session.phases) {
                    session.phases = Object.keys(cleanRecords).filter(k => /^L\d+$/.test(k) || k !== 'Summary');
                }
                
                activeOfflineSessionData = {
                    session: session,
                    records: cleanRecords
                };
                
                hideGlobalLoader();
                await showModal('Visualisasi Berhasil', `Berkas cadangan JSON sesi "${session.name}" berhasil dimuat ke tampilan pratinjau.`, 'success');
                renderOfflineWorkspace();
            } catch (err) {
                hideGlobalLoader();
                await showModal('Pemuatan Gagal', 'Error: ' + err.message, 'error');
            }
        };
        reader.readAsText(file);
    } else {
        const reader = new FileReader();
        reader.onload = async function(e) {
            try {
                const data = new Uint8Array(e.target.result);
                const workbook = XLSX.read(data, { type: 'array' });
                
                let sessionName = file.name.replace('.xlsx', '');
                let deviceId = 'OFFLINE_DEV';
                let deviceName = 'Offline Device';
                let startTime = '---';
                let endTime = '---';
                const records = {};
                
                // Parse metadata sheet if available
                if (workbook.SheetNames.includes('Summary')) {
                    const summarySheet = workbook.Sheets['Summary'];
                    const summaryRows = XLSX.utils.sheet_to_json(summarySheet, { header: 1 });
                    summaryRows.forEach(row => {
                        if (row && row.length >= 2) {
                            const key = String(row[0]).trim();
                            const val = String(row[1]).trim();
                            if (key === 'Nama Sesi') sessionName = val;
                            if (key === 'Device Name') deviceName = val;
                            if (key === 'Waktu Mulai') startTime = val;
                            if (key === 'Waktu Selesai') endTime = val;
                        }
                    });
                }
                
                // Parse other sheets as phases
                workbook.SheetNames.forEach(sheetName => {
                    if (sheetName === 'Summary') return;
                    
                    const ws = workbook.Sheets[sheetName];
                    const rows = XLSX.utils.sheet_to_json(ws);
                    if (!rows.length) return;
                    
                    const mapped = rows.map(r => {
                        const timestamp = r['Timestamp'] || '';
                        const offline = String(r['Status'] || '').toUpperCase() === 'OFFLINE';
                        
                        const getVal = (field) => {
                            const val = r[field];
                            return val != null ? parseFloat(val) : 0;
                        };
                        
                        return {
                            timestamp,
                            epoch: timestamp ? parseTimestampToEpoch(timestamp) : Date.now(),
                            offline,
                            Voltage: getVal('Voltage (V)'),
                            Current: getVal('Current (A)'),
                            Power: getVal('Power (W)'),
                            Frequency: getVal('Frequency (Hz)'),
                            Energy: getVal('Active Energy (kWh)'),
                            PowerFactor: getVal('Power Factor')
                        };
                    });
                    
                    records[sheetName] = mapped;
                });
                
                const phases = Object.keys(records);
                if (!phases.length) {
                    throw new Error("File Excel tidak berisi sheet sensor telemetri yang valid.");
                }
                
                activeOfflineSessionData = {
                    session: {
                        id: 'offline_' + Date.now(),
                        name: sessionName,
                        deviceId: deviceId,
                        deviceName: deviceName,
                        startTime: startTime,
                        endTime: endTime,
                        recordCount: Object.values(records).reduce((sum, arr) => sum + arr.length, 0),
                        phases: phases
                    },
                    records: records
                };
                
                hideGlobalLoader();
                await showModal('Visualisasi Berhasil', `Berkas Excel sesi "${sessionName}" berhasil dimuat ke tampilan pratinjau.`, 'success');
                renderOfflineWorkspace();
                
            } catch (err) {
                hideGlobalLoader();
                await showModal('Pemuatan Gagal', 'Error: ' + err.message, 'error');
            }
        };
        reader.readAsArrayBuffer(file);
    }
}

function renderOfflineWorkspace() {
    const sectionUploader = $('offlineUploaderSection');
    const sectionWorkspace = $('offlineWorkspaceSection');
    if (!sectionUploader || !sectionWorkspace) return;

    sectionUploader.style.display = 'none';
    sectionWorkspace.style.display = 'block';

    const s = activeOfflineSessionData.session;
    $('owSessionName').textContent = s.name || 'Tanpa nama';
    $('owMetaText').innerHTML = `Device: <strong>${s.deviceName}</strong> &middot; Start: <strong>${s.startTime}</strong> &middot; End: <strong>${s.endTime}</strong> &middot; Records: <strong>${s.recordCount}</strong>`;

    activeOfflineSelectedPhase = s.phases[0] || '';
    
    // Render tabs
    const tabsWrap = $('owSensorTabs');
    if (tabsWrap) {
        tabsWrap.innerHTML = s.phases.map(ph => {
            return `<button class="sensor-tab-btn ${ph === activeOfflineSelectedPhase ? 'active' : ''}" onclick="switchOfflineSensorTab('${ph}')">${ph}</button>`;
        }).join('');
    }

    // Reset param selector to Power
    const paramSelect = $('owParamSelect');
    if (paramSelect) paramSelect.value = 'Power';

    activeOfflineSelectedPage = 1;
    renderOfflineChart();
    renderOfflineTable();
}

function resetOfflineWorkspace() {
    activeOfflineSessionData = null;
    activeOfflineSelectedPhase = '';
    activeOfflineSelectedPage = 1;
    if (activeOfflineChart) {
        activeOfflineChart.destroy();
        activeOfflineChart = null;
    }

    const sectionUploader = $('offlineUploaderSection');
    const sectionWorkspace = $('offlineWorkspaceSection');
    if (sectionUploader) sectionUploader.style.display = 'block';
    if (sectionWorkspace) sectionWorkspace.style.display = 'none';
}

function switchOfflineSensorTab(phase) {
    activeOfflineSelectedPhase = phase;
    
    // Update active tab button style
    document.querySelectorAll('#owSensorTabs .sensor-tab-btn').forEach(btn => {
        btn.classList.toggle('active', btn.textContent === phase);
    });

    activeOfflineSelectedPage = 1;
    renderOfflineTable();
}

function onOwParamChange() {
    renderOfflineChart();
}

function renderOfflineChart() {
    const ctx = document.getElementById('owChartCanvas');
    if (!ctx) return;

    if (activeOfflineChart) {
        activeOfflineChart.destroy();
        activeOfflineChart = null;
    }

    if (!activeOfflineSessionData) return;

    const param = $('owParamSelect')?.value || 'Power';
    const filterMode = activeOfflineTimeFilter || 'session';

    const rawRecordsMap = activeOfflineSessionData.records || {};
    const phases = (activeOfflineSessionData.session.phases || []).map(ph => ({ phase: ph, name: ph }));
    const { sortedTimestamps, recordsMap } = aggregateRecordsByTime(rawRecordsMap, phases, param, filterMode);

    const totalSlots = sortedTimestamps.length;
    const batchSize = 3000;
    const totalPages = Math.ceil(totalSlots / batchSize) || 1;
    const currentPage = Math.min(Math.max(1, activeOfflineSelectedPage || 1), totalPages);
    activeOfflineSelectedPage = currentPage;

    const startIdx = (currentPage - 1) * batchSize;
    const endIdx = startIdx + batchSize;
    const chartLabels = sortedTimestamps.slice(startIdx, endIdx);

    _renderBatchNav('owChartBatchNav', currentPage, totalPages, startIdx + 1, Math.min(endIdx, totalSlots), totalSlots, (newPage) => {
        activeOfflineSelectedPage = newPage;
        renderOfflineChart();
    });

    const datasets = phases.map((p, idx) => {
        const records = recordsMap[p.phase] || [];
        const dataMap = {};
        records.forEach(r => {
            if (r.timestamp) dataMap[r.timestamp] = r;
        });

        const dataPoints = chartLabels.map(ts => {
            const rec = dataMap[ts];
            return rec ? (rec[param] != null ? rec[param] : null) : null;
        });

        const phaseColors = ['#00A651', '#1E90FF', '#FF8C00', '#8A2BE2', '#FF1493', '#00CED1'];
        const color = phaseColors[idx % phaseColors.length];

        return {
            label: p.name,
            data: dataPoints,
            borderColor: color,
            backgroundColor: color + '0a',
            borderWidth: 2,
            pointRadius: chartLabels.length > 60 ? 0 : 3,
            fill: false,
            tension: 0.15,
            spanGaps: true
        };
    });

    activeOfflineChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: chartLabels.map(ts => _formatTimestampForChart(ts)),
            datasets: datasets
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: true,
                    position: 'top',
                    labels: {
                        boxWidth: 12,
                        font: { family: 'var(--font-ui)', weight: 'bold', size: 11 },
                        color: 'var(--text-secondary)'
                    }
                },
                tooltip: {
                    enabled: false,
                    external: iSolarTooltipHandler,
                    mode: 'index',
                    intersect: false
                }
            },
            scales: {
                x: {
                    grid: { display: false },
                    ticks: {
                        maxTicksLimit: 10,
                        font: { family: 'var(--font-ui)', size: 9.5 },
                        color: 'var(--text-tertiary)'
                    }
                },
                y: {
                    grid: { color: 'rgba(0,0,0,0.03)' },
                    title: {
                        display: true,
                        text: param,
                        font: { family: 'var(--font-ui)', weight: 'bold', size: 11 },
                        color: 'var(--text-secondary)'
                    },
                    ticks: {
                        font: { family: 'var(--font-ui)', size: 9.5 },
                        color: 'var(--text-tertiary)'
                    }
                }
            }
        }
    });
    _initChartGestures(activeOfflineChart);
}

function renderOfflineTable() {
    const tbody = $('owTableBody');
    if (!tbody || !activeOfflineSessionData || !activeOfflineSelectedPhase) return;

    const records = activeOfflineSessionData.records[activeOfflineSelectedPhase] || [];
    const sorted = records.slice().sort((a, b) => parseTimestampToEpoch(b.timestamp) - parseTimestampToEpoch(a.timestamp));

    const pageSize = 20;
    const totalPages = Math.ceil(sorted.length / pageSize);
    const page = Math.max(1, Math.min(activeOfflineSelectedPage, totalPages));
    activeOfflineSelectedPage = page;

    const slice = sorted.slice((page - 1) * pageSize, page * pageSize);

    if (!slice.length) {
        tbody.innerHTML = `<tr><td colspan="7" style="text-align:center;padding:24px;color:var(--text-tertiary)">Tidak ada data.</td></tr>`;
        $('owPaginationBar').innerHTML = '';
        return;
    }

    tbody.innerHTML = slice.map(r => {
        const isOff = r.offline;
        return `<tr class="inner-record-row ${isOff ? 'record-offline' : ''}">
            <td>${r.timestamp || '---'}</td>
            <td style="text-align:right">${r.Voltage != null ? r.Voltage.toFixed(2) : '---'}</td>
            <td style="text-align:right">${r.Current != null ? r.Current.toFixed(2) : '---'}</td>
            <td style="text-align:right; font-weight:700; color:${isOff ? 'inherit' : 'var(--brand)'}">${r.Power != null ? r.Power.toFixed(2) : '---'}</td>
            <td style="text-align:right">${r.Frequency != null ? r.Frequency.toFixed(1) : '---'}</td>
            <td style="text-align:right">${r.Energy != null ? r.Energy.toFixed(4) : '---'}</td>
            <td style="text-align:right">${r.PowerFactor != null ? r.PowerFactor.toFixed(4) : '---'}</td>
        </tr>`;
    }).join('');

    if (totalPages > 1) {
        $('owPaginationBar').innerHTML = _renderOfflinePaginationBar(page, totalPages, sorted.length);
    } else {
        $('owPaginationBar').innerHTML = '';
    }
}

function _renderOfflinePaginationBar(page, totalPages, totalCount) {
    return `<div style="display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:12px;">
        <span style="font-size:11.5px; color:var(--text-tertiary); font-weight:600">Menampilkan ${Math.min(totalCount, (page - 1) * 20 + 1)}-${Math.min(totalCount, page * 20)} dari ${totalCount} record</span>
        <div style="display:flex; gap:4px">
            <button class="time-filter-btn" ${page <= 1 ? 'disabled style="opacity:0.4;cursor:not-allowed"' : ''} onclick="goOfflineTablePage(${page - 1})">Sebelumnya</button>
            <span style="display:flex; align-items:center; padding:0 10px; font-size:12px; font-weight:700; color:var(--text-secondary)">Halaman ${page} / ${totalPages}</span>
            <button class="time-filter-btn" ${page >= totalPages ? 'disabled style="opacity:0.4;cursor:not-allowed"' : ''} onclick="goOfflineTablePage(${page + 1})">Berikutnya</button>
        </div>
    </div>`;
}

/* ==========================================================================
   Theme Management (Light / Dark Mode) matching Sparta Energy Palette
   ========================================================================== */
function initTheme() {
    const savedTheme = localStorage.getItem('sem_theme');
    const isDark = savedTheme === 'dark' || (!savedTheme && window.matchMedia('(prefers-color-scheme: dark)').matches);
    if (isDark) {
        document.documentElement.classList.add('dark');
    } else {
        document.documentElement.classList.remove('dark');
    }
    _updateThemeUI(isDark);
}

function toggleTheme() {
    const isDark = document.documentElement.classList.toggle('dark');
    localStorage.setItem('sem_theme', isDark ? 'dark' : 'light');
    _updateThemeUI(isDark);
    
    // Update Chart themes dynamically
    if (typeof realtimeChart !== 'undefined' && realtimeChart) {
        _updateChartTheme(realtimeChart, isDark);
    }
    if (typeof activeOfflineChart !== 'undefined' && activeOfflineChart) {
        _updateChartTheme(activeOfflineChart, isDark);
    }
}

function _updateThemeUI(isDark) {
    const sunIcon = document.getElementById('themeIconSun');
    const moonIcon = document.getElementById('themeIconMoon');
    const themeText = document.getElementById('themeText');
    if (sunIcon && moonIcon) {
        if (isDark) {
            // Tampilkan icon Bulan & label "Dark" saat Mode Gelap aktif
            sunIcon.style.display = 'none';
            moonIcon.style.display = 'block';
            if (themeText) themeText.textContent = 'Dark';
        } else {
            // Tampilkan icon Matahari & label "Light" saat Mode Terang aktif
            sunIcon.style.display = 'block';
            moonIcon.style.display = 'none';
            if (themeText) themeText.textContent = 'Light';
        }
    }
}

function _updateChartTheme(chart, isDark) {
    if (!chart || !chart.options) return;
    const textColor = isDark ? '#94a3b8' : '#64748b';
    const gridColor = isDark ? 'rgba(255, 255, 255, 0.04)' : 'rgba(226, 232, 240, 0.7)';
    
    if (chart.options.scales?.x) {
        if (!chart.options.scales.x.ticks) chart.options.scales.x.ticks = {};
        if (!chart.options.scales.x.grid) chart.options.scales.x.grid = {};
        chart.options.scales.x.ticks.color = textColor;
        chart.options.scales.x.grid.color = gridColor;
        if (chart.options.scales.x.title) chart.options.scales.x.title.color = textColor;
    }
    if (chart.options.scales?.y) {
        if (!chart.options.scales.y.ticks) chart.options.scales.y.ticks = {};
        if (!chart.options.scales.y.grid) chart.options.scales.y.grid = {};
        chart.options.scales.y.ticks.color = textColor;
        chart.options.scales.y.grid.color = gridColor;
        if (chart.options.scales.y.title) chart.options.scales.y.title.color = textColor;
    }
    if (chart.options.plugins?.legend?.labels) {
        chart.options.plugins.legend.labels.color = isDark ? '#e2e8f0' : '#374151';
    }
    chart.update('none');
}

function goOfflineTablePage(page) {
    activeOfflineSelectedPage = page;
    renderOfflineTable();
}

document.addEventListener('DOMContentLoaded', async () => {
    initTheme();
    initOfflineDropZone();
    updateDateNavigatorUI();
    initChart();
    DOM.paramSelect?.addEventListener('change', changeParameter);
    document.querySelectorAll('.metric-card-compact[data-param]').forEach(card => {
        card.addEventListener('click', () => _switchParameter(card.dataset.param));
        card.classList.toggle('card-active', card.dataset.param === selectedParameter);
    });
    try {
        await loadDevices();
    } catch (err) {
        console.error("Error loading devices in init:", err);
    } finally {
        const globalLoader = document.getElementById('globalLoader');
        if (globalLoader) {
            globalLoader.classList.add('hidden');
            setTimeout(() => globalLoader.style.display = 'none', 500);
        }
    }
    setInterval(loadDevices, 30_000);
    setInterval(_saveChartCache, 60_000); // auto-save chart cache tiap 60 detik
    updateConnectionStatus('connecting');
    startConnectionMonitoring();
    startPhaseTimeoutMonitoring();
    startTimeWindowMonitoring();
    await syncCaptureStatus();
    _startStatusPolling();
    $('intervalInput')?.addEventListener('focus', () => { _intervalUserEdited = true; });
    $('intervalInput')?.addEventListener('input', () => { _intervalUserEdited = true; });
    $('intervalUnit')?.addEventListener('change', () => { _intervalUserEdited = true; });
    $('sessionNameInput')?.addEventListener('keydown', e => {
        if (e.key === 'Enter') { _renamingSessionId ? confirmRenameSession() : confirmStartCapture(); }
    });
    document.addEventListener('click', () => {
        document.querySelectorAll('.session-dropdown-menu').forEach(el => el.style.display = 'none');
    });
});
window.addEventListener('beforeunload', () => {
    _saveChartCache(); // simpan sebelum refresh / close
    if (_rafId) { cancelAnimationFrame(_rafId); _rafId = null; }
    if (_chartTimer) { clearInterval(_chartTimer); _chartTimer = null; }
});

if (typeof module !== 'undefined' && module.exports) {
    module.exports = {
        parseTimestamp,
        _buildExcelRow,
        _escapeAttr,
        _highlight
    };
}