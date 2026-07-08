var API = "/api/v1";
var COLORS = ["#5b6870", "#8b959d", "#b1bbc3", "#6b7b84", "#9ba8b0",
              "#7d8f99", "#4a5a63", "#94a0a8"];
var ACCENT = "#5b6870";

var charts = {};
var baseGrid = { left: 64, right: 28, top: 20, bottom: 32 };
var axisStyle = { color: "#a1a09b", fontSize: 11, fontFamily: "'Geist Mono','SF Mono',monospace" };
var splitLineStyle = { lineStyle: { color: "#f0f0ee" } };

function fmtNum(n) {
  if (n === null || n === undefined) return "--";
  if (n >= 1e8) return (n / 1e8).toFixed(2) + "yi";
  if (n >= 1e4) return (n / 1e4).toFixed(2) + "w";
  return Number(n).toLocaleString();
}

function fmtMoney(n) {
  if (n === null || n === undefined) return "--";
  return "\xA5" + Number(n).toLocaleString(undefined, { minimumFractionDigits: 2 });
}

function fmtPct(n) {
  if (typeof n !== "number") return "--";
  return (n * 100).toFixed(2) + "%";
}

function apiUrl(path) { return API + path; }

async function getJSON(url) {
  try {
    var resp = await axios.get(url);
    if (resp.data.code !== 0) throw new Error(resp.data.message);
    return resp.data.data;
  } catch (e) {
    console.error("API:", url, e.message);
    return null;
  }
}

function buildRangeUrl(path, hours) {
  var end = new Date();
  var start = new Date(end.getTime() - hours * 3600000);
  var f = function(d) { return d.toISOString().slice(0, 19).replace("T", " "); };
  return path + "?start=" + encodeURIComponent(f(start)) + "&end=" + encodeURIComponent(f(end));
}

function tooltipBase() {
  return { backgroundColor: "#fff", borderColor: "#eaeaea", borderWidth: 1,
           textStyle: { color: "#111", fontSize: 12 }, extraCssText: "border-radius:6px;box-shadow:0 2px 12px rgba(0,0,0,0.06);" };
}

async function loadRealtime() {
  var data = await getJSON(apiUrl("/stats/realtime?window=5"));
  if (!data) return;
  setVal("kpi-impressions", fmtNum(data.impressions));
  setVal("kpi-clicks", fmtNum(data.clicks));
  setVal("kpi-conversions", fmtNum(data.conversions));
  setVal("kpi-uv", fmtNum(data.uv));
  setVal("kpi-cost", fmtMoney(data.cost));
  setVal("kpi-revenue", fmtMoney(data.revenue));
  setVal("kpi-ctr", fmtPct(data.ctr));
  setVal("kpi-roi", data.roi ? data.roi.toFixed(2) : "--");
}

function setVal(id, val) {
  var el = document.getElementById(id);
  if (el) el.textContent = val;
}

async function loadHourly() {
  var data = await getJSON(apiUrl("/stats/hourly?hours=24"));
  if (!data || !data.length) return;
  var hours = data.map(function(d) { return new Date(d.hour).getHours() + "h"; });
  charts.hourly.setOption({
    tooltip: tooltipBase(),
    legend: { data: ["曝光", "点击", "转化"], textStyle: axisStyle, top: 0 },
    grid: { left: 80, right: 40, top: 40, bottom: 32 },
    xAxis: { type: "category", data: hours, axisLabel: axisStyle, axisTick: { show: false }, axisLine: { lineStyle: { color: "#eaeaea" } } },
    yAxis: { type: "value", axisLabel: axisStyle, splitLine: splitLineStyle, axisLine: { show: false } },
    series: [
      { name: "曝光", type: "line", smooth: true, symbol: "none",
        data: data.map(function(d) { return d.impressions; }),
        lineStyle: { color: COLORS[0], width: 2 },
        itemStyle: { color: COLORS[0] },
        areaStyle: { color: "rgba(91,104,112,0.05)" } },
      { name: "点击", type: "line", smooth: true, symbol: "none",
        data: data.map(function(d) { return d.clicks; }),
        lineStyle: { color: COLORS[2], width: 2 },
        itemStyle: { color: COLORS[2] },
        areaStyle: { color: "rgba(177,187,195,0.05)" } },
      { name: "转化", type: "line", smooth: true, symbol: "none",
        data: data.map(function(d) { return d.conversions; }),
        lineStyle: { color: COLORS[4], width: 2 },
        itemStyle: { color: COLORS[4] } }
    ]
  });
}

async function loadFunnel() {
  var data = await getJSON(apiUrl(buildRangeUrl("/stats/funnel", 24) + "&window=3600"));
  if (!data || !data.length) return;
  charts.funnel.setOption({
    tooltip: tooltipBase(),
    series: [{
      type: "funnel", left: "8%", right: "8%", top: 10, bottom: 10, sort: "descending", gap: 1,
      label: { show: true, position: "inside", color: "#fff", fontSize: 12, fontWeight: 600, formatter: "{b}\n{c}" },
      itemStyle: { borderColor: "#fff", borderWidth: 2 },
      data: data.map(function(d, i) {
        var names = { "impression": "曝光", "click": "点击", "conversion": "转化" };
        return { name: names[d.step] || d.step, value: d.count, itemStyle: { color: COLORS[i] } };
      })
    }]
  });
}

async function loadRegion() {
  var data = await getJSON(apiUrl(buildRangeUrl("/stats/regions", 24) + "&limit=10"));
  if (!data || !data.length) return;
  charts.region.setOption({
    tooltip: { trigger: "axis", axisPointer: { type: "shadow" }, backgroundColor: "#fff", borderColor: "#eaeaea", borderWidth: 1, textStyle: { color: "#111" } },
    grid: { left: 88, right: 32, top: 14, bottom: 28 },
    xAxis: { type: "value", axisLabel: axisStyle, splitLine: splitLineStyle, axisLine: { show: false } },
    yAxis: { type: "category", data: data.map(function(d) { return d.region; }).reverse(), axisLabel: { color: "#787774", fontSize: 11 }, axisTick: { show: false }, axisLine: { lineStyle: { color: "#eaeaea" } } },
    series: [{ type: "bar", data: data.map(function(d) { return d.impressions; }).reverse(),
      itemStyle: { color: ACCENT, borderRadius: [0, 3, 3, 0] }, barWidth: 14 }]
  });
}

async function loadDevice() {
  var data = await getJSON(apiUrl(buildRangeUrl("/stats/devices", 24)));
  if (!data || !data.length) return;
  charts.device.setOption({
    tooltip: tooltipBase(),
    legend: { bottom: 0, textStyle: axisStyle, itemWidth: 8, itemHeight: 8 },
    series: [{
      type: "pie", radius: ["50%", "72%"], center: ["50%", "46%"],
      label: { color: "#787774", fontSize: 11 },
      itemStyle: { borderColor: "#fff", borderWidth: 2 },
      data: data.map(function(d, i) {
        return { name: d.device, value: d.impressions, itemStyle: { color: COLORS[i] } };
      })
    }]
  });
}

async function loadTopAds() {
  var data = await getJSON(apiUrl(buildRangeUrl("/stats/top-ads", 24) + "&sort=revenue&limit=10"));
  if (!data || !data.length) return;
  charts.topAds.setOption({
    tooltip: { trigger: "axis", axisPointer: { type: "shadow" }, backgroundColor: "#fff", borderColor: "#eaeaea", borderWidth: 1, textStyle: { color: "#111" } },
    grid: { left: 124, right: 32, top: 14, bottom: 28 },
    xAxis: { type: "value", axisLabel: axisStyle, splitLine: splitLineStyle, axisLine: { show: false } },
    yAxis: { type: "category", data: data.map(function(d) { return d.ad_name || d.ad_id; }).reverse(), axisLabel: { color: "#787774", fontSize: 11, width: 110, overflow: "truncate" }, axisTick: { show: false }, axisLine: { lineStyle: { color: "#eaeaea" } } },
    series: [{ type: "bar", data: data.map(function(d) { return d.revenue; }).reverse(),
      itemStyle: { color: ACCENT, borderRadius: [0, 3, 3, 0] }, barWidth: 14 }]
  });
}

async function loadCampaigns() {
  var data = await getJSON(apiUrl(buildRangeUrl("/stats/campaigns", 24) + "&limit=10"));
  if (!data || !data.length) return;
  var tbody = document.querySelector("#table-campaigns tbody");
  tbody.innerHTML = data.map(function(c) {
    return "<tr>" +
      "<td>" + (c.campaign_name || c.campaign_id) + "</td>" +
      "<td class='num'>" + fmtNum(c.impressions) + "</td>" +
      "<td class='num'>" + fmtNum(c.uv) + "</td>" +
      "<td class='num'>" + fmtPct(c.ctr) + "</td>" +
      "<td class='num'>" + fmtPct(c.cvr) + "</td>" +
      "<td class='num'>" + fmtMoney(c.cost) + "</td>" +
      "<td class='num'>" + fmtMoney(c.revenue) + "</td>" +
      "<td class='num " + (c.roi >= 1 ? "roi-good" : "roi-bad") + "'>" + (c.roi ? c.roi.toFixed(2) : "--") + "</td>" +
      "</tr>";
  }).join("");
}

async function loadRetention() {
  var now = new Date();
  var date = new Date(now.getTime() - 7 * 86400000).toISOString().slice(0, 10);
  var data = await getJSON(apiUrl("/stats/retention?date=" + date + "&event_type=impression&days=7"));
  if (!data || !data.length) return;
  charts.retention.setOption({
    tooltip: tooltipBase(),
    grid: { left: 64, right: 32, top: 14, bottom: 32 },
    xAxis: { type: "category", data: data.map(function(d) { return "D+" + d.day; }), axisLabel: axisStyle, axisTick: { show: false }, axisLine: { lineStyle: { color: "#eaeaea" } } },
    yAxis: { type: "value", axisLabel: axisStyle, splitLine: splitLineStyle, axisLine: { show: false } },
    series: [{ type: "bar", data: data.map(function(d) { return d.users; }),
      itemStyle: { color: ACCENT, borderRadius: [3, 3, 0, 0] }, barWidth: 18 }]
  });
}

function initCharts() {
  var map = {
    hourly: "chart-hourly", funnel: "chart-funnel", region: "chart-region",
    device: "chart-device", topAds: "chart-topAds", retention: "chart-retention"
  };
  Object.keys(map).forEach(function(k) {
    var el = document.getElementById(map[k]);
    if (el) charts[k] = echarts.init(el);
  });
  window.addEventListener("resize", function() {
    Object.keys(charts).forEach(function(k) { if (charts[k]) charts[k].resize(); });
  });
}

async function refresh() {
  await Promise.all([
    loadRealtime(), loadHourly(), loadFunnel(), loadRegion(),
    loadDevice(), loadTopAds(), loadCampaigns(), loadRetention()
  ]);
  setVal("update-time", new Date().toLocaleTimeString("zh-CN", { hour12: false }));
}

document.addEventListener("DOMContentLoaded", function() {
  initCharts();
  refresh();
  setInterval(refresh, 10000);
});