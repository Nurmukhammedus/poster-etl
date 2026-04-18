// ═══════════════════════════════════════════════════════════
//  Poster ETL — панель управления
//  Google Sheets → Extensions → Apps Script → вставить весь код
// ═══════════════════════════════════════════════════════════

const CLOUD_RUN_URL = "https://poster-etl-819303951457.europe-west3.run.app/";

// ─────────────────────────────────────────────────────────────
//  Меню при открытии таблицы
// ─────────────────────────────────────────────────────────────

function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu("🚀 Poster ETL")
    .addItem("Открыть панель", "showSidebar")
    .addToUi();
}

function showSidebar() {
  const html = HtmlService.createHtmlOutputFromFile("sidebar")
    .setTitle("Poster ETL")
    .setWidth(340);
  SpreadsheetApp.getUi().showSidebar(html);
}

// ─────────────────────────────────────────────────────────────
//  Данные для HTML (вызывается из sidebar.html)
// ─────────────────────────────────────────────────────────────

function getDefaultDates() {
  const tz      = "Asia/Bishkek";
  const today   = Utilities.formatDate(new Date(), tz, "yyyy-MM-dd");
  const monthAgo = Utilities.formatDate(
    new Date(new Date().setDate(new Date().getDate() - 30)), tz, "yyyy-MM-dd"
  );
  return { today, monthAgo };
}

// ─────────────────────────────────────────────────────────────
//  Запуск ETL (fire-and-forget)
//
//  Apps Script обрывает соединение через ~6 мин — это нормально.
//  Cloud Run уже получил запрос и продолжает работу в фоне.
// ─────────────────────────────────────────────────────────────

function triggerETL(syncType, dateFrom, dateTo) {
  const payload = JSON.stringify({
    sync_type: syncType,
    date_from: dateFrom,
    date_to:   dateTo,
  });

  try {
    UrlFetchApp.fetch(CLOUD_RUN_URL, {
      method:             "post",
      contentType:        "application/json",
      payload:            payload,
      muteHttpExceptions: true,
    });
  } catch (e) {
    // Таймаут Apps Script — Cloud Run уже работает в фоне
    const msg = e.toString();
    if (!msg.includes("Timed out") && !msg.includes("Время ожидания")) {
      return { status: "error", message: msg };
    }
  }

  _logToSheet(syncType, dateFrom, dateTo);

  return {
    status:    "started",
    sync_type: syncType,
    date_from: dateFrom,
    date_to:   dateTo,
  };
}

// ─────────────────────────────────────────────────────────────
//  Открыть лист с логами
// ─────────────────────────────────────────────────────────────

function openLogsSheet() {
  const ss    = SpreadsheetApp.getActiveSpreadsheet();
  let   sheet = ss.getSheetByName("ETL Logs");
  if (!sheet) sheet = ss.insertSheet("ETL Logs");
  ss.setActiveSheet(sheet);
}

// ─────────────────────────────────────────────────────────────
//  Лог запусков → лист "ETL Logs"
// ─────────────────────────────────────────────────────────────

function _logToSheet(syncType, dateFrom, dateTo) {
  const ss    = SpreadsheetApp.getActiveSpreadsheet();
  let   sheet = ss.getSheetByName("ETL Logs");

  if (!sheet) {
    sheet = ss.insertSheet("ETL Logs");
    const header = ["Время запуска", "Тип синхронизации", "Дата от", "Дата до"];
    sheet.appendRow(header);
    sheet.getRange(1, 1, 1, header.length)
      .setFontWeight("bold")
      .setBackground("#1a73e8")
      .setFontColor("white");
    sheet.setFrozenRows(1);
  }

  const now = Utilities.formatDate(new Date(), "Asia/Bishkek", "yyyy-MM-dd HH:mm:ss");
  sheet.appendRow([now, syncType, dateFrom, dateTo]);
}
