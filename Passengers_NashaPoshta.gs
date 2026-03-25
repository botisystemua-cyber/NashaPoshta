// ================================================================
// Borispol_Vip_Travel_Passengers.gs — CRM Пасажири (менеджери)
// Живе в таблиці: Passengers_crm_Oksi
// Deploy: Web App → доступ "Будь-хто"
// ================================================================
//
// СТРУКТУРА КОДУ (для розробника):
//
// ── КОНФІГ (рядки 10-83) ──────────────────────────────────────
//   DB{}          — ID всіх Google Sheets таблиць системи
//   SS_ID         — головна таблиця (Passengers)
//   SHEETS{}      — назви аркушів в Passengers (Україна-ЄВ, Європа-УК, Календар...)
//   PAX_COLS[]    — колонки пасажирів
//   CAL_COLS[]    — колонки рейсів (Календар)
//   LAYOUTS{}     — розкладки місць в авто
//
// ── HELPERS (рядки ~86-115) ───────────────────────────────────
//   getSheet()    — відкриває аркуш з SS_ID
//   writeLog()    — логує дію в Archive_crm → "Логи"
//
// ── ПАСАЖИРИ: CRUD (рядки ~116-900) ──────────────────────────
//   apiGetAll()            — отримати всіх пасажирів (GET/POST)
//   apiAddPassenger()      — додати пасажира
//   apiUpdatePassenger()   — оновити пасажира
//   apiUpdateField()       — оновити одне поле
//   apiBulkUpdateField()   — масове оновлення поля
//   apiClonePassenger()    — клонувати пасажира
//   apiMoveDirection()     — змінити напрям (UA-EU ↔ EU-UA)
//   apiCheckDuplicate()    — перевірка дублікатів
//
// ── ВИДАЛЕННЯ / АРХІВУВАННЯ (рядки ~873-1110) ────────────────
//   apiDeletePassenger()   — soft delete (архівує з позначкою "Видалено")
//   apiDeleteFromSheet()   — фізичне видалення рядка (для маршрутів)
//   apiBulkDelete()        — масове soft delete
//   apiArchivePassenger()  — перенос пасажира в Archive_crm → "Архів"
//   apiRestorePassenger()  — відновлення з архіву назад в Passengers
//   apiDeleteFromArchive() — ЗАБЛОКОВАНО (архів = назавжди)
//   apiGetArchive()        — отримати записи з архіву
//
// ── РЕЙСИ: CRUD (рядки ~1110-1460) ──────────────────────────
//   apiGetTrips()          — отримати рейси (аркуш Календар)
//   apiCreateTrip()        — створити рейс
//   apiUpdateTrip()        — оновити рейс
//   apiAssignTrip()        — призначити пасажира на рейс
//   apiUnassignTrip()      — зняти з рейсу
//   apiReassignTrip()      — пересадити на інший рейс
//   apiArchiveTrip()       — архівувати рейс → Archive_crm → "Архів рейсів"
//   apiDeleteTrip()        — soft delete рейсу → Archive_crm → "Архів рейсів"
//   apiDuplicateTrip()     — дублювати рейс на нові дати
//   clearCalIdInPassengers() — зняти CAL_ID у пасажирів рейсу
//
// ── МАРШРУТИ (рядки ~1460-2000) ─────────────────────────────
//   apiGetRoutesList()     — список маршрутів (тільки Маршрут_*)
//   apiGetRouteSheet()     — дані одного маршруту (lazy load + кеш 3хв)
//   apiAddToRoute()        — додати ліда в маршрут (case-insensitive маппінг)
//   apiUpdateRouteField()  — оновити поле ліда в маршруті
//   apiCreateRoute()       — створити новий маршрут (копіює шаблони)
//   apiDeleteRoute()       — видалити маршрут (архівує → "Архів маршрутів")
//   apiDeleteLinkedSheets() — видалити Відправка_/Витрати_ (архівує)
//   archiveSheetToArchive() — хелпер: копіює аркуш в архів перед видаленням
//
// ── АВТОПАРК / РОЗСАДКА (рядки ~2000+) ─────────────────────
//   apiGetAutopark()       — список авто
//   apiGetStats()          — статистика
//
// ── РОУТЕР: doGet / doPost (рядки ~2070+) ───────────────────
//   doGet()   — обробка GET запитів (ping, getAll, getTrips, getStats)
//   doPost()  — обробка POST запитів (всі дії через action)
//
// ── ПРИНЦИП ВИДАЛЕННЯ / АРХІВУВАННЯ ─────────────────────────
//   Нічого не зникає назавжди! Кожна дія:
//   1. Копіює запис в Archive_crm (відповідний аркуш)
//   2. Видаляє з основної таблиці (deleteRow)
//   Аркуші в Archive_crm:
//     "Архів"           — пасажири
//     "Архів рейсів"    — рейси
//     "Архів маршрутів"  — записи маршрутів/відправок/витрат
//     "Логи"            — логи дій
//
// ================================================================

var HEADER_ROW = 1;
var DATA_START = 2;

// ── ВСІ ТАБЛИЦІ СИСТЕМИ (SpreadsheetApp.openById) ──
var DB = {
  PASSENGERS: '1dCztfyvqFgCEIA6nmtLGFv94QPNDJa5lhCsWpzDVXxY',
  POSYLKI:    '1kcF3JchG5n7OzB_K2h9hxBqap7xHDnKN5nwMO9Fm2eo',
  MARHRUT:    '1to9F6K4p46ZUVva0ZG7nCO7ZFbW1ve0SUnathsqhUW0',
  KLIYENTU:   '1e-V4gK63I3VPlwg_PpYmVOLAYF3YG_s866D5Ys0vVw4',
  FINANCE:    '1Np3hLCwUIWW9FqWJDZjF_dUQZMIjY16oEH_Z8zEBe_A',
  CONFIG:     '16j7sX17Ic45dbyyC7qvEG-8VuNtUdCz8d0S03FbABJs',
  ARCHIVE:    '1Id93R7TJeIP62Gye7fFnu4q3YBZpRr2x652RWphLUiE'
};

// Головна таблиця цього скрипта
var SS_ID = DB.PASSENGERS;

// ── АРКУШІ в Passengers_crm_v4 ──
var SHEETS = {
  PAX_UE: 'Україна-ЄВ',
  PAX_EU: 'Європа-УК',
  AUTOPARK: 'Автопарк',
  CALENDAR: 'Календар',
  SEATING: 'Розсадка по авто'
};

// ── COLUMNS ──
var PAX_COLS = [
  'PAX_ID','Ід_смарт','Напрям','SOURCE_SHEET','Дата створення',
  'Піб','Телефон пасажира','Телефон реєстратора','Кількість місць',
  'Адреса відправки','Адреса прибуття','Дата виїзду','Таймінг',
  'Номер авто','Місце в авто','RTE_ID','Ціна квитка','Валюта квитка',
  'Завдаток','Валюта завдатку','Вага багажу','Ціна багажу','Валюта багажу',
  'Борг','Статус оплати','Статус ліда','Статус CRM','Тег',
  'Примітка','Примітка СМС','CLI_ID','BOOKING_ID',
  'DATE_ARCHIVE','ARCHIVED_BY','ARCHIVE_REASON','ARCHIVE_ID','CAL_ID'
];

var AUTO_COLS = [
  'AUTO_ID','Назва авто','Держ. номер','Тип розкладки','Місткість',
  'Місце','Тип місця','Ціна UAH','Ціна CHF','Ціна EUR',
  'Ціна PLN','Ціна CZK','Ціна USD','Статус місця','Статус авто','Примітка'
];

var CAL_COLS = [
  'CAL_ID','RTE_ID','AUTO_ID','Назва авто','Тип розкладки',
  'Дата рейсу','Напрямок','Місто','Макс. місць','Вільні місця',
  'Зайняті місця','Список вільних','Список зайнятих','PAIRED_CAL_ID','Статус рейсу'
];

var SEAT_COLS = [
  'SEAT_ID','RTE_ID','CAL_ID','AUTO_ID','PAX_ID',
  'Дата','Напрям','Назва авто','Тип розкладки','Місце',
  'Тип місця','Ціна місця','Валюта','Піб','Телефон пасажира',
  'Статус','DATE_RESERVED'
];

var LAYOUTS = {
  '1-3-3': [
    {seat:'V1',type:'Водій'},
    {seat:'A1',type:'Пасажир'},{seat:'A2',type:'Пасажир'},{seat:'A3',type:'Пасажир'},
    {seat:'B1',type:'Пасажир'},{seat:'B2',type:'Пасажир'},{seat:'B3',type:'Пасажир'}
  ],
  '2-2-3': [
    {seat:'A1',type:'Пасажир'},{seat:'A2',type:'Пасажир'},
    {seat:'B1',type:'Пасажир'},{seat:'B2',type:'Пасажир'},
    {seat:'C1',type:'Пасажир'},{seat:'C2',type:'Пасажир'},{seat:'C3',type:'Пасажир'}
  ],
  '2-2-2': [
    {seat:'A1',type:'Пасажир'},{seat:'A2',type:'Пасажир'},
    {seat:'B1',type:'Пасажир'},{seat:'B2',type:'Пасажир'},
    {seat:'C1',type:'Пасажир'},{seat:'C2',type:'Пасажир'}
  ]
};


// ══════════════════════════════════════════════════════════════
// HELPERS
// ══════════════════════════════════════════════════════════════

function getSheet(name) {
  return SpreadsheetApp.openById(SS_ID).getSheetByName(name);
}

// ── ЛОГУВАННЯ в Archive_crm → аркуш "Логи" ──
var LOG_COLS = ['Дата','Менеджер','Дія','Деталі','PAX_ID','CAL_ID','RTE_ID'];

function writeLog(manager, action, details, ids) {
  try {
    var archSS = SpreadsheetApp.openById(DB.ARCHIVE);
    var logSheet = archSS.getSheetByName('Логи');
    if (!logSheet) {
      logSheet = archSS.insertSheet('Логи');
      logSheet.getRange(1, 1, 1, LOG_COLS.length).setValues([LOG_COLS]);
      logSheet.getRange(1, 1, 1, LOG_COLS.length).setFontWeight('bold');
    }
    ids = ids || {};
    logSheet.appendRow([
      now(),
      manager || '',
      action || '',
      details || '',
      ids.pax_id || '',
      ids.cal_id || '',
      ids.rte_id || ''
    ]);
  } catch(e) {
    // Логування не повинно ламати основну операцію
  }
}

function genId(prefix) {
  var d = Utilities.formatDate(new Date(), 'Europe/Kiev', 'yyyyMMdd');
  var r = Math.random().toString(36).substr(2, 4).toUpperCase();
  return prefix + '-' + d + '-' + r;
}

function now() {
  return Utilities.formatDate(new Date(), 'Europe/Kiev', 'dd.MM.yyyy HH:mm');
}

function today() {
  return Utilities.formatDate(new Date(), 'Europe/Kiev', 'dd.MM.yyyy');
}

function sheetAlias(alias) {
  if (alias === 'ue' || alias === 'ua-eu') return SHEETS.PAX_UE;
  if (alias === 'eu' || alias === 'eu-ua') return SHEETS.PAX_EU;
  return alias;
}

function resolveSheet(params) {
  // Спочатку пробуємо alias, потім шукаємо пасажира в обох аркушах
  if (params.sheet) return sheetAlias(params.sheet);
  if (params.pax_id) {
    var sh1 = getSheet(SHEETS.PAX_UE);
    if (sh1 && findRow(sh1, 'PAX_ID', params.pax_id)) return SHEETS.PAX_UE;
    var sh2 = getSheet(SHEETS.PAX_EU);
    if (sh2 && findRow(sh2, 'PAX_ID', params.pax_id)) return SHEETS.PAX_EU;
  }
  return SHEETS.PAX_UE;
}

function getHeaders(sheet) {
  return sheet.getRange(HEADER_ROW, 1, 1, sheet.getLastColumn()).getValues()[0];
}

function getAllData(sheet) {
  var lastRow = sheet.getLastRow();
  if (lastRow < DATA_START) return { headers: getHeaders(sheet), data: [] };
  var headers = getHeaders(sheet);
  var data = sheet.getRange(DATA_START, 1, lastRow - DATA_START + 1, headers.length).getValues();
  return { headers: headers, data: data };
}

function rowToObj(headers, row) {
  var obj = {};
  for (var i = 0; i < headers.length; i++) {
    obj[headers[i]] = row[i] !== undefined ? row[i] : '';
  }
  return obj;
}

function objToRow(headers, obj) {
  return headers.map(function(h) { return obj[h] !== undefined ? obj[h] : ''; });
}

function findRow(sheet, colName, value) {
  var info = getAllData(sheet);
  var colIdx = info.headers.indexOf(colName);
  if (colIdx === -1) return null;
  for (var i = 0; i < info.data.length; i++) {
    if (String(info.data[i][colIdx]) == String(value)) {
      return { rowNum: DATA_START + i, headers: info.headers, data: info.data[i] };
    }
  }
  return null;
}

function findAllRows(sheet, colName, value) {
  var info = getAllData(sheet);
  var colIdx = info.headers.indexOf(colName);
  if (colIdx === -1) return [];
  var results = [];
  for (var i = 0; i < info.data.length; i++) {
    if (String(info.data[i][colIdx]) == String(value)) {
      results.push({ rowNum: DATA_START + i, headers: info.headers, data: info.data[i] });
    }
  }
  return results;
}

function calcDebt(obj) {
  var price = parseFloat(obj['Ціна квитка']) || 0;
  var wp = parseFloat(obj['Ціна багажу']) || 0;
  var dep = parseFloat(obj['Завдаток']) || 0;
  return Math.max(0, price + wp - dep);
}

function paxObjFromData(headers, data, shName, rowNum) {
  var obj = rowToObj(headers, data);
  obj._sheet = shName;
  obj._rowNum = rowNum;
  obj['Борг'] = calcDebt(obj);
  return obj;
}


// ══════════════════════════════════════════════════════════════
// 1. PASSENGERS — READ
// ══════════════════════════════════════════════════════════════

// getAll — Отримати всіх пасажирів (з фільтрами)
function apiGetAll(params) {
  var shAlias = params.sheet || 'all';
  var results = [];

  function loadSheet(name) {
    var sh = getSheet(name);
    if (!sh) return;
    var info = getAllData(sh);
    for (var i = 0; i < info.data.length; i++) {
      if (!info.data[i][0] && !info.data[i][5]) continue;
      var obj = paxObjFromData(info.headers, info.data[i], name, DATA_START + i);

      if (params.filter) {
        if (params.filter.dir && params.filter.dir !== 'all') {
          var rawDir = String(obj['Напрям'] || '').toLowerCase();
          if (params.filter.dir === 'ua-eu' && !rawDir.match(/ук|ua/)) continue;
          if (params.filter.dir === 'eu-ua' && !rawDir.match(/єв|eu/)) continue;
        }
        if (params.filter.statusLid && params.filter.statusLid !== 'all') {
          if (obj['Статус ліда'] !== params.filter.statusLid) continue;
        }
        if (params.filter.statusOplata && params.filter.statusOplata !== 'all') {
          if (obj['Статус оплати'] !== params.filter.statusOplata) continue;
        }
        if (params.filter.statusCrm && params.filter.statusCrm !== 'all') {
          if (obj['Статус CRM'] !== params.filter.statusCrm) continue;
        }
        if (params.filter.tag && params.filter.tag !== 'all') {
          if (obj['Тег'] !== params.filter.tag) continue;
        }
        if (params.filter.cal_id) {
          if (params.filter.cal_id === 'none') {
            if (obj['CAL_ID'] && String(obj['CAL_ID']).trim() !== '') continue;
          } else {
            if (obj['CAL_ID'] !== params.filter.cal_id) continue;
          }
        }
        if (params.filter.date_from) {
          if (String(obj['Дата виїзду']) < params.filter.date_from) continue;
        }
        if (params.filter.date_to) {
          if (String(obj['Дата виїзду']) > params.filter.date_to) continue;
        }
        if (params.filter.search) {
          var s = params.filter.search.toLowerCase();
          if (String(obj['Піб'] || '').toLowerCase().indexOf(s) === -1 &&
              String(obj['Телефон пасажира'] || '').indexOf(s) === -1 &&
              String(obj['PAX_ID'] || '').toLowerCase().indexOf(s) === -1) continue;
        }
      }
      results.push(obj);
    }
  }

  if (shAlias === 'all' || shAlias === 'ue') loadSheet(SHEETS.PAX_UE);
  if (shAlias === 'all' || shAlias === 'eu') loadSheet(SHEETS.PAX_EU);

  return { ok: true, count: results.length, data: results };
}

// getOne — Отримати одного пасажира по PAX_ID
function apiGetOne(params) {
  var paxId = params.pax_id;
  if (!paxId) return { ok: false, error: 'pax_id не вказано' };

  var sheets = [SHEETS.PAX_UE, SHEETS.PAX_EU];
  for (var s = 0; s < sheets.length; s++) {
    var sh = getSheet(sheets[s]);
    if (!sh) continue;
    var found = findRow(sh, 'PAX_ID', paxId);
    if (found) {
      var obj = paxObjFromData(found.headers, found.data, sheets[s], found.rowNum);
      return { ok: true, data: obj };
    }
  }
  return { ok: false, error: 'Пасажир не знайдений: ' + paxId };
}

// getPassengersByTrip — Всі пасажири прив'язані до рейсу
function apiGetPassengersByTrip(params) {
  var calId = params.cal_id || '';
  if (!calId) return { ok: false, error: 'cal_id не вказано' };
  var results = [];

  [SHEETS.PAX_UE, SHEETS.PAX_EU].forEach(function(shName) {
    var sh = getSheet(shName);
    if (!sh) return;
    var info = getAllData(sh);
    var calIdx = info.headers.indexOf('CAL_ID');
    if (calIdx === -1) return;
    for (var i = 0; i < info.data.length; i++) {
      if (String(info.data[i][calIdx]) === String(calId)) {
        results.push(paxObjFromData(info.headers, info.data[i], shName, DATA_START + i));
      }
    }
  });

  return { ok: true, count: results.length, data: results };
}

// getStats — Статистика (лічильники)
function apiGetStats(params) {
  var all = 0, ue = 0, eu = 0;
  var byStatus = {}, byPay = {}, noTrip = 0, withTrip = 0;
  var totalDebt = 0;

  function countSheet(name, dir) {
    var sh = getSheet(name);
    if (!sh) return;
    var info = getAllData(sh);
    var statusIdx = info.headers.indexOf('Статус ліда');
    var payIdx = info.headers.indexOf('Статус оплати');
    var crmIdx = info.headers.indexOf('Статус CRM');
    var calIdx = info.headers.indexOf('CAL_ID');

    for (var i = 0; i < info.data.length; i++) {
      if (!info.data[i][0] && !info.data[i][5]) continue;
      var crm = String(info.data[i][crmIdx] || 'Активний');
      if (crm === 'Архів') continue;

      all++;
      if (dir === 'ue') ue++; else eu++;

      var st = String(info.data[i][statusIdx] || 'Новий');
      byStatus[st] = (byStatus[st] || 0) + 1;

      var pay = String(info.data[i][payIdx] || 'Не оплачено');
      byPay[pay] = (byPay[pay] || 0) + 1;

      var cal = String(info.data[i][calIdx] || '').trim();
      if (cal) withTrip++; else noTrip++;

      var obj = rowToObj(info.headers, info.data[i]);
      totalDebt += calcDebt(obj);
    }
  }

  countSheet(SHEETS.PAX_UE, 'ue');
  countSheet(SHEETS.PAX_EU, 'eu');

  var tripCount = 0;
  var calSheet = getSheet(SHEETS.CALENDAR);
  if (calSheet) {
    var calInfo = getAllData(calSheet);
    for (var i = 0; i < calInfo.data.length; i++) {
      if (calInfo.data[i][0]) tripCount++;
    }
  }

  return {
    ok: true,
    total: all, ue: ue, eu: eu,
    byStatus: byStatus, byPay: byPay,
    noTrip: noTrip, withTrip: withTrip,
    totalDebt: totalDebt, trips: tripCount
  };
}


// ══════════════════════════════════════════════════════════════
// 2. PASSENGERS — CREATE
// ══════════════════════════════════════════════════════════════

// addPassenger — Додати нового пасажира
function apiAddPassenger(params) {
  var shName = sheetAlias(params.sheet || 'ue');
  var sh = getSheet(shName);
  if (!sh) return { ok: false, error: 'Аркуш не знайдено: ' + shName };
  var headers = getHeaders(sh);
  var d = params.data || {};

  var paxId = genId('PAX');
  var obj = {};
  PAX_COLS.forEach(function(c) { obj[c] = ''; });

  obj['PAX_ID'] = paxId;
  obj['Дата створення'] = today();
  obj['SOURCE_SHEET'] = shName;
  obj['Напрям'] = shName === SHEETS.PAX_EU ? 'Європа-УК' : 'Україна-ЄВ';
  obj['Піб'] = d.name || '';
  obj['Телефон пасажира'] = d.phone || '';
  obj['Телефон реєстратора'] = d.phoneReg || '';
  obj['Кількість місць'] = d.seats || 1;
  obj['Адреса відправки'] = d.from || '';
  obj['Адреса прибуття'] = d.to || '';
  obj['Дата виїзду'] = d.date || '';
  obj['Таймінг'] = d.timing || '';
  obj['Ціна квитка'] = d.price || '';
  obj['Валюта квитка'] = d.currency || 'UAH';
  obj['Завдаток'] = d.deposit || '';
  obj['Валюта завдатку'] = d.currencyDeposit || d.currency || 'UAH';
  obj['Вага багажу'] = d.weight || '';
  obj['Ціна багажу'] = d.weightPrice || '';
  obj['Валюта багажу'] = d.currencyWeight || d.currency || 'UAH';
  obj['Статус оплати'] = d.payStatus || 'Не оплачено';
  obj['Статус ліда'] = d.leadStatus || 'Новий';
  obj['Статус CRM'] = 'Активний';
  obj['Тег'] = d.tag || '';
  obj['Примітка'] = d.note || '';
  obj['Примітка СМС'] = d.noteSms || '';

  var row = objToRow(headers, obj);
  sh.appendRow(row);

  // Автопідказка рейсу (suggestTrip) — якщо є дата
  var suggested = [];
  if (d.date) {
    suggested = findMatchingTrips(d.date, obj['Напрям']);
  }

  return { ok: true, pax_id: paxId, suggestedTrips: suggested };
}

// clonePassenger — Клонувати ліда (дубль для іншої дати)
function apiClonePassenger(params) {
  var paxId = params.pax_id;
  if (!paxId) return { ok: false, error: 'pax_id не вказано' };

  var shName = resolveSheet(params);
  var sh = getSheet(shName);
  if (!sh) return { ok: false, error: 'Аркуш не знайдено' };

  var found = findRow(sh, 'PAX_ID', paxId);
  if (!found) return { ok: false, error: 'Пасажир не знайдений' };

  var obj = rowToObj(found.headers, found.data);
  var newId = genId('PAX');

  // Копіюємо все крім системних
  obj['PAX_ID'] = newId;
  obj['Дата створення'] = today();
  obj['Статус ліда'] = 'Новий';
  obj['Статус оплати'] = 'Не оплачено';
  obj['Статус CRM'] = 'Активний';
  obj['CAL_ID'] = '';
  obj['Місце в авто'] = '';
  obj['Номер авто'] = '';
  obj['Завдаток'] = '';
  obj['Борг'] = '';
  obj['BOOKING_ID'] = '';
  obj['DATE_ARCHIVE'] = '';
  obj['ARCHIVED_BY'] = '';
  obj['ARCHIVE_REASON'] = '';
  obj['ARCHIVE_ID'] = '';

  // Нова дата якщо передана
  if (params.new_date) obj['Дата виїзду'] = params.new_date;

  sh.appendRow(objToRow(found.headers, obj));

  return { ok: true, pax_id: newId, cloned_from: paxId };
}

// checkDuplicates — Перевірка дублікатів
function apiCheckDuplicates(params) {
  function checkSheet(shName) {
    var sh = getSheet(shName);
    if (!sh) return null;
    var info = getAllData(sh);
    var pibIdx = info.headers.indexOf('Піб');
    var phoneIdx = info.headers.indexOf('Телефон пасажира');
    var dateIdx = info.headers.indexOf('Дата виїзду');
    var idIdx = info.headers.indexOf('PAX_ID');

    var pib = (params.pib || '').toLowerCase().trim();
    var phone = (params.phone || '').trim();
    var date = (params.date || '').trim();

    for (var i = 0; i < info.data.length; i++) {
      var rPib = String(info.data[i][pibIdx] || '').toLowerCase().trim();
      var rPhone = String(info.data[i][phoneIdx] || '').trim();
      var rDate = String(info.data[i][dateIdx] || '').trim();

      if (rPhone === phone && rPib === pib && rDate === date && phone && pib && date) {
        return { exact: true, soft: false, match: {
          pax_id: info.data[i][idIdx], pib: info.data[i][pibIdx], phone: rPhone
        }};
      }
      if (rPhone === phone && rPib === pib && phone && pib) {
        return { exact: false, soft: true, match: {
          pax_id: info.data[i][idIdx], pib: info.data[i][pibIdx], phone: rPhone
        }};
      }
    }
    return null;
  }

  var r1 = checkSheet(SHEETS.PAX_UE);
  if (r1) return r1;
  var r2 = checkSheet(SHEETS.PAX_EU);
  if (r2) return r2;
  return { exact: false, soft: false };
}


// ══════════════════════════════════════════════════════════════
// 3. PASSENGERS — UPDATE
// ══════════════════════════════════════════════════════════════

// updateField — Оновити одне поле
function apiUpdateField(params) {
  var shName = resolveSheet(params);
  var sh = getSheet(shName);
  if (!sh) return { ok: false, error: 'Аркуш не знайдено: ' + shName };
  var found = findRow(sh, 'PAX_ID', params.pax_id);
  if (!found) return { ok: false, error: 'Запис не знайдено: ' + params.pax_id };

  var colIdx = found.headers.indexOf(params.col);
  if (colIdx === -1) return { ok: false, error: 'Колонка не знайдена: ' + params.col };

  sh.getRange(found.rowNum, colIdx + 1).setValue(params.value);

  // Перерахунок боргу + автооновлення статусу оплати
  if (['Ціна квитка','Ціна багажу','Завдаток'].indexOf(params.col) !== -1) {
    var obj = rowToObj(found.headers, found.data);
    obj[params.col] = params.value;
    var debt = calcDebt(obj);
    var debtIdx = found.headers.indexOf('Борг');
    if (debtIdx !== -1) {
      sh.getRange(found.rowNum, debtIdx + 1).setValue(debt);
    }

    // Автооновлення Статус оплати (Y)
    var dep = parseFloat(obj['Завдаток']) || 0;
    var price = parseFloat(obj['Ціна квитка']) || 0;
    var newPayStatus = 'Не оплачено';
    if (dep > 0 && debt > 0) newPayStatus = 'Частково';
    if (dep > 0 && debt === 0) newPayStatus = 'Оплачено';
    var payStatusIdx = found.headers.indexOf('Статус оплати');
    if (payStatusIdx !== -1) {
      sh.getRange(found.rowNum, payStatusIdx + 1).setValue(newPayStatus);
    }

    // Автозапис платежу в Finance_crm при зміні Завдаток (S)
    if (params.col === 'Завдаток') {
      var oldDep = parseFloat(found.data[found.headers.indexOf('Завдаток')]) || 0;
      var newDep = parseFloat(params.value) || 0;
      var delta = newDep - oldDep;
      if (delta !== 0) {
        // Зчитуємо актуальні дані рядка після оновлення
        var updatedRow = sh.getRange(found.rowNum, 1, 1, found.headers.length).getValues()[0];
        var paxData = rowToObj(found.headers, updatedRow);
        paxData._sheet = shName;
        addPayment(paxData, params.manager || '', delta);
      }
    }
  }

  return { ok: true };
}

// updatePassenger — Оновити ВСІ поля пасажира за раз
function apiUpdatePassenger(params) {
  var shName = resolveSheet(params);
  var sh = getSheet(shName);
  if (!sh) return { ok: false, error: 'Аркуш не знайдено' };
  var found = findRow(sh, 'PAX_ID', params.pax_id);
  if (!found) return { ok: false, error: 'Запис не знайдено' };

  var obj = rowToObj(found.headers, found.data);
  var d = params.data || {};

  // Оновлюємо тільки передані поля
  var fieldMap = {
    name:'Піб', phone:'Телефон пасажира', phoneReg:'Телефон реєстратора',
    seats:'Кількість місць', from:'Адреса відправки', to:'Адреса прибуття',
    date:'Дата виїзду', timing:'Таймінг', price:'Ціна квитка', currency:'Валюта квитка',
    deposit:'Завдаток', currencyDeposit:'Валюта завдатку',
    weight:'Вага багажу', weightPrice:'Ціна багажу', currencyWeight:'Валюта багажу',
    payStatus:'Статус оплати', leadStatus:'Статус ліда', crmStatus:'Статус CRM',
    tag:'Тег', note:'Примітка', noteSms:'Примітка СМС',
    vehicle:'Номер авто', seatInCar:'Місце в авто', calId:'CAL_ID'
  };

  for (var key in d) {
    var col = fieldMap[key] || key;
    if (obj.hasOwnProperty(col)) {
      obj[col] = d[key];
    }
  }

  obj['Борг'] = calcDebt(obj);
  var row = objToRow(found.headers, obj);
  sh.getRange(found.rowNum, 1, 1, row.length).setValues([row]);

  return { ok: true };
}

// bulkUpdateField — Масове оновлення одного поля для N пасажирів
function apiBulkUpdateField(params) {
  var paxIds = params.pax_ids || [];
  var col = params.col || '';
  var value = params.value;
  if (!col) return { ok: false, error: 'Не вказано колонку' };

  var updated = 0;

  [SHEETS.PAX_UE, SHEETS.PAX_EU].forEach(function(shName) {
    var sh = getSheet(shName);
    if (!sh) return;
    var info = getAllData(sh);
    var idIdx = info.headers.indexOf('PAX_ID');
    var colIdx = info.headers.indexOf(col);
    if (colIdx === -1) return;

    for (var i = 0; i < info.data.length; i++) {
      if (paxIds.indexOf(String(info.data[i][idIdx])) !== -1) {
        sh.getRange(DATA_START + i, colIdx + 1).setValue(value);
        updated++;
      }
    }
  });

  return { ok: true, updated: updated };
}


// ══════════════════════════════════════════════════════════════
// 4. PASSENGERS — TRIP ASSIGNMENT
// ══════════════════════════════════════════════════════════════

// suggestTrips — Автопідказка рейсів по даті + напряму пасажира
function apiSuggestTrips(params) {
  var date = params.date || '';
  var direction = params.direction || '';
  if (!date) return { ok: true, data: [] };

  var suggested = findMatchingTrips(date, direction);
  return { ok: true, data: suggested };
}

// Внутрішня: пошук рейсів що збігаються по даті
function findMatchingTrips(date, direction) {
  var calSheet = getSheet(SHEETS.CALENDAR);
  if (!calSheet) return [];

  var info = getAllData(calSheet);
  var results = [];
  var dirLower = String(direction || '').toLowerCase();
  var isUE = dirLower.indexOf('ук') !== -1 || dirLower.indexOf('ua') !== -1 || dirLower.indexOf('україна') !== -1;
  var isEU = dirLower.indexOf('єв') !== -1 || dirLower.indexOf('eu') !== -1 || dirLower.indexOf('європа') !== -1;

  for (var i = 0; i < info.data.length; i++) {
    var obj = rowToObj(info.headers, info.data[i]);
    if (!obj['CAL_ID']) continue;
    if (obj['Статус рейсу'] === 'Архів' || obj['Статус рейсу'] === 'Виконано') continue;

    // Порівнюємо дату
    if (String(obj['Дата рейсу']).trim() !== String(date).trim()) continue;

    // Порівнюємо напрям (якщо вказано)
    if (direction) {
      var tDir = String(obj['Напрямок'] || '').toLowerCase();
      var tIsUE = tDir.indexOf('ук') !== -1 || tDir.indexOf('ua') !== -1 || tDir.indexOf('україна') !== -1;
      var tIsEU = tDir.indexOf('єв') !== -1 || tDir.indexOf('eu') !== -1 || tDir.indexOf('європа') !== -1;
      if (isUE && !tIsUE) continue;
      if (isEU && !tIsEU) continue;
    }

    results.push({
      cal_id: obj['CAL_ID'],
      auto_id: obj['AUTO_ID'] || '',
      auto_name: obj['Назва авто'] || '',
      layout: obj['Тип розкладки'] || '',
      date: obj['Дата рейсу'] || '',
      direction: obj['Напрямок'] || '',
      city: obj['Місто'] || '',
      max_seats: parseInt(obj['Макс. місць']) || 0,
      free_seats: parseInt(obj['Вільні місця']) || 0,
      occupied: parseInt(obj['Зайняті місця']) || 0,
      status: obj['Статус рейсу'] || ''
    });
  }

  return results;
}

// assignTrip — Призначити рейс (з валідацією місць + авто-статус)
function apiAssignTrip(params) {
  var calId = params.cal_id || '';
  var paxIds = params.pax_ids || [];
  var seatChoice = params.seat || '';  // конкретне місце або '' (вільна розсадка)
  if (!calId || paxIds.length === 0) return { ok: false, error: 'Не вказано cal_id або pax_ids' };

  // Перевіряємо рейс
  var calSheet = getSheet(SHEETS.CALENDAR);
  if (!calSheet) return { ok: false, error: 'Аркуш Календар не знайдений' };

  var calRow = findRow(calSheet, 'CAL_ID', calId);
  if (!calRow) return { ok: false, error: 'Рейс не знайдено: ' + calId };
  var calObj = rowToObj(calRow.headers, calRow.data);
  var freeSeats = parseInt(calObj['Вільні місця']);
  if (!isNaN(freeSeats) && freeSeats < paxIds.length) {
    return { ok: false, error: 'Недостатньо місць! Вільних: ' + freeSeats + ', потрібно: ' + paxIds.length };
  }

  var updated = 0;

  [SHEETS.PAX_UE, SHEETS.PAX_EU].forEach(function(shName) {
    var sh = getSheet(shName);
    if (!sh) return;
    var info = getAllData(sh);
    var idIdx = info.headers.indexOf('PAX_ID');
    var calIdx = info.headers.indexOf('CAL_ID');
    var statusIdx = info.headers.indexOf('Статус ліда');
    var seatIdx = info.headers.indexOf('Місце в авто');
    var vehicleIdx = info.headers.indexOf('Номер авто');
    if (idIdx === -1 || calIdx === -1) return;

    for (var i = 0; i < info.data.length; i++) {
      if (paxIds.indexOf(String(info.data[i][idIdx])) !== -1) {
        sh.getRange(DATA_START + i, calIdx + 1).setValue(calId);

        // Автоматичний статус
        if (statusIdx !== -1 && String(info.data[i][statusIdx]) === 'Новий') {
          sh.getRange(DATA_START + i, statusIdx + 1).setValue('В роботі');
        }

        // Записуємо авто з рейсу
        if (vehicleIdx !== -1 && calObj['Назва авто']) {
          sh.getRange(DATA_START + i, vehicleIdx + 1).setValue(calObj['Назва авто']);
        }

        // Місце: конкретне або "Вільна розсадка"
        if (seatIdx !== -1) {
          if (seatChoice) {
            sh.getRange(DATA_START + i, seatIdx + 1).setValue(seatChoice);
          } else {
            sh.getRange(DATA_START + i, seatIdx + 1).setValue('Вільна розсадка');
          }
        }

        updated++;
      }
    }
  });

  updateCalendarOccupancy(calId);

  return { ok: true, updated: updated };
}

// unassignTrip — Зняти пасажира з рейсу
function apiUnassignTrip(params) {
  var paxIds = params.pax_ids || [];
  if (params.pax_id) paxIds.push(params.pax_id);
  if (paxIds.length === 0) return { ok: false, error: 'pax_ids не вказано' };

  var affectedCalIds = {};

  [SHEETS.PAX_UE, SHEETS.PAX_EU].forEach(function(shName) {
    var sh = getSheet(shName);
    if (!sh) return;
    var info = getAllData(sh);
    var idIdx = info.headers.indexOf('PAX_ID');
    var calIdx = info.headers.indexOf('CAL_ID');
    var seatIdx = info.headers.indexOf('Місце в авто');
    var vehicleIdx = info.headers.indexOf('Номер авто');
    if (idIdx === -1 || calIdx === -1) return;

    for (var i = 0; i < info.data.length; i++) {
      if (paxIds.indexOf(String(info.data[i][idIdx])) !== -1) {
        var oldCalId = String(info.data[i][calIdx]);
        if (oldCalId) affectedCalIds[oldCalId] = true;

        sh.getRange(DATA_START + i, calIdx + 1).setValue('');
        if (seatIdx !== -1) sh.getRange(DATA_START + i, seatIdx + 1).setValue('');
        if (vehicleIdx !== -1) sh.getRange(DATA_START + i, vehicleIdx + 1).setValue('');
      }
    }
  });

  // Оновлюємо лічильники для всіх задіяних рейсів
  for (var cid in affectedCalIds) {
    updateCalendarOccupancy(cid);
  }

  return { ok: true };
}

// reassignTrip — Пересадити пасажира на інший рейс/авто
function apiReassignTrip(params) {
  var paxId = params.pax_id || '';
  var newCalId = params.new_cal_id || '';
  var newSeat = params.seat || '';
  if (!paxId || !newCalId) return { ok: false, error: 'pax_id та new_cal_id обов\'язкові' };

  // Спочатку знімаємо
  var unRes = apiUnassignTrip({ pax_ids: [paxId] });
  if (!unRes.ok) return unRes;

  // Потім призначаємо
  var asRes = apiAssignTrip({ cal_id: newCalId, pax_ids: [paxId], seat: newSeat });
  return asRes;
}

// Оновити зайнятість рейсу
function updateCalendarOccupancy(calId) {
  var calSheet = getSheet(SHEETS.CALENDAR);
  if (!calSheet) return;

  var found = findRow(calSheet, 'CAL_ID', calId);
  if (!found) return;

  var count = 0;
  var paxNames = [];

  [SHEETS.PAX_UE, SHEETS.PAX_EU].forEach(function(shName) {
    var sh = getSheet(shName);
    if (!sh) return;
    var info = getAllData(sh);
    var calIdx = info.headers.indexOf('CAL_ID');
    var pibIdx = info.headers.indexOf('Піб');
    if (calIdx === -1) return;
    for (var i = 0; i < info.data.length; i++) {
      if (String(info.data[i][calIdx]) === String(calId)) {
        count++;
        if (pibIdx !== -1 && info.data[i][pibIdx]) {
          paxNames.push(String(info.data[i][pibIdx]));
        }
      }
    }
  });

  var obj = rowToObj(found.headers, found.data);
  var maxSeats = parseInt(obj['Макс. місць']) || 0;
  var freeCount = Math.max(0, maxSeats - count);

  var occIdx = found.headers.indexOf('Зайняті місця');
  var freeIdx = found.headers.indexOf('Вільні місця');
  var occListIdx = found.headers.indexOf('Список зайнятих');
  var statusIdx = found.headers.indexOf('Статус рейсу');

  if (occIdx !== -1) calSheet.getRange(found.rowNum, occIdx + 1).setValue(count);
  if (freeIdx !== -1) calSheet.getRange(found.rowNum, freeIdx + 1).setValue(freeCount);
  if (occListIdx !== -1) calSheet.getRange(found.rowNum, occListIdx + 1).setValue(paxNames.join(', '));

  // Автоматично ставимо статус "Повний" якщо місць 0
  if (statusIdx !== -1 && freeCount <= 0 && maxSeats > 0) {
    calSheet.getRange(found.rowNum, statusIdx + 1).setValue('Повний');
  } else if (statusIdx !== -1 && freeCount > 0 && obj['Статус рейсу'] === 'Повний') {
    calSheet.getRange(found.rowNum, statusIdx + 1).setValue('Відкритий');
  }
}


// ══════════════════════════════════════════════════════════════
// 5. PASSENGERS — DELETE / ARCHIVE
// ══════════════════════════════════════════════════════════════

// deletePassenger — Архівує з позначкою "Видалено" (soft delete)
function apiDeletePassenger(params) {
  // Soft delete — архівуємо замість фізичного видалення
  var result = apiArchivePassenger({
    pax_id: params.pax_id,
    pax_ids: params.pax_ids || [],
    reason: 'Видалено',
    archived_by: params.manager || params.archived_by || 'Менеджер',
    sheet: params.sheet
  });

  return { ok: result.ok, message: result.ok ? 'Пасажира переміщено в архів з позначкою "Видалено"' : result.error };
}

// deleteFromSheet — Фізичне видалення рядка з аркуша (для маршрутів)
function apiDeleteFromSheet(params) {
  var shName = params.sheet;
  if (!shName) return { ok: false, error: 'sheet не вказано' };
  // Маршрути живуть в DB.MARHRUT, пасажири в SS_ID
  var ssId = shName.indexOf('Маршрут_') === 0 || shName.indexOf('Відправка_') === 0 || shName.indexOf('Витрати_') === 0 ? DB.MARHRUT : SS_ID;
  var sh = SpreadsheetApp.openById(ssId).getSheetByName(shName);
  if (!sh) return { ok: false, error: 'Аркуш не знайдено: ' + shName };
  var idCol = params.id_col || 'RTE_ID';
  var idVal = params.id_val || params.pax_id || params.rte_id;
  if (!idVal) return { ok: false, error: 'ID не вказано' };
  var found = findRow(sh, idCol, idVal);
  if (!found) return { ok: false, error: 'Запис не знайдено' };
  sh.deleteRow(found.rowNum);
  // Інвалідуємо кеш маршруту і списку
  try {
    var c = CacheService.getScriptCache();
    c.remove('routeSheet_' + shName);
    c.remove('routesList_v2');
  } catch(e) {}
  return { ok: true };
}

// bulkDelete — Масове видалення (soft delete — архівує з позначкою "Видалено")
function apiBulkDelete(params) {
  var paxIds = params.pax_ids || [];
  if (paxIds.length === 0) return { ok: false, error: 'pax_ids порожній' };

  // Soft delete — архівуємо замість видалення
  var result = apiArchivePassenger({
    pax_ids: paxIds,
    reason: 'Видалено (масове)',
    archived_by: params.archived_by || 'Менеджер'
  });

  return { ok: result.ok, deleted: result.archived || 0 };
}

// archivePassenger — Фізичний перенос рядків з Passengers в Archive_crm_v3
function apiArchivePassenger(params) {
  var paxIds = params.pax_ids || [];
  if (params.pax_id) paxIds.push(params.pax_id);
  if (paxIds.length === 0) return { ok: false, error: 'pax_ids не вказано' };

  var reason = params.reason || '';
  var archivedBy = params.archived_by || 'Менеджер';
  var archived = 0;

  // Відкриваємо таблицю архіву
  var archSS = SpreadsheetApp.openById(DB.ARCHIVE);
  var archSheet = archSS.getSheetByName('Архів') || archSS.getSheets()[0];
  var archHeaders = archSheet.getLastColumn() > 0 ? archSheet.getRange(HEADER_ROW, 1, 1, archSheet.getLastColumn()).getValues()[0] : [];

  // Якщо в архіві ще немає заголовків — створити з PAX_COLS + SOURCE_DIR
  if (archHeaders.length === 0 || String(archHeaders[0]).trim() === '') {
    var newHeaders = PAX_COLS.slice();
    if (newHeaders.indexOf('SOURCE_DIR') === -1) newHeaders.push('SOURCE_DIR');
    archSheet.getRange(1, 1, 1, newHeaders.length).setValues([newHeaders]);
    archHeaders = newHeaders;
  }

  [SHEETS.PAX_UE, SHEETS.PAX_EU].forEach(function(shName) {
    var sh = getSheet(shName);
    if (!sh) return;
    var info = getAllData(sh);
    var idIdx = info.headers.indexOf('PAX_ID');
    var crmIdx = info.headers.indexOf('Статус CRM');
    var dateArchIdx = info.headers.indexOf('DATE_ARCHIVE');
    var byIdx = info.headers.indexOf('ARCHIVED_BY');
    var reasonIdx = info.headers.indexOf('ARCHIVE_REASON');
    var archiveIdIdx = info.headers.indexOf('ARCHIVE_ID');

    // Збираємо рядки для видалення (з кінця!)
    var rowsToArchive = [];
    for (var i = 0; i < info.data.length; i++) {
      if (paxIds.indexOf(String(info.data[i][idIdx])) !== -1) {
        // Оновити архівні поля в даних
        var rowData = info.data[i].slice();
        if (crmIdx !== -1) rowData[crmIdx] = 'Архів';
        if (dateArchIdx !== -1) rowData[dateArchIdx] = now();
        if (byIdx !== -1) rowData[byIdx] = archivedBy;
        if (reasonIdx !== -1) rowData[reasonIdx] = reason;
        if (archiveIdIdx !== -1) rowData[archiveIdIdx] = genId('ARC');

        rowsToArchive.push({ rowNum: DATA_START + i, data: rowData, headers: info.headers, sourceDir: shName });
      }
    }

    // Записати в архівну таблицю
    for (var j = 0; j < rowsToArchive.length; j++) {
      var obj = rowToObj(rowsToArchive[j].headers, rowsToArchive[j].data);
      obj['SOURCE_DIR'] = rowsToArchive[j].sourceDir; // Запамʼятати звідки
      var archRow = archHeaders.map(function(h) { return obj[h] !== undefined ? obj[h] : ''; });
      archSheet.appendRow(archRow);
      archived++;
    }

    // Видалити з основної таблиці (з кінця щоб не зсувались рядки)
    rowsToArchive.sort(function(a, b) { return b.rowNum - a.rowNum; });
    for (var k = 0; k < rowsToArchive.length; k++) {
      sh.deleteRow(rowsToArchive[k].rowNum);
    }
  });

  return { ok: true, archived: archived };
}

// restorePassenger — Фізичний перенос рядків з Archive_crm_v3 назад в Passengers
function apiRestorePassenger(params) {
  var paxIds = params.pax_ids || [];
  if (params.pax_id) paxIds.push(params.pax_id);
  if (paxIds.length === 0) return { ok: false, error: 'pax_ids не вказано' };

  var archSS = SpreadsheetApp.openById(DB.ARCHIVE);
  var archSheet = archSS.getSheetByName('Архів') || archSS.getSheets()[0];
  var archInfo = getAllData(archSheet);
  var archIdIdx = archInfo.headers.indexOf('PAX_ID');

  // Зібрати існуючі PAX_ID в обох аркушах пасажирів (захист від дублів)
  var existingIds = {};
  [SHEETS.PAX_UE, SHEETS.PAX_EU].forEach(function(shName) {
    var sh = getSheet(shName);
    if (!sh) return;
    var info = getAllData(sh);
    var idIdx = info.headers.indexOf('PAX_ID');
    if (idIdx === -1) return;
    for (var i = 0; i < info.data.length; i++) {
      var id = String(info.data[i][idIdx]).trim();
      if (id) existingIds[id] = true;
    }
  });

  var restored = 0;
  var skipped = 0;
  var rowsToDelete = [];

  for (var i = 0; i < archInfo.data.length; i++) {
    var archPaxId = String(archInfo.data[i][archIdIdx]);
    if (paxIds.indexOf(archPaxId) !== -1) {
      // Перевірка: чи вже існує в пасажирах (захист від дублів)
      if (existingIds[archPaxId]) {
        // Вже є — тільки видалити з архіву, не додавати повторно
        rowsToDelete.push(DATA_START + i);
        skipped++;
        continue;
      }

      var obj = rowToObj(archInfo.headers, archInfo.data[i]);

      // Визначити куди повертати
      var targetShName = obj['SOURCE_DIR'] || SHEETS.PAX_UE;
      if (targetShName !== SHEETS.PAX_UE && targetShName !== SHEETS.PAX_EU) {
        var dir = obj['Напрям'] || '';
        targetShName = (dir.indexOf('eu-ua') !== -1 || dir.indexOf('EU→UA') !== -1) ? SHEETS.PAX_EU : SHEETS.PAX_UE;
      }

      var targetSh = getSheet(targetShName);
      if (!targetSh) continue;

      // Очистити архівні поля
      obj['Статус CRM'] = 'Активний';
      obj['DATE_ARCHIVE'] = '';
      obj['ARCHIVED_BY'] = '';
      obj['ARCHIVE_REASON'] = '';
      obj['ARCHIVE_ID'] = '';

      // Записати в основну таблицю
      var targetHeaders = getHeaders(targetSh);
      var newRow = targetHeaders.map(function(h) { return obj[h] !== undefined ? obj[h] : ''; });
      targetSh.appendRow(newRow);

      existingIds[archPaxId] = true; // Додали — запам'ятати щоб не дублювати
      rowsToDelete.push(DATA_START + i);
      restored++;
    }
  }

  // Видалити з архівної таблиці (з кінця)
  rowsToDelete.sort(function(a, b) { return b - a; });
  for (var r = 0; r < rowsToDelete.length; r++) {
    archSheet.deleteRow(rowsToDelete[r]);
  }

  return { ok: true, restored: restored, skipped: skipped };
}

// getArchive — Отримати записи з архіву (з пагінацією)
function apiGetArchive(params) {
  var offset = parseInt(params.offset) || 0;
  var limit = parseInt(params.limit) || 0; // 0 = всі (зворотна сумісність)

  var archSS = SpreadsheetApp.openById(DB.ARCHIVE);
  var archSheet = archSS.getSheetByName('Архів') || archSS.getSheets()[0];
  var lastRow = archSheet.getLastRow();
  var total = lastRow >= DATA_START ? lastRow - DATA_START + 1 : 0;

  if (total === 0) {
    return { ok: true, rows: [], total: 0, offset: 0, hasMore: false };
  }

  var headers = getHeaders(archSheet);

  // Якщо limit задано — читаємо тільки потрібний діапазон (швидше)
  if (limit > 0) {
    var startRow = DATA_START + offset;
    var rowsToRead = Math.min(limit, lastRow - startRow + 1);
    if (startRow > lastRow || rowsToRead <= 0) {
      return { ok: true, rows: [], total: total, offset: offset, hasMore: false };
    }
    var data = archSheet.getRange(startRow, 1, rowsToRead, headers.length).getValues();
    var rows = [];
    for (var i = 0; i < data.length; i++) {
      rows.push(rowToObj(headers, data[i]));
    }
    return { ok: true, rows: rows, total: total, offset: offset, hasMore: (offset + limit) < total };
  }

  // Без limit — повертаємо всі (зворотна сумісність)
  var info = getAllData(archSheet);
  var rows = [];
  for (var i = 0; i < info.data.length; i++) {
    rows.push(rowToObj(info.headers, info.data[i]));
  }
  return { ok: true, rows: rows, total: rows.length, offset: 0, hasMore: false };
}

// deleteFromArchive — Вимкнено (записи зберігаються в архіві назавжди)
function apiDeleteFromArchive(params) {
  return { ok: false, error: 'Видалення з архіву вимкнено. Записи зберігаються в архіві назавжди.' };
}

// moveDirection — Перенос пасажира між аркушами UE ↔ EU
function apiMoveDirection(params) {
  var paxId = params.pax_id;
  var targetDir = params.target_dir || '';
  if (!paxId || !targetDir) return { ok: false, error: 'pax_id та target_dir обов\'язкові' };

  var fromName = targetDir === 'eu-ua' ? SHEETS.PAX_UE : SHEETS.PAX_EU;
  var toName = targetDir === 'eu-ua' ? SHEETS.PAX_EU : SHEETS.PAX_UE;

  var fromSh = getSheet(fromName);
  var toSh = getSheet(toName);
  if (!fromSh || !toSh) return { ok: false, error: 'Аркуші не знайдені' };

  var found = findRow(fromSh, 'PAX_ID', paxId);
  if (!found) return { ok: false, error: 'Пасажир не знайдений в ' + fromName };

  var obj = rowToObj(found.headers, found.data);
  obj['Напрям'] = targetDir === 'eu-ua' ? 'Європа-УК' : 'Україна-ЄВ';
  obj['SOURCE_SHEET'] = toName;

  var toHeaders = getHeaders(toSh);
  toSh.appendRow(objToRow(toHeaders, obj));
  fromSh.deleteRow(found.rowNum);

  return { ok: true, moved_to: toName };
}


// ══════════════════════════════════════════════════════════════
// 6. TRIPS — CRUD
// ══════════════════════════════════════════════════════════════

// getTrips
function apiGetTrips(params) {
  var calSheet = getSheet(SHEETS.CALENDAR);
  if (!calSheet) return { ok: true, data: [] };

  var info = getAllData(calSheet);
  var results = [];

  for (var i = 0; i < info.data.length; i++) {
    if (!info.data[i][0]) continue;
    var obj = rowToObj(info.headers, info.data[i]);

    // Приховуємо видалені рейси (soft delete)
    if (obj['Статус рейсу'] === 'Видалено') continue;

    if (params.filter) {
      if (params.filter.status && params.filter.status !== 'all') {
        if (obj['Статус рейсу'] !== params.filter.status) continue;
      }
      if (params.filter.dir && params.filter.dir !== 'all') {
        var d = String(obj['Напрямок'] || '').toLowerCase();
        if (params.filter.dir === 'ua-eu' && !d.match(/ук|ua/)) continue;
        if (params.filter.dir === 'eu-ua' && !d.match(/єв|eu/)) continue;
      }
      if (params.filter.date) {
        if (String(obj['Дата рейсу']).trim() !== String(params.filter.date).trim()) continue;
      }
      if (params.filter.auto_id) {
        if (obj['AUTO_ID'] !== params.filter.auto_id) continue;
      }
    }

    results.push({
      cal_id: obj['CAL_ID'] || '',
      rte_id: obj['RTE_ID'] || '',
      auto_id: obj['AUTO_ID'] || '',
      auto_name: obj['Назва авто'] || '',
      layout: obj['Тип розкладки'] || '',
      date: obj['Дата рейсу'] || '',
      direction: obj['Напрямок'] || '',
      city: obj['Місто'] || '',
      max_seats: parseInt(obj['Макс. місць']) || 0,
      free_seats: parseInt(obj['Вільні місця']) || 0,
      occupied: parseInt(obj['Зайняті місця']) || 0,
      free_list: obj['Список вільних'] || '',
      occupied_list: obj['Список зайнятих'] || '',
      paired_id: obj['PAIRED_CAL_ID'] || '',
      status: obj['Статус рейсу'] || 'Відкритий',
      _rowNum: DATA_START + i
    });
  }

  return { ok: true, count: results.length, data: results };
}

// getTrip — Один рейс
function apiGetTrip(params) {
  var calId = params.cal_id;
  if (!calId) return { ok: false, error: 'cal_id не вказано' };

  var calSheet = getSheet(SHEETS.CALENDAR);
  if (!calSheet) return { ok: false, error: 'Аркуш Календар не знайдений' };
  var found = findRow(calSheet, 'CAL_ID', calId);
  if (!found) return { ok: false, error: 'Рейс не знайдено' };

  var obj = rowToObj(found.headers, found.data);

  // Додатково: хто в рейсі
  var paxRes = apiGetPassengersByTrip({ cal_id: calId });

  return {
    ok: true,
    trip: {
      cal_id: obj['CAL_ID'], auto_id: obj['AUTO_ID'], auto_name: obj['Назва авто'],
      layout: obj['Тип розкладки'], date: obj['Дата рейсу'], direction: obj['Напрямок'],
      city: obj['Місто'], max_seats: parseInt(obj['Макс. місць']) || 0,
      free_seats: parseInt(obj['Вільні місця']) || 0, occupied: parseInt(obj['Зайняті місця']) || 0,
      status: obj['Статус рейсу'], paired_id: obj['PAIRED_CAL_ID'] || ''
    },
    passengers: paxRes.data || []
  };
}

// createTrip
function apiCreateTrip(params) {
  var calSheet = getSheet(SHEETS.CALENDAR);
  var autoSheet = getSheet(SHEETS.AUTOPARK);
  if (!calSheet || !autoSheet) return { ok: false, error: 'Аркуші не знайдені' };

  var calHeaders = getHeaders(calSheet);
  var autoHeaders = getHeaders(autoSheet);

  var city = params.city || '';
  var dir = params.dir || 'ua-eu';
  var vehicles = params.vehicles || [];
  var dates = params.dates || [];
  var calIds = [];

  var dirText = dir === 'eu-ua' ? 'Європа-УК' : dir === 'bt' ? 'Загальний' : 'Україна-ЄВ';

  for (var v = 0; v < vehicles.length; v++) {
    var veh = vehicles[v];
    var autoId = genId('AUTO');
    var layout = veh.layout || '1-3-3';
    var seats = parseInt(veh.seats) || 7;
    var name = veh.name || 'Авто ' + (v + 1);
    var plate = veh.plate || '';

    var seatList = [];
    if (layout === 'bus') {
      for (var s = 1; s <= seats; s++) seatList.push({ seat: String(s), type: 'Пасажир' });
    } else {
      var layoutDef = LAYOUTS[layout];
      if (layoutDef) {
        for (var s = 0; s < layoutDef.length; s++) seatList.push(layoutDef[s]);
      }
    }
    if (veh.reserve) seatList.push({ seat: 'R1', type: 'Резервне' });

    // Autopark rows
    for (var s = 0; s < seatList.length; s++) {
      var autoObj = {};
      AUTO_COLS.forEach(function(c) { autoObj[c] = ''; });
      autoObj['AUTO_ID'] = autoId;
      autoObj['Назва авто'] = name;
      autoObj['Держ. номер'] = plate;
      autoObj['Тип розкладки'] = layout;
      autoObj['Місткість'] = seats;
      autoObj['Місце'] = seatList[s].seat;
      autoObj['Тип місця'] = seatList[s].type;
      autoObj['Статус місця'] = 'Вільне';
      autoObj['Статус авто'] = 'Активний';

      // Prices if provided
      if (veh.prices) {
        if (veh.prices.UAH) autoObj['Ціна UAH'] = veh.prices.UAH;
        if (veh.prices.CHF) autoObj['Ціна CHF'] = veh.prices.CHF;
        if (veh.prices.EUR) autoObj['Ціна EUR'] = veh.prices.EUR;
        if (veh.prices.PLN) autoObj['Ціна PLN'] = veh.prices.PLN;
        if (veh.prices.CZK) autoObj['Ціна CZK'] = veh.prices.CZK;
        if (veh.prices.USD) autoObj['Ціна USD'] = veh.prices.USD;
      }

      autoSheet.appendRow(objToRow(autoHeaders, autoObj));
    }

    var freeList = seatList.filter(function(x) { return x.type !== 'Водій'; }).map(function(x) { return x.seat; }).join(', ');
    var maxPaxSeats = seatList.filter(function(x) { return x.type !== 'Водій'; }).length;

    if (dir === 'bt') {
      for (var d = 0; d < dates.length; d++) {
        var calIdUe = genId('CAL');
        var calIdEu = genId('CAL');

        function makeCalRow(cid, dirTxt, paired) {
          var o = {};
          CAL_COLS.forEach(function(c) { o[c] = ''; });
          o['CAL_ID'] = cid; o['AUTO_ID'] = autoId; o['Назва авто'] = name;
          o['Тип розкладки'] = layout; o['Дата рейсу'] = dates[d];
          o['Напрямок'] = dirTxt; o['Місто'] = city;
          o['Макс. місць'] = maxPaxSeats; o['Вільні місця'] = maxPaxSeats;
          o['Зайняті місця'] = 0; o['Список вільних'] = freeList;
          o['PAIRED_CAL_ID'] = paired; o['Статус рейсу'] = 'Відкритий';
          return o;
        }

        calSheet.appendRow(objToRow(calHeaders, makeCalRow(calIdUe, 'Україна-ЄВ', calIdEu)));
        calSheet.appendRow(objToRow(calHeaders, makeCalRow(calIdEu, 'Європа-УК', calIdUe)));
        calIds.push(calIdUe, calIdEu);
      }
    } else {
      for (var d = 0; d < dates.length; d++) {
        var calId = genId('CAL');
        var calObj = {};
        CAL_COLS.forEach(function(c) { calObj[c] = ''; });
        calObj['CAL_ID'] = calId; calObj['AUTO_ID'] = autoId; calObj['Назва авто'] = name;
        calObj['Тип розкладки'] = layout; calObj['Дата рейсу'] = dates[d];
        calObj['Напрямок'] = dirText; calObj['Місто'] = city;
        calObj['Макс. місць'] = maxPaxSeats; calObj['Вільні місця'] = maxPaxSeats;
        calObj['Зайняті місця'] = 0; calObj['Список вільних'] = freeList;
        calObj['Статус рейсу'] = 'Відкритий';
        calSheet.appendRow(objToRow(calHeaders, calObj));
        calIds.push(calId);
      }
    }
  }

  return { ok: true, cal_ids: calIds };
}

// updateTrip
function apiUpdateTrip(params) {
  var calSheet = getSheet(SHEETS.CALENDAR);
  if (!calSheet) return { ok: false, error: 'Аркуш не знайдений' };

  var found = findRow(calSheet, 'CAL_ID', params.cal_id);
  if (!found) return { ok: false, error: 'Рейс не знайдено: ' + params.cal_id };

  var obj = rowToObj(found.headers, found.data);

  if (params.city !== undefined) obj['Місто'] = params.city;
  if (params.dir) {
    if (params.dir === 'ua-eu') obj['Напрямок'] = 'Україна-ЄВ';
    else if (params.dir === 'eu-ua') obj['Напрямок'] = 'Європа-УК';
    else obj['Напрямок'] = 'Загальний';
  }
  if (params.date) obj['Дата рейсу'] = params.date;
  if (params.dates && params.dates.length > 0) obj['Дата рейсу'] = params.dates[0];
  if (params.auto_name !== undefined) obj['Назва авто'] = params.auto_name;
  if (params.status) obj['Статус рейсу'] = params.status;
  if (params.max_seats !== undefined) obj['Макс. місць'] = params.max_seats;

  var row = objToRow(found.headers, obj);
  calSheet.getRange(found.rowNum, 1, 1, row.length).setValues([row]);

  return { ok: true };
}

// archiveTrip
function apiArchiveTrip(params) {
  var calSheet = getSheet(SHEETS.CALENDAR);
  if (!calSheet) return { ok: false, error: 'Аркуш Календар не знайдений' };

  var found = findRow(calSheet, 'CAL_ID', params.cal_id);
  if (!found) return { ok: false, error: 'Рейс не знайдено' };

  var obj = rowToObj(found.headers, found.data);

  // Переносимо рядок в Archive_crm → аркуш "Архів рейсів"
  var archSS = SpreadsheetApp.openById(DB.ARCHIVE);
  var archSheet = archSS.getSheetByName('Архів рейсів');
  if (!archSheet) {
    archSheet = archSS.insertSheet('Архів рейсів');
    archSheet.getRange(1, 1, 1, CAL_COLS.length + 3).setValues([CAL_COLS.concat(['DATE_ARCHIVE', 'ARCHIVED_BY', 'ARCHIVE_REASON'])]);
  }
  var archHeaders = archSheet.getRange(1, 1, 1, archSheet.getLastColumn()).getValues()[0];
  obj['DATE_ARCHIVE'] = Utilities.formatDate(new Date(), 'Europe/Kiev', 'dd.MM.yyyy HH:mm');
  obj['ARCHIVED_BY'] = params.archived_by || 'Менеджер';
  obj['ARCHIVE_REASON'] = 'Архівовано';
  var row = archHeaders.map(function(h) { return obj[h] || ''; });
  archSheet.appendRow(row);

  // Видаляємо рядок з Календар
  calSheet.deleteRow(found.rowNum);

  // Архівувати і пасажирів рейсу якщо потрібно
  if (params.archive_passengers) {
    var paxRes = apiGetPassengersByTrip({ cal_id: params.cal_id });
    if (paxRes.ok && paxRes.data.length > 0) {
      var ids = paxRes.data.map(function(p) { return p['PAX_ID']; });
      apiArchivePassenger({ pax_ids: ids, reason: 'Рейс архівовано', archived_by: params.archived_by || 'Система' });
    }
  }

  return { ok: true };
}

// deleteTrip — переносить в архів з позначкою "Видалено" і видаляє з Календар
function apiDeleteTrip(params) {
  var calSheet = getSheet(SHEETS.CALENDAR);
  if (!calSheet) return { ok: false, error: 'Аркуш Календар не знайдений' };

  var found = findRow(calSheet, 'CAL_ID', params.cal_id);
  if (!found) return { ok: false, error: 'Рейс не знайдено' };

  var obj = rowToObj(found.headers, found.data);

  // Знімаємо пасажирів з рейсу
  clearCalIdInPassengers(params.cal_id);

  // Переносимо рядок в Archive_crm → аркуш "Архів рейсів"
  var archSS = SpreadsheetApp.openById(DB.ARCHIVE);
  var archSheet = archSS.getSheetByName('Архів рейсів');
  if (!archSheet) {
    archSheet = archSS.insertSheet('Архів рейсів');
    archSheet.getRange(1, 1, 1, CAL_COLS.length + 3).setValues([CAL_COLS.concat(['DATE_ARCHIVE', 'ARCHIVED_BY', 'ARCHIVE_REASON'])]);
  }
  var archHeaders = archSheet.getRange(1, 1, 1, archSheet.getLastColumn()).getValues()[0];
  obj['DATE_ARCHIVE'] = Utilities.formatDate(new Date(), 'Europe/Kiev', 'dd.MM.yyyy HH:mm');
  obj['ARCHIVED_BY'] = params.archived_by || 'Менеджер';
  obj['ARCHIVE_REASON'] = 'Видалено';
  var row = archHeaders.map(function(h) { return obj[h] || ''; });
  archSheet.appendRow(row);

  // Видаляємо рядок з Календар
  calSheet.deleteRow(found.rowNum);

  return { ok: true };
}

// duplicateTrip — Дублювання рейсу на нову дату
function apiDuplicateTrip(params) {
  var calId = params.cal_id;
  var newDates = params.dates || [];
  if (!calId || newDates.length === 0) return { ok: false, error: 'cal_id та dates обов\'язкові' };

  var calSheet = getSheet(SHEETS.CALENDAR);
  if (!calSheet) return { ok: false, error: 'Аркуш не знайдений' };

  var found = findRow(calSheet, 'CAL_ID', calId);
  if (!found) return { ok: false, error: 'Рейс не знайдено' };

  var calHeaders = getHeaders(calSheet);
  var obj = rowToObj(found.headers, found.data);
  var newCalIds = [];

  for (var i = 0; i < newDates.length; i++) {
    var newId = genId('CAL');
    var newObj = {};
    CAL_COLS.forEach(function(c) { newObj[c] = obj[c] || ''; });
    newObj['CAL_ID'] = newId;
    newObj['Дата рейсу'] = newDates[i];
    newObj['Зайняті місця'] = 0;
    newObj['Вільні місця'] = parseInt(obj['Макс. місць']) || 0;
    newObj['Список зайнятих'] = '';
    newObj['Статус рейсу'] = 'Відкритий';
    newObj['PAIRED_CAL_ID'] = '';

    calSheet.appendRow(objToRow(calHeaders, newObj));
    newCalIds.push(newId);
  }

  return { ok: true, cal_ids: newCalIds };
}

function clearCalIdInPassengers(calId) {
  [SHEETS.PAX_UE, SHEETS.PAX_EU].forEach(function(shName) {
    var sh = getSheet(shName);
    if (!sh) return;
    var info = getAllData(sh);
    var calIdx = info.headers.indexOf('CAL_ID');
    var seatIdx = info.headers.indexOf('Місце в авто');
    var vehicleIdx = info.headers.indexOf('Номер авто');
    if (calIdx === -1) return;

    for (var i = 0; i < info.data.length; i++) {
      if (String(info.data[i][calIdx]) === String(calId)) {
        sh.getRange(DATA_START + i, calIdx + 1).setValue('');
        if (seatIdx !== -1) sh.getRange(DATA_START + i, seatIdx + 1).setValue('');
        if (vehicleIdx !== -1) sh.getRange(DATA_START + i, vehicleIdx + 1).setValue('');
      }
    }
  });
}


// ══════════════════════════════════════════════════════════════
// 7. AUTOPARK
// ══════════════════════════════════════════════════════════════

// getAutopark — Список всіх авто
function apiGetAutopark(params) {
  var sh = getSheet(SHEETS.AUTOPARK);
  if (!sh) return { ok: true, data: [] };

  var info = getAllData(sh);
  var results = [];
  var autoMap = {};

  for (var i = 0; i < info.data.length; i++) {
    var obj = rowToObj(info.headers, info.data[i]);
    if (!obj['AUTO_ID']) continue;

    var aid = obj['AUTO_ID'];
    if (!autoMap[aid]) {
      autoMap[aid] = {
        auto_id: aid,
        name: obj['Назва авто'] || '',
        plate: obj['Держ. номер'] || '',
        layout: obj['Тип розкладки'] || '',
        capacity: parseInt(obj['Місткість']) || 0,
        status: obj['Статус авто'] || '',
        seats: []
      };
    }
    autoMap[aid].seats.push({
      seat: obj['Місце'] || '',
      type: obj['Тип місця'] || '',
      status: obj['Статус місця'] || '',
      prices: {
        UAH: obj['Ціна UAH'] || '',
        CHF: obj['Ціна CHF'] || '',
        EUR: obj['Ціна EUR'] || '',
        PLN: obj['Ціна PLN'] || '',
        CZK: obj['Ціна CZK'] || '',
        USD: obj['Ціна USD'] || ''
      }
    });
  }

  for (var k in autoMap) results.push(autoMap[k]);

  return { ok: true, data: results };
}

// getAutoSeats — Місця конкретного авто (для вибору місця менеджером)
function apiGetAutoSeats(params) {
  var autoId = params.auto_id;
  if (!autoId) return { ok: false, error: 'auto_id не вказано' };

  var sh = getSheet(SHEETS.AUTOPARK);
  if (!sh) return { ok: false, error: 'Аркуш Автопарк не знайдений' };

  var rows = findAllRows(sh, 'AUTO_ID', autoId);
  var seats = [];

  for (var i = 0; i < rows.length; i++) {
    var obj = rowToObj(rows[i].headers, rows[i].data);
    seats.push({
      seat: obj['Місце'] || '',
      type: obj['Тип місця'] || '',
      status: obj['Статус місця'] || '',
      prices: {
        UAH: obj['Ціна UAH'] || '',
        CHF: obj['Ціна CHF'] || '',
        EUR: obj['Ціна EUR'] || ''
      }
    });
  }

  return { ok: true, auto_id: autoId, seats: seats };
}


// ══════════════════════════════════════════════════════════════
// 8. SEATING — Розсадка по авто
// ══════════════════════════════════════════════════════════════

// getSeating — Розсадка для конкретного рейсу
function apiGetSeating(params) {
  var calId = params.cal_id;
  if (!calId) return { ok: false, error: 'cal_id не вказано' };

  var sh = getSheet(SHEETS.SEATING);
  if (!sh) return { ok: true, data: [] };

  var rows = findAllRows(sh, 'CAL_ID', calId);
  var results = [];
  for (var i = 0; i < rows.length; i++) {
    results.push(rowToObj(rows[i].headers, rows[i].data));
  }

  return { ok: true, data: results };
}

// assignSeat — Конкретне місце пасажиру (менеджер обрав)
function apiAssignSeat(params) {
  var calId = params.cal_id;
  var paxId = params.pax_id;
  var seat = params.seat;
  if (!calId || !paxId || !seat) return { ok: false, error: 'cal_id, pax_id та seat обов\'язкові' };

  // Отримуємо дані рейсу
  var calSheet = getSheet(SHEETS.CALENDAR);
  if (!calSheet) return { ok: false, error: 'Календар не знайдений' };
  var calRow = findRow(calSheet, 'CAL_ID', calId);
  if (!calRow) return { ok: false, error: 'Рейс не знайдено' };
  var calObj = rowToObj(calRow.headers, calRow.data);

  // Отримуємо дані пасажира
  var paxData = apiGetOne({ pax_id: paxId });
  if (!paxData.ok) return paxData;

  // Записуємо в розсадку
  var seatSheet = getSheet(SHEETS.SEATING);
  if (seatSheet) {
    var seatHeaders = getHeaders(seatSheet);
    var seatObj = {};
    SEAT_COLS.forEach(function(c) { seatObj[c] = ''; });
    seatObj['SEAT_ID'] = genId('SEAT');
    seatObj['CAL_ID'] = calId;
    seatObj['AUTO_ID'] = calObj['AUTO_ID'] || '';
    seatObj['PAX_ID'] = paxId;
    seatObj['Дата'] = calObj['Дата рейсу'] || '';
    seatObj['Напрям'] = calObj['Напрямок'] || '';
    seatObj['Назва авто'] = calObj['Назва авто'] || '';
    seatObj['Тип розкладки'] = calObj['Тип розкладки'] || '';
    seatObj['Місце'] = seat;
    seatObj['Піб'] = paxData.data['Піб'] || '';
    seatObj['Телефон пасажира'] = paxData.data['Телефон пасажира'] || '';
    seatObj['Статус'] = 'Зайняте';
    seatObj['DATE_RESERVED'] = now();
    seatSheet.appendRow(objToRow(seatHeaders, seatObj));
  }

  // Оновлюємо поле "Місце в авто" у пасажира
  apiUpdateField({ pax_id: paxId, col: 'Місце в авто', value: seat });

  return { ok: true, seat_id: seatObj['SEAT_ID'] };
}

// freeSeat — Звільнити місце
function apiFreeSeat(params) {
  var seatId = params.seat_id;
  if (!seatId) return { ok: false, error: 'seat_id не вказано' };

  var sh = getSheet(SHEETS.SEATING);
  if (!sh) return { ok: false, error: 'Аркуш Розсадка не знайдений' };

  var found = findRow(sh, 'SEAT_ID', seatId);
  if (!found) return { ok: false, error: 'Місце не знайдено' };

  var obj = rowToObj(found.headers, found.data);
  var paxId = obj['PAX_ID'];

  sh.deleteRow(found.rowNum);

  // Очистити "Місце в авто" у пасажира
  if (paxId) {
    apiUpdateField({ pax_id: paxId, col: 'Місце в авто', value: '' });
  }

  return { ok: true };
}


// ══════════════════════════════════════════════════════════════
// ROUTES — Читання аркушів маршрутів з Marhrut_crm_v6
// ══════════════════════════════════════════════════════════════

// getRoutesList — ШВИДКИЙ: тільки імена аркушів + кількість рядків (без даних)
function apiGetRoutesList(params) {
  var cache = CacheService.getScriptCache();
  var cacheKey = 'routesList_v2';
  if (params && params.forceRefresh) {
    cache.remove(cacheKey);
  }
  var cached = (!params || !params.forceRefresh) ? cache.get(cacheKey) : null;
  if (cached) {
    return { ok: true, data: JSON.parse(cached), fromCache: true };
  }

  var ss = SpreadsheetApp.openById(DB.MARHRUT);
  var allSheets = ss.getSheets();
  var result = [];

  for (var s = 0; s < allSheets.length; s++) {
    var sheet = allSheets[s];
    var sheetName = sheet.getName();
    // Тільки Маршрут_* аркуші, пропускаємо Відправка_, Витрати_, шаблони, логи
    if (sheetName.indexOf('Маршрут_') !== 0) continue;
    if (sheetName === 'Маршрут_Шаблон') continue;

    var lastRow = sheet.getLastRow();
    var rowCount = lastRow >= 2 ? lastRow - 1 : 0;

    var paxCount = 0, parcelCount = 0;
    if (rowCount > 0) {
      // Читаємо тільки колонку B (Тип запису) — один getRange замість двох
      try {
        var typeData = sheet.getRange(2, 2, rowCount, 1).getValues();
        for (var r = 0; r < typeData.length; r++) {
          var val = String(typeData[r][0] || '');
          if (val.indexOf('Пасажир') >= 0) paxCount++;
          else if (val.indexOf('Посилк') >= 0) parcelCount++;
        }
      } catch(e) { /* аркуш може бути іншої структури */ }
    }

    result.push({ sheetName: sheetName, rowCount: rowCount, paxCount: paxCount, parcelCount: parcelCount });
  }

  cache.put(cacheKey, JSON.stringify(result), 300);
  return { ok: true, data: result };
}

// getRouteSheet — завантажити дані ОДНОГО аркуша маршруту
function apiGetRouteSheet(params) {
  var sheetName = params.sheetName;
  if (!sheetName) return { ok: false, error: 'sheetName is required' };

  var cache = CacheService.getScriptCache();
  var cacheKey = 'routeSheet_' + sheetName;
  if (params.forceRefresh) {
    cache.remove(cacheKey);
  } else {
    var cached = cache.get(cacheKey);
    if (cached) {
      return { ok: true, data: JSON.parse(cached), fromCache: true };
    }
  }

  var ss = SpreadsheetApp.openById(DB.MARHRUT);
  var sheet = ss.getSheetByName(sheetName);
  if (!sheet) return { ok: false, error: 'Sheet not found: ' + sheetName };

  var lastRow = sheet.getLastRow();
  var lastCol = sheet.getLastColumn();
  if (lastRow < 2 || lastCol < 1) {
    return { ok: true, data: { sheetName: sheetName, headers: [], rows: [], rowCount: 0 } };
  }

  // Один getRange для всіх даних (заголовки + дані) — швидше ніж два окремих
  var allData = sheet.getRange(1, 1, lastRow, lastCol).getValues();
  var headers = allData[0].map(function(h) { return String(h).replace(/[\r\n]+/g, ' ').replace(/\s+/g, ' ').trim(); });
  var dataRows = allData.slice(1);

  var rows = [];
  for (var i = 0; i < dataRows.length; i++) {
    var row = dataRows[i];
    var isEmpty = row.every(function(cell) { return String(cell).trim() === ''; });
    if (isEmpty) continue;

    var obj = {};
    for (var j = 0; j < headers.length; j++) {
      if (headers[j]) {
        var val = row[j];
        if (val instanceof Date) {
          obj[headers[j]] = Utilities.formatDate(val, 'Europe/Kiev', 'dd.MM.yyyy');
        } else {
          obj[headers[j]] = String(val !== null && val !== undefined ? val : '');
        }
      }
    }
    rows.push(obj);
  }

  var result = {
    sheetName: sheetName,
    headers: headers.filter(function(h) { return h !== ''; }),
    rows: rows,
    rowCount: rows.length
  };

  // Кеш на 3 хв (дані маршруту можуть змінюватись частіше)
  try { cache.put(cacheKey, JSON.stringify(result), 180); } catch(e) { /* занадто великий для кешу */ }
  return { ok: true, data: result };
}

// getRoutes — ЗАЛИШАЄМО для зворотної сумісності (але повільний)
function apiGetRoutes(params) {
  var ss = SpreadsheetApp.openById(DB.MARHRUT);
  var allSheets = ss.getSheets();
  var result = [];

  for (var s = 0; s < allSheets.length; s++) {
    var sheet = allSheets[s];
    var sheetName = sheet.getName();

    if (/^(Лог|Конфіг|Config|Log|Шаблон|Template)/i.test(sheetName)) continue;

    var lastRow = sheet.getLastRow();
    var lastCol = sheet.getLastColumn();
    if (lastRow < 2 || lastCol < 1) {
      result.push({ sheetName: sheetName, headers: [], rows: [], rowCount: 0 });
      continue;
    }

    var headers = sheet.getRange(1, 1, 1, lastCol).getValues()[0].map(function(h) { return String(h).replace(/[\r\n]+/g, ' ').replace(/\s+/g, ' ').trim(); });
    var dataRows = sheet.getRange(2, 1, lastRow - 1, lastCol).getValues();

    var rows = [];
    for (var i = 0; i < dataRows.length; i++) {
      var row = dataRows[i];
      var isEmpty = row.every(function(cell) { return String(cell).trim() === ''; });
      if (isEmpty) continue;

      var obj = {};
      for (var j = 0; j < headers.length; j++) {
        if (headers[j]) {
          var val = row[j];
          if (val instanceof Date) {
            obj[headers[j]] = Utilities.formatDate(val, 'Europe/Kiev', 'dd.MM.yyyy');
          } else {
            obj[headers[j]] = String(val !== null && val !== undefined ? val : '');
          }
        }
      }
      rows.push(obj);
    }

    result.push({
      sheetName: sheetName,
      headers: headers.filter(function(h) { return h !== ''; }),
      rows: rows,
      rowCount: rows.length
    });
  }

  return { ok: true, data: result };
}


// ══════════════════════════════════════════════════════════════
// ROUTES — CRUD (Маршрути, Відправка, Витрати)
// ══════════════════════════════════════════════════════════════

/**
 * Додати ліди в аркуш маршруту
 * data: { sheetName: 'Маршрут_Цюріх', leads: [{ 'Піб пасажира':'...', ... }] }
 */

// Оновити поле в аркуші маршруту (DB.MARHRUT)
function apiUpdateRouteField(params) {
  var sheetName = params.sheet;
  var rteId = params.rte_id || params.pax_id;
  var col = params.col;
  var value = params.value;
  if (!sheetName || !rteId || !col) return { ok: false, error: 'sheet, rte_id/pax_id, col обов\'язкові' };

  var ss = SpreadsheetApp.openById(DB.MARHRUT);
  var sheet = ss.getSheetByName(sheetName);
  if (!sheet) return { ok: false, error: 'Аркуш не знайдено: ' + sheetName };

  var lastRow = sheet.getLastRow();
  var lastCol = sheet.getLastColumn();
  if (lastRow < 2 || lastCol < 1) return { ok: false, error: 'Аркуш порожній' };

  var headers = sheet.getRange(1, 1, 1, lastCol).getValues()[0].map(function(h) { return String(h).replace(/[\r\n]+/g, ' ').replace(/\s+/g, ' ').trim(); });
  var colIdx = headers.indexOf(col);
  if (colIdx === -1) return { ok: false, error: 'Колонка не знайдена: ' + col };

  var rteIdCol = headers.indexOf('RTE_ID');
  var paxPkgCol = headers.indexOf('PAX_ID/PKG_ID');
  var data = sheet.getRange(2, 1, lastRow - 1, lastCol).getValues();
  var rowNum = -1;

  for (var i = 0; i < data.length; i++) {
    var rId = rteIdCol !== -1 ? String(data[i][rteIdCol]).trim() : '';
    var ppId = paxPkgCol !== -1 ? String(data[i][paxPkgCol]).trim() : '';
    if (rId === rteId || ppId === rteId) {
      rowNum = i + 2;
      break;
    }
  }

  if (rowNum === -1) return { ok: false, error: 'Запис не знайдено: ' + rteId };

  sheet.getRange(rowNum, colIdx + 1).setValue(value);

  // Інвалідуємо кеш маршруту
  try {
    var cache = CacheService.getScriptCache();
    cache.remove('routeSheet_' + sheetName);
  } catch(e) { /* ignore */ }

  return { ok: true };
}

function apiAddToRoute(params) {
  var sheetName = params.sheetName;
  var leads = params.leads;
  if (!sheetName || !leads || !leads.length) {
    return { ok: false, error: 'sheetName і leads обов\'язкові' };
  }

  var ss = SpreadsheetApp.openById(DB.MARHRUT);
  var sheet = ss.getSheetByName(sheetName);
  if (!sheet) return { ok: false, error: 'Аркуш "' + sheetName + '" не знайдено' };

  var lastCol = sheet.getLastColumn();
  if (lastCol < 1) return { ok: false, error: 'Аркуш порожній (немає заголовків)' };

  var headers = sheet.getRange(1, 1, 1, lastCol).getValues()[0].map(function(h) { return String(h).replace(/[\r\n]+/g, ' ').replace(/\s+/g, ' ').trim(); });

  var added = 0;
  for (var i = 0; i < leads.length; i++) {
    var lead = leads[i];
    // Маппінг: шукаємо значення за точним ключем або нормалізованим
    var row = headers.map(function(h) {
      if (lead[h] !== undefined && lead[h] !== null) return lead[h];
      // Пошук без урахування пробілів/регістру
      var keys = Object.keys(lead);
      for (var k = 0; k < keys.length; k++) {
        if (keys[k].trim().toLowerCase() === h.trim().toLowerCase()) return lead[keys[k]];
      }
      return '';
    });
    // Перевірка що рядок не повністю порожній
    var hasData = row.some(function(v) { return String(v).trim() !== ''; });
    if (hasData) {
      sheet.appendRow(row);
      added++;
    }
  }

  // Інвалідуємо кеш маршруту щоб обидва CRM бачили актуальні дані
  try {
    var cache = CacheService.getScriptCache();
    cache.remove('routeSheet_' + sheetName);
    cache.remove('routesList_v2');
  } catch(e) { /* ignore */ }

  return { ok: true, added: added };
}

/**
 * Створити новий маршрут (копіює 3 шаблони: Маршрут_, Відправка_, Витрати_)
 * data: { name: 'Цюріх_20260310' }
 */
function apiCreateRoute(params) {
  var name = params.name;
  if (!name || !name.trim()) return { ok: false, error: 'Назва маршруту обов\'язкова' };
  name = name.trim();

  var ss = SpreadsheetApp.openById(DB.MARHRUT);

  // Перевірка чи вже існує
  if (ss.getSheetByName('Маршрут_' + name)) {
    return { ok: false, error: 'Маршрут "' + name + '" вже існує' };
  }

  // Копіюємо шаблони
  var tplRoute = ss.getSheetByName('Маршрут_Шаблон');
  var tplDispatch = ss.getSheetByName('Відправка_Шаблон');
  var tplExpenses = ss.getSheetByName('Витрати_Шаблон');

  if (tplRoute) {
    var newRoute = tplRoute.copyTo(ss);
    newRoute.setName('Маршрут_' + name);
    newRoute.showSheet();
  } else {
    return { ok: false, error: 'Шаблон "Маршрут_Шаблон" не знайдено' };
  }

  if (tplDispatch) {
    var newDisp = tplDispatch.copyTo(ss);
    newDisp.setName('Відправка_' + name);
    newDisp.showSheet();
  }

  if (tplExpenses) {
    var newExp = tplExpenses.copyTo(ss);
    newExp.setName('Витрати_' + name);
    newExp.showSheet();
  }

  return { ok: true, created: ['Маршрут_' + name, 'Відправка_' + name, 'Витрати_' + name] };
}

/**
 * Видалити маршрут (аркуш Маршрут_назва)
 * data: { name: 'Цюріх_20260310' }
 */
function apiDeleteRoute(params) {
  var name = params.name;
  if (!name) return { ok: false, error: 'Назва маршруту обов\'язкова' };

  var ss = SpreadsheetApp.openById(DB.MARHRUT);
  var sheet = ss.getSheetByName('Маршрут_' + name);
  if (!sheet) return { ok: false, error: 'Маршрут "' + name + '" не знайдено' };

  // Перевіряємо що це не останній аркуш
  if (ss.getSheets().length <= 1) {
    return { ok: false, error: 'Неможливо видалити останній аркуш' };
  }

  // Архівуємо всі записи маршруту перед видаленням
  archiveSheetToArchive(sheet, 'Маршрут_' + name, 'Видалено (маршрут)', params.archived_by || 'Менеджер');

  ss.deleteSheet(sheet);
  return { ok: true };
}

/**
 * Видалити пов'язані аркуші Відправка та Витрати
 * data: { name: 'Цюріх_20260310' }
 */
function apiDeleteLinkedSheets(params) {
  var name = params.name;
  if (!name) return { ok: false, error: 'Назва обов\'язкова' };

  var ss = SpreadsheetApp.openById(DB.MARHRUT);
  var deleted = [];

  // Пробуємо обидва формати: з _ та з пробілом
  var variants = ['Відправка_' + name, 'Відправка ' + name, 'Витрати_' + name, 'Витрати ' + name];
  for (var i = 0; i < variants.length; i++) {
    var s = ss.getSheetByName(variants[i]);
    if (s && ss.getSheets().length > 1) {
      // Архівуємо записи перед видаленням
      archiveSheetToArchive(s, variants[i], 'Видалено (маршрут)', params.archived_by || 'Менеджер');
      ss.deleteSheet(s);
      deleted.push(variants[i]);
    }
  }

  return { ok: true, deleted: deleted };
}

/**
 * Хелпер: копіює всі записи аркуша в Archive_crm → "Архів маршрутів"
 */
function archiveSheetToArchive(sheet, sheetName, reason, archivedBy) {
  var lastCol = sheet.getLastColumn();
  var lastRow = sheet.getLastRow();
  if (lastCol < 1 || lastRow < 2) return; // Порожній аркуш

  var headers = sheet.getRange(1, 1, 1, lastCol).getValues()[0];
  var data = sheet.getRange(2, 1, lastRow - 1, lastCol).getValues();

  var archSS = SpreadsheetApp.openById(DB.ARCHIVE);
  var archSheet = archSS.getSheetByName('Архів маршрутів');
  if (!archSheet) {
    archSheet = archSS.insertSheet('Архів маршрутів');
    var archHeaders = headers.concat(['SOURCE_SHEET', 'DATE_ARCHIVE', 'ARCHIVED_BY', 'ARCHIVE_REASON']);
    archSheet.getRange(1, 1, 1, archHeaders.length).setValues([archHeaders]);
  }
  var archHeaders = archSheet.getRange(1, 1, 1, archSheet.getLastColumn()).getValues()[0];
  var now = Utilities.formatDate(new Date(), 'Europe/Kiev', 'dd.MM.yyyy HH:mm');

  for (var i = 0; i < data.length; i++) {
    var obj = {};
    for (var j = 0; j < headers.length; j++) {
      obj[headers[j]] = data[i][j];
    }
    obj['SOURCE_SHEET'] = sheetName;
    obj['DATE_ARCHIVE'] = now;
    obj['ARCHIVED_BY'] = archivedBy;
    obj['ARCHIVE_REASON'] = reason;

    var row = archHeaders.map(function(h) { return obj[h] !== undefined ? obj[h] : ''; });
    archSheet.appendRow(row);
  }
}

// ══════════════════════════════════════════════════════════════
// 10. FINANCE — Платежі (Finance_crm_v2)
// ══════════════════════════════════════════════════════════════

var FINANCE_SHEET_NAME = 'Платежі';
var FINANCE_COLS = [
  'PAY_ID','Дата створення','Хто вніс','Роль',
  'CLI_ID','PAX_ID','PKG_ID','RTE_ID','CAL_ID',
  'Ід_смарт','Тип платежу','Сума','Валюта',
  'Форма оплати','Статус платежу','Борг сума','Борг валюта',
  'Дата погашення','Примітка','DATE_ARCHIVE','ARCHIVED_BY'
];

// addPayment — Додає запис платежу в Finance_crm_v2.Платежі
// Викликається автоматично з apiUpdateField при зміні Завдаток (S)
// delta — різниця (нова сума - стара). Додатнє = доплата, від'ємне = повернення
function addPayment(paxData, managerName, delta) {
  var finSS = SpreadsheetApp.openById(DB.FINANCE);
  var finSheet = finSS.getSheetByName(FINANCE_SHEET_NAME);
  if (!finSheet) {
    // Створюємо аркуш якщо його ще немає
    finSheet = finSS.insertSheet(FINANCE_SHEET_NAME);
    finSheet.getRange(1, 1, 1, FINANCE_COLS.length).setValues([FINANCE_COLS]);
  }

  var payId = genId('PAY');
  var absDelta = Math.abs(delta);
  var debt = calcDebt(paxData);

  var payObj = {};
  FINANCE_COLS.forEach(function(c) { payObj[c] = ''; });

  payObj['PAY_ID'] = payId;
  payObj['Дата створення'] = now();
  payObj['Хто вніс'] = managerName || '';
  payObj['Роль'] = 'Менеджер';
  payObj['CLI_ID'] = paxData['CLI_ID'] || '';
  payObj['PAX_ID'] = paxData['PAX_ID'] || '';
  payObj['PKG_ID'] = '';
  payObj['RTE_ID'] = paxData['RTE_ID'] || '';
  payObj['CAL_ID'] = paxData['CAL_ID'] || '';
  payObj['Ід_смарт'] = paxData['Ід_смарт'] || '';
  payObj['Тип платежу'] = delta > 0 ? 'Завдаток' : 'Повернення';
  payObj['Сума'] = absDelta;
  payObj['Валюта'] = paxData['Валюта завдатку'] || paxData['Валюта квитка'] || 'UAH';
  payObj['Форма оплати'] = '';
  payObj['Статус платежу'] = delta > 0 ? 'Отримано' : 'Повернено';
  payObj['Борг сума'] = debt;
  payObj['Борг валюта'] = paxData['Валюта квитка'] || 'UAH';
  payObj['Дата погашення'] = '';
  payObj['Примітка'] = '';
  payObj['DATE_ARCHIVE'] = '';
  payObj['ARCHIVED_BY'] = '';

  var finHeaders = finSheet.getRange(1, 1, 1, finSheet.getLastColumn()).getValues()[0];
  var row = finHeaders.map(function(h) { return payObj[h] !== undefined ? payObj[h] : ''; });
  finSheet.appendRow(row);

  return { ok: true, pay_id: payId };
}

// apiGetPayments — Отримати платежі по PAX_ID
function apiGetPayments(params) {
  var paxId = params.pax_id;
  if (!paxId) return { ok: false, error: 'pax_id не вказано' };

  var finSS = SpreadsheetApp.openById(DB.FINANCE);
  var finSheet = finSS.getSheetByName(FINANCE_SHEET_NAME);
  if (!finSheet) return { ok: true, data: [] };

  var lastRow = finSheet.getLastRow();
  if (lastRow < 2) return { ok: true, data: [] };

  var headers = finSheet.getRange(1, 1, 1, finSheet.getLastColumn()).getValues()[0];
  var data = finSheet.getRange(2, 1, lastRow - 1, headers.length).getValues();

  var paxIdx = headers.indexOf('PAX_ID');
  if (paxIdx === -1) return { ok: true, data: [] };

  var results = [];
  for (var i = 0; i < data.length; i++) {
    if (String(data[i][paxIdx]) === String(paxId)) {
      var obj = {};
      for (var j = 0; j < headers.length; j++) {
        obj[headers[j]] = data[i][j] !== undefined ? data[i][j] : '';
      }
      results.push(obj);
    }
  }

  // Сортування: новіші зверху (формат dd.MM.yyyy HH:mm)
  results.sort(function(a, b) {
    var da = String(a['Дата створення']);
    var db = String(b['Дата створення']);
    // Конвертуємо dd.MM.yyyy HH:mm → yyyyMMddHHmm для правильного порівняння
    function toSortable(s) {
      var m = s.match(/(\d{2})\.(\d{2})\.(\d{4})\s*(\d{2}):?(\d{2})?/);
      if (m) return m[3] + m[2] + m[1] + m[4] + (m[5] || '00');
      var m2 = s.match(/(\d{2})\.(\d{2})\.(\d{4})/);
      if (m2) return m2[3] + m2[2] + m2[1] + '0000';
      return s;
    }
    return toSortable(db).localeCompare(toSortable(da));
  });

  return { ok: true, data: results };
}


// ══════════════════════════════════════════════════════════════
// doGet / doPost — UNIVERSAL ROUTER
// ══════════════════════════════════════════════════════════════

function doGet(e) {
  var action = (e && e.parameter) ? e.parameter.action || '' : '';
  var result = { ok: false, error: 'Unknown action' };

  try {
    switch (action) {
      case 'ping':
        result = { ok: true, message: 'Borispol Vip Travel CRM v3 API', version: '3.0', timestamp: new Date().toISOString() };
        break;
      case 'getAll':
        result = apiGetAll({ sheet: e.parameter.sheet || 'all', filter: {} });
        break;
      case 'getTrips':
        result = apiGetTrips({ filter: {} });
        break;
      case 'getStats':
        result = apiGetStats({});
        break;
      case 'getAutopark':
        result = apiGetAutopark({});
        break;
      default:
        result = { ok: false, error: 'Unknown GET action: ' + action };
    }
  } catch (err) {
    result = { ok: false, error: err.message };
  }

  return ContentService.createTextOutput(JSON.stringify(result))
    .setMimeType(ContentService.MimeType.JSON);
}

function doPost(e) {
  var body = {};
  try {
    body = JSON.parse(e.postData.contents);
  } catch (err) {
    return ContentService.createTextOutput(JSON.stringify({ ok: false, error: 'Invalid JSON: ' + err.message }))
      .setMimeType(ContentService.MimeType.JSON);
  }

  var action = body.action || '';
  var manager = body.manager || '';
  var result = { ok: false, error: 'Unknown action: ' + action };

  // Дії що потребують логування (запис/зміна даних)
  // Логуємо тільки важливі дії (додавання, видалення, архівація, рейси)
  // НЕ логуємо: updateField, updatePassenger, bulkUpdateField, updateTrip, updateRouteField
  var LOGGED_ACTIONS = {
    'addPassenger': 'Додано пасажира',
    'clonePassenger': 'Клоновано пасажира',
    'assignTrip': 'Призначено рейс',
    'unassignTrip': 'Знято з рейсу',
    'reassignTrip': 'Пересадка на рейс',
    'deletePassenger': 'Видалено пасажира',
    'bulkDelete': 'Масове видалення',
    'archivePassenger': 'Архівовано пасажира',
    'restorePassenger': 'Відновлено з архіву',
    'moveDirection': 'Зміна напряму',
    'createTrip': 'Створено рейс',
    'archiveTrip': 'Архівовано рейс',
    'deleteTrip': 'Видалено рейс',
    'duplicateTrip': 'Дубльовано рейс',
    'createRoute': 'Створено маршрут',
    'deleteRoute': 'Видалено маршрут'
  };

  try {
    switch (action) {
      // ── PASSENGERS READ ──
      case 'getAll':             result = apiGetAll(body); break;
      case 'getOne':             result = apiGetOne(body); break;
      case 'getPassengersByTrip':result = apiGetPassengersByTrip(body); break;
      case 'getStats':           result = apiGetStats(body); break;
      case 'checkDuplicates':    result = apiCheckDuplicates(body); break;
      case 'suggestTrips':       result = apiSuggestTrips(body); break;

      // ── PASSENGERS CREATE ──
      case 'addPassenger':       result = apiAddPassenger(body); break;
      case 'clonePassenger':     result = apiClonePassenger(body); break;

      // ── PASSENGERS UPDATE ──
      case 'updateField':        result = apiUpdateField(body); break;
      case 'updatePassenger':    result = apiUpdatePassenger(body); break;
      case 'bulkUpdateField':    result = apiBulkUpdateField(body); break;

      // ── PASSENGERS TRIP ──
      case 'assignTrip':         result = apiAssignTrip(body); break;
      case 'unassignTrip':       result = apiUnassignTrip(body); break;
      case 'reassignTrip':       result = apiReassignTrip(body); break;

      // ── PASSENGERS DELETE/ARCHIVE ──
      case 'deletePassenger':    result = apiDeletePassenger(body); break;
      case 'deleteFromSheet':    result = apiDeleteFromSheet(body); break;
      case 'bulkDelete':         result = apiBulkDelete(body); break;
      case 'archivePassenger':   result = apiArchivePassenger(body); break;
      case 'restorePassenger':   result = apiRestorePassenger(body); break;
      case 'getArchive':         result = apiGetArchive(body); break;
      case 'deleteFromArchive':  result = apiDeleteFromArchive(body); break;
      case 'moveDirection':      result = apiMoveDirection(body); break;

      // ── TRIPS ──
      case 'getTrips':           result = apiGetTrips(body); break;
      case 'getTrip':            result = apiGetTrip(body); break;
      case 'createTrip':         result = apiCreateTrip(body); break;
      case 'updateTrip':         result = apiUpdateTrip(body); break;
      case 'archiveTrip':        result = apiArchiveTrip(body); break;
      case 'deleteTrip':         result = apiDeleteTrip(body); break;
      case 'duplicateTrip':      result = apiDuplicateTrip(body); break;

      // ── ROUTES (Marhrut_crm_v6) ──
      case 'getRoutesList':      result = apiGetRoutesList(body); break;
      case 'getRouteSheet':      result = apiGetRouteSheet(body); break;
      case 'getRoutes':          result = apiGetRoutes(body); break;
      case 'addToRoute':         result = apiAddToRoute(body); break;
      case 'createRoute':        result = apiCreateRoute(body); break;
      case 'deleteRoute':        result = apiDeleteRoute(body); break;
      case 'deleteLinkedSheets': result = apiDeleteLinkedSheets(body); break;
      case 'updateRouteField':   result = apiUpdateRouteField(body); break;

      // ── AUTOPARK ──
      case 'getAutopark':        result = apiGetAutopark(body); break;
      case 'getAutoSeats':       result = apiGetAutoSeats(body); break;

      // ── SEATING ──
      case 'getSeating':         result = apiGetSeating(body); break;
      case 'assignSeat':         result = apiAssignSeat(body); break;
      case 'freeSeat':           result = apiFreeSeat(body); break;

      // ── FINANCE ──
      case 'getPayments':        result = apiGetPayments(body); break;

      // ── PRESENCE (онлайн менеджери) ──
      case 'heartbeat':          result = apiHeartbeat(body); break;
      case 'getOnlineManagers':  result = apiGetOnlineManagers(body); break;

      // ── ONBOARDING (навчання) ──
      case 'logOnboarding':
        var obStatus = body.completed ? 'Завершено' : ('Пропущено ' + body.stepsViewed + '/' + body.totalSteps);
        var obDetails = (body.categoryName || body.category || '') + ' — ' + obStatus;
        writeLog(manager, 'Навчання', obDetails);
        result = { ok: true };
        break;

      default:
        result = { ok: false, error: 'Unknown action: ' + action + '. Available: getAll, getOne, getPassengersByTrip, getStats, checkDuplicates, suggestTrips, addPassenger, clonePassenger, updateField, updatePassenger, bulkUpdateField, assignTrip, unassignTrip, reassignTrip, deletePassenger, bulkDelete, archivePassenger, restorePassenger, getArchive, deleteFromArchive, moveDirection, getTrips, getTrip, createTrip, updateTrip, archiveTrip, deleteTrip, duplicateTrip, getRoutesList, getRouteSheet, getRoutes, addToRoute, createRoute, deleteRoute, deleteLinkedSheets, updateRouteField, getAutopark, getAutoSeats, getSeating, assignSeat, freeSeat, getPayments, heartbeat, getOnlineManagers' };
    }

    // Логуємо успішні операції запису
    if (result.ok && LOGGED_ACTIONS[action]) {
      var logDetails = '';
      if (body.col) logDetails = body.col + ' = ' + body.value;
      else if (body.name) logDetails = body.name;
      else if (body.pax_ids) logDetails = body.pax_ids.length + ' записів';
      else if (body.reason) logDetails = body.reason;
      else if (result.pax_id) logDetails = result.pax_id;
      else if (result.cal_ids) logDetails = result.cal_ids.join(', ');

      writeLog(manager, LOGGED_ACTIONS[action], logDetails, {
        pax_id: body.pax_id || (body.pax_ids ? body.pax_ids[0] : '') || result.pax_id || '',
        cal_id: body.cal_id || body.new_cal_id || '',
        rte_id: body.rte_id || ''
      });
    }

  } catch (err) {
    result = { ok: false, error: err.message };
  }

  return ContentService.createTextOutput(JSON.stringify(result))
    .setMimeType(ContentService.MimeType.JSON);
}

// ================================================================
// PRESENCE — Онлайн-статус менеджерів (через CacheService)
// ================================================================

function apiHeartbeat(body) {
  var name = (body.manager || '').trim();
  if (!name) return { ok: false, error: 'manager is required' };

  var device = body.device || '';
  var os = body.os || '';
  var browser = body.browser || '';
  var pwa = body.pwa ? 'PWA' : 'Browser';
  var deviceLabel = [device, os, browser, pwa].filter(Boolean).join(' / ');

  var cache = CacheService.getScriptCache();
  var key = 'presence_' + name;
  var data = JSON.stringify({ name: name, ts: new Date().toISOString(), device: deviceLabel });
  cache.put(key, data, 90);

  var knownRaw = cache.get('presence_known_managers') || '[]';
  var known = JSON.parse(knownRaw);
  if (known.indexOf(name) === -1) {
    known.push(name);
    cache.put('presence_known_managers', JSON.stringify(known), 21600);
  }

  // Логуємо вхід з нового пристрою (раз на 10 хвилин на менеджера)
  var loginKey = 'device_log_' + name;
  var lastLog = cache.get(loginKey);
  if (!lastLog || lastLog !== deviceLabel) {
    cache.put(loginKey, deviceLabel, 600); // 10 хв
    writeLog(name, 'Вхід', deviceLabel);
  }

  return { ok: true };
}

function apiGetOnlineManagers(body) {
  var cache = CacheService.getScriptCache();
  var knownRaw = cache.get('presence_known_managers') || '[]';
  var known = JSON.parse(knownRaw);
  var online = [];
  if (known.length > 0) {
    var keys = known.map(function(n) { return 'presence_' + n; });
    var cached = cache.getAll(keys);
    for (var i = 0; i < known.length; i++) {
      var val = cached['presence_' + known[i]];
      if (val) {
        try {
          var parsed = JSON.parse(val);
          online.push({ name: parsed.name, ts: parsed.ts, device: parsed.device || '' });
        } catch (e) {}
      }
    }
  }
  return { ok: true, managers: online };
}
