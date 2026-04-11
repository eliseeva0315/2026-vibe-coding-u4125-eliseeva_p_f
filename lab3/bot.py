# -*- coding: utf-8 -*-
"""
Telegram-бот ателье мужских костюмов: сотрудники, важные даты, wishlist.
Используется библиотека python-telegram-bot (v21+, async API).
"""

from __future__ import annotations

import json
import csv
import logging
import os
import re
import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

# ---------------------------------------------------------------------------
# Конфигурация и пути
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent
DATA_FILE = BASE_DIR / "data.json"
CSV_FILE = BASE_DIR / "employees.csv"
ACTIVITY_DB = BASE_DIR / "activity.db"

# Состояния диалогов
(
    WISH_AUTHOR,
    WISH_PICK_EMPLOYEE,
    WISH_ITEM,
    NEW_EMP_NAME,
    NEW_EMP_DEPARTMENT,
    NEW_EMP_SPECIALIZATION,
    NEW_EMP_EXPERIENCE,
    NEW_EMP_PHONE,
    NEW_EMP_EMAIL,
    NEW_EMP_BIRTHDAY,
) = range(10)

# Состояние поиска сотрудника (после запроса текста поиска)
SEARCH_EMPLOYEE = 10

# Ограничения ввода (защита от некорректных/слишком длинных данных)
MAX_WISH_AUTHOR_LEN = 80
MAX_WISH_ITEM_LEN = 500

# Клавиатура главного меню (дублирует команды для удобства)
MAIN_MENU_KEYBOARD = ReplyKeyboardMarkup(
    [
        ["👥 Сотрудники", "📅 Важные даты"],
        ["🎁 Wishlist", "❓ Помощь"],
    ],
    resize_keyboard=True,
)

BTN_EMP_ALL = "Все сотрудники"

load_dotenv()
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Работа с JSON (сотрудники, даты, wishlist)
# ---------------------------------------------------------------------------


def load_json_safe() -> dict[str, Any]:
    """Загружает data.json; при ошибке возвращает пустую структуру."""
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        logger.error("Файл %s не найден", DATA_FILE)
        return {"employees": [], "important_dates": [], "wishlist": []}
    except json.JSONDecodeError as e:
        logger.exception("Ошибка разбора JSON: %s", e)
        return {"employees": [], "important_dates": [], "wishlist": []}

    data.setdefault("employees", [])
    data.setdefault("important_dates", [])
    data.setdefault("wishlist", [])
    return data


def save_wishlist(wishlist: list[dict[str, Any]]) -> bool:
    """Сохраняет только блок wishlist в data.json (атомарная перезапись)."""
    try:
        data = load_json_safe()
        data["wishlist"] = wishlist
        tmp = DATA_FILE.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        tmp.replace(DATA_FILE)
        return True
    except OSError as e:
        logger.exception("Не удалось сохранить wishlist: %s", e)
        return False


def save_data(data: dict[str, Any]) -> bool:
    """Сохраняет весь data.json (атомарная перезапись)."""
    try:
        tmp = DATA_FILE.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        tmp.replace(DATA_FILE)
        return True
    except OSError as e:
        logger.exception("Не удалось сохранить data.json: %s", e)
        return False


def load_employees_from_csv() -> list[dict[str, Any]]:
    """
    Загружает сотрудников из employees.csv.
    Ожидаемые колонки: name, department, role, email, phone, birthday.
    Если файла нет/ошибка/пустой файл — возвращает [].
    """
    if not CSV_FILE.exists():
        return []
    employees: list[dict[str, Any]] = []
    try:
        with open(CSV_FILE, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            required = {"name", "department", "role", "email", "phone", "birthday"}
            if not reader.fieldnames or not required.issubset(set(reader.fieldnames)):
                logger.warning("employees.csv: некорректные заголовки, ожидаются %s", sorted(required))
                return []
            for row in reader:
                name = str(row.get("name", "")).strip()
                if not name:
                    continue
                dep = normalize_department(str(row.get("department", "")).strip())
                role = str(row.get("role", "")).strip()
                email = str(row.get("email", "")).strip()
                phone = str(row.get("phone", "")).strip()
                birthday = str(row.get("birthday", "")).strip()
                employees.append(
                    {
                        "name": name,
                        "department": dep,
                        "role": role,
                        "specialization": role,
                        "experience_years": None,
                        "contact": {"email": email, "phone": phone},
                        "birthday": birthday,
                    }
                )
    except OSError as e:
        logger.warning("Не удалось прочитать employees.csv: %s", e)
        return []
    return employees


def get_employees_source() -> list[dict[str, Any]]:
    """Основной источник сотрудников — employees.csv."""
    return load_employees_from_csv()


def get_csv_error_message() -> str | None:
    """Человеко-понятная ошибка CSV для ответа пользователю."""
    if not CSV_FILE.exists():
        return "Файл employees\\.csv не найден\\. Проверьте, что он лежит рядом с bot\\.py\\."
    employees = load_employees_from_csv()
    if not employees:
        return "Файл employees\\.csv пустой или имеет неверный формат\\."
    return None


def append_employee_to_csv(employee: dict[str, Any]) -> bool:
    """Добавляет нового сотрудника в employees.csv."""
    required_header = ["name", "department", "role", "email", "phone", "birthday"]
    try:
        file_exists = CSV_FILE.exists()
        with open(CSV_FILE, "a", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=required_header)
            if not file_exists:
                writer.writeheader()
            contact = employee.get("contact") or {}
            writer.writerow(
                {
                    "name": str(employee.get("name", "")).strip(),
                    "department": normalize_department(str(employee.get("department", "")).strip()),
                    "role": str(employee.get("role", "")).strip(),
                    "email": str(contact.get("email", "")).strip(),
                    "phone": str(contact.get("phone", "")).strip(),
                    "birthday": str(employee.get("birthday", "")).strip(),
                }
            )
        return True
    except OSError as e:
        logger.warning("Не удалось записать в employees.csv: %s", e)
        return False


# ---------------------------------------------------------------------------
# SQLite: история активности (опционально)
# ---------------------------------------------------------------------------


def init_activity_db() -> None:
    """Создаёт таблицу логов, если её ещё нет."""
    try:
        conn = sqlite3.connect(ACTIVITY_DB)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS activity (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                username TEXT,
                action TEXT NOT NULL,
                detail TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
            """
        )
        conn.commit()
        conn.close()
    except sqlite3.Error as e:
        logger.warning("SQLite недоступен, логирование отключено: %s", e)


def log_activity(user_id: int, username: str | None, action: str, detail: str | None = None) -> None:
    """Пишет строку в журнал активности (ошибки БД не роняют бота)."""
    try:
        conn = sqlite3.connect(ACTIVITY_DB)
        conn.execute(
            "INSERT INTO activity (user_id, username, action, detail) VALUES (?, ?, ?, ?)",
            (user_id, username, action, detail),
        )
        conn.commit()
        conn.close()
    except sqlite3.Error as e:
        logger.debug("log_activity: %s", e)


# ---------------------------------------------------------------------------
# Логика дат: ближайшие дни рождения и события
# ---------------------------------------------------------------------------


def _parse_iso_date(s: str) -> date | None:
    try:
        return datetime.strptime(s.strip(), "%Y-%m-%d").date()
    except (ValueError, AttributeError):
        return None


def days_until_next_birthday(birthday_str: str, today: date) -> int | None:
    """
    Возвращает число дней до ближайшего дня рождения (по календарю MM-DD).
    Если строка некорректна — None.
    """
    if not birthday_str:
        return None
    parts = birthday_str.strip().split("-")
    if len(parts) != 3:
        return None
    try:
        month, day = int(parts[1]), int(parts[2])
        this_year = date(today.year, month, day)
    except ValueError:
        return None

    if this_year < today:
        try:
            next_b = date(today.year + 1, month, day)
        except ValueError:
            return None
    else:
        next_b = this_year

    delta = (next_b - today).days
    return delta


def filter_birthdays_within(employees: list[dict[str, Any]], days: int, today: date) -> list[tuple[dict[str, Any], int]]:
    """Сотрудники, у кого ближайший ДР в пределах `days` дней от сегодня."""
    out: list[tuple[dict[str, Any], int]] = []
    for emp in employees:
        b = emp.get("birthday")
        if not b:
            continue
        d = days_until_next_birthday(str(b), today)
        if d is not None and 0 <= d <= days:
            out.append((emp, d))
    out.sort(key=lambda x: x[1])
    return out


def filter_events_within(
    important_dates: list[dict[str, Any]], days: int, today: date
) -> list[tuple[str, date, int]]:
    """События с датой в окне [today, today+days] (по календарным датам)."""
    out: list[tuple[str, date, int]] = []
    end = today + timedelta(days=days)
    for row in important_dates:
        event_name = str(row.get("event", "")).strip()
        ds = row.get("date")
        if not ds:
            continue
        d = _parse_iso_date(str(ds))
        if d is None:
            continue
        if today <= d <= end:
            out.append((event_name, d, (d - today).days))
    out.sort(key=lambda x: x[2])
    return out


def format_birthday_ddmmyyyy(birthday_iso: str) -> str | None:
    """Дата рождения из ISO (ГГГГ-ММ-ДД) в вид ДД.ММ.ГГГГ."""
    d = _parse_iso_date(str(birthday_iso).strip())
    if d is None:
        return None
    return d.strftime("%d.%m.%Y")


def parse_birthday_ddmmyyyy_to_iso(s: str) -> str | None:
    """Парсит ДД.ММ.ГГГГ и возвращает ISO-дату ГГГГ-ММ-ДД."""
    try:
        d = datetime.strptime(s.strip(), "%d.%m.%Y").date()
        return d.isoformat()
    except (ValueError, AttributeError):
        return None


def normalize_department(dep: str) -> str:
    """Нормализует название отдела к каноничному значению."""
    value = str(dep or "").strip().lower()
    if value in {"консультант", "консультанты"}:
        return "консультанты"
    return str(dep or "").strip()


def extract_departments(employees: list[dict[str, Any]]) -> list[str]:
    """Уникальные отделы из сотрудников в порядке появления."""
    out: list[str] = []
    seen: set[str] = set()
    for emp in employees:
        dep = normalize_department(str(emp.get("department", "")).strip())
        if not dep:
            continue
        dep_lc = dep.lower()
        if dep_lc in seen:
            continue
        seen.add(dep_lc)
        out.append(dep)
    return out


def make_keyboard_by_items(items: list[str], row_size: int = 2) -> ReplyKeyboardMarkup:
    rows: list[list[str]] = []
    for i in range(0, len(items), row_size):
        rows.append(items[i : i + row_size])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


def find_employees_by_name_query(employees: list[dict[str, Any]], query: str) -> list[dict[str, Any]]:
    """Ищет сотрудников по name: полное совпадение имени или первого слова ФИО."""
    q = query.strip().lower()
    if not q:
        return []
    result: list[dict[str, Any]] = []
    for emp in employees:
        full_name = str(emp.get("name", "")).strip()
        if not full_name:
            continue
        full_lc = full_name.lower()
        first_lc = full_name.split()[0].lower()
        if q == full_lc or q == first_lc:
            result.append(emp)
    return result


def find_employee_by_author_string(
    author: str, employees: list[dict[str, Any]]
) -> dict[str, Any] | None:
    """
    Находит сотрудника по строке автора из wishlist (полное имя, имя или начало ФИО).
    Логика согласована с wish_item_for_employee.
    """
    if not author or not employees:
        return None
    a = author.strip()
    if not a:
        return None
    a_lower = a.lower()
    for emp in employees:
        name = str(emp.get("name", "")).strip()
        if not name:
            continue
        en_lower = name.lower()
        first_word = name.split()[0].lower()
        if a_lower == en_lower:
            return emp
        if a_lower == first_word:
            return emp
        if en_lower.startswith(a_lower + " "):
            return emp
    return None


def wish_item_for_employee(emp_name: str, wishlist: list[dict[str, Any]]) -> str | None:
    """
    Текст пожелания для сотрудника: совпадение поля wishlist.author с именем
    (полное имя, имя целиком или имя как первая часть ФИО).
    Берётся первое подходящее пожелание по порядку в списке.
    """
    if not emp_name or not wishlist:
        return None
    en = emp_name.strip()
    if not en:
        return None
    en_lower = en.lower()
    first_word = en.split()[0].lower()
    for w in wishlist:
        author = str(w.get("author", "")).strip()
        if not author:
            continue
        a_lower = author.lower()
        item = w.get("item")
        if item is None or not str(item).strip():
            continue
        text = str(item).strip()
        if a_lower == en_lower:
            return text
        if a_lower == first_word:
            return text
        if en_lower.startswith(a_lower + " "):
            return text
    return None


# ---------------------------------------------------------------------------
# Форматирование сообщений
# ---------------------------------------------------------------------------


def escape_md(text: str) -> str:
    """Экранирование для Telegram MarkdownV2 (минимальный набор)."""
    if not text:
        return ""
    special = r"_*[]()~`>#+-=|{}.!"
    return re.sub(f"([{re.escape(special)}])", r"\\\1", str(text))


def format_employee_card(emp: dict[str, Any]) -> str:
    """Текст карточки сотрудника (MarkdownV2)."""
    name = escape_md(emp.get("name", "—"))
    role = escape_md(emp.get("role", "—"))
    spec = emp.get("specialization")
    spec_line = f"*Специализация:* {escape_md(spec)}\n" if spec else ""
    exp = emp.get("experience_years")
    exp_line = f"*Опыт:* {escape_md(str(exp))} лет\n" if exp is not None else ""
    contact = emp.get("contact") or {}
    phone = contact.get("phone", "—")
    email = contact.get("email", "—")
    bday = escape_md(emp.get("birthday", "—"))

    return (
        f"*Имя:* {name}\n"
        f"*Роль:* {role}\n"
        f"{spec_line}"
        f"{exp_line}"
        f"*Телефон:* {escape_md(phone)}\n"
        f"*Email:* {escape_md(email)}\n"
        f"*День рождения:* {bday}"
    )


def format_important_dates_message(data: dict[str, Any]) -> str:
    """Текст: ДР за 30 дней + корпоративные/важные даты за 30 дней."""
    today = date.today()
    window = 30
    wishlist = data.get("wishlist") or []

    lines: list[str] = []
    lines.append("*Ближайшие 30 дней*\n")

    bd = filter_birthdays_within(data.get("employees", []), window, today)
    if not bd:
        lines.append("_Дни рождения сотрудников в этом окне не найдены\\._\n")
    else:
        lines.append("*Дни рождения сотрудников:*")
        for emp, _dleft in bd:
            name = str(emp.get("name", "")).strip() or "—"
            role = str(emp.get("role", "")).strip() or "—"
            department = normalize_department(str(emp.get("department", "")).strip()) or "—"
            bday_raw = str(emp.get("birthday", "")).strip()
            bday_ddmmyyyy = format_birthday_ddmmyyyy(bday_raw)
            bday_e = escape_md(bday_ddmmyyyy) if bday_ddmmyyyy else escape_md(bday_raw)

            wish_text = wish_item_for_employee(name, wishlist)
            name_e = escape_md(name)
            role_e = escape_md(role)
            dep_e = escape_md(department)
            if wish_text:
                wish_e = escape_md(wish_text)
                lines.append(f"• {name_e}, {role_e}, {dep_e} — {bday_e} — {wish_e}")
            else:
                lines.append(f"• {name_e}, {role_e}, {dep_e} — {bday_e}")
        lines.append("")

    ev = filter_events_within(data.get("important_dates", []), window, today)
    if not ev:
        lines.append("_Корпоративные события в этом окне не запланированы\\._")
    else:
        lines.append("*Важные даты и события:*")
        for title, d, dleft in ev:
            lines.append(f"• {escape_md(title)} — {d.isoformat()} \\(через {dleft} дн\\.\\)")

    return "\n".join(lines)


def format_wishlist(data: dict[str, Any]) -> str:
    wishes = data.get("wishlist") or []
    employees = data.get("employees") or []
    if not wishes:
        return "Пожеланий пока нет\\. Добавьте первое через меню или команду /wishlist\\."
    lines = ["*Список пожеланий:*"]
    for i, w in enumerate(wishes, start=1):
        author_raw = str(w.get("author", "")).strip() or "—"
        item_e = escape_md(str(w.get("item", "—")).strip())
        emp = find_employee_by_author_string(author_raw, employees)
        role = str(emp.get("role", "")).strip() if emp else ""
        department = normalize_department(str(emp.get("department", "")).strip()) if emp else ""
        author_e = escape_md(author_raw)
        if role:
            role_e = escape_md(role)
            dep_e = escape_md(department or "—")
            lines.append(f"{i}\\. {author_e}, {role_e}, {dep_e}: {item_e}")
        else:
            lines.append(f"{i}\\. {author_e}: {item_e}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Фильтрация сотрудников по отделам
# ---------------------------------------------------------------------------


def filter_employees_by_department(
    employees: list[dict[str, Any]], department_label: str
) -> list[dict[str, Any]]:
    """Отбор сотрудников по выбранному отделу."""
    if department_label == BTN_EMP_ALL:
        return list(employees)
    dep_norm = normalize_department(department_label).lower()
    return [
        e
        for e in employees
        if normalize_department(str(e.get("department", "")).strip()).lower() == dep_norm
    ]


# ---------------------------------------------------------------------------
# Обработчики команд
# ---------------------------------------------------------------------------


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Приветствие и главное меню."""
    user = update.effective_user
    if user:
        log_activity(user.id, user.username, "start", None)

    text = (
        "Здравствуйте\\! Я бот ателье мужских костюмов\\.\n\n"
        "Помогу найти контакты коллег, напомню о днях рождения и событиях, "
        "а также покажу список пожеланий \\(wishlist\\)\\.\n\n"
        "Выберите раздел в меню ниже или используйте команды\\."
    )
    await update.message.reply_text(
        text,
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=MAIN_MENU_KEYBOARD,
    )


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Справка по командам."""
    text = (
        "*Команды бота*\n\n"
        "/start — приветствие и главное меню\n"
        "/employees — список сотрудников по отделам \\(кнопки\\)\n"
        "/find [имя] — поиск сотрудника по имени\n"
        "/department [отдел] — сотрудники отдела\n"
        "/email [имя] — email сотрудника\n"
        "/important\\_dates — дни рождения и события в ближайшие 30 дней\n"
        "/wishlist — просмотр и добавление пожеланий\n"
        "/cancel — отменить ввод \\(в диалоге wishlist или поиске\\)\n\n"
        "Также можно пользоваться кнопками внизу экрана\\."
    )
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN_V2)


def _search_by_name(employees: list[dict[str, Any]], query: str) -> list[dict[str, Any]]:
    q = query.strip().lower()
    if not q:
        return []
    return [e for e in employees if q in str(e.get("name", "")).strip().lower()]


async def cmd_find(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Поиск сотрудника по имени: /find [имя]."""
    query = " ".join(context.args).strip()
    if not query:
        await update.message.reply_text(
            "Укажите имя после команды\\. Пример: /find Павел",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return
    csv_error = get_csv_error_message()
    if csv_error:
        await update.message.reply_text(csv_error, parse_mode=ParseMode.MARKDOWN_V2)
        return
    employees = get_employees_source()
    matches = _search_by_name(employees, query)
    if not matches:
        await update.message.reply_text(
            "Сотрудники не найдены\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return
    lines = [f"*Найдено: {len(matches)}*"]
    for emp in matches:
        lines.append(format_employee_card(emp))
        lines.append("")
    await update.message.reply_text("\n".join(lines).strip(), parse_mode=ParseMode.MARKDOWN_V2)


async def cmd_department(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Сотрудники отдела: /department [отдел]."""
    dep_raw = " ".join(context.args).strip()
    if not dep_raw:
        await update.message.reply_text(
            "Укажите отдел после команды\\. Пример: /department консультанты",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return
    csv_error = get_csv_error_message()
    if csv_error:
        await update.message.reply_text(csv_error, parse_mode=ParseMode.MARKDOWN_V2)
        return
    dep = normalize_department(dep_raw)
    employees = get_employees_source()
    matches = filter_employees_by_department(employees, dep)
    if not matches:
        await update.message.reply_text(
            "В этом отделе сотрудники не найдены\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return
    dep_e = escape_md(dep)
    lines = [f"*Отдел:* {dep_e}", ""]
    for emp in matches:
        name = escape_md(str(emp.get("name", "—")))
        role = escape_md(str(emp.get("role", "—")))
        lines.append(f"• {name} — {role}")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN_V2)


async def cmd_email(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Email сотрудника: /email [имя]."""
    query = " ".join(context.args).strip()
    if not query:
        await update.message.reply_text(
            "Укажите имя после команды\\. Пример: /email Павел",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return
    csv_error = get_csv_error_message()
    if csv_error:
        await update.message.reply_text(csv_error, parse_mode=ParseMode.MARKDOWN_V2)
        return
    employees = get_employees_source()
    matches = _search_by_name(employees, query)
    if not matches:
        await update.message.reply_text("Сотрудник не найден\\.", parse_mode=ParseMode.MARKDOWN_V2)
        return
    lines = [f"*Найдено: {len(matches)}*"]
    for emp in matches:
        name = escape_md(str(emp.get("name", "—")))
        email = escape_md(str((emp.get("contact") or {}).get("email", "—")))
        lines.append(f"• {name}: {email}")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN_V2)


async def cmd_employees(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Сценарий выбора сотрудников: только кнопки отделов."""
    csv_error = get_csv_error_message()
    if csv_error:
        await update.message.reply_text(csv_error, parse_mode=ParseMode.MARKDOWN_V2)
        return ConversationHandler.END
    employees = get_employees_source()
    departments = extract_departments(employees)
    button_labels = [BTN_EMP_ALL] + departments
    context.user_data["employee_department_labels"] = set(button_labels)
    keyboard = make_keyboard_by_items(button_labels, row_size=2)

    await update.message.reply_text(
        "Выберите отдел сотрудников кнопками ниже\\.\n\n"
        "Отмена: /cancel",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=keyboard,
    )
    return SEARCH_EMPLOYEE


async def employee_category_chosen(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обрабатывает нажатие кнопки отдела и показывает inline-список сотрудников."""
    raw = (update.message.text or "").strip()
    labels = context.user_data.get("employee_department_labels") or set()
    if raw not in labels:
        csv_error = get_csv_error_message()
        if csv_error:
            await update.message.reply_text(csv_error, parse_mode=ParseMode.MARKDOWN_V2)
            return ConversationHandler.END
        departments = extract_departments(get_employees_source())
        keyboard = make_keyboard_by_items([BTN_EMP_ALL] + departments, row_size=2)
        await update.message.reply_text(
            "Выберите отдел одной из кнопок ниже или нажмите /cancel\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=keyboard,
        )
        return SEARCH_EMPLOYEE

    employees = get_employees_source()
    matches = filter_employees_by_department(employees, raw)

    user = update.effective_user
    if user:
        log_activity(user.id, user.username, "employees_department", raw[:80])

    if not matches:
        departments = extract_departments(employees)
        await update.message.reply_text(
            "В этом отделе сотрудников нет\\. Выберите другую кнопку\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=make_keyboard_by_items([BTN_EMP_ALL] + departments, row_size=2),
        )
        return SEARCH_EMPLOYEE

    context.user_data["emp_pick"] = [employees.index(m) for m in matches if m in employees]

    rows: list[list[InlineKeyboardButton]] = []
    for i, emp in enumerate(matches):
        label = str(emp.get("name", "?"))[:32]
        rows.append([InlineKeyboardButton(label, callback_data=f"emp:{i}")])

    await update.message.reply_text(
        f"Найдено: {len(matches)}\\. Нажмите на сотрудника для подробностей\\.",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=InlineKeyboardMarkup(rows),
    )
    await update.message.reply_text(
        "Главное меню:",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=MAIN_MENU_KEYBOARD,
    )
    return ConversationHandler.END


async def cb_employee_card(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Показ карточки по нажатию inline-кнопки."""
    query = update.callback_query
    await query.answer()
    employees = get_employees_source()
    m = re.match(r"^emp:(\d+)$", query.data or "")
    if not m:
        await query.edit_message_text(
            "Некорректные данные кнопки\\.", parse_mode=ParseMode.MARKDOWN_V2
        )
        return
    idx_local = int(m.group(1))
    indices = context.user_data.get("emp_pick") or []
    if idx_local >= len(indices):
        await query.edit_message_text(
            "Данные устарели\\. Запустите /employees снова\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return
    real_idx = indices[idx_local]
    if real_idx < 0 or real_idx >= len(employees):
        await query.edit_message_text("Сотрудник не найден\\.", parse_mode=ParseMode.MARKDOWN_V2)
        return

    emp = employees[real_idx]
    user = update.effective_user
    if user:
        log_activity(user.id, user.username, "employee_view", emp.get("name"))

    await query.edit_message_text(
        format_employee_card(emp),
        parse_mode=ParseMode.MARKDOWN_V2,
    )


async def cmd_important_dates(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Важные даты за 30 дней."""
    user = update.effective_user
    if user:
        log_activity(user.id, user.username, "important_dates", None)

    csv_error = get_csv_error_message()
    if csv_error:
        await update.message.reply_text(csv_error, parse_mode=ParseMode.MARKDOWN_V2)
        return
    data = load_json_safe()
    data["employees"] = get_employees_source()
    text = format_important_dates_message(data)
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN_V2)


# --- Wishlist: просмотр и добавление ---------------------------------------


async def cmd_wishlist(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Показ wishlist и кнопка «Добавить»."""
    user = update.effective_user
    if user:
        log_activity(user.id, user.username, "wishlist_view", None)

    csv_error = get_csv_error_message()
    if csv_error:
        await update.message.reply_text(csv_error, parse_mode=ParseMode.MARKDOWN_V2)
        return
    data = load_json_safe()
    data["employees"] = get_employees_source()
    text = format_wishlist(data)
    keyboard = InlineKeyboardMarkup(
        [[InlineKeyboardButton("➕ Добавить пожелание", callback_data="wish_add")]]
    )
    await update.message.reply_text(
        text,
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=keyboard,
    )


async def cb_wish_add(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Старт диалога wishlist: запрос имени сотрудника."""
    query = update.callback_query
    await query.answer()
    context.user_data.pop("wish_candidate_map", None)
    context.user_data.pop("wish_author", None)
    await query.message.reply_text(
        "Введите имя сотрудника\\.\n\n/cancel — отмена",
        parse_mode=ParseMode.MARKDOWN_V2,
    )
    return WISH_AUTHOR


async def wish_author(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Поиск сотрудника по имени или запуск добавления нового сотрудника."""
    name_query = (update.message.text or "").strip()
    if not name_query:
        await update.message.reply_text("Пустой ввод\\. Введите имя или нажмите /cancel\\.")
        return WISH_AUTHOR
    if len(name_query) > MAX_WISH_AUTHOR_LEN:
        await update.message.reply_text(f"Слишком длинно\\. До {MAX_WISH_AUTHOR_LEN} символов\\.")
        return WISH_AUTHOR

    csv_error = get_csv_error_message()
    if csv_error:
        await update.message.reply_text(csv_error, parse_mode=ParseMode.MARKDOWN_V2)
        return ConversationHandler.END
    employees = get_employees_source()
    matches = find_employees_by_name_query(employees, name_query)

    if len(matches) == 1:
        context.user_data["wish_author"] = str(matches[0].get("name", "")).strip()
        await update.message.reply_text(
            "Опишите пожелание одним сообщением\\.\n\n/cancel — отмена",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=MAIN_MENU_KEYBOARD,
        )
        return WISH_ITEM

    if len(matches) > 1:
        label_to_name: dict[str, str] = {}
        labels: list[str] = []
        for emp in matches:
            emp_name = str(emp.get("name", "")).strip() or "—"
            emp_role = str(emp.get("role", "")).strip() or "—"
            emp_dep = normalize_department(str(emp.get("department", "")).strip()) or "—"
            label = f"{emp_name}, {emp_role}, {emp_dep}"
            label_to_name[label] = emp_name
            labels.append(label)
        context.user_data["wish_candidate_map"] = label_to_name
        await update.message.reply_text(
            "Найдено несколько сотрудников\\. Выберите нужного кнопкой\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=make_keyboard_by_items(labels, row_size=1),
        )
        return WISH_PICK_EMPLOYEE

    await update.message.reply_text(
        "Сотрудник не найден\\. Давайте добавим нового",
        parse_mode=ParseMode.MARKDOWN_V2,
    )
    await update.message.reply_text(
        "Введите имя и фамилию нового сотрудника\\.",
        parse_mode=ParseMode.MARKDOWN_V2,
    )
    return NEW_EMP_NAME


async def wish_pick_employee(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Выбор сотрудника из списка при совпадении имён."""
    label = (update.message.text or "").strip()
    candidate_map = context.user_data.get("wish_candidate_map") or {}
    if label not in candidate_map:
        await update.message.reply_text(
            "Выберите сотрудника кнопкой из списка или /cancel\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return WISH_PICK_EMPLOYEE

    context.user_data["wish_author"] = candidate_map[label]
    context.user_data.pop("wish_candidate_map", None)
    await update.message.reply_text(
        "Опишите пожелание одним сообщением\\.\n\n/cancel — отмена",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=MAIN_MENU_KEYBOARD,
    )
    return WISH_ITEM


async def new_emp_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Шаг 1: имя нового сотрудника."""
    full_name = (update.message.text or "").strip()
    if len(full_name) < 3:
        await update.message.reply_text("Введите корректные имя и фамилию\\.")
        return NEW_EMP_NAME
    context.user_data["new_emp_name"] = full_name

    csv_error = get_csv_error_message()
    if csv_error:
        await update.message.reply_text(csv_error, parse_mode=ParseMode.MARKDOWN_V2)
        return ConversationHandler.END
    departments = extract_departments(get_employees_source())
    if not departments:
        await update.message.reply_text(
            "Нет доступных отделов в data\\.json\\. Добавьте department у сотрудников\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=MAIN_MENU_KEYBOARD,
        )
        return ConversationHandler.END

    context.user_data["new_emp_departments"] = set(departments)
    await update.message.reply_text(
        "Выберите отдел кнопкой\\.",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=make_keyboard_by_items(departments, row_size=2),
    )
    return NEW_EMP_DEPARTMENT


async def new_emp_department(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Шаг 2: отдел нового сотрудника (только из кнопок)."""
    dep_input = (update.message.text or "").strip()
    dep_norm = normalize_department(dep_input)
    allowed = {normalize_department(x) for x in (context.user_data.get("new_emp_departments") or set())}
    if dep_norm not in allowed:
        await update.message.reply_text("Выберите отдел одной из кнопок\\.")
        return NEW_EMP_DEPARTMENT
    context.user_data["new_emp_department"] = dep_norm
    await update.message.reply_text("Введите специализацию\\.", reply_markup=MAIN_MENU_KEYBOARD)
    return NEW_EMP_SPECIALIZATION


async def new_emp_specialization(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Шаг 3: специализация."""
    specialization = (update.message.text or "").strip()
    if not specialization:
        await update.message.reply_text("Специализация не может быть пустой\\.")
        return NEW_EMP_SPECIALIZATION
    context.user_data["new_emp_specialization"] = specialization
    await update.message.reply_text("Введите опыт работы в годах \\(число\\)\\.", parse_mode=ParseMode.MARKDOWN_V2)
    return NEW_EMP_EXPERIENCE


async def new_emp_experience(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Шаг 4: опыт."""
    raw = (update.message.text or "").strip()
    if not raw.isdigit():
        await update.message.reply_text("Введите целое число лет опыта\\.")
        return NEW_EMP_EXPERIENCE
    years = int(raw)
    if years < 0 or years > 80:
        await update.message.reply_text("Введите значение от 0 до 80\\.")
        return NEW_EMP_EXPERIENCE
    context.user_data["new_emp_experience"] = years
    await update.message.reply_text("Введите телефон\\.")
    return NEW_EMP_PHONE


async def new_emp_phone(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Шаг 5: телефон."""
    phone = (update.message.text or "").strip()
    if len(phone) < 5:
        await update.message.reply_text("Введите корректный телефон\\.")
        return NEW_EMP_PHONE
    context.user_data["new_emp_phone"] = phone
    await update.message.reply_text("Введите email\\.")
    return NEW_EMP_EMAIL


async def new_emp_email(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Шаг 6: email."""
    email = (update.message.text or "").strip()
    if "@" not in email or "." not in email:
        await update.message.reply_text("Введите корректный email\\.")
        return NEW_EMP_EMAIL
    context.user_data["new_emp_email"] = email
    await update.message.reply_text(
        "Введите дату рождения в формате ДД\\.ММ\\.ГГГГ\\.",
        parse_mode=ParseMode.MARKDOWN_V2,
    )
    return NEW_EMP_BIRTHDAY


async def new_emp_birthday(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Шаг 7: дата рождения и сохранение сотрудника."""
    birthday_input = (update.message.text or "").strip()
    birthday_iso = parse_birthday_ddmmyyyy_to_iso(birthday_input)
    if not birthday_iso:
        await update.message.reply_text(
            "Неверный формат\\. Введите дату как ДД\\.ММ\\.ГГГГ\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return NEW_EMP_BIRTHDAY

    name = str(context.user_data.get("new_emp_name", "")).strip()
    department = normalize_department(str(context.user_data.get("new_emp_department", "")).strip())
    specialization = str(context.user_data.get("new_emp_specialization", "")).strip()
    experience_years = context.user_data.get("new_emp_experience")
    phone = str(context.user_data.get("new_emp_phone", "")).strip()
    email = str(context.user_data.get("new_emp_email", "")).strip()
    if not name or not department or not specialization:
        await update.message.reply_text(
            "Недостаточно данных для создания сотрудника\\. Начните снова через /wishlist\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=MAIN_MENU_KEYBOARD,
        )
        return ConversationHandler.END

    new_employee = {
        "name": name,
        "role": specialization,
        "department": department,
        "specialization": specialization,
        "experience_years": experience_years,
        "contact": {"phone": phone, "email": email},
        "birthday": birthday_iso,
    }

    data = load_json_safe()
    employees = list(data.get("employees") or [])
    employees.append(new_employee)
    data["employees"] = employees
    if not save_data(data):
        await update.message.reply_text(
            "Не удалось сохранить сотрудника\\. Попробуйте позже\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=MAIN_MENU_KEYBOARD,
        )
        return ConversationHandler.END

    if not append_employee_to_csv(new_employee):
        await update.message.reply_text(
            "Сотрудник добавлен в JSON, но не сохранён в employees\\.csv\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=MAIN_MENU_KEYBOARD,
        )
        return ConversationHandler.END

    context.user_data["wish_author"] = name
    await update.message.reply_text(
        "Сотрудник добавлен\\. Теперь введите пожелание\\.",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=MAIN_MENU_KEYBOARD,
    )
    return WISH_ITEM


async def wish_item(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Сохранение пожелания в JSON."""
    text = (update.message.text or "").strip()
    if not text:
        await update.message.reply_text("Пустой текст\\. Опишите пожелание или /cancel\\.")
        return WISH_ITEM
    if len(text) > MAX_WISH_ITEM_LEN:
        await update.message.reply_text(f"Слишком длинно\\. До {MAX_WISH_ITEM_LEN} символов\\.")
        return WISH_ITEM

    author = context.user_data.get("wish_author") or "Аноним"
    data = load_json_safe()
    wishes = list(data.get("wishlist") or [])
    wishes.append({"author": author, "item": text})

    user = update.effective_user
    if user:
        log_activity(user.id, user.username, "wishlist_add", f"{author}: {text[:100]}")

    if save_wishlist(wishes):
        await update.message.reply_text(
            "Пожелание сохранено\\. Спасибо\\!",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=MAIN_MENU_KEYBOARD,
        )
    else:
        await update.message.reply_text(
            "Не удалось сохранить файл\\. Попробуйте позже\\.",
            parse_mode=ParseMode.MARKDOWN_V2,
        )

    context.user_data.clear()
    return ConversationHandler.END


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Отмена любого активного диалога."""
    context.user_data.clear()
    await update.message.reply_text(
        "Отменено\\.",
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=MAIN_MENU_KEYBOARD,
    )
    return ConversationHandler.END


# --- Текстовые кнопки главного меню ----------------------------------------


async def handle_menu_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Маршрутизация нажатий ReplyKeyboard (кроме «Сотрудники» — там отдельный диалог)."""
    text = (update.message.text or "").strip()
    if text == "📅 Важные даты":
        await cmd_important_dates(update, context)
    elif text == "🎁 Wishlist":
        await cmd_wishlist(update, context)
    elif text == "❓ Помощь":
        await cmd_help(update, context)


async def post_init(application: Application) -> None:
    """Инициализация после старта приложения."""
    init_activity_db()


def main() -> None:
    """Точка входа: переменная окружения BOT_TOKEN обязательна."""
    token = os.environ.get("BOT_TOKEN", "").strip()
    if not token:
        logger.error("Не задан BOT_TOKEN. Создайте файл .env по образцу .env.example")
        raise SystemExit(1)

    application = (
        Application.builder()
        .token(token)
        .post_init(post_init)
        .build()
    )

    # Диалог: добавление пожелания (callback → автор → текст)
    wish_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(cb_wish_add, pattern=r"^wish_add$")],
        states={
            WISH_AUTHOR: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, wish_author),
            ],
            WISH_PICK_EMPLOYEE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, wish_pick_employee),
            ],
            WISH_ITEM: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, wish_item),
            ],
            NEW_EMP_NAME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, new_emp_name),
            ],
            NEW_EMP_DEPARTMENT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, new_emp_department),
            ],
            NEW_EMP_SPECIALIZATION: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, new_emp_specialization),
            ],
            NEW_EMP_EXPERIENCE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, new_emp_experience),
            ],
            NEW_EMP_PHONE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, new_emp_phone),
            ],
            NEW_EMP_EMAIL: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, new_emp_email),
            ],
            NEW_EMP_BIRTHDAY: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, new_emp_birthday),
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        name="wishlist_conv",
        per_chat=True,
        per_user=True,
    )

    # Диалог: поиск сотрудника после /employees или кнопки «Сотрудники»
    emp_conv = ConversationHandler(
        entry_points=[
            CommandHandler("employees", cmd_employees),
            MessageHandler(filters.Regex("^👥 Сотрудники$"), cmd_employees),
        ],
        states={
            SEARCH_EMPLOYEE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, employee_category_chosen),
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        name="employees_conv",
        per_chat=True,
        per_user=True,
    )

    # Сначала диалоги, чтобы не перехватывались общим текстовым обработчиком
    application.add_handler(emp_conv)
    application.add_handler(wish_conv)
    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CommandHandler("help", cmd_help))
    application.add_handler(CommandHandler("find", cmd_find))
    application.add_handler(CommandHandler("department", cmd_department))
    application.add_handler(CommandHandler("email", cmd_email))
    application.add_handler(CommandHandler("important_dates", cmd_important_dates))
    application.add_handler(CommandHandler("wishlist", cmd_wishlist))
    application.add_handler(CallbackQueryHandler(cb_employee_card, pattern=r"^emp:\d+$"))

    # Кнопки меню (не команды)
    application.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, handle_menu_text),
    )

    logger.info("Бот запущен")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()

